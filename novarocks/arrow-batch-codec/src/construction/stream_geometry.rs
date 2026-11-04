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

//! Borrowed geometry of Arrow 60.1.0's default StreamWriter operations.
//! The schema uses the shared numerical author. Batch spans follow writer.rs
//! write_array_data/get_byte_array_buffers/get_list_array_buffers exactly;
//! source stock is never substituted for selected copied bytes. Dynamic
//! library growth remains an opaque operation upper bound, not an allocator
//! invoice or a value/encoding capability.

use super::{
    IpcGeometryError, SchemaGeometry, SchemaRequestAccumulator, SchemaResourceSource,
    locked_field_location_layout, schema_backing_upper_bound, schema_writer_allocation_metrics,
    type_header_geometry,
};
use arrow_array::types::{Int16Type, Int32Type, Int64Type};
use arrow_array::{
    Array, BinaryArray, BinaryViewArray, DictionaryArray, FixedSizeListArray, LargeBinaryArray,
    LargeListArray, LargeListViewArray, LargeStringArray, ListArray, ListViewArray, MapArray,
    RecordBatch, RunArray, StringArray, StringViewArray, StructArray, UnionArray,
};
use arrow_buffer::Buffer;
use arrow_schema::{DataType, Field, Schema};
use novarocks_type_contract::{
    ArrowTypeProfile, MAX_VALUE_TYPE_NODES, ValueSchemaCreation, ValueTypeError, ValueTypeVisit,
    validate_arrow_layout_structure_with_profile_and_scratch_observed,
};
use std::alloc::Layout;

#[derive(Debug)]
pub enum IpcConstructionError<H, C> {
    Creator(H),
    Geometry(IpcGeometryError),
    Shape(ValueTypeError),
    Control(C),
    HashMap(novarocks_type_contract::owned_resources::hashmap::HashMapResourceError),
}
impl<H, C> From<IpcGeometryError> for IpcConstructionError<H, C> {
    fn from(cause: IpcGeometryError) -> Self {
        Self::Geometry(cause)
    }
}
impl<H, C> From<ValueTypeError> for IpcConstructionError<H, C> {
    fn from(cause: ValueTypeError) -> Self {
        Self::Shape(cause)
    }
}
impl<H, C> From<novarocks_type_contract::owned_resources::hashmap::HashMapResourceError>
    for IpcConstructionError<H, C>
{
    fn from(
        cause: novarocks_type_contract::owned_resources::hashmap::HashMapResourceError,
    ) -> Self {
        Self::HashMap(cause)
    }
}
impl<H: std::fmt::Display, C: std::fmt::Debug> std::fmt::Display for IpcConstructionError<H, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Creator(cause) => cause.fmt(f),
            Self::Geometry(cause) => cause.fmt(f),
            Self::Shape(cause) => write!(f, "Arrow IPC carrier structure: {cause:?}"),
            Self::Control(cause) => write!(f, "Arrow IPC construction control: {cause:?}"),
            Self::HashMap(cause) => {
                write!(f, "Arrow IPC dictionary tracker source model: {cause:?}")
            }
        }
    }
}
impl<H: std::error::Error + 'static, C: std::fmt::Debug> std::error::Error
    for IpcConstructionError<H, C>
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Creator(cause) => Some(cause),
            Self::Geometry(cause) => Some(cause),
            _ => None,
        }
    }
}
/// The original caller's checkpoints and source metadata author. This contract
/// reports construction work; it creates no owner, capacity or funding grant.
pub trait ConstructionWork {
    type Error: std::fmt::Debug;
    fn step(&mut self) -> Result<(), Self::Error>;
    fn flush(&mut self) -> Result<(), Self::Error>;
    fn source_metadata_bytes(&mut self, source: &dyn Array) -> Result<usize, Self::Error>;
    fn source_metadata_and_slots(
        &mut self,
        source: &dyn Array,
    ) -> Result<(usize, usize), Self::Error>;
}

fn add(a: usize, b: usize) -> Result<usize, IpcGeometryError> {
    a.checked_add(b).ok_or(IpcGeometryError::Overflow)
}
fn mul(a: usize, b: usize) -> Result<usize, IpcGeometryError> {
    a.checked_mul(b).ok_or(IpcGeometryError::Overflow)
}
fn align(bytes: usize) -> Result<usize, IpcGeometryError> {
    Ok(add(bytes, 63)? & !63)
}
fn bits(rows: usize) -> Result<usize, IpcGeometryError> {
    Ok(add(rows, 7)? / 8)
}

/// Sealed same-operation schema geometry. This contains no source authority,
/// semantic binding, allocation measurement, budget or memory permission.
pub struct IpcStreamSchemaFacts<'a> {
    source: &'a Schema,
    fields: usize,
    dictionaries: usize,
    profile: ArrowTypeProfile,
}
#[derive(Default)]
struct Counts {
    fields: usize,
    types: usize,
    strings: usize,
    string_bytes: usize,
    children: usize,
    child_requests: usize,
    unions: usize,
    union_requests: usize,
    dictionaries: usize,
}
impl Counts {
    fn field(&mut self, field: &Field) -> Result<(), IpcGeometryError> {
        // This consumer is the typed execution stream. Provider/source schemas
        // retain their separate original annotation/provenance codec.
        if !field.metadata().is_empty() {
            return Err(IpcGeometryError::SourceModel);
        }
        self.fields = add(self.fields, 1)?;
        self.strings = add(self.strings, 1)?;
        self.string_bytes = add(self.string_bytes, field.name().len())?;
        if matches!(field.data_type(), DataType::Dictionary(..)) {
            self.dictionaries = add(self.dictionaries, 1)?;
        }
        Ok(())
    }
    fn ty(&mut self, ty: &DataType) -> Result<(), IpcGeometryError> {
        self.types = add(self.types, 1)?;
        let arity = type_header_geometry(ty);
        self.children = add(self.children, arity.children)?;
        self.child_requests = add(self.child_requests, usize::from(arity.children != 0))?;
        self.unions = add(self.unions, arity.union_ids)?;
        self.union_requests = add(self.union_requests, usize::from(arity.union_ids != 0))?;
        if let Some(bytes) = arity.timezone_bytes {
            self.strings = add(self.strings, 1)?;
            self.string_bytes = add(self.string_bytes, bytes)?;
        }
        Ok(())
    }
}

fn declaration_counts<H, W: ConstructionWork + ?Sized>(
    schema: &Schema,
    profile: ArrowTypeProfile,
    work: &mut W,
) -> Result<Counts, IpcConstructionError<H, W::Error>> {
    if !schema.metadata().is_empty() {
        return Err(IpcGeometryError::SourceModel.into());
    }
    let mut counts = Counts::default();
    for field in schema.fields() {
        work.step().map_err(IpcConstructionError::Control)?;
        counts.field(field)?;
        // Stack initialization is bracketed by control, not billed as a fake
        // completed field visit loop. The sole type-contract grammar owns the
        // validated carrier walk; this model does not interpret annotations.
        work.flush().map_err(IpcConstructionError::Control)?;
        let mut scratch = [None; MAX_VALUE_TYPE_NODES];
        work.flush().map_err(IpcConstructionError::Control)?;
        validate_arrow_layout_structure_with_profile_and_scratch_observed::<
            IpcConstructionError<H, W::Error>,
        >(field.data_type(), profile, &mut scratch, |visit| {
            work.step().map_err(IpcConstructionError::Control)?;
            match visit {
                ValueTypeVisit::Field(field) => counts.field(field)?,
                ValueTypeVisit::TypeNode(ty) => counts.ty(ty)?,
                _ => {}
            }
            Ok(())
        })?;
    }
    Ok(counts)
}

/// Borrow the already installed recipe's declaration for a schema-free unit.
/// No Schema FlatBuffer or schema-constructor allocation is attributed here.
pub fn observe_unit_writer_declaration<'a, H, W: ConstructionWork + ?Sized>(
    schema: &'a Schema,
    work: &mut W,
) -> Result<IpcStreamSchemaFacts<'a>, IpcConstructionError<H, W::Error>> {
    let counts = declaration_counts::<H, W>(schema, ArrowTypeProfile::Value, work)?;
    Ok(IpcStreamSchemaFacts {
        source: schema,
        fields: counts.fields,
        dictionaries: counts.dictionaries,
        profile: ArrowTypeProfile::Value,
    })
}

/// Observe an actual schema-free IpcDataGenerator::encode operation. The
/// original batch supplies its immutable declaration and all copied extents;
/// no Schema message is written or included in the operation's request bound.
pub fn observe_unit_batch_construction<'a, H, W: ConstructionWork + ?Sized>(
    batch: &'a RecordBatch,
    creation: &mut ValueSchemaCreation<'_, H>,
    work: &mut W,
) -> Result<IpcStreamBatchFacts<'a>, IpcConstructionError<H, W::Error>> {
    let facts = observe_unit_writer_declaration::<H, W>(batch.schema_ref().as_ref(), work)?;
    observe_stream_writer_batch_construction(batch, &facts, creation, work)
}

pub fn observe_stream_writer_schema_construction<'a, H, W: ConstructionWork + ?Sized>(
    schema: &'a Schema,
    creation: &mut ValueSchemaCreation<'_, H>,
    work: &mut W,
) -> Result<IpcStreamSchemaFacts<'a>, IpcConstructionError<H, W::Error>> {
    let counts = declaration_counts::<H, W>(schema, ArrowTypeProfile::Physical, work)?;
    let backing = schema_backing_upper_bound(
        counts.fields,
        counts.types,
        0,
        counts.strings,
        counts.string_bytes,
    )?;
    let metrics = schema_writer_allocation_metrics::<IpcConstructionError<H, W::Error>>(
        SchemaGeometry {
            backing,
            tables: add(add(counts.fields, counts.types)?, 2)?,
            metadata_entries: 0,
            string_bytes: counts.string_bytes,
            field_occurrences: counts.fields,
            type_occurrences: counts.types,
            metadata_nonempty_fields: 0,
            child_offset_items: counts.children,
            child_offset_requests: counts.child_requests,
            union_id_items: counts.unions,
            union_id_requests: counts.union_requests,
        },
        SchemaResourceSource {
            // Preserve this Arrow library owner's existing conservative model.
            // Codec-owned source walkers supply their captured node count instead.
            walker_node_bound: if counts.types == counts.fields {
                1
            } else {
                MAX_VALUE_TYPE_NODES
            },
            // Actual copied name/parameter byte operations are known here.
            // This is a work bound; it is never an original source invoice.
            source_retained_bytes: counts.string_bytes,
            scratch_work: 0,
        },
        locked_field_location_layout(),
        &mut || work.step().map_err(IpcConstructionError::Control),
    )?;
    let mut dynamic = SchemaRequestAccumulator::default();
    // StreamWriter uses FlatBufferBuilder::new, unlike the codec emitter's
    // explicitly reserved builder. Its grow_downwards doubles from one byte;
    // include the complete prospective growth history independently.
    dynamic.geometric::<u8, IpcConstructionError<H, W::Error>>(backing, 1, 1, &mut || {
        work.step().map_err(IpcConstructionError::Control)
    })?;
    dynamic.geometric::<i64, IpcConstructionError<H, W::Error>>(
        counts.dictionaries,
        0,
        1,
        &mut || work.step().map_err(IpcConstructionError::Control),
    )?;
    work.flush().map_err(IpcConstructionError::Control)?;
    creation
        .observe_opaque_upper_bound(add(metrics.request_bytes, dynamic.bytes)?)
        .map_err(IpcConstructionError::Creator)?;
    Ok(IpcStreamSchemaFacts {
        source: schema,
        fields: counts.fields,
        dictionaries: counts.dictionaries,
        profile: ArrowTypeProfile::Physical,
    })
}

#[derive(Default)]
struct Body {
    nodes: usize,
    buffers: usize,
    copied: usize,
    scratch: usize,
    variadic_counts: usize,
    metadata_scratch: usize,
}
impl Body {
    fn buffer(&mut self, bytes: usize) -> Result<(), IpcGeometryError> {
        self.buffers = add(self.buffers, 1)?;
        self.copied = add(self.copied, align(bytes)?)?;
        Ok(())
    }
    fn bitmap(&mut self, rows: usize) -> Result<(), IpcGeometryError> {
        let bytes = bits(rows)?;
        self.buffer(bytes)?;
        // The original all-valid or misaligned BooleanBuffer::sliced branch
        // can construct this MutableBuffer; its locked capacity rounds to 64.
        self.scratch = add(self.scratch, align(bytes)?)?;
        Ok(())
    }
    fn offsets(&mut self, rows: usize, width: usize, start: usize) -> Result<(), IpcGeometryError> {
        let bytes = mul(add(rows, 1)?, width)?;
        self.buffer(bytes)?;
        if rows == 0 || start != 0 {
            self.scratch = add(self.scratch, align(bytes)?)?;
        }
        Ok(())
    }
}
fn offsets_span<O: TryInto<usize> + Copy>(
    offsets: &[O],
    start: usize,
    rows: usize,
) -> Result<(usize, usize), IpcGeometryError> {
    let end = add(start, rows)?;
    let first = offsets
        .get(start)
        .copied()
        .ok_or(IpcGeometryError::SourceModel)?
        .try_into()
        .map_err(|_| IpcGeometryError::SourceModel)?;
    let last = offsets
        .get(end)
        .copied()
        .ok_or(IpcGeometryError::SourceModel)?
        .try_into()
        .map_err(|_| IpcGeometryError::SourceModel)?;
    if last < first {
        return Err(IpcGeometryError::SourceModel);
    }
    Ok((first, last - first))
}
fn concrete<T: 'static>(array: &dyn Array) -> Result<&T, IpcGeometryError> {
    array
        .as_any()
        .downcast_ref()
        .ok_or(IpcGeometryError::SourceModel)
}

#[derive(Clone, Copy)]
struct PhysicalWalkDepth {
    current: usize,
    profile: ArrowTypeProfile,
}
impl PhysicalWalkDepth {
    fn root(profile: ArrowTypeProfile) -> Self {
        Self {
            current: 1,
            profile,
        }
    }
    fn child(self) -> Self {
        Self {
            current: self.current + 1,
            profile: self.profile,
        }
    }
    fn exceeded(self) -> bool {
        self.profile
            .physical_depth_limit()
            .is_some_and(|limit| self.current > limit)
    }
}
fn window<H, W: ConstructionWork + ?Sized>(
    array: &dyn Array,
    start: usize,
    rows: usize,
    body: &mut Body,
    work: &mut W,
    depth: PhysicalWalkDepth,
) -> Result<(), IpcConstructionError<H, W::Error>> {
    work.step().map_err(IpcConstructionError::Control)?;
    if depth.exceeded() || add(start, rows)? > array.len() {
        return Err(IpcGeometryError::SourceModel.into());
    }
    body.nodes = add(body.nodes, 1)?;
    if !matches!(
        array.data_type(),
        DataType::Null | DataType::Union(..) | DataType::RunEndEncoded(..)
    ) {
        body.bitmap(rows)?;
    }
    let ty = array.data_type();
    if let Some(width) = ty.primitive_width() {
        body.buffer(mul(rows, width)?)?;
        return Ok(());
    }
    match ty {
        DataType::Null => {}
        DataType::Boolean => body.bitmap(rows)?,
        DataType::FixedSizeBinary(width) => {
            body.buffer(mul(
                rows,
                usize::try_from(*width).map_err(|_| IpcGeometryError::SourceModel)?,
            )?)?;
        }
        DataType::Utf8 | DataType::Binary => {
            let offsets = if matches!(ty, DataType::Utf8) {
                concrete::<StringArray>(array)?.value_offsets()
            } else {
                concrete::<BinaryArray>(array)?.value_offsets()
            };
            let (offset, bytes) = offsets_span(offsets, start, rows)?;
            body.offsets(rows, 4, offset)?;
            body.buffer(if rows == 0 { 0 } else { bytes })?;
        }
        DataType::LargeUtf8 | DataType::LargeBinary => {
            let offsets = if matches!(ty, DataType::LargeUtf8) {
                concrete::<LargeStringArray>(array)?.value_offsets()
            } else {
                concrete::<LargeBinaryArray>(array)?.value_offsets()
            };
            let (offset, bytes) = offsets_span(offsets, start, rows)?;
            body.offsets(rows, 8, offset)?;
            body.buffer(if rows == 0 { 0 } else { bytes })?;
        }
        DataType::Utf8View | DataType::BinaryView => {
            let buffers = if matches!(ty, DataType::Utf8View) {
                concrete::<StringViewArray>(array)?.data_buffers()
            } else {
                concrete::<BinaryViewArray>(array)?.data_buffers()
            };
            // Pinned writer truncates the u128 views buffer, but deliberately
            // writes every existing external data buffer in its visible extent.
            body.buffer(mul(rows, std::mem::size_of::<u128>())?)?;
            for buffer in buffers.iter() {
                work.step().map_err(IpcConstructionError::Control)?;
                body.buffer(buffer.len())?;
            }
            body.variadic_counts = add(body.variadic_counts, 1)?;
        }
        DataType::ListView(_) | DataType::LargeListView(_) => {
            let (width, child) = if matches!(ty, DataType::ListView(_)) {
                (4, concrete::<ListViewArray>(array)?.values())
            } else {
                (8, concrete::<LargeListViewArray>(array)?.values())
            };
            body.buffer(mul(rows, width)?)?;
            body.buffer(mul(rows, width)?)?;
            // Original get_list_view_array_buffers keeps its full child for
            // nonempty views; only empty input slices the child to zero rows.
            window::<H, W>(
                child.as_ref(),
                0,
                if rows == 0 { 0 } else { child.len() },
                body,
                work,
                depth.child(),
            )?;
        }
        DataType::Union(fields, mode) => {
            let union = concrete::<UnionArray>(array)?;
            // write_array_data's generic Union branch copies the original
            // visible buffers; it does not optimize child dictionaries/arrays.
            body.buffer(union.type_ids().inner().len())?;
            if *mode == arrow_schema::UnionMode::Dense {
                body.buffer(
                    union
                        .offsets()
                        .ok_or(IpcGeometryError::SourceModel)?
                        .inner()
                        .len(),
                )?;
            }
            for (id, _) in fields.iter() {
                work.step().map_err(IpcConstructionError::Control)?;
                let child = union.child(id);
                if *mode == arrow_schema::UnionMode::Sparse {
                    // Arrow60.1 slices each owned sparse child ArrayData before
                    // writing it. The same source visitor bounds that clone's
                    // metadata separately from the outer column.to_data().
                    body.metadata_scratch = add(
                        body.metadata_scratch,
                        work.source_metadata_bytes(child.as_ref())
                            .map_err(IpcConstructionError::Control)?,
                    )?;
                }
                window::<H, W>(child.as_ref(), 0, child.len(), body, work, depth.child())?;
            }
        }
        DataType::RunEndEncoded(ends, _) => {
            macro_rules! runs {
                ($key:ty, $width:expr) => {{
                    let run = concrete::<RunArray<$key>>(array)?;
                    work.flush().map_err(IpcConstructionError::Control)?;
                    let (first, count) = if rows == 0 {
                        (0, 0)
                    } else {
                        let first = run.get_physical_index(start);
                        let last = run.get_physical_index(add(start, rows)? - 1);
                        (
                            first,
                            add(
                                last.checked_sub(first)
                                    .ok_or(IpcGeometryError::SourceModel)?,
                                1,
                            )?,
                        )
                    };
                    work.flush().map_err(IpcConstructionError::Control)?;
                    // Original unslice_run_array authors a primitive run-end
                    // child, then the physically selected values slice. It
                    // rebases end positions in a BufferBuilder on the sliced
                    // path; it never expands logical rows into plain values.
                    body.nodes = add(body.nodes, 1)?;
                    body.bitmap(count)?;
                    body.buffer(mul(count, $width)?)?;
                    if start != 0 || run.offset() != 0 || rows != run.run_ends().max_value() {
                        body.scratch = add(body.scratch, align(mul(count, $width)?)?)?;
                    }
                    window::<H, W>(
                        run.values().as_ref(),
                        first,
                        count,
                        body,
                        work,
                        depth.child(),
                    )?;
                }};
            }
            match ends.data_type() {
                DataType::Int16 => runs!(Int16Type, 2),
                DataType::Int32 => runs!(Int32Type, 4),
                DataType::Int64 => runs!(Int64Type, 8),
                _ => return Err(IpcGeometryError::SourceModel.into()),
            }
        }
        DataType::List(_) => {
            let list = concrete::<ListArray>(array)?;
            let (offset, count) = offsets_span(list.value_offsets(), start, rows)?;
            body.offsets(rows, 4, offset)?;
            window::<H, W>(
                list.values().as_ref(),
                if rows == 0 { 0 } else { offset },
                if rows == 0 { 0 } else { count },
                body,
                work,
                depth.child(),
            )?;
        }
        DataType::LargeList(_) => {
            let list = concrete::<LargeListArray>(array)?;
            let (offset, count) = offsets_span(list.value_offsets(), start, rows)?;
            body.offsets(rows, 8, offset)?;
            window::<H, W>(
                list.values().as_ref(),
                if rows == 0 { 0 } else { offset },
                if rows == 0 { 0 } else { count },
                body,
                work,
                depth.child(),
            )?;
        }
        DataType::Map(_, _) => {
            let map = concrete::<MapArray>(array)?;
            let (offset, count) = offsets_span(map.value_offsets(), start, rows)?;
            body.offsets(rows, 4, offset)?;
            window::<H, W>(
                map.entries(),
                if rows == 0 { 0 } else { offset },
                if rows == 0 { 0 } else { count },
                body,
                work,
                depth.child(),
            )?;
        }
        DataType::FixedSizeList(_, width) => {
            let list = concrete::<FixedSizeListArray>(array)?;
            let width = usize::try_from(*width).map_err(|_| IpcGeometryError::SourceModel)?;
            window::<H, W>(
                list.values().as_ref(),
                mul(start, width)?,
                mul(rows, width)?,
                body,
                work,
                depth.child(),
            )?;
        }
        DataType::Struct(_) => {
            for child in concrete::<StructArray>(array)?.columns() {
                window::<H, W>(child.as_ref(), start, rows, body, work, depth.child())?;
            }
        }
        DataType::Dictionary(keys, values)
            if keys.as_ref() == &DataType::Int32
                && matches!(values.as_ref(), DataType::Utf8 | DataType::LargeUtf8) =>
        {
            let dict = concrete::<DictionaryArray<Int32Type>>(array)?;
            body.buffer(mul(rows, 4)?)?;
            // RecordBatch nodes contain only the physical keys. Values have
            // their separate DictionaryBatch message, observed below.
            let _ = dict;
        }
        _ => return Err(IpcGeometryError::SourceModel.into()),
    }
    Ok(())
}

fn message_upper<H, W: ConstructionWork + ?Sized>(
    body: &Body,
    dictionary: bool,
    requests: &mut SchemaRequestAccumulator,
    work: &mut W,
) -> Result<(), IpcConstructionError<H, W::Error>> {
    // The generated FieldNode and Buffer wire structs each contain two i64
    // words. Their private Vec growth is an opaque bound, never a Layout
    // invoice. Message/RecordBatch slots use the SAME schema table envelope;
    // a DictionaryBatch adds one table in the existing numerical author.
    let tables = if dictionary { 3 } else { 2 };
    let headers = schema_backing_upper_bound(0, usize::from(dictionary), 0, 0, 0)?;
    let descriptors = mul(
        add(body.nodes, body.buffers)?,
        std::mem::size_of::<[i64; 2]>(),
    )?;
    // Two length-prefixed vectors with alignment padding are additional to
    // the schema envelope's existing vector/root padding.
    let mut backing = add(add(headers, descriptors)?, mul(2, 4 + 7)?)?;
    if body.variadic_counts != 0 {
        backing = add(backing, add(mul(body.variadic_counts, 8)?, 4 + 7)?)?;
    }
    // write_to_sink reuses a private Vec<EncodedBuffer> across dictionary
    // and record-batch messages. Each element is Raw(Buffer) or
    // Compressed(Vec<u8>); public payload size plus one alignment word is
    // a source-reviewed opaque enum envelope, not a private Layout claim.
    // Any doubling from a smaller existing capacity requests less than four
    // times the required slots in total, including old/new growth requests.
    let encoded_entry = add(
        std::mem::size_of::<Buffer>().max(std::mem::size_of::<Vec<u8>>()),
        std::mem::align_of::<Buffer>().max(std::mem::align_of::<Vec<u8>>()),
    )?;
    requests.bytes = add(
        requests.bytes,
        add(
            body.metadata_scratch,
            mul(mul(body.buffers, 4)?, encoded_entry)?,
        )?,
    )?;
    let mut observe = || work.step().map_err(IpcConstructionError::Control);
    requests.geometric::<[i64; 2], IpcConstructionError<H, W::Error>>(
        body.nodes,
        0,
        1,
        &mut observe,
    )?;
    requests.geometric::<[i64; 2], IpcConstructionError<H, W::Error>>(
        body.buffers,
        0,
        1,
        &mut observe,
    )?;
    requests.geometric::<i64, IpcConstructionError<H, W::Error>>(
        body.variadic_counts,
        4,
        1,
        &mut observe,
    )?;
    // Vec<u8>'s locked minimum nonzero growth is eight; Arrow MutableBuffer
    // capacity is separately the rounded scratch extent from the real branch.
    requests.geometric::<u8, IpcConstructionError<H, W::Error>>(body.copied, 8, 1, &mut observe)?;
    requests.exact::<u8, IpcConstructionError<H, W::Error>>(body.scratch, 1, &mut observe)?;
    requests.geometric::<u8, IpcConstructionError<H, W::Error>>(backing, 8, 1, &mut observe)?;
    requests.exact::<u8, IpcConstructionError<H, W::Error>>(backing, 1, &mut observe)?;
    let location = locked_field_location_layout();
    for capacity in [4, 8] {
        let extent = Layout::from_size_align(mul(location.size(), capacity)?, location.align())
            .map_err(|_| IpcGeometryError::Overflow)?;
        requests.layout(extent, 1, &mut observe)?;
    }
    requests.geometric::<u32, IpcConstructionError<H, W::Error>>(tables, 4, 1, &mut observe)?;
    Ok(())
}

fn dictionary_messages<H, W: ConstructionWork + ?Sized>(
    array: &dyn Array,
    requests: &mut SchemaRequestAccumulator,
    work: &mut W,
    depth: PhysicalWalkDepth,
) -> Result<(), IpcConstructionError<H, W::Error>> {
    work.step().map_err(IpcConstructionError::Control)?;
    if depth.exceeded() {
        return Err(IpcGeometryError::SourceModel.into());
    }
    match array.data_type() {
        DataType::Dictionary(keys, values)
            if keys.as_ref() == &DataType::Int32
                && matches!(values.as_ref(), DataType::Utf8 | DataType::LargeUtf8) =>
        {
            let values = concrete::<DictionaryArray<Int32Type>>(array)?.values();
            // collect_dict_updates and DictionaryTracker each call to_data;
            // make_array reconstructs the values for recursive discovery.
            // Arrow60.1 additionally clones the values ArrayData into the
            // collected DictionaryToEncode and again for message encoding.
            // Each operation keeps its own source-derived metadata envelope.
            for source in [
                array,
                array,
                values.as_ref(),
                values.as_ref(),
                values.as_ref(),
            ] {
                let metadata = work
                    .source_metadata_bytes(source)
                    .map_err(IpcConstructionError::Control)?;
                requests.bytes = add(requests.bytes, metadata)?;
            }
            let mut body = Body::default();
            window::<H, W>(
                values.as_ref(),
                0,
                values.len(),
                &mut body,
                work,
                depth.child(),
            )?;
            message_upper(&body, true, requests, work)?;
        }
        DataType::List(_) => dictionary_messages::<H, W>(
            concrete::<ListArray>(array)?.values().as_ref(),
            requests,
            work,
            depth.child(),
        )?,
        DataType::LargeList(_) => dictionary_messages::<H, W>(
            concrete::<LargeListArray>(array)?.values().as_ref(),
            requests,
            work,
            depth.child(),
        )?,
        DataType::RunEndEncoded(ends, _) => {
            macro_rules! runs {
                ($key:ty) => {{
                    let run = concrete::<RunArray<$key>>(array)?;
                    // Original dictionary discovery reconstructs the values
                    // from column.to_data before descending into its field.
                    let metadata = work
                        .source_metadata_bytes(array)
                        .map_err(IpcConstructionError::Control)?;
                    requests.bytes = add(requests.bytes, metadata)?;
                    dictionary_messages::<H, W>(
                        run.values().as_ref(),
                        requests,
                        work,
                        depth.child(),
                    )?;
                }};
            }
            match ends.data_type() {
                DataType::Int16 => runs!(Int16Type),
                DataType::Int32 => runs!(Int32Type),
                DataType::Int64 => runs!(Int64Type),
                _ => return Err(IpcGeometryError::SourceModel.into()),
            }
        }
        DataType::ListView(_) => dictionary_messages::<H, W>(
            concrete::<ListViewArray>(array)?.values().as_ref(),
            requests,
            work,
            depth.child(),
        )?,
        DataType::LargeListView(_) => dictionary_messages::<H, W>(
            concrete::<LargeListViewArray>(array)?.values().as_ref(),
            requests,
            work,
            depth.child(),
        )?,
        DataType::Union(fields, _) => {
            let union = concrete::<UnionArray>(array)?;
            for (id, _) in fields.iter() {
                work.step().map_err(IpcConstructionError::Control)?;
                dictionary_messages::<H, W>(
                    union.child(id).as_ref(),
                    requests,
                    work,
                    depth.child(),
                )?;
            }
        }
        DataType::FixedSizeList(_, _) => dictionary_messages::<H, W>(
            concrete::<FixedSizeListArray>(array)?.values().as_ref(),
            requests,
            work,
            depth.child(),
        )?,
        DataType::Map(_, _) => dictionary_messages::<H, W>(
            concrete::<MapArray>(array)?.entries(),
            requests,
            work,
            depth.child(),
        )?,
        DataType::Struct(_) => {
            for child in concrete::<StructArray>(array)?.columns() {
                dictionary_messages::<H, W>(child.as_ref(), requests, work, depth.child())?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Observe one actual default StreamWriter::write before invoking the locked
/// library. The stream must be a fresh writer, uncompressed MetadataV5 with
/// alignment 64, using the schema sealed by the preceding schema observer.
/// Repeated dictionary IDs may avoid replacement; every actual dictionary
/// occurrence is a conservative potential message, without expanding support.
/// Existing Array payloads remain source-owned. Only copied selected extents,
/// temporary Arrow metadata and opaque private growth are observed here.
pub fn observe_stream_writer_batch_construction<'a, H, W: ConstructionWork + ?Sized>(
    batch: &'a RecordBatch,
    schema: &IpcStreamSchemaFacts<'_>,
    creation: &mut ValueSchemaCreation<'_, H>,
    work: &mut W,
) -> Result<IpcStreamBatchFacts<'a>, IpcConstructionError<H, W::Error>> {
    if !std::ptr::eq(schema.source, batch.schema_ref().as_ref()) {
        return Err(IpcGeometryError::SourceModel.into());
    }
    let mut requests = SchemaRequestAccumulator::default();
    // Arrow60.1 collect_all_dicts creates flattened_fields and reserves a
    // private DictionaryToEncode vector at that field count. Its ArrayData,
    // i64 and bool payloads plus alignment padding give an opaque upper;
    // no private layout is mirrored or asserted. StreamWriter then reserves
    // one public (header_len, body_len) pair per possible dictionary message.
    requests.exact::<&Field, IpcConstructionError<H, W::Error>>(schema.fields, 1, &mut || {
        work.step().map_err(IpcConstructionError::Control)
    })?;
    requests.bytes = add(
        requests.bytes,
        mul(
            schema.fields,
            add(
                std::mem::size_of::<arrow_data::ArrayData>(),
                add(
                    std::mem::size_of::<i64>(),
                    mul(
                        2,
                        std::mem::align_of::<arrow_data::ArrayData>()
                            .max(std::mem::align_of::<i64>()),
                    )?,
                )?,
            )?,
        )?,
    )?;
    requests.exact::<(usize, usize), IpcConstructionError<H, W::Error>>(
        schema.dictionaries,
        1,
        &mut || work.step().map_err(IpcConstructionError::Control),
    )?;
    requests.exact::<i64, IpcConstructionError<H, W::Error>>(
        schema.dictionaries,
        1,
        &mut || work.step().map_err(IpcConstructionError::Control),
    )?;
    let tracker = novarocks_type_contract::owned_resources::hashmap::
        incremental_insert_allocation_upper_bound_observed::<i64, arrow_data::ArrayData, IpcConstructionError<H, W::Error>>(
            schema.dictionaries, || work.step().map_err(IpcConstructionError::Control),
        )?;
    requests.bytes = add(requests.bytes, tracker)?;
    let mut body = Body::default();
    let mut source_buffer_slots = 0usize;
    let mut to_data_metadata_upper = 0;
    for array in batch.columns() {
        // ONE original borrowed visitor supplies actual source buffer slots
        // and one to_data operation's metadata envelope. This contribution is
        // for record_batch_to_bytes ONLY; dictionary operations are separate.
        let (metadata, slots) = work
            .source_metadata_and_slots(array.as_ref())
            .map_err(IpcConstructionError::Control)?;
        source_buffer_slots = add(source_buffer_slots, slots)?;
        to_data_metadata_upper = add(to_data_metadata_upper, metadata)?;
        requests.bytes = add(requests.bytes, metadata)?;
        window::<H, W>(
            array.as_ref(),
            0,
            array.len(),
            &mut body,
            work,
            PhysicalWalkDepth::root(schema.profile),
        )?;
        dictionary_messages::<H, W>(
            array.as_ref(),
            &mut requests,
            work,
            PhysicalWalkDepth::root(schema.profile),
        )?;
    }
    // estimate_encoded_buffer_count reserves from the batch type. Its
    // ByteView estimate is three even for zero external regions; one extra
    // slot per node bounds that initial reserve beside later actual growth.
    let encoded_entry = add(
        std::mem::size_of::<Buffer>().max(std::mem::size_of::<Vec<u8>>()),
        std::mem::align_of::<Buffer>().max(std::mem::align_of::<Vec<u8>>()),
    )?;
    requests.bytes = add(
        requests.bytes,
        mul(add(body.buffers, body.nodes)?, encoded_entry)?,
    )?;
    message_upper::<H, W>(&body, false, &mut requests, work)?;
    work.flush().map_err(IpcConstructionError::Control)?;
    creation
        .observe_opaque_upper_bound(requests.bytes)
        .map_err(IpcConstructionError::Creator)?;
    Ok(IpcStreamBatchFacts {
        source: batch,
        buffer_slots_upper: source_buffer_slots,
        to_data_metadata_upper,
    })
}

/// Numeric loans from the SAME borrowed immutable source walk. No source
/// invoice, allocation measurement, semantic proof, storage owner or grant is
/// created here. The second operation below observes its own contribution.
pub struct IpcStreamBatchFacts<'a> {
    source: &'a RecordBatch,
    buffer_slots_upper: usize,
    to_data_metadata_upper: usize,
}
impl IpcStreamBatchFacts<'_> {
    pub fn buffer_slots_upper_bound(&self) -> usize {
        self.buffer_slots_upper
    }
}

/// Observe the independent record_batch_bytes/logical_bytes operation before
/// its allocating to_data + empty-then-insert HashSet<usize> body. Its source
/// loan must be the same immutable RecordBatch. Existing buffer deduplication
/// and returned byte counts remain wholly owned by the original caller.
pub fn observe_stream_logical_size_construction<H, W: ConstructionWork + ?Sized>(
    batch: &RecordBatch,
    facts: &IpcStreamBatchFacts<'_>,
    creation: &mut ValueSchemaCreation<'_, H>,
    work: &mut W,
) -> Result<(), IpcConstructionError<H, W::Error>> {
    if !std::ptr::eq(facts.source, batch) {
        return Err(IpcGeometryError::SourceModel.into());
    }
    // HashSet's exact locked table delegate is HashMap<usize, ()>. Its
    // occurrence bound comes from real visited source buffers, including
    // duplicate identities; it never claims the source retained capacity.
    let table = novarocks_type_contract::owned_resources::hashmap::
        incremental_insert_allocation_upper_bound_observed::<usize, (), IpcConstructionError<H, W::Error>>(
            facts.buffer_slots_upper, || work.step().map_err(IpcConstructionError::Control),
        )?;
    let upper = add(table, facts.to_data_metadata_upper)?;
    work.flush().map_err(IpcConstructionError::Control)?;
    creation
        .observe_opaque_upper_bound(upper)
        .map_err(IpcConstructionError::Creator)?;
    Ok(())
}
