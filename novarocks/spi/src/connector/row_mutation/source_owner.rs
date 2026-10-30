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

//! Construction ownership for a bounded COW source batch. No constructor
//! adopts an arbitrary Arrow allocation. Buffers are copied here; containers
//! are allocated here; the only schema/type backing comes from the checked
//! immutable schema. The opaque receipts cannot be detached from their batch.

use std::mem::size_of;
use std::ptr::NonNull;
use std::sync::Arc;

use arrow::array::{ArrayData, ArrayRef, RecordBatch, RecordBatchOptions, make_array};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};

use crate::connector::ConnectorPayloadRetentionGuard;

use super::{ConnectorError, ConnectorErrorKind, ConnectorRowConversionFootprint};

pub const MAX_CONNECTOR_ROW_MUTATION_SOURCE_BYTES: usize = 32 * 1024 * 1024;
const MAX_SCHEMA_BYTES: usize = 1024 * 1024;
const MAX_SCHEMA_NAMES_BYTES: usize = 256 * 1024;
const MAX_NAME_BYTES: usize = 64 * 1024;
const MAX_NODES: usize = 4096;
const MAX_DEPTH: usize = 64;
const MAX_ROWS: usize = 1024 * 1024;
// Covers the ArrayData/ArrayRef/header allocations and temporary constructor
// containers for one node. Payload and every vector capacity are separate.
const NODE_CONSTRUCTION_BYTES: usize = 2048;
const BUFFER_HEADER_BYTES: usize = 256;

#[derive(Debug)]
struct SourceIdentity {
    _retention: Option<ConnectorPayloadRetentionGuard>,
}

#[derive(Debug)]
pub struct ConnectorRowMutationSourceBuilder {
    schema: SchemaRef,
    identity: Arc<SourceIdentity>,
    charged: usize,
    nodes: usize,
    retention: Option<ConnectorPayloadRetentionGuard>,
    budgeted: bool,
}

#[derive(Debug)]
pub struct ConnectorRowMutationSourceBuffer {
    identity: Arc<SourceIdentity>,
    buffer: Buffer,
}

#[derive(Debug)]
pub struct ConnectorRowMutationSourceArray {
    identity: Arc<SourceIdentity>,
    data: ArrayData,
    depth: usize,
}

#[derive(Debug)]
pub struct ConnectorRowMutationSourceChildren {
    identity: Arc<SourceIdentity>,
    items: Vec<ConnectorRowMutationSourceArray>,
    limit: usize,
}

/// A receipt for this immutable batch's complete source ownership, including
/// conservative constructor coexistence. Only the safe factories above mint it.
#[derive(Debug)]
pub struct ConnectorRowMutationSourceBatch {
    batch: RecordBatch,
    source_bytes: usize,
    retention: Option<ConnectorPayloadRetentionGuard>,
}

impl ConnectorRowMutationSourceBatch {
    pub const fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    pub const fn source_bytes(&self) -> usize {
        self.source_bytes
    }

    pub(super) fn into_parts(self) -> (RecordBatch, Option<ConnectorPayloadRetentionGuard>) {
        (self.batch, self.retention)
    }
}

impl ConnectorRowMutationSourceChildren {
    pub fn push(&mut self, child: ConnectorRowMutationSourceArray) -> Result<(), ConnectorError> {
        if self.items.len() == self.limit || !Arc::ptr_eq(&self.identity, &child.identity) {
            return Err(invalid());
        }
        // The factory reserved the whole capacity. This push never grows it.
        self.items.push(child);
        Ok(())
    }
}

impl ConnectorRowMutationSourceBuilder {
    pub fn try_new(schema: SchemaRef) -> Result<Self, ConnectorError> {
        Self::try_new_inner(schema, None, false)
    }

    /// Governed construction requires the original byte-budget author.
    /// Plain lifetime retention cannot authorize new payload allocation.
    pub fn try_new_with_guard(
        schema: SchemaRef,
        guard: ConnectorPayloadRetentionGuard,
    ) -> Result<Self, ConnectorError> {
        if !guard.is_byte_budget() {
            return Err(no_byte_budget());
        }
        Self::try_new_inner(schema, Some(guard), true)
    }

    /// Explicit local lifetime retention; no Root byte authorization is claimed.
    /// Budget authors must use the governed constructor instead.
    pub fn try_new_with_retention(
        schema: SchemaRef,
        guard: ConnectorPayloadRetentionGuard,
    ) -> Result<Self, ConnectorError> {
        if guard.is_byte_budget() {
            return Err(no_byte_budget());
        }
        Self::try_new_inner(schema, Some(guard), false)
    }

    fn try_new_inner(
        schema: SchemaRef,
        retention: Option<ConnectorPayloadRetentionGuard>,
        budgeted: bool,
    ) -> Result<Self, ConnectorError> {
        if schema.fields().len() > MAX_NODES {
            return Err(exhausted());
        }
        let mut shape = SchemaShape::default();
        for field in schema.fields() {
            shape.field(field, 0)?;
        }
        let footprint = ConnectorRowConversionFootprint::for_schema(schema.as_ref())?;
        if footprint.schema_bytes > MAX_SCHEMA_BYTES {
            return Err(exhausted());
        }
        // Reserve before the identity Arc allocation or any Arrow construction.
        // Arrow's invalid-parts diagnostics may display two nested types.
        // Debug-escaped text is at most six times its bytes; keep that error
        // construction alongside the retained schema, before calling Arrow.
        let charged = checked_add(checked_mul(footprint.schema_bytes, 13)?, 512)?;
        Ok(Self {
            schema,
            identity: Arc::new(SourceIdentity {
                _retention: retention.clone(),
            }),
            charged,
            nodes: 0,
            retention,
            budgeted,
        })
    }

    fn reserve(&mut self, bytes: usize) -> Result<(), ConnectorError> {
        let next = checked_add(self.charged, bytes)?;
        self.charged = next;
        Ok(())
    }

    pub const fn reserved_bytes(&self) -> usize {
        self.charged
    }

    pub fn children(
        &mut self,
        count: usize,
    ) -> Result<ConnectorRowMutationSourceChildren, ConnectorError> {
        if count > MAX_NODES {
            return Err(exhausted());
        }
        self.reserve(checked_mul(
            count,
            size_of::<ConnectorRowMutationSourceArray>(),
        )?)?;
        Ok(ConnectorRowMutationSourceChildren {
            identity: Arc::clone(&self.identity),
            items: Vec::with_capacity(count),
            limit: count,
        })
    }

    pub fn copy_buffer(
        &mut self,
        bytes: &[u8],
    ) -> Result<ConnectorRowMutationSourceBuffer, ConnectorError> {
        let capacity = bytes.len().checked_add(63).ok_or_else(exhausted)? & !63;
        // Preserve the original local construction profile independently of
        // the Root payload budget: its header allowance is not payload bytes.
        let next = checked_add(self.charged, checked_add(capacity, BUFFER_HEADER_BYTES)?)?;
        let guard = if self.budgeted {
            // Pinned Arrow from_slice_ref uses a round-to-64 Layout of exactly
            // this capacity. Reserve before that allocation, including zero.
            Some(
                self.retention
                    .as_ref()
                    .expect("governed source author")
                    .reserve_owned_bytes(capacity)?,
            )
        } else {
            self.retention
                .as_ref()
                .map(ConnectorPayloadRetentionGuard::try_clone)
                .transpose()
                .map_err(|_| exhausted())?
        };
        let original = Buffer::from_slice_ref(bytes);
        let buffer = match guard {
            Some(guard) => retain_buffer(original, guard),
            None => original,
        };
        self.charged = next;
        Ok(ConnectorRowMutationSourceBuffer {
            identity: Arc::clone(&self.identity),
            buffer,
        })
    }

    /// Copy an already materialized ArrayData into this owner. The complete
    /// borrowed tree is checked before the first copy. No caller buffer,
    /// child container or DataType allocation is retained. The caller owns
    /// the original source and any work needed to materialize ArrayData.
    pub fn copy_data(
        &mut self,
        node: usize,
        data: &ArrayData,
    ) -> Result<ConnectorRowMutationSourceArray, ConnectorError> {
        let mut plan = CopyPlan::default();
        self.plan_copy(node, data, 0, &mut plan)?;
        if self
            .nodes
            .checked_add(plan.nodes)
            .is_none_or(|n| n > MAX_NODES)
        {
            return Err(exhausted());
        }
        checked_add(self.charged, plan.bytes)?;
        self.copy_data_inner(node, data).map(|(array, _)| array)
    }

    fn plan_copy(
        &self,
        node: usize,
        data: &ArrayData,
        depth: usize,
        plan: &mut CopyPlan,
    ) -> Result<usize, ConnectorError> {
        if depth >= MAX_DEPTH || data.len() > MAX_ROWS || plan.nodes == MAX_NODES {
            return Err(exhausted());
        }
        plan.nodes += 1;
        plan.rows = checked_add(plan.rows, data.len())?;
        if plan.rows > MAX_CONNECTOR_ROW_MUTATION_SOURCE_BYTES / size_of::<usize>() {
            return Err(exhausted());
        }
        let dt = type_at(&self.schema, node).ok_or_else(invalid)?;
        if data.offset() != 0 || data.data_type() != dt {
            return Err(invalid());
        }
        let parts = logical_parts(data)?;
        plan.bytes = checked_add(
            plan.bytes,
            checked_add(
                NODE_CONSTRUCTION_BYTES,
                checked_add(
                    checked_mul(
                        data.child_data().len(),
                        size_of::<ConnectorRowMutationSourceArray>()
                            + size_of::<ArrayData>()
                            + 2 * size_of::<ArrayRef>(),
                    )?,
                    checked_mul(2, size_of::<Buffer>())?,
                )?,
            )?,
        )?;
        for bytes in [parts.validity, parts.first, parts.second]
            .into_iter()
            .flatten()
        {
            let capacity = bytes.len().checked_add(63).ok_or_else(exhausted)? & !63;
            plan.bytes = checked_add(plan.bytes, checked_add(capacity, BUFFER_HEADER_BYTES)?)?;
        }
        let expected_children = match dt {
            DataType::List(_) | DataType::LargeList(_) | DataType::Dictionary(_, _) => 1,
            DataType::Struct(fields) => fields.len(),
            _ => 0,
        };
        if data.child_data().len() != expected_children {
            return Err(invalid());
        }
        if matches!(dt, DataType::Struct(_))
            && data
                .child_data()
                .iter()
                .any(|child| child.len() != data.len())
        {
            return Err(invalid());
        }
        let mut next = node + 1;
        for child in data.child_data() {
            next = self.plan_copy(next, child, depth + 1, plan)?;
        }
        Ok(next)
    }

    fn copy_data_inner(
        &mut self,
        node: usize,
        data: &ArrayData,
    ) -> Result<(ConnectorRowMutationSourceArray, usize), ConnectorError> {
        let parts = logical_parts(data)?;
        let validity = parts
            .validity
            .map(|bytes| self.copy_buffer(bytes))
            .transpose()?;
        let first = parts
            .first
            .map(|bytes| self.copy_buffer(bytes))
            .transpose()?;
        let second = parts
            .second
            .map(|bytes| self.copy_buffer(bytes))
            .transpose()?;
        let mut children = self.children(data.child_data().len())?;
        let mut next = node + 1;
        for child in data.child_data() {
            let (copy, after) = self.copy_data_inner(next, child)?;
            next = after;
            children.push(copy)?;
        }
        Ok((
            self.array(node, data.len(), validity, first, second, children)?,
            next,
        ))
    }

    /// `node` is the schema's preorder type-node ordinal. The factory selects
    /// the type itself, so equal but independently allocated caller DataTypes
    /// cannot smuggle uncharged Field or metadata backing into the result.
    pub fn array(
        &mut self,
        node: usize,
        len: usize,
        validity: Option<ConnectorRowMutationSourceBuffer>,
        first: Option<ConnectorRowMutationSourceBuffer>,
        second: Option<ConnectorRowMutationSourceBuffer>,
        children: ConnectorRowMutationSourceChildren,
    ) -> Result<ConnectorRowMutationSourceArray, ConnectorError> {
        if len > MAX_ROWS || self.nodes == MAX_NODES {
            return Err(exhausted());
        }
        if !Arc::ptr_eq(&self.identity, &children.identity)
            || children.items.len() != children.limit
            || [&validity, &first, &second]
                .into_iter()
                .flatten()
                .any(|buffer| !Arc::ptr_eq(&self.identity, &buffer.identity))
        {
            return Err(invalid());
        }
        let dt = type_at(&self.schema, node).ok_or_else(invalid)?;
        validate_parts(dt, len, &validity, &first, &second, &children.items)?;
        let depth = children
            .items
            .iter()
            .map(|child| child.depth)
            .max()
            .unwrap_or(0)
            + 1;
        if depth > MAX_DEPTH {
            return Err(exhausted());
        }
        // Both source receipt slots and the new ArrayData slots coexist while
        // the iterator is consumed. Neither vector is sized from its length
        // after it has already grown.
        self.reserve(checked_add(
            NODE_CONSTRUCTION_BYTES,
            checked_add(
                checked_mul(
                    children.items.len(),
                    size_of::<ArrayData>() + 2 * size_of::<ArrayRef>(),
                )?,
                checked_mul(2, size_of::<Buffer>())?,
            )?,
        )?)?;
        self.nodes += 1;
        let data_type = type_at(&self.schema, node).ok_or_else(invalid)?.clone();
        let child_data = children
            .items
            .into_iter()
            .map(|child| child.data)
            .collect::<Vec<_>>();
        let mut buffers = Vec::with_capacity(2);
        if let Some(first) = first {
            buffers.push(first.buffer);
        }
        if let Some(second) = second {
            buffers.push(second.buffer);
        }
        let data = ArrayData::builder(data_type)
            .len(len)
            .null_bit_buffer(validity.map(|buffer| buffer.buffer))
            .buffers(buffers)
            .child_data(child_data)
            .build()
            .map_err(|_| invalid())?;
        Ok(ConnectorRowMutationSourceArray {
            identity: Arc::clone(&self.identity),
            data,
            depth,
        })
    }

    pub fn finish(
        mut self,
        rows: usize,
        columns: ConnectorRowMutationSourceChildren,
    ) -> Result<ConnectorRowMutationSourceBatch, ConnectorError> {
        if rows > MAX_ROWS {
            return Err(exhausted());
        }
        if !Arc::ptr_eq(&self.identity, &columns.identity)
            || columns.items.len() != columns.limit
            || columns.items.len() != self.schema.fields().len()
            || columns
                .items
                .iter()
                .zip(self.schema.fields())
                .any(|(column, field)| {
                    column.data.len() != rows || column.data.data_type() != field.data_type()
                })
        {
            return Err(invalid());
        }
        self.reserve(checked_add(
            512,
            checked_mul(columns.items.len(), size_of::<ArrayRef>())?,
        )?)?;
        let arrays = columns
            .items
            .into_iter()
            .map(|column| make_array(column.data))
            .collect();
        let batch = RecordBatch::try_new_with_options(
            self.schema,
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .map_err(|_| invalid())?;
        Ok(ConnectorRowMutationSourceBatch {
            batch,
            source_bytes: self.charged,
            retention: self.retention,
        })
    }
}

fn retain_buffer(original: Buffer, guard: ConnectorPayloadRetentionGuard) -> Buffer {
    let pointer = NonNull::new(original.as_ptr() as *mut u8).unwrap();
    let len = original.len();
    let owner = Arc::new(RetainedBuffer {
        _original: original,
        _guard: guard,
    });
    // SAFETY: owner holds the immutable original allocation for every byte
    // of this view, including across Buffer clone/slice/to_data. The original
    // capacity stays in the source receipt: the custom Arrow view reports len,
    // which must never replace that receipt.
    unsafe { Buffer::from_custom_allocation(pointer, len, owner) }
}

// Field order releases the physical allocation before its capacity holder.
struct RetainedBuffer {
    _original: Buffer,
    _guard: ConnectorPayloadRetentionGuard,
}

fn no_byte_budget() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::InvalidRequest,
        "COW source construction requires its explicit byte-budget mode",
    )
}
fn exhausted() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "COW source construction exceeds its bounded ownership profile",
    )
}
fn invalid() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::InvalidRequest,
        "COW source parts do not belong to one checked schema and allocation owner",
    )
}
fn checked_add(a: usize, b: usize) -> Result<usize, ConnectorError> {
    a.checked_add(b)
        .filter(|value| *value <= MAX_CONNECTOR_ROW_MUTATION_SOURCE_BYTES)
        .ok_or_else(exhausted)
}
fn checked_mul(a: usize, b: usize) -> Result<usize, ConnectorError> {
    a.checked_mul(b)
        .filter(|value| *value <= MAX_CONNECTOR_ROW_MUTATION_SOURCE_BYTES)
        .ok_or_else(exhausted)
}

#[derive(Default)]
struct CopyPlan {
    nodes: usize,
    rows: usize,
    bytes: usize,
}
struct LogicalParts<'a> {
    validity: Option<&'a [u8]>,
    first: Option<&'a [u8]>,
    second: Option<&'a [u8]>,
}
fn logical_parts(data: &ArrayData) -> Result<LogicalParts<'_>, ConnectorError> {
    let dt = data.data_type();
    let n = data.len();
    let validity = data
        .nulls()
        .map(|nulls| {
            if nulls.offset() != 0 || nulls.len() != n || *dt == DataType::Null {
                return Err(invalid());
            }
            nulls
                .buffer()
                .as_slice()
                .get(..n.div_ceil(8))
                .ok_or_else(invalid)
        })
        .transpose()?;
    let first_len = match dt {
        DataType::Null | DataType::Struct(_) => None,
        DataType::Boolean => Some(n.div_ceil(8)),
        DataType::Utf8 | DataType::Binary | DataType::List(_) => {
            Some(checked_mul(n.checked_add(1).ok_or_else(exhausted)?, 4)?)
        }
        DataType::LargeUtf8 | DataType::LargeBinary | DataType::LargeList(_) => {
            Some(checked_mul(n.checked_add(1).ok_or_else(exhausted)?, 8)?)
        }
        DataType::Dictionary(_, _) => Some(checked_mul(n, 4)?),
        DataType::FixedSizeBinary(width) if *width >= 0 => Some(checked_mul(n, *width as usize)?),
        _ => Some(checked_mul(n, dt.primitive_width().ok_or_else(invalid)?)?),
    };
    let bytes = matches!(
        dt,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
    );
    let expected_buffers = usize::from(first_len.is_some()) + usize::from(bytes);
    if data.buffers().len() != expected_buffers {
        return Err(invalid());
    }
    let first = first_len
        .map(|n| data.buffers()[0].as_slice().get(..n).ok_or_else(invalid))
        .transpose()?;
    let offsets = matches!(
        dt,
        DataType::Utf8
            | DataType::Binary
            | DataType::List(_)
            | DataType::LargeUtf8
            | DataType::LargeBinary
            | DataType::LargeList(_)
    );
    let mut end = 0;
    if offsets {
        let values = first.ok_or_else(invalid)?;
        let width = if matches!(
            dt,
            DataType::LargeUtf8 | DataType::LargeBinary | DataType::LargeList(_)
        ) {
            8
        } else {
            4
        };
        let read = |index: usize| -> Result<usize, ConnectorError> {
            let at = index.checked_mul(width).ok_or_else(exhausted)?;
            let word = values.get(at..at + width).ok_or_else(invalid)?;
            if width == 4 {
                usize::try_from(i32::from_ne_bytes(word.try_into().unwrap())).map_err(|_| invalid())
            } else {
                usize::try_from(i64::from_ne_bytes(word.try_into().unwrap())).map_err(|_| invalid())
            }
        };
        if read(0)? != 0 {
            return Err(invalid());
        }
        end = read(n)?;
        let mut previous = 0;
        for index in 1..=n {
            let current = read(index)?;
            if current < previous || current > end {
                return Err(invalid());
            }
            previous = current;
        }
        if !bytes
            && data
                .child_data()
                .first()
                .is_none_or(|child| end > child.len())
        {
            return Err(invalid());
        }
    }
    let second = if bytes {
        Some(
            data.buffers()[1]
                .as_slice()
                .get(..end)
                .ok_or_else(invalid)?,
        )
    } else {
        None
    };
    if matches!(dt, DataType::Utf8 | DataType::LargeUtf8) {
        let text = std::str::from_utf8(second.ok_or_else(invalid)?).map_err(|_| invalid())?;
        let offsets = first.ok_or_else(invalid)?;
        let width = if *dt == DataType::LargeUtf8 { 8 } else { 4 };
        for word in offsets.chunks_exact(width) {
            let at = if width == 4 {
                i32::from_ne_bytes(word.try_into().unwrap()) as usize
            } else {
                i64::from_ne_bytes(word.try_into().unwrap()) as usize
            };
            if !text.is_char_boundary(at) {
                return Err(invalid());
            }
        }
    }
    Ok(LogicalParts {
        validity,
        first,
        second,
    })
}

#[derive(Default)]
struct SchemaShape {
    nodes: usize,
    names: usize,
}
impl SchemaShape {
    fn field(
        &mut self,
        field: &arrow::datatypes::Field,
        depth: usize,
    ) -> Result<(), ConnectorError> {
        if field.name().len() > MAX_NAME_BYTES {
            return Err(exhausted());
        }
        self.names = self
            .names
            .checked_add(field.name().len())
            .ok_or_else(exhausted)?;
        if self.names > MAX_SCHEMA_NAMES_BYTES {
            return Err(exhausted());
        }
        self.data_type(field.data_type(), depth)
    }
    fn data_type(&mut self, dt: &DataType, depth: usize) -> Result<(), ConnectorError> {
        if depth >= MAX_DEPTH || self.nodes == MAX_NODES {
            return Err(exhausted());
        }
        self.nodes += 1;
        use DataType as D;
        match dt {
            D::Null
            | D::Boolean
            | D::Int8
            | D::Int16
            | D::Int32
            | D::Int64
            | D::UInt8
            | D::UInt16
            | D::UInt32
            | D::UInt64
            | D::Float32
            | D::Float64
            | D::Date32
            | D::Date64
            | D::Decimal128(_, _)
            | D::Decimal256(_, _)
            | D::Utf8
            | D::LargeUtf8
            | D::Binary
            | D::LargeBinary => {}
            D::Time32(TimeUnit::Second | TimeUnit::Millisecond)
            | D::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond) => {}
            D::Timestamp(_, zone) => {
                if zone
                    .as_ref()
                    .is_some_and(|zone| zone.len() > MAX_NAME_BYTES)
                {
                    return Err(exhausted());
                }
            }
            D::FixedSizeBinary(width) if *width >= 0 => {}
            D::List(field) | D::LargeList(field) => self.field(field, depth + 1)?,
            D::Struct(fields) => {
                for field in fields {
                    self.field(field, depth + 1)?;
                }
            }
            D::Dictionary(key, value)
                if **key == D::Int32 && matches!(**value, D::Utf8 | D::LargeUtf8) =>
            {
                self.data_type(value, depth + 1)?
            }
            _ => return Err(invalid()),
        }
        Ok(())
    }
}

fn type_at(schema: &arrow::datatypes::Schema, target: usize) -> Option<&DataType> {
    fn visit<'a>(dt: &'a DataType, remaining: &mut usize) -> Option<&'a DataType> {
        if *remaining == 0 {
            return Some(dt);
        }
        *remaining -= 1;
        match dt {
            DataType::List(field) | DataType::LargeList(field) => {
                visit(field.data_type(), remaining)
            }
            DataType::Struct(fields) => fields
                .iter()
                .find_map(|field| visit(field.data_type(), remaining)),
            DataType::Dictionary(_, value) => visit(value, remaining),
            _ => None,
        }
    }
    let mut remaining = target;
    schema
        .fields()
        .iter()
        .find_map(|field| visit(field.data_type(), &mut remaining))
}

fn validate_parts(
    dt: &DataType,
    len: usize,
    validity: &Option<ConnectorRowMutationSourceBuffer>,
    first: &Option<ConnectorRowMutationSourceBuffer>,
    second: &Option<ConnectorRowMutationSourceBuffer>,
    children: &[ConnectorRowMutationSourceArray],
) -> Result<(), ConnectorError> {
    if validity
        .as_ref()
        .is_some_and(|b| b.buffer.len() != len.div_ceil(8) || *dt == DataType::Null)
    {
        return Err(invalid());
    }
    let width = match dt {
        DataType::Int8 | DataType::UInt8 => Some(1),
        DataType::Int16 | DataType::UInt16 => Some(2),
        DataType::Int32
        | DataType::UInt32
        | DataType::Float32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Dictionary(_, _) => Some(4),
        DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _) => Some(8),
        DataType::Decimal128(_, _) => Some(16),
        DataType::Decimal256(_, _) => Some(32),
        DataType::FixedSizeBinary(width) => Some(*width as usize),
        _ => None,
    };
    let expected = if let Some(width) = width {
        Some(checked_mul(len, width)?)
    } else {
        match dt {
            DataType::Boolean => Some(len.div_ceil(8)),
            DataType::Utf8 | DataType::Binary | DataType::List(_) => {
                Some(checked_mul(len.checked_add(1).ok_or_else(exhausted)?, 4)?)
            }
            DataType::LargeUtf8 | DataType::LargeBinary | DataType::LargeList(_) => {
                Some(checked_mul(len.checked_add(1).ok_or_else(exhausted)?, 8)?)
            }
            _ => None,
        }
    };
    if first.as_ref().map(|b| b.buffer.len()) != expected {
        return Err(invalid());
    }
    let bytes = matches!(
        dt,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
    );
    if bytes != second.is_some() {
        return Err(invalid());
    }
    let valid_children = match dt {
        DataType::List(field) | DataType::LargeList(field) => {
            children.len() == 1 && children[0].data.data_type() == field.data_type()
        }
        DataType::Dictionary(_, value) => {
            children.len() == 1 && children[0].data.data_type() == value.as_ref()
        }
        DataType::Struct(fields) => {
            children.len() == fields.len()
                && children.iter().zip(fields).all(|(child, field)| {
                    child.data.len() == len && child.data.data_type() == field.data_type()
                })
        }
        _ => children.is_empty(),
    };
    if !valid_children {
        return Err(invalid());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray, Int32Array};
    use arrow::datatypes::{Field, Int32Type, Schema};
    use std::collections::HashMap;

    fn schema(dt: DataType) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", dt, true)]))
    }

    #[test]
    fn owned_source_nested_arrays_have_one_bounded_owner() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "list",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
            Field::new(
                "struct",
                DataType::Struct(
                    vec![
                        Field::new("null", DataType::Null, true),
                        Field::new("text", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ),
            Field::new(
                "dict",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
        ]));
        let mut owner = ConnectorRowMutationSourceBuilder::try_new(schema).unwrap();
        let empty = owner.children(0).unwrap();
        let values = owner.copy_buffer(&[1, 0, 0, 0, 2, 0, 0, 0]).unwrap();
        let integers = owner.array(1, 2, None, Some(values), None, empty).unwrap();
        let mut children = owner.children(1).unwrap();
        children.push(integers).unwrap();
        let offsets = owner.copy_buffer(&[0, 0, 0, 0, 2, 0, 0, 0]).unwrap();
        let list = owner
            .array(0, 1, None, Some(offsets), None, children)
            .unwrap();
        let empty = owner.children(0).unwrap();
        let null = owner.array(3, 1, None, None, None, empty).unwrap();
        let empty = owner.children(0).unwrap();
        let offsets = owner.copy_buffer(&[0, 0, 0, 0, 1, 0, 0, 0]).unwrap();
        let data = owner.copy_buffer(b"x").unwrap();
        let text = owner
            .array(4, 1, None, Some(offsets), Some(data), empty)
            .unwrap();
        let mut children = owner.children(2).unwrap();
        children.push(null).unwrap();
        children.push(text).unwrap();
        let structure = owner.array(2, 1, None, None, None, children).unwrap();
        let empty = owner.children(0).unwrap();
        let offsets = owner.copy_buffer(&[0, 0, 0, 0, 1, 0, 0, 0]).unwrap();
        let data = owner.copy_buffer(b"d").unwrap();
        let text = owner
            .array(6, 1, None, Some(offsets), Some(data), empty)
            .unwrap();
        let mut children = owner.children(1).unwrap();
        children.push(text).unwrap();
        let keys = owner.copy_buffer(&[0, 0, 0, 0]).unwrap();
        let dictionary = owner.array(5, 1, None, Some(keys), None, children).unwrap();
        let mut columns = owner.children(3).unwrap();
        columns.push(list).unwrap();
        columns.push(structure).unwrap();
        columns.push(dictionary).unwrap();
        let source = owner.finish(1, columns).unwrap();
        assert_eq!(
            source
                .batch()
                .column(0)
                .as_list::<i32>()
                .values()
                .as_primitive::<Int32Type>()
                .values()
                .as_ref(),
            &[1, 2]
        );
        assert_eq!(
            source
                .batch()
                .column(1)
                .as_struct()
                .column(1)
                .as_string::<i32>()
                .value(0),
            "x"
        );
        assert_eq!(
            source
                .batch()
                .column(2)
                .as_dictionary::<Int32Type>()
                .values()
                .as_string::<i32>()
                .value(0),
            "d"
        );
        assert!(source.source_bytes() >= source.batch().get_array_memory_size());
        assert!(source.source_bytes() <= MAX_CONNECTOR_ROW_MUTATION_SOURCE_BYTES);
    }

    #[test]
    fn owned_source_refuses_foreign_receipts_and_incomplete_containers() {
        let schema = schema(DataType::Int32);
        let mut first = ConnectorRowMutationSourceBuilder::try_new(Arc::clone(&schema)).unwrap();
        let mut second = ConnectorRowMutationSourceBuilder::try_new(schema).unwrap();
        let values = first.copy_buffer(&[1, 0, 0, 0]).unwrap();
        let empty = second.children(0).unwrap();
        let before = first.reserved_bytes();
        assert_eq!(
            first
                .array(0, 1, None, Some(values), None, empty)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
        assert_eq!(first.reserved_bytes(), before);
        let mut slots = first.children(1).unwrap();
        let values = second.copy_buffer(&[1, 0, 0, 0]).unwrap();
        let empty = second.children(0).unwrap();
        let array = second.array(0, 1, None, Some(values), None, empty).unwrap();
        assert_eq!(
            slots.push(array).unwrap_err().kind(),
            ConnectorErrorKind::InvalidRequest
        );
        assert_eq!(
            first.finish(1, slots).unwrap_err().kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }

    #[test]
    fn owned_source_budget_is_checked_before_copy_or_capacity_growth() {
        let mut owner = ConnectorRowMutationSourceBuilder::try_new(schema(DataType::Null)).unwrap();
        owner.charged = MAX_CONNECTOR_ROW_MUTATION_SOURCE_BYTES - 320;
        let buffer = owner.copy_buffer(&[0_u8; 1]).unwrap();
        assert_eq!(buffer.buffer.capacity(), 64);
        assert_eq!(
            owner.reserved_bytes(),
            MAX_CONNECTOR_ROW_MUTATION_SOURCE_BYTES
        );
        let before = owner.reserved_bytes();
        assert_eq!(
            owner.copy_buffer(&[1]).unwrap_err().kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        assert_eq!(
            owner.children(1).unwrap_err().kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        assert_eq!(owner.reserved_bytes(), before);
        assert_eq!(
            owner.children(MAX_NODES + 1).unwrap_err().kind(),
            ConnectorErrorKind::ResourceExhausted
        );
    }

    #[test]
    fn owned_source_copies_foreign_slices_and_owns_its_container_capacity() {
        let foreign = bytes::Bytes::from(vec![7_u8; 65536]).slice(..4);
        let mut owner =
            ConnectorRowMutationSourceBuilder::try_new(schema(DataType::Int32)).unwrap();
        let values = owner.copy_buffer(foreign.as_ref()).unwrap();
        assert_ne!(values.buffer.as_ptr(), foreign.as_ptr());
        assert_eq!(values.buffer.capacity(), 64);
        let empty = owner.children(0).unwrap();
        let column = owner.array(0, 1, None, Some(values), None, empty).unwrap();
        let mut columns = owner.children(1).unwrap();
        assert_eq!(columns.items.capacity(), 1);
        columns.push(column).unwrap();
        assert_eq!(columns.items.capacity(), 1);
        let source = owner.finish(1, columns).unwrap();
        let (_, columns, _) = source.batch.into_parts();
        // collect may reuse the larger, previously charged receipt slots.
        assert!(columns.capacity() * size_of::<ArrayRef>() <= source.source_bytes);
    }

    #[test]
    fn owned_source_refuses_bad_schema_rows_and_parts() {
        let mut metadata = HashMap::new();
        metadata.insert("large".to_owned(), "m".repeat(MAX_SCHEMA_BYTES));
        let large_schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("v", DataType::Null, true)],
            metadata,
        ));
        assert_eq!(
            ConnectorRowMutationSourceBuilder::try_new(large_schema)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        let unsupported_schema = schema(DataType::FixedSizeList(
            Arc::new(Field::new("v", DataType::Null, true)),
            1,
        ));
        assert_eq!(
            ConnectorRowMutationSourceBuilder::try_new(unsupported_schema)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
        let mut owner =
            ConnectorRowMutationSourceBuilder::try_new(schema(DataType::Int32)).unwrap();
        let empty = owner.children(0).unwrap();
        let before = owner.reserved_bytes();
        assert_eq!(
            owner
                .array(0, MAX_ROWS + 1, None, None, None, empty)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        assert_eq!(owner.reserved_bytes(), before);
        let empty = owner.children(0).unwrap();
        let values = owner.copy_buffer(&[0]).unwrap();
        let before = owner.reserved_bytes();
        assert_eq!(
            owner
                .array(0, 1, None, Some(values), None, empty)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
        assert_eq!(owner.reserved_bytes(), before);
    }

    #[test]
    fn owned_source_copy_data_trims_logical_bytes_and_rejects_offset_before_copy() {
        let foreign = bytes::Bytes::from(vec![7_u8; 65536]).slice(..4);
        let data = ArrayData::builder(DataType::Int32)
            .len(1)
            .add_buffer(Buffer::from(foreign.clone()))
            .build()
            .unwrap();
        let mut owner =
            ConnectorRowMutationSourceBuilder::try_new(schema(DataType::Int32)).unwrap();
        let copy = owner.copy_data(0, &data).unwrap();
        assert_ne!(copy.data.buffers()[0].as_ptr(), data.buffers()[0].as_ptr());
        assert_eq!(copy.data.buffers()[0].capacity(), 64);
        let mut columns = owner.children(1).unwrap();
        columns.push(copy).unwrap();
        let receipt = owner.finish(1, columns).unwrap();
        assert!(receipt.source_bytes() < 65536);

        let data = Int32Array::from(vec![1, 2, 3]).to_data().slice(1, 1);
        let mut owner =
            ConnectorRowMutationSourceBuilder::try_new(schema(DataType::Int32)).unwrap();
        let before = owner.reserved_bytes();
        assert_eq!(
            owner.copy_data(0, &data).unwrap_err().kind(),
            ConnectorErrorKind::InvalidRequest
        );
        assert_eq!(owner.reserved_bytes(), before);
    }

    #[test]
    fn owned_source_copy_data_preflights_the_entire_tree_and_large_container() {
        let dt = DataType::Struct(vec![Field::new("child", DataType::Null, true)].into());
        let mut children = Vec::with_capacity(16384);
        children.push(ArrayData::new_null(&DataType::Null, 1));
        let old_capacity = children.capacity() * size_of::<ArrayData>();
        let data = ArrayData::builder(dt.clone())
            .len(1)
            .child_data(children)
            .build()
            .unwrap();
        let mut owner = ConnectorRowMutationSourceBuilder::try_new(schema(dt)).unwrap();
        let copy = owner.copy_data(0, &data).unwrap();
        let mut columns = owner.children(1).unwrap();
        columns.push(copy).unwrap();
        let receipt = owner.finish(1, columns).unwrap();
        assert!(receipt.source_bytes() < old_capacity);

        let dt = DataType::Struct(
            vec![
                Field::new("a", DataType::Null, true),
                Field::new("b", DataType::Null, true),
            ]
            .into(),
        );
        let data = ArrayData::builder(dt.clone())
            .len(MAX_ROWS + 1)
            .child_data(vec![
                ArrayData::new_null(&DataType::Null, MAX_ROWS + 1),
                ArrayData::new_null(&DataType::Null, MAX_ROWS + 1),
            ])
            .build()
            .unwrap();
        let mut owner = ConnectorRowMutationSourceBuilder::try_new(schema(dt)).unwrap();
        let before = owner.reserved_bytes();
        assert_eq!(
            owner.copy_data(0, &data).unwrap_err().kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        assert_eq!(owner.reserved_bytes(), before);
    }
}

#[cfg(test)]
mod retention_tests {
    use super::*;
    use crate::connector::ConnectorRowMutationSelection;
    use arrow::array::{Array, Int32Array, NullArray};
    use arrow::datatypes::{Field, Schema};

    #[test]
    fn source_buffer_slice_retains_actual_holder_and_original_capacity_receipt() {
        let holder = Arc::new(());
        let weak = Arc::downgrade(&holder);
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let mut builder = ConnectorRowMutationSourceBuilder::try_new_with_retention(
            schema.clone(),
            ConnectorPayloadRetentionGuard::new(holder),
        )
        .unwrap();
        let before = builder.reserved_bytes();
        let array = builder
            .copy_data(0, &Int32Array::from(vec![7, 8, 9]).to_data())
            .unwrap();
        // The copied 12-byte values still own the rounded 64-byte allocation.
        assert!(builder.reserved_bytes() >= before + 64 + BUFFER_HEADER_BYTES);
        let mut columns = builder.children(1).unwrap();
        columns.push(array).unwrap();
        let source = builder.finish(3, columns).unwrap();
        let original_receipt = source.source_bytes();
        let alias = source.batch().column(0).to_data().buffers()[0].slice_with_length(4, 4);
        let selection =
            ConnectorRowMutationSelection::try_new_owned(schema, vec![source], 3, 65536).unwrap();
        assert_eq!(
            selection.source_ownership.as_ref().unwrap()[0],
            original_receipt
        );
        drop(selection);
        assert!(weak.upgrade().is_some());
        assert_eq!(alias.as_slice(), 8_i32.to_ne_bytes());
        drop(alias);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn null_and_empty_selection_clones_retain_schema_and_index_holder() {
        for empty in [false, true] {
            let holder = Arc::new(());
            let weak = Arc::downgrade(&holder);
            let guard = ConnectorPayloadRetentionGuard::new(holder);
            let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Null, true)]));
            let sources = if empty {
                vec![]
            } else {
                let mut builder = ConnectorRowMutationSourceBuilder::try_new_with_retention(
                    schema.clone(),
                    guard.clone(),
                )
                .unwrap();
                let array = builder.copy_data(0, &NullArray::new(1).to_data()).unwrap();
                let mut columns = builder.children(1).unwrap();
                columns.push(array).unwrap();
                vec![builder.finish(1, columns).unwrap()]
            };
            let selection = ConnectorRowMutationSelection::try_new_owned_with_guard(
                schema, sources, 1, 65536, guard,
            )
            .unwrap();
            let alias = selection.clone();
            drop(selection);
            assert!(weak.upgrade().is_some());
            alias.validate().unwrap();
            drop(alias);
            assert!(weak.upgrade().is_none());
        }
    }

    #[test]
    fn guarded_source_refuses_excess_before_copy_and_drops_payload_before_holder() {
        let schema = Arc::new(Schema::empty());
        let mut builder = ConnectorRowMutationSourceBuilder::try_new_with_retention(
            schema,
            ConnectorPayloadRetentionGuard::new(()),
        )
        .unwrap();
        let before = builder.reserved_bytes();
        let excess = vec![0; MAX_CONNECTOR_ROW_MUTATION_SOURCE_BYTES];
        assert_eq!(
            builder.copy_buffer(&excess).unwrap_err().kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        assert_eq!(builder.reserved_bytes(), before);

        #[derive(Debug)]
        struct DropProbe {
            data: Vec<u8>,
            events: Arc<std::sync::Mutex<Vec<&'static str>>>,
        }
        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.events.lock().unwrap().push("payload");
            }
        }
        struct Holder(Arc<std::sync::Mutex<Vec<&'static str>>>);
        impl Drop for Holder {
            fn drop(&mut self) {
                self.0.lock().unwrap().push("holder");
            }
        }
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let payload = Arc::new(DropProbe {
            data: vec![1, 2, 3],
            events: events.clone(),
        });
        let pointer = NonNull::new(payload.data.as_ptr() as *mut u8).unwrap();
        // SAFETY: this immutable owner retains all three bytes.
        let original = unsafe { Buffer::from_custom_allocation(pointer, 3, payload) };
        let alias = retain_buffer(
            original,
            ConnectorPayloadRetentionGuard::new(Holder(events.clone())),
        )
        .slice(1);
        assert!(events.lock().unwrap().is_empty());
        drop(alias);
        assert_eq!(*events.lock().unwrap(), ["payload", "holder"]);
    }
}

#[cfg(test)]
mod byte_budget_tests {
    use super::*;
    use crate::connector::ConnectorPayloadByteBudget;
    use arrow::datatypes::{Field, Schema};
    use std::sync::Mutex;

    #[derive(Default)]
    struct State {
        held: usize,
        requests: Vec<usize>,
    }
    struct OriginalBudget {
        limit: usize,
        state: Arc<Mutex<State>>,
    }
    struct OriginalReceipt {
        bytes: usize,
        state: Arc<Mutex<State>>,
    }
    impl Drop for OriginalReceipt {
        fn drop(&mut self) {
            let mut state = self.state.lock().unwrap();
            state.held = state.held.checked_sub(self.bytes).unwrap();
        }
    }
    impl ConnectorPayloadByteBudget for OriginalBudget {
        fn reserve_bytes(
            &self,
            bytes: usize,
        ) -> Result<ConnectorPayloadRetentionGuard, ConnectorError> {
            let mut state = self.state.lock().unwrap();
            state.requests.push(bytes);
            let next = state
                .held
                .checked_add(bytes)
                .filter(|n| *n <= self.limit)
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::ResourceExhausted,
                        "original Root payload byte budget is full",
                    )
                })?;
            state.held = next;
            drop(state);
            Ok(ConnectorPayloadRetentionGuard::from_shared(Arc::new(
                OriginalReceipt {
                    bytes,
                    state: Arc::clone(&self.state),
                },
            )))
        }
    }
    fn budget(limit: usize) -> Arc<OriginalBudget> {
        Arc::new(OriginalBudget {
            limit,
            state: Arc::new(Mutex::new(State::default())),
        })
    }
    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]))
    }
    fn governed(budget: &Arc<OriginalBudget>) -> ConnectorRowMutationSourceBuilder {
        ConnectorRowMutationSourceBuilder::try_new_with_guard(
            schema(),
            ConnectorPayloadRetentionGuard::from_byte_budget(Arc::clone(budget)),
        )
        .unwrap()
    }
    fn batch(owner: &mut ConnectorRowMutationSourceBuilder) -> ConnectorRowMutationSourceChildren {
        let values = owner.copy_buffer(&1_i32.to_ne_bytes()).unwrap();
        let empty = owner.children(0).unwrap();
        let column = owner.array(0, 1, None, Some(values), None, empty).unwrap();
        let mut columns = owner.children(1).unwrap();
        columns.push(column).unwrap();
        columns
    }

    #[test]
    fn governed_source_requires_a_byte_author_and_local_retention_is_explicit() {
        let plain = ConnectorPayloadRetentionGuard::new(());
        assert_eq!(
            plain.reserve_owned_bytes(1).unwrap_err().kind(),
            ConnectorErrorKind::InvalidRequest
        );
        assert_eq!(
            ConnectorRowMutationSourceBuilder::try_new_with_guard(schema(), plain.clone())
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
        ConnectorRowMutationSourceBuilder::try_new_with_retention(schema(), plain).unwrap();
        let original = budget(64);
        assert_eq!(
            ConnectorRowMutationSourceBuilder::try_new_with_retention(
                schema(),
                ConnectorPayloadRetentionGuard::from_byte_budget(original),
            )
            .unwrap_err()
            .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }

    #[test]
    fn budget_capability_clone_does_not_charge_and_receipt_clone_releases_once() {
        let original = budget(64);
        let author = ConnectorPayloadRetentionGuard::from_byte_budget(Arc::clone(&original));
        let copy = author.try_clone().unwrap();
        assert!(original.state.lock().unwrap().requests.is_empty());
        let receipt = copy.reserve_owned_bytes(64).unwrap();
        let receipt_copy = receipt.try_clone().unwrap();
        assert_eq!(original.state.lock().unwrap().held, 64);
        drop((author, copy, receipt));
        assert_eq!(original.state.lock().unwrap().held, 64);
        drop(receipt_copy);
        assert_eq!(original.state.lock().unwrap().held, 0);
    }

    #[test]
    fn original_byte_refusal_precedes_copy_and_preserves_the_local_profile() {
        let original = budget(63);
        let mut owner = governed(&original);
        let before = owner.reserved_bytes();
        let error = owner.copy_buffer(&[1]).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
        assert_eq!(error.message(), "original Root payload byte budget is full");
        assert_eq!(owner.reserved_bytes(), before);
        let state = original.state.lock().unwrap();
        assert_eq!(state.requests, [64]);
        assert_eq!(state.held, 0);
    }

    #[test]
    fn exact_buffer_receipt_survives_finish_raw_array_and_last_buffer_slice() {
        let original = budget(64);
        let mut owner = governed(&original);
        let columns = batch(&mut owner);
        let source = owner.finish(1, columns).unwrap();
        let raw_array = Arc::clone(source.batch().column(0));
        let raw_data = raw_array.to_data();
        let last_slice = raw_data.buffers()[0].slice(1);
        assert_eq!(original.state.lock().unwrap().requests, [64]);
        // The original local profile includes schema/header allowances. These
        // are not reinterpreted as the Root's retained payload-byte debit.
        assert!(source.source_bytes() > 64);
        drop(source);
        assert_eq!(original.state.lock().unwrap().held, 64);
        drop((raw_array, raw_data));
        assert_eq!(original.state.lock().unwrap().held, 64);
        drop(last_slice);
        assert_eq!(original.state.lock().unwrap().held, 0);
    }

    #[test]
    fn independent_buffer_alias_does_not_pin_other_exited_payload_allocations() {
        let original = budget(128);
        let mut owner = governed(&original);
        let first = owner.copy_buffer(&[1]).unwrap();
        let second = owner.copy_buffer(&[2]).unwrap();
        let first_alias = first.buffer.clone();
        drop((first, second, owner));
        assert_eq!(original.state.lock().unwrap().held, 64);
        drop(first_alias);
        assert_eq!(original.state.lock().unwrap().held, 0);
    }

    #[test]
    fn local_profile_refusal_does_not_request_original_bytes() {
        let original = budget(64);
        let mut owner = governed(&original);
        owner.charged = MAX_CONNECTOR_ROW_MUTATION_SOURCE_BYTES;
        assert_eq!(
            owner.copy_buffer(&[1]).unwrap_err().kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        assert!(original.state.lock().unwrap().requests.is_empty());
    }
    #[test]
    fn one_source_cumulative_refusal_is_released_by_the_actual_buffer_exit() {
        let original = budget(64);
        let mut owner = governed(&original);
        let first = owner.copy_buffer(&[1]).unwrap();
        let before = owner.reserved_bytes();
        assert_eq!(
            owner.copy_buffer(&[2]).unwrap_err().kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        assert_eq!(owner.reserved_bytes(), before);
        assert_eq!(original.state.lock().unwrap().held, 64);
        drop(first);
        let second = owner.copy_buffer(&[2]).unwrap();
        assert_eq!(original.state.lock().unwrap().held, 64);
        drop((owner, second));
        assert_eq!(original.state.lock().unwrap().held, 0);
    }
}
