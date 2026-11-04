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

//! Shared borrowed Arrow RecordBatch geometry. Framing and semantic authority
//! remain with the caller; this module never decodes arrays or grants capacity.

use arrow_schema::{DataType, Field};
use novarocks_arrow_ipc_frame::{checked_range, nonnegative_length};
use novarocks_type_contract::CompileControlError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GeometryError {
    InvalidShape(&'static str),
    ResourceExhausted,
}
impl std::fmt::Display for GeometryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidShape(message) => f.write_str(message),
            Self::ResourceExhausted => f.write_str("Arrow IPC geometry resources exhausted"),
        }
    }
}
impl std::error::Error for GeometryError {}

/// The caller retains control and admission, including its original failure.
/// There are no default limits, wallets, or implicit permission in this trait.
pub trait GeometryWork {
    type Error: From<GeometryError> + From<CompileControlError>;
    fn has_admission(&self) -> bool;
    fn step(&mut self) -> Result<(), Self::Error>;
    fn flush(&mut self) -> Result<(), Self::Error>;
    /// `construction` is true only for the actual first allocation request.
    fn geometry_request(
        &mut self,
        layout: std::alloc::Layout,
        construction: bool,
    ) -> Result<(), Self::Error>;
    fn source_work(&mut self) -> Result<(), Self::Error>;
    /// The original type/field validator, with its exact caller provenance.
    fn validate_root(&mut self, field: &Field) -> Result<(), Self::Error>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GeometryProfile {
    Constant,
    Online,
}

fn shape(message: &'static str) -> GeometryError {
    GeometryError::InvalidShape(message)
}
fn add(a: usize, b: usize) -> Result<usize, GeometryError> {
    a.checked_add(b).ok_or(shape("IPC batch extent overflow"))
}
fn mul(a: usize, b: usize) -> Result<usize, GeometryError> {
    a.checked_mul(b).ok_or(shape("IPC batch extent overflow"))
}
fn length(n: i64) -> Result<usize, GeometryError> {
    nonnegative_length(n).map_err(|_| shape("negative or unrepresentable IPC batch extent"))
}
fn numeric_parent<T>(parent: bool, result: Result<T, GeometryError>) -> Result<T, GeometryError> {
    if parent {
        result.map_err(|_| GeometryError::ResourceExhausted)
    } else {
        result
    }
}
fn require<W: GeometryWork + ?Sized>(
    condition: bool,
    message: &'static str,
    work: &mut W,
) -> Result<(), W::Error> {
    work.step()?;
    if condition {
        Ok(())
    } else {
        Err(shape(message).into())
    }
}
fn limit<W: GeometryWork + ?Sized>(
    condition: bool,
    message: &'static str,
    work: &mut W,
) -> Result<(), W::Error> {
    if work.has_admission() && !condition {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    require(condition, message, work)
}

/// Explicit geometric admission, with no application defaults.
#[derive(Clone, Copy, Debug)]
pub struct FlatBatchProjectionLimits {
    pub max_metadata_bytes: usize,
    pub max_body_bytes: usize,
    pub max_rows: usize,
    pub max_buffer_descriptors: usize,
    /// Sum of every view's declared byte length, including NULL and repeated
    /// references. This is inspection work, not unique retained backing.
    pub max_view_validation_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlatBatchGeometry {
    pub rows: usize,
    pub null_count: usize,
    pub buffer_descriptors: usize,
    pub variadic_buffers: usize,
    pub body_bytes: usize,
    /// Sum of descriptor extents, counting overlap/repeated descriptions.
    /// Unreferenced body padding remains in body_bytes.
    pub described_buffer_bytes: usize,
    pub view_validation_bytes: usize,
}

pub enum Layout {
    Null,
    Bits,
    Fixed(usize),
    Offsets(usize),
    Views,
}

pub fn layout(ty: &DataType) -> Result<Layout, GeometryError> {
    match ty {
        DataType::Null => Ok(Layout::Null),
        DataType::Boolean => Ok(Layout::Bits),
        DataType::FixedSizeBinary(width) => usize::try_from(*width)
            .map(Layout::Fixed)
            .map_err(|_| GeometryError::InvalidShape("negative fixed binary width")),
        DataType::Utf8 | DataType::Binary => Ok(Layout::Offsets(4)),
        DataType::LargeUtf8 | DataType::LargeBinary => Ok(Layout::Offsets(8)),
        DataType::Utf8View | DataType::BinaryView => Ok(Layout::Views),
        _ => ty
            .primitive_width()
            .map(Layout::Fixed)
            .ok_or(GeometryError::InvalidShape(
                "constant RecordBatch is not a flat non-dictionary carrier",
            )),
    }
}

pub fn buffer<'a>(
    batch: arrow_ipc::RecordBatch<'_>,
    body: &'a [u8],
    index: usize,
) -> Result<&'a [u8], GeometryError> {
    let buffers = batch.buffers().ok_or(GeometryError::InvalidShape(
        "IPC batch buffer descriptors are missing",
    ))?;
    if index >= buffers.len() {
        return Err(GeometryError::InvalidShape(
            "IPC batch buffer index is invalid",
        ));
    }
    let descriptor = buffers.get(index);
    let range = checked_range(
        body.len(),
        length(descriptor.offset())?,
        length(descriptor.length())?,
    )
    .map_err(|_| GeometryError::InvalidShape("IPC batch buffer range exceeds body"))?;
    Ok(&body[range])
}

/// Inspect one already paired leaf occurrence in a larger recursive batch.
/// Shared layout and buffer rules remain the flat owner's sole author.
pub fn inspect_leaf<W: GeometryWork + ?Sized>(
    batch: arrow_ipc::RecordBatch<'_>,
    body: &[u8],
    layout: Layout,
    rows: usize,
    null_count: usize,
    buffer_start: usize,
    variadic_buffers: usize,
    limits: Option<FlatBatchProjectionLimits>,
    work: &mut W,
) -> Result<FlatBatchGeometry, W::Error> {
    let parent = work.has_admission();
    let count = match layout {
        Layout::Null => 0,
        Layout::Bits | Layout::Fixed(_) => 2,
        Layout::Offsets(_) => 3,
        Layout::Views => numeric_parent(parent, add(2, variadic_buffers))?,
    };
    let buffers = batch.buffers().ok_or(GeometryError::InvalidShape(
        "IPC batch buffer descriptors are missing",
    ))?;
    require(
        numeric_parent(parent, add(buffer_start, count))? <= buffers.len(),
        "IPC leaf descriptor range exceeds batch",
        work,
    )?;
    let leaf_buffer = |index| {
        buffer(
            batch,
            body,
            numeric_parent(parent, add(buffer_start, index))?,
        )
    };
    let mut described_buffer_bytes = 0;
    for index in 0..count {
        let bytes = leaf_buffer(index)?;
        described_buffer_bytes = numeric_parent(parent, add(described_buffer_bytes, bytes.len()))?;
        work.step()?;
    }
    let bitmap_bytes = numeric_parent(parent, add(rows / 8, usize::from(!rows.is_multiple_of(8))))?;
    if !matches!(layout, Layout::Null) && null_count != 0 {
        require(
            leaf_buffer(0)?.len() >= bitmap_bytes,
            "IPC batch validity bitmap is too short",
            work,
        )?;
    }
    let mut view_validation_bytes = 0;
    match layout {
        Layout::Null => require(
            null_count == rows,
            "IPC Null node must mark every row null",
            work,
        )?,
        Layout::Bits => require(
            leaf_buffer(1)?.len() >= bitmap_bytes,
            "IPC Boolean values are too short",
            work,
        )?,
        Layout::Fixed(width) => require(
            leaf_buffer(1)?.len() >= numeric_parent(parent, mul(rows, width))?,
            "IPC fixed-width values are too short",
            work,
        )?,
        Layout::Offsets(width) => {
            let offsets = leaf_buffer(1)?;
            // Arrow's typed_offsets converts the entire descriptor before
            // selecting N+1 entries. A partial trailing element would panic
            // in Buffer::typed_data even when the useful prefix is complete.
            require(
                offsets.len() % width == 0,
                "IPC byte offsets contain a partial element",
                work,
            )?;
            // Arrow accepts an empty offsets buffer for an empty array and
            // constructs its empty OffsetBuffer later. Do not reject it here.
            let minimum = if rows == 0 && offsets.is_empty() {
                0
            } else {
                numeric_parent(parent, mul(numeric_parent(parent, add(rows, 1))?, width))?
            };
            require(
                offsets.len() >= minimum,
                "IPC byte offsets are too short",
                work,
            )?;
        }
        Layout::Views => {
            let views = leaf_buffer(1)?;
            // The same whole-buffer typed conversion is used for u128 views.
            // Extra complete records are allowed; a partial one is unsafe.
            require(
                views.len() % 16 == 0,
                "IPC view records contain a partial element",
                work,
            )?;
            let extent = numeric_parent(parent, mul(rows, 16))?;
            require(
                views.len() >= extent,
                "IPC view records are too short",
                work,
            )?;
            for record in views[..extent].as_chunks::<16>().0 {
                let read = |start| {
                    u32::from_le_bytes([
                        record[start],
                        record[start + 1],
                        record[start + 2],
                        record[start + 3],
                    ])
                };
                let bytes = usize::try_from(read(0)).map_err(|_| {
                    GeometryError::InvalidShape("IPC view length is unrepresentable")
                })?;
                view_validation_bytes = numeric_parent(parent, add(view_validation_bytes, bytes))?;
                limit(
                    limits.is_none_or(|limits| {
                        view_validation_bytes <= limits.max_view_validation_bytes
                    }),
                    "IPC view validation byte envelope exceeded",
                    work,
                )?;
                if bytes > 12 {
                    let index = usize::try_from(read(8)).map_err(|_| {
                        GeometryError::InvalidShape("IPC view index is unrepresentable")
                    })?;
                    require(
                        index < variadic_buffers,
                        "IPC view references an invalid buffer",
                        work,
                    )?;
                    let payload = leaf_buffer(numeric_parent(parent, add(index, 2))?)?;
                    let start = usize::try_from(read(12)).map_err(|_| {
                        GeometryError::InvalidShape("IPC view offset is unrepresentable")
                    })?;
                    checked_range(payload.len(), start, bytes).map_err(|_| {
                        GeometryError::InvalidShape("IPC view range exceeds its buffer")
                    })?;
                    work.step()?;
                }
            }
        }
    }
    Ok(FlatBatchGeometry {
        rows,
        null_count,
        buffer_descriptors: count,
        variadic_buffers,
        body_bytes: body.len(),
        described_buffer_bytes,
        view_validation_bytes,
    })
}
/// One actual FieldNode occurrence in declaration DFS order. All extents are
/// checked against the original batch/body before publication.
#[derive(Clone, Copy, Debug)]
pub struct RecursiveNodeGeometry<'a> {
    pub field: &'a Field,
    pub rows: usize,
    pub null_count: usize,
    pub depth: usize,
    pub list_map_ancestors: usize,
    pub children: usize,
    pub subtree_nodes: usize,
    pub subtree_buffer_descriptors: usize,
    pub buffer_start: usize,
    pub buffer_count: usize,
    pub variadic_buffers: usize,
    pub described_buffer_bytes: usize,
    pub view_validation_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecursiveBatchGeometry {
    pub rows: usize,
    pub null_count: usize,
    pub field_nodes: usize,
    pub total_rows: usize,
    pub maximum_depth: usize,
    pub buffer_descriptors: usize,
    pub variadic_buffers: usize,
    pub view_fields: usize,
    pub body_bytes: usize,
    pub described_buffer_bytes: usize,
    pub view_validation_bytes: usize,
}

/// Geometry scratch has its own explicit request envelope. The preceding
/// framing/type stage and subsequent reader host retain their own admission.
#[derive(Clone, Copy, Debug)]
pub struct RecursiveBatchProjectionLimits {
    pub flat: FlatBatchProjectionLimits,
    pub max_field_nodes: usize,
    pub max_total_rows: usize,
    pub max_geometry_request_bytes: usize,
}

pub struct CheckedRecursiveBatch<'a> {
    pub nodes: Vec<RecursiveNodeGeometry<'a>>,
    pub geometry: RecursiveBatchGeometry,
    pub scratch_request_bytes: usize,
    pub scratch_request_count: usize,
}
pub fn preflight_record_batch<'a, W: GeometryWork + ?Sized>(
    batch: arrow_ipc::RecordBatch<'_>,
    body: &[u8],
    expected: &[&'a Field],
    limits: Option<RecursiveBatchProjectionLimits>,
    profile: GeometryProfile,
    work: &mut W,
) -> Result<CheckedRecursiveBatch<'a>, W::Error> {
    preflight_record_batch_with_profile(batch, body, expected, limits, profile, work, |_| Ok(()))
}

/// The frame owner validates its message profile at the original boundary:
/// known geometry admission and type validation precede the caller's ordinary
/// header checks. This preserves an already known resource refusal when a
/// cooperative controller has a pending quantum. The batch walker stays unique.
pub fn preflight_record_batch_with_profile<'a, W: GeometryWork + ?Sized>(
    batch: arrow_ipc::RecordBatch<'_>,
    body: &[u8],
    expected: &[&'a Field],
    limits: Option<RecursiveBatchProjectionLimits>,
    profile: GeometryProfile,
    work: &mut W,
    validate_message_profile: impl FnOnce(&mut W) -> Result<(), W::Error>,
) -> Result<CheckedRecursiveBatch<'a>, W::Error> {
    if work.has_admission()
        && let Some(raw) = batch.nodes()
    {
        let layout = std::alloc::Layout::array::<RecursiveNodeGeometry<'_>>(raw.len())
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        if limits.is_some_and(|limits| {
            raw.len() > limits.max_field_nodes || layout.size() > limits.max_geometry_request_bytes
        }) {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        work.geometry_request(layout, true)?;
    }
    limit(
        limits.is_none_or(|limits| body.len() <= limits.flat.max_body_bytes),
        "recursive IPC body envelope exceeded",
        work,
    )?;
    work.source_work()?;
    for field in expected {
        work.validate_root(field)?;
    }
    validate_message_profile(work)?;
    require(
        batch.compression().is_none(),
        "compressed recursive IPC batch is unsupported",
        work,
    )?;
    let rows = length(batch.length())?;
    limit(
        limits.is_none_or(|limits| rows <= limits.flat.max_rows),
        "recursive IPC root row envelope exceeded",
        work,
    )?;
    let raw_nodes = batch
        .nodes()
        .ok_or(shape("recursive IPC nodes are missing"))?;
    let buffers = batch
        .buffers()
        .ok_or(shape("recursive IPC buffers are missing"))?;
    require(
        (expected.is_empty() || !raw_nodes.is_empty())
            && limits.is_none_or(|limits| raw_nodes.len() <= limits.max_field_nodes),
        "recursive IPC node envelope exceeded",
        work,
    )?;
    limit(
        limits.is_none_or(|limits| buffers.len() <= limits.flat.max_buffer_descriptors),
        "recursive IPC buffer envelope exceeded",
        work,
    )?;
    let scratch =
        std::alloc::Layout::array::<RecursiveNodeGeometry<'_>>(raw_nodes.len()).map_err(|_| {
            if work.has_admission() {
                GeometryError::ResourceExhausted
            } else {
                shape("recursive IPC geometry layout is not representable")
            }
        })?;
    if work.has_admission() {
        if limits.is_some_and(|limits| scratch.size() > limits.max_geometry_request_bytes) {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        work.geometry_request(scratch, false)?;
    }
    limit(
        limits.is_none_or(|limits| scratch.size() <= limits.max_geometry_request_bytes),
        "recursive IPC geometry request envelope exceeded",
        work,
    )?;
    work.flush()?;
    let mut nodes = Vec::new();
    nodes
        .try_reserve_exact(raw_nodes.len())
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.flush()?;
    let mut walker = Walker {
        batch,
        body,
        limits,
        nodes,
        profile,
        node_cursor: 0,
        buffer_cursor: 0,
        view_cursor: 0,
        total_rows: 0,
        described: 0,
        view_bytes: 0,
        total_variadic: 0,
        max_depth: 0,
    };
    let mut null_count = 0usize;
    for field in expected {
        let root = walker.field(field, 1, 0, work)?;
        require(
            root.rows == rows,
            "recursive IPC root node length differs from batch",
            work,
        )?;
        null_count = numeric_parent(work.has_admission(), add(null_count, root.null_count))?;
    }
    require(
        walker.node_cursor == raw_nodes.len(),
        "recursive IPC batch has extra FieldNodes",
        work,
    )?;
    require(
        walker.buffer_cursor == buffers.len(),
        "recursive IPC batch has extra buffers",
        work,
    )?;
    require(
        batch.variadicBufferCounts().map_or(0, |c| c.len()) == walker.view_cursor,
        "recursive IPC batch has extra variadic counts",
        work,
    )?;
    let geometry = RecursiveBatchGeometry {
        rows,
        null_count,
        field_nodes: walker.node_cursor,
        total_rows: walker.total_rows,
        maximum_depth: walker.max_depth,
        buffer_descriptors: walker.buffer_cursor,
        variadic_buffers: walker.total_variadic,
        view_fields: walker.view_cursor,
        body_bytes: body.len(),
        described_buffer_bytes: walker.described,
        view_validation_bytes: walker.view_bytes,
    };
    Ok(CheckedRecursiveBatch {
        nodes: walker.nodes,
        geometry,
        scratch_request_bytes: scratch.size(),
        scratch_request_count: usize::from(scratch.size() != 0),
    })
}
struct Walker<'b, 'a> {
    batch: arrow_ipc::RecordBatch<'b>,
    body: &'b [u8],
    limits: Option<RecursiveBatchProjectionLimits>,
    profile: GeometryProfile,
    nodes: Vec<RecursiveNodeGeometry<'a>>,
    node_cursor: usize,
    buffer_cursor: usize,
    view_cursor: usize,
    total_rows: usize,
    described: usize,
    view_bytes: usize,
    total_variadic: usize,
    max_depth: usize,
}
impl<'b, 'a> Walker<'b, 'a> {
    fn field<W: GeometryWork + ?Sized>(
        &mut self,
        field: &'a Field,
        depth: usize,
        list_map_ancestors: usize,
        work: &mut W,
    ) -> Result<RecursiveNodeGeometry<'a>, W::Error> {
        let parent = work.has_admission();
        let raw = self
            .batch
            .nodes()
            .ok_or(shape("recursive IPC nodes are missing"))?;
        require(
            self.node_cursor < raw.len(),
            "recursive IPC FieldNode is missing",
            work,
        )?;
        let node = raw.get(self.node_cursor);
        let rows = length(node.length())?;
        let null_count = length(node.null_count())?;
        require(
            null_count <= rows,
            "recursive IPC node NULL count exceeds its rows",
            work,
        )?;
        self.total_rows = numeric_parent(parent, add(self.total_rows, rows))?;
        limit(
            self.limits
                .is_none_or(|limits| self.total_rows <= limits.max_total_rows),
            "recursive IPC total row envelope exceeded",
            work,
        )?;
        self.max_depth = self.max_depth.max(depth);
        let index = self.node_cursor;
        self.node_cursor = numeric_parent(parent, add(self.node_cursor, 1))?;
        let start = self.buffer_cursor;
        let mut item = RecursiveNodeGeometry {
            field,
            rows,
            null_count,
            depth,
            list_map_ancestors,
            children: 0,
            subtree_nodes: 0,
            subtree_buffer_descriptors: 0,
            buffer_start: start,
            buffer_count: 0,
            variadic_buffers: 0,
            described_buffer_bytes: 0,
            view_validation_bytes: 0,
        };
        // The checked node envelope reserves this exact DFS capacity once.
        self.nodes.push(item);
        work.step()?;
        match field.data_type() {
            DataType::Struct(fields) => {
                item.described_buffer_bytes = self.container_validity(rows, null_count, work)?;
                item.buffer_count = 1;
                item.children = fields.len();
                for child in fields {
                    let child = self.field(
                        child,
                        numeric_parent(parent, add(depth, 1))?,
                        list_map_ancestors,
                        work,
                    )?;
                    require(
                        child.rows == rows,
                        "recursive IPC Struct child length differs from parent",
                        work,
                    )?;
                }
            }
            DataType::List(child) | DataType::LargeList(child) | DataType::Map(child, _) => {
                item.described_buffer_bytes = self.container_validity(rows, null_count, work)?;
                item.buffer_count = 2;
                item.children = 1;
                let width = if matches!(field.data_type(), DataType::LargeList(_)) {
                    8
                } else {
                    4
                };
                let offsets = buffer(self.batch, self.body, self.buffer_cursor)?;
                self.described = numeric_parent(parent, add(self.described, offsets.len()))?;
                item.described_buffer_bytes =
                    numeric_parent(parent, add(item.described_buffer_bytes, offsets.len()))?;
                self.buffer_cursor = numeric_parent(parent, add(self.buffer_cursor, 1))?;
                require(
                    offsets.len() % width == 0,
                    "recursive IPC list offsets contain a partial element",
                    work,
                )?;
                let entries = if rows == 0 && offsets.is_empty() {
                    0
                } else {
                    numeric_parent(parent, add(rows, 1))?
                };
                require(
                    offsets.len() >= numeric_parent(parent, mul(entries, width))?,
                    "recursive IPC list offsets are too short",
                    work,
                )?;
                let mut previous = 0;
                for offset in offsets[..mul(entries, width)?].chunks_exact(width) {
                    let signed = if width == 4 {
                        i64::from(i32::from_le_bytes(
                            offset
                                .try_into()
                                .map_err(|_| shape("recursive IPC offset width mismatch"))?,
                        ))
                    } else {
                        i64::from_le_bytes(
                            offset
                                .try_into()
                                .map_err(|_| shape("recursive IPC offset width mismatch"))?,
                        )
                    };
                    let current = length(signed)?;
                    require(
                        current >= previous,
                        "recursive IPC list offsets are not monotone",
                        work,
                    )?;
                    previous = current;
                }
                let child = self.field(
                    child,
                    numeric_parent(parent, add(depth, 1))?,
                    numeric_parent(parent, add(list_map_ancestors, 1))?,
                    work,
                )?;
                require(
                    previous <= child.rows,
                    "recursive IPC list offsets exceed child extent",
                    work,
                )?;
            }
            DataType::Dictionary(key, _) if self.profile == GeometryProfile::Online => {
                let leaf = inspect_leaf(
                    self.batch,
                    self.body,
                    layout(key)?,
                    rows,
                    null_count,
                    start,
                    0,
                    self.limits.map(|limits| limits.flat),
                    work,
                )?;
                item.described_buffer_bytes = leaf.described_buffer_bytes;
                item.buffer_count = leaf.buffer_descriptors;
                self.buffer_cursor =
                    numeric_parent(parent, add(self.buffer_cursor, leaf.buffer_descriptors))?;
                self.described =
                    numeric_parent(parent, add(self.described, leaf.described_buffer_bytes))?;
            }
            DataType::FixedSizeList(child, width) if self.profile == GeometryProfile::Online => {
                item.described_buffer_bytes = self.container_validity(rows, null_count, work)?;
                item.buffer_count = 1;
                item.children = 1;
                let width =
                    usize::try_from(*width).map_err(|_| shape("negative fixed list width"))?;
                let child = self.field(
                    child,
                    numeric_parent(parent, add(depth, 1))?,
                    list_map_ancestors,
                    work,
                )?;
                require(
                    child.rows >= numeric_parent(parent, mul(rows, width))?,
                    "recursive IPC fixed list child extent is too short",
                    work,
                )?;
            }
            DataType::RunEndEncoded(ends, values) if self.profile == GeometryProfile::Online => {
                item.children = 2;
                let ends = self.field(
                    ends,
                    numeric_parent(parent, add(depth, 1))?,
                    list_map_ancestors,
                    work,
                )?;
                let values = self.field(
                    values,
                    numeric_parent(parent, add(depth, 1))?,
                    list_map_ancestors,
                    work,
                )?;
                require(
                    ends.rows == values.rows,
                    "recursive IPC run children have different extents",
                    work,
                )?;
            }
            DataType::Union(fields, mode) if self.profile == GeometryProfile::Online => {
                item.children = fields.len();
                item.buffer_count = if *mode == arrow_schema::UnionMode::Dense {
                    2
                } else {
                    1
                };
                for ordinal in 0..item.buffer_count {
                    let bytes = buffer(self.batch, self.body, self.buffer_cursor)?;
                    let minimum =
                        numeric_parent(parent, mul(rows, if ordinal == 0 { 1 } else { 4 }))?;
                    require(
                        bytes.len() >= minimum,
                        "recursive IPC union buffer extent is too short",
                        work,
                    )?;
                    item.described_buffer_bytes =
                        numeric_parent(parent, add(item.described_buffer_bytes, bytes.len()))?;
                    self.described = numeric_parent(parent, add(self.described, bytes.len()))?;
                    self.buffer_cursor = numeric_parent(parent, add(self.buffer_cursor, 1))?;
                }
                for (_, child) in fields.iter() {
                    let child = self.field(
                        child,
                        numeric_parent(parent, add(depth, 1))?,
                        list_map_ancestors,
                        work,
                    )?;
                    if *mode == arrow_schema::UnionMode::Sparse {
                        require(
                            child.rows == rows,
                            "recursive IPC sparse union child length differs from parent",
                            work,
                        )?;
                    }
                }
            }
            DataType::ListView(child) | DataType::LargeListView(child)
                if self.profile == GeometryProfile::Online =>
            {
                item.described_buffer_bytes = self.container_validity(rows, null_count, work)?;
                item.buffer_count = 3;
                item.children = 1;
                let width = if matches!(field.data_type(), DataType::LargeListView(_)) {
                    8
                } else {
                    4
                };
                let offsets = buffer(self.batch, self.body, self.buffer_cursor)?;
                let sizes = buffer(
                    self.batch,
                    self.body,
                    numeric_parent(parent, add(self.buffer_cursor, 1))?,
                )?;
                for bytes in [offsets, sizes] {
                    require(
                        bytes.len() % width == 0
                            && bytes.len() >= numeric_parent(parent, mul(rows, width))?,
                        "recursive IPC list view extent is invalid",
                        work,
                    )?;
                    item.described_buffer_bytes =
                        numeric_parent(parent, add(item.described_buffer_bytes, bytes.len()))?;
                    self.described = numeric_parent(parent, add(self.described, bytes.len()))?;
                }
                self.buffer_cursor = numeric_parent(parent, add(self.buffer_cursor, 2))?;
                let child = self.field(
                    child,
                    numeric_parent(parent, add(depth, 1))?,
                    numeric_parent(parent, add(list_map_ancestors, 1))?,
                    work,
                )?;
                for (offset, size) in offsets
                    .chunks_exact(width)
                    .zip(sizes.chunks_exact(width))
                    .take(rows)
                {
                    let read = |bytes: &[u8]| -> Result<usize, GeometryError> {
                        let signed = if width == 4 {
                            i64::from(i32::from_le_bytes(
                                bytes
                                    .try_into()
                                    .map_err(|_| shape("list view offset width mismatch"))?,
                            ))
                        } else {
                            i64::from_le_bytes(
                                bytes
                                    .try_into()
                                    .map_err(|_| shape("list view offset width mismatch"))?,
                            )
                        };
                        length(signed)
                    };
                    require(
                        numeric_parent(parent, add(read(offset)?, read(size)?))? <= child.rows,
                        "recursive IPC list view range exceeds child extent",
                        work,
                    )?;
                }
            }
            DataType::Dictionary(..)
            | DataType::Union(..)
            | DataType::RunEndEncoded(..)
            | DataType::FixedSizeList(..)
            | DataType::ListView(..)
            | DataType::LargeListView(..) => {
                return Err(
                    shape("recursive IPC carrier is outside the frozen supported profile").into(),
                );
            }
            ty => {
                let layout = layout(ty)?;
                let variadic = if matches!(layout, Layout::Views) {
                    let counts = self
                        .batch
                        .variadicBufferCounts()
                        .ok_or(shape("recursive IPC view variadic count is missing"))?;
                    require(
                        self.view_cursor < counts.len(),
                        "recursive IPC view variadic count is missing",
                        work,
                    )?;
                    let count = counts.get(self.view_cursor);
                    numeric_parent(
                        parent,
                        count
                            .checked_add(2)
                            .ok_or(shape("recursive IPC view count overflow")),
                    )?;
                    self.view_cursor = numeric_parent(parent, add(self.view_cursor, 1))?;
                    length(count)?
                } else {
                    0
                };
                let leaf = inspect_leaf(
                    self.batch,
                    self.body,
                    layout,
                    rows,
                    null_count,
                    start,
                    variadic,
                    self.limits.map(|limits| limits.flat),
                    work,
                )?;
                item.described_buffer_bytes = leaf.described_buffer_bytes;
                item.view_validation_bytes = leaf.view_validation_bytes;
                item.buffer_count = leaf.buffer_descriptors;
                item.variadic_buffers = variadic;
                self.total_variadic = numeric_parent(parent, add(self.total_variadic, variadic))?;
                self.buffer_cursor =
                    numeric_parent(parent, add(self.buffer_cursor, leaf.buffer_descriptors))?;
                self.described =
                    numeric_parent(parent, add(self.described, leaf.described_buffer_bytes))?;
                self.view_bytes =
                    numeric_parent(parent, add(self.view_bytes, leaf.view_validation_bytes))?;
                limit(
                    self.limits.is_none_or(|limits| {
                        self.view_bytes <= limits.flat.max_view_validation_bytes
                    }),
                    "recursive IPC cumulative view byte envelope exceeded",
                    work,
                )?;
            }
        }
        item.subtree_nodes = self.node_cursor - index;
        item.subtree_buffer_descriptors = self.buffer_cursor - start;
        self.nodes[index] = item;
        work.step()?;
        Ok(item)
    }
    fn container_validity<W: GeometryWork + ?Sized>(
        &mut self,
        rows: usize,
        null_count: usize,
        work: &mut W,
    ) -> Result<usize, W::Error> {
        let parent = work.has_admission();
        let validity = buffer(self.batch, self.body, self.buffer_cursor)?;
        self.described = numeric_parent(parent, add(self.described, validity.len()))?;
        self.buffer_cursor = numeric_parent(parent, add(self.buffer_cursor, 1))?;
        if null_count != 0 {
            require(
                validity.len()
                    >= numeric_parent(parent, add(rows / 8, usize::from(!rows.is_multiple_of(8))))?,
                "recursive IPC container validity bitmap is too short",
                work,
            )?;
        }
        work.step()?;
        Ok(validity.len())
    }
}
