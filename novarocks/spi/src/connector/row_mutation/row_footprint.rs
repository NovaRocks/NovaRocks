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

//! Borrowed preflight for the admitted Arrow 60.1.0 row converter. This is a
//! structural bound, not a second memory account. Re-audit on Arrow upgrades.
//!
//! Arrow creates all child Rows before the parent Rows, and retains them while
//! allocating the parent's offsets, variable lengths and encoded buffer. Its
//! constructor additionally converts synthetic null arrays for dictionaries,
//! structs and unions. Both paths are covered before invoking Arrow.
//!
//! Buffer capacity is Arrow's public declared capacity. A foreign/custom Buffer
//! owner can retain more memory than that declaration (including a sliced Bytes
//! owner). Eligibility therefore requires a known bounded source, or a caller
//! proof charging that owner's backing through retained_source_bytes. This
//! helper does not admit arbitrary unknown backing or close the legacy ingress.
//! Private container capacities must also come from bounded constructors; any
//! unreported spare allocation belongs to that caller source proof.

use std::mem::size_of;

use arrow::array::{Array, ArrayRef, AsArray, UnionArray};
use arrow::datatypes::{DataType, Field, Schema, UnionMode};
use arrow::downcast_dictionary_array;
use arrow::record_batch::RecordBatch;

use super::{ConnectorError, ConnectorErrorKind};

pub const MAX_CONNECTOR_ROW_CONVERSION_WORKSPACE_BYTES: usize = 256 * 1024 * 1024;
const MAX_FIELDS: usize = 4096;
const MAX_DEPTH: usize = 64;
const MAX_ROWS: usize = 1_048_576;
// Engineering bound for borrowed traversal, including repeated nested references.
const MAX_VISITS: usize = MAX_CONNECTOR_ROW_CONVERSION_WORKSPACE_BYTES / size_of::<usize>();
// These cover private Codec/Encoder/Rows headers, temporary ArrayData/ArrayRef
// vectors, vector reallocation and Arrow's 64-byte buffer alignment. Payload
// copies and offsets are charged separately. None scales with payload width.
const NODE_HEADERS: usize = 2048;

#[derive(Clone, Copy, Debug)]
pub struct ConnectorRowConversionFootprint {
    pub schema_bytes: usize,
    pub converter_bytes: usize,
    pub constructor_peak_bytes: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct ConnectorRowConversionBatchFootprint {
    pub input_bytes: usize,
    pub converter_bytes: usize,
    pub encoded_rows_bytes: usize,
    /// Nested child Rows (including their local copy peaks), length vectors
    /// and private encoder/array headers.
    pub temporary_bytes: usize,
    /// Extra parent encoded-buffer capacity during allocation/copy.
    pub copy_peak_bytes: usize,
    pub peak_bytes: usize,
}

impl ConnectorRowConversionBatchFootprint {
    pub fn checked_peak_with(
        &self,
        extra_retained: usize,
        extra_transient: usize,
    ) -> Result<usize, ConnectorError> {
        add(add(self.peak_bytes, extra_retained)?, extra_transient)
    }
}

impl ConnectorRowConversionFootprint {
    pub fn checked_constructor_peak_with(
        &self,
        extra_retained: usize,
        extra_transient: usize,
    ) -> Result<usize, ConnectorError> {
        add(
            add(self.constructor_peak_bytes, extra_retained)?,
            extra_transient,
        )
    }

    /// Check before cloning SortFields or constructing a RowConverter.
    pub fn for_schema(schema: &Schema) -> Result<Self, ConnectorError> {
        let mut footprint = Self::for_fields(schema.fields().iter().map(|f| f.as_ref()))?;
        let metadata = metadata_bytes(schema.metadata())?;
        footprint.schema_bytes = add(footprint.schema_bytes, metadata)?;
        footprint.constructor_peak_bytes = add(footprint.constructor_peak_bytes, metadata)?;
        Ok(footprint)
    }

    /// Borrow fields directly when the caller has not materialized a Schema.
    pub fn for_fields<'a>(
        fields: impl ExactSizeIterator<Item = &'a Field>,
    ) -> Result<Self, ConnectorError> {
        if fields.len() > MAX_FIELDS {
            return Err(exhausted());
        }
        let mut walk = Walk::default();
        let mut schema_bytes = add(
            add(size_of::<Schema>(), 4 * size_of::<usize>())?,
            mul(fields.len(), size_of::<std::sync::Arc<Field>>())?,
        )?;
        let mut converter_bytes = NODE_HEADERS;
        let mut constructor_peak_bytes = NODE_HEADERS;
        for field in fields {
            let shape = walk.field(field, 0)?;
            schema_bytes = add(schema_bytes, shape.schema)?;
            converter_bytes = add(converter_bytes, shape.converter)?;
            constructor_peak_bytes = add(constructor_peak_bytes, shape.constructor)?;
        }
        constructor_peak_bytes = add(schema_bytes, constructor_peak_bytes)?;
        check(constructor_peak_bytes)?;
        Ok(Self {
            schema_bytes,
            converter_bytes,
            constructor_peak_bytes,
        })
    }

    /// Count each retained batch independently. Equal Schema values do not
    /// imply shared metadata, names or DataType allocations.
    pub fn retained_batch_bytes(batch: &RecordBatch) -> Result<usize, ConnectorError> {
        add(
            add(
                size_of::<RecordBatch>(),
                Self::retained_schema_bytes(batch.schema_ref())?,
            )?,
            Self::retained_columns_bytes(batch.columns())?,
        )
    }

    pub fn retained_schema_bytes(schema: &Schema) -> Result<usize, ConnectorError> {
        Self::for_schema(schema).map(|p| p.schema_bytes)
    }

    /// Array memory_size includes publicly declared buffer capacities and child
    /// Array headers, but not all DataType/Field backing. Charge the logical root types and every actual
    /// child Array's type separately, even where Arc aliases overlap.
    pub fn retained_columns_bytes(columns: &[ArrayRef]) -> Result<usize, ConnectorError> {
        if columns.len() > MAX_FIELDS {
            return Err(exhausted());
        }
        let mut walk = Walk::default();
        for array in columns {
            walk.data_type(array.data_type(), 0)?;
        }
        let mut bytes = mul(columns.len(), 2 * size_of::<ArrayRef>())?;
        for array in columns {
            let type_bytes = walk.array_type_bytes(array.as_ref(), 0)?;
            bytes = add(bytes, add(type_bytes, array.get_array_memory_size())?)?;
        }
        Ok(bytes)
    }

    /// `retained_source_bytes` includes other retained batches and any caller
    /// scratch that must coexist, including separately proved foreign/custom
    /// owner backing. Current columns' declared buffer capacities are also
    /// included, conservatively even if Arc aliases overlap.
    pub fn for_columns(
        &self,
        columns: &[ArrayRef],
        retained_source_bytes: usize,
    ) -> Result<ConnectorRowConversionBatchFootprint, ConnectorError> {
        if columns.len() > MAX_FIELDS || columns.first().is_some_and(|a| a.len() > MAX_ROWS) {
            return Err(exhausted());
        }
        self.columns_with_walk(columns, retained_source_bytes, &mut Walk::default())
    }

    pub(super) fn preflight_batches(
        &self,
        batches: &[RecordBatch],
        retained_source_bytes: usize,
    ) -> Result<(), ConnectorError> {
        let mut walk = Walk::default();
        for batch in batches {
            self.columns_with_walk(batch.columns(), retained_source_bytes, &mut walk)?;
        }
        Ok(())
    }

    fn columns_with_walk(
        &self,
        columns: &[ArrayRef],
        retained_source_bytes: usize,
        walk: &mut Walk,
    ) -> Result<ConnectorRowConversionBatchFootprint, ConnectorError> {
        let input_bytes = add(
            retained_source_bytes,
            Self::retained_columns_bytes(columns)?,
        )?;
        let base = add(add(input_bytes, self.schema_bytes)?, self.converter_bytes)?;
        check(base)?;
        let profile = walk.columns(columns, 0)?;
        let peak_bytes = add(base, profile.peak)?;
        check(peak_bytes)?;
        Ok(ConnectorRowConversionBatchFootprint {
            input_bytes,
            converter_bytes: self.converter_bytes,
            encoded_rows_bytes: profile.encoded,
            temporary_bytes: profile.temporary,
            copy_peak_bytes: profile.copy,
            peak_bytes,
        })
    }
}

#[derive(Default)]
struct Walk {
    nodes: usize,
    visits: usize,
}
#[derive(Default)]
struct Shape {
    schema: usize,
    converter: usize,
    constructor: usize,
}
#[derive(Default)]
struct Profile {
    encoded: usize,
    temporary: usize,
    copy: usize,
    peak: usize,
}

fn exhausted() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "row-mutation Arrow row conversion exceeds its bounded workspace",
    )
}
fn check(value: usize) -> Result<usize, ConnectorError> {
    if value > MAX_CONNECTOR_ROW_CONVERSION_WORKSPACE_BYTES {
        Err(exhausted())
    } else {
        Ok(value)
    }
}
fn add(a: usize, b: usize) -> Result<usize, ConnectorError> {
    check(a.checked_add(b).ok_or_else(exhausted)?)
}
fn mul(a: usize, b: usize) -> Result<usize, ConnectorError> {
    check(a.checked_mul(b).ok_or_else(exhausted)?)
}
fn metadata_bytes(metadata: &arrow::datatypes::Metadata) -> Result<usize, ConnectorError> {
    // Metadata is already key-sorted in Arrow 60, so canonical hashing
    // allocates no borrowed-key vector or sorting scratch. Its shared Arc
    // and BTree nodes remain live with their original String capacities.
    let mut bytes = check(
        novarocks_type_contract::owned_resources::arrow_metadata::retained_backing_upper_bound(
            metadata.len(),
        )
        .map_err(|_| exhausted())?,
    )?;
    for (key, value) in metadata {
        bytes = add(bytes, add(key.capacity(), value.capacity())?)?;
    }
    Ok(bytes)
}

impl Walk {
    fn visit(&mut self, depth: usize) -> Result<(), ConnectorError> {
        if depth >= MAX_DEPTH {
            return Err(exhausted());
        }
        self.visits = self.visits.checked_add(1).ok_or_else(exhausted)?;
        if self.visits > MAX_VISITS {
            return Err(exhausted());
        }
        Ok(())
    }
    fn array_type_bytes(
        &mut self,
        array: &dyn Array,
        depth: usize,
    ) -> Result<usize, ConnectorError> {
        self.visit(depth)?;
        // The logical type is independently bounded at each array. Visits stay
        // cumulative because repeated child type trees can otherwise hide work.
        self.nodes = 0;
        let mut bytes = self.data_type(array.data_type(), depth)?.schema;
        match array.data_type() {
            DataType::Dictionary(_, _) => {
                bytes = add(
                    bytes,
                    self.array_type_bytes(array.as_any_dictionary().values().as_ref(), depth + 1)?,
                )?
            }
            DataType::Struct(_) => {
                for child in array.as_struct().columns() {
                    bytes = add(bytes, self.array_type_bytes(child.as_ref(), depth + 1)?)?;
                }
            }
            DataType::List(_) => {
                bytes = add(
                    bytes,
                    self.array_type_bytes(array.as_list::<i32>().values().as_ref(), depth + 1)?,
                )?
            }
            DataType::LargeList(_) => {
                bytes = add(
                    bytes,
                    self.array_type_bytes(array.as_list::<i64>().values().as_ref(), depth + 1)?,
                )?
            }
            DataType::ListView(_) => {
                bytes = add(
                    bytes,
                    self.array_type_bytes(
                        array.as_list_view::<i32>().values().as_ref(),
                        depth + 1,
                    )?,
                )?
            }
            DataType::LargeListView(_) => {
                bytes = add(
                    bytes,
                    self.array_type_bytes(
                        array.as_list_view::<i64>().values().as_ref(),
                        depth + 1,
                    )?,
                )?
            }
            DataType::FixedSizeList(_, _) => {
                bytes = add(
                    bytes,
                    self.array_type_bytes(array.as_fixed_size_list().values().as_ref(), depth + 1)?,
                )?
            }
            DataType::RunEndEncoded(run, _) => {
                macro_rules! run_types {
                    ($ty:ty) => {{
                        let a = array.as_run::<$ty>();
                        // Run-end scalar buffers are already in memory_size; their
                        // primitive DataType has no heap backing.
                        self.array_type_bytes(a.values().as_ref(), depth + 1)?
                    }};
                }
                bytes = add(
                    bytes,
                    match run.data_type() {
                        DataType::Int16 => run_types!(arrow::datatypes::Int16Type),
                        DataType::Int32 => run_types!(arrow::datatypes::Int32Type),
                        DataType::Int64 => run_types!(arrow::datatypes::Int64Type),
                        _ => return Err(exhausted()),
                    },
                )?;
            }
            DataType::Union(fields, _) => {
                let union = array
                    .as_any()
                    .downcast_ref::<UnionArray>()
                    .ok_or_else(exhausted)?;
                for (id, _) in fields.iter() {
                    bytes = add(
                        bytes,
                        self.array_type_bytes(union.child(id).as_ref(), depth + 1)?,
                    )?;
                }
            }
            _ => {}
        }
        Ok(bytes)
    }

    fn field(&mut self, field: &Field, depth: usize) -> Result<Shape, ConnectorError> {
        let mut shape = self.data_type(field.data_type(), depth)?;
        shape.schema = add(
            shape.schema,
            add(
                add(
                    add(size_of::<Field>(), 2 * size_of::<usize>())?,
                    field.name().capacity(),
                )?,
                metadata_bytes(field.metadata())?,
            )?,
        )?;
        Ok(shape)
    }
    fn data_type(&mut self, dt: &DataType, depth: usize) -> Result<Shape, ConnectorError> {
        self.visit(depth)?;
        self.nodes = self.nodes.checked_add(1).ok_or_else(exhausted)?;
        if self.nodes > MAX_FIELDS {
            return Err(exhausted());
        }
        let mut result = Shape {
            schema: size_of::<DataType>(),
            converter: NODE_HEADERS,
            constructor: NODE_HEADERS,
        };
        let mut child = |shape: Shape| -> Result<(), ConnectorError> {
            result.schema = add(result.schema, shape.schema)?;
            result.converter = add(result.converter, shape.converter)?;
            result.constructor = add(result.constructor, shape.constructor)?;
            Ok(())
        };
        match dt {
            DataType::Map(_, _) => {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "row-mutation selection schema contains an unsupported Map value",
                ));
            }
            DataType::FixedSizeList(_, width) if *width < 0 => return Err(exhausted()),
            DataType::List(f)
            | DataType::LargeList(f)
            | DataType::ListView(f)
            | DataType::LargeListView(f)
            | DataType::FixedSizeList(f, _) => child(self.field(f, depth + 1)?)?,
            DataType::Struct(fields) => {
                child(Shape {
                    schema: add(
                        2 * size_of::<usize>(),
                        mul(fields.len(), size_of::<std::sync::Arc<Field>>())?,
                    )?,
                    ..Shape::default()
                })?;
                for f in fields {
                    child(self.field(f, depth + 1)?)?;
                }
            }
            DataType::Union(fields, _) => {
                child(Shape {
                    schema: add(
                        2 * size_of::<usize>(),
                        mul(fields.len(), size_of::<(i8, std::sync::Arc<Field>)>())?,
                    )?,
                    ..Shape::default()
                })?;
                if fields.is_empty() {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "row-mutation selection cannot canonicalize an empty Union",
                    ));
                }
                for (id, f) in fields.iter() {
                    if id < 0 {
                        return Err(ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            "row-mutation selection cannot canonicalize a negative Union type id",
                        ));
                    }
                    child(self.field(f, depth + 1)?)?;
                }
            }
            DataType::Dictionary(key, value) => {
                child(self.data_type(key, depth + 1)?)?;
                child(self.data_type(value, depth + 1)?)?;
            }
            DataType::RunEndEncoded(run, value) => {
                child(self.field(run, depth + 1)?)?;
                child(self.field(value, depth + 1)?)?;
            }
            DataType::Timestamp(_, Some(tz)) => {
                result.schema = add(result.schema, add(tz.len(), 2 * size_of::<usize>())?)?
            }
            DataType::FixedSizeBinary(width) if *width < 0 => return Err(exhausted()),
            _ => {}
        }
        // Each recursive converter clones its subtree's DataType and moves a
        // Vec<SortField> into an Arc slice. Charge both live copies.
        let clones = mul(result.schema, 2)?;
        result.converter = add(result.converter, clones)?;
        result.constructor = add(result.constructor, clones)?;
        match dt {
            DataType::Dictionary(_, value) => {
                let p = self.null_profile(value, 1, depth + 1)?;
                result.converter = add(result.converter, p.encoded)?;
                result.constructor = add(result.constructor, p.peak)?;
            }
            DataType::Struct(fields) => {
                for f in fields {
                    let p = self.null_profile(f.data_type(), 1, depth + 1)?;
                    result.converter = add(result.converter, p.encoded)?;
                    result.constructor = add(result.constructor, p.peak)?;
                }
            }
            DataType::Union(fields, _) => {
                for (_, f) in fields.iter() {
                    let p = self.null_profile(f.data_type(), 1, depth + 1)?;
                    result.converter = add(result.converter, p.encoded)?;
                    result.constructor = add(result.constructor, p.peak)?;
                }
            }
            _ => {}
        }
        result.constructor = add(result.constructor, result.converter)?;
        Ok(result)
    }
    fn null_profile(
        &mut self,
        dt: &DataType,
        n: usize,
        depth: usize,
    ) -> Result<Profile, ConnectorError> {
        self.visit(depth)?;
        let mut child_peak = 0;
        let mut array_bytes = add(
            NODE_HEADERS,
            add(
                mul(n.div_ceil(8), 64)?,
                mul(n, dt.primitive_width().unwrap_or(0))?,
            )?,
        )?;
        let mut include = |p: Profile| -> Result<(), ConnectorError> {
            child_peak = add(child_peak, p.peak)?;
            Ok(())
        };
        match dt {
            DataType::FixedSizeList(f, width) => {
                let width = usize::try_from(*width).map_err(|_| exhausted())?;
                include(self.null_profile(f.data_type(), mul(n, width)?, depth + 1)?)?;
            }
            DataType::List(f)
            | DataType::LargeList(f)
            | DataType::ListView(f)
            | DataType::LargeListView(f) => {
                array_bytes = add(array_bytes, mul(add(n, 1)?, 16)?)?;
                include(self.null_profile(f.data_type(), 0, depth + 1)?)?;
            }
            DataType::Struct(fields) => {
                for f in fields {
                    include(self.null_profile(f.data_type(), n, depth + 1)?)?;
                }
            }
            DataType::Dictionary(k, v) => {
                array_bytes = add(array_bytes, mul(n, k.primitive_width().unwrap_or(8))?)?;
                include(self.null_profile(v, 0, depth + 1)?)?;
            }
            DataType::Union(fields, mode) => {
                array_bytes = add(array_bytes, mul(n, 8)?)?;
                for (idx, (_, f)) in fields.iter().enumerate() {
                    include(self.null_profile(
                        f.data_type(),
                        if idx == 0 || *mode == UnionMode::Sparse {
                            n
                        } else {
                            0
                        },
                        depth + 1,
                    )?)?;
                }
            }
            DataType::RunEndEncoded(r, v) => {
                let max = match r.data_type() {
                    DataType::Int16 => i16::MAX as usize,
                    DataType::Int32 => i32::MAX as usize,
                    DataType::Int64 => usize::MAX,
                    _ => return Err(exhausted()),
                };
                if n > max {
                    return Err(exhausted());
                }
                include(self.null_profile(v.data_type(), usize::from(n != 0), depth + 1)?)?;
            }
            DataType::Binary | DataType::Utf8 | DataType::LargeBinary | DataType::LargeUtf8 => {
                array_bytes = add(array_bytes, mul(add(n, 1)?, 8)?)?
            }
            DataType::BinaryView | DataType::Utf8View => {
                array_bytes = add(array_bytes, mul(n, 16)?)?
            }
            DataType::FixedSizeBinary(width) => {
                array_bytes = add(
                    array_bytes,
                    mul(n, usize::try_from(*width).map_err(|_| exhausted())?)?,
                )?
            }
            _ => {}
        }
        let encoded = mul(n, self.null_len(dt, depth)?)?;
        let mut p = profile(n, 1, encoded, child_peak)?;
        p.peak = add(p.peak, mul(array_bytes, 2)?)?;
        Ok(p)
    }
    fn null_len(&mut self, dt: &DataType, depth: usize) -> Result<usize, ConnectorError> {
        self.visit(depth)?;
        if let Some(width) = dt.primitive_width() {
            return add(width, 1);
        }
        match dt {
            DataType::Null | DataType::Boolean => Ok(2),
            DataType::FixedSizeBinary(width) => {
                add(1, usize::try_from(*width).map_err(|_| exhausted())?)
            }
            DataType::Struct(fields) => fields
                .iter()
                .try_fold(1, |n, f| add(n, self.null_len(f.data_type(), depth + 1)?)),
            DataType::Dictionary(_, v) => self.null_len(v, depth + 1),
            DataType::Union(fields, _) => add(
                1,
                self.null_len(
                    fields.iter().next().ok_or_else(exhausted)?.1.data_type(),
                    depth + 1,
                )?,
            ),
            DataType::RunEndEncoded(_, v) => self.null_len(v.data_type(), depth + 1),
            _ => Ok(1),
        }
    }
    fn columns(&mut self, columns: &[ArrayRef], depth: usize) -> Result<Profile, ConnectorError> {
        let n = columns.first().map_or(0, |a| a.len());
        let mut encoded = 0;
        let mut children = 0;
        // Reject row-offset/length expansion before scanning values.
        profile(n, columns.len(), 0, 0)?;
        for array in columns {
            if array.len() != n {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "row-mutation row conversion columns have different lengths",
                ));
            }
            children = add(children, self.children(array.as_ref(), depth)?.peak)?;
            if let Some(width) = fixed_len(array.data_type())? {
                encoded = add(encoded, mul(n, width)?)?;
            } else {
                for index in 0..n {
                    encoded = add(encoded, self.row_len(array.as_ref(), index, depth)?)?;
                }
            }
        }
        profile(n, columns.len(), encoded, children)
    }
    fn single(&mut self, array: &ArrayRef, depth: usize) -> Result<Profile, ConnectorError> {
        self.columns(std::slice::from_ref(array), depth)
    }
    fn children(&mut self, array: &dyn Array, depth: usize) -> Result<Profile, ConnectorError> {
        self.visit(depth)?;
        match array.data_type() {
            DataType::Dictionary(_, _) => {
                self.single(array.as_any_dictionary().values(), depth + 1)
            }
            DataType::Struct(_) => self.columns(array.as_struct().columns(), depth + 1),
            DataType::List(_) => self.single(array.as_list::<i32>().values(), depth + 1),
            DataType::LargeList(_) => self.single(array.as_list::<i64>().values(), depth + 1),
            DataType::ListView(_) => self.single(array.as_list_view::<i32>().values(), depth + 1),
            DataType::LargeListView(_) => {
                self.single(array.as_list_view::<i64>().values(), depth + 1)
            }
            DataType::FixedSizeList(_, _) => {
                self.single(array.as_fixed_size_list().values(), depth + 1)
            }
            DataType::RunEndEncoded(r, _) => match r.data_type() {
                DataType::Int16 => self.single(
                    array.as_run::<arrow::datatypes::Int16Type>().values(),
                    depth + 1,
                ),
                DataType::Int32 => self.single(
                    array.as_run::<arrow::datatypes::Int32Type>().values(),
                    depth + 1,
                ),
                DataType::Int64 => self.single(
                    array.as_run::<arrow::datatypes::Int64Type>().values(),
                    depth + 1,
                ),
                _ => Err(exhausted()),
            },
            DataType::Union(fields, _) => {
                let union = array
                    .as_any()
                    .downcast_ref::<UnionArray>()
                    .ok_or_else(exhausted)?;
                let mut p = Profile::default();
                for (id, _) in fields.iter() {
                    p.peak = add(p.peak, self.single(union.child(id), depth + 1)?.peak)?;
                }
                Ok(p)
            }
            _ => Ok(Profile::default()),
        }
    }
    fn row_len(
        &mut self,
        array: &dyn Array,
        index: usize,
        depth: usize,
    ) -> Result<usize, ConnectorError> {
        self.visit(depth)?;
        if let Some(width) = fixed_len(array.data_type())? {
            return Ok(width);
        }
        match array.data_type() {
            DataType::Dictionary(_, v) => {
                if array.is_null(index) {
                    return self.null_len(v, depth + 1);
                }
                downcast_dictionary_array! {
                    array => self.row_len(array.values().as_ref(), array.key(index).ok_or_else(exhausted)?, depth + 1),
                    _ => Err(exhausted()),
                }
            }
            DataType::Struct(fields) => {
                if array.is_null(index) {
                    return fields
                        .iter()
                        .try_fold(1, |n, f| add(n, self.null_len(f.data_type(), depth + 1)?));
                }
                array.as_struct().columns().iter().try_fold(1, |n, a| {
                    add(n, self.row_len(a.as_ref(), index, depth + 1)?)
                })
            }
            DataType::List(_) => {
                let a = array.as_list::<i32>();
                let offsets = a.value_offsets();
                self.list_len(
                    array,
                    index,
                    a.values(),
                    offsets[index] as usize,
                    offsets[index + 1] as usize,
                    true,
                    depth,
                )
            }
            DataType::LargeList(_) => {
                let a = array.as_list::<i64>();
                let offsets = a.value_offsets();
                self.list_len(
                    array,
                    index,
                    a.values(),
                    offsets[index] as usize,
                    offsets[index + 1] as usize,
                    true,
                    depth,
                )
            }
            DataType::ListView(_) => {
                let a = array.as_list_view::<i32>();
                let start = a.value_offsets()[index] as usize;
                self.list_len(
                    array,
                    index,
                    a.values(),
                    start,
                    add(start, a.value_sizes()[index] as usize)?,
                    true,
                    depth,
                )
            }
            DataType::LargeListView(_) => {
                let a = array.as_list_view::<i64>();
                let start = a.value_offsets()[index] as usize;
                self.list_len(
                    array,
                    index,
                    a.values(),
                    start,
                    add(start, a.value_sizes()[index] as usize)?,
                    true,
                    depth,
                )
            }
            DataType::FixedSizeList(_, width) => {
                let a = array.as_fixed_size_list();
                let start = a.value_offset_at(index);
                self.list_len(
                    array,
                    index,
                    a.values(),
                    start,
                    add(start, *width as usize)?,
                    false,
                    depth,
                )
            }
            DataType::RunEndEncoded(r, _) => match r.data_type() {
                DataType::Int16 => {
                    let a = array.as_run::<arrow::datatypes::Int16Type>();
                    self.row_len(a.values().as_ref(), a.get_physical_index(index), depth + 1)
                }
                DataType::Int32 => {
                    let a = array.as_run::<arrow::datatypes::Int32Type>();
                    self.row_len(a.values().as_ref(), a.get_physical_index(index), depth + 1)
                }
                DataType::Int64 => {
                    let a = array.as_run::<arrow::datatypes::Int64Type>();
                    self.row_len(a.values().as_ref(), a.get_physical_index(index), depth + 1)
                }
                _ => Err(exhausted()),
            },
            DataType::Union(_, _) => {
                let a = array
                    .as_any()
                    .downcast_ref::<UnionArray>()
                    .ok_or_else(exhausted)?;
                add(
                    1,
                    self.row_len(
                        a.child(a.type_id(index)).as_ref(),
                        a.value_offset(index),
                        depth + 1,
                    )?,
                )
            }
            _ if array.is_null(index) => Ok(1),
            DataType::Binary => padded(array.as_binary::<i32>().value(index).len()),
            DataType::LargeBinary => padded(array.as_binary::<i64>().value(index).len()),
            DataType::Utf8 => padded(array.as_string::<i32>().value(index).len()),
            DataType::LargeUtf8 => padded(array.as_string::<i64>().value(index).len()),
            DataType::BinaryView => padded(array.as_binary_view().value(index).len()),
            DataType::Utf8View => padded(array.as_string_view().value(index).len()),
            _ => Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation selection values cannot be canonically sized",
            )),
        }
    }
    fn list_len(
        &mut self,
        parent: &dyn Array,
        index: usize,
        values: &ArrayRef,
        start: usize,
        end: usize,
        padded_children: bool,
        depth: usize,
    ) -> Result<usize, ConnectorError> {
        if parent.is_null(index) {
            return Ok(1);
        }
        if start > end || end > values.len() {
            return Err(exhausted());
        }
        // Even all-Null children consume visits: overlapping list-view references
        // must not hide an unbounded amount of borrowed preflight work.
        let mut length = 1;
        for i in start..end {
            let child = self.row_len(values.as_ref(), i, depth + 1)?;
            length = add(
                length,
                if padded_children {
                    padded(child)?
                } else {
                    child
                },
            )?;
        }
        Ok(length)
    }
}

fn fixed_len(dt: &DataType) -> Result<Option<usize>, ConnectorError> {
    if let Some(width) = dt.primitive_width() {
        return Ok(Some(add(width, 1)?));
    }
    Ok(match dt {
        DataType::Null | DataType::Boolean => Some(2),
        DataType::FixedSizeBinary(width) => {
            Some(add(1, usize::try_from(*width).map_err(|_| exhausted())?)?)
        }
        _ => None,
    })
}
fn padded(length: usize) -> Result<usize, ConnectorError> {
    if length <= 32 {
        add(1, mul(length.div_ceil(8), 9)?)
    } else {
        add(4, mul(length.div_ceil(32), 33)?)
    }
}
fn profile(
    rows: usize,
    columns: usize,
    encoded: usize,
    children_peak: usize,
) -> Result<Profile, ConnectorError> {
    let offsets = mul(add(rows, 1)?, size_of::<usize>())?;
    let lengths = mul(rows, 2 * size_of::<usize>())?;
    let headers = add(NODE_HEADERS, mul(columns, NODE_HEADERS)?)?;
    let retained = add(encoded.max(8), offsets)?;
    let temporary = add(add(lengths, headers)?, children_peak)?;
    let copy = encoded.max(8);
    let peak = add(add(retained, temporary)?, copy)?;
    Ok(Profile {
        encoded,
        temporary,
        copy,
        peak,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ListViewArray, NullArray};
    use arrow::buffer::ScalarBuffer;
    use std::sync::Arc;

    #[test]
    fn repeated_reference_traversal_is_refused_before_exceeding_visit_limit() {
        let array = ListViewArray::try_new(
            Arc::new(Field::new("item", DataType::Null, true)),
            ScalarBuffer::from(vec![0, 0]),
            ScalarBuffer::from(vec![8, 8]),
            Arc::new(NullArray::new(8)),
            None,
        )
        .unwrap();
        let mut walk = Walk {
            nodes: 0,
            visits: MAX_VISITS - 4,
        };
        let error = walk.row_len(&array, 0, 0).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
        assert_eq!(walk.visits, MAX_VISITS + 1);
    }
}
