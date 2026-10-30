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

//! Pinned Arrow60.1.0 RecordBatchDecoder public array/header construction.
//! This model reads only the already constructed, verified source schema and the
//! borrowed record metadata; it never authors execution semantics.
use super::reader_geometry::ReaderStructureFailure;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::Buffer;
use arrow_data::ArrayData;
use arrow_schema::{DataType, IntervalUnit, Schema, TimeUnit};
use novarocks_type_contract::owned_resources::{
    layout::{LayoutResourceError, arc_layout},
    profile::LOCKED_TOOLCHAIN,
    vec::original_resize_growth,
};
use std::alloc::Layout;

/// ONE pinned private Bytes/Arc structural upper from the original custody
/// author. Payload storage is excluded; this does not authorize bytes itself.
pub fn original_reader_buffer_header_upper_bound() -> Result<usize, LayoutResourceError> {
    crate::arrow_result_custody::original_buffer_header_upper_bound()
        .map_err(|_| LayoutResourceError::Arithmetic)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReaderArrayHeaderClass {
    Retained,
    Temporary,
}

pub struct ReaderArrayStructureObserver<'a, H> {
    pub request: &'a mut dyn FnMut(Layout, ReaderArrayHeaderClass) -> Result<(), H>,
    pub opaque: &'a mut dyn FnMut(usize, ReaderArrayHeaderClass) -> Result<(), H>,
    pub work: &'a mut dyn FnMut() -> Result<(), H>,
}
impl<H> ReaderArrayStructureObserver<'_, H> {
    fn step(&mut self) -> Result<(), ReaderStructureFailure<H>> {
        (self.work)().map_err(ReaderStructureFailure::Creator)
    }
    fn layout(
        &mut self,
        layout: Layout,
        class: ReaderArrayHeaderClass,
    ) -> Result<(), ReaderStructureFailure<H>> {
        (self.request)(layout, class).map_err(ReaderStructureFailure::Creator)
    }
    fn array<T>(
        &mut self,
        count: usize,
        class: ReaderArrayHeaderClass,
    ) -> Result<(), ReaderStructureFailure<H>> {
        if count == 0 {
            return Ok(());
        }
        self.layout(
            Layout::array::<T>(count).map_err(|_| LayoutResourceError::Arithmetic)?,
            class,
        )
    }
    fn shared<T>(
        &mut self,
        class: ReaderArrayHeaderClass,
    ) -> Result<(), ReaderStructureFailure<H>> {
        self.layout(arc_layout(Layout::new::<T>())?, class)
    }
    fn pushes<T>(
        &mut self,
        count: usize,
        final_class: ReaderArrayHeaderClass,
    ) -> Result<(), ReaderStructureFailure<H>> {
        let (mut len, mut capacity) = (0usize, 0usize);
        while len < count {
            let next = len.checked_add(1).ok_or(LayoutResourceError::Arithmetic)?;
            let facts = original_resize_growth::<T>(len, capacity, next)?;
            if let Some(layout) = facts.requested_backing {
                self.layout(
                    layout,
                    if facts.requested_capacity >= count {
                        final_class
                    } else {
                        ReaderArrayHeaderClass::Temporary
                    },
                )?;
            }
            len = next;
            capacity = facts.requested_capacity;
            self.step()?;
        }
        Ok(())
    }
    fn clone_type(
        &mut self,
        ty: &DataType,
        class: ReaderArrayHeaderClass,
    ) -> Result<(), ReaderStructureFailure<H>> {
        self.step()?;
        if let DataType::Dictionary(key, value) = ty {
            self.layout(Layout::new::<DataType>(), class)?;
            self.clone_type(key, class)?;
            self.layout(Layout::new::<DataType>(), class)?;
            self.clone_type(value, class)?;
        }
        // Every other DataType clone either moves scalar fields or shares its
        // existing Field/Fields/UnionFields/timezone Arc. No source label clone.
        Ok(())
    }
    fn union_slots(
        &mut self,
        fields: &arrow_schema::UnionFields,
    ) -> Result<usize, ReaderStructureFailure<H>> {
        let mut maximum = 0usize;
        for (id, _) in fields.iter() {
            self.step()?;
            maximum = maximum.max(usize::try_from(id).map_err(|_| {
                ReaderStructureFailure::SourceModel("IPC Union has negative type id")
            })?);
        }
        maximum
            .checked_add(1)
            .ok_or(LayoutResourceError::Arithmetic.into())
    }
    fn body(
        &mut self,
        ty: &DataType,
        class: ReaderArrayHeaderClass,
    ) -> Result<(), ReaderStructureFailure<H>> {
        macro_rules! shared {
            ($t:ty) => {
                self.shared::<$t>(class)
            };
        }
        match ty {
            DataType::Null => shared!(NullArray),
            DataType::Boolean => shared!(BooleanArray),
            DataType::Int8 => shared!(Int8Array),
            DataType::Int16 => shared!(Int16Array),
            DataType::Int32 => shared!(Int32Array),
            DataType::Int64 => shared!(Int64Array),
            DataType::UInt8 => shared!(UInt8Array),
            DataType::UInt16 => shared!(UInt16Array),
            DataType::UInt32 => shared!(UInt32Array),
            DataType::UInt64 => shared!(UInt64Array),
            DataType::Float16 => shared!(Float16Array),
            DataType::Float32 => shared!(Float32Array),
            DataType::Float64 => shared!(Float64Array),
            DataType::Utf8 => shared!(StringArray),
            DataType::LargeUtf8 => shared!(LargeStringArray),
            DataType::Binary => shared!(BinaryArray),
            DataType::LargeBinary => shared!(LargeBinaryArray),
            DataType::Utf8View => shared!(StringViewArray),
            DataType::BinaryView => shared!(BinaryViewArray),
            DataType::FixedSizeBinary(_) => shared!(FixedSizeBinaryArray),
            DataType::Date32 => shared!(Date32Array),
            DataType::Date64 => shared!(Date64Array),
            DataType::Time32(TimeUnit::Second) => shared!(Time32SecondArray),
            DataType::Time32(TimeUnit::Millisecond) => shared!(Time32MillisecondArray),
            DataType::Time64(TimeUnit::Microsecond) => shared!(Time64MicrosecondArray),
            DataType::Time64(TimeUnit::Nanosecond) => shared!(Time64NanosecondArray),
            DataType::Timestamp(TimeUnit::Second, _) => shared!(TimestampSecondArray),
            DataType::Timestamp(TimeUnit::Millisecond, _) => shared!(TimestampMillisecondArray),
            DataType::Timestamp(TimeUnit::Microsecond, _) => shared!(TimestampMicrosecondArray),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => shared!(TimestampNanosecondArray),
            DataType::Duration(TimeUnit::Second) => shared!(DurationSecondArray),
            DataType::Duration(TimeUnit::Millisecond) => shared!(DurationMillisecondArray),
            DataType::Duration(TimeUnit::Microsecond) => shared!(DurationMicrosecondArray),
            DataType::Duration(TimeUnit::Nanosecond) => shared!(DurationNanosecondArray),
            DataType::Interval(IntervalUnit::YearMonth) => shared!(IntervalYearMonthArray),
            DataType::Interval(IntervalUnit::DayTime) => shared!(IntervalDayTimeArray),
            DataType::Interval(IntervalUnit::MonthDayNano) => shared!(IntervalMonthDayNanoArray),
            DataType::Decimal32(..) => shared!(Decimal32Array),
            DataType::Decimal64(..) => shared!(Decimal64Array),
            DataType::Decimal128(..) => shared!(Decimal128Array),
            DataType::Decimal256(..) => shared!(Decimal256Array),
            DataType::List(_) => shared!(ListArray),
            DataType::LargeList(_) => shared!(LargeListArray),
            DataType::ListView(_) => shared!(ListViewArray),
            DataType::LargeListView(_) => shared!(LargeListViewArray),
            DataType::FixedSizeList(..) => shared!(FixedSizeListArray),
            DataType::Struct(_) => shared!(StructArray),
            DataType::Map(..) => shared!(MapArray),
            DataType::Union(..) => shared!(UnionArray),
            DataType::Dictionary(key, _) if key.as_ref() == &DataType::Int32 => {
                shared!(DictionaryArray<Int32Type>)
            }
            DataType::RunEndEncoded(ends, _) => match ends.data_type() {
                DataType::Int16 => shared!(RunArray<Int16Type>),
                DataType::Int32 => shared!(RunArray<Int32Type>),
                DataType::Int64 => shared!(RunArray<Int64Type>),
                _ => Err(ReaderStructureFailure::SourceModel(
                    "IPC RunArray end type is unsupported",
                )),
            },
            _ => Err(ReaderStructureFailure::SourceModel(
                "IPC array header source is unsupported",
            )),
        }
    }

    fn clone_data(
        &mut self,
        ty: &DataType,
        class: ReaderArrayHeaderClass,
        variadic: &mut impl Iterator<Item = i64>,
    ) -> Result<(), ReaderStructureFailure<H>> {
        // ArrayRef::into_data delegates to the original array's to_data.
        // Its type moves to the new make_array result; variable Buffer tables
        // are consumed/dropped unless the specific public array adopts them.
        self.clone_type(ty, class)?;
        match ty {
            DataType::Null => {}
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => {
                self.array::<Buffer>(2, ReaderArrayHeaderClass::Temporary)?
            }
            DataType::List(field) | DataType::LargeList(field) | DataType::Map(field, _) => {
                self.array::<Buffer>(1, ReaderArrayHeaderClass::Temporary)?;
                self.array::<ArrayData>(1, ReaderArrayHeaderClass::Temporary)?;
                self.clone_data(field.data_type(), class, variadic)?;
            }
            DataType::FixedSizeList(field, _) => {
                self.array::<ArrayData>(1, ReaderArrayHeaderClass::Temporary)?;
                self.clone_data(field.data_type(), class, variadic)?;
            }
            DataType::ListView(field) | DataType::LargeListView(field) => {
                self.array::<Buffer>(2, ReaderArrayHeaderClass::Temporary)?;
                self.array::<ArrayData>(1, ReaderArrayHeaderClass::Temporary)?;
                self.clone_data(field.data_type(), class, variadic)?;
            }
            DataType::Struct(fields) => {
                // to_data first clones StructArray's actual children Vec.
                self.array::<ArrayRef>(fields.len(), ReaderArrayHeaderClass::Temporary)?;
                if !LOCKED_TOOLCHAIN {
                    return Err(ReaderStructureFailure::SourceModel(
                        "IPC Struct in-place collection model drift",
                    ));
                }
                // StructArray::to_data collects a TrustedLen slice/map into
                // Vec<ArrayData>; StructArray::from(ArrayData) maps that owned
                // Vec into Vec<ArrayRef>. Pinned std may adopt/shrink it.
                let bytes = Layout::array::<ArrayData>(fields.len())
                    .map_err(|_| LayoutResourceError::Arithmetic)?
                    .size();
                let same_alignment =
                    std::mem::align_of::<ArrayData>() == std::mem::align_of::<ArrayRef>();
                let can_adopt = same_alignment
                    && std::mem::size_of::<ArrayData>() >= std::mem::size_of::<ArrayRef>();
                if can_adopt && bytes % std::mem::size_of::<ArrayRef>() == 0 {
                    self.array::<ArrayData>(fields.len(), class)?;
                } else {
                    self.array::<ArrayData>(fields.len(), ReaderArrayHeaderClass::Temporary)?;
                    let count = if can_adopt {
                        bytes / std::mem::size_of::<ArrayRef>()
                    } else {
                        fields.len()
                    };
                    self.array::<ArrayRef>(count, class)?;
                }
                for field in fields {
                    self.clone_data(field.data_type(), class, variadic)?;
                }
            }
            DataType::Dictionary(_, values) => {
                self.array::<Buffer>(1, ReaderArrayHeaderClass::Temporary)?;
                self.array::<ArrayData>(1, ReaderArrayHeaderClass::Temporary)?;
                self.clone_data(values, class, variadic)?;
            }
            DataType::Union(fields, mode) => {
                let slots = self.union_slots(fields)?;
                self.array::<Option<ArrayRef>>(slots, ReaderArrayHeaderClass::Temporary)?;
                self.array::<Buffer>(
                    if *mode == arrow_schema::UnionMode::Dense {
                        2
                    } else {
                        1
                    },
                    ReaderArrayHeaderClass::Temporary,
                )?;
                // UnionFields::iter exposes an opaque non-TrustedLen iterator;
                // its exact lower size hint enters the original minimum reserve.
                let minimum = original_resize_growth::<ArrayData>(0, 0, 1)?.requested_capacity;
                if !fields.is_empty() {
                    self.array::<ArrayData>(
                        fields.len().max(minimum),
                        ReaderArrayHeaderClass::Temporary,
                    )?;
                }
                for (_, field) in fields.iter() {
                    self.clone_data(field.data_type(), class, variadic)?;
                }
            }
            DataType::RunEndEncoded(ends, values) => {
                self.array::<ArrayData>(2, ReaderArrayHeaderClass::Temporary)?;
                self.clone_data(ends.data_type(), class, variadic)?;
                self.clone_data(values.data_type(), class, variadic)?;
            }
            DataType::Utf8View | DataType::BinaryView => {
                let count = usize::try_from(variadic.next().ok_or(
                    ReaderStructureFailure::SourceModel("IPC source variadic count missing"),
                )?)
                .map_err(|_| LayoutResourceError::Arithmetic)?;
                // ByteViewArray::to_data clones the shared buffer slice into a
                // Vec, then inserts its view buffer at zero using original grow.
                self.array::<Buffer>(count, ReaderArrayHeaderClass::Temporary)?;
                let growth = original_resize_growth::<Buffer>(
                    count,
                    count,
                    count
                        .checked_add(1)
                        .ok_or(LayoutResourceError::Arithmetic)?,
                )?;
                if let Some(layout) = growth.requested_backing {
                    self.layout(layout, ReaderArrayHeaderClass::Temporary)?;
                }
            }
            _ => self.array::<Buffer>(1, ReaderArrayHeaderClass::Temporary)?,
        }
        Ok(())
    }
    fn make_data(
        &mut self,
        ty: &DataType,
        class: ReaderArrayHeaderClass,
        variadic: &mut impl Iterator<Item = i64>,
    ) -> Result<(), ReaderStructureFailure<H>> {
        self.step()?;
        // These constructors consume the already modelled ArrayData tables;
        // they do not allocate another buffer payload or clone the DataType.
        match ty {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _)
            | DataType::ListView(field)
            | DataType::LargeListView(field) => {
                self.make_data(field.data_type(), class, variadic)?
            }
            DataType::Map(field, _) => {
                self.make_inline_struct(field.data_type(), class, variadic)?
            }
            DataType::Struct(fields) => {
                for field in fields {
                    self.make_data(field.data_type(), class, variadic)?;
                }
            }
            DataType::Dictionary(_, values) => self.make_data(values, class, variadic)?,
            DataType::Union(fields, _) => {
                let slots = self.union_slots(fields)?;
                self.array::<Option<ArrayRef>>(slots, class)?;
                for (_, field) in fields.iter() {
                    self.make_data(field.data_type(), class, variadic)?;
                }
            }
            DataType::RunEndEncoded(_, values) => {
                // RunArray consumes the run-end buffer into inline ScalarBuffer;
                // only its value child passes through make_array.
                self.make_data(values.data_type(), class, variadic)?;
            }
            DataType::Utf8View | DataType::BinaryView => {
                let count = usize::try_from(variadic.next().ok_or(
                    ReaderStructureFailure::SourceModel("IPC source variadic count missing"),
                )?)
                .map_err(|_| LayoutResourceError::Arithmetic)?;
                // From<ArrayData> consumes the view buffer, then collects the
                // remaining owning IntoIter directly into a real Arc<[Buffer]>.
                self.layout(
                    arc_layout(
                        Layout::array::<Buffer>(count)
                            .map_err(|_| LayoutResourceError::Arithmetic)?,
                    )?,
                    class,
                )?;
            }
            _ => {}
        }
        self.body(ty, class)
    }
    fn make_inline_struct(
        &mut self,
        ty: &DataType,
        class: ReaderArrayHeaderClass,
        variadic: &mut impl Iterator<Item = i64>,
    ) -> Result<(), ReaderStructureFailure<H>> {
        let DataType::Struct(fields) = ty else {
            return Err(ReaderStructureFailure::SourceModel(
                "IPC Map entries are not a Struct",
            ));
        };
        // MapArray::from(ArrayData) converts its child directly into inline
        // StructArray. Its children are make_array results, but entries has no
        // extra Arc allocation. clone_data already models the adopted table.
        for field in fields {
            self.make_data(field.data_type(), class, variadic)?;
        }
        Ok(())
    }
    fn private_headers(
        &mut self,
        count: usize,
        class: ReaderArrayHeaderClass,
    ) -> Result<(), ReaderStructureFailure<H>> {
        if count == 0 {
            return Ok(());
        }
        let bytes = original_reader_buffer_header_upper_bound()?
            .checked_mul(count)
            .ok_or(LayoutResourceError::Arithmetic)?;
        (self.opaque)(bytes, class).map_err(ReaderStructureFailure::Creator)
    }
    fn buffer_branches(
        &mut self,
        batch: arrow_ipc::RecordBatch<'_>,
        body: &[u8],
        class: ReaderArrayHeaderClass,
    ) -> Result<(), ReaderStructureFailure<H>> {
        let Some(buffers) = batch.buffers() else {
            return Ok(());
        };
        for buffer in buffers.iter() {
            self.step()?;
            let start =
                usize::try_from(buffer.offset()).map_err(|_| LayoutResourceError::Arithmetic)?;
            let len =
                usize::try_from(buffer.length()).map_err(|_| LayoutResourceError::Arithmetic)?;
            let end = start
                .checked_add(len)
                .ok_or(LayoutResourceError::Arithmetic)?;
            let source = body
                .get(start..end)
                .ok_or(ReaderStructureFailure::SourceModel(
                    "IPC buffer extent exceeds original message body",
                ))?;
            // align_buffers can replace at most one private Bytes Arc per
            // original buffer. Variable/validity buffers need no alignment;
            // using ALL buffers is a conservative operation envelope,
            // never a claim that each buffer actually copied or exact stock.
            // Empty legacy offsets can allocate OffsetBuffer::new_empty;
            // include that mutually exclusive constructor in the same bound.
            self.private_headers(1, class)?;
            if batch.compression().is_some() && !source.is_empty() {
                let prefix: [u8; 8] = source
                    .get(..8)
                    .ok_or(ReaderStructureFailure::SourceModel(
                        "IPC compressed buffer prefix missing",
                    ))?
                    .try_into()
                    .expect("checked extent");
                let length = i64::from_le_bytes(prefix);
                if length >= 0 {
                    // Same original decompression branch: empty Buffer or
                    // decompressed Vec adoption creates exactly one Bytes Arc.
                    // No decompressed payload bytes are invoiced here.
                    self.private_headers(1, class)?;
                }
            }
        }
        Ok(())
    }
    pub fn dictionary_delta(
        &mut self,
        schema: &Schema,
        id: i64,
    ) -> Result<(), ReaderStructureFailure<H>> {
        fn find(field: &arrow_schema::Field, id: i64) -> Option<&DataType> {
            #[allow(deprecated)]
            if field.dict_id() == Some(id)
                && let DataType::Dictionary(_, value) = field.data_type()
            {
                return Some(value);
            }
            match field.data_type() {
                DataType::Struct(fields) => fields.iter().find_map(|field| find(field, id)),
                DataType::Union(fields, _) => fields.iter().find_map(|(_, field)| find(field, id)),
                DataType::List(field)
                | DataType::LargeList(field)
                | DataType::ListView(field)
                | DataType::LargeListView(field)
                | DataType::FixedSizeList(field, _)
                | DataType::Map(field, _)
                | DataType::RunEndEncoded(_, field) => find(field, id),
                _ => None,
            }
        }
        self.step()?;
        let ty = schema
            .fields()
            .iter()
            .find_map(|field| find(field, id))
            .ok_or(ReaderStructureFailure::SourceModel(
                "IPC delta dictionary source missing",
            ))?;
        if !matches!(ty, DataType::Utf8 | DataType::LargeUtf8) {
            return Err(ReaderStructureFailure::SourceModel(
                "IPC delta values source is unsupported",
            ));
        }
        // The original concat_bytes uses GenericByteBuilder::finish, NOT
        // MutableArrayData: two adopted data headers and at most one null
        // header. Its payload Vecs are unchanged original backing.
        self.private_headers(3, ReaderArrayHeaderClass::Retained)?;
        self.pushes::<Buffer>(2, ReaderArrayHeaderClass::Temporary)?;
        self.body(ty, ReaderArrayHeaderClass::Temporary)
    }
    fn possible_vector<T>(&mut self, bound: usize) -> Result<(), ReaderStructureFailure<H>> {
        // Number of null-bearing children is unknown before the original
        // decoder. ONE original Vec growth envelope for at most this source
        // field count is an opaque operation bound, not actual Vec capacity.
        let (mut length, mut capacity, mut bytes) = (0usize, 0usize, 0usize);
        while length < bound {
            self.step()?;
            let next = length
                .checked_add(1)
                .ok_or(LayoutResourceError::Arithmetic)?;
            let growth = original_resize_growth::<T>(length, capacity, next)?;
            if let Some(layout) = growth.requested_backing {
                bytes = bytes
                    .checked_add(layout.size())
                    .ok_or(LayoutResourceError::Arithmetic)?;
            }
            length = next;
            capacity = growth.requested_capacity;
        }
        if bytes != 0 {
            (self.opaque)(bytes, ReaderArrayHeaderClass::Temporary)
                .map_err(ReaderStructureFailure::Creator)?;
        }
        Ok(())
    }
    fn logical_null_headers(&mut self, ty: &DataType) -> Result<(), ReaderStructureFailure<H>> {
        self.step()?;
        match ty {
            DataType::Null => self.private_headers(1, ReaderArrayHeaderClass::Temporary)?,
            DataType::Dictionary(_, values) => {
                // Installed dictionaries have scalar string values. The
                // original values logical_nulls is a cheap borrowed clone;
                // key/value combination can create one BooleanBuffer header.
                if !matches!(values.as_ref(), DataType::Utf8 | DataType::LargeUtf8) {
                    return Err(ReaderStructureFailure::SourceModel(
                        "IPC logical-null dictionary values unsupported",
                    ));
                }
                self.private_headers(1, ReaderArrayHeaderClass::Temporary)?;
            }
            DataType::RunEndEncoded(_, values) => {
                self.logical_null_headers(values.data_type())?;
                self.private_headers(1, ReaderArrayHeaderClass::Temporary)?;
            }
            DataType::Union(fields, mode) => {
                for (_, field) in fields.iter() {
                    self.logical_null_headers(field.data_type())?;
                }
                if fields.len() == 1 {
                    // Single sparse field just returns its original logical
                    // NullBuffer. Dense remapping uses original gather only.
                    if *mode == arrow_schema::UnionMode::Dense {
                        self.private_headers(3, ReaderArrayHeaderClass::Temporary)?;
                        self.array::<(i8, arrow_buffer::NullBuffer)>(
                            1,
                            ReaderArrayHeaderClass::Temporary,
                        )?;
                    }
                } else if !fields.is_empty() {
                    // Dense gather creates one-null, one-valid and result
                    // buffers. Sparse mask strategies need no more headers.
                    self.private_headers(3, ReaderArrayHeaderClass::Temporary)?;
                    self.possible_vector::<(i8, arrow_buffer::NullBuffer)>(fields.len())?;
                    if *mode == arrow_schema::UnionMode::Dense {
                        return Ok(());
                    }
                    // Sparse helper temporary bit-chunk tables and its optional
                    // without-null IDs table are actual public element types.
                    self.possible_vector::<(i8, arrow_buffer::bit_chunk_iterator::BitChunks<'_>)>(
                        fields.len(),
                    )?;
                    self.possible_vector::<(i8, arrow_buffer::bit_chunk_iterator::BitChunkIterator<'_>)>(fields.len())?;
                    self.possible_vector::<i8>(fields.len())?;
                    // Optional skip-fully-null strategy creates two unique-ID
                    // HashSets. Reuse the SAME original RawTable upper model.
                    let work = &mut *self.work;
                    let upper = novarocks_type_contract::owned_resources::hashmap::incremental_insert_allocation_upper_bound_observed::<i8, (), ReaderStructureFailure<H>>(
                        fields.len(), || work().map_err(ReaderStructureFailure::Creator),
                    )?;
                    let upper = upper
                        .checked_mul(2)
                        .ok_or(LayoutResourceError::Arithmetic)?;
                    (self.opaque)(upper, ReaderArrayHeaderClass::Temporary)
                        .map_err(ReaderStructureFailure::Creator)?;
                }
            }
            // Every other actual Array logical_nulls path shares its existing
            // physical NullBuffer, with no new heap header.
            _ => {}
        }
        Ok(())
    }
    fn validation_null_headers(&mut self, ty: &DataType) -> Result<(), ReaderStructureFailure<H>> {
        // ArrayDataBuilder::validate_data calls validate_nulls once for its
        // own root. It does not recurse child validate_nulls: child data has
        // already passed its separate original decoder constructor.
        if let DataType::FixedSizeList(field, _) = ty
            && !field.is_nullable()
        {
            self.private_headers(1, ReaderArrayHeaderClass::Temporary)?;
        }
        Ok(())
    }
    fn data_layout_table(&mut self, ty: &DataType) -> Result<(), ReaderStructureFailure<H>> {
        self.step()?;
        // ArrayData::align_buffers and validate call arrow_data::layout at
        // every real ArrayData child. The public BufferSpec Vec is temporary;
        // observing it neither parses values nor creates a replacement layout.
        let count = match ty {
            DataType::Null
            | DataType::Struct(_)
            | DataType::FixedSizeList(..)
            | DataType::RunEndEncoded(..) => 0,
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::ListView(_)
            | DataType::LargeListView(_) => 2,
            DataType::Union(_, mode) if *mode == arrow_schema::UnionMode::Dense => 2,
            _ => 1,
        };
        self.array::<arrow_data::BufferSpec>(count, ReaderArrayHeaderClass::Temporary)?;
        match ty {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _)
            | DataType::ListView(field)
            | DataType::LargeListView(field)
            | DataType::Map(field, _) => self.data_layout_table(field.data_type())?,
            DataType::Struct(fields) => {
                for field in fields {
                    self.data_layout_table(field.data_type())?;
                }
            }
            DataType::Union(fields, _) => {
                for (_, field) in fields.iter() {
                    self.data_layout_table(field.data_type())?;
                }
            }
            DataType::Dictionary(_, value) => self.data_layout_table(value)?,
            DataType::RunEndEncoded(ends, value) => {
                self.data_layout_table(ends.data_type())?;
                self.data_layout_table(value.data_type())?;
            }
            _ => {}
        }
        Ok(())
    }
    fn data_builder_tables(&mut self, ty: &DataType) -> Result<(), ReaderStructureFailure<H>> {
        // Real builder uses align_buffers(true), then validate_data. Child
        // validation invokes validate recursively via get_valid_child_data.
        self.data_layout_table(ty)?;
        self.data_layout_table(ty)?;
        self.validation_null_headers(ty)
    }
    fn read_node(
        &mut self,
        field: &arrow_schema::Field,
        class: ReaderArrayHeaderClass,
        variadic: &mut (impl Iterator<Item = i64> + Clone),
        dictionary_present: &mut impl FnMut(i64) -> Result<bool, H>,
    ) -> Result<(), ReaderStructureFailure<H>> {
        self.step()?;
        let ty = field.data_type();
        match ty {
            DataType::Struct(fields) => {
                for field in fields {
                    self.read_node(field, class, variadic, dictionary_present)?;
                }
                self.pushes::<ArrayRef>(fields.len(), class)?;
                for field in fields {
                    if !field.is_nullable() {
                        self.logical_null_headers(field.data_type())?;
                    }
                }
                return self.body(ty, class);
            }
            DataType::Union(fields, mode) => {
                self.array::<ArrayRef>(fields.len(), ReaderArrayHeaderClass::Temporary)?;
                let mut child_variadic = variadic.clone();
                for (_, field) in fields.iter() {
                    self.read_node(
                        field,
                        ReaderArrayHeaderClass::Temporary,
                        variadic,
                        dictionary_present,
                    )?;
                }
                let slots = self.union_slots(fields)?;
                self.array::<i32>(slots, ReaderArrayHeaderClass::Temporary)?;
                // try_new delegates new_unchecked: original add_buffer Vec,
                // owning children into_data collection, then Self::from(data).
                self.pushes::<Buffer>(
                    if *mode == arrow_schema::UnionMode::Dense {
                        2
                    } else {
                        1
                    },
                    ReaderArrayHeaderClass::Temporary,
                )?;
                self.array::<ArrayData>(fields.len(), ReaderArrayHeaderClass::Temporary)?;
                let mut make_variadic = child_variadic.clone();
                for (_, field) in fields.iter() {
                    self.clone_data(field.data_type(), class, &mut child_variadic)?;
                }
                self.array::<Option<ArrayRef>>(slots, class)?;
                for (_, field) in fields.iter() {
                    self.make_data(field.data_type(), class, &mut make_variadic)?;
                }
                return self.body(ty, class);
            }
            DataType::List(child)
            | DataType::LargeList(child)
            | DataType::Map(child, _)
            | DataType::FixedSizeList(child, _)
            | DataType::ListView(child)
            | DataType::LargeListView(child) => {
                let mut clone_variadic = variadic.clone();
                self.read_node(
                    child,
                    ReaderArrayHeaderClass::Temporary,
                    variadic,
                    dictionary_present,
                )?;
                let mut make_variadic = clone_variadic.clone();
                self.clone_data(child.data_type(), class, &mut clone_variadic)?;
                if matches!(ty, DataType::Map(..)) {
                    self.make_inline_struct(child.data_type(), class, &mut make_variadic)?;
                } else {
                    self.make_data(child.data_type(), class, &mut make_variadic)?;
                }
                if !matches!(ty, DataType::FixedSizeList(..)) {
                    self.pushes::<Buffer>(
                        if matches!(ty, DataType::ListView(_) | DataType::LargeListView(_)) {
                            2
                        } else {
                            1
                        },
                        ReaderArrayHeaderClass::Temporary,
                    )?;
                }
                self.pushes::<ArrayData>(1, ReaderArrayHeaderClass::Temporary)?;
            }
            DataType::Dictionary(_, values) => {
                #[allow(deprecated)]
                let id = field.dict_id().ok_or(ReaderStructureFailure::SourceModel(
                    "IPC dictionary occurrence id missing",
                ))?;
                if !dictionary_present(id).map_err(ReaderStructureFailure::Creator)? {
                    // Original omitted-dictionary case authors an empty values
                    // array. It creates public array/table structures before
                    // the same to_data/make_array route below. Buffer ownership
                    // headers are added by the separate primitive adoption model.
                    // ArrayData::new_empty byte values has two data Buffer
                    // headers that survive child reconstruction, and one
                    // zero-length null header dropped by build_unchecked.
                    self.private_headers(2, ReaderArrayHeaderClass::Retained)?;
                    self.private_headers(1, ReaderArrayHeaderClass::Temporary)?;
                    self.array::<Buffer>(2, ReaderArrayHeaderClass::Temporary)?;
                    self.body(values, ReaderArrayHeaderClass::Temporary)?;
                }
                self.clone_data(values, class, &mut std::iter::empty())?;
                self.make_data(values, class, &mut std::iter::empty())?;
                self.pushes::<Buffer>(1, ReaderArrayHeaderClass::Temporary)?;
                self.pushes::<ArrayData>(1, ReaderArrayHeaderClass::Temporary)?;
            }
            DataType::RunEndEncoded(ends, values) => {
                self.read_node(
                    ends,
                    ReaderArrayHeaderClass::Temporary,
                    variadic,
                    dictionary_present,
                )?;
                let mut clone_variadic = variadic.clone();
                self.read_node(
                    values,
                    ReaderArrayHeaderClass::Temporary,
                    variadic,
                    dictionary_present,
                )?;
                // The initial run-end PrimitiveArray is temporary. Its to_data
                // buffer table is consumed inline by RunArray::from, without
                // constructing another PrimitiveArray Arc.
                self.clone_data(ends.data_type(), class, &mut std::iter::empty())?;
                let mut make_variadic = clone_variadic.clone();
                self.clone_data(values.data_type(), class, &mut clone_variadic)?;
                self.make_data(values.data_type(), class, &mut make_variadic)?;
                self.pushes::<ArrayData>(2, ReaderArrayHeaderClass::Temporary)?;
            }
            DataType::Null => {}
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => {
                self.array::<Buffer>(2, ReaderArrayHeaderClass::Temporary)?
            }
            DataType::Utf8View | DataType::BinaryView => {
                let extra = usize::try_from(variadic.next().ok_or(
                    ReaderStructureFailure::SourceModel("IPC variadic count missing"),
                )?)
                .map_err(|_| LayoutResourceError::Arithmetic)?;
                let all = extra
                    .checked_add(2)
                    .ok_or(LayoutResourceError::Arithmetic)?;
                self.pushes::<Buffer>(all, ReaderArrayHeaderClass::Temporary)?;
                self.array::<Buffer>(
                    extra
                        .checked_add(1)
                        .ok_or(LayoutResourceError::Arithmetic)?,
                    ReaderArrayHeaderClass::Temporary,
                )?;
                self.layout(
                    arc_layout(
                        Layout::array::<Buffer>(extra)
                            .map_err(|_| LayoutResourceError::Arithmetic)?,
                    )?,
                    class,
                )?;
            }
            _ => self.pushes::<Buffer>(1, ReaderArrayHeaderClass::Temporary)?,
        }
        self.clone_type(ty, class)?;
        if !matches!(ty, DataType::Null) {
            self.data_builder_tables(ty)?;
        }
        self.body(ty, class)
    }
    fn field_lookup(
        &mut self,
        field: &arrow_schema::Field,
    ) -> Result<usize, ReaderStructureFailure<H>> {
        // Original Field::fields starts vec![self], recursively builds the
        // descendants, then appends them. fields_with_dict_id filters that
        // owning Vec in place without allocating a second reference table.
        self.array::<&arrow_schema::Field>(1, ReaderArrayHeaderClass::Temporary)?;
        let descendants = self.descendant_lookup(field.data_type())?;
        if descendants != 0 {
            let growth = original_resize_growth::<&arrow_schema::Field>(
                1,
                1,
                descendants
                    .checked_add(1)
                    .ok_or(LayoutResourceError::Arithmetic)?,
            )?;
            if let Some(layout) = growth.requested_backing {
                self.layout(layout, ReaderArrayHeaderClass::Temporary)?;
            }
        }
        self.step()?;
        descendants
            .checked_add(1)
            .ok_or(LayoutResourceError::Arithmetic.into())
    }
    fn flat_segment<T>(
        &mut self,
        length: &mut usize,
        capacity: &mut usize,
        count: usize,
    ) -> Result<(), ReaderStructureFailure<H>> {
        if count == 0 {
            return Ok(());
        }
        let target = length
            .checked_add(count)
            .ok_or(LayoutResourceError::Arithmetic)?;
        if *length == 0 {
            // FlatMap's first owning inner IntoIter exposes its remaining lower
            // size hint. SpecFromIter reserves the WHOLE first segment (min4),
            // rather than fabricating a sequence of tiny growth allocations.
            let minimum = original_resize_growth::<T>(0, 0, 1)?.requested_capacity;
            *capacity = count.max(minimum);
            self.array::<T>(*capacity, ReaderArrayHeaderClass::Temporary)?;
            *length = 1;
            self.step()?;
        }
        while *length < target {
            let next = length
                .checked_add(1)
                .ok_or(LayoutResourceError::Arithmetic)?;
            let growth = original_resize_growth::<T>(*length, *capacity, next)?;
            if let Some(layout) = growth.requested_backing {
                self.layout(layout, ReaderArrayHeaderClass::Temporary)?;
            }
            *length = next;
            *capacity = growth.requested_capacity;
            self.step()?;
        }
        Ok(())
    }
    fn descendant_lookup(&mut self, ty: &DataType) -> Result<usize, ReaderStructureFailure<H>> {
        let mut descendants = 0usize;
        let mut capacity = 0usize;
        match ty {
            DataType::Struct(fields) => {
                for field in fields {
                    let count = self.field_lookup(field)?;
                    self.flat_segment::<&arrow_schema::Field>(
                        &mut descendants,
                        &mut capacity,
                        count,
                    )?;
                }
            }
            DataType::Union(fields, _) => {
                for (_, field) in fields.iter() {
                    let count = self.field_lookup(field)?;
                    self.flat_segment::<&arrow_schema::Field>(
                        &mut descendants,
                        &mut capacity,
                        count,
                    )?;
                }
            }
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field)
            | DataType::FixedSizeList(field, _)
            | DataType::Map(field, _)
            | DataType::RunEndEncoded(_, field) => descendants = self.field_lookup(field)?,
            DataType::Dictionary(_, values) => descendants = self.descendant_lookup(values)?,
            _ => {}
        }
        Ok(descendants)
    }
    pub fn dictionary_batch(
        &mut self,
        schema: &Schema,
        id: i64,
        batch: arrow_ipc::RecordBatch<'_>,
        body: &[u8],
    ) -> Result<(), ReaderStructureFailure<H>> {
        self.buffer_branches(batch, body, ReaderArrayHeaderClass::Retained)?;
        let mut matches = 0usize;
        let mut capacity = 0usize;
        let mut value = None;
        fn find<'a>(
            field: &'a arrow_schema::Field,
            id: i64,
            count: &mut usize,
            value: &mut Option<&'a DataType>,
        ) -> Result<(), LayoutResourceError> {
            #[allow(deprecated)]
            if field.dict_id() == Some(id)
                && let DataType::Dictionary(_, values) = field.data_type()
            {
                *count = count
                    .checked_add(1)
                    .ok_or(LayoutResourceError::Arithmetic)?;
                if value.is_none() {
                    *value = Some(values.as_ref());
                }
            }
            match field.data_type() {
                DataType::Struct(fields) => {
                    for child in fields {
                        find(child, id, count, value)?;
                    }
                }
                DataType::Union(fields, _) => {
                    for (_, child) in fields.iter() {
                        find(child, id, count, value)?;
                    }
                }
                DataType::List(child)
                | DataType::LargeList(child)
                | DataType::ListView(child)
                | DataType::LargeListView(child)
                | DataType::FixedSizeList(child, _)
                | DataType::Map(child, _)
                | DataType::RunEndEncoded(_, child) => find(child, id, count, value)?,
                _ => {}
            }
            Ok(())
        }
        for field in schema.fields() {
            self.field_lookup(field)?;
            let mut count = 0usize;
            find(field, id, &mut count, &mut value)?;
            self.flat_segment::<&arrow_schema::Field>(&mut matches, &mut capacity, count)?;
        }
        let value = value.ok_or(ReaderStructureFailure::SourceModel(
            "IPC dictionary id has no source field",
        ))?;
        if !matches!(value, DataType::Utf8 | DataType::LargeUtf8) {
            return Err(ReaderStructureFailure::SourceModel(
                "IPC dictionary values source is unsupported",
            ));
        }
        // get_dictionary_values authors a genuine one-column temporary schema
        // with empty label and required source value DataType. No metadata or
        // execution semantic declaration is read/recreated here.
        self.array::<arrow_schema::Field>(1, ReaderArrayHeaderClass::Temporary)?;
        self.shared::<arrow_schema::Field>(ReaderArrayHeaderClass::Temporary)?;
        // Vec<Field> in-place collect shrinks to Vec<FieldRef> before Arc slice.
        self.array::<arrow_schema::FieldRef>(1, ReaderArrayHeaderClass::Temporary)?;
        self.layout(
            arc_layout(
                Layout::array::<arrow_schema::FieldRef>(1)
                    .map_err(|_| LayoutResourceError::Arithmetic)?,
            )?,
            ReaderArrayHeaderClass::Temporary,
        )?;
        self.shared::<Schema>(ReaderArrayHeaderClass::Temporary)?;
        self.array::<Buffer>(2, ReaderArrayHeaderClass::Temporary)?;
        self.data_builder_tables(value)?;
        self.body(value, ReaderArrayHeaderClass::Temporary)?;
        self.pushes::<ArrayRef>(1, ReaderArrayHeaderClass::Temporary)?;
        Ok(())
    }
    pub fn record_batch(
        &mut self,
        schema: &Schema,
        batch: arrow_ipc::RecordBatch<'_>,
        body: &[u8],
        root_class: ReaderArrayHeaderClass,
        mut dictionary_present: impl FnMut(i64) -> Result<bool, H>,
    ) -> Result<(), ReaderStructureFailure<H>> {
        self.buffer_branches(batch, body, root_class)?;
        let counts = batch.variadicBufferCounts();
        if let Some(counts) = counts
            && !counts.is_empty()
        {
            self.array::<i64>(counts.len().max(4), ReaderArrayHeaderClass::Temporary)?;
        }
        let mut variadic = counts.into_iter().flat_map(|counts| counts.iter());
        for field in schema.fields() {
            self.read_node(field, root_class, &mut variadic, &mut dictionary_present)?;
        }
        // Original root column Vec belongs to RecordBatch, which materializer
        // consumes and destroys. Nested array-owned tables use Retained above.
        self.pushes::<ArrayRef>(schema.fields().len(), ReaderArrayHeaderClass::Temporary)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_buffer::{OffsetBuffer, ScalarBuffer};
    use arrow_schema::{Field, UnionFields};
    use std::io::Cursor;
    use std::sync::Arc;
    fn sources() -> Vec<ArrayRef> {
        let entries = StructArray::try_new(
            vec![
                Field::new("key", DataType::Int32, false),
                Field::new("value", DataType::Utf8, true),
            ]
            .into(),
            vec![
                Arc::new(Int32Array::from(vec![7, 8])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
            ],
            None,
        )
        .unwrap();
        let map = MapArray::try_new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1, 2])),
            entries,
            None,
            false,
        )
        .unwrap();
        let run = RunArray::<Int32Type>::try_new(
            &Int32Array::from(vec![2, 4]),
            &StringArray::from(vec!["first", "second"]),
        )
        .unwrap();
        let fields = UnionFields::try_new(
            vec![4, 9],
            vec![
                Field::new("number", DataType::Int32, false),
                Field::new("text", DataType::Utf8, true),
            ],
        )
        .unwrap();
        let union = UnionArray::try_new(
            fields,
            ScalarBuffer::from(vec![4_i8, 9]),
            Some(ScalarBuffer::from(vec![0_i32, 0])),
            vec![
                Arc::new(Int32Array::from(vec![7])),
                Arc::new(StringArray::from(vec![Some("text")])),
            ],
        )
        .unwrap();
        let views = StringViewArray::from(vec!["an external reader view value", "small"]);
        let nested_views = ListArray::try_new(
            Arc::new(Field::new("item", DataType::Utf8View, true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1, 2])),
            Arc::new(views.clone()),
            None,
        )
        .unwrap();
        vec![
            Arc::new(map),
            Arc::new(run),
            Arc::new(union),
            Arc::new(views),
            Arc::new(nested_views),
        ]
    }
    fn write(batch: &RecordBatch) -> Vec<u8> {
        let mut bytes = Vec::new();
        let mut writer =
            arrow_ipc::writer::StreamWriter::try_new(&mut bytes, batch.schema().as_ref()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        bytes
    }
    fn record(bytes: &[u8], offset: usize) -> (arrow_ipc::RecordBatch<'_>, &[u8]) {
        // This fixture uses the actual standard continuation produced by the
        // original writer, and the original verified flatbuffer reader.
        assert_eq!(&bytes[offset..offset + 4], &[255; 4]);
        let len = u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().unwrap()) as usize;
        let message = arrow_ipc::root_as_message(&bytes[offset + 8..offset + 8 + len]).unwrap();
        let body_start = offset + 8 + len;
        (
            message.header_as_record_batch().unwrap(),
            &bytes[body_start..body_start + message.bodyLength() as usize],
        )
    }
    #[test]
    fn reader_array_headers_preserve_real_map_run_union_and_nested_view_roundtrip() {
        for array in sources() {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                array.data_type().clone(),
                true,
            )]));
            let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
            let bytes = write(&batch);
            let mut reader =
                arrow_ipc::reader::StreamReader::try_new(Cursor::new(bytes.as_slice()), None)
                    .unwrap();
            let (metadata, body) = record(&bytes, reader.get_ref().position() as usize);
            let mut retained = 0;
            let mut temporary = 0;
            ReaderArrayStructureObserver {
                request: &mut |layout, class| {
                    if layout.size() != 0 {
                        match class {
                            ReaderArrayHeaderClass::Retained => retained += 1,
                            ReaderArrayHeaderClass::Temporary => temporary += 1,
                        }
                    };
                    Ok::<_, ()>(())
                },
                opaque: &mut |_, _| Ok(()),
                work: &mut || Ok(()),
            }
            .record_batch(
                reader.schema().as_ref(),
                metadata,
                body,
                ReaderArrayHeaderClass::Retained,
                |_| Ok(false),
            )
            .unwrap();
            assert!(retained > 0);
            assert!(temporary > 0);
            let decoded = reader.next().unwrap().unwrap();
            assert_eq!(decoded.num_rows(), batch.num_rows());
            assert_eq!(decoded.column(0).data_type(), batch.column(0).data_type());
            assert_eq!(decoded.column(0).to_data(), batch.column(0).to_data());
        }
    }
    #[test]
    fn reader_array_first_creator_refusal_has_no_tail_work_or_second_request() {
        let array = sources().remove(0);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            array.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        let bytes = write(&batch);
        let mut reader =
            arrow_ipc::reader::StreamReader::try_new(Cursor::new(bytes.as_slice()), None).unwrap();
        let (metadata, body) = record(&bytes, reader.get_ref().position() as usize);
        let refused = std::cell::Cell::new(false);
        let requests = std::cell::Cell::new(0);
        let failure = ReaderArrayStructureObserver {
            request: &mut |_, _| {
                requests.set(requests.get() + 1);
                refused.set(true);
                Err("original creator")
            },
            opaque: &mut |_, _| {
                requests.set(requests.get() + 1);
                refused.set(true);
                Err("original creator")
            },
            work: &mut || {
                assert!(
                    !refused.get(),
                    "tail work must not replace first creator refusal"
                );
                Ok(())
            },
        }
        .record_batch(
            reader.schema().as_ref(),
            metadata,
            body,
            ReaderArrayHeaderClass::Retained,
            |_| Ok(false),
        )
        .unwrap_err();
        assert!(matches!(
            failure,
            ReaderStructureFailure::Creator("original creator")
        ));
        assert_eq!(requests.get(), 1);
        // The model refusal happened before the original decoder operation;
        // no input was consumed, and its source remains valid and unchanged.
        let decoded = reader.next().unwrap().unwrap();
        assert_eq!(decoded.column(0).to_data(), batch.column(0).to_data());
    }
    #[test]
    fn reader_decompression_empty_buffer_uses_original_arrow_constructor_and_first_header_refusal()
    {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let nodes = builder.create_vector(&[arrow_ipc::FieldNode::new(0, 0)]);
        let buffers =
            builder.create_vector(&[arrow_ipc::Buffer::new(0, 0), arrow_ipc::Buffer::new(0, 8)]);
        let compression = arrow_ipc::BodyCompression::create(
            &mut builder,
            &arrow_ipc::BodyCompressionArgs {
                codec: arrow_ipc::CompressionType::ZSTD,
                method: arrow_ipc::BodyCompressionMethod::BUFFER,
            },
        );
        let record = arrow_ipc::RecordBatch::create(
            &mut builder,
            &arrow_ipc::RecordBatchArgs {
                length: 0,
                nodes: Some(nodes),
                buffers: Some(buffers),
                compression: Some(compression),
                ..Default::default()
            },
        );
        builder.finish(record, None);
        let metadata =
            flatbuffers::root::<arrow_ipc::RecordBatch>(builder.finished_data()).unwrap();
        let body = [0_u8; 8];
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let mut opaque = 0;
        ReaderArrayStructureObserver {
            request: &mut |_, _| Ok::<_, &str>(()),
            opaque: &mut |_, _| {
                opaque += 1;
                Ok(())
            },
            work: &mut || Ok(()),
        }
        .record_batch(
            schema.as_ref(),
            metadata,
            &body,
            ReaderArrayHeaderClass::Retained,
            |_| Ok(false),
        )
        .unwrap();
        assert!(opaque > 0);
        let decoded = arrow_ipc::reader::read_record_batch(
            &Buffer::from(body.to_vec()),
            metadata,
            schema.clone(),
            &std::collections::HashMap::new(),
            None,
            &arrow_ipc::MetadataVersion::V5,
        )
        .unwrap();
        assert_eq!(decoded.num_rows(), 0);
        assert_eq!(decoded.column(0).data_type(), &DataType::Int32);
        let refused = std::cell::Cell::new(false);
        let calls = std::cell::Cell::new(0);
        let error = ReaderArrayStructureObserver {
            request: &mut |_, _| {
                assert!(!refused.get());
                Ok(())
            },
            opaque: &mut |_, _| {
                calls.set(calls.get() + 1);
                refused.set(true);
                Err("original header refusal")
            },
            work: &mut || {
                assert!(!refused.get(), "no tail after original header refusal");
                Ok(())
            },
        }
        .record_batch(
            schema.as_ref(),
            metadata,
            &body,
            ReaderArrayHeaderClass::Retained,
            |_| Ok(false),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ReaderStructureFailure::Creator("original header refusal")
        ));
        assert_eq!(calls.get(), 1);
    }
    #[test]
    fn reader_misaligned_original_buffer_roundtrip_uses_same_private_header_bound() {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let nodes = builder.create_vector(&[arrow_ipc::FieldNode::new(1, 0)]);
        let buffers =
            builder.create_vector(&[arrow_ipc::Buffer::new(0, 0), arrow_ipc::Buffer::new(1, 4)]);
        let record = arrow_ipc::RecordBatch::create(
            &mut builder,
            &arrow_ipc::RecordBatchArgs {
                length: 1,
                nodes: Some(nodes),
                buffers: Some(buffers),
                ..Default::default()
            },
        );
        builder.finish(record, None);
        let metadata =
            flatbuffers::root::<arrow_ipc::RecordBatch>(builder.finished_data()).unwrap();
        let body = [0_u8, 1, 0, 0, 0];
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let mut bounds = 0;
        ReaderArrayStructureObserver {
            request: &mut |_, _| Ok::<_, ()>(()),
            opaque: &mut |_, _| {
                bounds += 1;
                Ok(())
            },
            work: &mut || Ok(()),
        }
        .record_batch(
            schema.as_ref(),
            metadata,
            &body,
            ReaderArrayHeaderClass::Retained,
            |_| Ok(false),
        )
        .unwrap();
        assert!(bounds > 0);
        let decoded = arrow_ipc::reader::read_record_batch(
            &Buffer::from(body.to_vec()),
            metadata,
            schema,
            &std::collections::HashMap::new(),
            None,
            &arrow_ipc::MetadataVersion::V5,
        )
        .unwrap();
        assert_eq!(decoded.num_rows(), 1);
        assert_eq!(
            decoded
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            1
        );
    }
    #[test]
    fn reader_validation_nullmask_headers_preserve_masked_fixed_list_and_dictionary_values_nulls() {
        let item = Arc::new(Field::new("item", DataType::Int32, false));
        let list: ArrayRef = Arc::new(
            FixedSizeListArray::try_new(
                item,
                2,
                Arc::new(Int32Array::from(vec![None, None, Some(7), Some(8)])),
                Some(arrow_buffer::NullBuffer::from(vec![false, true])),
            )
            .unwrap(),
        );
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![0, 2]),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("b")])),
            )
            .unwrap(),
        );
        let fields = vec![Field::new("dict", dictionary.data_type().clone(), false)].into();
        let nested: ArrayRef =
            Arc::new(StructArray::try_new(fields, vec![dictionary], None).unwrap());
        let run: ArrayRef = Arc::new(
            RunArray::<Int32Type>::try_new(
                &Int32Array::from(vec![2, 4]),
                &Int32Array::from(vec![Some(1), None]),
            )
            .unwrap(),
        );
        let run_fields = vec![Field::new("run", run.data_type().clone(), false)].into();
        let masked_run: ArrayRef = Arc::new(
            StructArray::try_new(
                run_fields,
                vec![run],
                Some(arrow_buffer::NullBuffer::from(vec![
                    true, true, false, false,
                ])),
            )
            .unwrap(),
        );
        let union_fields = UnionFields::try_new(
            vec![1, 2],
            vec![
                Field::new("number", DataType::Int32, false),
                Field::new("unused", DataType::Null, true),
            ],
        )
        .unwrap();
        let union: ArrayRef = Arc::new(
            UnionArray::try_new(
                union_fields,
                ScalarBuffer::from(vec![1_i8, 1]),
                Some(ScalarBuffer::from(vec![0_i32, 1])),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2])),
                    Arc::new(NullArray::new(1)),
                ],
            )
            .unwrap(),
        );
        let required_union_fields =
            vec![Field::new("union", union.data_type().clone(), false)].into();
        let required_union: ArrayRef =
            Arc::new(StructArray::try_new(required_union_fields, vec![union], None).unwrap());
        for array in [list, nested, masked_run, required_union] {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                array.data_type().clone(),
                true,
            )]));
            let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
            let bytes = write(&batch);
            let mut reader =
                arrow_ipc::reader::StreamReader::try_new(Cursor::new(bytes.as_slice()), None)
                    .unwrap();
            // Dictionary stream has messages before its record; use the
            // original reader to obtain that record's matching source schema.
            let decoded = reader.next().unwrap().unwrap();
            assert_eq!(decoded.column(0).to_data(), batch.column(0).to_data());
            let mut calls = 0;
            let ty = batch.column(0).data_type();
            let mut model = ReaderArrayStructureObserver {
                request: &mut |_, _| Ok::<_, ()>(()),
                opaque: &mut |_, class| {
                    assert_eq!(class, ReaderArrayHeaderClass::Temporary);
                    calls += 1;
                    Ok(())
                },
                work: &mut || Ok(()),
            };
            match ty {
                DataType::FixedSizeList(..) => model.validation_null_headers(ty).unwrap(),
                DataType::Struct(fields) => {
                    model.logical_null_headers(fields[0].data_type()).unwrap()
                }
                _ => unreachable!(),
            }
            assert!(calls > 0);
            // Preserve the required child declarations; NULL is allowed only
            // through the original parent mask or unused dictionary entry.
            match ty {
                DataType::FixedSizeList(field, _) => assert!(!field.is_nullable()),
                DataType::Struct(fields) => assert!(!fields[0].is_nullable()),
                _ => unreachable!(),
            }
        }
    }
}
