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

//! Locked Arrow representation adapter. No SQL semantics or capacity authority.
use crate::{ByteLayout, ValueError, ValueViolation};
use allocator_api2::{
    alloc::{Allocator, Global},
    vec::Vec as AllocVec,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, LargeBinaryArray, LargeStringArray, StringArray,
    StringViewArray,
};
use arrow_buffer::{ArrowNativeType, Buffer, NullBuffer, ScalarBuffer};
use arrow_data::ByteView;
use novarocks_failure_contract::RepresentationBoundary;
use std::{any::TypeId, sync::Arc};

#[derive(Clone, Copy)]
pub(crate) enum ByteSpan<'a> {
    Inline(&'a [u8]),
    Region {
        buffer: &'a Buffer,
        offset: usize,
        len: usize,
    },
}
impl<'a> ByteSpan<'a> {
    pub(crate) fn bytes(self) -> &'a [u8] {
        match self {
            Self::Inline(bytes) => bytes,
            Self::Region {
                buffer,
                offset,
                len,
            } => &buffer.as_slice()[offset..offset + len],
        }
    }
}
#[cfg(test)]
thread_local! {
    static CONCRETE_BINDINGS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
#[cfg(test)]
pub(crate) fn concrete_bindings() -> usize {
    CONCRETE_BINDINGS.get()
}

fn concrete<T: Array + 'static>(array: &dyn Array) -> Result<&T, ValueError> {
    #[cfg(test)]
    CONCRETE_BINDINGS.set(CONCRETE_BINDINGS.get() + 1);
    array
        .as_any()
        .downcast_ref()
        .ok_or(ValueError::Invariant(ValueViolation::Carrier))
}
pub(crate) fn byte_span(array: &dyn Array, row: usize) -> Result<Option<ByteSpan<'_>>, ValueError> {
    if row >= array.len() {
        return Err(ValueError::Invariant(ValueViolation::Bounds));
    }
    if array.is_null(row) {
        return Ok(None);
    }
    macro_rules! offsets {
        ($ty:ty) => {{
            let a = concrete::<$ty>(array)?;
            let start = usize::try_from(a.value_offsets()[row])
                .map_err(|_| ValueError::Invariant(ValueViolation::Bounds))?;
            let end = usize::try_from(a.value_offsets()[row + 1])
                .map_err(|_| ValueError::Invariant(ValueViolation::Bounds))?;
            if end < start || end > a.values().len() {
                return Err(ValueError::Invariant(ValueViolation::Bounds));
            }
            ByteSpan::Region {
                buffer: a.values(),
                offset: start,
                len: end - start,
            }
        }};
    }
    macro_rules! views {
        ($ty:ty) => {{
            let a = concrete::<$ty>(array)?;
            let raw = a.views()[row];
            let v = ByteView::from(raw);
            if v.length <= 12 {
                ByteSpan::Inline(a.value(row).as_ref())
            } else {
                let buffer = a
                    .data_buffers()
                    .get(v.buffer_index as usize)
                    .ok_or(ValueError::Invariant(ValueViolation::Bounds))?;
                let offset = v.offset as usize;
                let len = v.length as usize;
                if offset.checked_add(len).is_none_or(|end| end > buffer.len()) {
                    return Err(ValueError::Invariant(ValueViolation::Bounds));
                }
                ByteSpan::Region {
                    buffer,
                    offset,
                    len,
                }
            }
        }};
    }
    Ok(Some(match array.data_type() {
        arrow_schema::DataType::Utf8 => offsets!(StringArray),
        arrow_schema::DataType::LargeUtf8 => offsets!(LargeStringArray),
        arrow_schema::DataType::Binary => offsets!(BinaryArray),
        arrow_schema::DataType::LargeBinary => offsets!(LargeBinaryArray),
        arrow_schema::DataType::Utf8View => views!(StringViewArray),
        arrow_schema::DataType::BinaryView => views!(BinaryViewArray),
        _ => return Err(ValueError::Invariant(ValueViolation::Carrier)),
    }))
}

pub(crate) fn inline_view(bytes: &[u8]) -> Result<u128, ValueError> {
    if bytes.len() > 12 {
        return Err(ValueError::Invariant(ValueViolation::Bounds));
    }
    let mut raw = [0; 16];
    raw[..4].copy_from_slice(&(bytes.len() as u32).to_le_bytes());
    raw[4..4 + bytes.len()].copy_from_slice(bytes);
    Ok(u128::from_le_bytes(raw))
}
/// Signed field bounds come from the Arrow format, independently of Rust u32.
/// The sum offset+length is checked against the region, not limited to i32.
pub(crate) fn external_view(
    layout: ByteLayout,
    bytes: &[u8],
    buffer_index: usize,
    offset: usize,
) -> Result<u128, ValueError> {
    let invalid = |required| {
        ValueError::Representation(layout.representation(
            RepresentationBoundary::MinimumUnit,
            Some(required),
            Some(i32::MAX as u64),
            None,
        ))
    };
    if bytes.len() <= 12 {
        return inline_view(bytes);
    }
    if bytes.len() > i32::MAX as usize {
        return Err(invalid(bytes.len() as u64));
    }
    // Exhausting one output's region table is not an unrepresentable value.
    // Callers can flush an output or rebase a source slice before this adapter.
    if buffer_index > i32::MAX as usize {
        return Err(ValueError::Representation(
            crate::error::field_representation(
                7,
                RepresentationBoundary::SingleOutput,
                u64::try_from(buffer_index)
                    .ok()
                    .and_then(|v| v.checked_add(1)),
                Some(i32::MAX as u64 + 1),
                None,
            ),
        ));
    }
    if offset > i32::MAX as usize {
        return Err(ValueError::Representation(
            crate::error::field_representation(
                8,
                RepresentationBoundary::SingleOutput,
                u64::try_from(offset).ok(),
                Some(i32::MAX as u64),
                None,
            ),
        ));
    }
    Ok(ByteView {
        length: bytes.len() as u32,
        prefix: u32::from_le_bytes(bytes[..4].try_into().unwrap()),
        buffer_index: buffer_index as u32,
        offset: offset as u32,
    }
    .into())
}

/// Build only after the caller has checked every descriptor against retained
/// regions and proved UTF8 from str or an admitted string source.
///
/// # Safety
/// Every descriptor is valid for buffers, all string ranges are UTF8 and nulls
/// (when present) have exactly views.len() bits. Null slots also have legal views.
pub(crate) unsafe fn make_view_unchecked(
    layout: ByteLayout,
    views: ScalarBuffer<u128>,
    buffers: Vec<Buffer>,
    nulls: Option<NullBuffer>,
) -> Result<ArrayRef, ValueError> {
    let array: ArrayRef = match layout {
        ByteLayout::Utf8View => {
            let buffers: Arc<[Buffer]> = buffers.into();
            Arc::new(unsafe { StringViewArray::new_unchecked(views, buffers, nulls) })
        }
        ByteLayout::BinaryView => {
            let buffers: Arc<[Buffer]> = buffers.into();
            Arc::new(unsafe { BinaryViewArray::new_unchecked(views, buffers, nulls) })
        }
        _ => return Err(ValueError::Invariant(ValueViolation::Layout)),
    };
    debug_assert!(array.to_data().validate_full().is_ok());
    Ok(array)
}

pub(crate) fn view_parts(
    array: &dyn Array,
) -> Result<(&ScalarBuffer<u128>, &[Buffer]), ValueError> {
    match array.data_type() {
        arrow_schema::DataType::Utf8View => {
            let a = concrete::<StringViewArray>(array)?;
            Ok((a.views(), a.data_buffers()))
        }
        arrow_schema::DataType::BinaryView => {
            let a = concrete::<BinaryViewArray>(array)?;
            Ok((a.views(), a.data_buffers()))
        }
        _ => Err(ValueError::Invariant(ValueViolation::Layout)),
    }
}

/// Global storage becomes ordinary Arrow backing. Foreign allocators release
/// their original storage after copying into standard backing; no deallocator
/// is guessed and no custom-buffer penalty is imposed on ordinary output.
pub(crate) fn into_arrow_buffer<T: ArrowNativeType, A: Allocator + 'static>(
    values: AllocVec<T, A>,
) -> Buffer {
    if TypeId::of::<A>() == TypeId::of::<Global>() {
        let (ptr, len, capacity, allocator) = values.into_raw_parts_with_alloc();
        drop(allocator);
        // SAFETY: allocator-api2 Global delegates to std::alloc with the same
        // Layout<T>; the TypeId check proves the exact allocator, not a proxy.
        // Ownership transfers once, with the original element count/capacity.
        Buffer::from_vec(unsafe { Vec::from_raw_parts(ptr, len, capacity) })
    } else {
        Buffer::from_vec(values.as_slice().to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn inline_external_and_sliced_source_ranges() {
        let source = StringArray::from(vec![Some("short"), None, Some("a much longer value")]);
        let sliced = source.slice(1, 2);
        assert!(byte_span(&sliced, 0).unwrap().is_none());
        let span = byte_span(&sliced, 1).unwrap().unwrap();
        assert_eq!(span.bytes(), b"a much longer value");
        let ByteSpan::Region {
            buffer,
            offset,
            len,
        } = span
        else {
            panic!("expected region")
        };
        let raw = external_view(ByteLayout::Utf8View, span.bytes(), 0, offset).unwrap();
        // SAFETY: raw was checked against exactly this retained source region;
        // the value comes from an admitted StringArray and the one slot is valid.
        let output = unsafe {
            make_view_unchecked(
                ByteLayout::Utf8View,
                vec![raw].into(),
                vec![buffer.clone()],
                None,
            )
        }
        .unwrap();
        assert_eq!(
            byte_span(output.as_ref(), 0).unwrap().unwrap().bytes(),
            &buffer.as_slice()[offset..offset + len]
        );
        assert_eq!(inline_view(b"abc").unwrap() as u32, 3);
    }
    #[test]
    fn address_fields_are_not_minimum_value_length_facts() {
        let bytes = b"a longer byte value";
        for (index, offset, code) in [(i32::MAX as usize + 1, 0, 7), (0, i32::MAX as usize + 1, 8)]
        {
            let error = external_view(ByteLayout::Utf8View, bytes, index, offset).unwrap_err();
            let facts = error.representation().unwrap();
            assert_eq!(facts.boundary, RepresentationBoundary::SingleOutput);
            assert_eq!(facts.layout.code.get(), code);
            assert_eq!(facts.representable_prefix, None);
        }
    }
    #[test]
    fn global_buffer_retains_standard_mutability() {
        let mut values = AllocVec::new_in(Global);
        values.extend_from_slice(b"test");
        let buffer = into_arrow_buffer(values);
        assert!(buffer.into_mutable().is_ok());
    }
}

/// Identity of the visible offsets, payload and validity storage. Entries keep
/// the source array alive; these addresses never outlive that custody.
/// Slots: offsets(base,offset,len), values(base,offset,len), nulls(base,offset,
/// bit-offset,bit-length), array length, frozen physical byte-layout id.
pub(crate) fn byte_storage_identity(array: &dyn Array) -> Result<[usize; 12], ValueError> {
    let layout = ByteLayout::from_data_type(array.data_type())
        .ok_or(ValueError::Invariant(ValueViolation::Carrier))?;
    macro_rules! buffers {
        ($ty:ty) => {{
            let a = concrete::<$ty>(array)?;
            (a.offsets().inner().inner(), a.values())
        }};
    }
    let (offsets, values) = match layout {
        ByteLayout::Utf8 => buffers!(StringArray),
        ByteLayout::LargeUtf8 => buffers!(LargeStringArray),
        ByteLayout::Binary => buffers!(BinaryArray),
        ByteLayout::LargeBinary => buffers!(LargeBinaryArray),
        _ => return Err(ValueError::Invariant(ValueViolation::Layout)),
    };
    let mut key = [0; 12];
    key[..6].copy_from_slice(&[
        offsets.data_ptr().as_ptr() as usize,
        offsets.ptr_offset(),
        offsets.len(),
        values.data_ptr().as_ptr() as usize,
        values.ptr_offset(),
        values.len(),
    ]);
    if let Some(nulls) = array.nulls() {
        let b = nulls.inner().inner();
        key[6..10].copy_from_slice(&[
            b.data_ptr().as_ptr() as usize,
            b.ptr_offset(),
            nulls.offset(),
            nulls.len(),
        ]);
    }
    key[10] = array.len();
    key[11] = layout as usize;
    Ok(key)
}

pub(crate) fn make_offsets(
    layout: ByteLayout,
    offsets: Buffer,
    payload: Buffer,
    nulls: Option<NullBuffer>,
) -> Result<ArrayRef, ValueError> {
    use arrow_buffer::OffsetBuffer;
    macro_rules! array {
        ($ty:ty,$offset:ty) => {{
            let offsets = OffsetBuffer::new(ScalarBuffer::<$offset>::from(offsets));
            Arc::new(<$ty>::try_new(offsets, payload, nulls)?) as ArrayRef
        }};
    }
    Ok(match layout {
        ByteLayout::Utf8 => array!(StringArray, i32),
        ByteLayout::LargeUtf8 => array!(LargeStringArray, i64),
        ByteLayout::Binary => array!(BinaryArray, i32),
        ByteLayout::LargeBinary => array!(LargeBinaryArray, i64),
        _ => return Err(ValueError::Invariant(ValueViolation::Layout)),
    })
}

pub(crate) fn make_nulls(validity: Buffer, len: usize, null_count: usize) -> Option<NullBuffer> {
    if null_count == 0 {
        None
    } else {
        // The caller tracks the packed validity bits transactionally. Using
        // the safe constructor also verifies the exact null count in debug.
        let nulls = NullBuffer::new(arrow_buffer::BooleanBuffer::new(validity, 0, len));
        debug_assert_eq!(nulls.null_count(), null_count);
        Some(nulls)
    }
}

pub(crate) struct BorrowedNode {
    pub data_buffers: usize,
    pub child_count: usize,
}
pub(crate) fn borrowed_node(array: &dyn Array) -> Result<BorrowedNode, ValueError> {
    use arrow_schema::{DataType, UnionMode};
    let (data_buffers, child_count) = match array.data_type() {
        DataType::Null => (0, 0),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => (2, 0),
        DataType::Utf8View | DataType::BinaryView => (1 + view_parts(array)?.1.len(), 0),
        DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _) => (1, 1),
        DataType::ListView(_) | DataType::LargeListView(_) => (2, 1),
        DataType::FixedSizeList(_, _) => (0, 1),
        DataType::Struct(fields) => (0, fields.len()),
        DataType::Union(fields, mode) => {
            (if *mode == UnionMode::Dense { 2 } else { 1 }, fields.len())
        }
        DataType::Dictionary(..) => (1, 1),
        DataType::RunEndEncoded(..) => (0, 2),
        _ => (1, 0),
    };
    // Match actual built-in headers before publishing structural facts. An
    // empty container still needs this check, even without a child to visit.
    if data_buffers != 0 {
        borrowed_buffer(array, 0)?;
    } else {
        use arrow_array::{FixedSizeListArray, NullArray, RunArray, StructArray, types::*};
        match array.data_type() {
            DataType::Null => {
                concrete::<NullArray>(array)?;
            }
            DataType::Struct(_) => {
                concrete::<StructArray>(array)?;
            }
            DataType::FixedSizeList(..) => {
                concrete::<FixedSizeListArray>(array)?;
            }
            DataType::RunEndEncoded(ends, _) => match ends.data_type() {
                DataType::Int16 => {
                    concrete::<RunArray<Int16Type>>(array)?;
                }
                DataType::Int32 => {
                    concrete::<RunArray<Int32Type>>(array)?;
                }
                DataType::Int64 => {
                    concrete::<RunArray<Int64Type>>(array)?;
                }
                _ => return Err(ValueError::Invariant(ValueViolation::Carrier)),
            },
            _ => return Err(ValueError::Invariant(ValueViolation::Carrier)),
        }
    }
    Ok(BorrowedNode {
        data_buffers,
        child_count,
    })
}
pub(crate) enum BorrowedChild<'a> {
    Array(&'a dyn Array),
    RunEnds {
        data_type: &'a arrow_schema::DataType,
        buffer: &'a Buffer,
    },
}
pub(crate) fn borrowed_child(
    array: &dyn Array,
    index: usize,
) -> Result<BorrowedChild<'_>, ValueError> {
    use arrow_array::types::*;
    use arrow_array::*;
    use arrow_schema::DataType;
    let bounds = || ValueError::Invariant(ValueViolation::Bounds);
    macro_rules! one {
        ($ty:ty,$method:ident) => {{
            if index != 0 {
                return Err(bounds());
            }
            concrete::<$ty>(array)?.$method().as_ref()
        }};
    }
    macro_rules! dict {
        ($ty:ty) => {
            one!(DictionaryArray<$ty>, values)
        };
    }
    macro_rules! run {
        ($ty:ty,$ends:expr) => {{
            let a = concrete::<RunArray<$ty>>(array)?;
            match index {
                0 => {
                    return Ok(BorrowedChild::RunEnds {
                        data_type: $ends.data_type(),
                        buffer: a.run_ends().inner().inner(),
                    });
                }
                1 => a.values().as_ref(),
                _ => return Err(bounds()),
            }
        }};
    }
    Ok(BorrowedChild::Array(match array.data_type() {
        DataType::List(_) => one!(ListArray, values),
        DataType::LargeList(_) => one!(LargeListArray, values),
        DataType::ListView(_) => one!(ListViewArray, values),
        DataType::LargeListView(_) => one!(LargeListViewArray, values),
        DataType::FixedSizeList(_, _) => one!(FixedSizeListArray, values),
        DataType::Map(_, _) => {
            if index != 0 {
                return Err(bounds());
            }
            concrete::<MapArray>(array)?.entries()
        }
        DataType::Struct(_) => concrete::<StructArray>(array)?
            .columns()
            .get(index)
            .ok_or_else(bounds)?
            .as_ref(),
        DataType::Union(fields, _) => {
            let (id, _) = fields.iter().nth(index).ok_or_else(bounds)?;
            concrete::<UnionArray>(array)?.child(id).as_ref()
        }
        DataType::Dictionary(key, _) => match key.as_ref() {
            DataType::Int8 => dict!(Int8Type),
            DataType::Int16 => dict!(Int16Type),
            DataType::Int32 => dict!(Int32Type),
            DataType::Int64 => dict!(Int64Type),
            DataType::UInt8 => dict!(UInt8Type),
            DataType::UInt16 => dict!(UInt16Type),
            DataType::UInt32 => dict!(UInt32Type),
            DataType::UInt64 => dict!(UInt64Type),
            _ => return Err(bounds()),
        },
        DataType::RunEndEncoded(ends, _) => match ends.data_type() {
            DataType::Int16 => run!(Int16Type, ends),
            DataType::Int32 => run!(Int32Type, ends),
            DataType::Int64 => run!(Int64Type, ends),
            _ => return Err(bounds()),
        },
        _ => return Err(bounds()),
    }))
}

/// Adopt one visible source region without copying payload. Offset rebasing
/// exposes only a slice already owned by the source; no hidden bytes are read.
pub(crate) fn append_span_view(
    layout: ByteLayout,
    span: ByteSpan<'_>,
    regions: &mut Vec<Buffer>,
) -> Result<u128, ValueError> {
    let bytes = span.bytes();
    if bytes.len() <= 12 {
        return inline_view(bytes);
    }
    let ByteSpan::Region {
        buffer,
        offset,
        len,
    } = span
    else {
        return Err(ValueError::Invariant(ValueViolation::Carrier));
    };
    let (region, offset) = if offset > i32::MAX as usize {
        (buffer.slice_with_length(offset, len), 0)
    } else {
        (buffer.clone(), offset)
    };
    let identity = |b: &Buffer| (b.data_ptr(), b.ptr_offset(), b.len());
    let index = if let Some(index) = regions
        .iter()
        .position(|b| identity(b) == identity(&region))
    {
        index
    } else {
        regions
            .try_reserve(1)
            .map_err(|source| ValueError::Allocation {
                requested: std::mem::size_of::<Buffer>(),
                source,
            })?;
        let index = regions.len();
        regions.push(region);
        index
    };
    external_view(layout, bytes, index, offset)
}

#[cfg(test)]
mod structural_tests {
    use super::*;
    use arrow_array::{Int16Array, RunArray, types::Int16Type};
    #[test]
    fn run_ends_are_borrowed_as_the_actual_implicit_primitive_child() {
        let ends = Int16Array::from(vec![2, 4]);
        let values = StringArray::from(vec!["a", "b"]);
        let run = RunArray::<Int16Type>::try_new(&ends, &values).unwrap();
        let node = borrowed_node(&run).unwrap();
        assert_eq!((node.data_buffers, node.child_count), (0, 2));
        let BorrowedChild::RunEnds { data_type, buffer } = borrowed_child(&run, 0).unwrap() else {
            panic!("run ends")
        };
        assert_eq!(data_type, &arrow_schema::DataType::Int16);
        assert_eq!(buffer.as_ptr(), run.run_ends().inner().inner().as_ptr());
        let BorrowedChild::Array(child) = borrowed_child(&run, 1).unwrap() else {
            panic!("values")
        };
        assert_eq!(child.len(), 2);
    }
    #[test]
    fn offset_and_validity_slice_geometry_are_part_of_cache_identity() {
        let source = StringArray::from(vec![Some("a"), None, Some("c"), None, Some("e")]);
        let a = source.slice(0, 3);
        let b = source.slice(1, 3);
        let ka = byte_storage_identity(&a).unwrap();
        let kb = byte_storage_identity(&b).unwrap();
        assert_ne!(ka, kb);
        assert_eq!(ka[0], kb[0]);
        assert_eq!(ka[3], kb[3]);
        assert_ne!(ka[1], kb[1]);
        assert_ne!(ka[8], kb[8]);
    }
}

/// Descriptor-only fast path. External payload is never touched for length or
/// prefix; inline bytes borrow the admitted descriptor through Arrow's value API.
type InlinePrefix<'a> = (usize, [u8; 4], Option<&'a [u8]>);
pub(crate) fn view_inline_prefix(
    array: &dyn Array,
    row: usize,
) -> Result<InlinePrefix<'_>, ValueError> {
    if row >= array.len() {
        return Err(ValueError::Invariant(ValueViolation::Bounds));
    }
    let (views, _) = view_parts(array)?;
    let raw = views[row];
    let bytes = raw.to_le_bytes();
    let length = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
    let prefix = bytes[4..8].try_into().unwrap();
    let inline = if length <= 12 {
        Some(match array.data_type() {
            arrow_schema::DataType::Utf8View => {
                concrete::<StringViewArray>(array)?.value(row).as_bytes()
            }
            arrow_schema::DataType::BinaryView => concrete::<BinaryViewArray>(array)?.value(row),
            _ => return Err(ValueError::Invariant(ValueViolation::Layout)),
        })
    } else {
        None
    };
    Ok((length, prefix, inline))
}

/// Borrow only a node's mounted data buffers, independently of logical row
/// reachability. NULL buffers are exposed separately by Array::nulls().
pub(crate) fn borrowed_buffer(array: &dyn Array, index: usize) -> Result<&Buffer, ValueError> {
    use arrow_array::types::*;
    use arrow_array::*;
    use arrow_schema::{DataType, IntervalUnit, TimeUnit};
    let invalid = || ValueError::Invariant(ValueViolation::Bounds);
    macro_rules! single {
        ($value:expr) => {{
            if index != 0 {
                return Err(invalid());
            }
            $value
        }};
    }
    macro_rules! primitive {
        ($ty:ty) => {
            single!(concrete::<PrimitiveArray<$ty>>(array)?.values().inner())
        };
    }
    macro_rules! dictionary {
        ($ty:ty) => {
            single!(
                concrete::<DictionaryArray<$ty>>(array)?
                    .keys()
                    .values()
                    .inner()
            )
        };
    }
    macro_rules! bytes {
        ($ty:ty) => {{
            let array = concrete::<$ty>(array)?;
            match index {
                0 => array.offsets().inner().inner(),
                1 => array.values(),
                _ => return Err(invalid()),
            }
        }};
    }
    Ok(match array.data_type() {
        DataType::Boolean => single!(concrete::<BooleanArray>(array)?.values().inner()),
        DataType::Int8 => primitive!(Int8Type),
        DataType::Int16 => primitive!(Int16Type),
        DataType::Int32 => primitive!(Int32Type),
        DataType::Int64 => primitive!(Int64Type),
        DataType::UInt8 => primitive!(UInt8Type),
        DataType::UInt16 => primitive!(UInt16Type),
        DataType::UInt32 => primitive!(UInt32Type),
        DataType::UInt64 => primitive!(UInt64Type),
        DataType::Float16 => primitive!(Float16Type),
        DataType::Float32 => primitive!(Float32Type),
        DataType::Float64 => primitive!(Float64Type),
        DataType::Date32 => primitive!(Date32Type),
        DataType::Date64 => primitive!(Date64Type),
        DataType::Time32(TimeUnit::Second) => primitive!(Time32SecondType),
        DataType::Time32(TimeUnit::Millisecond) => primitive!(Time32MillisecondType),
        DataType::Time64(TimeUnit::Microsecond) => primitive!(Time64MicrosecondType),
        DataType::Time64(TimeUnit::Nanosecond) => primitive!(Time64NanosecondType),
        DataType::Timestamp(TimeUnit::Second, _) => primitive!(TimestampSecondType),
        DataType::Timestamp(TimeUnit::Millisecond, _) => primitive!(TimestampMillisecondType),
        DataType::Timestamp(TimeUnit::Microsecond, _) => primitive!(TimestampMicrosecondType),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => primitive!(TimestampNanosecondType),
        DataType::Duration(TimeUnit::Second) => primitive!(DurationSecondType),
        DataType::Duration(TimeUnit::Millisecond) => primitive!(DurationMillisecondType),
        DataType::Duration(TimeUnit::Microsecond) => primitive!(DurationMicrosecondType),
        DataType::Duration(TimeUnit::Nanosecond) => primitive!(DurationNanosecondType),
        DataType::Interval(IntervalUnit::YearMonth) => primitive!(IntervalYearMonthType),
        DataType::Interval(IntervalUnit::DayTime) => primitive!(IntervalDayTimeType),
        DataType::Interval(IntervalUnit::MonthDayNano) => primitive!(IntervalMonthDayNanoType),
        DataType::Decimal32(..) => primitive!(Decimal32Type),
        DataType::Decimal64(..) => primitive!(Decimal64Type),
        DataType::Decimal128(..) => primitive!(Decimal128Type),
        DataType::Decimal256(..) => primitive!(Decimal256Type),
        DataType::Utf8 => bytes!(StringArray),
        DataType::LargeUtf8 => bytes!(LargeStringArray),
        DataType::Binary => bytes!(BinaryArray),
        DataType::LargeBinary => bytes!(LargeBinaryArray),
        DataType::FixedSizeBinary(_) => single!(concrete::<FixedSizeBinaryArray>(array)?.values()),
        DataType::Utf8View | DataType::BinaryView => {
            let (views, buffers) = view_parts(array)?;
            if index == 0 {
                views.inner()
            } else {
                buffers.get(index - 1).ok_or_else(invalid)?
            }
        }
        DataType::List(_) => single!(concrete::<ListArray>(array)?.offsets().inner().inner()),
        DataType::LargeList(_) => {
            single!(concrete::<LargeListArray>(array)?.offsets().inner().inner())
        }
        DataType::ListView(_) => {
            let a = concrete::<ListViewArray>(array)?;
            match index {
                0 => a.offsets().inner(),
                1 => a.sizes().inner(),
                _ => return Err(invalid()),
            }
        }
        DataType::LargeListView(_) => {
            let a = concrete::<LargeListViewArray>(array)?;
            match index {
                0 => a.offsets().inner(),
                1 => a.sizes().inner(),
                _ => return Err(invalid()),
            }
        }
        DataType::Map(_, _) => single!(concrete::<MapArray>(array)?.offsets().inner().inner()),
        DataType::Union(..) => {
            let a = concrete::<UnionArray>(array)?;
            match index {
                0 => a.type_ids().inner(),
                1 => a.offsets().ok_or_else(invalid)?.inner(),
                _ => return Err(invalid()),
            }
        }
        DataType::Dictionary(key, _) => match key.as_ref() {
            DataType::Int8 => dictionary!(Int8Type),
            DataType::Int16 => dictionary!(Int16Type),
            DataType::Int32 => dictionary!(Int32Type),
            DataType::Int64 => dictionary!(Int64Type),
            DataType::UInt8 => dictionary!(UInt8Type),
            DataType::UInt16 => dictionary!(UInt16Type),
            DataType::UInt32 => dictionary!(UInt32Type),
            DataType::UInt64 => dictionary!(UInt64Type),
            _ => return Err(invalid()),
        },
        _ => return Err(invalid()),
    })
}

pub(crate) enum StructuralRef<'a> {
    Leaf,
    Struct {
        fields: &'a arrow_schema::Fields,
        children: &'a [ArrayRef],
    },
    Union {
        fields: &'a arrow_schema::UnionFields,
        array: &'a arrow_array::UnionArray,
    },
    List32 {
        field: &'a arrow_schema::FieldRef,
        offsets: &'a arrow_buffer::OffsetBuffer<i32>,
        values: &'a ArrayRef,
    },
    List64 {
        field: &'a arrow_schema::FieldRef,
        offsets: &'a arrow_buffer::OffsetBuffer<i64>,
        values: &'a ArrayRef,
    },
    FixedSizeList {
        field: &'a arrow_schema::FieldRef,
        width: usize,
        values: &'a ArrayRef,
        child_start: usize,
    },
    Map {
        entries_field: &'a arrow_schema::FieldRef,
        sorted: bool,
        offsets: &'a arrow_buffer::OffsetBuffer<i32>,
        keys: &'a ArrayRef,
        values: &'a ArrayRef,
    },
}
pub(crate) fn structural_view(source: &ArrayRef) -> Result<StructuralRef<'_>, ValueError> {
    use arrow_array::*;
    use arrow_schema::DataType;
    Ok(match source.data_type() {
        DataType::Struct(fields) => StructuralRef::Struct {
            fields,
            children: concrete::<StructArray>(source.as_ref())?.columns(),
        },
        DataType::Union(fields, _) => StructuralRef::Union {
            fields,
            array: concrete::<UnionArray>(source.as_ref())?,
        },
        DataType::List(field) => {
            let a = concrete::<ListArray>(source.as_ref())?;
            StructuralRef::List32 {
                field,
                offsets: a.offsets(),
                values: a.values(),
            }
        }
        DataType::LargeList(field) => {
            let a = concrete::<LargeListArray>(source.as_ref())?;
            StructuralRef::List64 {
                field,
                offsets: a.offsets(),
                values: a.values(),
            }
        }
        DataType::FixedSizeList(field, width) => {
            let a = concrete::<FixedSizeListArray>(source.as_ref())?;
            StructuralRef::FixedSizeList {
                field,
                width: usize::try_from(*width)
                    .map_err(|_| ValueError::Invariant(ValueViolation::Structure))?,
                values: a.values(),
                child_start: 0,
            }
        }
        DataType::Map(entries_field, sorted) => {
            let a = concrete::<MapArray>(source.as_ref())?;
            StructuralRef::Map {
                entries_field,
                sorted: *sorted,
                offsets: a.offsets(),
                keys: a.keys(),
                values: a.values(),
            }
        }
        _ => StructuralRef::Leaf,
    })
}

/// Arrow's high-level constructors impose extra logical masks for these
/// required encoded children. Default validated ArrayData preserves the
/// original physical contract already proved under active ancestors.
pub(crate) fn requires_default_validated_container(target: &arrow_schema::DataType) -> bool {
    use arrow_schema::{DataType, Field};

    let extra_mask = |field: &Field| {
        !field.is_nullable()
            && matches!(
                field.data_type(),
                DataType::Dictionary(_, _)
                    | DataType::RunEndEncoded(_, _)
                    | DataType::Union(_, _)
                    | DataType::Null
            )
    };
    match target {
        DataType::Struct(fields) => fields.iter().any(|field| extra_mask(field)),
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            extra_mask(field)
        }
        DataType::Map(entries, _) => {
            matches!(entries.data_type(), DataType::Struct(fields) if fields.iter().any(|field| extra_mask(field)))
        }
        _ => false,
    }
}

/// Rebox admitted containers with their original offset/null geometry. The
/// caller has observed the actual wrapper/field work before entering here.
pub(crate) fn rebuild_structural(
    source: &ArrayRef,
    target: &arrow_schema::DataType,
    children: Vec<ArrayRef>,
) -> Result<ArrayRef, ValueError> {
    use arrow_array::{
        FixedSizeListArray, LargeListArray, ListArray, MapArray, StructArray, UnionArray,
        make_array,
    };
    use arrow_schema::DataType;
    let same_shape = match (source.data_type(), target) {
        (arrow_schema::DataType::Struct(a), arrow_schema::DataType::Struct(b)) => {
            a.len() == b.len()
        }
        (arrow_schema::DataType::List(_), arrow_schema::DataType::List(_))
        | (arrow_schema::DataType::LargeList(_), arrow_schema::DataType::LargeList(_)) => true,
        (arrow_schema::DataType::Map(_, a), arrow_schema::DataType::Map(_, b)) => a == b,
        (
            arrow_schema::DataType::FixedSizeList(_, a),
            arrow_schema::DataType::FixedSizeList(_, b),
        ) => a == b,
        (DataType::Union(a, am), DataType::Union(b, bm)) => {
            am == bm && a.iter().map(|(id, _)| id).eq(b.iter().map(|(id, _)| id))
        }
        _ => false,
    };
    if !same_shape {
        return Err(ValueError::Invariant(ValueViolation::Structure));
    }
    if !requires_default_validated_container(target) {
        let nulls = source.nulls().cloned();
        let output: Option<ArrayRef> = match (target, structural_view(source)?) {
            (DataType::Struct(fields), _) => Some(Arc::new(StructArray::try_new_with_length(
                fields.clone(),
                children,
                nulls,
                source.len(),
            )?)),
            (DataType::List(field), StructuralRef::List32 { offsets, .. })
                if children.len() == 1 =>
            {
                Some(Arc::new(ListArray::try_new(
                    field.clone(),
                    offsets.clone(),
                    children
                        .into_iter()
                        .next()
                        .ok_or_else(|| ValueError::Invariant(ValueViolation::Structure))?,
                    nulls,
                )?))
            }
            (DataType::LargeList(field), StructuralRef::List64 { offsets, .. })
                if children.len() == 1 =>
            {
                Some(Arc::new(LargeListArray::try_new(
                    field.clone(),
                    offsets.clone(),
                    children
                        .into_iter()
                        .next()
                        .ok_or_else(|| ValueError::Invariant(ValueViolation::Structure))?,
                    nulls,
                )?))
            }
            (DataType::FixedSizeList(field, width), _) if children.len() == 1 => {
                Some(Arc::new(FixedSizeListArray::try_new_with_length(
                    field.clone(),
                    *width,
                    children
                        .into_iter()
                        .next()
                        .ok_or_else(|| ValueError::Invariant(ValueViolation::Structure))?,
                    nulls,
                    source.len(),
                )?))
            }
            (DataType::Union(fields, _), StructuralRef::Union { array, .. }) => {
                Some(Arc::new(UnionArray::try_new(
                    fields.clone(),
                    array.type_ids().clone(),
                    array.offsets().cloned(),
                    children,
                )?))
            }
            (DataType::Map(entries, sorted), StructuralRef::Map { offsets, .. }) => {
                let map = concrete::<MapArray>(source.as_ref())?;
                let DataType::Struct(fields) = entries.data_type() else {
                    return Err(ValueError::Invariant(ValueViolation::Structure));
                };
                let values = StructArray::try_new_with_length(
                    fields.clone(),
                    children,
                    map.entries().nulls().cloned(),
                    map.entries().len(),
                )?;
                Some(Arc::new(MapArray::try_new(
                    entries.clone(),
                    offsets.clone(),
                    values,
                    nulls,
                    *sorted,
                )?))
            }
            _ => None,
        };
        // Every supported ordinary branch owns the supplied child vector;
        // unsupported shapes are rejected, never retried under weaker checks.
        return output.ok_or(ValueError::Invariant(ValueViolation::Structure));
    }
    let child_data = match target {
        DataType::Union(fields, _) if fields.len() == children.len() => {
            children.into_iter().map(|child| child.to_data()).collect()
        }
        DataType::Struct(fields) if fields.len() == children.len() => {
            children.iter().map(|c| c.to_data()).collect()
        }
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(..)
            if children.len() == 1 =>
        {
            children.iter().map(|c| c.to_data()).collect()
        }
        DataType::Map(entries, _) if children.len() == 2 => {
            let a = concrete::<MapArray>(source.as_ref())?;
            let entries = a
                .entries()
                .to_data()
                .into_builder()
                .data_type(entries.data_type().clone())
                .child_data(children.iter().map(|c| c.to_data()).collect())
                .build()?;
            vec![entries]
        }
        _ => return Err(ValueError::Invariant(ValueViolation::Structure)),
    };
    let data = source
        .to_data()
        .into_builder()
        .data_type(target.clone())
        .child_data(child_data)
        .build()?;
    Ok(make_array(data))
}

pub(crate) fn source_dictionary(
    source: &ArrayRef,
) -> Result<(&arrow_array::Int32Array, &ArrayRef), ValueError> {
    let dictionary =
        concrete::<arrow_array::DictionaryArray<arrow_array::types::Int32Type>>(source.as_ref())?;
    Ok((dictionary.keys(), dictionary.values()))
}

/// Project actual child encodings into the frozen parent layout. Field names
/// and NULL declarations come exclusively from the frozen layout author.
#[expect(
    deprecated,
    reason = "Arrow 58 exposes retained IPC dictionary IDs only through these field APIs."
)]
pub(crate) fn metadata_free_type_rebuild(
    source: &arrow_schema::DataType,
    target: &arrow_schema::DataType,
    children: &[arrow_schema::DataType],
) -> Result<arrow_schema::DataType, ValueError> {
    use arrow_schema::{DataType, Field};
    let invalid = || ValueError::Invariant(ValueViolation::Structure);
    let field = |actual: &Field, frozen: &Field, data_type: &DataType| {
        let result = if matches!(data_type, DataType::Dictionary(..)) {
            // IPC dictionary descriptors belong to the actual retained
            // encoding; names and NULL declarations still belong to the
            // frozen layout. No source annotations author semantic meaning.
            if !matches!(actual.data_type(), DataType::Dictionary(..)) {
                return Err(invalid());
            }
            Field::new_dict(
                frozen.name(),
                data_type.clone(),
                frozen.is_nullable(),
                actual.dict_id().ok_or_else(invalid)?,
                actual.dict_is_ordered().ok_or_else(invalid)?,
            )
        } else {
            Field::new(frozen.name(), data_type.clone(), frozen.is_nullable())
        };
        Ok(Arc::new(result))
    };
    Ok(match (source, target) {
        (DataType::Struct(actual), DataType::Struct(fields))
            if actual.len() == fields.len() && fields.len() == children.len() =>
        {
            DataType::Struct(
                actual
                    .iter()
                    .zip(fields)
                    .zip(children)
                    .map(|((a, f), t)| field(a, f, t))
                    .collect::<Result<_, _>>()?,
            )
        }
        (DataType::Union(actual, am), DataType::Union(fields, mode))
            if am == mode && actual.len() == fields.len() && fields.len() == children.len() =>
        {
            let mut ids = Vec::with_capacity(fields.len());
            let mut output = Vec::with_capacity(fields.len());
            for (((aid, actual), (id, frozen)), child) in
                actual.iter().zip(fields.iter()).zip(children)
            {
                if aid != id {
                    return Err(invalid());
                }
                ids.push(id);
                output.push(field(actual, frozen, child)?);
            }
            DataType::Union(arrow_schema::UnionFields::try_new(ids, output)?, *mode)
        }
        (DataType::List(a), DataType::List(f)) if children.len() == 1 => {
            DataType::List(field(a, f, &children[0])?)
        }
        (DataType::LargeList(a), DataType::LargeList(f)) if children.len() == 1 => {
            DataType::LargeList(field(a, f, &children[0])?)
        }
        (DataType::FixedSizeList(a, aw), DataType::FixedSizeList(f, width))
            if aw == width && children.len() == 1 =>
        {
            DataType::FixedSizeList(field(a, f, &children[0])?, *width)
        }
        (DataType::Map(a, asort), DataType::Map(entries, sorted))
            if asort == sorted && children.len() == 2 =>
        {
            let (DataType::Struct(actual), DataType::Struct(fields)) =
                (a.data_type(), entries.data_type())
            else {
                return Err(invalid());
            };
            if actual.len() != 2 || fields.len() != 2 {
                return Err(invalid());
            }
            let fields = actual
                .iter()
                .zip(fields)
                .zip(children)
                .map(|((a, f), t)| field(a, f, t))
                .collect::<Result<_, _>>()?;
            DataType::Map(field(a, entries, &DataType::Struct(fields))?, *sorted)
        }
        _ => return Err(invalid()),
    })
}

#[cfg(test)]
pub(crate) fn tracked_string_source(
    values: &[&str],
    drops: Arc<std::sync::atomic::AtomicUsize>,
) -> ArrayRef {
    use std::ptr::NonNull;
    struct Owner {
        bytes: Vec<u8>,
        drops: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl Drop for Owner {
        fn drop(&mut self) {
            self.drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let mut bytes = Vec::new();
    let mut offsets = vec![0i32];
    for value in values {
        bytes.extend_from_slice(value.as_bytes());
        offsets.push(i32::try_from(bytes.len()).unwrap());
    }
    let owner = Arc::new(Owner { bytes, drops });
    let ptr = NonNull::new(owner.bytes.as_ptr().cast_mut()).unwrap();
    let len = owner.bytes.len();
    // SAFETY: the owner keeps the unchanged Vec allocation alive for every
    // Buffer clone/slice. Its normal Drop releases Vec exactly once; zero length
    // still uses Vec's nonnull dangling pointer and grants no readable bytes.
    let payload = unsafe { Buffer::from_custom_allocation(ptr, len, owner) };
    make_offsets(ByteLayout::Utf8, Buffer::from_vec(offsets), payload, None).unwrap()
}

pub(crate) fn rewrap_dictionary(
    keys: &arrow_array::Int32Array,
    values: ArrayRef,
    validity: Option<NullBuffer>,
) -> Result<ArrayRef, ValueError> {
    use arrow_array::{DictionaryArray, Int32Array, types::Int32Type};
    let keys = Int32Array::new(keys.values().clone(), validity);
    Ok(Arc::new(DictionaryArray::<Int32Type>::try_new(
        keys, values,
    )?))
}

pub(crate) fn byte_declared_backing_capacity(array: &dyn Array) -> Result<usize, ValueError> {
    let node = borrowed_node(array)?;
    let mut capacity = 0usize;
    for index in 0..node.data_buffers {
        capacity = capacity.saturating_add(borrowed_buffer(array, index)?.capacity());
    }
    if let Some(nulls) = array.nulls() {
        capacity = capacity.saturating_add(nulls.inner().inner().capacity());
    }
    Ok(capacity)
}

/// Explicit publication copy for foreign allocators. Global keeps standard
/// zero-copy Arrow ownership; the return count describes actual copied bytes.
pub(crate) fn into_arrow_buffer_observed<T: ArrowNativeType, A: Allocator + 'static, E>(
    values: AllocVec<T, A>,
    growth: &mut crate::control::Growth<'_, E>,
    work: &mut crate::control::Work<'_, E>,
) -> Result<(Buffer, usize), crate::ValueFailure<E>> {
    if TypeId::of::<A>() == TypeId::of::<Global>() {
        return Ok((into_arrow_buffer(values), 0));
    }
    let layout = std::alloc::Layout::array::<T>(values.len()).map_err(|_| {
        ValueError::Representation(crate::error::field_representation(
            11,
            RepresentationBoundary::SingleOutput,
            None,
            Some(isize::MAX as u64),
            None,
        ))
    })?;
    if layout.size() != 0 {
        growth(layout).map_err(crate::ValueFailure::Caller)?;
    }
    let mut output = Vec::new();
    output
        .try_reserve_exact(values.len())
        .map_err(|source| ValueError::Allocation {
            requested: layout.size(),
            source,
        })?;
    let turn = (64 * 1024 / std::mem::size_of::<T>().max(1)).max(1);
    for chunk in values.chunks(turn) {
        work(std::mem::size_of_val(chunk)).map_err(crate::ValueFailure::Caller)?;
        output.extend_from_slice(chunk);
    }
    Ok((Buffer::from_vec(output), layout.size()))
}

pub(crate) fn byte_payload_capacity(array: &dyn Array) -> Result<u128, ValueError> {
    match ByteLayout::from_data_type(array.data_type())
        .ok_or(ValueError::Invariant(ValueViolation::Layout))?
    {
        ByteLayout::Utf8View | ByteLayout::BinaryView => Ok(view_parts(array)?
            .1
            .iter()
            .map(|b| b.capacity() as u128)
            .sum()),
        _ => Ok(borrowed_buffer(array, 1)?.capacity() as u128),
    }
}

pub(crate) fn view_external_location(
    array: &dyn Array,
    row: usize,
) -> Result<Option<(usize, usize, usize)>, ValueError> {
    if row >= array.len() {
        return Err(ValueError::Invariant(ValueViolation::Bounds));
    }
    if array.is_null(row) {
        return Ok(None);
    }
    let (views, buffers) = view_parts(array)?;
    let descriptor = ByteView::from(views[row]);
    if descriptor.length <= 12 {
        return Ok(None);
    }
    let index = descriptor.buffer_index as usize;
    let offset = descriptor.offset as usize;
    let len = descriptor.length as usize;
    let buffer = buffers
        .get(index)
        .ok_or(ValueError::Invariant(ValueViolation::Bounds))?;
    if offset.checked_add(len).is_none_or(|end| end > buffer.len()) {
        return Err(ValueError::Invariant(ValueViolation::Bounds));
    }
    Ok(Some((index, offset, len)))
}

/// The binding owner has proved every physical NULL unreachable beneath an
/// inactive ancestor. Rebuild safely over the same storage; removing a bitmap
/// never changes a descendant's separately computed logical reachability.
pub(crate) fn without_nulls(source: &ArrayRef) -> Result<ArrayRef, ValueError> {
    let data = source.to_data().into_builder().nulls(None).build()?;
    Ok(arrow_array::make_array(data))
}

/// A borrowed view storage binding. Carrier identification and downcast happen
/// once, while row reads retain checked Arrow geometry and logical validity.
#[derive(Clone, Copy)]
pub(crate) struct BoundView<'a> {
    views: &'a [u128],
    buffers: &'a [Buffer],
    nulls: Option<&'a NullBuffer>,
}
impl<'a> BoundView<'a> {
    pub(crate) fn bind(array: &'a dyn Array) -> Result<Self, ValueError> {
        let (views, buffers) = view_parts(array)?;
        Ok(Self {
            views,
            buffers,
            nulls: array.nulls(),
        })
    }
    pub(crate) fn value(self, row: usize) -> Result<Option<&'a [u8]>, ValueError> {
        let raw = self
            .views
            .get(row)
            .ok_or(ValueError::Invariant(ValueViolation::Bounds))?;
        if self.nulls.is_some_and(|nulls| nulls.is_null(row)) {
            return Ok(None);
        }
        let view = ByteView::from(*raw);
        let len = view.length as usize;
        if len <= 12 {
            // SAFETY: raw borrows a descriptor from the already admitted view
            // array. Its inline extent is exactly its checked length <= 12;
            // the returned slice borrows that descriptor, never a temporary.
            return Ok(Some(unsafe { BinaryViewArray::inline_value(raw, len) }));
        }
        let data = self
            .buffers
            .get(view.buffer_index as usize)
            .ok_or(ValueError::Invariant(ValueViolation::Bounds))?;
        let start = view.offset as usize;
        let end = start
            .checked_add(len)
            .ok_or(ValueError::Invariant(ValueViolation::Bounds))?;
        Ok(Some(
            data.as_slice()
                .get(start..end)
                .ok_or(ValueError::Invariant(ValueViolation::Bounds))?,
        ))
    }
    pub(crate) fn prefix(self, row: usize) -> Result<[u8; 4], ValueError> {
        let raw = self
            .views
            .get(row)
            .ok_or(ValueError::Invariant(ValueViolation::Bounds))?;
        Ok(ByteView::from(*raw).prefix.to_le_bytes())
    }
}

/// Immutable view parts retain the actual Arrow backing without a self borrow.
/// Cloning buffer handles does not copy payload or grant allocation authority.
pub(crate) struct OwnedView {
    views: ScalarBuffer<u128>,
    buffers: Vec<Buffer>,
    nulls: Option<NullBuffer>,
    utf8: bool,
}
impl OwnedView {
    pub(crate) fn bind_observed<E, G, W>(
        array: &dyn Array,
        growth: &mut G,
        work: &mut W,
    ) -> Result<Self, crate::ValueFailure<E>>
    where
        G: FnMut(std::alloc::Layout) -> Result<(), E>,
        W: FnMut(usize) -> Result<(), E>,
    {
        let (views, source_buffers) = view_parts(array)?;
        work(0).map_err(crate::ValueFailure::Caller)?;
        let mut buffers = Vec::new();
        let layout = std::alloc::Layout::array::<Buffer>(source_buffers.len())
            .map_err(|_| ValueError::Invariant(ValueViolation::Bounds))?;
        if !source_buffers.is_empty() {
            growth(layout).map_err(crate::ValueFailure::Caller)?;
            buffers
                .try_reserve_exact(source_buffers.len())
                .map_err(|source| ValueError::Allocation {
                    requested: layout.size(),
                    source,
                })?;
        }
        // Initialize retained handles in bounded metadata blocks after the
        // caller admits the complete allocation. Payload is never copied.
        for block in source_buffers.chunks(1024) {
            work(std::mem::size_of_val(block)).map_err(crate::ValueFailure::Caller)?;
            buffers.extend_from_slice(block);
        }
        Ok(Self {
            views: views.clone(),
            buffers,
            nulls: array.nulls().cloned(),
            utf8: matches!(array.data_type(), arrow_schema::DataType::Utf8View),
        })
    }
    pub(crate) fn len(&self) -> usize {
        self.views.len()
    }
    pub(crate) fn is_utf8(&self) -> bool {
        self.utf8
    }
    pub(crate) fn view(&self) -> BoundView<'_> {
        BoundView {
            views: &self.views,
            buffers: &self.buffers,
            nulls: self.nulls.as_ref(),
        }
    }
}
