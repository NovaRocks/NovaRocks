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

//! The original canonical component decoder under caller construction ports.
//! Tests use the same parser through a private resource-free fixture context.
//! Result custody remains with the actual published-array owner.

use super::{
    EncodedCursor, GroupKeyOutputKind, GroupKeyOutputTally, group_key_decode_arrow,
    group_key_decode_utf8, group_key_decoding_contract, group_key_request_arithmetic,
};
use crate::{
    exec::{chunk::ArrayNormalizationStep, pipeline::operator::InputConstructionOwner},
    runtime::fragment::{ExecutionFailure, ExecutionResult},
};
use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, Decimal256Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, ListArray, MapArray,
    NullArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, Fields, TimeUnit};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer, i256};
use novarocks_functions::{
    KernelEvaluationControl,
    selected_copy::{
        CopyConstructionError, preflight_arrow_node_structure_construction,
        preflight_fixed_binary_constructor_extent,
    },
};
use novarocks_type_contract::{ValueSchemaCreation, ValueTypeStorageLifetime};
use novarocks_value_arrays::{
    ByteLayout, ValueError, ValueFailure, ValueViolation,
    build::{ValueBuilder, WriteFailure},
    control::ValueStructure,
};
use std::{alloc::Layout, sync::Arc};

const COPY_BLOCK: usize = 64 * 1024;

/// Independent codec expectations share the parser without exposing another
/// production construction path or granting caller capacity.
#[cfg(test)]
struct TestResources;
struct Governed<'a> {
    owner: &'a dyn InputConstructionOwner,
    control: &'a dyn KernelEvaluationControl,
}
trait DecodeResources {
    fn checkpoint(&self) -> ExecutionResult<()>;
    fn request(&self, layout: Layout) -> ExecutionResult<()>;
    fn opaque(&self, bytes: usize) -> ExecutionResult<()>;
    fn storage(&self) -> ValueTypeStorageLifetime;
    fn structure(&self, event: ValueStructure) -> ExecutionResult<()>;
}
#[cfg(test)]
impl DecodeResources for TestResources {
    fn checkpoint(&self) -> ExecutionResult<()> {
        Ok(())
    }
    fn request(&self, _: Layout) -> ExecutionResult<()> {
        Ok(())
    }
    fn opaque(&self, _: usize) -> ExecutionResult<()> {
        Ok(())
    }
    fn storage(&self) -> ValueTypeStorageLifetime {
        ValueTypeStorageLifetime::pure()
    }
    fn structure(&self, _: ValueStructure) -> ExecutionResult<()> {
        Ok(())
    }
}
impl DecodeResources for Governed<'_> {
    fn checkpoint(&self) -> ExecutionResult<()> {
        self.control.checkpoint(0).map_err(ExecutionFailure::from)
    }
    fn request(&self, layout: Layout) -> ExecutionResult<()> {
        self.owner.request(layout).map_err(ExecutionFailure::from)
    }
    fn opaque(&self, bytes: usize) -> ExecutionResult<()> {
        self.owner
            .observe_opaque_upper_bound(bytes)
            .map_err(ExecutionFailure::from)
    }
    fn storage(&self) -> ValueTypeStorageLifetime {
        self.owner.storage_lifetime()
    }
    fn structure(&self, event: ValueStructure) -> ExecutionResult<()> {
        self.owner
            .observe(
                0,
                &[],
                ArrayNormalizationStep::ValueStructure { path: &[], event },
            )
            .map_err(ExecutionFailure::from)
    }
}

#[cfg(test)]
pub(crate) fn decode_group_key_rows(
    data_type: &DataType,
    rows: &[Option<&[u8]>],
) -> ExecutionResult<ArrayRef> {
    decode(data_type, rows, &TestResources)
}

/// Decode the original non-NULL component payloads under the actual caller's
/// construction and control ports. Complete value binding and the signed
/// output layout are checked by that caller, never inferred from these bytes.
/// The returned array must keep the caller's finite creator when published.
pub(crate) fn decode_group_key_rows_construction(
    data_type: &DataType,
    rows: &[Option<&[u8]>],
    owner: &dyn InputConstructionOwner,
    control: &dyn KernelEvaluationControl,
) -> ExecutionResult<ArrayRef> {
    decode(data_type, rows, &Governed { owner, control })
}

fn vector<T>(count: usize, resource: &impl DecodeResources) -> ExecutionResult<Vec<T>> {
    resource.checkpoint()?;
    if count != 0 {
        resource.request(Layout::array::<T>(count).map_err(|_| group_key_request_arithmetic())?)?;
    }
    let mut values = Vec::new();
    values.try_reserve_exact(count).map_err(|source| {
        ExecutionFailure::from_domain(ValueError::Allocation {
            requested: count.saturating_mul(size_of::<T>()),
            source,
        })
    })?;
    resource.checkpoint()?;
    Ok(values)
}
fn copy_error(cause: CopyConstructionError<ExecutionFailure>) -> ExecutionFailure {
    match cause {
        CopyConstructionError::Creator(cause) => cause,
        CopyConstructionError::Resource(cause) => {
            crate::exec::pipeline::operator::ConstructionFailure::Resource(cause).into()
        }
        CopyConstructionError::Copy(cause) => cause.into_kernel().into(),
    }
}
fn node(ty: &DataType, has_nulls: bool, resource: &impl DecodeResources) -> ExecutionResult<()> {
    let mut request = |layout| resource.request(layout);
    let mut opaque = |bytes| resource.opaque(bytes);
    let mut creation = ValueSchemaCreation::new(resource.storage(), &mut request, &mut opaque);
    preflight_arrow_node_structure_construction(ty, has_nulls, &mut creation, &mut |_| {
        resource.checkpoint()
    })
    .map_err(copy_error)
}
fn original(cause: ValueFailure<ExecutionFailure>) -> ExecutionFailure {
    match cause {
        ValueFailure::Caller(cause) => cause,
        ValueFailure::Value(cause) => ExecutionFailure::from_domain(cause),
    }
}
fn bit_capacity(rows: usize) -> ExecutionResult<usize> {
    rows.checked_add(7)
        .map(|n| n / 8)
        .ok_or_else(group_key_request_arithmetic)
}
/// Build exactly the selected logical validity with no uninterruptible bitmap
/// initialization/popcount. Hidden/padding bits do not contribute to the count.
struct Validity {
    bits: Vec<u8>,
    len: usize,
    null_count: usize,
}
fn validity(
    rows: &[Option<&[u8]>],
    resource: &impl DecodeResources,
) -> ExecutionResult<Option<Validity>> {
    let mut bits = vector::<u8>(bit_capacity(rows.len())?, resource)?;
    let mut null_count = 0;
    for block in rows.chunks(8) {
        resource.checkpoint()?;
        let mut byte = 0;
        for (i, row) in block.iter().enumerate() {
            if row.is_some() {
                byte |= 1 << i;
            } else {
                null_count += 1;
            }
        }
        bits.push(byte);
    }
    resource.checkpoint()?;
    if null_count == 0 {
        return Ok(None);
    }
    Ok(Some(Validity {
        bits,
        len: rows.len(),
        null_count,
    }))
}
/// Publish only after this actual node's metadata request has succeeded.
fn publish_validity(validity: Option<Validity>) -> Option<NullBuffer> {
    validity.map(
        |Validity {
             bits,
             len,
             null_count,
         }| {
            // SAFETY: validity() sets each of exactly len bits iff its row is Some;
            // null_count counts precisely the complementary None rows. Padding is
            // excluded by the BooleanBuffer's logical length.
            unsafe {
                NullBuffer::new_unchecked(
                    BooleanBuffer::new(Buffer::from_vec(bits), 0, len),
                    null_count,
                )
            }
        },
    )
}

fn fixed_values<T: Default>(
    rows: &[Option<&[u8]>],
    marker: u8,
    width: usize,
    resource: &impl DecodeResources,
    decode_value: impl Fn(&[u8]) -> ExecutionResult<T>,
) -> ExecutionResult<Vec<T>> {
    let mut values = vector(rows.len(), resource)?;
    for block in rows.chunks(256) {
        resource.checkpoint()?;
        for row in block {
            values.push(match row {
                Some(row) => decode_fixed_value(row, marker, width, &decode_value)?,
                None => T::default(),
            });
        }
    }
    resource.checkpoint()?;
    Ok(values)
}
fn decode(
    ty: &DataType,
    rows: &[Option<&[u8]>],
    resource: &impl DecodeResources,
) -> ExecutionResult<ArrayRef> {
    resource.checkpoint()?;
    macro_rules! fixed_array {
        ($marker:expr, $size:expr, $array:ty, $decode:expr) => {{
            let values = fixed_values(rows, $marker, $size, resource, $decode)?;
            let nulls = validity(rows, resource)?;
            node(ty, nulls.is_some(), resource)?;
            let nulls = publish_validity(nulls);
            let result = <$array>::try_new(ScalarBuffer::from(values), nulls)
                .map_err(group_key_decode_arrow)?;
            resource.checkpoint()?;
            Ok(Arc::new(result) as ArrayRef)
        }};
    }
    match ty {
        DataType::Null => {
            for block in rows.chunks(256) {
                resource.checkpoint()?;
                if block.iter().any(Option::is_some) {
                    return Err(group_key_decoding_contract(
                        "canonical Null group-key contains a non-null value",
                    ));
                }
            }
            node(ty, false, resource)?;
            Ok(Arc::new(NullArray::new(rows.len())))
        }
        DataType::Boolean => {
            let mut values = vector::<u8>(bit_capacity(rows.len())?, resource)?;
            for block in rows.chunks(8) {
                resource.checkpoint()?;
                let mut bits = 0;
                for (i, row) in block.iter().enumerate() {
                    if let Some(row) = row
                        && decode_fixed_value(row, 1, 1, |b| Ok(b[0] != 0))?
                    {
                        bits |= 1 << i;
                    }
                }
                values.push(bits);
            }
            let nulls = validity(rows, resource)?;
            node(ty, nulls.is_some(), resource)?;
            let nulls = publish_validity(nulls);
            let result = BooleanArray::new(
                BooleanBuffer::new(Buffer::from_vec(values), 0, rows.len()),
                nulls,
            );
            resource.checkpoint()?;
            Ok(Arc::new(result))
        }
        DataType::Int8 => fixed_array!(2, 1, Int8Array, |b: &[u8]| Ok(b[0] as i8)),
        DataType::Int16 => fixed_array!(3, 2, Int16Array, |b: &[u8]| Ok(i16::from_le_bytes(
            b.try_into().expect("i16 bytes")
        ))),
        DataType::Int32 => fixed_array!(4, 4, Int32Array, |b: &[u8]| Ok(i32::from_le_bytes(
            b.try_into().expect("i32 bytes")
        ))),
        DataType::Int64 => fixed_array!(5, 8, Int64Array, |b: &[u8]| Ok(i64::from_le_bytes(
            b.try_into().expect("i64 bytes")
        ))),
        DataType::Float32 => fixed_array!(6, 4, Float32Array, |b: &[u8]| Ok(f32::from_bits(
            u32::from_le_bytes(b.try_into().expect("f32 bytes"))
        ))),
        DataType::Float64 => fixed_array!(7, 8, Float64Array, |b: &[u8]| Ok(f64::from_bits(
            u64::from_le_bytes(b.try_into().expect("f64 bytes"))
        ))),
        DataType::Date32 => fixed_array!(9, 4, Date32Array, |b: &[u8]| Ok(i32::from_le_bytes(
            b.try_into().expect("date32 bytes")
        ))),
        DataType::Timestamp(unit, timezone) => {
            let marker = match unit {
                TimeUnit::Second => 10,
                TimeUnit::Millisecond => 11,
                TimeUnit::Microsecond => 12,
                TimeUnit::Nanosecond => 13,
            };
            let values = fixed_values(rows, marker, 8, resource, |b| {
                Ok(i64::from_le_bytes(b.try_into().expect("timestamp bytes")))
            })?;
            let nulls = validity(rows, resource)?;
            node(ty, nulls.is_some(), resource)?;
            let nulls = publish_validity(nulls);
            let result: ArrayRef = match unit {
                TimeUnit::Second => Arc::new(
                    TimestampSecondArray::new(values.into(), nulls)
                        .with_timezone_opt(timezone.clone()),
                ),
                TimeUnit::Millisecond => Arc::new(
                    TimestampMillisecondArray::new(values.into(), nulls)
                        .with_timezone_opt(timezone.clone()),
                ),
                TimeUnit::Microsecond => Arc::new(
                    TimestampMicrosecondArray::new(values.into(), nulls)
                        .with_timezone_opt(timezone.clone()),
                ),
                TimeUnit::Nanosecond => Arc::new(
                    TimestampNanosecondArray::new(values.into(), nulls)
                        .with_timezone_opt(timezone.clone()),
                ),
            };
            resource.checkpoint()?;
            Ok(result)
        }
        DataType::Decimal128(precision, scale) => {
            let values = fixed_values(rows, 14, 16, resource, |b| {
                Ok(i128::from_le_bytes(b.try_into().expect("decimal128 bytes")))
            })?;
            let nulls = validity(rows, resource)?;
            node(ty, nulls.is_some(), resource)?;
            let nulls = publish_validity(nulls);
            let result = Decimal128Array::new(values.into(), nulls)
                .with_precision_and_scale(*precision, *scale)
                .map_err(group_key_decode_arrow)?;
            resource.checkpoint()?;
            Ok(Arc::new(result))
        }
        DataType::Decimal256(precision, scale) => {
            let values = fixed_values(rows, 19, 32, resource, |b| {
                Ok(i256::from_le_bytes(b.try_into().expect("decimal256 bytes")))
            })?;
            let nulls = validity(rows, resource)?;
            node(ty, nulls.is_some(), resource)?;
            let nulls = publish_validity(nulls);
            let result = Decimal256Array::new(values.into(), nulls)
                .with_precision_and_scale(*precision, *scale)
                .map_err(group_key_decode_arrow)?;
            resource.checkpoint()?;
            Ok(Arc::new(result))
        }
        DataType::FixedSizeBinary(16) => {
            preflight_fixed_binary_constructor_extent(16, rows.len())
                .map_err(|e| ExecutionFailure::from(e.into_kernel()))?;
            let mut values = vector::<u8>(
                rows.len()
                    .checked_mul(16)
                    .ok_or_else(group_key_request_arithmetic)?,
                resource,
            )?;
            for block in rows.chunks(256) {
                resource.checkpoint()?;
                for row in block {
                    let value = match row {
                        Some(row) => decode_fixed_value(row, 15, 16, |b| {
                            Ok(i128::from_le_bytes(b.try_into().expect("largeint bytes")))
                        })?,
                        None => 0,
                    };
                    values
                        .extend_from_slice(&novarocks_functions::largeint::i128_to_be_bytes(value));
                }
            }
            let nulls = validity(rows, resource)?;
            node(ty, nulls.is_some(), resource)?;
            let nulls = publish_validity(nulls);
            let result = FixedSizeBinaryArray::try_new(16, Buffer::from_vec(values), nulls)
                .map_err(group_key_decode_arrow)?;
            resource.checkpoint()?;
            Ok(Arc::new(result))
        }
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::BinaryView
        | DataType::LargeBinary => bytes(ty, rows, resource),
        DataType::List(field) => list(field, rows, resource),
        DataType::Struct(fields) => structure(fields, rows, resource),
        DataType::Map(field, ordered) => map(field, *ordered, rows, resource),
        other => Err(group_key_decoding_contract(format_args!(
            "canonical group-key decode unsupported type: {other:?}"
        ))),
    }
}
fn decode_fixed_value<T>(
    bytes: &[u8],
    marker: u8,
    payload_len: usize,
    decode: impl FnOnce(&[u8]) -> ExecutionResult<T>,
) -> ExecutionResult<T> {
    if bytes.first().copied() != Some(marker) || bytes.len() != payload_len.saturating_add(1) {
        return Err(group_key_decoding_contract(
            "invalid canonical fixed-width group key",
        ));
    }
    decode(&bytes[1..])
}
fn decode_variable_value(bytes: &[u8], marker: u8) -> ExecutionResult<&[u8]> {
    let mut cursor = EncodedCursor::new(bytes);
    cursor.expect_marker(marker)?;
    let len = cursor.read_u32()? as usize;
    let value = cursor.read_slice(len)?;
    cursor.finish()?;
    Ok(value)
}
fn utf8<'a>(bytes: &'a [u8], resource: &impl DecodeResources) -> ExecutionResult<&'a str> {
    let mut start = 0;
    while start < bytes.len() {
        resource.checkpoint()?;
        let end = start.saturating_add(COPY_BLOCK).min(bytes.len());
        match std::str::from_utf8(&bytes[start..end]) {
            Ok(_) => start = end,
            Err(e) if e.error_len().is_none() && end < bytes.len() => {
                // At most three bytes of an incomplete code point are retried
                // with the next bounded block; no validated prefix is rescanned.
                if e.valid_up_to() == 0 {
                    return Err(group_key_decode_utf8(e));
                }
                start += e.valid_up_to();
            }
            Err(e) => return Err(group_key_decode_utf8(e)),
        }
    }
    resource.checkpoint()?;
    // SAFETY: the standard validator accepted each contiguous complete prefix;
    // any incomplete suffix was retried with the next block. Reaching len means
    // every byte belongs to exactly one accepted UTF8 sequence.
    Ok(unsafe { std::str::from_utf8_unchecked(bytes) })
}
fn bytes(
    ty: &DataType,
    rows: &[Option<&[u8]>],
    resource: &impl DecodeResources,
) -> ExecutionResult<ArrayRef> {
    let (marker, kind) = match ty {
        DataType::Utf8 => (8, Some(GroupKeyOutputKind::Utf8)),
        DataType::LargeUtf8 | DataType::Utf8View => (8, None),
        DataType::Binary => (20, Some(GroupKeyOutputKind::Binary)),
        DataType::BinaryView => (20, None),
        DataType::LargeBinary => (21, None),
        _ => unreachable!("canonical bytes branch"),
    };
    // Header-only size facts precede all output builder allocations. The real
    // signed offsets limit stays a representation boundary, never a quota.
    // The canonical marker is independent of the actual output layout: view
    // and Int64-offset outputs must not inherit an Int32 total-payload gate.
    // Their actual constructor extents remain with ValueBuilder.
    if let Some(kind) = kind {
        let mut tally = GroupKeyOutputTally::default();
        for row in rows {
            resource.checkpoint()?;
            if let Some(row) = row {
                tally.add(decode_variable_value(row, marker)?.len())?;
            }
        }
        tally.finish(kind)?;
    }
    let layout = ByteLayout::from_data_type(ty).expect("canonical byte layout");
    let mut builder = ValueBuilder::new(layout);
    for block in rows.chunks(256) {
        resource.checkpoint()?;
        for row in block {
            let mut growth = |layout| resource.request(layout);
            let mut work = |_| resource.checkpoint();
            let result = match row {
                None => builder.append_null(&mut growth, &mut work),
                Some(row) => {
                    let value = decode_variable_value(row, marker)?;
                    if layout.is_utf8() {
                        builder.append_str(utf8(value, resource)?, &mut growth, &mut work)
                    } else {
                        builder.append_bytes(value, &mut growth, &mut work)
                    }
                }
            };
            match result {
                Ok(()) => {}
                Err(WriteFailure::Failure(cause)) => return Err(original(cause)),
                Err(WriteFailure::Full) => {
                    return Err(ExecutionFailure::from_domain(ValueError::Invariant(
                        ValueViolation::RowTransaction,
                    )));
                }
            }
        }
    }
    let built = builder
        .finish_structured_observed(
            &mut |layout| resource.request(layout),
            &mut |_| resource.checkpoint(),
            &mut |event| resource.structure(event),
        )
        .map_err(original)?;
    resource.checkpoint()?;
    Ok(built.array)
}
fn nested_count(
    rows: &[Option<&[u8]>],
    marker: u8,
    kind: GroupKeyOutputKind,
    resource: &impl DecodeResources,
) -> ExecutionResult<usize> {
    let mut tally = GroupKeyOutputTally::default();
    let mut total = 0usize;
    for row in rows {
        resource.checkpoint()?;
        if let Some(row) = row {
            let mut cursor = EncodedCursor::new(row);
            cursor.expect_marker(marker)?;
            let count = cursor.read_u32()? as usize;
            total = total
                .checked_add(count)
                .ok_or_else(group_key_request_arithmetic)?;
            tally.add(count)?;
        }
    }
    tally.finish(kind)?;
    Ok(total)
}
fn list(
    field: &Arc<arrow::datatypes::Field>,
    rows: &[Option<&[u8]>],
    resource: &impl DecodeResources,
) -> ExecutionResult<ArrayRef> {
    let count = nested_count(rows, 16, GroupKeyOutputKind::List, resource)?;
    let mut offsets = vector::<i32>(
        rows.len()
            .checked_add(1)
            .ok_or_else(group_key_request_arithmetic)?,
        resource,
    )?;
    let mut children = vector(count, resource)?;
    offsets.push(0);
    for row in rows {
        resource.checkpoint()?;
        if let Some(row) = row {
            let mut cursor = EncodedCursor::new(row);
            cursor.expect_marker(16)?;
            let count = cursor.read_u32()? as usize;
            for _ in 0..count {
                resource.checkpoint()?;
                children.push(cursor.read_nested()?);
            }
            cursor.finish()?;
        }
        offsets
            .push(i32::try_from(children.len()).expect("preflighted canonical list output extent"));
    }
    let child = decode(field.data_type(), &children, resource)?;
    let nulls = validity(rows, resource)?;
    node(
        &DataType::List(Arc::clone(field)),
        nulls.is_some(),
        resource,
    )?;
    let nulls = publish_validity(nulls);
    resource.checkpoint()?;
    // OffsetBuffer validation and Arrow's final immutable node constructor are
    // indivisible upstream calls. All parser/allocation/copy loops above remain
    // governed; no claim is made of callbacks inside these library operations.
    let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
    let result = ListArray::try_new(Arc::clone(field), offsets, child, nulls)
        .map_err(group_key_decode_arrow)?;
    resource.checkpoint()?;
    Ok(Arc::new(result))
}
/// The original node envelope includes exactly this retained child-reference
/// table as well as its opaque Arrow headers. Request it before allocating the
/// table, then do not add a second invoice for the same Vec backing.
fn struct_children(
    fields: &Fields,
    has_nulls: bool,
    resource: &impl DecodeResources,
) -> ExecutionResult<Vec<ArrayRef>> {
    node(&DataType::Struct(fields.clone()), has_nulls, resource)?;
    resource.checkpoint()?;
    let mut arrays = Vec::new();
    arrays.try_reserve_exact(fields.len()).map_err(|source| {
        ExecutionFailure::from_domain(ValueError::Allocation {
            requested: fields.len().saturating_mul(size_of::<ArrayRef>()),
            source,
        })
    })?;
    resource.checkpoint()?;
    Ok(arrays)
}

fn structure(
    fields: &Fields,
    rows: &[Option<&[u8]>],
    resource: &impl DecodeResources,
) -> ExecutionResult<ArrayRef> {
    let mut columns = vector::<Vec<Option<&[u8]>>>(fields.len(), resource)?;
    for _ in fields {
        resource.checkpoint()?;
        columns.push(vector(rows.len(), resource)?);
    }
    for row in rows {
        resource.checkpoint()?;
        match row {
            None => {
                for column in &mut columns {
                    resource.checkpoint()?;
                    column.push(None);
                }
            }
            Some(row) => {
                let mut cursor = EncodedCursor::new(row);
                cursor.expect_marker(17)?;
                if cursor.read_u32()? as usize != fields.len() {
                    return Err(group_key_decoding_contract(
                        "canonical struct group-key field count mismatch",
                    ));
                }
                for column in &mut columns {
                    resource.checkpoint()?;
                    column.push(cursor.read_nested()?);
                }
                cursor.finish()?;
            }
        }
    }
    let nulls = validity(rows, resource)?;
    let mut arrays = struct_children(fields, nulls.is_some(), resource)?;
    for (field, column) in fields.iter().zip(columns) {
        resource.checkpoint()?;
        arrays.push(decode(field.data_type(), &column, resource)?);
    }
    let nulls = publish_validity(nulls);
    resource.checkpoint()?;
    // The exact upstream constructor retains required-field null implication
    // validation. Its own bitmap comparison has no callback in Arrow 58.4.
    let result = StructArray::try_new_with_length(fields.clone(), arrays, nulls, rows.len())
        .map_err(group_key_decode_arrow)?;
    resource.checkpoint()?;
    Ok(Arc::new(result))
}
fn map(
    field: &Arc<arrow::datatypes::Field>,
    ordered: bool,
    rows: &[Option<&[u8]>],
    resource: &impl DecodeResources,
) -> ExecutionResult<ArrayRef> {
    let DataType::Struct(fields) = field.data_type() else {
        return Err(group_key_decoding_contract(
            "map group key requires struct entry type",
        ));
    };
    if fields.len() != 2 {
        return Err(group_key_decoding_contract(
            "map group key requires key/value entry fields",
        ));
    }
    let count = nested_count(rows, 18, GroupKeyOutputKind::Map, resource)?;
    let mut offsets = vector::<i32>(
        rows.len()
            .checked_add(1)
            .ok_or_else(group_key_request_arithmetic)?,
        resource,
    )?;
    let mut keys = vector(count, resource)?;
    let mut values = vector(count, resource)?;
    offsets.push(0);
    for row in rows {
        resource.checkpoint()?;
        if let Some(row) = row {
            let mut cursor = EncodedCursor::new(row);
            cursor.expect_marker(18)?;
            let count = cursor.read_u32()? as usize;
            for _ in 0..count {
                resource.checkpoint()?;
                keys.push(cursor.read_nested()?);
                values.push(cursor.read_nested()?);
            }
            cursor.finish()?;
        }
        offsets.push(i32::try_from(keys.len()).expect("preflighted canonical map output extent"));
    }
    let key = decode(fields[0].data_type(), &keys, resource)?;
    let value = decode(fields[1].data_type(), &values, resource)?;
    let mut arrays = struct_children(fields, false, resource)?;
    arrays.push(key);
    arrays.push(value);
    resource.checkpoint()?;
    let entries = StructArray::try_new_with_length(fields.clone(), arrays, None, count)
        .map_err(group_key_decode_arrow)?;
    let nulls = validity(rows, resource)?;
    node(
        &DataType::Map(Arc::clone(field), ordered),
        nulls.is_some(),
        resource,
    )?;
    let nulls = publish_validity(nulls);
    resource.checkpoint()?;
    let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
    let result = MapArray::try_new(Arc::clone(field), offsets, entries, nulls, ordered)
        .map_err(group_key_decode_arrow)?;
    resource.checkpoint()?;
    Ok(Arc::new(result))
}

#[cfg(test)]
#[path = "decode_tests.rs"]
mod tests;
