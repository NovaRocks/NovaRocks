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
    OutputPrefix, OutputRows, allocate, arithmetic,
    column::{ByteInput, Column, Shape},
};
use crate::{
    ByteLayout, ValueError, ValueFailure, ValueViolation, arrow_raw,
    build::{ValueBuilder, WriteFailure},
    control::{Growth, Work},
    hydrate::child_rows,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, DictionaryArray, FixedSizeListArray, Int32Array, LargeListArray,
    ListArray, MapArray, StructArray, make_array, new_empty_array, types::Int32Type,
};
use arrow_buffer::{
    BooleanBuffer, MutableBuffer, NullBuffer, OffsetBuffer, ScalarBuffer, alloc::ALIGNMENT,
};
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType};
use std::{alloc::Layout, sync::Arc};

#[derive(Clone, Copy)]
enum Rows<'a> {
    Constant(usize),
    Source {
        map: &'a OutputRows<'a>,
        start: usize,
        len: usize,
    },
    Child(&'a [Option<usize>]),
}

impl Rows<'_> {
    fn len(self) -> usize {
        match self {
            Self::Constant(len) | Self::Source { len, .. } => len,
            Self::Child(rows) => rows.len(),
        }
    }

    fn at(self, ordinal: usize) -> Result<Option<usize>, ValueError> {
        match self {
            Self::Constant(_) => Ok(Some(0)),
            Self::Source { map, start, .. } => Ok(Some(map.get(start + ordinal)?)),
            Self::Child(rows) => Ok(rows[ordinal]),
        }
    }
}

pub(super) fn build_prefix<E>(
    prefix: &OutputPrefix<'_>,
    growth: &mut Growth<'_, E>,
    work: &mut Work<'_, E>,
) -> Result<Vec<ArrayRef>, ValueFailure<E>> {
    let mut output = allocate(prefix.columns.len(), growth)?;
    for column in prefix.columns {
        work(1).map_err(ValueFailure::Caller)?;
        let array = if column.borrow {
            let OutputRows::Contiguous(range) = prefix.row_map else {
                return Err(transaction().into());
            };
            column.source.slice(range.start + prefix.start, prefix.len)
        } else {
            let rows = if column.constant {
                Rows::Constant(prefix.len)
            } else {
                Rows::Source {
                    map: prefix.row_map,
                    start: prefix.start,
                    len: prefix.len,
                }
            };
            build_column(column, rows, growth, work)?
        };
        output.push(array);
    }
    work(0).map_err(ValueFailure::Caller)?;
    Ok(output)
}

fn build_column<E>(
    column: &Column,
    rows: Rows<'_>,
    growth: &mut Growth<'_, E>,
    work: &mut Work<'_, E>,
) -> Result<ArrayRef, ValueFailure<E>> {
    if rows.len() != column.committed.rows {
        return Err(transaction().into());
    }
    if rows.len() == 0 {
        return Ok(new_empty_array(&column.target));
    }
    match column.shape {
        Shape::Bytes { source, target } => build_bytes(column, rows, source, target, growth, work),
        Shape::Dictionary(_) => {
            let (keys, values) = arrow_raw::source_dictionary(&column.source)?;
            let keys = build_fixed(
                keys,
                rows,
                Shape::Fixed(4),
                column.committed.nulls,
                growth,
                work,
            )?;
            let keys = keys
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(transaction)?;
            let dictionary =
                DictionaryArray::<Int32Type>::try_new(keys.clone(), Arc::clone(values))
                    .map_err(ValueError::from)?;
            Ok(Arc::new(dictionary))
        }
        Shape::Null | Shape::Boolean | Shape::Fixed(_) => build_fixed(
            column.source.as_ref(),
            rows,
            column.shape,
            column.committed.nulls,
            growth,
            work,
        ),
        Shape::Struct | Shape::List(_) | Shape::FixedList(_) | Shape::Map => {
            build_container(column, rows, growth, work)
        }
    }
}

fn build_bytes<E>(
    column: &Column,
    rows: Rows<'_>,
    source: ByteInput,
    target: ByteLayout,
    growth: &mut Growth<'_, E>,
    work: &mut Work<'_, E>,
) -> Result<ArrayRef, ValueFailure<E>> {
    let variable = match source {
        ByteInput::Variable(layout) => Some(column.byte_source(layout)?),
        ByteInput::Fixed => None,
    };
    let facts = column.committed;
    let payload = if target.is_view() {
        facts.inline_bytes
    } else {
        usize::try_from(facts.logical_bytes).map_err(|_| arithmetic())?
    };
    let mut builder = ValueBuilder::new(target);
    builder.reserve_planned_observed(rows.len(), payload, facts.adopted, growth, work)?;
    for ordinal in 0..rows.len() {
        work(1).map_err(ValueFailure::Caller)?;
        let row = rows.at(ordinal)?;
        let result = match (variable, row) {
            (Some(source), Some(row)) => match source.mapped_row(row)? {
                Some(row) => {
                    builder.append_array_value(source.values().as_ref(), row, growth, work)
                }
                None => builder.append_null(growth, work),
            },
            (None, Some(row)) => match arrow_raw::fixed_binary_span(column.source.as_ref(), row)? {
                Some(span) => builder.append_bytes(span.bytes(), growth, work),
                None => builder.append_null(growth, work),
            },
            (_, None) => builder.append_null(growth, work),
        };
        match result {
            Ok(()) => (),
            Err(WriteFailure::Full) => return Err(transaction().into()),
            Err(WriteFailure::Failure(error)) => return Err(error),
        }
    }
    Ok(builder.finish_observed(growth, work)?.array)
}

fn build_fixed<E>(
    source: &dyn Array,
    rows: Rows<'_>,
    shape: Shape,
    nulls: usize,
    growth: &mut Growth<'_, E>,
    work: &mut Work<'_, E>,
) -> Result<ArrayRef, ValueFailure<E>> {
    let len = rows.len();
    if matches!(shape, Shape::Null) {
        return Ok(Arc::new(arrow_array::NullArray::new(len)));
    }
    let mut validity = validity_buffer(len, nulls, growth)?;
    if matches!(shape, Shape::Boolean) {
        let source = source
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(transaction)?;
        let mut values = zeroed_arrow_buffer(len.div_ceil(8), growth)?;
        for ordinal in 0..len {
            work(1).map_err(ValueFailure::Caller)?;
            if let Some(row) = rows.at(ordinal)?.filter(|&row| source.is_valid(row)) {
                mark_valid(&mut validity, ordinal);
                if source.value(row) {
                    set_bit(&mut values, ordinal);
                }
            }
        }
        return Ok(Arc::new(BooleanArray::new(
            BooleanBuffer::new(values.into(), 0, len),
            finish_validity(validity, len),
        )));
    }

    let Shape::Fixed(width) = shape else {
        return Err(transaction().into());
    };
    let source_values = arrow_raw::borrowed_buffer(source, 0)?;
    let bytes = len.checked_mul(width).ok_or_else(arithmetic)?;
    let mut values = zeroed_arrow_buffer(bytes, growth)?;
    for ordinal in 0..len {
        work(1).map_err(ValueFailure::Caller)?;
        if let Some(row) = rows.at(ordinal)?.filter(|&row| source.is_valid(row)) {
            mark_valid(&mut validity, ordinal);
            let start = row
                .checked_add(source.offset())
                .and_then(|absolute| absolute.checked_mul(width))
                .ok_or_else(arithmetic)?;
            let end = start.checked_add(width).ok_or_else(arithmetic)?;
            let input = source_values
                .as_slice()
                .get(start..end)
                .ok_or_else(arithmetic)?;
            let output = &mut values.as_slice_mut()[ordinal * width..(ordinal + 1) * width];
            const COPY_TURN: usize = 64 * 1024;
            for (output, input) in output.chunks_mut(COPY_TURN).zip(input.chunks(COPY_TURN)) {
                work(input.len()).map_err(ValueFailure::Caller)?;
                output.copy_from_slice(input);
            }
        }
    }
    let data = ArrayData::builder(source.data_type().clone())
        .len(len)
        .add_buffer(values.into())
        .nulls(finish_validity(validity, len))
        .build()
        .map_err(ValueError::from)?;
    Ok(make_array(data))
}

fn build_container<E>(
    column: &Column,
    rows: Rows<'_>,
    growth: &mut Growth<'_, E>,
    work: &mut Work<'_, E>,
) -> Result<ArrayRef, ValueFailure<E>> {
    let len = rows.len();
    let has_offsets = matches!(column.shape, Shape::List(_) | Shape::Map);
    let mut selections = allocate(column.children.len(), growth)?;
    for child in &column.children {
        selections.push(allocate::<Option<usize>, E>(child.committed.rows, growth)?);
    }
    let mut offsets = if has_offsets {
        Offsets::new(column.shape, len, growth)?
    } else {
        Offsets::None
    };
    let mut validity = validity_buffer(len, column.committed.nulls, growth)?;
    for ordinal in 0..len {
        work(1).map_err(ValueFailure::Caller)?;
        let row = rows.at(ordinal)?.filter(|&row| !column.source.is_null(row));
        if let Some(row) = row {
            mark_valid(&mut validity, ordinal);
            child_rows(&column.source, row, &mut |index, start, end| {
                for child_row in start..end {
                    work(1).map_err(ValueFailure::Caller)?;
                    selections[index].push(Some(child_row));
                }
                Ok(())
            })?;
        } else {
            match column.shape {
                Shape::Struct => {
                    for selection in &mut selections {
                        selection.push(None);
                    }
                }
                Shape::FixedList(width) => {
                    for _ in 0..width {
                        work(1).map_err(ValueFailure::Caller)?;
                        selections[0].push(None);
                    }
                }
                _ => (),
            }
        }
        if has_offsets {
            offsets.push(selections[0].len())?;
        }
    }
    let nulls = finish_validity(validity, len);
    let mut output = allocate(column.children.len(), growth)?;
    for (child, selection) in column.children.iter().zip(selections.iter()) {
        output.push(build_column(child, Rows::Child(selection), growth, work)?);
    }
    let array: ArrayRef = match (&column.target, offsets) {
        (DataType::Struct(fields), Offsets::None) => Arc::new(
            StructArray::try_new_with_length(fields.clone(), output, nulls, len)
                .map_err(ValueError::from)?,
        ),
        (DataType::FixedSizeList(field, width), Offsets::None) => Arc::new(
            FixedSizeListArray::try_new_with_length(
                field.clone(),
                *width,
                output.pop().ok_or_else(transaction)?,
                nulls,
                len,
            )
            .map_err(ValueError::from)?,
        ),
        (DataType::List(field), Offsets::I32(offsets)) => Arc::new(
            ListArray::try_new(
                field.clone(),
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                output.pop().ok_or_else(transaction)?,
                nulls,
            )
            .map_err(ValueError::from)?,
        ),
        (DataType::LargeList(field), Offsets::I64(offsets)) => Arc::new(
            LargeListArray::try_new(
                field.clone(),
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                output.pop().ok_or_else(transaction)?,
                nulls,
            )
            .map_err(ValueError::from)?,
        ),
        (DataType::Map(entries, sorted), Offsets::I32(offsets)) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(transaction().into());
            };
            let entries_data =
                StructArray::try_new(fields.clone(), output, None).map_err(ValueError::from)?;
            Arc::new(
                MapArray::try_new(
                    entries.clone(),
                    OffsetBuffer::new(ScalarBuffer::from(offsets)),
                    entries_data,
                    nulls,
                    *sorted,
                )
                .map_err(ValueError::from)?,
            )
        }
        _ => return Err(transaction().into()),
    };
    Ok(array)
}

enum Offsets {
    None,
    I32(Vec<i32>),
    I64(Vec<i64>),
}

impl Offsets {
    fn new<E>(
        shape: Shape,
        len: usize,
        growth: &mut Growth<'_, E>,
    ) -> Result<Self, ValueFailure<E>> {
        let count = len.checked_add(1).ok_or_else(arithmetic)?;
        let mut offsets = match shape {
            Shape::List(4) | Shape::Map => Self::I32(allocate(count, growth)?),
            Shape::List(8) => Self::I64(allocate(count, growth)?),
            _ => return Err(transaction().into()),
        };
        offsets.push(0)?;
        Ok(offsets)
    }

    fn push(&mut self, offset: usize) -> Result<(), ValueError> {
        match self {
            Self::None => return Err(transaction()),
            Self::I32(offsets) => offsets.push(i32::try_from(offset).map_err(|_| transaction())?),
            Self::I64(offsets) => offsets.push(i64::try_from(offset).map_err(|_| transaction())?),
        }
        Ok(())
    }
}

fn zeroed_arrow_buffer<E>(
    bytes: usize,
    growth: &mut Growth<'_, E>,
) -> Result<MutableBuffer, ValueFailure<E>> {
    if bytes != 0 {
        let layout = Layout::from_size_align(bytes, ALIGNMENT).map_err(|_| arithmetic())?;
        growth(layout).map_err(ValueFailure::Caller)?;
    }
    MutableBuffer::try_from_len_zeroed(bytes)
        .map_err(|error| ValueError::from(ArrowError::MemoryError(error.to_string())).into())
}

fn validity_buffer<E>(
    rows: usize,
    nulls: usize,
    growth: &mut Growth<'_, E>,
) -> Result<Option<MutableBuffer>, ValueFailure<E>> {
    if nulls == 0 {
        return Ok(None);
    }
    Ok(Some(zeroed_arrow_buffer(rows.div_ceil(8), growth)?))
}

fn mark_valid(validity: &mut Option<MutableBuffer>, index: usize) {
    if let Some(validity) = validity {
        set_bit(validity, index);
    }
}

fn set_bit(buffer: &mut MutableBuffer, index: usize) {
    buffer.as_slice_mut()[index / 8] |= 1 << (index % 8);
}

fn finish_validity(bitmap: Option<MutableBuffer>, len: usize) -> Option<NullBuffer> {
    bitmap.map(|bytes| NullBuffer::new(BooleanBuffer::new(bytes.into(), 0, len)))
}

fn transaction() -> ValueError {
    ValueError::Invariant(ValueViolation::RowTransaction)
}
