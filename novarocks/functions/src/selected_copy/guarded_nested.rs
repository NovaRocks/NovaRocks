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

//! The pinned Arrow List/Map/Struct interleave author. List and Map ranges
//! include the physical offsets beneath NULL parents. Struct recursively
//! copies every field under the original indices. This observes extent and
//! control without granting capacity.
use super::*;
use arrow_array::types::{BinaryType, LargeBinaryType, LargeUtf8Type, Utf8Type};
use std::mem::size_of;

/// Closed nested indexed carriers. Views, dictionaries, Union and run-end
/// encodings retain their separate unsupported boundary.
pub fn supports_guarded_result(ty: &DataType) -> bool {
    match ty {
        DataType::List(field) => supports_guarded_result(field.data_type()),
        DataType::Map(entries, _) => {
            if entries.is_nullable() {
                return false;
            }
            let DataType::Struct(fields) = entries.data_type() else {
                return false;
            };
            fields.len() == 2
                && !fields[0].is_nullable()
                && supports_guarded_result(entries.data_type())
        }
        DataType::Struct(fields) => fields
            .iter()
            .all(|field| supports_guarded_result(field.data_type())),
        DataType::FixedSizeBinary(width) => *width > 0,
        DataType::Null
        | DataType::Boolean
        | DataType::Utf8
        | DataType::Binary
        | DataType::LargeUtf8
        | DataType::LargeBinary => true,
        _ => ty.primitive_width().is_some(),
    }
}
/// Static offsets and validity before demanded children run. List/Map child
/// counts are checked by the exact physical range author after choices exist.
/// Keep the old flat-only Values-construction gate independent of this port.
pub fn guarded_result_extent(ty: &DataType, rows: usize) -> Result<(), CopyError> {
    match ty {
        // NullArray carries a length without a payload buffer or bitmap.
        DataType::Null => Ok(()),
        DataType::List(field) => offset_child_extent(field, rows),
        DataType::Map(entries, _) if supports_guarded_result(ty) => {
            offset_child_extent(entries, rows)
        }
        DataType::Struct(fields) => {
            interleave_bitmap_extent(rows)?;
            buffer_extent(fields.len(), size_of::<ArrayRef>())?;
            for field in fields {
                guarded_result_extent(field.data_type(), rows)?;
            }
            Ok(())
        }
        _ if supports_guarded_result(ty) => guarded_interleave_extent(ty, rows),
        other => Err(CopyError::unsupported(other)),
    }
}

fn offset_child_extent(field: &arrow_schema::Field, rows: usize) -> Result<(), CopyError> {
    buffer_extent(add(rows, 1)?, size_of::<i32>())?;
    interleave_bitmap_extent(rows)?;
    guarded_result_extent(field.data_type(), 0)
}

pub(super) fn preflight_nested_interleave(
    ty: &DataType,
    sources: &[ArrayRef],
    choices: &[(usize, usize)],
    mut observe: impl FnMut(bool) -> Result<(), KernelFailure>,
) -> Result<(), CopyError> {
    let mut work = CopyObservation(
        WorkObservation::Legacy(&mut observe),
        None,
        ScratchCoverage::ChildTables,
        None,
        None,
    );
    work.boundary()?;
    let result = (|| {
        guarded_result_extent(ty, choices.len())?;
        validate_interleave_plan(ty, sources, choices, &mut work)?;
        if choices.is_empty() {
            return Ok(());
        }
        // Exact ordered source-row choices, compressed only into existing
        // range facts. No payload, substitute copy, or unselected row is read.
        let mut blocks = work.scratch::<Block>(choices.len(), None)?;
        for &(source, row) in choices {
            work.step()?;
            let mut ranges = work.scratch::<Range<usize>>(1, None)?;
            work.push(&mut ranges, row..add(row, 1)?)?;
            work.push(
                &mut blocks,
                Block {
                    source,
                    ranges,
                    repeats: 1,
                    raw_null: false,
                    raw_outside: 0,
                },
            )?;
        }
        let selection = Selection {
            blocks,
            nulls: 0,
            null_ops: 0,
        };
        let mut borrowed = work.scratch::<&dyn Array>(sources.len(), None)?;
        for source in sources {
            work.push(&mut borrowed, source.as_ref())?;
            work.step()?;
        }
        visit_interleave(&borrowed, &selection, &mut work)
    })();
    if matches!(&result, Err(CopyError::Control(_) | CopyError::Extent(_))) {
        return result;
    }
    work.boundary()?;
    result
}

fn visit_interleave(
    sources: &[&dyn Array],
    selection: &Selection,
    work: &mut CopyObservation<'_>,
) -> Result<(), CopyError> {
    work.step()?;
    let first = *sources
        .first()
        .ok_or(CopyError::Invalid("nested interleave has no actual source"))?;
    let ty = first.data_type();
    let rows = selection.len(work)?;
    guarded_result_extent(ty, rows)?;
    // Arrow interleave returns new_empty_array before downcasting a source.
    // Its empty child offset/metadata extents were checked statically above.
    if rows == 0 {
        return Ok(());
    }
    buffer_extent(sources.len(), size_of::<&dyn Array>())?;
    match ty {
        DataType::Null => Ok(()),
        DataType::Struct(fields) => {
            for field in 0..fields.len() {
                work.step()?;
                let children = child_sources(sources, work, |source| {
                    let source: &StructArray = downcast(source)?;
                    Ok(source.column(field).as_ref())
                })?;
                visit_interleave(&children, selection, work)?;
            }
            Ok(())
        }
        DataType::List(_) => visit_offset_children(
            sources,
            selection,
            work,
            |source| Ok(downcast::<GenericListArray<i32>>(source)?.value_offsets()),
            |source| Ok(downcast::<GenericListArray<i32>>(source)?.values().as_ref()),
        ),
        DataType::Map(_, _) => visit_offset_children(
            sources,
            selection,
            work,
            |source| Ok(downcast::<MapArray>(source)?.value_offsets()),
            |source| Ok(downcast::<MapArray>(source)?.entries()),
        ),
        DataType::Utf8 => selected_bytes::<Utf8Type>(sources, selection, false, work),
        DataType::LargeUtf8 => selected_bytes::<LargeUtf8Type>(sources, selection, true, work),
        DataType::Binary => selected_bytes::<BinaryType>(sources, selection, false, work),
        DataType::LargeBinary => selected_bytes::<LargeBinaryType>(sources, selection, true, work),
        _ => fixed_interleave_extent(ty, rows),
    }
}
fn visit_offset_children(
    sources: &[&dyn Array],
    selection: &Selection,
    work: &mut CopyObservation<'_>,
    offsets: impl for<'a> FnMut(&'a dyn Array) -> Result<&'a [i32], CopyError>,
    child: impl for<'a> FnMut(&'a dyn Array) -> Result<&'a dyn Array, CopyError>,
) -> Result<(), CopyError> {
    // Arrow interleave_list and interleave_map copy the selected physical
    // offsets, including spans beneath NULL parents.
    let selected =
        offset_selection::<i32, _>(sources, selection, CopyMode::Extend, false, work, offsets)?;
    buffer_extent(selected.len(work)?, size_of::<(usize, usize)>())?;
    let children = child_sources(sources, work, child)?;
    visit_interleave(&children, &selected, work)
}

fn selected_bytes<T: ByteArrayType>(
    sources: &[&dyn Array],
    selection: &Selection,
    large: bool,
    work: &mut CopyObservation<'_>,
) -> Result<(), CopyError> {
    let mut total = 0;
    for block in &selection.blocks {
        work.step()?;
        let array: &GenericByteArray<T> = downcast(sources[block.source])?;
        let mut one = 0;
        for range in &block.ranges {
            for row in range.clone() {
                work.step()?;
                let start = offset_value(array.value_offsets()[row])?;
                let end = offset_value(array.value_offsets()[row + 1])?;
                one = add(
                    one,
                    end.checked_sub(start)
                        .ok_or(CopyError::Invalid("copy source offsets are not monotonic"))?,
                )?;
            }
        }
        total = add(total, mul(one, block.repeats)?)?;
    }
    byte_interleave_payload_extent(total, large)
}

#[cfg(test)]
#[path = "guarded_nested_tests.rs"]
mod tests;
