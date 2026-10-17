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

//! The pinned Arrow List/Struct interleave author. List ranges include the
//! physical offsets beneath NULL parents; Struct recursively copies every
//! field under the original indices. This observes extent/control, not grants.
use super::*;
use arrow_array::types::{BinaryType, LargeBinaryType, LargeUtf8Type, Utf8Type};
use std::mem::size_of;

/// Closed nested indexed carriers. Views, dictionaries, Map, Union and run-end
/// encodings retain their separate unsupported boundary.
pub fn supports_guarded_result(ty: &DataType) -> bool {
    match ty {
        DataType::List(field) => supports_guarded_result(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .all(|field| supports_guarded_result(field.data_type())),
        DataType::Boolean
        | DataType::Utf8
        | DataType::Binary
        | DataType::LargeUtf8
        | DataType::LargeBinary => true,
        _ => ty.primitive_width().is_some(),
    }
}
/// Static offsets/validity before demanded children run. Unknown List child
/// counts are checked by the exact physical range author after choices exist.
/// Keep the old flat-only Values-construction gate independent of this port.
pub fn guarded_result_extent(ty: &DataType, rows: usize) -> Result<(), CopyError> {
    match ty {
        DataType::List(field) => {
            buffer_extent(add(rows, 1)?, size_of::<i32>())?;
            interleave_bitmap_extent(rows)?;
            guarded_result_extent(field.data_type(), 0)
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
        other => Err(CopyError::Unsupported(other.clone())),
    }
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
    if matches!(&result, Err(CopyError::Control(_))) {
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
        DataType::List(_) => {
            // Extend's existing physical-offset mapper, unlike take's mapper,
            // keeps NULL parent spans. That is exactly Arrow interleave_list.
            let child = offset_selection::<i32, _>(
                sources,
                selection,
                CopyMode::Extend,
                false,
                work,
                |source| {
                    let source: &GenericListArray<i32> = downcast(source)?;
                    Ok(source.value_offsets())
                },
            )?;
            let child_rows = child.len(work)?;
            buffer_extent(child_rows, size_of::<(usize, usize)>())?;
            let children = child_sources(sources, work, |source| {
                let source: &GenericListArray<i32> = downcast(source)?;
                Ok(source.values().as_ref())
            })?;
            visit_interleave(&children, &child, work)
        }
        DataType::Utf8 => selected_bytes::<Utf8Type>(sources, selection, false, work),
        DataType::LargeUtf8 => selected_bytes::<LargeUtf8Type>(sources, selection, true, work),
        DataType::Binary => selected_bytes::<BinaryType>(sources, selection, false, work),
        DataType::LargeBinary => selected_bytes::<LargeBinaryType>(sources, selection, true, work),
        _ => fixed_interleave_extent(ty, rows),
    }
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
                one = add(one, end.checked_sub(start).ok_or(CopyError::Extent)?)?;
            }
        }
        total = add(total, mul(one, block.repeats)?)?;
    }
    byte_interleave_payload_extent(total, large)
}

#[cfg(test)]
#[path = "guarded_nested_tests.rs"]
mod tests;
