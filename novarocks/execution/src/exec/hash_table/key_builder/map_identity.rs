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

//! The versioned grouping identity for values that contain a MAP. The legacy
//! encoder remains the author of positional representatives and scalar leaves.
// Design: ADR-0265 (docs/adr/ADR-0265-row-declarations-and-canonical-map-keys.md)
use super::*;
use allocator_api2::{
    alloc::{Allocator, Global},
    vec::Vec as AllocatedVec,
};
use novarocks_type_contract::{ValueKind, ValueTypeRef, key_character_bytes_observed};
use novarocks_value_arrays::{ValueError, ValueViolation};
use std::{alloc::Layout, cmp::Ordering, ops::Range};

pub(crate) fn contains_map(declaration: &ValueTypeRef) -> bool {
    matches!(declaration.kind(), ValueKind::Map { .. })
        || (0..declaration.child_count())
            .any(|index| contains_map(declaration.child_type_at(index).expect("declared child")))
}

pub(crate) fn append_map_identity_nullable(
    array: &ArrayRef,
    row: usize,
    declaration: &ValueTypeRef,
    out: &mut AggregateVec<u8>,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    checkpoint()?;
    let allocator = out.allocator().clone();
    let mut encoder = IdentityEncoder {
        allocator: &allocator,
        request: &|_| Ok(()),
        checkpoint,
        work: ObservedCanonicalKeyWork::new(checkpoint),
        map_scratch: AllocatedVec::new_in(allocator.clone()),
    };
    encoder.nullable(array, row, declaration, out)?;
    checkpoint()
}

pub(crate) fn hash_map_identity_column_observed(
    array: &ArrayRef,
    declaration: &ValueTypeRef,
    hashes: &mut [Option<u64>],
    request: &dyn Fn(Layout) -> ExecutionResult<()>,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
    accumulate: impl Fn(u64, Option<u64>) -> u64,
) -> ExecutionResult<()> {
    let mut encoder = IdentityEncoder {
        allocator: &Global,
        request,
        checkpoint,
        work: ObservedCanonicalKeyWork::new(checkpoint),
        map_scratch: AllocatedVec::new_in(Global),
    };
    let mut bytes = AllocatedVec::new_in(Global);
    for (row, hash) in hashes.iter_mut().enumerate() {
        if row.is_multiple_of(256) {
            checkpoint()?;
        }
        let Some(accumulator) = hash else { continue };
        let leaf = if array.is_null(row) {
            if !declaration.nullable() {
                return Err(ExecutionFailure::from_domain(ValueError::invariant(
                    ValueViolation::RequiredNull,
                )));
            }
            None
        } else {
            bytes.clear();
            encoder.non_null(array, row, declaration, &mut bytes)?;
            let mut sink = HashingKeyByteSink::new(0);
            encoder.work.extend(&mut sink, &bytes)?;
            Some(sink.finish())
        };
        *accumulator = accumulate(*accumulator, leaf);
    }
    checkpoint()
}

struct IdentityEncoder<'a, A: Allocator> {
    allocator: &'a A,
    request: &'a dyn Fn(Layout) -> ExecutionResult<()>,
    checkpoint: &'a dyn Fn() -> ExecutionResult<()>,
    work: ObservedCanonicalKeyWork<'a>,
    map_scratch: AllocatedVec<MapScratch<A>, A>,
}

impl<A: Allocator + Clone> IdentityEncoder<'_, A> {
    fn write(&mut self, output: &mut AllocatedVec<u8, A>, bytes: &[u8]) -> ExecutionResult<()> {
        self.work.extend(
            &mut IdentitySink {
                output,
                request: self.request,
            },
            bytes,
        )
    }

    fn nullable(
        &mut self,
        array: &ArrayRef,
        row: usize,
        declaration: &ValueTypeRef,
        output: &mut AllocatedVec<u8, A>,
    ) -> ExecutionResult<()> {
        self.work.step()?;
        if matches!(array.data_type(), DataType::Null) || array.is_null(row) {
            if !declaration.nullable() {
                return Err(ExecutionFailure::from_domain(ValueError::invariant(
                    ValueViolation::RequiredNull,
                )));
            }
            return self.write(output, &[0]);
        }

        let start = output.len();
        self.write(output, &[1, 0, 0, 0, 0])?;
        self.non_null(array, row, declaration, output)?;
        let length = group_key_component_len(output.len() - start - 5, "MAP identity component")?;
        output[start + 1..start + 5].copy_from_slice(&length.to_le_bytes());
        Ok(())
    }

    fn sequence_header(
        &mut self,
        output: &mut AllocatedVec<u8, A>,
        tag: u8,
        count: usize,
    ) -> ExecutionResult<()> {
        self.write(output, &[tag])?;
        self.write(
            output,
            &group_key_component_len(count, "MAP identity entries")?.to_le_bytes(),
        )
    }

    fn non_null(
        &mut self,
        array: &ArrayRef,
        row: usize,
        declaration: &ValueTypeRef,
        output: &mut AllocatedVec<u8, A>,
    ) -> ExecutionResult<()> {
        self.work.step()?;
        match declaration.kind() {
            ValueKind::List(item) => {
                let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                    group_key_encoding_contract("MAP identity List has the wrong carrier")
                })?;
                let rows = offsets(list.value_offsets(), row)?;
                self.sequence_header(output, 16, rows.len())?;
                for row in rows {
                    self.nullable(list.values(), row, item, output)?;
                }
                Ok(())
            }
            ValueKind::Struct(fields) => {
                let structure = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| {
                        group_key_encoding_contract("MAP identity Struct has the wrong carrier")
                    })?;
                if structure.num_columns() != fields.len() {
                    return Err(group_key_encoding_contract(
                        "MAP identity Struct fields do not match their declaration",
                    ));
                }
                self.sequence_header(output, 17, fields.len())?;
                for (field, column) in fields.iter().zip(structure.columns()) {
                    self.nullable(column, row, field.value_type(), output)?;
                }
                Ok(())
            }
            ValueKind::Map { key, value } => {
                let map = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                    group_key_encoding_contract("MAP identity Map has the wrong carrier")
                })?;
                self.map(map, offsets(map.value_offsets(), row)?, key, value, output)
            }
            ValueKind::BoundedCharacter(character) => {
                let (bytes, marker, component) = canonical_byte_value(array, row)?;
                let bytes =
                    key_character_bytes_observed(*character, bytes, |_| (self.checkpoint)())?;
                self.write(output, &[marker])?;
                self.write(
                    output,
                    &group_key_component_len(bytes.len(), component)?.to_le_bytes(),
                )?;
                self.write(output, bytes)
            }
            _ => encode_group_key_non_null_value_with_policy::<false, false, false, _, _>(
                array,
                row,
                Some(declaration),
                &mut IdentitySink {
                    output,
                    request: self.request,
                },
                &mut self.work,
            ),
        }
    }

    fn map(
        &mut self,
        map: &MapArray,
        rows: Range<usize>,
        key: &ValueTypeRef,
        value: &ValueTypeRef,
        output: &mut AllocatedVec<u8, A>,
    ) -> ExecutionResult<()> {
        self.sequence_header(output, 18, rows.len())?;
        if rows.len() <= 1 {
            for row in rows {
                self.nullable(map.keys(), row, key, output)?;
                self.nullable(map.values(), row, value, output)?;
            }
            return Ok(());
        }

        // Return each completed recursion frame to the column's scratch pool.
        // The construction owner observes actual capacity growth, not a new
        // allocation for every row that fits the existing buffers.
        let mut frame = self
            .map_scratch
            .pop()
            .unwrap_or_else(|| MapScratch::new(self.allocator));
        frame.key_bytes.clear();
        frame.entries.clear();
        frame.sort.clear();
        reserve(&mut frame.entries, rows.len(), self.request)?;
        reserve(&mut frame.sort, rows.len(), self.request)?;
        for row in rows {
            let start = frame.key_bytes.len();
            self.nullable(map.keys(), row, key, &mut frame.key_bytes)?;
            let entry = EntryRef {
                row,
                start,
                end: frame.key_bytes.len(),
            };
            frame.entries.push(entry);
            frame.sort.push(entry);
        }
        sort_key_references(
            &mut frame.entries,
            &mut frame.sort,
            self.checkpoint,
            |a, b| {
                compare_bytes(
                    &frame.key_bytes[a.start..a.end],
                    &frame.key_bytes[b.start..b.end],
                    self.checkpoint,
                )
            },
        )?;
        for entry in &frame.entries {
            self.write(output, &frame.key_bytes[entry.start..entry.end])?;
            self.nullable(map.values(), entry.row, value, output)?;
        }
        let capacity = checked_encoded_len_add(self.map_scratch.len(), 1)?;
        reserve(&mut self.map_scratch, capacity, self.request)?;
        self.map_scratch.push(frame);
        Ok(())
    }
}

fn offsets(offsets: &[i32], row: usize) -> ExecutionResult<Range<usize>> {
    let start = offsets.get(row).and_then(|&n| usize::try_from(n).ok());
    let end = offsets.get(row + 1).and_then(|&n| usize::try_from(n).ok());
    match (start, end) {
        (Some(start), Some(end)) if start <= end => Ok(start..end),
        _ => Err(group_key_encoding_contract(
            "invalid MAP identity child offsets",
        )),
    }
}

#[derive(Clone, Copy)]
struct EntryRef {
    row: usize,
    start: usize,
    end: usize,
}

struct MapScratch<A: Allocator> {
    key_bytes: AllocatedVec<u8, A>,
    entries: AllocatedVec<EntryRef, A>,
    sort: AllocatedVec<EntryRef, A>,
}

impl<A: Allocator + Clone> MapScratch<A> {
    fn new(allocator: &A) -> Self {
        Self {
            key_bytes: AllocatedVec::new_in(allocator.clone()),
            entries: AllocatedVec::new_in(allocator.clone()),
            sort: AllocatedVec::new_in(allocator.clone()),
        }
    }
}

fn reserve<T, A: Allocator>(
    values: &mut AllocatedVec<T, A>,
    total: usize,
    request: &dyn Fn(Layout) -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    if total <= values.capacity() {
        return Ok(());
    }
    let layout = Layout::array::<T>(total).map_err(|_| group_key_request_arithmetic())?;
    request(layout)?;
    values
        .try_reserve_exact(total - values.len())
        .map_err(|source| {
            ExecutionFailure::from_domain(ValueError::Allocator {
                requested: layout.size(),
                source,
            })
        })
}

struct IdentitySink<'a, A: Allocator> {
    output: &'a mut AllocatedVec<u8, A>,
    request: &'a dyn Fn(Layout) -> ExecutionResult<()>,
}

impl<A: Allocator> KeyByteSink for IdentitySink<'_, A> {
    fn push_byte(&mut self, value: u8) -> ExecutionResult<()> {
        self.extend_bytes(&[value])
    }

    fn extend_bytes(&mut self, bytes: &[u8]) -> ExecutionResult<()> {
        let total = checked_encoded_len_add(self.output.len(), bytes.len())?;
        if total > self.output.capacity() {
            let capacity = total.max(self.output.capacity().saturating_mul(2)).max(8);
            reserve(self.output, capacity, self.request)?;
        }
        self.output.extend_from_slice(bytes);
        Ok(())
    }
}

fn compare_bytes(
    left: &[u8],
    right: &[u8],
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<Ordering> {
    let common = left.len().min(right.len());
    for (left, right) in left[..common]
        .chunks(64 * 1024)
        .zip(right[..common].chunks(64 * 1024))
    {
        checkpoint()?;
        let ordering = left.cmp(right);
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(left.len().cmp(&right.len()))
}

pub(crate) fn identity_bytes_equal(
    left: &[u8],
    right: &[u8],
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<bool> {
    Ok(left.len() == right.len() && compare_bytes(left, right, checkpoint)? == Ordering::Equal)
}

/// Cooperative merge sort for original references. The caller owns scratch
/// allocation and comparison work, which can include arbitrary-sized keys.
pub(crate) fn sort_key_references<T: Copy>(
    rows: &mut [T],
    scratch: &mut [T],
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
    mut compare: impl FnMut(T, T) -> ExecutionResult<Ordering>,
) -> ExecutionResult<()> {
    let mut width = 1usize;
    while width < rows.len() {
        checkpoint()?;
        for start in (0..rows.len()).step_by(width.saturating_mul(2)) {
            let middle = start.saturating_add(width).min(rows.len());
            let end = middle.saturating_add(width).min(rows.len());
            let (mut left, mut right) = (start, middle);
            for (offset, slot) in scratch[start..end].iter_mut().enumerate() {
                if offset.is_multiple_of(256) {
                    checkpoint()?;
                }
                if left < middle
                    && (right == end || compare(rows[left], rows[right])? != Ordering::Greater)
                {
                    *slot = rows[left];
                    left += 1;
                } else {
                    *slot = rows[right];
                    right += 1;
                }
            }
        }
        for (i, (to, &from)) in rows.iter_mut().zip(&*scratch).enumerate() {
            if i.is_multiple_of(256) {
                checkpoint()?;
            }
            *to = from;
        }
        width = width.saturating_mul(2);
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn permuted_map_fixture() -> ArrayRef {
    use arrow::array::Float64Array;

    let entries = StructArray::new(
        vec![
            arrow::datatypes::Field::new("key", DataType::Int32, false),
            arrow::datatypes::Field::new("value", DataType::Float64, true),
        ]
        .into(),
        vec![
            Arc::new(Int32Array::from(vec![2, 1, 1, 2, 1, 2, 4, 3])),
            Arc::new(Float64Array::from(vec![
                None,
                Some(-0.0),
                Some(0.0),
                None,
                None,
                Some(0.0),
                Some(99.0),
                Some(100.0),
            ])),
        ],
        None,
    );
    Arc::new(MapArray::new(
        Arc::new(arrow::datatypes::Field::new(
            "entries",
            entries.data_type().clone(),
            false,
        )),
        arrow_buffer::OffsetBuffer::from_lengths([2; 4]),
        entries,
        Some(arrow_buffer::NullBuffer::from(vec![
            true, true, true, false,
        ])),
        false,
    ))
}
