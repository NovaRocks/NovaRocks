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
//! Join-owned physical row topology for a published K index.
//!
//! No key bytes, hashes, equality policy or direct integer index live here.

// Design: ADR-0205 (docs/adr/ADR-0205-shared-local-key-index-and-consumer-row-topology.md)

use super::join_hash_map::method::{JoinCursorFailure, arithmetic, map_contract};
use crate::{
    exec::hash_table::{
        allocation::{AggregateAllocator, AggregateVec},
        key_index::{BulkAssignments, BulkKey, BulkMatch, BulkWork},
    },
    runtime::fragment::{ExecutionFailure, ExecutionResult},
};
use novarocks_type_contract::ValueTypeStorageLifetime;

pub(crate) fn allocated<T>(
    allocator: &AggregateAllocator,
    count: usize,
) -> ExecutionResult<AggregateVec<T>> {
    let mut values = AggregateVec::new_in(allocator.clone());
    values
        .try_reserve_exact(count)
        .map_err(|source| allocator.allocation_error("join row topology", source))?;
    Ok(values)
}
fn fill<T: Copy>(work: &BulkWork<'_>, count: usize, value: T) -> ExecutionResult<AggregateVec<T>> {
    let mut output = allocated(&work.allocator, count)?;
    for _ in 0..count {
        work.control.checkpoint(0)?;
        output.push(value);
    }
    Ok(output)
}

/// A CSR over physical build rows. K's partition/KeyId is only a coordinate.
/// Both directories and rows use the original allocator, including old+new peaks.
pub(crate) struct JoinHashTable {
    partition_offsets: AggregateVec<usize>,
    group_offsets: AggregateVec<usize>,
    group_rows: AggregateVec<u32>,
    _storage: ValueTypeStorageLifetime,
}
impl JoinHashTable {
    pub(crate) fn from_assignments(
        assignments: &BulkAssignments,
        page_bases: &[usize],
        row_count: usize,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<Self> {
        // This is the pre-existing physical BuildRowId representation, not a
        // key/domain quota. ROW_NONE is no longer needed as a chain sentinel.
        if row_count > (u32::MAX as usize).saturating_add(1) {
            return Err(ExecutionFailure::from_domain(
                JoinCursorFailure::RowIdExtent,
            ));
        }
        let mut class_counts = allocated(&work.allocator, 0)?;
        for chunk in assignments.iter() {
            work.control.checkpoint(0)?;
            for &found in chunk.matches() {
                work.control.checkpoint(0)?;
                if let BulkMatch::Found(key) = found {
                    let p = key.partition();
                    while class_counts.len() <= p {
                        work.control.checkpoint(0)?;
                        class_counts.try_reserve(1).map_err(|source| {
                            work.allocator
                                .allocation_error("join partition coordinates", source)
                        })?;
                        class_counts.push(0usize);
                    }
                    class_counts[p] = class_counts[p]
                        .max(key.id().index().checked_add(1).ok_or_else(arithmetic)?);
                } else if matches!(found, BulkMatch::Miss) {
                    return Err(map_contract("published build assignment contains a miss"));
                }
            }
        }
        let mut partition_offsets = allocated(
            &work.allocator,
            class_counts.len().checked_add(1).ok_or_else(arithmetic)?,
        )?;
        partition_offsets.push(0usize);
        for &count in &class_counts {
            work.control.checkpoint(0)?;
            let next = partition_offsets
                .last()
                .copied()
                .unwrap()
                .checked_add(count)
                .ok_or_else(arithmetic)?;
            partition_offsets.push(next);
        }
        let groups = partition_offsets.last().copied().unwrap();
        let mut counts = fill(work, groups, 0usize)?;
        let mut row_groups = fill(work, row_count, usize::MAX)?;
        for chunk in assignments.iter() {
            work.control.checkpoint(0)?;
            let page = chunk.page();
            if page.run_index() != 0 {
                return Err(map_contract("serial join capture has a foreign run"));
            }
            let base = *page_bases
                .get(page.page_index())
                .ok_or_else(|| map_contract("join assignment page is absent"))?;
            for (ordinal, &found) in chunk.matches().iter().enumerate() {
                work.control.checkpoint(0)?;
                let row = base
                    .checked_add(chunk.row(ordinal)?)
                    .filter(|row| *row < row_count)
                    .ok_or_else(|| map_contract("join assignment row is absent"))?;
                if let BulkMatch::Found(key) = found {
                    let group = Self::coordinate(&partition_offsets, key)?;
                    if row_groups[row] != usize::MAX {
                        return Err(map_contract("join physical row assigned more than once"));
                    }
                    row_groups[row] = group;
                    counts[group] = counts[group].checked_add(1).ok_or_else(arithmetic)?;
                }
            }
        }
        let mut group_offsets = allocated(
            &work.allocator,
            groups.checked_add(1).ok_or_else(arithmetic)?,
        )?;
        group_offsets.push(0usize);
        for &count in &counts {
            work.control.checkpoint(0)?;
            if count == 0 {
                return Err(map_contract("published dense class has no physical row"));
            }
            group_offsets.push(
                group_offsets
                    .last()
                    .copied()
                    .unwrap()
                    .checked_add(count)
                    .ok_or_else(arithmetic)?,
            );
        }
        let mut group_rows = fill(work, group_offsets.last().copied().unwrap(), 0u32)?;
        for group in 0..groups {
            work.control.checkpoint(0)?;
            counts[group] = group_offsets[group];
        }
        // Preserve the existing descending BuildRowId order independently of
        // task completion order, without sorting or a key/hash directory.
        for row in (0..row_count).rev() {
            work.control.checkpoint(0)?;
            let group = row_groups[row];
            if group != usize::MAX {
                let slot = counts[group];
                group_rows[slot] = u32::try_from(row)
                    .map_err(|_| ExecutionFailure::from_domain(JoinCursorFailure::RowIdExtent))?;
                counts[group] += 1;
            }
        }
        work.control.checkpoint(0)?;
        Ok(Self {
            partition_offsets,
            group_offsets,
            group_rows,
            _storage: work.owner.storage_lifetime(),
        })
    }
    fn coordinate(offsets: &[usize], key: BulkKey) -> ExecutionResult<usize> {
        let start = *offsets
            .get(key.partition())
            .ok_or_else(|| map_contract("join partition is absent"))?;
        let end = *offsets
            .get(key.partition().checked_add(1).ok_or_else(arithmetic)?)
            .ok_or_else(|| map_contract("join partition is absent"))?;
        start
            .checked_add(key.id().index())
            .filter(|group| *group < end)
            .ok_or_else(|| map_contract("join class is absent"))
    }
    pub(crate) fn group(&self, key: BulkKey) -> ExecutionResult<usize> {
        Self::coordinate(&self.partition_offsets, key)
    }
    pub(crate) fn group_build_rows(&self, group: usize) -> Result<&[u32], JoinCursorFailure> {
        let start = *self
            .group_offsets
            .get(group)
            .ok_or(JoinCursorFailure::GroupUnavailable { group })?;
        let end = *self
            .group_offsets
            .get(
                group
                    .checked_add(1)
                    .ok_or(JoinCursorFailure::GroupUnavailable { group })?,
            )
            .ok_or(JoinCursorFailure::GroupUnavailable { group })?;
        self.group_rows
            .get(start..end)
            .ok_or(JoinCursorFailure::GroupUnavailable { group })
    }
}
