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
//! Hash-table primitives for join key indexing.
//!
//! Responsibilities:
//! - Builds hash buckets and row-reference chains from build-side key arrays.
//! - Supports null-sensitive key handling and efficient group-id lookup for probing.
//!
//! Key exported interfaces:
//! - Types: `JoinHashTable`.
//! - Functions: `row_has_forbidden_null`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::mem;
use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::DataType;

use crate::exec::hash_table::key_builder::{GroupKeyArrayView, build_group_key_views};
use crate::exec::hash_table::key_strategy::GroupKeyStrategy;
use crate::exec::hash_table::key_table::{KeyLookup, KeyTable};
use crate::runtime::mem_tracker::MemTracker;

const ENABLE_GROUP_KEY_OPTIMIZATIONS: bool = true;
const ROW_NONE: u32 = u32::MAX;

pub(crate) enum SerializedRows {
    Arrow(arrow::row::Rows),
    Fallback(Vec<Vec<u8>>),
}

impl SerializedRows {
    fn row_bytes(&self, row: usize) -> Result<&[u8], String> {
        match self {
            Self::Arrow(rows) => Ok(rows.row(row).data()),
            Self::Fallback(rows) => rows.get(row).map(|v| v.as_slice()).ok_or_else(|| {
                format!(
                    "fallback join row missing at row={} (rows={})",
                    row,
                    rows.len()
                )
            }),
        }
    }
}

/// Hash-table container for join key buckets and build-row reference chains.
pub(crate) struct JoinHashTable {
    key_table: KeyTable,
    null_safe_eq: Vec<bool>,
    group_head: Vec<u32>,
    row_next: Vec<u32>,
    row_batch_index: Vec<u32>,
    row_in_batch: Vec<u32>,
    row_count: usize,
    group_offsets: Option<Vec<u32>>,
    group_rows: Option<Vec<u32>>,
    mem_tracker: Option<Arc<MemTracker>>,
    accounted_bytes: i64,
}

/// Check whether the row has null on any key that is not null-safe (`=` semantics).
pub(crate) fn row_has_forbidden_null(
    views: &[GroupKeyArrayView<'_>],
    row: usize,
    null_safe_eq: &[bool],
) -> bool {
    for (idx, view) in views.iter().enumerate() {
        if *null_safe_eq.get(idx).unwrap_or(&false) {
            continue;
        }
        let is_null = match view {
            GroupKeyArrayView::Int(view) => view.value_at(row).is_none(),
            GroupKeyArrayView::Float(view) => view.value_at(row).is_none(),
            GroupKeyArrayView::Boolean(arr) => arr.is_null(row),
            GroupKeyArrayView::Utf8(arr) => arr.is_null(row),
            GroupKeyArrayView::Date32(arr) => arr.is_null(row),
            GroupKeyArrayView::TimestampSecond(arr) => arr.is_null(row),
            GroupKeyArrayView::TimestampMillisecond(arr) => arr.is_null(row),
            GroupKeyArrayView::TimestampMicrosecond(arr) => arr.is_null(row),
            GroupKeyArrayView::TimestampNanosecond(arr) => arr.is_null(row),
            GroupKeyArrayView::Decimal128(arr) => arr.is_null(row),
            GroupKeyArrayView::Decimal256(arr) => arr.is_null(row),
            GroupKeyArrayView::LargeIntBinary(arr) => arr.is_null(row),
            GroupKeyArrayView::ListUtf8 { list, .. } => list.is_null(row),
            GroupKeyArrayView::ListInt32 { list, .. } => list.is_null(row),
            GroupKeyArrayView::Complex(arr) => arr.is_null(row),
        };
        if is_null {
            return true;
        }
    }
    false
}

impl JoinHashTable {
    pub(crate) fn new(key_types: Vec<DataType>, null_safe_eq: Vec<bool>) -> Result<Self, String> {
        if key_types.is_empty() {
            return Err("join hash table requires join keys".to_string());
        }
        if key_types.len() != null_safe_eq.len() {
            return Err(format!(
                "join hash table null-safe key count mismatch: key_types={} flags={}",
                key_types.len(),
                null_safe_eq.len()
            ));
        }
        let key_table = KeyTable::new(key_types, ENABLE_GROUP_KEY_OPTIMIZATIONS)?;
        if key_table.key_strategy() == GroupKeyStrategy::Scalar {
            return Err("join hash table requires join keys".to_string());
        }
        Ok(Self {
            key_table,
            null_safe_eq,
            group_head: Vec::new(),
            row_next: Vec::new(),
            row_batch_index: Vec::new(),
            row_in_batch: Vec::new(),
            row_count: 0,
            group_offsets: None,
            group_rows: None,
            mem_tracker: None,
            accounted_bytes: 0,
        })
    }

    pub(crate) fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        if let Some(current) = self.mem_tracker.as_ref() {
            if Arc::ptr_eq(current, &tracker) {
                return;
            }
            current.release(self.accounted_bytes);
        }
        let bytes = self.tracked_bytes();
        tracker.consume(bytes);
        self.mem_tracker = Some(Arc::clone(&tracker));
        self.accounted_bytes = bytes;

        let key_table = MemTracker::new_child("KeyTable", &tracker);
        self.key_table.set_mem_tracker(key_table);
    }

    pub(crate) fn key_strategy(&self) -> GroupKeyStrategy {
        self.key_table.key_strategy()
    }

    pub(crate) fn null_safe_eq(&self) -> &[bool] {
        &self.null_safe_eq
    }

    pub(crate) fn hash_seed(&self) -> u64 {
        self.key_table.hash_seed()
    }

    pub(crate) fn compressed_ctx(
        &self,
    ) -> Option<&crate::exec::hash_table::key_layout::CompressedKeyContext> {
        self.key_table.compressed_ctx()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.group_head.is_empty()
    }

    pub(crate) fn build_rows_or_fallback(
        &self,
        arrays: &[arrow::array::ArrayRef],
    ) -> Result<SerializedRows, String> {
        match self.key_table.build_rows(arrays) {
            Ok(rows) => Ok(SerializedRows::Arrow(rows)),
            Err(err) if err.contains("row converter not initialized") => {
                let fallback_rows = self.key_table.build_rows_fallback(arrays)?;
                Ok(SerializedRows::Fallback(fallback_rows))
            }
            Err(err) => Err(err),
        }
    }

    pub(crate) fn group_rows_slice(&self, group_id: usize) -> Result<&[u32], String> {
        let offsets = self
            .group_offsets
            .as_ref()
            .ok_or_else(|| "join group offsets missing".to_string())?;
        let rows = self
            .group_rows
            .as_ref()
            .ok_or_else(|| "join group rows missing".to_string())?;
        if group_id + 1 >= offsets.len() {
            return Err("join group id out of bounds".to_string());
        }
        let start = offsets[group_id] as usize;
        let end = offsets[group_id + 1] as usize;
        Ok(&rows[start..end])
    }

    pub(crate) fn add_build_batch(
        &mut self,
        key_arrays: &[arrow::array::ArrayRef],
        num_rows: usize,
        batch_index: u32,
    ) -> Result<(), String> {
        if self.group_offsets.is_some() || self.group_rows.is_some() {
            return Err("join hash table already finalized".to_string());
        }
        if key_arrays.len() != self.key_table.key_types().len() {
            return Err("join key length mismatch".to_string());
        }
        for (array, expected) in key_arrays.iter().zip(self.key_table.key_types()) {
            if array.data_type() != expected {
                return Err("join key type mismatch".to_string());
            }
        }
        if num_rows == 0 {
            return Ok(());
        }
        let next_row_count = self
            .row_count
            .checked_add(num_rows)
            .ok_or_else(|| "join build row count overflow".to_string())?;
        if next_row_count > u32::MAX as usize {
            return Err("join build row count overflow".to_string());
        }
        let base_row_id = self.row_count as u32;
        self.row_next.resize(next_row_count, ROW_NONE);
        self.row_batch_index.resize(next_row_count, 0);
        self.row_in_batch.resize(next_row_count, 0);
        self.row_count = next_row_count;

        let key_views = build_group_key_views(key_arrays)?;
        self.key_table.ensure_compressed_ctx(&key_views)?;

        for row in 0..num_rows {
            let row_id = base_row_id + row as u32;
            let slot = row_id as usize;
            self.row_batch_index[slot] = batch_index;
            self.row_in_batch[slot] = row as u32;
        }

        match self.key_table.key_strategy() {
            GroupKeyStrategy::OneNumber => {
                let view = key_views
                    .get(0)
                    .ok_or_else(|| "join one number key view missing".to_string())?;
                let hashes = self.key_table.build_one_number_hashes(view, num_rows)?;
                for row in 0..num_rows {
                    if row_has_forbidden_null(&key_views, row, &self.null_safe_eq) {
                        continue;
                    }
                    let lookup =
                        self.key_table
                            .find_or_insert_one_number(view, row, hashes[row])?;
                    self.handle_lookup(lookup, base_row_id + row as u32)?;
                }
            }
            GroupKeyStrategy::OneString => {
                let view = key_views
                    .get(0)
                    .ok_or_else(|| "join one string key view missing".to_string())?;
                let GroupKeyArrayView::Utf8(arr) = view else {
                    return Err("join one string key expects Utf8 view".to_string());
                };
                let hashes = self.key_table.build_group_hashes(&key_views, num_rows)?;
                for row in 0..num_rows {
                    if row_has_forbidden_null(&key_views, row, &self.null_safe_eq) {
                        continue;
                    }
                    let key = if arr.is_null(row) {
                        None
                    } else {
                        Some(arr.value(row))
                    };
                    let lookup =
                        self.key_table
                            .find_or_insert_one_string(view, row, key, hashes[row])?;
                    self.handle_lookup(lookup, base_row_id + row as u32)?;
                }
            }
            GroupKeyStrategy::FixedSize => {
                let hashes = self.key_table.build_group_hashes(&key_views, num_rows)?;
                for row in 0..num_rows {
                    if row_has_forbidden_null(&key_views, row, &self.null_safe_eq) {
                        continue;
                    }
                    let lookup =
                        self.key_table
                            .find_or_insert_fixed_size(&key_views, row, hashes[row])?;
                    self.handle_lookup(lookup, base_row_id + row as u32)?;
                }
            }
            GroupKeyStrategy::CompressedFixed => {
                let keys = self
                    .key_table
                    .build_compressed_flags(&key_views, num_rows)?;
                let hashes = self.key_table.build_group_hashes(&key_views, num_rows)?;
                let mut rows_opt = None;
                for row in 0..num_rows {
                    if row_has_forbidden_null(&key_views, row, &self.null_safe_eq) {
                        continue;
                    }
                    let lookup = if keys[row] {
                        self.key_table
                            .find_or_insert_compressed(&key_views, row, hashes[row])?
                    } else {
                        if rows_opt.is_none() {
                            rows_opt = Some(self.build_rows_or_fallback(key_arrays)?);
                        }
                        let rows = rows_opt.as_ref().expect("join rows");
                        let row_bytes = rows.row_bytes(row)?;
                        self.key_table.find_or_insert_from_row(
                            &key_views,
                            row,
                            row_bytes,
                            hashes[row],
                        )?
                    };
                    self.handle_lookup(lookup, base_row_id + row as u32)?;
                }
            }
            GroupKeyStrategy::Serialized => {
                let rows = self.build_rows_or_fallback(key_arrays)?;
                let hashes = self.key_table.build_group_hashes(&key_views, num_rows)?;
                for row in 0..num_rows {
                    if row_has_forbidden_null(&key_views, row, &self.null_safe_eq) {
                        continue;
                    }
                    let row_bytes = rows.row_bytes(row)?;
                    let lookup = self.key_table.find_or_insert_from_row(
                        &key_views,
                        row,
                        row_bytes,
                        hashes[row],
                    )?;
                    self.handle_lookup(lookup, base_row_id + row as u32)?;
                }
            }
            GroupKeyStrategy::Scalar => {
                return Err("join hash table does not support empty keys".to_string());
            }
        }
        self.refresh_accounting();
        Ok(())
    }

    pub(crate) fn finalize_groups(&mut self) -> Result<(), String> {
        if self.group_offsets.is_some() || self.group_rows.is_some() {
            return Ok(());
        }
        let group_count = self.group_head.len();
        let mut counts = vec![0u32; group_count];
        for group_id in 0..group_count {
            let mut row = *self
                .group_head
                .get(group_id)
                .ok_or_else(|| "join group id out of bounds".to_string())?;
            while row != ROW_NONE {
                counts[group_id] = counts[group_id]
                    .checked_add(1)
                    .ok_or_else(|| "join group row count overflow".to_string())?;
                row = self.next_row(row)?;
            }
        }

        let mut offsets = Vec::with_capacity(group_count + 1);
        offsets.push(0);
        let mut total = 0u32;
        for count in &counts {
            total = total
                .checked_add(*count)
                .ok_or_else(|| "join group rows overflow".to_string())?;
            offsets.push(total);
        }

        let mut rows = vec![0u32; total as usize];
        let mut write_pos = offsets[..group_count].to_vec();
        for group_id in 0..group_count {
            let mut row = *self
                .group_head
                .get(group_id)
                .ok_or_else(|| "join group id out of bounds".to_string())?;
            while row != ROW_NONE {
                let slot = *write_pos
                    .get(group_id)
                    .ok_or_else(|| "join group id out of bounds".to_string())?;
                let idx = slot as usize;
                if idx >= rows.len() {
                    return Err("join group row index out of bounds".to_string());
                }
                rows[idx] = row;
                write_pos[group_id] = slot
                    .checked_add(1)
                    .ok_or_else(|| "join group row count overflow".to_string())?;
                row = self.next_row(row)?;
            }
        }

        self.group_offsets = Some(offsets);
        self.group_rows = Some(rows);
        self.refresh_accounting();
        Ok(())
    }

    pub(crate) fn lookup_serialized(
        &self,
        row_bytes: &[u8],
        hash: u64,
    ) -> Result<Option<usize>, String> {
        self.key_table.lookup_serialized(row_bytes, hash)
    }

    pub(crate) fn lookup_one_string(&self, key: &str, hash: u64) -> Result<Option<usize>, String> {
        self.key_table.lookup_one_string(key, hash)
    }

    pub(crate) fn lookup_one_string_null(&self) -> Option<usize> {
        self.key_table.lookup_one_string_null()
    }

    pub(crate) fn lookup_one_string_batch(
        &self,
        view: &GroupKeyArrayView<'_>,
        hashes: &[u64],
        nulls: &[bool],
    ) -> Result<Vec<Option<usize>>, String> {
        if hashes.len() != nulls.len() {
            return Err("join lookup batch length mismatch".to_string());
        }
        let GroupKeyArrayView::Utf8(arr) = view else {
            return Err("join one string key expects Utf8 view".to_string());
        };
        let mut group_ids = vec![None; hashes.len()];
        for row in 0..hashes.len() {
            if nulls[row] {
                continue;
            }
            if arr.is_null(row) {
                group_ids[row] = self.lookup_one_string_null();
                continue;
            }
            let key = arr.value(row);
            group_ids[row] = self.lookup_one_string(key, hashes[row])?;
        }
        Ok(group_ids)
    }

    pub(crate) fn lookup_one_number(
        &self,
        view: &GroupKeyArrayView<'_>,
        row: usize,
        hash: u64,
    ) -> Result<Option<usize>, String> {
        self.key_table.lookup_one_number(view, row, hash)
    }

    pub(crate) fn lookup_one_number_batch(
        &self,
        view: &GroupKeyArrayView<'_>,
        hashes: &[u64],
        nulls: &[bool],
    ) -> Result<Vec<Option<usize>>, String> {
        if hashes.len() != nulls.len() {
            return Err("join lookup batch length mismatch".to_string());
        }
        let mut group_ids = vec![None; hashes.len()];
        for row in 0..hashes.len() {
            if nulls[row] {
                continue;
            }
            group_ids[row] = self.lookup_one_number(view, row, hashes[row])?;
        }
        Ok(group_ids)
    }

    pub(crate) fn lookup_fixed_size(
        &self,
        views: &[GroupKeyArrayView<'_>],
        row: usize,
        hash: u64,
    ) -> Result<Option<usize>, String> {
        self.key_table.lookup_fixed_size(views, row, hash)
    }

    pub(crate) fn lookup_fixed_size_batch(
        &self,
        views: &[GroupKeyArrayView<'_>],
        hashes: &[u64],
        nulls: &[bool],
    ) -> Result<Vec<Option<usize>>, String> {
        if hashes.len() != nulls.len() {
            return Err("join lookup batch length mismatch".to_string());
        }
        let mut group_ids = vec![None; hashes.len()];
        for row in 0..hashes.len() {
            if nulls[row] {
                continue;
            }
            group_ids[row] = self.lookup_fixed_size(views, row, hashes[row])?;
        }
        Ok(group_ids)
    }

    pub(crate) fn lookup_compressed(
        &self,
        views: &[GroupKeyArrayView<'_>],
        row: usize,
        hash: u64,
    ) -> Result<Option<usize>, String> {
        self.key_table.lookup_compressed(views, row, hash)
    }

    pub(crate) fn lookup_compressed_batch(
        &self,
        views: &[GroupKeyArrayView<'_>],
        keys: &[bool],
        rows: Option<&SerializedRows>,
        hashes: &[u64],
        nulls: &[bool],
    ) -> Result<Vec<Option<usize>>, String> {
        if hashes.len() != nulls.len() || hashes.len() != keys.len() {
            return Err("join lookup batch length mismatch".to_string());
        }
        let mut group_ids = vec![None; hashes.len()];
        for row in 0..hashes.len() {
            if nulls[row] {
                continue;
            }
            let group_id = if keys[row] {
                self.lookup_compressed(views, row, hashes[row])?
            } else {
                let rows = rows.ok_or_else(|| "join row data missing".to_string())?;
                let row_bytes = rows.row_bytes(row)?;
                self.lookup_serialized(row_bytes, hashes[row])?
            };
            group_ids[row] = group_id;
        }
        Ok(group_ids)
    }

    pub(crate) fn lookup_serialized_batch(
        &self,
        rows: &SerializedRows,
        hashes: &[u64],
        nulls: &[bool],
    ) -> Result<Vec<Option<usize>>, String> {
        if hashes.len() != nulls.len() {
            return Err("join lookup batch length mismatch".to_string());
        }
        let mut group_ids = vec![None; hashes.len()];
        for row in 0..hashes.len() {
            if nulls[row] {
                continue;
            }
            let row_bytes = rows.row_bytes(row)?;
            group_ids[row] = self.lookup_serialized(row_bytes, hashes[row])?;
        }
        Ok(group_ids)
    }

    pub(crate) fn row_location(&self, row_id: u32) -> Result<(u32, u32), String> {
        let slot = row_id as usize;
        let batch_idx = *self
            .row_batch_index
            .get(slot)
            .ok_or_else(|| "join row id out of bounds".to_string())?;
        let row_idx = *self
            .row_in_batch
            .get(slot)
            .ok_or_else(|| "join row id out of bounds".to_string())?;
        Ok((batch_idx, row_idx))
    }

    fn handle_lookup(&mut self, lookup: KeyLookup, row_id: u32) -> Result<(), String> {
        if lookup.is_new {
            if lookup.group_id != self.group_head.len() {
                return Err("join group id out of bounds".to_string());
            }
            self.group_head.push(ROW_NONE);
        }
        self.link_row(lookup.group_id, row_id)
    }

    fn link_row(&mut self, group_id: usize, row_id: u32) -> Result<(), String> {
        let head = self
            .group_head
            .get(group_id)
            .copied()
            .ok_or_else(|| "join group id out of bounds".to_string())?;
        let slot = row_id as usize;
        if slot >= self.row_next.len() {
            return Err("join row id out of bounds".to_string());
        }
        self.row_next[slot] = head;
        self.group_head[group_id] = row_id;
        Ok(())
    }

    fn next_row(&self, row_id: u32) -> Result<u32, String> {
        self.row_next
            .get(row_id as usize)
            .copied()
            .ok_or_else(|| "join row id out of bounds".to_string())
    }

    fn refresh_accounting(&mut self) {
        let Some(tracker) = self.mem_tracker.as_ref() else {
            return;
        };
        let bytes = self.tracked_bytes();
        let delta = bytes - self.accounted_bytes;
        if delta > 0 {
            tracker.consume(delta);
        } else if delta < 0 {
            tracker.release(-delta);
        }
        self.accounted_bytes = bytes;
    }

    fn tracked_bytes(&self) -> i64 {
        fn vec_bytes<T>(v: &Vec<T>) -> i64 {
            let bytes = v.capacity().saturating_mul(mem::size_of::<T>());
            i64::try_from(bytes).unwrap_or(i64::MAX)
        }
        fn opt_vec_bytes<T>(v: &Option<Vec<T>>) -> i64 {
            v.as_ref().map(vec_bytes).unwrap_or(0)
        }

        vec_bytes(&self.group_head)
            .saturating_add(vec_bytes(&self.row_next))
            .saturating_add(vec_bytes(&self.row_batch_index))
            .saturating_add(vec_bytes(&self.row_in_batch))
            .saturating_add(opt_vec_bytes(&self.group_offsets))
            .saturating_add(opt_vec_bytes(&self.group_rows))
    }
}

impl Drop for JoinHashTable {
    fn drop(&mut self) {
        if let Some(tracker) = self.mem_tracker.as_ref() {
            tracker.release(self.accounted_bytes);
        }
    }
}
