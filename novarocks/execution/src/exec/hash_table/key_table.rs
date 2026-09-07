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
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType;
use hashbrown::raw::RawTable;
#[cfg(test)]
use std::mem::{align_of, size_of};
use std::sync::Arc;

use crate::exec::expr::agg::{AggregateAllocator, AggregateVec};
use crate::exec::hash_table::hash::seed_from_hasher;
use crate::exec::hash_table::key_builder::{
    GroupKeyArrayView, build_compressed_flags, build_group_key_hashes, build_one_number_hashes,
    encode_group_key_row_tracked,
};
use crate::exec::hash_table::key_column::{KeyColumn, key_column_from_type_in};
use crate::exec::hash_table::key_layout::{CompressedKeyContext, build_compressed_key_context};
use crate::exec::hash_table::key_storage::{RowKey, RowStorage};
use crate::exec::hash_table::key_strategy::{GroupKeyStrategy, pick_group_key_strategy};
use crate::runtime::mem_tracker::MemTracker;
use hashbrown::hash_map::DefaultHashBuilder;

#[derive(Clone, Copy, Debug)]
struct KeyEntry {
    group_id: usize,
    hash: u64,
}

struct DictKeyMap {
    values_ptr: Option<usize>,
    code_to_group: AggregateVec<Option<usize>>,
}

impl DictKeyMap {
    fn new_in(allocator: AggregateAllocator) -> Self {
        Self {
            values_ptr: None,
            code_to_group: AggregateVec::new_in(allocator),
        }
    }

    fn reset_for(&mut self, values_ptr: usize, values_len: usize) -> Result<(), String> {
        if self.values_ptr != Some(values_ptr) {
            self.values_ptr = Some(values_ptr);
            self.code_to_group.clear();
        }
        if self.code_to_group.len() < values_len {
            let additional = values_len - self.code_to_group.len();
            if self.code_to_group.try_reserve(additional).is_err() {
                return Err(self
                    .code_to_group
                    .allocator()
                    .allocation_error("reserve dictionary group-key map"));
            }
            self.code_to_group.resize(values_len, None);
        }
        Ok(())
    }
}

pub struct KeyLookup {
    pub group_id: usize,
    pub is_new: bool,
}

pub struct KeyTable {
    key_strategy: GroupKeyStrategy,
    key_types: AggregateVec<DataType>,
    key_columns: AggregateVec<KeyColumn>,
    varlen_table: RawTable<KeyEntry, AggregateAllocator>,
    fixed_size_table: RawTable<KeyEntry, AggregateAllocator>,
    compressed_table: RawTable<KeyEntry, AggregateAllocator>,
    one_number_table: RawTable<KeyEntry, AggregateAllocator>,
    one_string_null: Option<usize>,
    dict_key_map: DictKeyMap,
    row_storage: RowStorage,
    varlen_keys: AggregateVec<RowKey>,
    compressed_ctx: Option<CompressedKeyContext>,
    key_columns_retained_bytes: usize,
    memory_limit_exceeded: bool,
    hash_seed: u64,
}

impl KeyTable {
    #[cfg(test)]
    pub fn new(key_types: Vec<DataType>, enable_optimizations: bool) -> Result<Self, String> {
        let tracker = MemTracker::new_child(
            "KeyTableTest",
            &crate::runtime::mem_tracker::process_mem_tracker(),
        );
        Self::new_with_tracker(key_types, enable_optimizations, tracker)
    }

    pub fn new_with_tracker(
        key_types: Vec<DataType>,
        enable_optimizations: bool,
        tracker: Arc<MemTracker>,
    ) -> Result<Self, String> {
        let mut key_strategy = pick_group_key_strategy(&key_types);
        if !enable_optimizations {
            key_strategy = GroupKeyStrategy::Serialized;
        }
        let allocator = AggregateAllocator::new(Arc::clone(&tracker));
        let mut tracked_key_types = AggregateVec::new_in(allocator.clone());
        tracked_key_types
            .try_reserve_exact(key_types.len())
            .map_err(|_| allocator.allocation_error("reserve group-key types"))?;
        tracked_key_types.extend(key_types);
        let mut key_columns = AggregateVec::new_in(allocator.clone());
        key_columns
            .try_reserve_exact(tracked_key_types.len())
            .map_err(|_| allocator.allocation_error("reserve group-key columns"))?;
        for data_type in &tracked_key_types {
            key_columns.push(key_column_from_type_in(data_type, allocator.clone())?);
        }
        let key_columns_retained_bytes = key_columns
            .iter()
            .map(KeyColumn::retained_bytes)
            .fold(0usize, usize::saturating_add);
        Ok(Self {
            key_strategy,
            key_types: tracked_key_types,
            key_columns,
            varlen_table: RawTable::new_in(allocator.clone()),
            fixed_size_table: RawTable::new_in(allocator.clone()),
            compressed_table: RawTable::new_in(allocator.clone()),
            one_number_table: RawTable::new_in(allocator.clone()),
            one_string_null: None,
            dict_key_map: DictKeyMap::new_in(allocator.clone()),
            row_storage: RowStorage::new_with_tracker(
                64 * 1024,
                MemTracker::new_child("RowStorage", &tracker),
            ),
            varlen_keys: AggregateVec::new_in(allocator),
            compressed_ctx: None,
            key_columns_retained_bytes,
            memory_limit_exceeded: false,
            hash_seed: seed_from_hasher(&DefaultHashBuilder::default()),
        })
    }

    pub fn key_strategy(&self) -> GroupKeyStrategy {
        self.key_strategy
    }

    pub fn key_types(&self) -> &[DataType] {
        &self.key_types
    }

    pub(crate) fn key_columns(&self) -> &[KeyColumn] {
        &self.key_columns
    }

    pub fn hash_seed(&self) -> u64 {
        self.hash_seed
    }

    pub(crate) fn compressed_ctx(&self) -> Option<&CompressedKeyContext> {
        self.compressed_ctx.as_ref()
    }

    #[allow(dead_code)]
    pub fn group_count(&self) -> usize {
        self.varlen_keys.len()
    }

    pub fn ensure_compressed_ctx(&mut self, views: &[GroupKeyArrayView<'_>]) -> Result<(), String> {
        self.ensure_memory_available()?;
        if self.key_strategy != GroupKeyStrategy::CompressedFixed {
            return Ok(());
        }
        if self.compressed_ctx.is_some() {
            return Ok(());
        }
        match build_compressed_key_context(
            views,
            &self.key_types,
            self.compressed_table.allocator().clone(),
        ) {
            Ok(ctx) => {
                self.compressed_ctx = Some(ctx);
                Ok(())
            }
            Err(error) if error.contains("ResourceExhausted") => {
                self.memory_limit_exceeded = true;
                Err(error)
            }
            Err(_) => {
                self.key_strategy = GroupKeyStrategy::Serialized;
                Ok(())
            }
        }
    }

    pub(crate) fn build_rows_fallback(
        &self,
        arrays: &[ArrayRef],
    ) -> Result<AggregateVec<AggregateVec<u8>>, String> {
        let allocator = self.varlen_table.allocator().clone();
        let Some(first) = arrays.first() else {
            return Ok(AggregateVec::new_in(allocator));
        };
        let num_rows = first.len();
        for (idx, array) in arrays.iter().enumerate() {
            if array.len() != num_rows {
                return Err(format!(
                    "group key column row count mismatch in fallback row encoding: idx={} expected_rows={} actual_rows={}",
                    idx,
                    num_rows,
                    array.len()
                ));
            }
        }
        let mut rows = AggregateVec::new_in(allocator.clone());
        rows.try_reserve_exact(num_rows)
            .map_err(|_| allocator.allocation_error("reserve fallback group-key rows"))?;
        for row in 0..num_rows {
            let mut encoded = AggregateVec::new_in(allocator.clone());
            for array in arrays {
                match encode_group_key_row_tracked(array, row, allocator.clone())? {
                    None => {
                        encoded.try_reserve(1).map_err(|_| {
                            allocator.allocation_error("grow fallback group-key row")
                        })?;
                        encoded.push(0);
                    }
                    Some(value) => {
                        let additional = 5usize
                            .checked_add(value.len())
                            .ok_or_else(|| "fallback group row length overflow".to_string())?;
                        encoded.try_reserve(additional).map_err(|_| {
                            allocator.allocation_error("grow fallback group-key row")
                        })?;
                        encoded.push(1);
                        let len = u32::try_from(value.len()).map_err(|_| {
                            "fallback group row encoded value length overflow".to_string()
                        })?;
                        encoded.extend_from_slice(&len.to_le_bytes());
                        encoded.extend_from_slice(&value);
                    }
                }
            }
            rows.push(encoded);
        }
        Ok(rows)
    }

    /// Returns all heap retained by the table, including row bytes, in O(1).
    #[cfg(test)]
    pub(crate) fn retained_bytes(&self) -> usize {
        self.own_retained_bytes()
            .saturating_add(self.row_storage.retained_bytes())
    }

    #[cfg(test)]
    fn own_retained_bytes(&self) -> usize {
        let raw_tables = raw_table_retained_bytes(&self.varlen_table)
            .saturating_add(raw_table_retained_bytes(&self.fixed_size_table))
            .saturating_add(raw_table_retained_bytes(&self.compressed_table))
            .saturating_add(raw_table_retained_bytes(&self.one_number_table));
        let compressed = self
            .compressed_ctx
            .as_ref()
            .map(compressed_context_retained_bytes)
            .unwrap_or(0);
        self.key_types
            .capacity()
            .saturating_mul(size_of::<DataType>())
            .saturating_add(
                self.key_columns
                    .capacity()
                    .saturating_mul(size_of::<KeyColumn>()),
            )
            .saturating_add(raw_tables)
            .saturating_add(self.key_columns_retained_bytes)
            .saturating_add(
                self.varlen_keys
                    .capacity()
                    .saturating_mul(size_of::<RowKey>()),
            )
            .saturating_add(
                self.dict_key_map
                    .code_to_group
                    .capacity()
                    .saturating_mul(size_of::<Option<usize>>()),
            )
            .saturating_add(compressed)
    }

    fn ensure_memory_available(&self) -> Result<(), String> {
        if self.memory_limit_exceeded {
            Err("key table memory limit was previously exceeded".to_string())
        } else {
            Ok(())
        }
    }

    fn push_column_value(
        &mut self,
        column_index: usize,
        view: &GroupKeyArrayView<'_>,
        row: usize,
    ) -> Result<(), String> {
        self.ensure_memory_available()?;
        let column = self
            .key_columns
            .get_mut(column_index)
            .ok_or_else(|| "group key column missing".to_string())?;
        // Capture the cached baseline before reserve: a successful reserve may
        // grow one of the tracked backing vectors even though the logical push
        // has not happened yet.
        let before = column.retained_bytes();
        if let Err(error) = column.try_reserve_value_from_view(view, row) {
            self.memory_limit_exceeded = true;
            return Err(error);
        }
        let result = column.push_value_from_view(view, row);
        let after = column.retained_bytes();
        self.key_columns_retained_bytes = self
            .key_columns_retained_bytes
            .checked_sub(before)
            .expect("key column retained-memory cache underflow")
            .saturating_add(after);
        result
    }

    fn alloc_row_copy(&mut self, bytes: &[u8]) -> Result<RowKey, String> {
        match self.row_storage.alloc_copy(bytes) {
            Ok(key) => Ok(key),
            Err(error) => {
                self.memory_limit_exceeded = true;
                Err(error)
            }
        }
    }

    pub fn find_or_insert_from_row(
        &mut self,
        views: &[GroupKeyArrayView<'_>],
        row: usize,
        row_bytes: &[u8],
        hash: u64,
    ) -> Result<KeyLookup, String> {
        self.ensure_memory_available()?;
        if let Err(error) = reserve_raw_table(&mut self.varlen_table, "serialized group-key table")
        {
            self.memory_limit_exceeded = true;
            return Err(error);
        }
        let mut error = None;
        let result = {
            let keys = &self.varlen_keys;
            let table = &mut self.varlen_table;
            table.find_or_find_insert_slot(
                hash,
                |entry| match keys.get(entry.group_id) {
                    Some(stored) => stored.as_slice() == row_bytes,
                    None => {
                        error = Some("group key index out of bounds".to_string());
                        false
                    }
                },
                |entry| entry.hash,
            )
        };
        if let Some(err) = error {
            return Err(err);
        }

        match result {
            Ok(bucket) => Ok(KeyLookup {
                group_id: unsafe { bucket.as_ref().group_id },
                is_new: false,
            }),
            Err(slot) => {
                for (column_index, view) in views.iter().enumerate().take(self.key_columns.len()) {
                    self.push_column_value(column_index, view, row)?;
                }
                let group_id = self.alloc_group()?;
                let stored_key = self.alloc_row_copy(row_bytes)?;
                if let Some(slot_key) = self.varlen_keys.get_mut(group_id) {
                    *slot_key = stored_key;
                } else {
                    return Err("group key index out of bounds".to_string());
                }
                let entry = KeyEntry { group_id, hash };
                unsafe {
                    self.varlen_table.insert_in_slot(hash, slot, entry);
                }
                Ok(KeyLookup {
                    group_id,
                    is_new: true,
                })
            }
        }
    }

    pub fn find_or_insert_one_number(
        &mut self,
        view: &GroupKeyArrayView<'_>,
        row: usize,
        hash: u64,
    ) -> Result<KeyLookup, String> {
        self.ensure_memory_available()?;
        if let Err(error) =
            reserve_raw_table(&mut self.one_number_table, "one-number group-key table")
        {
            self.memory_limit_exceeded = true;
            return Err(error);
        }
        let mut error = None;
        let result = {
            let key_columns = &self.key_columns;
            let table = &mut self.one_number_table;
            table.find_or_find_insert_slot(
                hash,
                |entry| {
                    let col = key_columns.first();
                    match col {
                        Some(col) => match col.value_equals(entry.group_id, view, row) {
                            Ok(equal) => equal,
                            Err(err) => {
                                error = Some(err);
                                false
                            }
                        },
                        None => {
                            error = Some("one number key column missing".to_string());
                            false
                        }
                    }
                },
                |entry| entry.hash,
            )
        };
        if let Some(err) = error {
            return Err(err);
        }

        match result {
            Ok(bucket) => Ok(KeyLookup {
                group_id: unsafe { bucket.as_ref().group_id },
                is_new: false,
            }),
            Err(slot) => {
                if self.key_columns.is_empty() {
                    return Err("one number key column missing".to_string());
                }
                self.push_column_value(0, view, row)?;
                let group_id = self.alloc_group()?;
                let entry = KeyEntry { group_id, hash };
                let table = &mut self.one_number_table;
                unsafe {
                    table.insert_in_slot(hash, slot, entry);
                }
                Ok(KeyLookup {
                    group_id,
                    is_new: true,
                })
            }
        }
    }

    pub fn find_or_insert_one_string(
        &mut self,
        view: &GroupKeyArrayView<'_>,
        row: usize,
        key: Option<&str>,
        hash: u64,
    ) -> Result<KeyLookup, String> {
        self.ensure_memory_available()?;
        if !matches!(view, GroupKeyArrayView::Utf8(_)) {
            return Err("one string key expects Utf8 view".to_string());
        }
        self.find_or_insert_one_string_value(view, row, key, hash)
    }

    pub fn find_or_insert_one_string_like(
        &mut self,
        view: &GroupKeyArrayView<'_>,
        row: usize,
        hash: u64,
    ) -> Result<KeyLookup, String> {
        self.ensure_memory_available()?;
        match view {
            GroupKeyArrayView::Utf8(arr) => {
                let key = (!arr.is_null(row)).then(|| arr.value(row));
                self.find_or_insert_one_string_value(view, row, key, hash)
            }
            GroupKeyArrayView::Dictionary(dict) => {
                let Some(code) = dict.code_at(row)? else {
                    return self.find_or_insert_one_string_value(view, row, None, hash);
                };
                if let Err(error) = self
                    .dict_key_map
                    .reset_for(dict.values_ptr(), dict.values_len())
                {
                    self.memory_limit_exceeded = true;
                    return Err(error);
                }
                if let Some(Some(group_id)) = self.dict_key_map.code_to_group.get(code).copied() {
                    let col = self
                        .key_columns
                        .first()
                        .ok_or_else(|| "one string key column missing".to_string())?;
                    if col.value_equals(group_id, view, row)? {
                        return Ok(KeyLookup {
                            group_id,
                            is_new: false,
                        });
                    }
                }
                let key = dict.value_str_for_code(code)?;
                let lookup = self.find_or_insert_one_string_value(view, row, Some(key), hash)?;
                let slot = self
                    .dict_key_map
                    .code_to_group
                    .get_mut(code)
                    .ok_or_else(|| "dictionary group key code out of bounds".to_string())?;
                *slot = Some(lookup.group_id);
                Ok(lookup)
            }
            _ => Err("one string key expects Utf8 or Dictionary view".to_string()),
        }
    }

    fn find_or_insert_one_string_value(
        &mut self,
        view: &GroupKeyArrayView<'_>,
        row: usize,
        key: Option<&str>,
        hash: u64,
    ) -> Result<KeyLookup, String> {
        self.ensure_memory_available()?;
        let row_is_null = match view {
            GroupKeyArrayView::Utf8(arr) => arr.is_null(row),
            GroupKeyArrayView::Dictionary(dict) => dict.is_null(row),
            _ => return Err("one string key expects Utf8 or Dictionary view".to_string()),
        };
        match key {
            Some(key) => {
                if row_is_null {
                    return Err("one string key requires non-null row".to_string());
                }
                self.find_or_insert_one_string_non_null(view, row, key, hash)
            }
            None => {
                if !row_is_null {
                    return Err("one string key null requires null row".to_string());
                }
                if let Some(group_id) = self.one_string_null {
                    return Ok(KeyLookup {
                        group_id,
                        is_new: false,
                    });
                }
                if self.key_columns.is_empty() {
                    return Err("one string key column missing".to_string());
                }
                self.push_column_value(0, view, row)?;
                let group_id = self.alloc_group()?;
                self.one_string_null = Some(group_id);
                Ok(KeyLookup {
                    group_id,
                    is_new: true,
                })
            }
        }
    }

    fn find_or_insert_one_string_non_null(
        &mut self,
        view: &GroupKeyArrayView<'_>,
        row: usize,
        key: &str,
        hash: u64,
    ) -> Result<KeyLookup, String> {
        self.ensure_memory_available()?;
        if let Err(error) = reserve_raw_table(&mut self.varlen_table, "string group-key table") {
            self.memory_limit_exceeded = true;
            return Err(error);
        }
        let mut error = None;
        let key_bytes = key.as_bytes();
        let result = {
            let keys = &self.varlen_keys;
            let table = &mut self.varlen_table;
            table.find_or_find_insert_slot(
                hash,
                |entry| match keys.get(entry.group_id) {
                    Some(stored) => stored.as_slice() == key_bytes,
                    None => {
                        error = Some("group key index out of bounds".to_string());
                        false
                    }
                },
                |entry| entry.hash,
            )
        };
        if let Some(err) = error {
            return Err(err);
        }
        match result {
            Ok(bucket) => Ok(KeyLookup {
                group_id: unsafe { bucket.as_ref().group_id },
                is_new: false,
            }),
            Err(slot) => {
                if self.key_columns.is_empty() {
                    return Err("one string key column missing".to_string());
                }
                self.push_column_value(0, view, row)?;
                let group_id = self.alloc_group()?;
                let stored_key = self.alloc_row_copy(key_bytes)?;
                if let Some(slot_key) = self.varlen_keys.get_mut(group_id) {
                    *slot_key = stored_key;
                } else {
                    return Err("group key index out of bounds".to_string());
                }
                let entry = KeyEntry { group_id, hash };
                unsafe {
                    self.varlen_table.insert_in_slot(hash, slot, entry);
                }
                Ok(KeyLookup {
                    group_id,
                    is_new: true,
                })
            }
        }
    }

    pub fn find_or_insert_fixed_size(
        &mut self,
        views: &[GroupKeyArrayView<'_>],
        row: usize,
        hash: u64,
    ) -> Result<KeyLookup, String> {
        self.ensure_memory_available()?;
        if let Err(error) =
            reserve_raw_table(&mut self.fixed_size_table, "fixed-size group-key table")
        {
            self.memory_limit_exceeded = true;
            return Err(error);
        }
        let mut error = None;
        let result = {
            let key_columns = &self.key_columns;
            let table = &mut self.fixed_size_table;
            table.find_or_find_insert_slot(
                hash,
                |entry| match keys_equal(key_columns, views, entry.group_id, row) {
                    Ok(equal) => equal,
                    Err(err) => {
                        error = Some(err);
                        false
                    }
                },
                |entry| entry.hash,
            )
        };
        if let Some(err) = error {
            return Err(err);
        }

        match result {
            Ok(bucket) => Ok(KeyLookup {
                group_id: unsafe { bucket.as_ref().group_id },
                is_new: false,
            }),
            Err(slot) => {
                for (column_index, view) in views.iter().enumerate().take(self.key_columns.len()) {
                    self.push_column_value(column_index, view, row)?;
                }
                let group_id = self.alloc_group()?;
                let entry = KeyEntry { group_id, hash };
                let table = &mut self.fixed_size_table;
                unsafe {
                    table.insert_in_slot(hash, slot, entry);
                }
                Ok(KeyLookup {
                    group_id,
                    is_new: true,
                })
            }
        }
    }

    pub fn find_or_insert_compressed(
        &mut self,
        views: &[GroupKeyArrayView<'_>],
        row: usize,
        hash: u64,
    ) -> Result<KeyLookup, String> {
        self.ensure_memory_available()?;
        if let Err(error) =
            reserve_raw_table(&mut self.compressed_table, "compressed group-key table")
        {
            self.memory_limit_exceeded = true;
            return Err(error);
        }
        let mut error = None;
        let result = {
            let key_columns = &self.key_columns;
            let table = &mut self.compressed_table;
            table.find_or_find_insert_slot(
                hash,
                |entry| match keys_equal(key_columns, views, entry.group_id, row) {
                    Ok(equal) => equal,
                    Err(err) => {
                        error = Some(err);
                        false
                    }
                },
                |entry| entry.hash,
            )
        };
        if let Some(err) = error {
            return Err(err);
        }

        match result {
            Ok(bucket) => Ok(KeyLookup {
                group_id: unsafe { bucket.as_ref().group_id },
                is_new: false,
            }),
            Err(slot) => {
                for (column_index, view) in views.iter().enumerate().take(self.key_columns.len()) {
                    self.push_column_value(column_index, view, row)?;
                }
                let group_id = self.alloc_group()?;
                let entry = KeyEntry { group_id, hash };
                let table = &mut self.compressed_table;
                unsafe {
                    table.insert_in_slot(hash, slot, entry);
                }
                Ok(KeyLookup {
                    group_id,
                    is_new: true,
                })
            }
        }
    }

    pub fn lookup_serialized(&self, row_bytes: &[u8], hash: u64) -> Result<Option<usize>, String> {
        let keys = &self.varlen_keys;
        let mut error = None;
        let entry = self
            .varlen_table
            .get(hash, |entry| match keys.get(entry.group_id) {
                Some(stored) => stored.as_slice() == row_bytes,
                None => {
                    error = Some("group key index out of bounds".to_string());
                    false
                }
            });
        if let Some(err) = error {
            return Err(err);
        }
        Ok(entry.map(|entry| entry.group_id))
    }

    pub fn lookup_one_string(&self, key: &str, hash: u64) -> Result<Option<usize>, String> {
        let keys = &self.varlen_keys;
        let key_bytes = key.as_bytes();
        let mut error = None;
        let entry = self
            .varlen_table
            .get(hash, |entry| match keys.get(entry.group_id) {
                Some(stored) => stored.as_slice() == key_bytes,
                None => {
                    error = Some("group key index out of bounds".to_string());
                    false
                }
            });
        if let Some(err) = error {
            return Err(err);
        }
        Ok(entry.map(|entry| entry.group_id))
    }

    pub fn lookup_one_string_null(&self) -> Option<usize> {
        self.one_string_null
    }

    pub fn lookup_one_number(
        &self,
        view: &GroupKeyArrayView<'_>,
        row: usize,
        hash: u64,
    ) -> Result<Option<usize>, String> {
        let mut error = None;
        let entry = self.one_number_table.get(hash, |entry| {
            let col = self.key_columns.first();
            match col {
                Some(col) => match col.value_equals(entry.group_id, view, row) {
                    Ok(equal) => equal,
                    Err(err) => {
                        error = Some(err);
                        false
                    }
                },
                None => {
                    error = Some("one number key column missing".to_string());
                    false
                }
            }
        });
        if let Some(err) = error {
            return Err(err);
        }
        Ok(entry.map(|entry| entry.group_id))
    }

    pub fn lookup_fixed_size(
        &self,
        views: &[GroupKeyArrayView<'_>],
        row: usize,
        hash: u64,
    ) -> Result<Option<usize>, String> {
        let mut error = None;
        let entry = self.fixed_size_table.get(hash, |entry| {
            match keys_equal(&self.key_columns, views, entry.group_id, row) {
                Ok(equal) => equal,
                Err(err) => {
                    error = Some(err);
                    false
                }
            }
        });
        if let Some(err) = error {
            return Err(err);
        }
        Ok(entry.map(|entry| entry.group_id))
    }

    pub fn lookup_compressed(
        &self,
        views: &[GroupKeyArrayView<'_>],
        row: usize,
        hash: u64,
    ) -> Result<Option<usize>, String> {
        let mut error = None;
        let entry = self.compressed_table.get(hash, |entry| {
            match keys_equal(&self.key_columns, views, entry.group_id, row) {
                Ok(equal) => equal,
                Err(err) => {
                    error = Some(err);
                    false
                }
            }
        });
        if let Some(err) = error {
            return Err(err);
        }
        Ok(entry.map(|entry| entry.group_id))
    }

    pub fn build_compressed_flags(
        &self,
        views: &[GroupKeyArrayView<'_>],
        num_rows: usize,
    ) -> Result<Vec<bool>, String> {
        let ctx = self
            .compressed_ctx
            .as_ref()
            .ok_or_else(|| "compressed key context missing".to_string())?;
        build_compressed_flags(ctx, views, num_rows)
    }

    pub fn build_group_hashes(
        &self,
        views: &[GroupKeyArrayView<'_>],
        num_rows: usize,
    ) -> Result<Vec<u64>, String> {
        build_group_key_hashes(views, num_rows, self.hash_seed)
    }

    pub fn build_one_number_hashes(
        &self,
        view: &GroupKeyArrayView<'_>,
        num_rows: usize,
    ) -> Result<Vec<u64>, String> {
        build_one_number_hashes(view, num_rows, self.hash_seed)
    }

    fn alloc_group(&mut self) -> Result<usize, String> {
        let group_id = self.varlen_keys.len();
        if self.varlen_keys.try_reserve(1).is_err() {
            self.memory_limit_exceeded = true;
            return Err(self
                .varlen_keys
                .allocator()
                .allocation_error("reserve group-key row pointers"));
        }
        self.varlen_keys.push(RowKey::empty());
        Ok(group_id)
    }
}

#[cfg(test)]
fn compressed_context_retained_bytes(ctx: &CompressedKeyContext) -> usize {
    ctx.bases
        .capacity()
        .saturating_mul(size_of::<i128>())
        .saturating_add(ctx.used_bits.capacity().saturating_mul(size_of::<u8>()))
        .saturating_add(ctx.null_offsets.capacity().saturating_mul(size_of::<u16>()))
        .saturating_add(
            ctx.value_offsets
                .capacity()
                .saturating_mul(size_of::<u16>()),
        )
}

fn reserve_raw_table(
    table: &mut RawTable<KeyEntry, AggregateAllocator>,
    owner: &str,
) -> Result<(), String> {
    let allocator = table.allocator().clone();
    table
        .try_reserve(1, |entry| entry.hash)
        .map_err(|_| allocator.allocation_error(&format!("reserve {owner}")))
}

#[cfg(test)]
fn raw_table_retained_bytes<T, A: allocator_api2::alloc::Allocator>(
    table: &RawTable<T, A>,
) -> usize {
    if table.capacity() == 0 {
        return 0;
    }

    // Mirrors hashbrown 0.14's RawTable allocation layout: all buckets,
    // alignment padding, one control byte per bucket, and one cloned SIMD
    // control group. This is allocator-requested capacity, not live entries.
    #[cfg(all(
        target_feature = "sse2",
        any(target_arch = "x86", target_arch = "x86_64"),
        not(miri)
    ))]
    const GROUP_WIDTH: usize = 16;
    #[cfg(all(
        not(all(
            target_feature = "sse2",
            any(target_arch = "x86", target_arch = "x86_64"),
            not(miri)
        )),
        target_arch = "aarch64",
        target_feature = "neon",
        target_endian = "little",
        not(miri)
    ))]
    const GROUP_WIDTH: usize = 8;
    #[cfg(not(any(
        all(
            target_feature = "sse2",
            any(target_arch = "x86", target_arch = "x86_64"),
            not(miri)
        ),
        all(
            target_arch = "aarch64",
            target_feature = "neon",
            target_endian = "little",
            not(miri)
        )
    )))]
    const GROUP_WIDTH: usize = size_of::<usize>();

    let buckets = table.buckets();
    let ctrl_align = align_of::<T>().max(GROUP_WIDTH);
    let bucket_bytes = size_of::<T>().saturating_mul(buckets);
    let ctrl_offset = bucket_bytes.saturating_add(ctrl_align - 1) & !(ctrl_align - 1);
    ctrl_offset
        .saturating_add(buckets)
        .saturating_add(GROUP_WIDTH)
}

fn keys_equal(
    key_columns: &[KeyColumn],
    views: &[GroupKeyArrayView<'_>],
    group_id: usize,
    row: usize,
) -> Result<bool, String> {
    if key_columns.len() != views.len() {
        return Err("group key length mismatch".to_string());
    }
    for (col, view) in key_columns.iter().zip(views.iter()) {
        if !col.value_equals(group_id, view, row)? {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::hash_table::key_builder::{GroupKeyArrayView, build_group_key_views};
    use arrow::array::{Array, ArrayRef, DictionaryArray, Int64Array, ListArray, UInt32Array};
    use arrow::compute::take;
    use arrow::datatypes::{DataType, Field, Int32Type};
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use std::sync::Arc;

    fn dict_utf8(values: Vec<Option<&str>>) -> ArrayRef {
        Arc::new(values.into_iter().collect::<DictionaryArray<Int32Type>>())
    }

    #[test]
    fn dictionary_cache_resets_when_values_ptr_changes() {
        let mut table = KeyTable::new(vec![DataType::Utf8], true).expect("table");

        let first = dict_utf8(vec![Some("A"), Some("B")]);
        let first_arrays = [first];
        let first_views = build_group_key_views(&first_arrays).expect("first views");
        let first_hashes = table
            .build_group_hashes(&first_views, 2)
            .expect("first hashes");
        let a = table
            .find_or_insert_one_string_like(&first_views[0], 0, first_hashes[0])
            .expect("insert A");
        let b = table
            .find_or_insert_one_string_like(&first_views[0], 1, first_hashes[1])
            .expect("insert B");
        assert_ne!(a.group_id, b.group_id);

        let second = dict_utf8(vec![Some("B"), Some("A")]);
        let second_arrays = [second];
        let second_views = build_group_key_views(&second_arrays).expect("second views");
        let second_hashes = table
            .build_group_hashes(&second_views, 2)
            .expect("second hashes");
        let b_again = table
            .find_or_insert_one_string_like(&second_views[0], 0, second_hashes[0])
            .expect("lookup B after values ptr change");
        let a_again = table
            .find_or_insert_one_string_like(&second_views[0], 1, second_hashes[1])
            .expect("lookup A after values ptr change");

        assert_eq!(b_again.group_id, b.group_id);
        assert_eq!(a_again.group_id, a.group_id);
        assert_eq!(table.group_count(), 2);
    }

    #[test]
    fn dictionary_cache_reuses_same_values_identity_for_duplicate_code() {
        let mut table = KeyTable::new(vec![DataType::Utf8], true).expect("table");

        let dict = dict_utf8(vec![Some("A"), Some("B"), Some("A")]);
        let arrays = [dict];
        let views = build_group_key_views(&arrays).expect("views");
        let hashes = table.build_group_hashes(&views, 3).expect("hashes");
        let a = table
            .find_or_insert_one_string_like(&views[0], 0, hashes[0])
            .expect("insert A");
        let b = table
            .find_or_insert_one_string_like(&views[0], 1, hashes[1])
            .expect("insert B");
        let a_again = table
            .find_or_insert_one_string_like(&views[0], 2, hashes[2])
            .expect("lookup A");

        assert_ne!(a.group_id, b.group_id);
        assert_eq!(a_again.group_id, a.group_id);
        assert!(!a_again.is_new);
        assert_eq!(table.group_count(), 2);
    }

    #[test]
    fn dictionary_cache_verifies_stale_entry_before_returning() {
        let mut table = KeyTable::new(vec![DataType::Utf8], true).expect("table");

        let first = dict_utf8(vec![Some("A"), Some("B")]);
        let first_arrays = [first];
        let first_views = build_group_key_views(&first_arrays).expect("first views");
        let first_hashes = table
            .build_group_hashes(&first_views, 2)
            .expect("first hashes");
        let a = table
            .find_or_insert_one_string_like(&first_views[0], 0, first_hashes[0])
            .expect("insert A");
        let b = table
            .find_or_insert_one_string_like(&first_views[0], 1, first_hashes[1])
            .expect("insert B");

        let second = dict_utf8(vec![Some("B"), Some("A")]);
        let second_arrays = [second];
        let second_views = build_group_key_views(&second_arrays).expect("second views");
        let second_hashes = table
            .build_group_hashes(&second_views, 2)
            .expect("second hashes");
        let GroupKeyArrayView::Dictionary(dict) = &second_views[0] else {
            panic!("expected dictionary view");
        };
        table.dict_key_map.values_ptr = Some(dict.values_ptr());
        table
            .dict_key_map
            .code_to_group
            .try_reserve_exact(2)
            .expect("reserve test dictionary map");
        table
            .dict_key_map
            .code_to_group
            .extend([Some(a.group_id), Some(b.group_id)]);

        let b_again = table
            .find_or_insert_one_string_like(&second_views[0], 0, second_hashes[0])
            .expect("lookup B with stale code cache");

        assert_eq!(b_again.group_id, b.group_id);
        assert!(!b_again.is_new);
        assert_eq!(table.dict_key_map.code_to_group[0], Some(b.group_id));
    }

    #[test]
    fn dictionary_null_key_reuses_existing_null_group() {
        let mut table = KeyTable::new(vec![DataType::Utf8], true).expect("table");

        let dict = dict_utf8(vec![None, Some("A"), None]);
        let arrays = [dict];
        let views = build_group_key_views(&arrays).expect("views");
        let hashes = table.build_group_hashes(&views, 3).expect("hashes");
        let null = table
            .find_or_insert_one_string_like(&views[0], 0, hashes[0])
            .expect("insert null");
        let a = table
            .find_or_insert_one_string_like(&views[0], 1, hashes[1])
            .expect("insert A");
        let null_again = table
            .find_or_insert_one_string_like(&views[0], 2, hashes[2])
            .expect("lookup null");

        assert_ne!(null.group_id, a.group_id);
        assert_eq!(null_again.group_id, null.group_id);
        assert!(!null_again.is_new);
        assert_eq!(table.group_count(), 2);
    }

    #[test]
    fn raw_table_resize_is_rejected_before_allocation() {
        let query = MemTracker::new_root("query");
        {
            let mut table =
                KeyTable::new_with_tracker(vec![DataType::Int64], true, Arc::clone(&query))
                    .expect("table");
            let baseline = table.retained_bytes();
            assert_eq!(query.current(), baseline as i64);
            query
                .install_limit_once((baseline + 1) as i64)
                .expect("install limit");

            let arrays = [Arc::new(Int64Array::from(vec![Some(7)])) as ArrayRef];
            let views = build_group_key_views(&arrays).expect("views");
            let hashes = table.build_one_number_hashes(&views[0], 1).expect("hash");
            let error = table
                .find_or_insert_one_number(&views[0], 0, hashes[0])
                .err()
                .expect("raw table allocation must cross the limit");
            assert!(error.contains("ResourceExhausted"), "{error}");

            assert_eq!(table.retained_bytes(), baseline);
            assert_eq!(query.current(), baseline as i64);
            assert_eq!(table.group_count(), 0);

            let current = query.current();
            let retry_error = table
                .find_or_insert_one_number(&views[0], 0, hashes[0])
                .err()
                .expect("limit failure must be latched");
            assert!(retry_error.contains("previously exceeded"), "{retry_error}");
            assert_eq!(query.current(), current);
        }
        assert_eq!(query.current(), 0);
    }

    #[test]
    fn key_column_growth_is_rejected_before_allocation() {
        let query = MemTracker::new_root("query");
        {
            let mut table =
                KeyTable::new_with_tracker(vec![DataType::Int64], true, Arc::clone(&query))
                    .expect("table");
            let baseline = table.retained_bytes();
            assert_eq!(query.current(), baseline as i64);
            query
                .install_limit_once((baseline + 1) as i64)
                .expect("install limit");

            let arrays = [Arc::new(Int64Array::from(vec![Some(7)])) as ArrayRef];
            let views = build_group_key_views(&arrays).expect("views");
            let error = table
                .push_column_value(0, &views[0], 0)
                .expect_err("key-column vector growth must cross the limit");
            assert!(error.contains("ResourceExhausted"), "{error}");
            assert_eq!(table.retained_bytes(), baseline);
            assert_eq!(query.current(), baseline as i64);

            let retry = table
                .push_column_value(0, &views[0], 0)
                .expect_err("limit rejection must latch the key table");
            assert!(retry.contains("previously exceeded"), "{retry}");
            assert_eq!(query.current(), baseline as i64);
        }
        assert_eq!(query.current(), 0);
    }

    #[test]
    fn dictionary_reset_growth_is_limit_checked_without_untracked_capacity() {
        let query = MemTracker::new_root("query");
        {
            let mut table =
                KeyTable::new_with_tracker(vec![DataType::Utf8], true, Arc::clone(&query))
                    .expect("table");
            let baseline = table.retained_bytes();
            query
                .install_limit_once((baseline + 1) as i64)
                .expect("install limit");

            let dict = dict_utf8(vec![Some("A"), Some("B")]);
            let arrays = [dict];
            let views = build_group_key_views(&arrays).expect("views");
            let hashes = table.build_group_hashes(&views, 2).expect("hashes");
            let error = table
                .find_or_insert_one_string_like(&views[0], 0, hashes[0])
                .err()
                .expect("dictionary cache growth must cross the limit");
            assert!(error.contains("ResourceExhausted"), "{error}");
            assert_eq!(table.dict_key_map.code_to_group.capacity(), 0);
            assert_eq!(table.group_count(), 0);
            assert_eq!(query.current(), table.retained_bytes() as i64);
        }
        assert_eq!(query.current(), 0);
    }

    #[test]
    fn successful_string_insert_tracks_all_owned_capacity_and_releases_on_drop() {
        let query = MemTracker::new_root("query");
        {
            let mut table =
                KeyTable::new_with_tracker(vec![DataType::Utf8], true, Arc::clone(&query))
                    .expect("table");
            let baseline = table.retained_bytes();

            let array = Arc::new(arrow::array::StringArray::from(vec![Some(
                "a retained variable-length key",
            )])) as ArrayRef;
            let arrays = [array];
            let views = build_group_key_views(&arrays).expect("views");
            let hashes = table.build_group_hashes(&views, 1).expect("hashes");
            table
                .find_or_insert_one_string_like(&views[0], 0, hashes[0])
                .expect("insert string key");

            assert_eq!(table.group_count(), 1);
            assert!(table.retained_bytes() > baseline);
            assert_eq!(query.current(), table.retained_bytes() as i64);
        }
        assert_eq!(query.current(), 0);
    }

    #[test]
    fn key_column_retained_cache_matches_columns_after_multi_column_insert() {
        let mut table = KeyTable::new(vec![DataType::Int64, DataType::Utf8], false).expect("table");
        let arrays = [
            Arc::new(Int64Array::from(vec![Some(7)])) as ArrayRef,
            Arc::new(arrow::array::StringArray::from(vec![Some("retained-key")])) as ArrayRef,
        ];
        let views = build_group_key_views(&arrays).expect("views");
        let rows = table.build_rows_fallback(&arrays).expect("rows");
        let hashes = table.build_group_hashes(&views, 1).expect("hashes");
        table
            .find_or_insert_from_row(&views, 0, rows[0].as_slice(), hashes[0])
            .expect("insert key");

        let scanned = table
            .key_columns
            .iter()
            .map(KeyColumn::retained_bytes)
            .fold(0usize, usize::saturating_add);
        assert_eq!(table.key_columns_retained_bytes, scanned);
    }

    fn complex_list_array() -> ArrayRef {
        Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            OffsetBuffer::new(vec![0, 2, 4, 5, 5].into()),
            Arc::new(Int64Array::from(vec![
                Some(1),
                None,
                Some(1),
                None,
                Some(2),
            ])),
            Some(NullBuffer::from(vec![true, true, true, false])),
        ))
    }

    #[test]
    fn complex_key_lookup_output_and_drop_are_exact_tracked() {
        let query = MemTracker::new_root("query");
        {
            let array = complex_list_array();
            let arrays = [Arc::clone(&array)];
            let views = build_group_key_views(&arrays).expect("views");
            let mut table = KeyTable::new_with_tracker(
                vec![array.data_type().clone()],
                false,
                Arc::clone(&query),
            )
            .expect("table");
            let rows = table.build_rows_fallback(&arrays).expect("fallback rows");
            let hashes = table
                .build_group_hashes(&views, array.len())
                .expect("hashes");
            let mut lookups = Vec::new();
            for row in 0..array.len() {
                lookups.push(
                    table
                        .find_or_insert_from_row(&views, row, rows[row].as_slice(), hashes[row])
                        .expect("lookup"),
                );
            }
            assert_eq!(lookups[0].group_id, lookups[1].group_id);
            assert!(!lookups[1].is_new);
            assert_ne!(lookups[0].group_id, lookups[2].group_id);
            assert_ne!(lookups[2].group_id, lookups[3].group_id);

            let output = table.key_columns()[0].to_array().expect("output keys");
            let expected = take(
                array.as_ref(),
                &UInt32Array::from(vec![0_u32, 2_u32, 3_u32]),
                None,
            )
            .expect("take expected rows");
            assert_eq!(output.as_ref(), expected.as_ref());
        }
        assert_eq!(query.current(), 0);
    }

    #[test]
    fn fallback_row_scratch_rejects_before_allocation() {
        let query = MemTracker::new_root("query");
        {
            let array = complex_list_array();
            let table = KeyTable::new_with_tracker(
                vec![array.data_type().clone()],
                false,
                Arc::clone(&query),
            )
            .expect("table");
            let baseline = query.current();
            query
                .install_limit_once(baseline.saturating_add(1))
                .expect("install limit");
            let error = table
                .build_rows_fallback(&[array])
                .expect_err("fallback scratch must be rejected before allocation");
            assert!(error.contains("ResourceExhausted"), "{error}");
            assert_eq!(query.current(), baseline);
        }
        assert_eq!(query.current(), 0);
    }
}
