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
use arrow::row::{RowConverter, Rows, SortField};
use hashbrown::raw::RawTable;
use std::sync::Arc;

use crate::exec::hash_table::hash::seed_from_hasher;
use crate::exec::hash_table::key_builder::{
    GroupKeyArrayView, build_compressed_flags, build_group_key_hashes, build_one_number_hashes,
    encode_group_key_row,
};
use crate::exec::hash_table::key_column::{KeyColumn, key_column_from_type};
use crate::exec::hash_table::key_layout::{CompressedKeyContext, build_compressed_key_context};
use crate::exec::hash_table::key_storage::{RowKey, RowStorage};
use crate::exec::hash_table::key_strategy::{GroupKeyStrategy, pick_group_key_strategy};
use hashbrown::hash_map::DefaultHashBuilder;
use novarocks_execution::runtime::mem_tracker::MemTracker;

#[derive(Clone, Copy, Debug)]
struct KeyEntry {
    group_id: usize,
    hash: u64,
}

#[derive(Default)]
struct DictKeyMap {
    values_ptr: Option<usize>,
    code_to_group: Vec<Option<usize>>,
}

impl DictKeyMap {
    fn reset_for(&mut self, values_ptr: usize, values_len: usize) {
        if self.values_ptr != Some(values_ptr) {
            self.values_ptr = Some(values_ptr);
            self.code_to_group.clear();
        }
        if self.code_to_group.len() < values_len {
            self.code_to_group.resize(values_len, None);
        }
    }
}

pub(crate) struct KeyLookup {
    pub(crate) group_id: usize,
    pub(crate) is_new: bool,
}

pub(crate) struct KeyTable {
    key_strategy: GroupKeyStrategy,
    key_types: Vec<DataType>,
    key_columns: Vec<KeyColumn>,
    varlen_table: RawTable<KeyEntry>,
    fixed_size_table: RawTable<KeyEntry>,
    compressed_table: RawTable<KeyEntry>,
    one_number_table: RawTable<KeyEntry>,
    one_string_null: Option<usize>,
    dict_key_map: DictKeyMap,
    row_storage: RowStorage,
    varlen_keys: Vec<RowKey>,
    compressed_ctx: Option<CompressedKeyContext>,
    row_converter: Option<RowConverter>,
    hash_seed: u64,
}

impl KeyTable {
    pub(crate) fn new(
        key_types: Vec<DataType>,
        enable_optimizations: bool,
    ) -> Result<Self, String> {
        let mut key_strategy = pick_group_key_strategy(&key_types);
        if !enable_optimizations {
            key_strategy = GroupKeyStrategy::Serialized;
        }
        let mut key_columns = Vec::with_capacity(key_types.len());
        for data_type in &key_types {
            key_columns.push(key_column_from_type(data_type)?);
        }
        let mut row_converter = None;
        if matches!(
            key_strategy,
            GroupKeyStrategy::Serialized | GroupKeyStrategy::CompressedFixed
        ) && !key_types.is_empty()
        {
            let fields = key_types
                .iter()
                .cloned()
                .map(SortField::new)
                .collect::<Vec<_>>();
            match RowConverter::new(fields) {
                Ok(converter) => {
                    row_converter = Some(converter);
                }
                Err(_) => {
                    // Nested key types are not supported by Arrow RowConverter. Keep serialized
                    // strategy and use fallback row-byte encoding at call sites.
                    key_strategy = GroupKeyStrategy::Serialized;
                }
            }
        }
        Ok(Self {
            key_strategy,
            key_types,
            key_columns,
            varlen_table: RawTable::new(),
            fixed_size_table: RawTable::new(),
            compressed_table: RawTable::new(),
            one_number_table: RawTable::new(),
            one_string_null: None,
            dict_key_map: DictKeyMap::default(),
            row_storage: RowStorage::new(64 * 1024),
            varlen_keys: Vec::new(),
            compressed_ctx: None,
            row_converter,
            hash_seed: seed_from_hasher(&DefaultHashBuilder::default()),
        })
    }

    pub(crate) fn key_strategy(&self) -> GroupKeyStrategy {
        self.key_strategy
    }

    pub(crate) fn key_types(&self) -> &[DataType] {
        &self.key_types
    }

    pub(crate) fn key_columns(&self) -> &[KeyColumn] {
        &self.key_columns
    }

    pub(crate) fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        let row_storage = MemTracker::new_child("RowStorage", &tracker);
        self.row_storage.set_mem_tracker(row_storage);
    }

    pub(crate) fn hash_seed(&self) -> u64 {
        self.hash_seed
    }

    pub(crate) fn compressed_ctx(&self) -> Option<&CompressedKeyContext> {
        self.compressed_ctx.as_ref()
    }

    #[allow(dead_code)]
    pub(crate) fn group_count(&self) -> usize {
        self.varlen_keys.len()
    }

    pub(crate) fn ensure_compressed_ctx(
        &mut self,
        views: &[GroupKeyArrayView<'_>],
    ) -> Result<(), String> {
        if self.key_strategy != GroupKeyStrategy::CompressedFixed {
            return Ok(());
        }
        if self.compressed_ctx.is_some() {
            return Ok(());
        }
        match build_compressed_key_context(views, &self.key_types) {
            Ok(ctx) => {
                self.compressed_ctx = Some(ctx);
                Ok(())
            }
            Err(_) => {
                self.key_strategy = GroupKeyStrategy::Serialized;
                Ok(())
            }
        }
    }

    pub(crate) fn build_rows(&self, arrays: &[ArrayRef]) -> Result<Rows, String> {
        let converter = self
            .row_converter
            .as_ref()
            .ok_or_else(|| "row converter not initialized".to_string())?;
        converter.convert_columns(arrays).map_err(|e| e.to_string())
    }

    pub(crate) fn build_rows_fallback(&self, arrays: &[ArrayRef]) -> Result<Vec<Vec<u8>>, String> {
        let Some(first) = arrays.first() else {
            return Ok(Vec::new());
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
        let mut rows = Vec::with_capacity(num_rows);
        for row in 0..num_rows {
            let mut encoded = Vec::new();
            for array in arrays {
                match encode_group_key_row(array, row)? {
                    None => encoded.push(0),
                    Some(value) => {
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

    pub(crate) fn find_or_insert_from_row(
        &mut self,
        views: &[GroupKeyArrayView<'_>],
        row: usize,
        row_bytes: &[u8],
        hash: u64,
    ) -> Result<KeyLookup, String> {
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
                for (col, view) in self.key_columns.iter_mut().zip(views.iter()) {
                    col.push_value_from_view(view, row)?;
                }
                let group_id = self.alloc_group()?;
                let stored_key = self.row_storage.alloc_copy(row_bytes);
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

    pub(crate) fn find_or_insert_one_number(
        &mut self,
        view: &GroupKeyArrayView<'_>,
        row: usize,
        hash: u64,
    ) -> Result<KeyLookup, String> {
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
                let col = self
                    .key_columns
                    .get_mut(0)
                    .ok_or_else(|| "one number key column missing".to_string())?;
                col.push_value_from_view(view, row)?;
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

    pub(crate) fn find_or_insert_one_string(
        &mut self,
        view: &GroupKeyArrayView<'_>,
        row: usize,
        key: Option<&str>,
        hash: u64,
    ) -> Result<KeyLookup, String> {
        if !matches!(view, GroupKeyArrayView::Utf8(_)) {
            return Err("one string key expects Utf8 view".to_string());
        }
        self.find_or_insert_one_string_value(view, row, key, hash)
    }

    pub(crate) fn find_or_insert_one_string_like(
        &mut self,
        view: &GroupKeyArrayView<'_>,
        row: usize,
        hash: u64,
    ) -> Result<KeyLookup, String> {
        match view {
            GroupKeyArrayView::Utf8(arr) => {
                let key = (!arr.is_null(row)).then(|| arr.value(row));
                self.find_or_insert_one_string_value(view, row, key, hash)
            }
            GroupKeyArrayView::Dictionary(dict) => {
                let Some(code) = dict.code_at(row)? else {
                    return self.find_or_insert_one_string_value(view, row, None, hash);
                };
                self.dict_key_map
                    .reset_for(dict.values_ptr(), dict.values_len());
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
                let col = self
                    .key_columns
                    .get_mut(0)
                    .ok_or_else(|| "one string key column missing".to_string())?;
                col.push_value_from_view(view, row)?;
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
                let col = self
                    .key_columns
                    .get_mut(0)
                    .ok_or_else(|| "one string key column missing".to_string())?;
                col.push_value_from_view(view, row)?;
                let group_id = self.alloc_group()?;
                let stored_key = self.row_storage.alloc_copy(key_bytes);
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

    pub(crate) fn find_or_insert_fixed_size(
        &mut self,
        views: &[GroupKeyArrayView<'_>],
        row: usize,
        hash: u64,
    ) -> Result<KeyLookup, String> {
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
                for (col, view) in self.key_columns.iter_mut().zip(views.iter()) {
                    col.push_value_from_view(view, row)?;
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

    pub(crate) fn find_or_insert_compressed(
        &mut self,
        views: &[GroupKeyArrayView<'_>],
        row: usize,
        hash: u64,
    ) -> Result<KeyLookup, String> {
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
                for (col, view) in self.key_columns.iter_mut().zip(views.iter()) {
                    col.push_value_from_view(view, row)?;
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

    pub(crate) fn lookup_serialized(
        &self,
        row_bytes: &[u8],
        hash: u64,
    ) -> Result<Option<usize>, String> {
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

    pub(crate) fn lookup_one_string(&self, key: &str, hash: u64) -> Result<Option<usize>, String> {
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

    pub(crate) fn lookup_one_string_null(&self) -> Option<usize> {
        self.one_string_null
    }

    pub(crate) fn lookup_one_number(
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

    pub(crate) fn lookup_fixed_size(
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

    pub(crate) fn lookup_compressed(
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

    pub(crate) fn build_compressed_flags(
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

    pub(crate) fn build_group_hashes(
        &self,
        views: &[GroupKeyArrayView<'_>],
        num_rows: usize,
    ) -> Result<Vec<u64>, String> {
        build_group_key_hashes(views, num_rows, self.hash_seed)
    }

    pub(crate) fn build_one_number_hashes(
        &self,
        view: &GroupKeyArrayView<'_>,
        num_rows: usize,
    ) -> Result<Vec<u64>, String> {
        build_one_number_hashes(view, num_rows, self.hash_seed)
    }

    fn alloc_group(&mut self) -> Result<usize, String> {
        let group_id = self.varlen_keys.len();
        self.varlen_keys.push(RowKey::empty());
        Ok(group_id)
    }
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
    use arrow::array::{ArrayRef, DictionaryArray};
    use arrow::datatypes::{DataType, Int32Type};
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
        table.dict_key_map.code_to_group = vec![Some(a.group_id), Some(b.group_id)];

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
}
