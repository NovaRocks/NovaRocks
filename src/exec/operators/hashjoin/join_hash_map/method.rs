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
//! M1 join hash map method wrapper.
//!
//! This module owns the join-facing hash map abstraction.  M1 only supports the
//! existing chained hash-table implementation and deliberately delegates storage
//! and lookup primitives to `JoinHashTable`.

use std::ops::Deref;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;

use super::search::JoinSelection;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::hash_table::key_builder::{
    GroupKeyArrayView, build_compressed_flags, build_group_key_hashes, build_group_key_views,
    build_one_number_hashes,
};
use crate::exec::hash_table::key_strategy::GroupKeyStrategy;
use crate::exec::operators::hashjoin::join_hash_table::{JoinHashTable, row_has_forbidden_null};
use crate::runtime::mem_tracker::MemTracker;

/// Join-owned hash map abstraction.  M1 wraps the existing chained table.
pub(crate) struct JoinHashMap {
    table: JoinHashTable,
}

impl JoinHashMap {
    pub(crate) fn new_chained(
        key_types: Vec<DataType>,
        null_safe_eq: Vec<bool>,
    ) -> Result<Self, String> {
        Ok(Self {
            table: JoinHashTable::new(key_types, null_safe_eq)?,
        })
    }

    pub(crate) fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.table.set_mem_tracker(tracker);
    }

    pub(crate) fn hash_seed(&self) -> u64 {
        self.table.hash_seed()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    pub(crate) fn add_build_rows(
        &mut self,
        key_arrays: &[ArrayRef],
        num_rows: usize,
    ) -> Result<(), String> {
        self.table.add_build_rows(key_arrays, num_rows)
    }

    pub(crate) fn finalize(&mut self) -> Result<(), String> {
        self.table.finalize_groups()
    }

    pub(crate) fn lookup_selection(
        &self,
        arena: &ExprArena,
        probe_keys: &[ExprId],
        probe: &Chunk,
    ) -> Result<(Vec<Option<usize>>, JoinSelection), String> {
        let group_ids = self.lookup_group_ids(arena, probe_keys, probe)?;
        let mut selection = JoinSelection::new();
        for (probe_row, group_id_opt) in group_ids.iter().enumerate() {
            let Some(group_id) = group_id_opt else {
                continue;
            };
            let rows = self.table.group_build_rows(*group_id)?;
            for &build_row in rows {
                selection.push(probe_row as u32, build_row);
            }
        }
        Ok((group_ids, selection))
    }

    pub(crate) fn lookup_group_ids(
        &self,
        arena: &ExprArena,
        probe_keys: &[ExprId],
        probe: &Chunk,
    ) -> Result<Vec<Option<usize>>, String> {
        let probe_len = probe.len();
        if probe_len == 0 {
            return Ok(Vec::new());
        }
        if probe_keys.is_empty() {
            return Err("join hash table does not support empty keys".to_string());
        }

        let mut probe_key_arrays = Vec::with_capacity(probe_keys.len());
        for expr in probe_keys {
            probe_key_arrays.push(arena.eval(*expr, probe).map_err(|e| e.to_string())?);
        }

        let key_views = build_group_key_views(&probe_key_arrays).map_err(|e| e.to_string())?;
        let nulls = build_nulls(&key_views, probe_len, self.table.null_safe_eq());

        match self.table.key_strategy() {
            GroupKeyStrategy::OneNumber => {
                let view = key_views
                    .first()
                    .ok_or_else(|| "join one number key view missing".to_string())?;
                let hashes = build_one_number_hashes(view, probe_len, self.table.hash_seed())
                    .map_err(|e| e.to_string())?;
                self.table
                    .lookup_one_number_batch(view, &hashes, &nulls)
                    .map_err(|e| e.to_string())
            }
            GroupKeyStrategy::OneString => {
                let view = key_views
                    .first()
                    .ok_or_else(|| "join one string key view missing".to_string())?;
                let hashes = build_group_key_hashes(&key_views, probe_len, self.table.hash_seed())
                    .map_err(|e| e.to_string())?;
                self.table
                    .lookup_one_string_batch(view, &hashes, &nulls)
                    .map_err(|e| e.to_string())
            }
            GroupKeyStrategy::FixedSize => {
                let hashes = build_group_key_hashes(&key_views, probe_len, self.table.hash_seed())
                    .map_err(|e| e.to_string())?;
                self.table
                    .lookup_fixed_size_batch(&key_views, &hashes, &nulls)
                    .map_err(|e| e.to_string())
            }
            GroupKeyStrategy::CompressedFixed => {
                let ctx = self
                    .table
                    .compressed_ctx()
                    .ok_or_else(|| "join compressed key context missing".to_string())?;
                let keys = build_compressed_flags(ctx, &key_views, probe_len)
                    .map_err(|e| e.to_string())?;
                let hashes = build_group_key_hashes(&key_views, probe_len, self.table.hash_seed())
                    .map_err(|e| e.to_string())?;
                let need_rows = keys
                    .iter()
                    .zip(nulls.iter())
                    .any(|(key, is_null)| !*is_null && !*key);
                let rows = if need_rows {
                    Some(self.table.build_rows_or_fallback(&probe_key_arrays)?)
                } else {
                    None
                };
                self.table
                    .lookup_compressed_batch(&key_views, &keys, rows.as_ref(), &hashes, &nulls)
                    .map_err(|e| e.to_string())
            }
            GroupKeyStrategy::Serialized => {
                let rows = self.table.build_rows_or_fallback(&probe_key_arrays)?;
                let hashes = build_group_key_hashes(&key_views, probe_len, self.table.hash_seed())
                    .map_err(|e| e.to_string())?;
                self.table
                    .lookup_serialized_batch(&rows, &hashes, &nulls)
                    .map_err(|e| e.to_string())
            }
            GroupKeyStrategy::Scalar => {
                Err("join hash table does not support empty keys".to_string())
            }
        }
    }
}

impl Deref for JoinHashMap {
    type Target = JoinHashTable;

    fn deref(&self) -> &Self::Target {
        &self.table
    }
}

fn build_nulls(views: &[GroupKeyArrayView<'_>], len: usize, null_safe_eq: &[bool]) -> Vec<bool> {
    let mut nulls = Vec::with_capacity(len);
    for row in 0..len {
        nulls.push(row_has_forbidden_null(views, row, null_safe_eq));
    }
    nulls
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::JoinHashMap;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::expr::{ExprArena, ExprNode};

    const KEY_SLOT_ID: SlotId = SlotId(1);

    fn int32_chunk(values: Vec<Option<i32>>) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, true)]));
        let array = Arc::new(Int32Array::from(values)) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[KEY_SLOT_ID])
                .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    #[test]
    fn chained_map_lookup_selection_preserves_group_rows_and_null_semantics() {
        let mut map = JoinHashMap::new_chained(vec![DataType::Int32], vec![false]).expect("map");
        let build = int32_chunk(vec![Some(1), Some(2), Some(1), None]);
        map.add_build_rows(build.columns(), build.len())
            .expect("add build");
        map.finalize().expect("finalize");

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(KEY_SLOT_ID), DataType::Int32);
        let probe = int32_chunk(vec![Some(1), Some(3), None, Some(2)]);

        let (group_ids, selection) = map
            .lookup_selection(&arena, &[probe_key], &probe)
            .expect("lookup");

        assert_eq!(group_ids.len(), 4);
        assert!(group_ids[0].is_some());
        assert!(group_ids[1].is_none());
        assert!(group_ids[2].is_none());
        assert!(group_ids[3].is_some());
        assert_eq!(selection.probe, vec![0, 0, 3]);
        assert_eq!(selection.build, vec![2, 0, 1]);
        assert_eq!(
            map.group_build_rows(group_ids[0].expect("group"))
                .expect("group rows"),
            &[2, 0]
        );
    }

    #[test]
    fn chained_map_null_safe_key_matches_null_group() {
        let mut map = JoinHashMap::new_chained(vec![DataType::Int32], vec![true]).expect("map");
        let build = int32_chunk(vec![Some(1), None]);
        map.add_build_rows(build.columns(), build.len())
            .expect("add build");
        map.finalize().expect("finalize");

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(KEY_SLOT_ID), DataType::Int32);
        let probe = int32_chunk(vec![None, Some(2)]);

        let (group_ids, selection) = map
            .lookup_selection(&arena, &[probe_key], &probe)
            .expect("lookup");

        assert!(group_ids[0].is_some());
        assert!(group_ids[1].is_none());
        assert_eq!(selection.probe, vec![0]);
        assert_eq!(selection.build, vec![1]);
    }
}
