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
//! Join hash map method wrapper.
//!
//! This module owns the join-facing hash map abstraction and dispatches between
//! the existing chained hash table and specialized join-owned lookup methods.

use std::mem;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;

use super::search::{JoinSelection, ProbeMask, SearchStats};
use crate::exec::chunk::Chunk;
use crate::exec::expr::agg::IntArrayView;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::hash_table::key_builder::{
    GroupKeyArrayView, build_compressed_flags, build_group_key_hashes, build_group_key_views,
    build_one_number_hashes,
};
use crate::exec::hash_table::key_strategy::GroupKeyStrategy;
use crate::exec::operators::hashjoin::join_hash_table::{JoinHashTable, row_has_forbidden_null};
use novarocks_execution::runtime::mem_tracker::MemTracker;

const DIRECT_RANGE_ROW_MULTIPLIER: u64 = 8;
const DIRECT_RANGE_MAX_LEN: u64 = 16 * 1024 * 1024;
const DIRECT_SET_MAX_BYTES: u64 = 16 * 1024 * 1024;
const DIRECT_SET_BUCKET_BYTE_FACTOR: u64 = 64;
const ROW_NONE: u32 = u32::MAX;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JoinHashMapBuildPurpose {
    RowMatches,
    PresenceOnly,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct JoinHashMapBuildOptions {
    pub(crate) purpose: JoinHashMapBuildPurpose,
    pub(crate) direct_range_row_multiplier: u64,
    pub(crate) direct_range_max_len: u64,
    pub(crate) direct_set_max_bytes: u64,
}

impl Default for JoinHashMapBuildOptions {
    fn default() -> Self {
        Self {
            purpose: JoinHashMapBuildPurpose::RowMatches,
            direct_range_row_multiplier: DIRECT_RANGE_ROW_MULTIPLIER,
            direct_range_max_len: DIRECT_RANGE_MAX_LEN,
            direct_set_max_bytes: DIRECT_SET_MAX_BYTES,
        }
    }
}

#[derive(Clone)]
pub(crate) struct BuildKeyBatch {
    arrays: Vec<ArrayRef>,
    num_rows: usize,
}

impl BuildKeyBatch {
    pub(crate) fn new(arrays: Vec<ArrayRef>, num_rows: usize) -> Result<Self, String> {
        for array in &arrays {
            if array.len() != num_rows {
                return Err(format!(
                    "join build key batch length mismatch: array={} rows={}",
                    array.len(),
                    num_rows
                ));
            }
        }
        Ok(Self { arrays, num_rows })
    }

    pub(crate) fn arrays(&self) -> &[ArrayRef] {
        &self.arrays
    }

    pub(crate) fn num_rows(&self) -> usize {
        self.num_rows
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JoinHashMapMethodKind {
    Chained,
    DirectInt {
        min: i64,
        len: usize,
        not_null: bool,
    },
    DirectIntSet {
        min: i64,
        len: usize,
        not_null: bool,
    },
}

impl JoinHashMapMethodKind {
    pub(crate) fn as_profile_str(&self) -> &'static str {
        match self {
            Self::Chained => "Chained",
            Self::DirectInt { not_null: true, .. } => "DirectIntNotNull",
            Self::DirectInt {
                not_null: false, ..
            } => "DirectIntNullable",
            Self::DirectIntSet { not_null: true, .. } => "DirectIntSetNotNull",
            Self::DirectIntSet {
                not_null: false, ..
            } => "DirectIntSetNullable",
        }
    }
}

pub(crate) enum JoinHashMap {
    Chained(ChainedJoinHashMap),
    DirectInt(DirectIntJoinHashMap),
    DirectIntSet(DirectIntJoinHashSet),
}

pub(crate) struct ChainedJoinHashMap {
    table: JoinHashTable,
}

pub(crate) struct DirectIntJoinHashMap {
    data_type: DataType,
    min: i64,
    max: i64,
    len: usize,
    first: Vec<u32>,
    next: Vec<u32>,
    row_count: usize,
    indexed_rows: usize,
    not_null: bool,
    runtime_filter_hash_seed: u64,
    mem_tracker: Option<Arc<MemTracker>>,
    accounted_bytes: i64,
}

pub(crate) struct DirectIntJoinHashSet {
    data_type: DataType,
    min: i64,
    max: i64,
    len: usize,
    bits: Vec<u8>,
    row_count: usize,
    indexed_rows: usize,
    not_null: bool,
    runtime_filter_hash_seed: u64,
    mem_tracker: Option<Arc<MemTracker>>,
    accounted_bytes: i64,
}

struct DirectIntStats {
    min: i64,
    max: i64,
    row_count: usize,
    indexed_rows: usize,
    not_null: bool,
}

impl JoinHashMap {
    pub(crate) fn new_chained(
        key_types: Vec<DataType>,
        null_safe_eq: Vec<bool>,
    ) -> Result<Self, String> {
        Ok(Self::Chained(ChainedJoinHashMap {
            table: JoinHashTable::new(key_types, null_safe_eq)?,
        }))
    }

    pub(crate) fn build_from_key_batches(
        key_types: Vec<DataType>,
        null_safe_eq: Vec<bool>,
        batches: &[BuildKeyBatch],
        options: JoinHashMapBuildOptions,
    ) -> Result<Self, String> {
        Self::build_from_key_batches_with_tracker(key_types, null_safe_eq, batches, options, None)
    }

    pub(crate) fn build_from_key_batches_with_tracker(
        key_types: Vec<DataType>,
        null_safe_eq: Vec<bool>,
        batches: &[BuildKeyBatch],
        options: JoinHashMapBuildOptions,
        tracker: Option<Arc<MemTracker>>,
    ) -> Result<Self, String> {
        if let Some(direct_set) = DirectIntJoinHashSet::try_build(
            &key_types,
            &null_safe_eq,
            batches,
            options,
            tracker.as_ref().map(Arc::clone),
        )? {
            return Ok(Self::DirectIntSet(direct_set));
        }
        if let Some(direct) = DirectIntJoinHashMap::try_build(
            &key_types,
            &null_safe_eq,
            batches,
            options,
            tracker.as_ref().map(Arc::clone),
        )? {
            return Ok(Self::DirectInt(direct));
        }
        let mut chained = Self::new_chained(key_types, null_safe_eq)?;
        if let Some(tracker) = tracker {
            chained.set_mem_tracker(tracker);
        }
        for batch in batches {
            chained.add_build_rows(batch.arrays(), batch.num_rows())?;
        }
        chained.finalize()?;
        Ok(chained)
    }

    pub(crate) fn method_kind(&self) -> JoinHashMapMethodKind {
        match self {
            Self::Chained(_) => JoinHashMapMethodKind::Chained,
            Self::DirectInt(map) => JoinHashMapMethodKind::DirectInt {
                min: map.min,
                len: map.len,
                not_null: map.not_null,
            },
            Self::DirectIntSet(map) => JoinHashMapMethodKind::DirectIntSet {
                min: map.min,
                len: map.len,
                not_null: map.not_null,
            },
        }
    }

    pub(crate) fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        match self {
            Self::Chained(map) => map.table.set_mem_tracker(tracker),
            Self::DirectInt(map) => map.set_mem_tracker(tracker),
            Self::DirectIntSet(map) => map.set_mem_tracker(tracker),
        }
    }

    pub(crate) fn hash_seed(&self) -> u64 {
        match self {
            Self::Chained(map) => map.table.hash_seed(),
            Self::DirectInt(map) => map.runtime_filter_hash_seed,
            Self::DirectIntSet(map) => map.runtime_filter_hash_seed,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Self::Chained(map) => map.table.is_empty(),
            Self::DirectInt(map) => map.indexed_rows == 0,
            Self::DirectIntSet(map) => map.indexed_rows == 0,
        }
    }

    pub(crate) fn add_build_rows(
        &mut self,
        key_arrays: &[ArrayRef],
        num_rows: usize,
    ) -> Result<(), String> {
        match self {
            Self::Chained(map) => map.table.add_build_rows(key_arrays, num_rows),
            Self::DirectInt(_) => {
                Err("direct integer join hash map does not support incremental build".to_string())
            }
            Self::DirectIntSet(_) => Err(
                "presence-only direct integer join set does not support incremental build"
                    .to_string(),
            ),
        }
    }

    pub(crate) fn finalize(&mut self) -> Result<(), String> {
        match self {
            Self::Chained(map) => map.table.finalize_groups(),
            Self::DirectInt(_) => {
                Err("direct integer join hash map is finalized during build".to_string())
            }
            Self::DirectIntSet(_) => {
                Err("presence-only direct integer join set is finalized during build".to_string())
            }
        }
    }

    pub(crate) fn lookup_selection(
        &self,
        arena: &ExprArena,
        probe_keys: &[ExprId],
        probe: &Chunk,
    ) -> Result<(Vec<Option<usize>>, JoinSelection), String> {
        match self {
            Self::Chained(map) => map.lookup_selection(arena, probe_keys, probe),
            Self::DirectInt(map) => map.lookup_selection(arena, probe_keys, probe),
            Self::DirectIntSet(_) => {
                Err("presence-only direct integer join set cannot enumerate build rows".to_string())
            }
        }
    }

    pub(crate) fn lookup_membership(
        &self,
        arena: &ExprArena,
        probe_keys: &[ExprId],
        probe: &Chunk,
    ) -> Result<Vec<bool>, String> {
        Ok(self
            .search_membership(arena, probe_keys, probe)?
            .0
            .into_vec())
    }

    pub(crate) fn search_membership(
        &self,
        arena: &ExprArena,
        probe_keys: &[ExprId],
        probe: &Chunk,
    ) -> Result<(ProbeMask, SearchStats), String> {
        match self {
            Self::Chained(map) => map.search_membership(arena, probe_keys, probe),
            Self::DirectInt(map) => map.search_membership(arena, probe_keys, probe),
            Self::DirectIntSet(map) => map.search_membership(arena, probe_keys, probe),
        }
    }

    pub(crate) fn lookup_group_ids(
        &self,
        arena: &ExprArena,
        probe_keys: &[ExprId],
        probe: &Chunk,
    ) -> Result<Vec<Option<usize>>, String> {
        match self {
            Self::Chained(map) => map.lookup_group_ids(arena, probe_keys, probe),
            Self::DirectInt(map) => map.lookup_group_ids(arena, probe_keys, probe),
            Self::DirectIntSet(_) => {
                Err("presence-only direct integer join set cannot return group ids".to_string())
            }
        }
    }

    pub(crate) fn search_pairs(
        &self,
        arena: &ExprArena,
        probe_keys: &[ExprId],
        probe: &Chunk,
    ) -> Result<(JoinSelection, SearchStats), String> {
        match self {
            Self::Chained(map) => map.search_pairs(arena, probe_keys, probe),
            Self::DirectInt(map) => map.search_pairs(arena, probe_keys, probe),
            Self::DirectIntSet(_) => {
                Err("presence-only direct integer join set cannot enumerate build rows".to_string())
            }
        }
    }
}

impl ChainedJoinHashMap {
    fn lookup_selection(
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

    fn search_pairs(
        &self,
        arena: &ExprArena,
        probe_keys: &[ExprId],
        probe: &Chunk,
    ) -> Result<(JoinSelection, SearchStats), String> {
        let group_ids = self.lookup_group_ids(arena, probe_keys, probe)?;
        let stats = SearchStats::from_group_ids(&group_ids);
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
        Ok((selection, stats))
    }

    fn search_membership(
        &self,
        arena: &ExprArena,
        probe_keys: &[ExprId],
        probe: &Chunk,
    ) -> Result<(ProbeMask, SearchStats), String> {
        let group_ids = self.lookup_group_ids(arena, probe_keys, probe)?;
        let stats = SearchStats::from_group_ids(&group_ids);
        let mut mask = ProbeMask::new(group_ids.len(), false);
        for (row, group_id) in group_ids.iter().enumerate() {
            if group_id.is_some() {
                mask.set(row, true)?;
            }
        }
        Ok((mask, stats))
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

impl DirectIntJoinHashMap {
    fn try_build(
        key_types: &[DataType],
        null_safe_eq: &[bool],
        batches: &[BuildKeyBatch],
        options: JoinHashMapBuildOptions,
        tracker: Option<Arc<MemTracker>>,
    ) -> Result<Option<Self>, String> {
        if key_types.len() != 1 || null_safe_eq.len() != 1 || null_safe_eq[0] {
            return Ok(None);
        }
        if !is_direct_int_type(&key_types[0]) {
            return Ok(None);
        }
        let stats = collect_direct_int_stats(&key_types[0], batches)?;
        if stats.indexed_rows == 0 {
            return Ok(None);
        }
        let range = checked_direct_range(stats.min, stats.max)?;
        if range > options.direct_range_max_len {
            return Ok(None);
        }
        let row_gate = (stats.indexed_rows as u64)
            .checked_mul(options.direct_range_row_multiplier)
            .ok_or_else(|| "join direct range gate overflow".to_string())?;
        if range > row_gate {
            return Ok(None);
        }
        let len = usize::try_from(range)
            .map_err(|_| "direct integer join range length overflow".to_string())?;
        let mut map = Self {
            data_type: key_types[0].clone(),
            min: stats.min,
            max: stats.max,
            len,
            first: vec![ROW_NONE; len],
            next: vec![ROW_NONE; stats.row_count],
            row_count: stats.row_count,
            indexed_rows: stats.indexed_rows,
            not_null: stats.not_null,
            runtime_filter_hash_seed: fallback_hash_seed(key_types, null_safe_eq)?,
            mem_tracker: None,
            accounted_bytes: 0,
        };
        if let Some(tracker) = tracker {
            map.set_mem_tracker(tracker);
        }
        map.fill_from_batches(batches)?;
        Ok(Some(map))
    }

    fn fill_from_batches(&mut self, batches: &[BuildKeyBatch]) -> Result<(), String> {
        let mut base_row = 0usize;
        for batch in batches {
            let array = batch
                .arrays()
                .first()
                .ok_or_else(|| "direct integer join build key missing".to_string())?;
            if array.data_type() != &self.data_type {
                return Err("direct integer join key type mismatch".to_string());
            }
            let view = IntArrayView::new(array)?;
            for row in 0..batch.num_rows() {
                let global_row = base_row
                    .checked_add(row)
                    .ok_or_else(|| "direct integer join row count overflow".to_string())?;
                let Some(value) = view.value_at(row) else {
                    continue;
                };
                let bucket = direct_bucket(value, self.min, self.max, self.len)
                    .ok_or_else(|| "direct integer join key outside collected range".to_string())?;
                let row_id = u32::try_from(global_row)
                    .map_err(|_| "direct integer join row id overflow".to_string())?;
                self.next[global_row] = self.first[bucket];
                self.first[bucket] = row_id;
            }
            base_row = base_row
                .checked_add(batch.num_rows())
                .ok_or_else(|| "direct integer join row count overflow".to_string())?;
        }
        if base_row != self.row_count {
            return Err("direct integer join row count mismatch".to_string());
        }
        Ok(())
    }

    fn lookup_group_ids(
        &self,
        arena: &ExprArena,
        probe_keys: &[ExprId],
        probe: &Chunk,
    ) -> Result<Vec<Option<usize>>, String> {
        let probe_len = probe.len();
        if probe_len == 0 {
            return Ok(Vec::new());
        }
        let probe_array = eval_single_probe_int_key(arena, probe_keys, probe, &self.data_type)?;
        let probe_view = IntArrayView::new(&probe_array)?;
        let mut group_ids = Vec::with_capacity(probe_len);
        for row in 0..probe_len {
            let group_id = match probe_view.value_at(row) {
                Some(value) => self.lookup_existing_bucket(value),
                _ => None,
            };
            group_ids.push(group_id);
        }
        Ok(group_ids)
    }

    fn lookup_selection(
        &self,
        arena: &ExprArena,
        probe_keys: &[ExprId],
        probe: &Chunk,
    ) -> Result<(Vec<Option<usize>>, JoinSelection), String> {
        let probe_len = probe.len();
        if probe_len == 0 {
            return Ok((Vec::new(), JoinSelection::new()));
        }
        let probe_array = eval_single_probe_int_key(arena, probe_keys, probe, &self.data_type)?;
        let probe_view = IntArrayView::new(&probe_array)?;
        let mut group_ids = Vec::with_capacity(probe_len);
        let mut selection = JoinSelection::new();
        for probe_row in 0..probe_len {
            let Some(value) = probe_view.value_at(probe_row) else {
                group_ids.push(None);
                continue;
            };
            let Some(bucket) = self.lookup_existing_bucket(value) else {
                group_ids.push(None);
                continue;
            };
            group_ids.push(Some(bucket));
            let mut build_row = self.first[bucket];
            while build_row != ROW_NONE {
                selection.push(probe_row as u32, build_row);
                build_row = self.next_row(build_row)?;
            }
        }
        Ok((group_ids, selection))
    }

    fn search_pairs(
        &self,
        arena: &ExprArena,
        probe_keys: &[ExprId],
        probe: &Chunk,
    ) -> Result<(JoinSelection, SearchStats), String> {
        let probe_len = probe.len();
        if probe_len == 0 {
            return Ok((
                JoinSelection::new(),
                SearchStats {
                    lookup_hit_rows: 0,
                    lookup_miss_rows: 0,
                },
            ));
        }
        let probe_array = eval_single_probe_int_key(arena, probe_keys, probe, &self.data_type)?;
        let probe_view = IntArrayView::new(&probe_array)?;
        let mut cursor = Vec::with_capacity(probe_len);
        let mut hit_rows = 0u64;
        for probe_row in 0..probe_len {
            let head = match probe_view.value_at(probe_row) {
                Some(value) => direct_bucket(value, self.min, self.max, self.len)
                    .map(|bucket| self.first[bucket])
                    .unwrap_or(ROW_NONE),
                None => ROW_NONE,
            };
            if head != ROW_NONE {
                hit_rows += 1;
            }
            cursor.push(head);
        }

        let mut selection = JoinSelection::new();
        for (probe_row, mut build_row) in cursor.into_iter().enumerate() {
            while build_row != ROW_NONE {
                selection.push(probe_row as u32, build_row);
                build_row = self.next_row(build_row)?;
            }
        }
        Ok((
            selection,
            SearchStats {
                lookup_hit_rows: hit_rows,
                lookup_miss_rows: probe_len as u64 - hit_rows,
            },
        ))
    }

    fn search_membership(
        &self,
        arena: &ExprArena,
        probe_keys: &[ExprId],
        probe: &Chunk,
    ) -> Result<(ProbeMask, SearchStats), String> {
        let probe_len = probe.len();
        if probe_len == 0 {
            return Ok((
                ProbeMask::new(0, false),
                SearchStats {
                    lookup_hit_rows: 0,
                    lookup_miss_rows: 0,
                },
            ));
        }
        let probe_array = eval_single_probe_int_key(arena, probe_keys, probe, &self.data_type)?;
        let probe_view = IntArrayView::new(&probe_array)?;
        let mut mask = ProbeMask::new(probe_len, false);
        let mut hit_rows = 0u64;
        for row in 0..probe_len {
            let matches = match probe_view.value_at(row) {
                Some(value) => direct_bucket(value, self.min, self.max, self.len)
                    .map(|bucket| self.first[bucket] != ROW_NONE)
                    .unwrap_or(false),
                None => false,
            };
            if matches {
                mask.set(row, true)?;
                hit_rows += 1;
            }
        }
        Ok((
            mask,
            SearchStats {
                lookup_hit_rows: hit_rows,
                lookup_miss_rows: probe_len as u64 - hit_rows,
            },
        ))
    }

    fn lookup_existing_bucket(&self, value: i64) -> Option<usize> {
        let bucket = direct_bucket(value, self.min, self.max, self.len)?;
        let row = self.first[bucket];
        (row != ROW_NONE).then_some(bucket)
    }

    fn next_row(&self, row_id: u32) -> Result<u32, String> {
        self.next
            .get(row_id as usize)
            .copied()
            .ok_or_else(|| "direct integer join row id out of bounds".to_string())
    }

    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        if let Some(current) = self.mem_tracker.as_ref() {
            if Arc::ptr_eq(current, &tracker) {
                return;
            }
            current.release(self.accounted_bytes);
        }
        let bytes = self.tracked_bytes();
        tracker.consume(bytes);
        self.mem_tracker = Some(tracker);
        self.accounted_bytes = bytes;
    }

    fn tracked_bytes(&self) -> i64 {
        fn vec_bytes<T>(v: &Vec<T>) -> i64 {
            let bytes = v.capacity().saturating_mul(mem::size_of::<T>());
            i64::try_from(bytes).unwrap_or(i64::MAX)
        }

        vec_bytes(&self.first).saturating_add(vec_bytes(&self.next))
    }
}

impl Drop for DirectIntJoinHashMap {
    fn drop(&mut self) {
        if let Some(tracker) = self.mem_tracker.as_ref()
            && self.accounted_bytes > 0
        {
            tracker.release(self.accounted_bytes);
            self.accounted_bytes = 0;
        }
    }
}

impl DirectIntJoinHashSet {
    fn try_build(
        key_types: &[DataType],
        null_safe_eq: &[bool],
        batches: &[BuildKeyBatch],
        options: JoinHashMapBuildOptions,
        tracker: Option<Arc<MemTracker>>,
    ) -> Result<Option<Self>, String> {
        // DirectIntSet stores one bit per integer key in the observed
        // [min, max] range. It is only useful for presence-only joins: once
        // callers need group ids or build rows, the bitset no longer carries
        // enough information.
        if options.purpose != JoinHashMapBuildPurpose::PresenceOnly {
            return Ok(None);
        }
        // The direct bit index is computed from a single non-null-safe integer
        // key. Multi-key joins need tuple hashing, and null-safe equality needs
        // explicit NULL group semantics rather than treating NULL as a miss.
        if key_types.len() != 1 || null_safe_eq.len() != 1 || null_safe_eq[0] {
            return Ok(None);
        }
        if !is_direct_int_type(&key_types[0]) {
            return Ok(None);
        }
        let stats = collect_direct_int_stats(&key_types[0], batches)?;
        if stats.indexed_rows == 0 {
            return Ok(None);
        }
        let range = checked_direct_range(stats.min, stats.max)?;
        if range >= u32::MAX as u64 {
            return Ok(None);
        }
        let bitset_bytes = direct_set_bytes(range)?;
        let bucket_gate = direct_set_bucket_gate(stats.indexed_rows);
        // A direct set is a range-indexed structure, so sparse keys can waste
        // memory even when there are few build rows. Fall back unless the bitset
        // fits the absolute cap or remains small relative to the indexed rows.
        if bitset_bytes > options.direct_set_max_bytes && bitset_bytes > bucket_gate {
            return Ok(None);
        }
        let len = usize::try_from(range)
            .map_err(|_| "direct integer join set range length overflow".to_string())?;
        let bitset_len = usize::try_from(bitset_bytes)
            .map_err(|_| "direct integer join set byte size overflow".to_string())?;
        let mut set = Self {
            data_type: key_types[0].clone(),
            min: stats.min,
            max: stats.max,
            len,
            bits: vec![0; bitset_len],
            row_count: stats.row_count,
            indexed_rows: stats.indexed_rows,
            not_null: stats.not_null,
            runtime_filter_hash_seed: fallback_hash_seed(key_types, null_safe_eq)?,
            mem_tracker: None,
            accounted_bytes: 0,
        };
        if let Some(tracker) = tracker {
            set.set_mem_tracker(tracker);
        }
        set.fill_from_batches(batches)?;
        Ok(Some(set))
    }

    fn fill_from_batches(&mut self, batches: &[BuildKeyBatch]) -> Result<(), String> {
        let mut base_row = 0usize;
        for batch in batches {
            let array = batch
                .arrays()
                .first()
                .ok_or_else(|| "direct integer join set build key missing".to_string())?;
            if array.data_type() != &self.data_type {
                return Err("direct integer join set key type mismatch".to_string());
            }
            let view = IntArrayView::new(array)?;
            for row in 0..batch.num_rows() {
                let Some(value) = view.value_at(row) else {
                    continue;
                };
                let bucket =
                    direct_bucket(value, self.min, self.max, self.len).ok_or_else(|| {
                        "direct integer join set key outside collected range".to_string()
                    })?;
                self.set_bucket(bucket)?;
            }
            base_row = base_row
                .checked_add(batch.num_rows())
                .ok_or_else(|| "direct integer join set row count overflow".to_string())?;
        }
        if base_row != self.row_count {
            return Err("direct integer join set row count mismatch".to_string());
        }
        self.refresh_accounting();
        Ok(())
    }

    fn search_membership(
        &self,
        arena: &ExprArena,
        probe_keys: &[ExprId],
        probe: &Chunk,
    ) -> Result<(ProbeMask, SearchStats), String> {
        let probe_len = probe.len();
        if probe_len == 0 {
            return Ok((
                ProbeMask::new(0, false),
                SearchStats {
                    lookup_hit_rows: 0,
                    lookup_miss_rows: 0,
                },
            ));
        }
        let probe_array = eval_single_probe_int_key(arena, probe_keys, probe, &self.data_type)?;
        let probe_view = IntArrayView::new(&probe_array)?;
        let mut mask = ProbeMask::new(probe_len, false);
        let mut hit_rows = 0u64;
        for row in 0..probe_len {
            let matches = match probe_view.value_at(row) {
                Some(value) => self.contains_value(value),
                None => false,
            };
            if matches {
                mask.set(row, true)?;
                hit_rows += 1;
            }
        }
        Ok((
            mask,
            SearchStats {
                lookup_hit_rows: hit_rows,
                lookup_miss_rows: probe_len as u64 - hit_rows,
            },
        ))
    }

    fn set_bucket(&mut self, bucket: usize) -> Result<(), String> {
        let byte = bucket / 8;
        let bit = (bucket % 8) as u8;
        let Some(slot) = self.bits.get_mut(byte) else {
            return Err("direct integer join set bucket out of bounds".to_string());
        };
        *slot |= 1u8 << bit;
        Ok(())
    }

    fn contains_value(&self, value: i64) -> bool {
        let Some(bucket) = direct_bucket(value, self.min, self.max, self.len) else {
            return false;
        };
        let byte = bucket / 8;
        let bit = (bucket % 8) as u8;
        self.bits
            .get(byte)
            .is_some_and(|slot| (*slot & (1u8 << bit)) != 0)
    }

    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        if let Some(current) = self.mem_tracker.as_ref() {
            if Arc::ptr_eq(current, &tracker) {
                return;
            }
            current.release(self.accounted_bytes);
        }
        let bytes = self.tracked_bytes();
        tracker.consume(bytes);
        self.mem_tracker = Some(tracker);
        self.accounted_bytes = bytes;
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
        let bytes = self.bits.capacity().saturating_mul(mem::size_of::<u8>());
        i64::try_from(bytes).unwrap_or(i64::MAX)
    }
}

impl Drop for DirectIntJoinHashSet {
    fn drop(&mut self) {
        if let Some(tracker) = self.mem_tracker.as_ref()
            && self.accounted_bytes > 0
        {
            tracker.release(self.accounted_bytes);
            self.accounted_bytes = 0;
        }
    }
}

fn collect_direct_int_stats(
    data_type: &DataType,
    batches: &[BuildKeyBatch],
) -> Result<DirectIntStats, String> {
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    let mut row_count = 0usize;
    let mut indexed_rows = 0usize;
    let mut not_null = true;
    for batch in batches {
        if batch.arrays().len() != 1 {
            return Err("direct integer join requires one build key".to_string());
        }
        let array = batch
            .arrays()
            .first()
            .ok_or_else(|| "direct integer join build key missing".to_string())?;
        if array.len() != batch.num_rows() {
            return Err(format!(
                "join build key batch length mismatch: array={} rows={}",
                array.len(),
                batch.num_rows()
            ));
        }
        if array.data_type() != data_type {
            return Err("direct integer join key type mismatch".to_string());
        }
        // Use observed Arrow nulls for the M2 fast path. This is stricter than
        // trusting planner metadata and keeps the fast path correct for computed
        // build-key expressions.
        if array.null_count() > 0 {
            not_null = false;
        }
        let next_row_count = row_count
            .checked_add(batch.num_rows())
            .ok_or_else(|| "direct integer join row count overflow".to_string())?;
        if next_row_count > u32::MAX as usize {
            return Err("direct integer join row count overflow".to_string());
        }
        row_count = next_row_count;
        let view = IntArrayView::new(array)?;
        for row in 0..batch.num_rows() {
            let Some(value) = view.value_at(row) else {
                continue;
            };
            min = min.min(value);
            max = max.max(value);
            indexed_rows = indexed_rows
                .checked_add(1)
                .ok_or_else(|| "direct integer join indexed row count overflow".to_string())?;
        }
    }
    Ok(DirectIntStats {
        min,
        max,
        row_count,
        indexed_rows,
        not_null,
    })
}

fn checked_direct_range(min: i64, max: i64) -> Result<u64, String> {
    if max < min {
        return Err("direct integer join range is empty".to_string());
    }
    let range = (max as i128) - (min as i128) + 1;
    u64::try_from(range).map_err(|_| "direct integer join range overflow".to_string())
}

fn direct_set_bytes(range: u64) -> Result<u64, String> {
    range
        .checked_add(7)
        .map(|bits| bits / 8)
        .ok_or_else(|| "direct integer join set byte size overflow".to_string())
}

fn direct_set_bucket_gate(indexed_rows: usize) -> u64 {
    let bucket_count = u64::try_from(indexed_rows)
        .unwrap_or(u64::MAX)
        .max(1)
        .checked_next_power_of_two()
        .unwrap_or(u64::MAX);
    bucket_count.saturating_mul(DIRECT_SET_BUCKET_BYTE_FACTOR)
}

fn direct_bucket(value: i64, min: i64, max: i64, len: usize) -> Option<usize> {
    if value < min || value > max {
        return None;
    }
    let bucket = usize::try_from(value - min).ok()?;
    (bucket < len).then_some(bucket)
}

fn fallback_hash_seed(key_types: &[DataType], null_safe_eq: &[bool]) -> Result<u64, String> {
    let table = JoinHashTable::new(key_types.to_vec(), null_safe_eq.to_vec())?;
    Ok(table.hash_seed())
}

fn eval_single_probe_int_key(
    arena: &ExprArena,
    probe_keys: &[ExprId],
    probe: &Chunk,
    data_type: &DataType,
) -> Result<ArrayRef, String> {
    if probe_keys.len() != 1 {
        return Err("direct integer join requires one probe key".to_string());
    }
    let array = arena
        .eval(probe_keys[0], probe)
        .map_err(|e| e.to_string())?;
    if array.data_type() != data_type {
        return Err("direct integer join probe key type mismatch".to_string());
    }
    IntArrayView::new(&array)?;
    Ok(array)
}

fn is_direct_int_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
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
    use std::mem;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::{
        BuildKeyBatch, JoinHashMap, JoinHashMapBuildOptions, JoinHashMapBuildPurpose,
        JoinHashMapMethodKind,
    };
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exec::operators::hashjoin::join_hash_map::search::SearchStats;
    use novarocks_execution::runtime::mem_tracker::MemTracker;
    use novarocks_types::SlotId;

    const KEY_SLOT_ID: SlotId = SlotId(1);

    #[test]
    fn method_kind_profile_strings_are_stable() {
        assert_eq!(JoinHashMapMethodKind::Chained.as_profile_str(), "Chained");
        assert_eq!(
            JoinHashMapMethodKind::DirectInt {
                min: 1,
                len: 2,
                not_null: true,
            }
            .as_profile_str(),
            "DirectIntNotNull"
        );
        assert_eq!(
            JoinHashMapMethodKind::DirectInt {
                min: 1,
                len: 2,
                not_null: false,
            }
            .as_profile_str(),
            "DirectIntNullable"
        );
    }

    #[test]
    fn build_options_default_to_row_matches() {
        let options = JoinHashMapBuildOptions::default();

        assert_eq!(options.purpose, JoinHashMapBuildPurpose::RowMatches);
        assert_eq!(options.direct_set_max_bytes, 16 * 1024 * 1024);
    }

    #[test]
    fn direct_set_method_kind_profile_strings_are_stable() {
        assert_eq!(
            JoinHashMapMethodKind::DirectIntSet {
                min: 1,
                len: 2,
                not_null: true,
            }
            .as_profile_str(),
            "DirectIntSetNotNull"
        );
        assert_eq!(
            JoinHashMapMethodKind::DirectIntSet {
                min: 1,
                len: 2,
                not_null: false,
            }
            .as_profile_str(),
            "DirectIntSetNullable"
        );
    }

    fn int32_chunk(values: Vec<Option<i32>>) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, true)]));
        let array = Arc::new(Int32Array::from(values)) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[KEY_SLOT_ID])
                .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    fn int32_not_null_chunk(values: Vec<i32>) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
        let array = Arc::new(Int32Array::from(values)) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[KEY_SLOT_ID])
                .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    fn int64_chunk(values: Vec<Option<i64>>) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
        let array = Arc::new(Int64Array::from(values)) as ArrayRef;
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
        let JoinHashMap::Chained(chained) = &map else {
            panic!("expected chained map");
        };
        assert_eq!(
            chained
                .table
                .group_build_rows(group_ids[0].expect("group"))
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
        assert_eq!(map.method_kind(), JoinHashMapMethodKind::Chained);
    }

    #[test]
    fn direct_map_sparse_range_falls_back_to_chained() {
        let build = int32_chunk(vec![Some(1), Some(1_000_000)]);
        let batch = BuildKeyBatch::new(build.columns().to_vec(), build.len()).expect("batch");
        let map = JoinHashMap::build_from_key_batches(
            vec![DataType::Int32],
            vec![false],
            &[batch],
            JoinHashMapBuildOptions::default(),
        )
        .expect("map");

        assert_eq!(map.method_kind(), JoinHashMapMethodKind::Chained);
    }

    #[test]
    fn direct_map_range_cap_falls_back_to_chained() {
        let build = int32_chunk(vec![Some(0), Some((16 * 1024 * 1024) as i32)]);
        let batch = BuildKeyBatch::new(build.columns().to_vec(), build.len()).expect("batch");
        let options = JoinHashMapBuildOptions {
            direct_range_row_multiplier: u64::MAX / 4,
            direct_range_max_len: 16 * 1024 * 1024,
            ..JoinHashMapBuildOptions::default()
        };
        let map = JoinHashMap::build_from_key_batches(
            vec![DataType::Int32],
            vec![false],
            &[batch],
            options,
        )
        .expect("map");

        assert_eq!(map.method_kind(), JoinHashMapMethodKind::Chained);
    }

    #[test]
    fn direct_map_null_safe_equality_falls_back_to_chained() {
        let build = int32_chunk(vec![Some(1), None]);
        let batch = BuildKeyBatch::new(build.columns().to_vec(), build.len()).expect("batch");
        let map = JoinHashMap::build_from_key_batches(
            vec![DataType::Int32],
            vec![true],
            &[batch],
            JoinHashMapBuildOptions::default(),
        )
        .expect("map");

        assert_eq!(map.method_kind(), JoinHashMapMethodKind::Chained);
    }

    #[test]
    fn direct_int_set_selected_for_presence_only_sparse_range() {
        let build = int32_chunk(vec![Some(1), Some(1_000_000)]);
        let batch = BuildKeyBatch::new(build.columns().to_vec(), build.len()).expect("batch");
        let options = JoinHashMapBuildOptions {
            purpose: JoinHashMapBuildPurpose::PresenceOnly,
            direct_set_max_bytes: 1024 * 1024,
            ..JoinHashMapBuildOptions::default()
        };
        let map = JoinHashMap::build_from_key_batches(
            vec![DataType::Int32],
            vec![false],
            &[batch],
            options,
        )
        .expect("map");

        assert_eq!(
            map.method_kind(),
            JoinHashMapMethodKind::DirectIntSet {
                min: 1,
                len: 1_000_000,
                not_null: true,
            }
        );
    }

    #[test]
    fn direct_int_set_next_power_of_two_bucket_gate_allows_boundary_range() {
        let build = int32_chunk(vec![Some(0), Some(1), Some(2047)]);
        let batch = BuildKeyBatch::new(build.columns().to_vec(), build.len()).expect("batch");
        let options = JoinHashMapBuildOptions {
            purpose: JoinHashMapBuildPurpose::PresenceOnly,
            direct_set_max_bytes: 0,
            ..JoinHashMapBuildOptions::default()
        };
        let map = JoinHashMap::build_from_key_batches(
            vec![DataType::Int32],
            vec![false],
            &[batch],
            options,
        )
        .expect("map");

        assert_eq!(
            map.method_kind(),
            JoinHashMapMethodKind::DirectIntSet {
                min: 0,
                len: 2048,
                not_null: true,
            }
        );
    }

    #[test]
    fn direct_int_set_not_selected_for_row_matches_purpose() {
        let build = int32_chunk(vec![Some(1), Some(1_000_000)]);
        let batch = BuildKeyBatch::new(build.columns().to_vec(), build.len()).expect("batch");
        let map = JoinHashMap::build_from_key_batches(
            vec![DataType::Int32],
            vec![false],
            &[batch],
            JoinHashMapBuildOptions::default(),
        )
        .expect("map");

        assert_eq!(map.method_kind(), JoinHashMapMethodKind::Chained);
    }

    #[test]
    fn direct_int_set_null_safe_equality_falls_back_to_chained() {
        let build = int32_chunk(vec![Some(1), None]);
        let batch = BuildKeyBatch::new(build.columns().to_vec(), build.len()).expect("batch");
        let options = JoinHashMapBuildOptions {
            purpose: JoinHashMapBuildPurpose::PresenceOnly,
            ..JoinHashMapBuildOptions::default()
        };
        let map = JoinHashMap::build_from_key_batches(
            vec![DataType::Int32],
            vec![true],
            &[batch],
            options,
        )
        .expect("map");

        assert_eq!(map.method_kind(), JoinHashMapMethodKind::Chained);
    }

    #[test]
    fn direct_int_set_lookup_membership_handles_nulls() {
        let build = int32_chunk(vec![Some(10), None, Some(12)]);
        let batch = BuildKeyBatch::new(build.columns().to_vec(), build.len()).expect("batch");
        let options = JoinHashMapBuildOptions {
            purpose: JoinHashMapBuildPurpose::PresenceOnly,
            ..JoinHashMapBuildOptions::default()
        };
        let map = JoinHashMap::build_from_key_batches(
            vec![DataType::Int32],
            vec![false],
            &[batch],
            options,
        )
        .expect("map");

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(KEY_SLOT_ID), DataType::Int32);
        let probe = int32_chunk(vec![Some(9), Some(10), Some(11), None, Some(12), Some(13)]);
        let (mask, stats) = map
            .search_membership(&arena, &[probe_key], &probe)
            .expect("search membership");
        let membership = map
            .lookup_membership(&arena, &[probe_key], &probe)
            .expect("membership");

        assert_eq!(mask.as_slice(), &[false, true, false, false, true, false]);
        assert_eq!(stats.lookup_hit_rows, 2);
        assert_eq!(stats.lookup_miss_rows, 4);
        assert_eq!(membership, vec![false, true, false, false, true, false]);
    }

    #[test]
    fn search_membership_empty_probe_skips_direct_key_eval() {
        let build = int32_chunk(vec![Some(10), Some(12)]);
        let batch = BuildKeyBatch::new(build.columns().to_vec(), build.len()).expect("batch");
        let direct = JoinHashMap::build_from_key_batches(
            vec![DataType::Int32],
            vec![false],
            std::slice::from_ref(&batch),
            JoinHashMapBuildOptions::default(),
        )
        .expect("direct");
        let direct_set = JoinHashMap::build_from_key_batches(
            vec![DataType::Int32],
            vec![false],
            &[batch],
            JoinHashMapBuildOptions {
                purpose: JoinHashMapBuildPurpose::PresenceOnly,
                ..JoinHashMapBuildOptions::default()
            },
        )
        .expect("direct set");
        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(KEY_SLOT_ID), DataType::Int32);
        let empty_probe = Chunk::default();

        for map in [&direct, &direct_set] {
            let (mask, stats) = map
                .search_membership(&arena, &[probe_key], &empty_probe)
                .expect("empty membership");
            assert_eq!(mask.len(), 0);
            assert!(mask.as_slice().is_empty());
            assert_eq!(stats.lookup_hit_rows, 0);
            assert_eq!(stats.lookup_miss_rows, 0);
            assert_eq!(
                map.lookup_membership(&arena, &[probe_key], &empty_probe)
                    .expect("empty compat"),
                Vec::<bool>::new()
            );
        }
    }

    #[test]
    fn search_membership_matches_lookup_membership_for_chained_and_direct() {
        let mut chained =
            JoinHashMap::new_chained(vec![DataType::Int32], vec![false]).expect("map");
        let build = int32_chunk(vec![Some(1), Some(2), Some(1), None]);
        chained
            .add_build_rows(build.columns(), build.len())
            .expect("add build");
        chained.finalize().expect("finalize");

        let batch = BuildKeyBatch::new(build.columns().to_vec(), build.len()).expect("batch");
        let direct = JoinHashMap::build_from_key_batches(
            vec![DataType::Int32],
            vec![false],
            &[batch],
            JoinHashMapBuildOptions::default(),
        )
        .expect("direct");

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(KEY_SLOT_ID), DataType::Int32);
        let probe = int32_chunk(vec![Some(1), Some(3), None, Some(2)]);

        for map in [&chained, &direct] {
            let (mask, stats) = map
                .search_membership(&arena, &[probe_key], &probe)
                .expect("membership");
            assert_eq!(mask.as_slice(), &[true, false, false, true]);
            assert_eq!(stats.lookup_hit_rows, 2);
            assert_eq!(stats.lookup_miss_rows, 2);
            assert_eq!(
                map.lookup_membership(&arena, &[probe_key], &probe)
                    .expect("compat"),
                vec![true, false, false, true]
            );
        }
    }

    #[test]
    fn direct_int_set_rejects_row_enumeration() {
        let build = int32_chunk(vec![Some(10), Some(12)]);
        let batch = BuildKeyBatch::new(build.columns().to_vec(), build.len()).expect("batch");
        let options = JoinHashMapBuildOptions {
            purpose: JoinHashMapBuildPurpose::PresenceOnly,
            ..JoinHashMapBuildOptions::default()
        };
        let map = JoinHashMap::build_from_key_batches(
            vec![DataType::Int32],
            vec![false],
            &[batch],
            options,
        )
        .expect("map");

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(KEY_SLOT_ID), DataType::Int32);
        let probe = int32_chunk(vec![Some(10)]);
        let err = map
            .lookup_selection(&arena, &[probe_key], &probe)
            .expect_err("set cannot enumerate rows");

        assert_eq!(
            err,
            "presence-only direct integer join set cannot enumerate build rows"
        );
    }

    #[test]
    fn search_pairs_matches_chained_lookup_selection_and_stats() {
        let mut map = JoinHashMap::new_chained(vec![DataType::Int32], vec![false]).expect("map");
        let build = int32_chunk(vec![Some(1), Some(2), Some(1), None]);
        map.add_build_rows(build.columns(), build.len())
            .expect("add build");
        map.finalize().expect("finalize");

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(KEY_SLOT_ID), DataType::Int32);
        let probe = int32_chunk(vec![Some(1), Some(3), None, Some(2)]);

        let (group_ids, lookup_selection) = map
            .lookup_selection(&arena, &[probe_key], &probe)
            .expect("lookup");
        let (selection, stats) = map
            .search_pairs(&arena, &[probe_key], &probe)
            .expect("search pairs");

        assert_eq!(selection, lookup_selection);
        assert_eq!(stats, SearchStats::from_group_ids(&group_ids));
        assert_eq!(stats.lookup_hit_rows, 2);
        assert_eq!(stats.lookup_miss_rows, 2);
    }

    #[test]
    fn search_pairs_matches_null_safe_chained_lookup_selection() {
        let mut map = JoinHashMap::new_chained(vec![DataType::Int32], vec![true]).expect("map");
        let build = int32_chunk(vec![Some(1), None, Some(1), None]);
        map.add_build_rows(build.columns(), build.len())
            .expect("add build");
        map.finalize().expect("finalize");

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(KEY_SLOT_ID), DataType::Int32);
        let probe = int32_chunk(vec![None, Some(1), Some(2)]);

        let (group_ids, lookup_selection) = map
            .lookup_selection(&arena, &[probe_key], &probe)
            .expect("lookup");
        let (selection, stats) = map
            .search_pairs(&arena, &[probe_key], &probe)
            .expect("search pairs");

        assert_eq!(selection, lookup_selection);
        assert_eq!(selection.probe, vec![0, 0, 1, 1]);
        assert_eq!(selection.build, vec![3, 1, 2, 0]);
        assert_eq!(stats, SearchStats::from_group_ids(&group_ids));
        assert_eq!(stats.lookup_hit_rows, 2);
        assert_eq!(stats.lookup_miss_rows, 1);
    }

    #[test]
    fn search_pairs_matches_direct_lookup_selection_and_preserves_order() {
        let build = int32_chunk(vec![Some(100), Some(101), Some(100), Some(103)]);
        let batch = BuildKeyBatch::new(build.columns().to_vec(), build.len()).expect("batch");
        let map = JoinHashMap::build_from_key_batches(
            vec![DataType::Int32],
            vec![false],
            &[batch],
            JoinHashMapBuildOptions::default(),
        )
        .expect("map");
        assert_eq!(
            map.method_kind(),
            JoinHashMapMethodKind::DirectInt {
                min: 100,
                len: 4,
                not_null: true,
            }
        );

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(KEY_SLOT_ID), DataType::Int32);
        let probe = int32_chunk(vec![Some(100), Some(102), Some(103), None]);

        let (group_ids, lookup_selection) = map
            .lookup_selection(&arena, &[probe_key], &probe)
            .expect("lookup");
        let (selection, stats) = map
            .search_pairs(&arena, &[probe_key], &probe)
            .expect("search pairs");

        assert_eq!(selection, lookup_selection);
        assert_eq!(selection.probe, vec![0, 0, 2]);
        assert_eq!(selection.build, vec![2, 0, 3]);
        assert_eq!(stats, SearchStats::from_group_ids(&group_ids));
        assert_eq!(stats.lookup_hit_rows, 2);
        assert_eq!(stats.lookup_miss_rows, 2);
    }

    #[test]
    fn search_pairs_direct_handles_nulls_and_i64_min_like_lookup() {
        let min = i64::MIN;
        let build = int64_chunk(vec![Some(min), None, Some(min + 1), Some(min)]);
        let batch = BuildKeyBatch::new(build.columns().to_vec(), build.len()).expect("batch");
        let map = JoinHashMap::build_from_key_batches(
            vec![DataType::Int64],
            vec![false],
            &[batch],
            JoinHashMapBuildOptions::default(),
        )
        .expect("map");
        assert_eq!(
            map.method_kind(),
            JoinHashMapMethodKind::DirectInt {
                min,
                len: 2,
                not_null: false,
            }
        );

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(KEY_SLOT_ID), DataType::Int64);
        let probe = int64_chunk(vec![None, Some(i64::MAX), Some(min), Some(min + 1)]);

        let (group_ids, lookup_selection) = map
            .lookup_selection(&arena, &[probe_key], &probe)
            .expect("lookup");
        let (selection, stats) = map
            .search_pairs(&arena, &[probe_key], &probe)
            .expect("search pairs");

        assert_eq!(selection, lookup_selection);
        assert_eq!(selection.probe, vec![2, 2, 3]);
        assert_eq!(selection.build, vec![3, 0, 2]);
        assert_eq!(stats, SearchStats::from_group_ids(&group_ids));
        assert_eq!(stats.lookup_hit_rows, 2);
        assert_eq!(stats.lookup_miss_rows, 2);
    }

    #[test]
    fn search_pairs_empty_probe_returns_empty_with_zero_stats() {
        let build = int32_chunk(vec![Some(10)]);
        let batch = BuildKeyBatch::new(build.columns().to_vec(), build.len()).expect("batch");
        let map = JoinHashMap::build_from_key_batches(
            vec![DataType::Int32],
            vec![false],
            &[batch],
            JoinHashMapBuildOptions::default(),
        )
        .expect("map");
        assert_eq!(
            map.method_kind(),
            JoinHashMapMethodKind::DirectInt {
                min: 10,
                len: 1,
                not_null: true,
            }
        );

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(KEY_SLOT_ID), DataType::Int32);
        let probe = int32_chunk(Vec::new());

        let (selection, stats) = map
            .search_pairs(&arena, &[probe_key], &probe)
            .expect("search pairs");

        assert!(selection.is_empty());
        assert_eq!(stats.lookup_hit_rows, 0);
        assert_eq!(stats.lookup_miss_rows, 0);
    }

    #[test]
    fn search_pairs_rejects_direct_int_set_row_enumeration() {
        let build = int32_chunk(vec![Some(10), Some(12)]);
        let batch = BuildKeyBatch::new(build.columns().to_vec(), build.len()).expect("batch");
        let options = JoinHashMapBuildOptions {
            purpose: JoinHashMapBuildPurpose::PresenceOnly,
            ..JoinHashMapBuildOptions::default()
        };
        let map = JoinHashMap::build_from_key_batches(
            vec![DataType::Int32],
            vec![false],
            &[batch],
            options,
        )
        .expect("map");

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(KEY_SLOT_ID), DataType::Int32);
        let probe = int32_chunk(vec![Some(10)]);
        let err = map
            .search_pairs(&arena, &[probe_key], &probe)
            .expect_err("set cannot enumerate rows");

        assert_eq!(
            err,
            "presence-only direct integer join set cannot enumerate build rows"
        );
    }

    #[test]
    fn direct_int_set_mem_tracker_accounts_only_bitset() {
        let root = MemTracker::new_root("direct-set-test");
        {
            let build = int32_chunk(vec![Some(0), Some(7)]);
            let batch = BuildKeyBatch::new(build.columns().to_vec(), build.len()).expect("batch");
            let options = JoinHashMapBuildOptions {
                purpose: JoinHashMapBuildPurpose::PresenceOnly,
                ..JoinHashMapBuildOptions::default()
            };
            let mut map = JoinHashMap::build_from_key_batches(
                vec![DataType::Int32],
                vec![false],
                &[batch],
                options,
            )
            .expect("map");

            map.set_mem_tracker(Arc::clone(&root));

            assert!(root.current() >= 1);
            assert!(
                root.current() < (8 * mem::size_of::<u32>()) as i64,
                "set should not account first/next arrays: {}",
                root.current()
            );
        }
        assert_eq!(root.current(), 0);
    }

    #[test]
    fn direct_map_row_gate_overflow_returns_error() {
        let build = int32_chunk(vec![Some(0), Some(1)]);
        let batch = BuildKeyBatch::new(build.columns().to_vec(), build.len()).expect("batch");
        let options = JoinHashMapBuildOptions {
            direct_range_row_multiplier: u64::MAX / 2 + 1,
            direct_range_max_len: u64::MAX,
            ..JoinHashMapBuildOptions::default()
        };
        let result = JoinHashMap::build_from_key_batches(
            vec![DataType::Int32],
            vec![false],
            &[batch],
            options,
        );

        match result {
            Err(err) => assert_eq!(err, "join direct range gate overflow"),
            Ok(_) => panic!("expected direct range gate overflow"),
        }
    }

    #[test]
    fn direct_map_lookup_selection_uses_key_minus_min_and_preserves_duplicates() {
        let build = int32_chunk(vec![Some(100), Some(101), Some(100), Some(103)]);
        let batch = BuildKeyBatch::new(build.columns().to_vec(), build.len()).expect("batch");
        let map = JoinHashMap::build_from_key_batches(
            vec![DataType::Int32],
            vec![false],
            &[batch],
            JoinHashMapBuildOptions::default(),
        )
        .expect("map");
        assert_eq!(
            map.method_kind(),
            JoinHashMapMethodKind::DirectInt {
                min: 100,
                len: 4,
                not_null: true,
            }
        );

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(KEY_SLOT_ID), DataType::Int32);
        let probe = int32_chunk(vec![Some(100), Some(102), Some(103), None]);

        let (group_ids, selection) = map
            .lookup_selection(&arena, &[probe_key], &probe)
            .expect("lookup");

        assert_eq!(group_ids.len(), 4);
        assert!(group_ids[0].is_some());
        assert!(group_ids[1].is_none());
        assert!(group_ids[2].is_some());
        assert!(group_ids[3].is_none());
        assert_eq!(selection.probe, vec![0, 0, 2]);
        assert_eq!(selection.build, vec![2, 0, 3]);
    }

    #[test]
    fn direct_map_skips_null_build_rows_without_compressing_build_row_ids() {
        let build = int32_chunk(vec![Some(10), None, Some(10)]);
        let batch = BuildKeyBatch::new(build.columns().to_vec(), build.len()).expect("batch");
        let map = JoinHashMap::build_from_key_batches(
            vec![DataType::Int32],
            vec![false],
            &[batch],
            JoinHashMapBuildOptions::default(),
        )
        .expect("map");

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(KEY_SLOT_ID), DataType::Int32);
        let probe = int32_chunk(vec![Some(10), None]);

        let (_group_ids, selection) = map
            .lookup_selection(&arena, &[probe_key], &probe)
            .expect("lookup");

        assert_eq!(selection.probe, vec![0, 0]);
        assert_eq!(selection.build, vec![2, 0]);
    }

    #[test]
    fn direct_map_lookup_group_ids_do_not_require_group_row_slices() {
        let build = int32_chunk(vec![Some(10), None, Some(10)]);
        let batch = BuildKeyBatch::new(build.columns().to_vec(), build.len()).expect("batch");
        let map = JoinHashMap::build_from_key_batches(
            vec![DataType::Int32],
            vec![false],
            &[batch],
            JoinHashMapBuildOptions::default(),
        )
        .expect("map");

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(KEY_SLOT_ID), DataType::Int32);
        let probe = int32_chunk(vec![Some(10)]);
        let group_ids = map
            .lookup_group_ids(&arena, &[probe_key], &probe)
            .expect("lookup ids");
        let (_lookup_ids, selection) = map
            .lookup_selection(&arena, &[probe_key], &probe)
            .expect("lookup selection");

        assert!(group_ids[0].is_some());
        assert_eq!(selection.probe, vec![0, 0]);
        assert_eq!(selection.build, vec![2, 0]);
    }

    #[test]
    fn direct_map_set_mem_tracker_accounts_direct_allocations() {
        let root = MemTracker::new_root("direct-map-test");
        {
            let build = int32_chunk(vec![Some(100), Some(101), Some(100), Some(103)]);
            let batch = BuildKeyBatch::new(build.columns().to_vec(), build.len()).expect("batch");
            let mut map = JoinHashMap::build_from_key_batches(
                vec![DataType::Int32],
                vec![false],
                &[batch],
                JoinHashMapBuildOptions::default(),
            )
            .expect("map");

            map.set_mem_tracker(Arc::clone(&root));

            let expected = ((4 + build.len()) * mem::size_of::<u32>()) as i64;
            assert_eq!(root.current(), expected);
        }
        assert_eq!(root.current(), 0);
    }

    #[test]
    fn direct_map_records_not_null_when_build_keys_have_no_nulls() {
        let build = int32_chunk(vec![Some(7), Some(8), Some(7)]);
        let batch = BuildKeyBatch::new(build.columns().to_vec(), build.len()).expect("batch");
        let map = JoinHashMap::build_from_key_batches(
            vec![DataType::Int32],
            vec![false],
            &[batch],
            JoinHashMapBuildOptions::default(),
        )
        .expect("map");

        assert_eq!(
            map.method_kind(),
            JoinHashMapMethodKind::DirectInt {
                min: 7,
                len: 2,
                not_null: true,
            }
        );
    }

    #[test]
    fn direct_map_records_not_null_fast_path_for_non_nullable_build_keys() {
        let build = int32_not_null_chunk(vec![7, 8, 7]);
        let batch = BuildKeyBatch::new(build.columns().to_vec(), build.len()).expect("batch");
        let map = JoinHashMap::build_from_key_batches(
            vec![DataType::Int32],
            vec![false],
            &[batch],
            JoinHashMapBuildOptions::default(),
        )
        .expect("map");

        assert_eq!(
            map.method_kind(),
            JoinHashMapMethodKind::DirectInt {
                min: 7,
                len: 2,
                not_null: true,
            }
        );
    }

    #[test]
    fn direct_map_nullable_probe_null_still_misses() {
        let build = int32_not_null_chunk(vec![7, 8, 7]);
        let batch = BuildKeyBatch::new(build.columns().to_vec(), build.len()).expect("batch");
        let map = JoinHashMap::build_from_key_batches(
            vec![DataType::Int32],
            vec![false],
            &[batch],
            JoinHashMapBuildOptions::default(),
        )
        .expect("map");

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(KEY_SLOT_ID), DataType::Int32);
        let probe = int32_chunk(vec![None, Some(8)]);
        let (group_ids, selection) = map
            .lookup_selection(&arena, &[probe_key], &probe)
            .expect("lookup");

        assert!(group_ids[0].is_none());
        assert!(group_ids[1].is_some());
        assert_eq!(selection.probe, vec![1]);
        assert_eq!(selection.build, vec![1]);
    }

    #[test]
    fn direct_map_i64_min_range_does_not_overflow_probe_bucket() {
        let min = i64::MIN;
        let build = int64_chunk(vec![Some(min), Some(min + 1)]);
        let batch = BuildKeyBatch::new(build.columns().to_vec(), build.len()).expect("batch");
        let map = JoinHashMap::build_from_key_batches(
            vec![DataType::Int64],
            vec![false],
            &[batch],
            JoinHashMapBuildOptions::default(),
        )
        .expect("map");

        assert_eq!(
            map.method_kind(),
            JoinHashMapMethodKind::DirectInt {
                min,
                len: 2,
                not_null: true,
            }
        );

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(KEY_SLOT_ID), DataType::Int64);
        let probe = int64_chunk(vec![Some(i64::MAX), Some(min + 1), Some(min)]);
        let (group_ids, selection) = map
            .lookup_selection(&arena, &[probe_key], &probe)
            .expect("lookup");

        assert!(group_ids[0].is_none());
        assert!(group_ids[1].is_some());
        assert!(group_ids[2].is_some());
        assert_eq!(selection.probe, vec![1, 2]);
        assert_eq!(selection.build, vec![1, 0]);
    }

    #[test]
    fn direct_map_direct_storage_is_exposed_through_selection_only() {
        let build = int32_chunk(vec![Some(0), Some(15)]);
        let batch = BuildKeyBatch::new(build.columns().to_vec(), build.len()).expect("batch");
        let map = JoinHashMap::build_from_key_batches(
            vec![DataType::Int32],
            vec![false],
            &[batch],
            JoinHashMapBuildOptions::default(),
        )
        .expect("map");

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(KEY_SLOT_ID), DataType::Int32);
        let probe = int32_chunk(vec![Some(15), Some(1), Some(0)]);
        let (_group_ids, selection) = map
            .lookup_selection(&arena, &[probe_key], &probe)
            .expect("lookup");

        assert_eq!(selection.probe, vec![0, 2]);
        assert_eq!(selection.build, vec![1, 0]);
    }
}
