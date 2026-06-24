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
//! Core probe engine for hash-join output assembly.
//!
//! Responsibilities:
//! - Executes key lookup, match expansion, and join-type specific row construction.
//! - Implements null-aware semantics and row-shaping rules reused by probe processors.
//!
//! Key exported interfaces:
//! - Types: `HashJoinProbeCore`.
//! - Functions: `join_type_str`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{Array, BooleanArray};
use arrow::compute::{concat_batches, filter_record_batch};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use super::build_artifact::JoinBuildArtifact;
use super::join_hash_map::match_flags::BuildMatchFlags;
use super::join_hash_map::method::JoinHashMap;
use super::join_hash_map::search::{JoinSelection, SearchStats, append_cross_selection};
use super::join_probe_utils::cross_join_batches;
use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::node::join::JoinType;
use crate::exec::runtime_filter::LocalRuntimeFilterSet;
use crate::exec::schema_compat::{align_schema_to_batches, normalize_batch_to_schema};
use crate::runtime::profile::{CounterRef, clamp_u128_to_i64};

fn concat_compatible_batches(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    context: &str,
) -> Result<RecordBatch, String> {
    let schema = align_schema_to_batches(schema, batches, context)?;
    let batches = batches
        .iter()
        .map(|batch| normalize_batch_to_schema(&schema, batch, context))
        .collect::<Result<Vec<_>, _>>()?;
    concat_batches(&schema, &batches).map_err(|e| e.to_string())
}

/// Core hash-join probing engine that performs key lookup and join-type specific row assembly.
pub(crate) struct HashJoinProbeCore {
    arena: Arc<ExprArena>,
    join_type: JoinType,
    probe_keys: Vec<ExprId>,
    residual_predicate: Option<ExprId>,
    probe_is_left: bool,
    left_chunk_schema: ChunkSchemaRef,
    right_chunk_schema: ChunkSchemaRef,
    join_scope_chunk_schema: ChunkSchemaRef,
    join_scope_schema: SchemaRef,
    build_loaded: bool,
    build_chunk: Option<Arc<Chunk>>,
    build_null_key_rows: Option<Arc<Vec<u32>>>,
    build_table: Option<Arc<JoinHashMap>>,
    runtime_filters: Option<Arc<LocalRuntimeFilterSet>>,
    output_schema: Option<SchemaRef>,
    build_matched: Option<BuildMatchFlags>,
    global_build_row_count: usize,
    global_build_has_null_key: bool,
    build_partition_row_count: usize,
    build_partition_has_null_key: bool,
    output_rows: u64,
    lookup_hit_rows: u64,
    lookup_miss_rows: u64,
    residual_rows_checked: u64,
    residual_eval_pairs: u64,
    residual_eval_batches: u64,
    residual_matched_rows: u64,
    residual_group_rows_total: u64,
    pending_output_batches: VecDeque<RecordBatch>,
    search_timer: Option<CounterRef>,
    output_timer: Option<CounterRef>,
}

impl HashJoinProbeCore {
    pub(crate) fn new(
        arena: Arc<ExprArena>,
        join_type: JoinType,
        probe_keys: Vec<ExprId>,
        residual_predicate: Option<ExprId>,
        probe_is_left: bool,
        left_chunk_schema: ChunkSchemaRef,
        right_chunk_schema: ChunkSchemaRef,
        join_scope_chunk_schema: ChunkSchemaRef,
    ) -> Self {
        Self {
            arena,
            join_type,
            probe_keys,
            residual_predicate,
            probe_is_left,
            left_chunk_schema,
            right_chunk_schema,
            join_scope_schema: join_scope_chunk_schema.arrow_schema_ref(),
            join_scope_chunk_schema,
            build_loaded: false,
            build_chunk: None,
            build_null_key_rows: None,
            build_table: None,
            runtime_filters: None,
            output_schema: None,
            build_matched: None,
            global_build_row_count: 0,
            global_build_has_null_key: false,
            build_partition_row_count: 0,
            build_partition_has_null_key: false,
            output_rows: 0,
            lookup_hit_rows: 0,
            lookup_miss_rows: 0,
            residual_rows_checked: 0,
            residual_eval_pairs: 0,
            residual_eval_batches: 0,
            residual_matched_rows: 0,
            residual_group_rows_total: 0,
            pending_output_batches: VecDeque::new(),
            search_timer: None,
            output_timer: None,
        }
    }

    pub(crate) fn set_phase_timers(&mut self, search_timer: CounterRef, output_timer: CounterRef) {
        self.search_timer = Some(search_timer);
        self.output_timer = Some(output_timer);
    }

    #[inline]
    fn record_timer_ns(timer: Option<&CounterRef>, start: std::time::Instant) {
        if let Some(timer) = timer {
            timer.add(clamp_u128_to_i64(start.elapsed().as_nanos()));
        }
    }

    #[inline]
    fn record_search_ns(&self, start: std::time::Instant) {
        Self::record_timer_ns(self.search_timer.as_ref(), start);
    }

    #[inline]
    fn record_output_ns(&self, start: std::time::Instant) {
        Self::record_timer_ns(self.output_timer.as_ref(), start);
    }

    pub(crate) fn join_type(&self) -> JoinType {
        self.join_type
    }

    #[allow(dead_code)] // used by planned right-semi/anti join paths
    pub(crate) fn probe_chunk_schema(&self) -> &ChunkSchemaRef {
        if self.probe_is_left {
            &self.left_chunk_schema
        } else {
            &self.right_chunk_schema
        }
    }

    pub(crate) fn build_chunk_schema(&self) -> &ChunkSchemaRef {
        if self.probe_is_left {
            &self.right_chunk_schema
        } else {
            &self.left_chunk_schema
        }
    }

    pub(crate) fn join_scope_chunk_schema(&self) -> &ChunkSchemaRef {
        &self.join_scope_chunk_schema
    }

    pub(crate) fn is_build_loaded(&self) -> bool {
        self.build_loaded
    }

    pub(crate) fn probe_is_left(&self) -> bool {
        self.probe_is_left
    }

    pub(crate) fn set_build_artifact(
        &mut self,
        artifact: Arc<JoinBuildArtifact>,
        global_build_row_count: usize,
        global_build_has_null_key: bool,
    ) -> Result<(), String> {
        if self.build_loaded {
            return Ok(());
        }
        self.build_chunk = artifact.build_store.as_ref().map(|store| store.chunk());
        self.build_null_key_rows = artifact.build_null_key_rows.clone();
        self.build_table = artifact.build_table.clone();
        self.runtime_filters = artifact.runtime_filters.clone();
        self.build_partition_row_count = artifact.build_row_count;
        self.build_partition_has_null_key = artifact.build_has_null_key;
        self.global_build_row_count = global_build_row_count;
        self.global_build_has_null_key = global_build_has_null_key;
        if matches!(
            self.join_type,
            JoinType::FullOuter | JoinType::RightOuter | JoinType::RightSemi | JoinType::RightAnti
        ) {
            self.build_matched = Some(BuildMatchFlags::new(artifact.build_row_count));
        }
        self.build_loaded = true;
        Ok(())
    }

    pub(crate) fn probe_keys_len(&self) -> usize {
        self.probe_keys.len()
    }

    pub(crate) fn has_residual_predicate(&self) -> bool {
        self.residual_predicate.is_some()
    }

    pub(crate) fn build_store_rows(&self) -> usize {
        self.build_chunk
            .as_ref()
            .map(|chunk| chunk.len())
            .unwrap_or(0)
    }

    pub(crate) fn build_table_present(&self) -> bool {
        self.build_table.is_some()
    }

    pub(crate) fn output_rows(&self) -> u64 {
        self.output_rows
    }

    pub(crate) fn build_partition_row_count(&self) -> usize {
        self.build_partition_row_count
    }

    pub(crate) fn build_partition_has_null_key(&self) -> bool {
        self.build_partition_has_null_key
    }

    pub(crate) fn lookup_hit_rows(&self) -> u64 {
        self.lookup_hit_rows
    }

    pub(crate) fn lookup_miss_rows(&self) -> u64 {
        self.lookup_miss_rows
    }

    pub(crate) fn residual_rows_checked(&self) -> u64 {
        self.residual_rows_checked
    }

    pub(crate) fn residual_eval_pairs(&self) -> u64 {
        self.residual_eval_pairs
    }

    pub(crate) fn residual_eval_batches(&self) -> u64 {
        self.residual_eval_batches
    }

    pub(crate) fn residual_matched_rows(&self) -> u64 {
        self.residual_matched_rows
    }

    pub(crate) fn residual_group_rows_total(&self) -> u64 {
        self.residual_group_rows_total
    }

    pub(crate) fn has_pending_output(&self) -> bool {
        !self.pending_output_batches.is_empty()
    }

    pub(crate) fn pop_pending_output(&mut self) -> Result<Option<Chunk>, String> {
        let Some(batch) = self.pending_output_batches.pop_front() else {
            return Ok(None);
        };
        Ok(Some(Chunk::try_new_with_chunk_schema(
            batch,
            Arc::clone(&self.join_scope_chunk_schema),
        )?))
    }

    /// Extend a probe-only batch with NULL-filled build-side columns so that
    /// the result matches the full join-scope schema.  This mirrors StarRocks
    /// BE's `_build_default_output` for SEMI/ANTI joins: the pruned side's
    /// slots must still appear in the output chunk (as NULLs) because
    /// downstream operators (SORT sort_tuple_slot_exprs, EXCHANGE, ANALYTIC)
    /// may reference them.
    fn extend_with_null_build_columns(&self, probe_batch: RecordBatch) -> Result<Chunk, String> {
        use crate::exec::chunk::ChunkSchema;
        use arrow::array::new_null_array;
        let num_rows = probe_batch.num_rows();
        let output_start = std::time::Instant::now();
        let build_schema = self.build_chunk_schema();
        let probe_col_count = probe_batch.num_columns();
        let mut columns: Vec<arrow::array::ArrayRef> = probe_batch.columns().to_vec();
        for slot in build_schema.slots() {
            columns.push(new_null_array(slot.field().data_type(), num_rows));
        }
        // Build a ChunkSchema where the NULL-filled build-side slots are nullable,
        // so that downstream operators (LOCAL_EXCHANGE, SORT, etc.) that
        // reconstruct Arrow schemas from ChunkSchema do not reject the NULLs.
        let adjusted_slots: Vec<_> = self
            .join_scope_chunk_schema
            .slots()
            .iter()
            .enumerate()
            .map(|(i, slot)| {
                if i >= probe_col_count {
                    slot.with_nullable(true)
                } else {
                    slot.clone()
                }
            })
            .collect();
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(adjusted_slots)
                .map_err(|e| format!("extend_with_null_build_columns schema: {e}"))?,
        );
        let chunk = Chunk::try_new_with_columns(chunk_schema, columns)
            .map_err(|e| format!("extend_with_null_build_columns: {e}"))?;
        self.record_output_ns(output_start);
        Ok(chunk)
    }

    /// Extend a build-only batch with NULL-filled probe-side columns.
    /// Mirror of `extend_with_null_build_columns` for RIGHT SEMI/ANTI.
    fn extend_with_null_probe_columns(&self, build_batch: RecordBatch) -> Result<Chunk, String> {
        use crate::exec::chunk::ChunkSchema;
        use arrow::array::new_null_array;
        let num_rows = build_batch.num_rows();
        let output_start = std::time::Instant::now();
        let probe_schema = self.probe_chunk_schema();
        let probe_col_count = probe_schema.slots().len();
        let mut columns: Vec<arrow::array::ArrayRef> =
            Vec::with_capacity(probe_col_count + build_batch.num_columns());
        for slot in probe_schema.slots() {
            columns.push(new_null_array(slot.field().data_type(), num_rows));
        }
        columns.extend(build_batch.columns().iter().cloned());
        let adjusted_slots: Vec<_> = self
            .join_scope_chunk_schema
            .slots()
            .iter()
            .enumerate()
            .map(|(i, slot)| {
                if i < probe_col_count {
                    slot.with_nullable(true)
                } else {
                    slot.clone()
                }
            })
            .collect();
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(adjusted_slots)
                .map_err(|e| format!("extend_with_null_probe_columns schema: {e}"))?,
        );
        let chunk = Chunk::try_new_with_columns(chunk_schema, columns)
            .map_err(|e| format!("extend_with_null_probe_columns: {e}"))?;
        self.record_output_ns(output_start);
        Ok(chunk)
    }

    pub(crate) fn right_semi_anti_output_chunk(
        &mut self,
        build_output: Option<RecordBatch>,
    ) -> Result<Option<Chunk>, String> {
        let Some(build_batch) = build_output else {
            return Ok(None);
        };
        self.output_rows = self
            .output_rows
            .saturating_add(build_batch.num_rows() as u64);
        Ok(Some(self.extend_with_null_probe_columns(build_batch)?))
    }

    pub(crate) fn join_probe_chunks(
        &mut self,
        probe_chunks: Vec<Chunk>,
    ) -> Result<Option<Chunk>, String> {
        if probe_chunks.is_empty() {
            return Ok(None);
        }
        let probe_chunks = self.apply_runtime_filters(probe_chunks)?;
        if probe_chunks.is_empty() {
            return Ok(None);
        }

        match self.join_type {
            JoinType::Inner => self.join_inner(probe_chunks),
            JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                self.join_outer(probe_chunks)
            }
            JoinType::LeftSemi | JoinType::RightSemi | JoinType::LeftAnti | JoinType::RightAnti => {
                self.join_semi_anti(probe_chunks)
            }
            JoinType::NullAwareLeftAnti => self.join_null_aware_left_anti(probe_chunks),
        }
    }

    pub(crate) fn finish(
        &mut self,
        probe_chunks: Vec<Chunk>,
        merged_build_flags: Option<Vec<bool>>,
    ) -> Result<Option<Chunk>, String> {
        let out = self.join_probe_chunks(probe_chunks)?;
        self.finish_from_probe_output(out, merged_build_flags, true)
    }

    pub(crate) fn finish_from_probe_output(
        &mut self,
        mut out: Option<Chunk>,
        merged_build_flags: Option<Vec<bool>>,
        count_build_rows: bool,
    ) -> Result<Option<Chunk>, String> {
        match self.join_type {
            JoinType::RightAnti if self.probe_is_left => {
                let flags = merged_build_flags
                    .or_else(|| self.take_build_matched())
                    .unwrap_or_default();
                let build_out = self.build_right_semi_anti_output_with_flags(&flags, false)?;
                out = self.right_semi_anti_output_chunk(build_out)?;
            }
            JoinType::RightSemi if self.probe_is_left => {
                let flags = merged_build_flags
                    .or_else(|| self.take_build_matched())
                    .unwrap_or_default();
                let build_out = self.build_right_semi_anti_output_with_flags(&flags, true)?;
                out = self.right_semi_anti_output_chunk(build_out)?;
            }
            JoinType::FullOuter | JoinType::RightOuter => {
                let flags = merged_build_flags
                    .or_else(|| self.take_build_matched())
                    .unwrap_or_default();
                let schema = Arc::clone(self.join_scope_chunk_schema());
                let build_unmatched = self.build_unmatched_build_output_from_flags(&flags)?;
                out = self.merge_join_outputs(out, build_unmatched, &schema, count_build_rows)?;
            }
            _ => {}
        }
        Ok(out)
    }

    fn compact_selection_by_residual(
        &mut self,
        probe: &Chunk,
        build: &Chunk,
        selection: &mut JoinSelection,
        pred: ExprId,
    ) -> Result<(), String> {
        if selection.is_empty() {
            return Ok(());
        }
        let mut kept = JoinSelection::new();
        let mut offset = 0usize;
        while offset < selection.len() {
            let end = (offset
                + crate::exec::operators::hashjoin::join_hash_map::gather::MAX_JOIN_OUTPUT_ROWS_PER_BATCH)
                .min(selection.len());
            let output_start = std::time::Instant::now();
            let candidate = if self.probe_is_left {
                crate::exec::operators::hashjoin::join_hash_map::gather::gather_join_batch(
                    probe,
                    build,
                    &selection.probe[offset..end],
                    &selection.build[offset..end],
                    &self.join_scope_schema,
                )?
            } else {
                crate::exec::operators::hashjoin::join_hash_map::gather::gather_join_batch(
                    build,
                    probe,
                    &selection.build[offset..end],
                    &selection.probe[offset..end],
                    &self.join_scope_schema,
                )?
            }
            .ok_or_else(|| "join residual candidate batch missing".to_string())?;
            self.record_output_ns(output_start);
            let candidate_chunk = Chunk::try_new_with_chunk_schema(
                candidate,
                Arc::clone(&self.join_scope_chunk_schema),
            )?;
            let mask_arr = self
                .arena
                .eval(pred, &candidate_chunk)
                .map_err(|e| e.to_string())?;
            let mask = mask_arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "join residual predicate must return boolean array".to_string())?;
            if mask.len() != end - offset {
                return Err(format!(
                    "join residual mask length mismatch: mask={} selection={}",
                    mask.len(),
                    end - offset
                ));
            }
            let matched_before = kept.len();
            for mask_idx in 0..mask.len() {
                if mask.is_valid(mask_idx) && mask.value(mask_idx) {
                    kept.push(
                        selection.probe[offset + mask_idx],
                        selection.build[offset + mask_idx],
                    );
                }
            }
            self.residual_eval_batches = self.residual_eval_batches.saturating_add(1);
            self.residual_eval_pairs = self
                .residual_eval_pairs
                .saturating_add((end - offset) as u64);
            self.residual_matched_rows = self
                .residual_matched_rows
                .saturating_add((kept.len() - matched_before) as u64);
            offset = end;
        }
        *selection = kept;
        Ok(())
    }

    fn mark_probe_matches_from_selection(
        matched: &mut [bool],
        selection: &JoinSelection,
        context: &str,
    ) -> Result<u64, String> {
        let mut newly_marked = 0u64;
        for &probe_row in &selection.probe {
            let slot = probe_row as usize;
            let Some(matched_slot) = matched.get_mut(slot) else {
                return Err(format!(
                    "{context} probe row out of bounds: row={} rows={}",
                    slot,
                    matched.len()
                ));
            };
            if !*matched_slot {
                *matched_slot = true;
                newly_marked = newly_marked.saturating_add(1);
            }
        }
        Ok(newly_marked)
    }

    fn compact_null_aware_selection(
        &mut self,
        probe: &Chunk,
        build: &Chunk,
        selection: &mut JoinSelection,
        pred: ExprId,
        matched: &mut [bool],
        residual_matched_probe_rows: &mut [bool],
        context: &str,
    ) -> Result<(), String> {
        if selection.is_empty() {
            return Ok(());
        }

        let residual_matched_rows_before = self.residual_matched_rows;
        self.compact_selection_by_residual(probe, build, selection, pred)?;
        Self::mark_probe_matches_from_selection(matched, selection, context)?;
        let unique_probe_rows = Self::mark_probe_matches_from_selection(
            residual_matched_probe_rows,
            selection,
            context,
        )?;
        self.residual_matched_rows = residual_matched_rows_before.saturating_add(unique_probe_rows);
        Ok(())
    }

    fn compact_cross_selection_in_chunks(
        &mut self,
        probe: &Chunk,
        build: &Chunk,
        probe_rows: &[u32],
        build_rows: &[u32],
        pred: ExprId,
        matched: &mut [bool],
        residual_matched_probe_rows: &mut [bool],
        context: &str,
    ) -> Result<(), String> {
        if probe_rows.is_empty() || build_rows.is_empty() {
            return Ok(());
        }

        const MAX_CROSS_SELECTION_PAIRS: usize = 16 * 1024;
        let mut probe_pos = 0usize;
        let mut build_pos = 0usize;
        while probe_pos < probe_rows.len() {
            let mut selection = JoinSelection::new();
            while probe_pos < probe_rows.len() && selection.len() < MAX_CROSS_SELECTION_PAIRS {
                let probe_row = probe_rows[probe_pos];
                let probe_slot = probe_row as usize;
                let Some(matched_slot) = matched.get(probe_slot) else {
                    return Err(format!(
                        "{context} probe row out of bounds: row={} rows={}",
                        probe_slot,
                        matched.len()
                    ));
                };
                if *matched_slot {
                    probe_pos += 1;
                    build_pos = 0;
                    continue;
                }
                if build_pos >= build_rows.len() {
                    probe_pos += 1;
                    build_pos = 0;
                    continue;
                }

                let before_len = selection.len();
                let stopped = append_cross_selection(
                    &mut selection,
                    &[probe_row],
                    &build_rows[build_pos..],
                    MAX_CROSS_SELECTION_PAIRS,
                );
                let appended = selection.len() - before_len;
                build_pos = build_pos
                    .checked_add(appended)
                    .ok_or_else(|| "join residual cross selection overflow".to_string())?;
                if build_pos >= build_rows.len() {
                    probe_pos += 1;
                    build_pos = 0;
                }
                if stopped {
                    break;
                }
            }

            if selection.is_empty() {
                continue;
            }
            self.compact_null_aware_selection(
                probe,
                build,
                &mut selection,
                pred,
                matched,
                residual_matched_probe_rows,
                context,
            )?;
        }
        Ok(())
    }

    fn build_unmatched_build_output_from_flags(
        &mut self,
        flags: &[bool],
    ) -> Result<Option<RecordBatch>, String> {
        let Some(build_chunk) = self.build_chunk.as_ref() else {
            return Ok(None);
        };
        if flags.len() != build_chunk.len() {
            return Err(format!(
                "join build match flags length mismatch: flags={} build_rows={}",
                flags.len(),
                build_chunk.len()
            ));
        }
        let indices = flags
            .iter()
            .enumerate()
            .filter_map(|(row, matched)| (!matched).then_some(row as u32))
            .collect::<Vec<_>>();
        let output_start = std::time::Instant::now();
        let out = if self.probe_is_left {
            crate::exec::operators::hashjoin::join_hash_map::gather::gather_null_left_with_right(
                build_chunk,
                &indices,
                &self.left_chunk_schema.arrow_schema_ref(),
                &self.join_scope_schema,
            )?
        } else {
            crate::exec::operators::hashjoin::join_hash_map::gather::gather_left_with_null_right(
                build_chunk,
                &indices,
                &self.right_chunk_schema.arrow_schema_ref(),
                &self.join_scope_schema,
            )?
        };
        self.record_output_ns(output_start);
        Ok(out)
    }

    /// Take ownership of the local build-matched flags.  Used by broadcast
    /// join to merge per-driver flags into a shared accumulator.
    pub(crate) fn take_build_matched(&mut self) -> Option<Vec<bool>> {
        Some(self.build_matched.take()?.into_vec())
    }

    /// Produce output from externally-provided build-matched flags (e.g.
    /// after merging flags from all broadcast probe drivers).
    pub(crate) fn build_right_semi_anti_output_with_flags(
        &mut self,
        flags: &[bool],
        want_matched: bool,
    ) -> Result<Option<RecordBatch>, String> {
        self.build_right_semi_anti_output_from_flat_flags(flags, want_matched)
    }

    pub(crate) fn build_right_semi_anti_output(
        &mut self,
        want_matched: bool,
    ) -> Result<Option<RecordBatch>, String> {
        if self
            .build_chunk
            .as_ref()
            .map(|chunk| chunk.is_empty())
            .unwrap_or(true)
        {
            return Ok(None);
        }
        let Some(flags) = self.build_matched.as_ref() else {
            return Ok(None);
        };
        // Clone to satisfy borrow checker (flags borrows self).
        let flags = flags.clone().into_vec();
        self.build_right_semi_anti_output_from_flat_flags(&flags, want_matched)
    }

    fn build_right_semi_anti_output_from_flat_flags(
        &mut self,
        flags: &[bool],
        want_matched: bool,
    ) -> Result<Option<RecordBatch>, String> {
        let Some(build_chunk) = self.build_chunk.as_ref() else {
            return Ok(None);
        };
        if flags.len() != build_chunk.len() {
            return Err(format!(
                "join build match flags length mismatch: flags={} build_rows={}",
                flags.len(),
                build_chunk.len()
            ));
        }
        let mask = flags
            .iter()
            .map(|v| if want_matched { *v } else { !*v })
            .collect::<Vec<_>>();
        let mask = BooleanArray::from(mask);
        let output_start = std::time::Instant::now();
        let filtered = filter_record_batch(&build_chunk.batch, &mask).map_err(|e| e.to_string())?;
        self.record_output_ns(output_start);
        if filtered.num_rows() == 0 {
            return Ok(None);
        }
        Ok(Some(filtered))
    }

    pub(crate) fn merge_join_outputs(
        &mut self,
        left: Option<Chunk>,
        right: Option<RecordBatch>,
        batch_chunk_schema: &ChunkSchemaRef,
        count_right_rows: bool,
    ) -> Result<Option<Chunk>, String> {
        if count_right_rows && let Some(batch) = right.as_ref() {
            self.output_rows = self.output_rows.saturating_add(batch.num_rows() as u64);
        }
        match (left, right) {
            (None, None) => Ok(None),
            (Some(chunk), None) => Ok(Some(chunk)),
            (None, Some(batch)) => Ok(Some(Chunk::try_new_with_chunk_schema(
                batch,
                Arc::clone(batch_chunk_schema),
            )?)),
            (Some(left_chunk), Some(right_batch)) => {
                if left_chunk.is_empty() && right_batch.num_rows() == 0 {
                    return Ok(Some(left_chunk));
                }
                if left_chunk.is_empty() {
                    return Ok(Some(Chunk::try_new_with_chunk_schema(
                        right_batch,
                        Arc::clone(batch_chunk_schema),
                    )?));
                }
                if right_batch.num_rows() == 0 {
                    return Ok(Some(left_chunk));
                }
                let batches = vec![left_chunk.batch, right_batch];
                let batch =
                    concat_compatible_batches(&self.join_scope_schema, &batches, "join merge")?;
                Ok(Some(Chunk::try_new_with_chunk_schema(
                    batch,
                    Arc::clone(&self.join_scope_chunk_schema),
                )?))
            }
        }
    }

    fn join_inner(&mut self, probe_chunks: Vec<Chunk>) -> Result<Option<Chunk>, String> {
        let build_chunk_opt = self.build_chunk.clone();
        if self.probe_keys.is_empty()
            && build_chunk_opt
                .as_ref()
                .map(|chunk| chunk.is_empty())
                .unwrap_or(true)
        {
            return Ok(None);
        }
        let output_schema = Arc::clone(
            self.output_schema
                .get_or_insert_with(|| Arc::clone(&self.join_scope_schema)),
        );

        let mut output_batches = Vec::new();
        let mut residual_applied_during_selection = false;
        if self.probe_keys.is_empty() {
            let Some(right) = build_chunk_opt.as_ref() else {
                return Ok(None);
            };
            for left in probe_chunks {
                let output_start = std::time::Instant::now();
                let batches = cross_join_batches(&left, right, &output_schema)?;
                self.record_output_ns(output_start);
                output_batches.extend(batches);
            }
        } else {
            let Some(table) = self.build_table.clone() else {
                return Ok(None);
            };
            let Some(build_chunk) = self.build_chunk.clone() else {
                return Ok(None);
            };
            if table.is_empty() || build_chunk.is_empty() {
                return Ok(None);
            }
            residual_applied_during_selection = self.residual_predicate.is_some();
            for probe in probe_chunks {
                let search_start = std::time::Instant::now();
                let (group_ids, mut selection) =
                    table.lookup_selection(&self.arena, &self.probe_keys, &probe)?;
                self.record_search_ns(search_start);
                let stats = SearchStats::from_group_ids(&group_ids);
                self.lookup_hit_rows = self.lookup_hit_rows.saturating_add(stats.lookup_hit_rows);
                self.lookup_miss_rows =
                    self.lookup_miss_rows.saturating_add(stats.lookup_miss_rows);
                if selection.is_empty() {
                    continue;
                }
                if let Some(pred) = self.residual_predicate {
                    self.residual_rows_checked = self
                        .residual_rows_checked
                        .saturating_add(stats.lookup_hit_rows);
                    self.residual_group_rows_total = self
                        .residual_group_rows_total
                        .saturating_add(selection.len() as u64);
                    self.compact_selection_by_residual(&probe, &build_chunk, &mut selection, pred)?;
                }
                if selection.is_empty() {
                    continue;
                }
                let output_start = std::time::Instant::now();
                let batches =
                    crate::exec::operators::hashjoin::join_hash_map::gather::gather_join_batches(
                        &probe,
                        &build_chunk,
                        &selection.probe,
                        &selection.build,
                        &output_schema,
                    )?;
                self.record_output_ns(output_start);
                output_batches.extend(batches);
            }
        }

        if !residual_applied_during_selection && let Some(pred) = self.residual_predicate {
            let mut filtered = Vec::with_capacity(output_batches.len());
            for batch in output_batches.into_iter() {
                if batch.num_rows() == 0 {
                    continue;
                }
                let chunk = Chunk::try_new_with_chunk_schema(
                    batch,
                    Arc::clone(&self.join_scope_chunk_schema),
                )?;
                let mask_arr = self.arena.eval(pred, &chunk).map_err(|e| e.to_string())?;
                let mask = mask_arr
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        "join residual predicate must return boolean array".to_string()
                    })?;
                let filtered_batch = filter_record_batch(&chunk.batch, mask)
                    .map_err(|e| format!("join residual filter failed: {e}"))?;
                if filtered_batch.num_rows() > 0 {
                    filtered.push(filtered_batch);
                }
            }
            output_batches = filtered;
        }

        if output_batches.is_empty() {
            return Ok(None);
        }
        let output_rows: usize = output_batches.iter().map(|b| b.num_rows()).sum();
        self.output_rows = self.output_rows.saturating_add(output_rows as u64);
        let first = output_batches.remove(0);
        self.pending_output_batches.extend(output_batches);
        Ok(Some(Chunk::try_new_with_chunk_schema(
            first,
            Arc::clone(&self.join_scope_chunk_schema),
        )?))
    }

    fn join_outer(&mut self, probe_chunks: Vec<Chunk>) -> Result<Option<Chunk>, String> {
        if self.probe_keys.is_empty() {
            return Err("outer join requires non-empty eq join keys".to_string());
        }
        let output_schema = Arc::clone(&self.join_scope_schema);
        let output_unmatched_probe =
            matches!(self.join_type, JoinType::LeftOuter | JoinType::FullOuter);
        let track_build_matches =
            matches!(self.join_type, JoinType::FullOuter | JoinType::RightOuter);

        let table_opt = self.build_table.clone();
        let build_chunk_opt = self.build_chunk.clone();
        let has_build = table_opt.as_ref().map(|t| !t.is_empty()).unwrap_or(false)
            && build_chunk_opt
                .as_ref()
                .map(|chunk| !chunk.is_empty())
                .unwrap_or(false);

        let mut output_batches = Vec::new();
        for probe in probe_chunks {
            if probe.is_empty() {
                continue;
            }

            if !has_build {
                if !output_unmatched_probe {
                    continue;
                }
                let indices = (0..probe.len()).map(|i| i as u32).collect::<Vec<_>>();
                let output_start = std::time::Instant::now();
                let batch = if self.probe_is_left {
                    crate::exec::operators::hashjoin::join_hash_map::gather::gather_left_with_null_right(
                        &probe,
                        &indices,
                        &self.right_chunk_schema.arrow_schema_ref(),
                        &output_schema,
                    )?
                } else {
                    crate::exec::operators::hashjoin::join_hash_map::gather::gather_null_left_with_right(
                        &probe,
                        &indices,
                        &self.left_chunk_schema.arrow_schema_ref(),
                        &output_schema,
                    )?
                };
                self.record_output_ns(output_start);
                if let Some(batch) = batch {
                    output_batches.push(batch);
                }
                continue;
            }

            let table = table_opt.as_ref().expect("build table");
            let build_chunk = build_chunk_opt.as_ref().expect("build chunk");
            let search_start = std::time::Instant::now();
            let (group_ids, mut selection) =
                table.lookup_selection(&self.arena, &self.probe_keys, &probe)?;
            self.record_search_ns(search_start);
            let stats = SearchStats::from_group_ids(&group_ids);
            self.lookup_hit_rows = self.lookup_hit_rows.saturating_add(stats.lookup_hit_rows);
            self.lookup_miss_rows = self.lookup_miss_rows.saturating_add(stats.lookup_miss_rows);
            let mut probe_matched = vec![false; probe.len()];
            if !selection.is_empty() {
                if let Some(pred) = self.residual_predicate {
                    self.residual_rows_checked = self
                        .residual_rows_checked
                        .saturating_add(stats.lookup_hit_rows);
                    self.residual_group_rows_total = self
                        .residual_group_rows_total
                        .saturating_add(selection.len() as u64);
                    self.compact_selection_by_residual(&probe, build_chunk, &mut selection, pred)?;
                }
                for (&probe_row, &build_row) in selection.probe.iter().zip(selection.build.iter()) {
                    probe_matched[probe_row as usize] = true;
                    if track_build_matches && let Some(flags) = self.build_matched.as_mut() {
                        flags.mark(build_row)?;
                    }
                }
                if !selection.is_empty() {
                    let output_start = std::time::Instant::now();
                    let batches = if self.probe_is_left {
                        crate::exec::operators::hashjoin::join_hash_map::gather::gather_join_batches(
                            &probe,
                            build_chunk,
                            &selection.probe,
                            &selection.build,
                            &output_schema,
                        )?
                    } else {
                        crate::exec::operators::hashjoin::join_hash_map::gather::gather_join_batches(
                            build_chunk,
                            &probe,
                            &selection.build,
                            &selection.probe,
                            &output_schema,
                        )?
                    };
                    self.record_output_ns(output_start);
                    output_batches.extend(batches);
                }
            }

            if output_unmatched_probe {
                let mut unmatched = Vec::new();
                for (row, matched) in probe_matched.iter().enumerate() {
                    if !*matched {
                        unmatched.push(row as u32);
                    }
                }
                if !unmatched.is_empty() {
                    let output_start = std::time::Instant::now();
                    let batch = if self.probe_is_left {
                        crate::exec::operators::hashjoin::join_hash_map::gather::gather_left_with_null_right(
                            &probe,
                            &unmatched,
                            &self.right_chunk_schema.arrow_schema_ref(),
                            &output_schema,
                        )?
                    } else {
                        crate::exec::operators::hashjoin::join_hash_map::gather::gather_null_left_with_right(
                            &probe,
                            &unmatched,
                            &self.left_chunk_schema.arrow_schema_ref(),
                            &output_schema,
                        )?
                    };
                    self.record_output_ns(output_start);
                    if let Some(batch) = batch {
                        output_batches.push(batch);
                    }
                }
            }
        }

        if output_batches.is_empty() {
            return Ok(None);
        }
        let output_rows: usize = output_batches.iter().map(|b| b.num_rows()).sum();
        self.output_rows = self.output_rows.saturating_add(output_rows as u64);
        if output_batches.len() == 1 {
            return Ok(Some(Chunk::try_new_with_chunk_schema(
                output_batches.remove(0),
                Arc::clone(&self.join_scope_chunk_schema),
            )?));
        }
        let batch =
            concat_compatible_batches(&output_schema, &output_batches, "inner join concat")?;
        Ok(Some(Chunk::try_new_with_chunk_schema(
            batch,
            Arc::clone(&self.join_scope_chunk_schema),
        )?))
    }

    fn join_semi_anti(&mut self, probe_chunks: Vec<Chunk>) -> Result<Option<Chunk>, String> {
        if self.probe_keys.is_empty() {
            return Err("semi/anti join requires non-empty eq join keys".to_string());
        }

        if self.probe_is_left && matches!(self.join_type, JoinType::RightSemi | JoinType::RightAnti)
        {
            // For both RIGHT SEMI and RIGHT ANTI: only mark which build
            // rows matched, do not produce output during probing.  The
            // actual output is deferred to `finish_one()` where per-driver
            // flags are merged to avoid duplicates across parallel drivers.
            self.mark_build_matches_for_semi_anti(probe_chunks)?;
            return Ok(None);
        }

        let output_schema = probe_chunks[0].schema();

        let is_semi = matches!(self.join_type, JoinType::LeftSemi | JoinType::RightSemi);
        let is_anti = !is_semi;

        let Some(table) = self.build_table.clone() else {
            if is_anti {
                let batches: Vec<_> = probe_chunks.into_iter().map(|c| c.batch).collect();
                if batches.is_empty() {
                    return Ok(None);
                }
                let output_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                self.output_rows = self.output_rows.saturating_add(output_rows as u64);
                let probe_batch = if batches.len() == 1 {
                    batches.into_iter().next().expect("one batch")
                } else {
                    let output_start = std::time::Instant::now();
                    let batch =
                        concat_compatible_batches(&output_schema, &batches, "anti join concat")?;
                    self.record_output_ns(output_start);
                    batch
                };
                return Ok(Some(self.extend_with_null_build_columns(probe_batch)?));
            }
            return Ok(None);
        };
        if table.is_empty()
            || self
                .build_chunk
                .as_ref()
                .map(|chunk| chunk.is_empty())
                .unwrap_or(true)
        {
            if is_anti {
                let batches: Vec<_> = probe_chunks.into_iter().map(|c| c.batch).collect();
                if batches.is_empty() {
                    return Ok(None);
                }
                let output_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                self.output_rows = self.output_rows.saturating_add(output_rows as u64);
                let probe_batch = if batches.len() == 1 {
                    batches.into_iter().next().expect("one batch")
                } else {
                    let output_start = std::time::Instant::now();
                    let batch =
                        concat_compatible_batches(&output_schema, &batches, "anti join concat")?;
                    self.record_output_ns(output_start);
                    batch
                };
                return Ok(Some(self.extend_with_null_build_columns(probe_batch)?));
            }
            return Ok(None);
        }

        if matches!(
            self.join_type,
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::RightSemi | JoinType::RightAnti
        ) {
            let is_semi = matches!(self.join_type, JoinType::LeftSemi | JoinType::RightSemi);
            let mut output_batches = Vec::new();
            for probe in probe_chunks {
                let search_start = std::time::Instant::now();
                let (group_ids, mut selection) =
                    table.lookup_selection(&self.arena, &self.probe_keys, &probe)?;
                self.record_search_ns(search_start);
                let stats = SearchStats::from_group_ids(&group_ids);
                self.lookup_hit_rows = self.lookup_hit_rows.saturating_add(stats.lookup_hit_rows);
                self.lookup_miss_rows =
                    self.lookup_miss_rows.saturating_add(stats.lookup_miss_rows);

                let residual_matched_rows_before = if let Some(pred) = self.residual_predicate
                    && !selection.is_empty()
                {
                    let build_chunk = self
                        .build_chunk
                        .clone()
                        .ok_or_else(|| "semi/anti join build chunk missing".to_string())?;
                    self.residual_rows_checked = self
                        .residual_rows_checked
                        .saturating_add(stats.lookup_hit_rows);
                    self.residual_group_rows_total = self
                        .residual_group_rows_total
                        .saturating_add(selection.len() as u64);
                    let residual_matched_rows_before = self.residual_matched_rows;
                    self.compact_selection_by_residual(&probe, &build_chunk, &mut selection, pred)?;
                    Some(residual_matched_rows_before)
                } else {
                    None
                };

                let mut matched = vec![false; probe.len()];
                for probe_row in selection.probe.iter() {
                    let slot = *probe_row as usize;
                    if slot >= matched.len() {
                        return Err(format!(
                            "semi/anti probe row out of bounds: row={} rows={}",
                            slot,
                            matched.len()
                        ));
                    }
                    matched[slot] = true;
                }
                if let Some(residual_matched_rows_before) = residual_matched_rows_before {
                    let matched_probe_rows = matched.iter().filter(|matched| **matched).count();
                    self.residual_matched_rows =
                        residual_matched_rows_before.saturating_add(matched_probe_rows as u64);
                }

                let keep = matched
                    .into_iter()
                    .map(|matched| if is_semi { matched } else { !matched })
                    .collect::<Vec<bool>>();
                let output_start = std::time::Instant::now();
                let mask = BooleanArray::from(keep);
                let filtered_batch = filter_record_batch(&probe.batch, &mask)
                    .map_err(|e| format!("semi/anti filter failed: {e}"))?;
                self.record_output_ns(output_start);
                if filtered_batch.num_rows() > 0 {
                    output_batches.push(filtered_batch);
                }
            }

            if output_batches.is_empty() {
                return Ok(None);
            }
            let output_rows: usize = output_batches.iter().map(|b| b.num_rows()).sum();
            self.output_rows = self.output_rows.saturating_add(output_rows as u64);
            let probe_batch = if output_batches.len() == 1 {
                output_batches.remove(0)
            } else {
                let output_start = std::time::Instant::now();
                let batch = concat_compatible_batches(
                    &output_schema,
                    &output_batches,
                    "semi anti join concat",
                )?;
                self.record_output_ns(output_start);
                batch
            };
            return Ok(Some(self.extend_with_null_build_columns(probe_batch)?));
        }
        Ok(None)
    }

    fn join_null_aware_left_anti(
        &mut self,
        probe_chunks: Vec<Chunk>,
    ) -> Result<Option<Chunk>, String> {
        if self.probe_keys.is_empty() {
            return Err("semi/anti join requires non-empty eq join keys".to_string());
        }
        if self.global_build_row_count == 0 {
            let output_schema = probe_chunks[0].schema();
            let batches: Vec<_> = probe_chunks.into_iter().map(|c| c.batch).collect();
            if batches.is_empty() {
                return Ok(None);
            }
            let output_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            self.output_rows = self.output_rows.saturating_add(output_rows as u64);
            let probe_batch = if batches.len() == 1 {
                batches.into_iter().next().expect("one batch")
            } else {
                let output_start = std::time::Instant::now();
                let batch = concat_compatible_batches(
                    &output_schema,
                    &batches,
                    "null aware anti join concat",
                )?;
                self.record_output_ns(output_start);
                batch
            };
            return Ok(Some(self.extend_with_null_build_columns(probe_batch)?));
        }

        let output_schema = probe_chunks[0].schema();
        let table_opt = self.build_table.clone();
        let has_residual = self.residual_predicate.is_some();
        if !has_residual && self.global_build_has_null_key {
            return Ok(None);
        }
        if has_residual && self.build_null_key_rows.is_none() {
            return Err(
                "null-aware left anti join with residual requires build null-key rows".to_string(),
            );
        }

        let build_null_key_rows = self.build_null_key_rows.clone();
        let flat_build_null_key_rows: &[u32] = build_null_key_rows
            .as_ref()
            .map(|rows| rows.as_slice())
            .unwrap_or(&[]);
        let build_chunk_for_residual = if has_residual {
            self.build_chunk.clone()
        } else {
            None
        };

        let mut output_batches = Vec::new();
        for probe in probe_chunks {
            let mut probe_key_arrays = Vec::with_capacity(self.probe_keys.len());
            for key in &self.probe_keys {
                probe_key_arrays.push(self.arena.eval(*key, &probe).map_err(|e| e.to_string())?);
            }

            let (group_ids, mut equal_selection) = if let Some(table) = table_opt.as_ref() {
                let search_start = std::time::Instant::now();
                let result = if has_residual {
                    table.lookup_selection(&self.arena, &self.probe_keys, &probe)?
                } else {
                    (
                        table.lookup_group_ids(&self.arena, &self.probe_keys, &probe)?,
                        JoinSelection::new(),
                    )
                };
                self.record_search_ns(search_start);
                result
            } else {
                (vec![None; probe.len()], JoinSelection::new())
            };

            let probe_null_rows = (0..probe.len())
                .map(|row| {
                    probe_key_arrays
                        .iter()
                        .any(|key_array| !key_array.is_valid(row))
                })
                .collect::<Vec<_>>();
            let mut matched_equal = vec![false; probe.len()];
            let mut matched_null_key = vec![false; probe.len()];
            let mut matched_any = vec![false; probe.len()];
            let mut residual_matched_probe_rows = vec![false; probe.len()];

            if let Some(pred) = self.residual_predicate {
                if table_opt.is_some() {
                    let stats = SearchStats::from_group_ids(&group_ids);
                    self.residual_rows_checked = self
                        .residual_rows_checked
                        .saturating_add(stats.lookup_hit_rows);
                    self.residual_group_rows_total = self
                        .residual_group_rows_total
                        .saturating_add(equal_selection.len() as u64);
                    if !equal_selection.is_empty() {
                        let build_chunk = build_chunk_for_residual.as_ref().ok_or_else(|| {
                            "null-aware anti join build chunk missing".to_string()
                        })?;
                        self.compact_null_aware_selection(
                            &probe,
                            build_chunk,
                            &mut equal_selection,
                            pred,
                            &mut matched_equal,
                            &mut residual_matched_probe_rows,
                            "null-aware anti equality residual",
                        )?;
                    }
                }

                if !flat_build_null_key_rows.is_empty() {
                    let build_chunk = build_chunk_for_residual
                        .as_ref()
                        .ok_or_else(|| "null-aware anti join build chunk missing".to_string())?;
                    let probe_rows = (0..probe.len()).map(|row| row as u32).collect::<Vec<_>>();
                    self.compact_cross_selection_in_chunks(
                        &probe,
                        build_chunk,
                        &probe_rows,
                        flat_build_null_key_rows,
                        pred,
                        &mut matched_null_key,
                        &mut residual_matched_probe_rows,
                        "null-aware anti null-key residual",
                    )?;
                }

                let has_local_build_rows = build_chunk_for_residual
                    .as_ref()
                    .map(|chunk| !chunk.is_empty())
                    .unwrap_or(self.build_partition_row_count > 0);
                if has_local_build_rows && probe_null_rows.iter().any(|is_null| *is_null) {
                    let build_chunk = build_chunk_for_residual
                        .as_ref()
                        .ok_or_else(|| "null-aware anti join build chunk missing".to_string())?;
                    let all_build_rows = (0..build_chunk.len())
                        .map(|row| {
                            u32::try_from(row)
                                .map_err(|_| "join residual build row id overflow".to_string())
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let probe_rows = probe_null_rows
                        .iter()
                        .enumerate()
                        .filter_map(|(row, is_null)| is_null.then_some(row as u32))
                        .collect::<Vec<_>>();
                    self.compact_cross_selection_in_chunks(
                        &probe,
                        build_chunk,
                        &probe_rows,
                        &all_build_rows,
                        pred,
                        &mut matched_any,
                        &mut residual_matched_probe_rows,
                        "null-aware anti all-build residual",
                    )?;
                }
            }

            let mut keep = Vec::with_capacity(probe.len());
            for (row, group_id_opt) in group_ids.iter().enumerate() {
                let key_is_null = probe_null_rows[row];
                if self.residual_predicate.is_some() {
                    if key_is_null {
                        keep.push(!matched_any[row]);
                    } else {
                        keep.push(!(matched_equal[row] || matched_null_key[row]));
                    }
                } else {
                    let matched = group_id_opt.is_some();
                    keep.push(!key_is_null && !matched);
                }
            }

            let output_start = std::time::Instant::now();
            let mask = BooleanArray::from(keep);
            let filtered_batch = filter_record_batch(&probe.batch, &mask)
                .map_err(|e| format!("null-aware anti filter failed: {e}"))?;
            self.record_output_ns(output_start);
            if filtered_batch.num_rows() > 0 {
                output_batches.push(filtered_batch);
            }
        }

        if output_batches.is_empty() {
            return Ok(None);
        }
        let output_rows: usize = output_batches.iter().map(|b| b.num_rows()).sum();
        self.output_rows = self.output_rows.saturating_add(output_rows as u64);
        let probe_batch = if output_batches.len() == 1 {
            output_batches.remove(0)
        } else {
            let output_start = std::time::Instant::now();
            let batch = concat_compatible_batches(
                &output_schema,
                &output_batches,
                "null aware anti join output concat",
            )?;
            self.record_output_ns(output_start);
            batch
        };
        Ok(Some(self.extend_with_null_build_columns(probe_batch)?))
    }

    fn mark_build_matches_for_semi_anti(&mut self, probe_chunks: Vec<Chunk>) -> Result<(), String> {
        if probe_chunks.is_empty() {
            return Ok(());
        }
        let Some(table) = self.build_table.clone() else {
            return Ok(());
        };
        let Some(build_chunk) = self.build_chunk.clone() else {
            return Ok(());
        };
        if table.is_empty() || build_chunk.is_empty() {
            return Ok(());
        }
        if self.build_matched.is_none() {
            return Ok(());
        }

        for probe in probe_chunks {
            if probe.is_empty() {
                continue;
            }
            let search_start = std::time::Instant::now();
            let (group_ids, mut selection) =
                table.lookup_selection(&self.arena, &self.probe_keys, &probe)?;
            self.record_search_ns(search_start);
            let stats = SearchStats::from_group_ids(&group_ids);
            self.lookup_hit_rows = self.lookup_hit_rows.saturating_add(stats.lookup_hit_rows);
            self.lookup_miss_rows = self.lookup_miss_rows.saturating_add(stats.lookup_miss_rows);

            if selection.is_empty() {
                continue;
            }
            let residual_matched_rows_before = if let Some(pred) = self.residual_predicate {
                self.residual_rows_checked = self
                    .residual_rows_checked
                    .saturating_add(stats.lookup_hit_rows);
                self.residual_group_rows_total = self
                    .residual_group_rows_total
                    .saturating_add(selection.len() as u64);
                let residual_matched_rows_before = self.residual_matched_rows;
                self.compact_selection_by_residual(&probe, &build_chunk, &mut selection, pred)?;
                Some(residual_matched_rows_before)
            } else {
                None
            };

            let Some(flags) = self.build_matched.as_mut() else {
                return Ok(());
            };
            let mut unique_build_rows_marked = 0u64;
            for build_row in selection.build {
                if flags.mark(build_row)? {
                    unique_build_rows_marked = unique_build_rows_marked.saturating_add(1);
                }
            }
            if let Some(residual_matched_rows_before) = residual_matched_rows_before {
                self.residual_matched_rows =
                    residual_matched_rows_before.saturating_add(unique_build_rows_marked);
            }
        }

        Ok(())
    }

    fn apply_runtime_filters(&self, chunks: Vec<Chunk>) -> Result<Vec<Chunk>, String> {
        if !self.should_apply_runtime_filters() {
            return Ok(chunks);
        }
        let Some(filters) = self.runtime_filters.as_ref() else {
            return Ok(chunks);
        };
        let mut out = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            if let Some(filtered) =
                filters.filter_probe_chunk(&self.arena, &self.probe_keys, chunk)?
                && !filtered.is_empty()
            {
                out.push(filtered);
            }
        }
        Ok(out)
    }

    fn should_apply_runtime_filters(&self) -> bool {
        matches!(
            self.join_type,
            JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
        )
    }
}

/// Return a stable string label for one join type, used by diagnostics and errors.
pub(crate) fn join_type_str(join_type: JoinType) -> &'static str {
    match join_type {
        JoinType::Inner => "INNER",
        JoinType::LeftOuter => "LEFT_OUTER",
        JoinType::RightOuter => "RIGHT_OUTER",
        JoinType::FullOuter => "FULL_OUTER",
        JoinType::LeftSemi => "LEFT_SEMI",
        JoinType::RightSemi => "RIGHT_SEMI",
        JoinType::LeftAnti => "LEFT_ANTI",
        JoinType::RightAnti => "RIGHT_ANTI",
        JoinType::NullAwareLeftAnti => "NULL_AWARE_LEFT_ANTI",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;

    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{ChunkSchema, ChunkSchemaRef};
    use crate::exec::expr::ExprNode;
    use crate::exec::operators::hashjoin::join_hash_map::build_store::BuildStore;

    const LEFT_K_SLOT_ID: SlotId = SlotId::new(1);
    const LEFT_V_SLOT_ID: SlotId = SlotId::new(2);
    const RIGHT_K_SLOT_ID: SlotId = SlotId::new(3);
    const RIGHT_W_SLOT_ID: SlotId = SlotId::new(4);

    fn schema_kv(k_name: &str, v_name: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(k_name, DataType::Int32, false),
            Field::new(v_name, DataType::Int32, false),
        ]))
    }

    fn join_schema(left: &SchemaRef, right: &SchemaRef) -> SchemaRef {
        let mut fields = left.fields().to_vec();
        fields.extend(right.fields().to_vec());
        Arc::new(Schema::new(fields))
    }

    fn chunk_schema_of(schema: &SchemaRef, slot_ids: &[SlotId]) -> ChunkSchemaRef {
        ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), slot_ids)
            .expect("chunk schema")
    }

    fn chunk_of_two(schema: SchemaRef, slot_ids: &[SlotId], k: &[i32], v: &[i32]) -> Chunk {
        assert_eq!(k.len(), v.len());
        let k_arr = Arc::new(Int32Array::from(k.to_vec())) as ArrayRef;
        let v_arr = Arc::new(Int32Array::from(v.to_vec())) as ArrayRef;
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![k_arr, v_arr]).expect("record batch");
        Chunk::new_with_chunk_schema(batch, chunk_schema_of(&schema, slot_ids))
    }

    #[test]
    fn right_semi_residual_counts_unique_build_rows_marked() {
        let left_schema = schema_kv("lk", "lv");
        let right_schema = schema_kv("rk", "rw");
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let left_v = arena.push_typed(ExprNode::SlotId(LEFT_V_SLOT_ID), DataType::Int32);
        let right_w = arena.push_typed(ExprNode::SlotId(RIGHT_W_SLOT_ID), DataType::Int32);
        let residual = arena.push_typed(ExprNode::Lt(left_v, right_w), DataType::Boolean);
        let arena = Arc::new(arena);

        let build_chunk = chunk_of_two(
            Arc::clone(&right_schema),
            &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID],
            &[1],
            &[10],
        );
        let mut build_table =
            JoinHashMap::new_chained(vec![DataType::Int32], vec![false]).expect("build table");
        let build_key_arrays = vec![
            build_chunk
                .column_by_slot_id(RIGHT_K_SLOT_ID)
                .expect("build key"),
        ];
        build_table
            .add_build_rows(&build_key_arrays, build_chunk.len())
            .expect("add build rows");
        build_table.finalize().expect("finalize build table");
        let artifact = Arc::new(JoinBuildArtifact::new(
            Some(BuildStore::new(build_chunk.clone())),
            Some(build_table),
            1,
            false,
            None,
            None,
        ));

        let mut core = HashJoinProbeCore::new(
            Arc::clone(&arena),
            JoinType::RightSemi,
            vec![probe_key],
            Some(residual),
            true,
            chunk_schema_of(&left_schema, &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID]),
            chunk_schema_of(&right_schema, &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID]),
            chunk_schema_of(
                &join_scope_schema,
                &[
                    LEFT_K_SLOT_ID,
                    LEFT_V_SLOT_ID,
                    RIGHT_K_SLOT_ID,
                    RIGHT_W_SLOT_ID,
                ],
            ),
        );
        core.set_build_artifact(artifact, 1, false)
            .expect("set build");

        let probe_chunk = chunk_of_two(
            Arc::clone(&left_schema),
            &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID],
            &[1, 1],
            &[1, 2],
        );

        let probe_out = core.join_probe_chunks(vec![probe_chunk]).expect("probe");
        assert!(probe_out.is_none());

        let build_out = core
            .build_right_semi_anti_output(true)
            .expect("right semi output")
            .expect("build output");
        assert_eq!(build_out.num_rows(), 1);
        assert_eq!(core.residual_matched_rows(), 1);
    }

    #[test]
    fn null_aware_left_anti_residual_allows_empty_local_build_partition() {
        let left_schema = schema_kv("lk", "lv");
        let right_schema = schema_kv("rk", "rw");
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let left_v = arena.push_typed(ExprNode::SlotId(LEFT_V_SLOT_ID), DataType::Int32);
        let right_w = arena.push_typed(ExprNode::SlotId(RIGHT_W_SLOT_ID), DataType::Int32);
        let residual = arena.push_typed(ExprNode::Lt(left_v, right_w), DataType::Boolean);
        let arena = Arc::new(arena);

        let artifact = Arc::new(JoinBuildArtifact::new(
            None,
            None,
            0,
            false,
            Some(Arc::new(Vec::new())),
            None,
        ));
        let mut core = HashJoinProbeCore::new(
            Arc::clone(&arena),
            JoinType::NullAwareLeftAnti,
            vec![probe_key],
            Some(residual),
            true,
            chunk_schema_of(&left_schema, &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID]),
            chunk_schema_of(&right_schema, &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID]),
            chunk_schema_of(
                &join_scope_schema,
                &[
                    LEFT_K_SLOT_ID,
                    LEFT_V_SLOT_ID,
                    RIGHT_K_SLOT_ID,
                    RIGHT_W_SLOT_ID,
                ],
            ),
        );
        core.set_build_artifact(artifact, 1, false)
            .expect("set build");

        let probe_chunk = chunk_of_two(
            Arc::clone(&left_schema),
            &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID],
            &[1, 2],
            &[100, 200],
        );

        let out = core
            .join_probe_chunks(vec![probe_chunk])
            .expect("empty local build partition should not require build chunk")
            .expect("probe rows survive empty local build partition");

        assert_eq!(out.len(), 2);
        let left_k = out
            .columns()
            .first()
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let left_v = out
            .columns()
            .get(1)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            (0..out.len())
                .map(|row| (left_k.value(row), left_v.value(row)))
                .collect::<Vec<_>>(),
            vec![(1, 100), (2, 200)]
        );
    }
}
