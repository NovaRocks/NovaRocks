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

use std::sync::Arc;

use arrow::array::{Array, BooleanArray, UInt32Array};
use arrow::compute::{concat_batches, filter_record_batch, take_record_batch};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use super::build_artifact::JoinBuildArtifact;
use super::join_hash_table::JoinHashTable;
use super::join_probe_utils::{
    build_join_batch, build_left_with_null_right, build_null_left_with_right, concat_schemas,
    cross_join_chunk, keyed_join_chunk, lookup_group_ids,
};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::node::join::JoinType;
use crate::exec::runtime_filter::LocalRuntimeFilterSet;

/// Core hash-join probing engine that performs key lookup and join-type specific row assembly.
pub(crate) struct HashJoinProbeCore {
    arena: Arc<ExprArena>,
    join_type: JoinType,
    probe_keys: Vec<ExprId>,
    residual_predicate: Option<ExprId>,
    probe_is_left: bool,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    join_scope_schema: SchemaRef,
    build_loaded: bool,
    build_batches: Arc<Vec<Chunk>>,
    build_null_key_rows: Option<Arc<Vec<Vec<u32>>>>,
    build_table: Option<Arc<JoinHashTable>>,
    runtime_filters: Option<Arc<LocalRuntimeFilterSet>>,
    output_schema: Option<SchemaRef>,
    build_matched: Option<Vec<Vec<bool>>>,
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
}

impl HashJoinProbeCore {
    pub(crate) fn new(
        arena: Arc<ExprArena>,
        join_type: JoinType,
        probe_keys: Vec<ExprId>,
        residual_predicate: Option<ExprId>,
        probe_is_left: bool,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        join_scope_schema: SchemaRef,
    ) -> Self {
        Self {
            arena,
            join_type,
            probe_keys,
            residual_predicate,
            probe_is_left,
            left_schema,
            right_schema,
            join_scope_schema,
            build_loaded: false,
            build_batches: Arc::new(Vec::new()),
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
        }
    }

    pub(crate) fn join_type(&self) -> JoinType {
        self.join_type
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
        self.build_batches = Arc::clone(&artifact.build_batches);
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
            let mut flags = Vec::with_capacity(self.build_batches.len());
            for b in self.build_batches.iter() {
                flags.push(vec![false; b.len()]);
            }
            self.build_matched = Some(flags);
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

    pub(crate) fn build_batches_len(&self) -> usize {
        self.build_batches.len()
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

    pub(crate) fn build_full_outer_unmatched_build(
        &mut self,
    ) -> Result<Option<RecordBatch>, String> {
        if self.build_batches.is_empty() {
            return Ok(None);
        }
        let Some(flags) = self.build_matched.as_ref() else {
            return Ok(None);
        };

        let output_schema = Arc::clone(&self.join_scope_schema);

        let mut batches = Vec::new();
        for (batch_idx, build_batch) in self.build_batches.iter().enumerate() {
            let matched = flags
                .get(batch_idx)
                .ok_or_else(|| "join build match flags missing".to_string())?;
            if matched.is_empty() {
                continue;
            }
            let mut indices = Vec::new();
            for (row, is_matched) in matched.iter().enumerate() {
                if !*is_matched {
                    indices.push(row as u32);
                }
            }
            let Some(batch) = build_null_left_with_right(
                build_batch,
                &indices,
                &self.left_schema,
                &output_schema,
            )?
            else {
                continue;
            };
            if batch.num_rows() > 0 {
                batches.push(batch);
            }
        }

        if batches.is_empty() {
            return Ok(None);
        }
        if batches.len() == 1 {
            return Ok(Some(batches.remove(0)));
        }
        let batch = concat_batches(&output_schema, &batches).map_err(|e| e.to_string())?;
        Ok(Some(batch))
    }

    fn build_right_semi_output_from_indices(
        &self,
        indices_by_batch: &[Vec<u32>],
    ) -> Result<Option<RecordBatch>, String> {
        if self.build_batches.is_empty() || indices_by_batch.is_empty() {
            return Ok(None);
        }

        let output_schema = self
            .build_batches
            .first()
            .map(|b| b.schema())
            .ok_or_else(|| "join build schema missing".to_string())?;

        let mut batches = Vec::new();
        for (batch_idx, build_batch) in self.build_batches.iter().enumerate() {
            let Some(indices) = indices_by_batch.get(batch_idx) else {
                continue;
            };
            if indices.is_empty() {
                continue;
            }
            let idx_arr = UInt32Array::from(indices.clone());
            let taken =
                take_record_batch(&build_batch.batch, &idx_arr).map_err(|e| e.to_string())?;
            if taken.num_rows() > 0 {
                batches.push(taken);
            }
        }

        if batches.is_empty() {
            return Ok(None);
        }
        if batches.len() == 1 {
            return Ok(Some(batches.remove(0)));
        }
        let batch = concat_batches(&output_schema, &batches).map_err(|e| e.to_string())?;
        Ok(Some(batch))
    }

    pub(crate) fn build_right_semi_anti_output(
        &mut self,
        want_matched: bool,
    ) -> Result<Option<RecordBatch>, String> {
        if self.build_batches.is_empty() {
            return Ok(None);
        }
        let Some(flags) = self.build_matched.as_ref() else {
            return Ok(None);
        };

        let output_schema = self
            .build_batches
            .first()
            .map(|b| b.schema())
            .ok_or_else(|| "join build schema missing".to_string())?;

        let mut batches = Vec::new();
        for (batch_idx, build_batch) in self.build_batches.iter().enumerate() {
            let matched = flags
                .get(batch_idx)
                .ok_or_else(|| "join build match flags missing".to_string())?;
            if matched.is_empty() {
                continue;
            }
            let mask = matched
                .iter()
                .map(|v| if want_matched { *v } else { !*v })
                .collect::<Vec<_>>();
            let mask = BooleanArray::from(mask);
            let filtered =
                filter_record_batch(&build_batch.batch, &mask).map_err(|e| e.to_string())?;
            if filtered.num_rows() > 0 {
                batches.push(filtered);
            }
        }

        if batches.is_empty() {
            return Ok(None);
        }
        if batches.len() == 1 {
            return Ok(Some(batches.remove(0)));
        }
        let batch = concat_batches(&output_schema, &batches).map_err(|e| e.to_string())?;
        Ok(Some(batch))
    }

    pub(crate) fn merge_join_outputs(
        &mut self,
        left: Option<Chunk>,
        right: Option<RecordBatch>,
        count_right_rows: bool,
    ) -> Result<Option<Chunk>, String> {
        if count_right_rows {
            if let Some(batch) = right.as_ref() {
                self.output_rows = self.output_rows.saturating_add(batch.num_rows() as u64);
            }
        }
        match (left, right) {
            (None, None) => Ok(None),
            (Some(chunk), None) => Ok(Some(chunk)),
            (None, Some(batch)) => Ok(Some(Chunk::new(batch))),
            (Some(left_chunk), Some(right_batch)) => {
                if left_chunk.is_empty() && right_batch.num_rows() == 0 {
                    return Ok(Some(left_chunk));
                }
                if left_chunk.is_empty() {
                    return Ok(Some(Chunk::new(right_batch)));
                }
                if right_batch.num_rows() == 0 {
                    return Ok(Some(left_chunk));
                }
                let batches = vec![left_chunk.batch, right_batch];
                let batch =
                    concat_batches(&self.join_scope_schema, &batches).map_err(|e| e.to_string())?;
                Ok(Some(Chunk::new(batch)))
            }
        }
    }

    fn join_inner(&mut self, probe_chunks: Vec<Chunk>) -> Result<Option<Chunk>, String> {
        if self.build_batches.is_empty() {
            return Ok(None);
        }
        let right_schema = self
            .build_batches
            .first()
            .map(|b| b.schema())
            .ok_or_else(|| "join build schema missing".to_string())?;

        let output_schema = self
            .output_schema
            .get_or_insert_with(|| concat_schemas(probe_chunks[0].schema(), right_schema.clone()));

        let mut output_batches = Vec::new();
        if self.probe_keys.is_empty() {
            for left in probe_chunks {
                for right in self.build_batches.iter() {
                    if let Some(batch) = cross_join_chunk(&left, right, output_schema)? {
                        output_batches.push(batch);
                    }
                }
            }
        } else {
            let Some(table) = self.build_table.as_ref() else {
                return Ok(None);
            };
            if table.is_empty() {
                return Ok(None);
            }
            for left in probe_chunks {
                let batches = keyed_join_chunk(
                    &self.arena,
                    &self.probe_keys,
                    &left,
                    self.build_batches.as_ref(),
                    table,
                    output_schema,
                )?;
                output_batches.extend(batches);
            }
        }

        if let Some(pred) = self.residual_predicate {
            let mut filtered = Vec::with_capacity(output_batches.len());
            for batch in output_batches.into_iter() {
                if batch.num_rows() == 0 {
                    continue;
                }
                let chunk = Chunk::new(batch);
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
        if output_batches.len() == 1 {
            return Ok(Some(Chunk::new(output_batches.remove(0))));
        }

        let batch = concat_batches(output_schema, &output_batches).map_err(|e| e.to_string())?;
        Ok(Some(Chunk::new(batch)))
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

        let table_opt = self.build_table.as_ref();
        let has_build =
            table_opt.map(|t| !t.is_empty()).unwrap_or(false) && !self.build_batches.is_empty();

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
                let batch = if self.probe_is_left {
                    build_left_with_null_right(
                        &probe,
                        &indices,
                        &self.right_schema,
                        &output_schema,
                    )?
                } else {
                    build_null_left_with_right(&probe, &indices, &self.left_schema, &output_schema)?
                };
                if let Some(batch) = batch {
                    output_batches.push(batch);
                }
                continue;
            }

            let table = table_opt.expect("build table");
            let group_ids = lookup_group_ids(&self.arena, &self.probe_keys, &probe, table)?;
            if group_ids.is_empty() {
                continue;
            }

            let mut probe_matched = vec![false; probe.len()];
            let mut probe_indices_by_batch = vec![Vec::<u32>::new(); self.build_batches.len()];
            let mut build_indices_by_batch = vec![Vec::<u32>::new(); self.build_batches.len()];

            for (probe_row, group_id_opt) in group_ids.iter().enumerate() {
                let Some(group_id) = group_id_opt else {
                    continue;
                };
                let rows = table
                    .group_rows_slice(*group_id)
                    .map_err(|e| e.to_string())?;
                for &build_row in rows {
                    let (batch_idx, row_idx) =
                        table.row_location(build_row).map_err(|e| e.to_string())?;
                    let slot = batch_idx as usize;
                    if slot >= self.build_batches.len() {
                        return Err("join row batch index out of bounds".to_string());
                    }
                    probe_indices_by_batch[slot].push(probe_row as u32);
                    build_indices_by_batch[slot].push(row_idx);
                }
            }

            for (batch_idx, build_batch) in self.build_batches.iter().enumerate() {
                let probe_indices = &probe_indices_by_batch[batch_idx];
                if probe_indices.is_empty() {
                    continue;
                }
                let build_indices = &build_indices_by_batch[batch_idx];

                let candidate = if self.probe_is_left {
                    build_join_batch(
                        &probe,
                        build_batch,
                        probe_indices,
                        build_indices,
                        &output_schema,
                    )?
                } else {
                    build_join_batch(
                        build_batch,
                        &probe,
                        build_indices,
                        probe_indices,
                        &output_schema,
                    )?
                };
                let Some(candidate) = candidate else {
                    continue;
                };
                if candidate.num_rows() == 0 {
                    continue;
                }

                let mask = if let Some(pred) = self.residual_predicate {
                    let chunk = Chunk::new(candidate.clone());
                    let mask_arr = self.arena.eval(pred, &chunk).map_err(|e| e.to_string())?;
                    mask_arr
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| {
                            "join residual predicate must return boolean array".to_string()
                        })?
                        .clone()
                } else {
                    BooleanArray::from(vec![true; candidate.num_rows()])
                };

                for i in 0..mask.len() {
                    if !mask.is_valid(i) || !mask.value(i) {
                        continue;
                    }
                    let probe_row = *probe_indices
                        .get(i)
                        .ok_or_else(|| "join probe index out of bounds".to_string())?
                        as usize;
                    if probe_row < probe_matched.len() {
                        probe_matched[probe_row] = true;
                    }

                    if track_build_matches {
                        let build_row = *build_indices
                            .get(i)
                            .ok_or_else(|| "join build index out of bounds".to_string())?
                            as usize;
                        if let Some(flags) = self.build_matched.as_mut() {
                            if let Some(batch_flags) = flags.get_mut(batch_idx) {
                                if build_row < batch_flags.len() {
                                    batch_flags[build_row] = true;
                                }
                            }
                        }
                    }
                }

                let filtered = if self.residual_predicate.is_some() {
                    filter_record_batch(&candidate, &mask)
                        .map_err(|e| format!("join residual filter failed: {e}"))?
                } else {
                    candidate
                };
                if filtered.num_rows() > 0 {
                    output_batches.push(filtered);
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
                    let batch = if self.probe_is_left {
                        build_left_with_null_right(
                            &probe,
                            &unmatched,
                            &self.right_schema,
                            &output_schema,
                        )?
                    } else {
                        build_null_left_with_right(
                            &probe,
                            &unmatched,
                            &self.left_schema,
                            &output_schema,
                        )?
                    };
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
            return Ok(Some(Chunk::new(output_batches.remove(0))));
        }
        let batch = concat_batches(&output_schema, &output_batches).map_err(|e| e.to_string())?;
        Ok(Some(Chunk::new(batch)))
    }

    fn join_semi_anti(&mut self, probe_chunks: Vec<Chunk>) -> Result<Option<Chunk>, String> {
        if self.probe_keys.is_empty() {
            return Err("semi/anti join requires non-empty eq join keys".to_string());
        }

        if self.probe_is_left && matches!(self.join_type, JoinType::RightSemi | JoinType::RightAnti)
        {
            let want_output = self.join_type == JoinType::RightSemi;
            let output_indices =
                self.mark_build_matches_for_semi_anti(probe_chunks, want_output)?;
            if !want_output {
                return Ok(None);
            }
            let Some(output_indices) = output_indices else {
                return Ok(None);
            };
            let Some(batch) = self.build_right_semi_output_from_indices(&output_indices)? else {
                return Ok(None);
            };
            self.output_rows = self.output_rows.saturating_add(batch.num_rows() as u64);
            return Ok(Some(Chunk::new(batch)));
        }

        let output_schema = probe_chunks[0].schema();

        let is_semi = matches!(self.join_type, JoinType::LeftSemi | JoinType::RightSemi);
        let is_anti = !is_semi;

        let Some(table) = self.build_table.as_ref() else {
            if is_anti {
                let batches = probe_chunks
                    .into_iter()
                    .map(|c| c.batch)
                    .collect::<Vec<_>>();
                if batches.is_empty() {
                    return Ok(None);
                }
                let output_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                self.output_rows = self.output_rows.saturating_add(output_rows as u64);
                if batches.len() == 1 {
                    return Ok(Some(Chunk::new(batches.into_iter().next().unwrap())));
                }
                let batch = concat_batches(&output_schema, &batches).map_err(|e| e.to_string())?;
                return Ok(Some(Chunk::new(batch)));
            }
            return Ok(None);
        };
        if table.is_empty() || self.build_batches.is_empty() {
            if is_anti {
                let batches = probe_chunks
                    .into_iter()
                    .map(|c| c.batch)
                    .collect::<Vec<_>>();
                if batches.is_empty() {
                    return Ok(None);
                }
                let output_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                self.output_rows = self.output_rows.saturating_add(output_rows as u64);
                if batches.len() == 1 {
                    return Ok(Some(Chunk::new(batches.into_iter().next().unwrap())));
                }
                let batch = concat_batches(&output_schema, &batches).map_err(|e| e.to_string())?;
                return Ok(Some(Chunk::new(batch)));
            }
            return Ok(None);
        }

        let build_schema = self
            .build_batches
            .first()
            .map(|b| b.schema())
            .ok_or_else(|| "join build schema missing".to_string())?;
        let join_scope_schema = if matches!(self.join_type, JoinType::LeftSemi | JoinType::LeftAnti)
        {
            concat_schemas(output_schema.clone(), build_schema)
        } else {
            concat_schemas(build_schema, output_schema.clone())
        };

        let pred = self.residual_predicate;
        let mut output_batches = Vec::new();
        for probe in probe_chunks {
            let group_ids = lookup_group_ids(&self.arena, &self.probe_keys, &probe, table)?;
            if group_ids.is_empty() {
                continue;
            }

            for group_id_opt in group_ids.iter() {
                if group_id_opt.is_some() {
                    self.lookup_hit_rows = self.lookup_hit_rows.saturating_add(1);
                } else {
                    self.lookup_miss_rows = self.lookup_miss_rows.saturating_add(1);
                }
            }

            let keep = if let Some(pred) = pred {
                let (matched, rows_checked, eval_batches, eval_pairs, group_rows_total) =
                    Self::probe_chunk_matches_residual(
                        &self.arena,
                        self.join_type,
                        self.build_batches.as_ref(),
                        table,
                        &probe,
                        &group_ids,
                        &join_scope_schema,
                        pred,
                    )?;
                self.residual_rows_checked =
                    self.residual_rows_checked.saturating_add(rows_checked);
                self.residual_eval_batches =
                    self.residual_eval_batches.saturating_add(eval_batches);
                self.residual_eval_pairs = self.residual_eval_pairs.saturating_add(eval_pairs);
                self.residual_group_rows_total = self
                    .residual_group_rows_total
                    .saturating_add(group_rows_total);
                self.residual_matched_rows = self
                    .residual_matched_rows
                    .saturating_add(matched.iter().filter(|v| **v).count() as u64);

                matched
                    .into_iter()
                    .map(|matched| if is_semi { matched } else { !matched })
                    .collect::<Vec<bool>>()
            } else {
                group_ids
                    .iter()
                    .map(|group_id_opt| {
                        let matched = group_id_opt.is_some();
                        if is_semi { matched } else { !matched }
                    })
                    .collect::<Vec<bool>>()
            };

            let mask = BooleanArray::from(keep);
            let filtered_batch = filter_record_batch(&probe.batch, &mask)
                .map_err(|e| format!("semi/anti filter failed: {e}"))?;
            if filtered_batch.num_rows() > 0 {
                output_batches.push(filtered_batch);
            }
        }

        if output_batches.is_empty() {
            return Ok(None);
        }
        let output_rows: usize = output_batches.iter().map(|b| b.num_rows()).sum();
        self.output_rows = self.output_rows.saturating_add(output_rows as u64);
        if output_batches.len() == 1 {
            return Ok(Some(Chunk::new(output_batches.remove(0))));
        }
        let batch = concat_batches(&output_schema, &output_batches).map_err(|e| e.to_string())?;
        Ok(Some(Chunk::new(batch)))
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
            let batches = probe_chunks
                .into_iter()
                .map(|c| c.batch)
                .collect::<Vec<_>>();
            if batches.is_empty() {
                return Ok(None);
            }
            let output_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            self.output_rows = self.output_rows.saturating_add(output_rows as u64);
            if batches.len() == 1 {
                return Ok(Some(Chunk::new(batches.into_iter().next().unwrap())));
            }
            let batch = concat_batches(&output_schema, &batches).map_err(|e| e.to_string())?;
            return Ok(Some(Chunk::new(batch)));
        }

        let output_schema = probe_chunks[0].schema();
        let table_opt = self.build_table.as_ref();
        let has_residual = self.residual_predicate.is_some();
        if !has_residual && self.global_build_has_null_key {
            return Ok(None);
        }
        if has_residual && self.build_null_key_rows.is_none() {
            return Err(
                "null-aware left anti join with residual requires build null-key rows".to_string(),
            );
        }

        let build_null_rows_by_batch: &[Vec<u32>] = self
            .build_null_key_rows
            .as_ref()
            .map(|rows| rows.as_slice())
            .unwrap_or(&[]);
        if !build_null_rows_by_batch.is_empty()
            && build_null_rows_by_batch.len() != self.build_batches.len()
        {
            return Err(format!(
                "null-aware anti join build null-key rows size mismatch: batches={} null_key_batches={}",
                self.build_batches.len(),
                build_null_rows_by_batch.len()
            ));
        }

        let mut output_batches = Vec::new();
        for probe in probe_chunks {
            let mut probe_key_arrays = Vec::with_capacity(self.probe_keys.len());
            for key in &self.probe_keys {
                probe_key_arrays.push(self.arena.eval(*key, &probe).map_err(|e| e.to_string())?);
            }

            let group_ids = if let Some(table) = table_opt {
                lookup_group_ids(&self.arena, &self.probe_keys, &probe, table)?
            } else {
                vec![None; probe.len()]
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

            if let Some(pred) = self.residual_predicate {
                if let Some(table) = table_opt {
                    let (matched, rows_checked, eval_batches, eval_pairs, group_rows_total) =
                        Self::probe_chunk_matches_residual(
                            &self.arena,
                            self.join_type,
                            self.build_batches.as_ref(),
                            table,
                            &probe,
                            &group_ids,
                            &self.join_scope_schema,
                            pred,
                        )?;
                    self.residual_rows_checked =
                        self.residual_rows_checked.saturating_add(rows_checked);
                    self.residual_eval_batches =
                        self.residual_eval_batches.saturating_add(eval_batches);
                    self.residual_eval_pairs = self.residual_eval_pairs.saturating_add(eval_pairs);
                    self.residual_group_rows_total = self
                        .residual_group_rows_total
                        .saturating_add(group_rows_total);
                    self.residual_matched_rows = self
                        .residual_matched_rows
                        .saturating_add(matched.iter().filter(|v| **v).count() as u64);
                    matched_equal = matched;
                }

                if !build_null_rows_by_batch.is_empty() {
                    matched_null_key = Self::probe_chunk_matches_residual_against_build_rows(
                        &self.arena,
                        &probe,
                        self.build_batches.as_ref(),
                        build_null_rows_by_batch,
                        &self.join_scope_schema,
                        pred,
                        None,
                    )?;
                }

                if probe_null_rows.iter().any(|is_null| *is_null) {
                    let mut all_build_rows_by_batch = Vec::with_capacity(self.build_batches.len());
                    for build_batch in self.build_batches.iter() {
                        let mut rows = Vec::with_capacity(build_batch.len());
                        for row in 0..build_batch.len() {
                            rows.push(row as u32);
                        }
                        all_build_rows_by_batch.push(rows);
                    }
                    matched_any = Self::probe_chunk_matches_residual_against_build_rows(
                        &self.arena,
                        &probe,
                        self.build_batches.as_ref(),
                        &all_build_rows_by_batch,
                        &self.join_scope_schema,
                        pred,
                        Some(&probe_null_rows),
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

            let mask = BooleanArray::from(keep);
            let filtered_batch = filter_record_batch(&probe.batch, &mask)
                .map_err(|e| format!("null-aware anti filter failed: {e}"))?;
            if filtered_batch.num_rows() > 0 {
                output_batches.push(filtered_batch);
            }
        }

        if output_batches.is_empty() {
            return Ok(None);
        }
        let output_rows: usize = output_batches.iter().map(|b| b.num_rows()).sum();
        self.output_rows = self.output_rows.saturating_add(output_rows as u64);
        if output_batches.len() == 1 {
            return Ok(Some(Chunk::new(output_batches.remove(0))));
        }
        let batch = concat_batches(&output_schema, &output_batches).map_err(|e| e.to_string())?;
        Ok(Some(Chunk::new(batch)))
    }

    fn mark_build_matches_for_semi_anti(
        &mut self,
        probe_chunks: Vec<Chunk>,
        collect_output: bool,
    ) -> Result<Option<Vec<Vec<u32>>>, String> {
        let mut output_indices = if collect_output {
            Some(vec![Vec::new(); self.build_batches.len()])
        } else {
            None
        };

        if probe_chunks.is_empty() {
            return Ok(output_indices);
        }
        if self.build_batches.is_empty() {
            return Ok(output_indices);
        }
        let Some(table) = self.build_table.as_ref() else {
            return Ok(output_indices);
        };
        if table.is_empty() {
            return Ok(output_indices);
        }
        let Some(flags) = self.build_matched.as_mut() else {
            return Ok(output_indices);
        };

        let output_schema = Arc::clone(&self.join_scope_schema);
        let has_residual = self.residual_predicate.is_some();

        for probe in probe_chunks {
            if probe.is_empty() {
                continue;
            }
            let group_ids = lookup_group_ids(&self.arena, &self.probe_keys, &probe, table)?;
            if group_ids.is_empty() {
                continue;
            }

            if !has_residual {
                for group_id_opt in group_ids.iter() {
                    let Some(group_id) = group_id_opt else {
                        continue;
                    };
                    let rows = table
                        .group_rows_slice(*group_id)
                        .map_err(|e| e.to_string())?;
                    for &build_row in rows {
                        let (batch_idx, row_idx) =
                            table.row_location(build_row).map_err(|e| e.to_string())?;
                        let slot = batch_idx as usize;
                        if let Some(batch_flags) = flags.get_mut(slot) {
                            let row_idx = row_idx as usize;
                            if row_idx < batch_flags.len() {
                                if !batch_flags[row_idx] {
                                    batch_flags[row_idx] = true;
                                    if let Some(indices) = output_indices.as_mut() {
                                        indices[slot].push(row_idx as u32);
                                    }
                                }
                            }
                        }
                    }
                }
                continue;
            }

            let mut probe_indices_by_batch = vec![Vec::<u32>::new(); self.build_batches.len()];
            let mut build_indices_by_batch = vec![Vec::<u32>::new(); self.build_batches.len()];

            for (probe_row, group_id_opt) in group_ids.iter().enumerate() {
                let Some(group_id) = group_id_opt else {
                    continue;
                };
                let rows = table
                    .group_rows_slice(*group_id)
                    .map_err(|e| e.to_string())?;
                for &build_row in rows {
                    let (batch_idx, row_idx) =
                        table.row_location(build_row).map_err(|e| e.to_string())?;
                    let slot = batch_idx as usize;
                    if slot >= self.build_batches.len() {
                        return Err("join row batch index out of bounds".to_string());
                    }
                    probe_indices_by_batch[slot].push(probe_row as u32);
                    build_indices_by_batch[slot].push(row_idx);
                }
            }

            for (batch_idx, build_batch) in self.build_batches.iter().enumerate() {
                let probe_indices = &probe_indices_by_batch[batch_idx];
                if probe_indices.is_empty() {
                    continue;
                }
                let build_indices = &build_indices_by_batch[batch_idx];
                let candidate = build_join_batch(
                    &probe,
                    build_batch,
                    probe_indices,
                    build_indices,
                    &output_schema,
                )?;
                let Some(candidate) = candidate else {
                    continue;
                };
                if candidate.num_rows() == 0 {
                    continue;
                }

                let mask = if let Some(pred) = self.residual_predicate {
                    let chunk = Chunk::new(candidate.clone());
                    let mask_arr = self.arena.eval(pred, &chunk).map_err(|e| e.to_string())?;
                    mask_arr
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| {
                            "join residual predicate must return boolean array".to_string()
                        })?
                        .clone()
                } else {
                    BooleanArray::from(vec![true; candidate.num_rows()])
                };

                for i in 0..mask.len() {
                    if !mask.is_valid(i) || !mask.value(i) {
                        continue;
                    }
                    let build_row = *build_indices
                        .get(i)
                        .ok_or_else(|| "join build index out of bounds".to_string())?
                        as usize;
                    if let Some(batch_flags) = flags.get_mut(batch_idx) {
                        if build_row < batch_flags.len() {
                            if !batch_flags[build_row] {
                                batch_flags[build_row] = true;
                                if let Some(indices) = output_indices.as_mut() {
                                    indices[batch_idx].push(build_row as u32);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(output_indices)
    }

    fn probe_chunk_matches_residual_against_build_rows(
        arena: &ExprArena,
        probe: &Chunk,
        build_batches: &[Chunk],
        build_rows_by_batch: &[Vec<u32>],
        join_scope_schema: &SchemaRef,
        pred: ExprId,
        probe_row_filter: Option<&[bool]>,
    ) -> Result<Vec<bool>, String> {
        if build_batches.len() != build_rows_by_batch.len() {
            return Err(format!(
                "join residual build rows size mismatch: batches={} row_sets={}",
                build_batches.len(),
                build_rows_by_batch.len()
            ));
        }

        let mut matched = vec![false; probe.len()];
        if probe.is_empty() || build_batches.is_empty() {
            return Ok(matched);
        }

        if let Some(filter) = probe_row_filter {
            if filter.len() != probe.len() {
                return Err(format!(
                    "join residual probe row filter length mismatch: filter={} probe={}",
                    filter.len(),
                    probe.len()
                ));
            }
        }

        let probe_rows = (0..probe.len())
            .filter(|row| probe_row_filter.map(|filter| filter[*row]).unwrap_or(true))
            .map(|row| row as u32)
            .collect::<Vec<_>>();
        if probe_rows.is_empty() {
            return Ok(matched);
        }

        const MAX_EVAL_PAIRS: usize = 16 * 1024;
        for (batch_idx, build_rows) in build_rows_by_batch.iter().enumerate() {
            if build_rows.is_empty() {
                continue;
            }
            let Some(build_batch) = build_batches.get(batch_idx) else {
                continue;
            };

            let mut left_indices = Vec::new();
            let mut right_indices = Vec::new();
            let reserve_cap = probe_rows
                .len()
                .saturating_mul(build_rows.len())
                .min(MAX_EVAL_PAIRS);
            left_indices.reserve(reserve_cap);
            right_indices.reserve(reserve_cap);

            for &probe_row in &probe_rows {
                let probe_slot = probe_row as usize;
                if probe_slot >= matched.len() || matched[probe_slot] {
                    continue;
                }
                for &build_row in build_rows {
                    left_indices.push(probe_row);
                    right_indices.push(build_row);
                    if left_indices.len() == MAX_EVAL_PAIRS {
                        let Some(joined) = build_join_batch(
                            probe,
                            build_batch,
                            &left_indices,
                            &right_indices,
                            join_scope_schema,
                        )?
                        else {
                            left_indices.clear();
                            right_indices.clear();
                            continue;
                        };

                        let joined_chunk = Chunk::new(joined);
                        let mask_arr =
                            arena.eval(pred, &joined_chunk).map_err(|e| e.to_string())?;
                        let mask = mask_arr
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .ok_or_else(|| {
                                "join residual predicate must return boolean array".to_string()
                            })?;
                        for i in 0..mask.len() {
                            if !mask.is_valid(i) || !mask.value(i) {
                                continue;
                            }
                            let probe_idx = *left_indices
                                .get(i)
                                .ok_or_else(|| "join probe index out of bounds".to_string())?
                                as usize;
                            if probe_idx < matched.len() {
                                matched[probe_idx] = true;
                            }
                        }
                        left_indices.clear();
                        right_indices.clear();
                    }
                }
            }

            if left_indices.is_empty() {
                continue;
            }
            let Some(joined) = build_join_batch(
                probe,
                build_batch,
                &left_indices,
                &right_indices,
                join_scope_schema,
            )?
            else {
                continue;
            };
            let joined_chunk = Chunk::new(joined);
            let mask_arr = arena.eval(pred, &joined_chunk).map_err(|e| e.to_string())?;
            let mask = mask_arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "join residual predicate must return boolean array".to_string())?;
            for i in 0..mask.len() {
                if !mask.is_valid(i) || !mask.value(i) {
                    continue;
                }
                let probe_idx = *left_indices
                    .get(i)
                    .ok_or_else(|| "join probe index out of bounds".to_string())?
                    as usize;
                if probe_idx < matched.len() {
                    matched[probe_idx] = true;
                }
            }
        }

        Ok(matched)
    }

    fn probe_chunk_matches_residual(
        arena: &ExprArena,
        join_type: JoinType,
        build_batches: &[Chunk],
        table: &JoinHashTable,
        probe: &Chunk,
        group_ids: &[Option<usize>],
        join_scope_schema: &SchemaRef,
        pred: ExprId,
    ) -> Result<(Vec<bool>, u64, u64, u64, u64), String> {
        if group_ids.len() != probe.len() {
            return Err("join group id vector length mismatch".to_string());
        }
        if build_batches.is_empty() {
            return Ok((vec![false; probe.len()], 0, 0, 0, 0));
        }

        let mut matched = vec![false; probe.len()];
        let mut rows_checked = 0u64;
        let mut group_rows_total = 0u64;

        let mut pairs_by_batch = vec![Vec::<(u32, u32)>::new(); build_batches.len()];
        for (probe_row, group_id_opt) in group_ids.iter().enumerate() {
            let Some(group_id) = group_id_opt else {
                continue;
            };
            rows_checked = rows_checked.saturating_add(1);
            let rows = table
                .group_rows_slice(*group_id)
                .map_err(|e| e.to_string())?;
            group_rows_total = group_rows_total.saturating_add(rows.len() as u64);
            for &build_row_id in rows {
                let (batch_idx, row_idx) = table
                    .row_location(build_row_id)
                    .map_err(|e| e.to_string())?;
                let slot = batch_idx as usize;
                if slot >= pairs_by_batch.len() {
                    return Err("join row batch index out of bounds".to_string());
                }
                pairs_by_batch[slot].push((probe_row as u32, row_idx));
            }
        }

        if rows_checked == 0 {
            return Ok((matched, 0, 0, 0, group_rows_total));
        }

        const MAX_EVAL_PAIRS: usize = 16 * 1024;
        let mut eval_batches = 0u64;
        let mut eval_pairs = 0u64;

        let is_left = matches!(
            join_type,
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::NullAwareLeftAnti
        );

        for (batch_idx, pairs) in pairs_by_batch.into_iter().enumerate() {
            if pairs.is_empty() {
                continue;
            }
            let Some(build_batch) = build_batches.get(batch_idx) else {
                continue;
            };

            let mut offset = 0usize;
            while offset < pairs.len() {
                let end = (offset + MAX_EVAL_PAIRS).min(pairs.len());
                let slice = &pairs[offset..end];
                offset = end;

                let mut left_indices = Vec::with_capacity(slice.len());
                let mut right_indices = Vec::with_capacity(slice.len());
                for &(probe_row, build_row) in slice {
                    let probe_slot = probe_row as usize;
                    if probe_slot >= matched.len() {
                        return Err("join probe index out of bounds".to_string());
                    }
                    if matched[probe_slot] {
                        continue;
                    }
                    if is_left {
                        left_indices.push(probe_row);
                        right_indices.push(build_row);
                    } else {
                        left_indices.push(build_row);
                        right_indices.push(probe_row);
                    }
                }

                if left_indices.is_empty() {
                    continue;
                }

                let joined = if is_left {
                    build_join_batch(
                        probe,
                        build_batch,
                        &left_indices,
                        &right_indices,
                        join_scope_schema,
                    )?
                } else {
                    build_join_batch(
                        build_batch,
                        probe,
                        &left_indices,
                        &right_indices,
                        join_scope_schema,
                    )?
                };

                let Some(joined) = joined else {
                    continue;
                };

                let joined_chunk = Chunk::new(joined);
                let mask_arr = arena.eval(pred, &joined_chunk).map_err(|e| e.to_string())?;
                eval_batches = eval_batches.saturating_add(1);
                eval_pairs = eval_pairs.saturating_add(joined_chunk.len() as u64);
                let mask = mask_arr
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        "join residual predicate must return boolean array".to_string()
                    })?;
                for i in 0..mask.len() {
                    if !(mask.is_valid(i) && mask.value(i)) {
                        continue;
                    }
                    let probe_row = if is_left {
                        *left_indices
                            .get(i)
                            .ok_or_else(|| "join probe index out of bounds".to_string())?
                    } else {
                        *right_indices
                            .get(i)
                            .ok_or_else(|| "join probe index out of bounds".to_string())?
                    };
                    let probe_row = probe_row as usize;
                    if probe_row < matched.len() {
                        matched[probe_row] = true;
                    }
                }
            }
        }

        Ok((
            matched,
            rows_checked,
            eval_batches,
            eval_pairs,
            group_rows_total,
        ))
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
            {
                if !filtered.is_empty() {
                    out.push(filtered);
                }
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
