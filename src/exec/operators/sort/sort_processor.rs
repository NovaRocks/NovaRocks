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
//! Sort processor for ORDER BY execution.
//!
//! Responsibilities:
//! - Accumulates, orders, and emits rows according to sort key specification.
//! - Supports final materialization of sorted output chunks for downstream consumers.
//!
//! Key exported interfaces:
//! - Types: `SortProcessorFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::Arc;
use std::time::Instant;

use crate::exec::chunk::Chunk;
use crate::exec::expr::ExprArena;
use crate::exec::node::sort::{SortExpression, SortTopNType};
use crate::exec::operators::sort::normalize_sort_key_array;
use crate::exec::operators::sort::{
    ChunksSorter, ChunksSorterFullSort, ChunksSorterHeapSort, ChunksSorterTopN,
    SpillableChunksSorter,
};
use crate::exec::spill::spiller::{SpillFile, Spiller};
use crate::exec::spill::{SpillConfig, SpillMode, SpillProfile};

use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::novarocks_logging::warn;
use crate::runtime::runtime_state::RuntimeState;

use arrow::array::ArrayRef;
use arrow::compute::{SortColumn, SortOptions, concat_batches, lexsort_to_indices, take};
use arrow::datatypes::SchemaRef;
use arrow::row::{RowConverter, SortField};

/// Factory for sort processors that materialize ORDER BY output chunks.
pub struct SortProcessorFactory {
    name: String,
    arena: Arc<ExprArena>,
    order_by: Vec<SortExpression>,
    use_top_n: bool,
    limit: Option<usize>,
    offset: usize,
    topn_type: SortTopNType,
    max_buffered_rows: Option<usize>,
    max_buffered_bytes: Option<usize>,
}

impl SortProcessorFactory {
    pub fn new(
        node_id: i32,
        arena: Arc<ExprArena>,
        order_by: Vec<SortExpression>,
        limit: Option<usize>,
        offset: usize,
        topn_type: SortTopNType,
        max_buffered_rows: Option<usize>,
        max_buffered_bytes: Option<usize>,
    ) -> Self {
        Self::new_internal(
            node_id,
            arena,
            order_by,
            false,
            limit,
            offset,
            topn_type,
            max_buffered_rows,
            max_buffered_bytes,
        )
    }

    pub fn new_topn(
        node_id: i32,
        arena: Arc<ExprArena>,
        order_by: Vec<SortExpression>,
        limit: Option<usize>,
        offset: usize,
        topn_type: SortTopNType,
        max_buffered_rows: Option<usize>,
        max_buffered_bytes: Option<usize>,
    ) -> Self {
        Self::new_internal(
            node_id,
            arena,
            order_by,
            true,
            limit,
            offset,
            topn_type,
            max_buffered_rows,
            max_buffered_bytes,
        )
    }

    fn new_internal(
        node_id: i32,
        arena: Arc<ExprArena>,
        order_by: Vec<SortExpression>,
        use_top_n: bool,
        limit: Option<usize>,
        offset: usize,
        topn_type: SortTopNType,
        max_buffered_rows: Option<usize>,
        max_buffered_bytes: Option<usize>,
    ) -> Self {
        let name = if node_id >= 0 {
            if use_top_n {
                format!("TOP_N (id={node_id})")
            } else {
                format!("SORT (id={node_id})")
            }
        } else if use_top_n {
            "TOP_N".to_string()
        } else {
            "SORT".to_string()
        };
        Self {
            name,
            arena,
            order_by,
            use_top_n,
            limit,
            offset,
            topn_type,
            max_buffered_rows,
            max_buffered_bytes,
        }
    }
}

impl OperatorFactory for SortProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(SortProcessorOperator {
            name: self.name.clone(),
            arena: Arc::clone(&self.arena),
            order_by: self.order_by.clone(),
            use_top_n: self.use_top_n,
            limit: self.limit,
            offset: self.offset,
            topn_type: self.topn_type,
            max_buffered_rows: self.max_buffered_rows,
            max_buffered_bytes: self.max_buffered_bytes,
            buffered: Vec::new(),
            buffered_bytes: 0,
            buffered_rows: 0,
            spill_state: None,
            pending_output: None,
            finishing: false,
            finished: false,
            profile_initialized: false,
            profiles: None,
        })
    }
}

struct SortProcessorOperator {
    name: String,
    arena: Arc<ExprArena>,
    order_by: Vec<SortExpression>,
    use_top_n: bool,
    limit: Option<usize>,
    offset: usize,
    topn_type: SortTopNType,
    max_buffered_rows: Option<usize>,
    max_buffered_bytes: Option<usize>,
    buffered: Vec<Chunk>,
    buffered_bytes: i64,
    buffered_rows: i64,
    spill_state: Option<SortSpillState>,
    pending_output: Option<Chunk>,
    finishing: bool,
    finished: bool,
    profile_initialized: bool,
    profiles: Option<crate::runtime::profile::OperatorProfiles>,
}

#[derive(Debug)]
struct SpillRun {
    schema: SchemaRef,
    file: SpillFile,
}

struct SortSpillState {
    config: SpillConfig,
    spiller: Spiller,
    profile: Option<SpillProfile>,
    runs: Vec<SpillRun>,
}

impl Operator for SortProcessorOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn set_profiles(&mut self, profiles: crate::runtime::profile::OperatorProfiles) {
        self.profiles = Some(profiles);
    }

    fn is_finished(&self) -> bool {
        self.finished
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
}

impl ProcessorOperator for SortProcessorOperator {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished && self.pending_output.is_none()
    }

    fn has_output(&self) -> bool {
        self.pending_output.is_some()
    }

    fn push_chunk(&mut self, state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        self.init_profile_if_needed();
        if self.pending_output.is_some() {
            return Err("sort received input while output buffer is full".to_string());
        }
        if !chunk.is_empty() {
            let chunk_bytes = i64::try_from(chunk.estimated_bytes()).unwrap_or(i64::MAX);
            let chunk_rows = i64::try_from(chunk.len()).unwrap_or(i64::MAX);
            self.buffered_bytes = self.buffered_bytes.saturating_add(chunk_bytes);
            self.buffered_rows = self.buffered_rows.saturating_add(chunk_rows);
            self.buffered.push(chunk);
        }
        self.maybe_prune_topn_buffered()?;
        self.maybe_init_spill_state(state)?;
        self.maybe_spill_buffered()?;
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        let out = self.pending_output.take();
        if self.finishing && self.pending_output.is_none() {
            self.finished = true;
        }
        Ok(out)
    }

    fn set_finishing(&mut self, state: &RuntimeState) -> Result<(), String> {
        if self.finishing || self.finished {
            return Ok(());
        }
        self.init_profile_if_needed();
        self.finishing = true;

        self.maybe_init_spill_state(state)?;
        let out = self.build_final_output_with_spill()?;
        self.pending_output = out;
        if self.pending_output.is_none() {
            self.finished = true;
        }
        Ok(())
    }
}

impl SortProcessorOperator {
    const HEAP_SORTER_LIMIT: usize = 1024;
    const TOPN_PRUNE_FACTOR: usize = 4;
    const TOPN_PRUNE_MIN_ROWS: usize = 4096;
    const TOPN_PRUNE_MIN_CHUNKS: usize = 8;

    fn init_profile_if_needed(&mut self) {
        if self.profile_initialized {
            return;
        }
        self.profile_initialized = true;
        let sort_type = if self.use_top_n { "TOPN" } else { "FULL_SORT" };
        let sort_keys = self.order_by.len();
        if let Some(profile) = self.profiles.as_ref() {
            profile.common.add_info_string("SortType", sort_type);
            profile
                .common
                .add_info_string("SortKeys", format!("{sort_keys}"));
            profile.common.add_info_string(
                "Limit",
                self.limit
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "-1".to_string()),
            );
            profile
                .common
                .add_info_string("Offset", format!("{}", self.offset));
            profile.common.add_info_string(
                "TopNType",
                match self.topn_type {
                    SortTopNType::RowNumber => "ROW_NUMBER",
                    SortTopNType::Rank => "RANK",
                    SortTopNType::DenseRank => "DENSE_RANK",
                },
            );
        }
    }

    fn maybe_init_spill_state(&mut self, state: &RuntimeState) -> Result<(), String> {
        if self.spill_state.is_some() {
            return Ok(());
        }
        let Some(config) = state.spill_config().cloned() else {
            return Ok(());
        };
        if !config.enable_spill || config.spill_mode == SpillMode::None {
            return Ok(());
        }
        let manager = state
            .spill_manager()
            .ok_or_else(|| "spill manager is missing".to_string())?;
        let spiller = Spiller::new_from_config(config.spill_encode_level)?;
        self.spill_state = Some(SortSpillState {
            config,
            spiller,
            profile: manager.profile(),
            runs: Vec::new(),
        });
        Ok(())
    }

    fn rows_to_keep_for_topn(&self) -> Option<usize> {
        if !self.use_top_n || self.topn_type != SortTopNType::RowNumber {
            return None;
        }
        self.limit.map(|limit| self.offset.saturating_add(limit))
    }

    fn rank_like_limit_for_topn(&self) -> Option<usize> {
        if !self.use_top_n || self.topn_type == SortTopNType::RowNumber {
            return None;
        }
        self.limit
    }

    fn is_rank_like_topn(&self) -> bool {
        self.use_top_n && self.topn_type != SortTopNType::RowNumber
    }

    fn topn_cutoff_base(&self) -> Option<usize> {
        self.rows_to_keep_for_topn()
            .or_else(|| self.rank_like_limit_for_topn())
    }

    fn build_topn_sorter(&self) -> Option<SpillableChunksSorter> {
        let sorter: Box<dyn ChunksSorter> = if let Some(rows_to_keep) = self.rows_to_keep_for_topn()
        {
            if rows_to_keep <= Self::HEAP_SORTER_LIMIT {
                Box::new(ChunksSorterHeapSort::new(
                    Arc::clone(&self.arena),
                    self.order_by.clone(),
                    rows_to_keep,
                ))
            } else {
                Box::new(ChunksSorterTopN::new(
                    Arc::clone(&self.arena),
                    self.order_by.clone(),
                    SortTopNType::RowNumber,
                    rows_to_keep,
                ))
            }
        } else if let Some(rank_limit) = self.rank_like_limit_for_topn() {
            Box::new(ChunksSorterTopN::new(
                Arc::clone(&self.arena),
                self.order_by.clone(),
                self.topn_type,
                rank_limit,
            ))
        } else {
            return None;
        };
        Some(SpillableChunksSorter::new(sorter))
    }

    fn sort_chunks_for_topn_mode(&self, chunks: &[Chunk]) -> Result<Option<Chunk>, String> {
        let Some(sorter) = self.build_topn_sorter() else {
            return Ok(None);
        };
        sorter.sort_chunks(chunks)
    }

    fn topn_prune_row_threshold(&self, rows_to_keep: usize) -> usize {
        if let Some(max_rows) = self.max_buffered_rows {
            return max_rows.max(rows_to_keep.saturating_add(1));
        }
        rows_to_keep
            .saturating_mul(Self::TOPN_PRUNE_FACTOR)
            .max(Self::TOPN_PRUNE_MIN_ROWS)
    }

    fn recompute_buffered_stats(&mut self) {
        let mut bytes = 0i64;
        let mut rows = 0i64;
        for chunk in &self.buffered {
            bytes =
                bytes.saturating_add(i64::try_from(chunk.estimated_bytes()).unwrap_or(i64::MAX));
            rows = rows.saturating_add(i64::try_from(chunk.len()).unwrap_or(i64::MAX));
        }
        self.buffered_bytes = bytes;
        self.buffered_rows = rows;
    }

    fn set_buffered_to_single_chunk(&mut self, chunk: Chunk) {
        self.buffered.clear();
        if !chunk.is_empty() {
            self.buffered.push(chunk);
        }
        self.recompute_buffered_stats();
    }

    fn truncate_buffered_prefix(&mut self, rows_to_keep: usize) {
        if rows_to_keep == 0 {
            self.buffered.clear();
            self.buffered_bytes = 0;
            self.buffered_rows = 0;
            return;
        }
        let mut remain = rows_to_keep;
        let mut kept = Vec::new();
        for chunk in &self.buffered {
            if remain == 0 {
                break;
            }
            if chunk.len() <= remain {
                kept.push(chunk.clone());
                remain -= chunk.len();
            } else {
                kept.push(chunk.slice(0, remain));
                remain = 0;
            }
        }
        self.buffered = kept;
        self.recompute_buffered_stats();
    }

    fn maybe_prune_topn_buffered(&mut self) -> Result<(), String> {
        let Some(cutoff_base) = self.topn_cutoff_base() else {
            return Ok(());
        };
        if self.buffered.is_empty() {
            return Ok(());
        }
        if cutoff_base == 0 {
            self.buffered.clear();
            self.buffered_bytes = 0;
            self.buffered_rows = 0;
            return Ok(());
        }
        let buffered_rows = usize::try_from(self.buffered_rows.max(0)).unwrap_or(usize::MAX);
        if buffered_rows <= cutoff_base {
            return Ok(());
        }

        let row_threshold = self.topn_prune_row_threshold(cutoff_base);
        let bytes_threshold = self.max_buffered_bytes;
        let rows_exceed = buffered_rows >= row_threshold;
        let bytes_exceed = bytes_threshold
            .map(|max_bytes| {
                usize::try_from(self.buffered_bytes.max(0)).unwrap_or(usize::MAX) >= max_bytes
            })
            .unwrap_or(false);
        if !rows_exceed && !bytes_exceed && self.buffered.len() < Self::TOPN_PRUNE_MIN_CHUNKS {
            return Ok(());
        }

        if self.order_by.is_empty() {
            if self.topn_type == SortTopNType::RowNumber {
                self.truncate_buffered_prefix(cutoff_base);
            }
            return Ok(());
        }

        if let Some(topn_chunk) = self.sort_chunks_for_topn_mode(&self.buffered)? {
            self.set_buffered_to_single_chunk(topn_chunk);
        } else {
            self.buffered.clear();
            self.buffered_bytes = 0;
            self.buffered_rows = 0;
        }
        Ok(())
    }

    fn maybe_spill_buffered(&mut self) -> Result<(), String> {
        let should_spill = match self.spill_state.as_ref() {
            Some(spill) => self.should_spill_buffered(spill),
            None => false,
        };
        if !should_spill {
            return Ok(());
        }
        let mut spill = self.spill_state.take().expect("spill state checked");
        let result = self.spill_buffered_run(&mut spill);
        self.spill_state = Some(spill);
        result
    }

    fn should_spill_buffered(&self, spill: &SortSpillState) -> bool {
        if self.buffered.is_empty() {
            return false;
        }
        if spill.config.spill_mode == SpillMode::None {
            return false;
        }
        if let Some(max_rows) = self.max_buffered_rows {
            let buffered_rows = usize::try_from(self.buffered_rows.max(0)).unwrap_or(usize::MAX);
            if buffered_rows >= max_rows {
                return true;
            }
        }
        if let Some(max_bytes) = self.max_buffered_bytes {
            let buffered_bytes = usize::try_from(self.buffered_bytes.max(0)).unwrap_or(usize::MAX);
            if buffered_bytes >= max_bytes {
                return true;
            }
        }
        let min_bytes = spill.config.spill_operator_min_bytes.unwrap_or(0);
        if self.buffered_bytes < min_bytes {
            return false;
        }
        if spill.config.spill_mode == SpillMode::Force {
            return true;
        }
        if let Some(max_bytes) = spill
            .config
            .spill_operator_max_bytes
            .or_else(|| spill_mem_table_bytes(spill))
        {
            return self.buffered_bytes >= max_bytes;
        }
        false
    }

    fn spill_buffered_run(&mut self, spill: &mut SortSpillState) -> Result<(), String> {
        if self.buffered.is_empty() {
            return Ok(());
        }
        let start = Instant::now();
        let run = self.build_sorted_run(&self.buffered)?;
        let schema = run.schema();
        let run_rows = self.buffered_rows.max(0);
        let run_bytes = self.buffered_bytes.max(0);
        let spill_file = spill.spiller.spill_chunks(schema.clone(), &[run])?;
        spill.runs.push(SpillRun {
            schema,
            file: spill_file,
        });
        if let Some(profile) = spill.profile.as_ref() {
            let elapsed_ns = start.elapsed().as_nanos();
            let elapsed_ns = i64::try_from(elapsed_ns).unwrap_or(i64::MAX);
            profile.spill_time.add(elapsed_ns);
            profile
                .spill_rows
                .add(i64::try_from(run_rows).unwrap_or(i64::MAX));
            profile
                .spill_bytes
                .add(i64::try_from(run_bytes).unwrap_or(i64::MAX));
        }
        self.buffered.clear();
        self.buffered_bytes = 0;
        self.buffered_rows = 0;
        Ok(())
    }

    fn build_final_output_with_spill(&mut self) -> Result<Option<Chunk>, String> {
        let Some(mut spill) = self.spill_state.take() else {
            let out = self.build_final_output_from_chunks(&self.buffered)?;
            self.clear_buffered();
            return Ok(out);
        };

        let use_spill = !spill.runs.is_empty() || spill.config.spill_mode == SpillMode::Force;
        if use_spill && !self.buffered.is_empty() {
            self.spill_buffered_run(&mut spill)?;
        }

        if !use_spill {
            let out = self.build_final_output_from_chunks(&self.buffered)?;
            self.clear_buffered();
            return Ok(out);
        }

        if self.topn_cutoff_base().is_some() {
            let out = self.build_final_topn_output_with_spill(&mut spill)?;
            self.clear_buffered();
            return Ok(out);
        }

        let mut chunks = self.restore_spill_runs(&mut spill)?;
        if !self.buffered.is_empty() {
            let tail = self.build_sorted_run(&self.buffered)?;
            chunks.push(tail);
            self.clear_buffered();
        }
        self.build_final_output_from_chunks(&chunks)
    }

    fn clear_buffered(&mut self) {
        self.buffered.clear();
        self.buffered_bytes = 0;
        self.buffered_rows = 0;
    }

    fn build_final_topn_output_with_spill(
        &mut self,
        spill: &mut SortSpillState,
    ) -> Result<Option<Chunk>, String> {
        let cutoff_base = self.topn_cutoff_base().unwrap_or(0);
        if cutoff_base == 0 {
            return Ok(None);
        }

        let sorter = self
            .build_topn_sorter()
            .ok_or_else(|| "topn sorter is missing for topn mode".to_string())?;

        let mut candidate: Option<Chunk> = None;
        let runs = std::mem::take(&mut spill.runs);
        for run in runs {
            candidate = self.merge_spill_run_into_topn_candidate(
                &sorter,
                spill,
                run,
                cutoff_base,
                candidate,
            )?;
        }

        if !self.buffered.is_empty() {
            let mut inputs = Vec::with_capacity(self.buffered.len().saturating_add(1));
            if let Some(prev) = candidate.take() {
                inputs.push(prev);
            }
            inputs.extend(self.buffered.clone());
            candidate = sorter.sort_chunks(&inputs)?;
        }

        let Some(topn_chunk) = candidate else {
            return Ok(None);
        };
        if self.topn_type != SortTopNType::RowNumber {
            return Ok(Some(topn_chunk));
        }
        let len = topn_chunk.len();
        if self.offset >= len {
            return Ok(None);
        }
        let take_len = self.limit.unwrap_or(0).min(len - self.offset);
        if take_len == 0 {
            return Ok(None);
        }
        Ok(Some(topn_chunk.slice(self.offset, take_len)))
    }

    fn merge_spill_run_into_topn_candidate(
        &self,
        sorter: &SpillableChunksSorter,
        spill: &mut SortSpillState,
        run: SpillRun,
        _cutoff_base: usize,
        mut candidate: Option<Chunk>,
    ) -> Result<Option<Chunk>, String> {
        let start = Instant::now();
        let mut restore_rows = 0i64;
        let mut restore_bytes = 0i64;

        let mut stream = spill.spiller.open_stream(run.schema.clone(), &run.file)?;
        while let Some(batch) = stream.next_batch()? {
            let chunk = Chunk::try_new(batch).map_err(|e| e.to_string())?;
            restore_rows = restore_rows.saturating_add(chunk.len() as i64);
            restore_bytes = restore_bytes
                .saturating_add(i64::try_from(chunk.estimated_bytes()).unwrap_or(i64::MAX));

            candidate = sorter.merge_candidate(candidate.take(), chunk)?;
        }

        if let Err(err) = std::fs::remove_file(&run.file.path) {
            warn!(
                "Sort spill remove file failed: path={} error={}",
                run.file.path.display(),
                err
            );
        }
        if let Some(profile) = spill.profile.as_ref() {
            let elapsed_ns = start.elapsed().as_nanos();
            let elapsed_ns = i64::try_from(elapsed_ns).unwrap_or(i64::MAX);
            profile.restore_time.add(elapsed_ns);
            profile.restore_rows.add(restore_rows);
            profile.restore_bytes.add(restore_bytes);
            profile.spill_read_io_count.add(1);
        }
        Ok(candidate)
    }

    fn restore_spill_runs(&self, spill: &mut SortSpillState) -> Result<Vec<Chunk>, String> {
        let mut out = Vec::new();
        let runs = std::mem::take(&mut spill.runs);
        for run in runs {
            let restored = self.restore_single_spill_run(spill, run)?;
            out.extend(restored);
        }
        Ok(out)
    }

    fn restore_single_spill_run(
        &self,
        spill: &mut SortSpillState,
        run: SpillRun,
    ) -> Result<Vec<Chunk>, String> {
        let start = Instant::now();
        let mut restore_rows = 0i64;
        let mut restore_bytes = 0i64;
        let mut out = Vec::new();

        let mut stream = spill.spiller.open_stream(run.schema.clone(), &run.file)?;
        while let Some(batch) = stream.next_batch()? {
            let chunk = Chunk::try_new(batch).map_err(|e| e.to_string())?;
            restore_rows = restore_rows.saturating_add(chunk.len() as i64);
            restore_bytes = restore_bytes
                .saturating_add(i64::try_from(chunk.estimated_bytes()).unwrap_or(i64::MAX));
            out.push(chunk);
        }

        if let Err(err) = std::fs::remove_file(&run.file.path) {
            warn!(
                "Sort spill remove file failed: path={} error={}",
                run.file.path.display(),
                err
            );
        }
        if let Some(profile) = spill.profile.as_ref() {
            let elapsed_ns = start.elapsed().as_nanos();
            let elapsed_ns = i64::try_from(elapsed_ns).unwrap_or(i64::MAX);
            profile.restore_time.add(elapsed_ns);
            profile.restore_rows.add(restore_rows);
            profile.restore_bytes.add(restore_bytes);
            profile.spill_read_io_count.add(1);
        }
        Ok(out)
    }

    fn build_sorted_run(&self, chunks: &[Chunk]) -> Result<Chunk, String> {
        if let Some(cutoff_base) = self.topn_cutoff_base() {
            if cutoff_base == 0 {
                let empty = arrow::record_batch::RecordBatch::new_empty(chunks[0].schema());
                return Chunk::try_new(empty).map_err(|e| e.to_string());
            }
            return self
                .sort_chunks_for_topn_mode(chunks)?
                .ok_or_else(|| "topn sorted run expected non-empty output".to_string());
        }
        ChunksSorterFullSort::new(Arc::clone(&self.arena), self.order_by.clone())
            .sort_chunks(chunks)?
            .ok_or_else(|| "full sort run expected non-empty output".to_string())
    }

    fn build_final_output_from_chunks(&self, chunks: &[Chunk]) -> Result<Option<Chunk>, String> {
        if chunks.is_empty() {
            return Ok(None);
        }
        if let Some(rank_limit) = self.rank_like_limit_for_topn() {
            if rank_limit == 0 {
                return Ok(None);
            }
            return self.sort_chunks_for_topn_mode(chunks);
        }
        if self.is_rank_like_topn() {
            return self.build_rank_like_topn_output_from_chunks(chunks);
        }
        if let Some(rows_to_keep) = self.rows_to_keep_for_topn() {
            if rows_to_keep == 0 {
                return Ok(None);
            }
            let Some(topn_chunk) = self.sort_chunks_for_topn_mode(chunks)? else {
                return Ok(None);
            };
            let len = topn_chunk.len();
            if self.offset >= len {
                return Ok(None);
            }
            let take_len = self.limit.unwrap_or(0).min(len - self.offset);
            if take_len == 0 {
                return Ok(None);
            }
            return Ok(Some(topn_chunk.slice(self.offset, take_len)));
        }

        let batch = self.concat_batches_for_chunks(chunks)?;
        if self.order_by.is_empty() {
            let len = batch.num_rows();
            if self.offset >= len {
                return Ok(None);
            }
            let take_len = if let Some(l) = self.limit {
                l.min(len - self.offset)
            } else {
                len - self.offset
            };
            let sliced = batch.slice(self.offset, take_len);
            return Ok(Some(Chunk::new(sliced)));
        }
        let indices = self.sort_indices(&batch)?;
        let len = indices.len();
        if self.offset >= len {
            return Ok(None);
        }
        let take_len = if let Some(l) = self.limit {
            l.min(len - self.offset)
        } else {
            len - self.offset
        };
        let sliced_indices = indices.slice(self.offset, take_len);
        let sorted_batch = self.take_batch(&batch, sliced_indices.as_ref())?;
        Ok(Some(Chunk::new(sorted_batch)))
    }

    /// Build final output for `RANK`/`DENSE_RANK` topn semantics.
    ///
    /// The output boundary is rank-based instead of row-count-based:
    /// - `RANK`: include all rows whose `RANK()` <= `limit`
    /// - `DENSE_RANK`: include all rows whose `DENSE_RANK()` <= `limit`
    ///
    /// Rows tied on ORDER BY keys at the boundary are included together.
    fn build_rank_like_topn_output_from_chunks(
        &self,
        chunks: &[Chunk],
    ) -> Result<Option<Chunk>, String> {
        if self.offset != 0 {
            return Err(format!(
                "topn_type {:?} requires offset=0, got {}",
                self.topn_type, self.offset
            ));
        }
        let rank_limit = self.limit.unwrap_or(0);
        if rank_limit == 0 {
            return Ok(None);
        }

        let batch = self.concat_batches_for_chunks(chunks)?;
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        if self.order_by.is_empty() {
            // Without order-by keys all rows belong to one peer group (rank = 1).
            return Ok(Some(Chunk::new(batch)));
        }

        let indices = self.sort_indices(&batch)?;
        let sorted_batch = self.take_batch(&batch, indices.as_ref())?;
        let key_columns = self.eval_order_by_columns(&sorted_batch)?;
        let key_rows = self.convert_sort_keys_to_rows(&key_columns)?;
        let cutoff = rank_like_cutoff(
            self.topn_type,
            rank_limit,
            sorted_batch.num_rows(),
            |lhs, rhs| key_rows.row(lhs) == key_rows.row(rhs),
        );
        if cutoff == 0 {
            return Ok(None);
        }
        Ok(Some(Chunk::new(sorted_batch.slice(0, cutoff))))
    }

    fn concat_batches_for_chunks(
        &self,
        chunks: &[Chunk],
    ) -> Result<arrow::record_batch::RecordBatch, String> {
        let schema = chunks[0].schema();
        let batches: Vec<_> = chunks.iter().map(|c| c.batch.clone()).collect();
        concat_batches(&schema, &batches).map_err(|e| e.to_string())
    }

    fn sort_indices(
        &self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> Result<arrow::array::ArrayRef, String> {
        let key_columns = self.eval_order_by_columns(batch)?;
        let sort_columns = self.build_sort_columns(&key_columns);
        let indices = lexsort_to_indices(&sort_columns, None).map_err(|e| e.to_string())?;
        Ok(std::sync::Arc::new(indices))
    }

    fn eval_order_by_columns(
        &self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> Result<Vec<ArrayRef>, String> {
        let chunk = Chunk::new(batch.clone());
        let mut columns = Vec::with_capacity(self.order_by.len());
        for sort_expr in &self.order_by {
            let values = self
                .arena
                .eval(sort_expr.expr, &chunk)
                .map_err(|e| e.to_string())?;
            columns.push(normalize_sort_key_array(&values)?);
        }
        Ok(columns)
    }

    fn build_sort_columns(&self, key_columns: &[ArrayRef]) -> Vec<SortColumn> {
        key_columns
            .iter()
            .zip(self.order_by.iter())
            .map(|(values, sort_expr)| SortColumn {
                values: values.clone(),
                options: Some(SortOptions {
                    descending: !sort_expr.asc,
                    nulls_first: sort_expr.nulls_first,
                }),
            })
            .collect()
    }

    fn convert_sort_keys_to_rows(
        &self,
        key_columns: &[ArrayRef],
    ) -> Result<arrow::row::Rows, String> {
        let fields = key_columns
            .iter()
            .zip(self.order_by.iter())
            .map(|(col, expr)| {
                SortField::new_with_options(
                    col.data_type().clone(),
                    SortOptions {
                        descending: !expr.asc,
                        nulls_first: expr.nulls_first,
                    },
                )
            })
            .collect::<Vec<_>>();
        let converter = RowConverter::new(fields).map_err(|e| e.to_string())?;
        converter
            .convert_columns(key_columns)
            .map_err(|e| e.to_string())
    }

    fn take_batch(
        &self,
        batch: &arrow::record_batch::RecordBatch,
        indices: &dyn arrow::array::Array,
    ) -> Result<arrow::record_batch::RecordBatch, String> {
        let columns = batch
            .columns()
            .iter()
            .map(|c| take(c.as_ref(), indices, None))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        arrow::array::RecordBatch::try_new(batch.schema(), columns).map_err(|e| e.to_string())
    }
}

/// Compute output row count for rank-based topn.
///
/// `peer_equal(i, j)` returns whether sorted row `i` and `j` belong to the same
/// ORDER BY peer group.
fn rank_like_cutoff<F>(
    topn_type: SortTopNType,
    rank_limit: usize,
    total_rows: usize,
    mut peer_equal: F,
) -> usize
where
    F: FnMut(usize, usize) -> bool,
{
    if rank_limit == 0 || total_rows == 0 {
        return 0;
    }
    let mut rank = 1usize;
    let mut dense_rank = 1usize;
    let mut cutoff = 0usize;
    for idx in 0..total_rows {
        if idx > 0 && !peer_equal(idx - 1, idx) {
            rank = idx + 1;
            dense_rank = dense_rank.saturating_add(1);
        }
        let keep = match topn_type {
            SortTopNType::RowNumber => idx < rank_limit,
            SortTopNType::Rank => rank <= rank_limit,
            SortTopNType::DenseRank => dense_rank <= rank_limit,
        };
        if !keep {
            break;
        }
        cutoff = idx + 1;
    }
    cutoff
}

fn spill_mem_table_bytes(spill: &SortSpillState) -> Option<i64> {
    let size = spill.config.spill_mem_table_size?;
    let num = spill.config.spill_mem_table_num?;
    let size = i64::from(size);
    let num = i64::from(num);
    Some(size.saturating_mul(num))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rank_like_cutoff_row_number_is_fixed_row_count() {
        // Peer groups: [0,1], [2], [3,4]
        let groups = [0usize, 0, 1, 2, 2];
        let cutoff = rank_like_cutoff(SortTopNType::RowNumber, 3, groups.len(), |l, r| {
            groups[l] == groups[r]
        });
        assert_eq!(cutoff, 3);
    }

    #[test]
    fn rank_like_cutoff_rank_expands_boundary_ties() {
        // ORDER BY values: [10,10,9,8,8]
        // RANK values:      [1, 1,3,4,4]
        let groups = [0usize, 0, 1, 2, 2];
        let cutoff = rank_like_cutoff(SortTopNType::Rank, 4, groups.len(), |l, r| {
            groups[l] == groups[r]
        });
        assert_eq!(cutoff, 5);
    }

    #[test]
    fn rank_like_cutoff_dense_rank_uses_distinct_peer_groups() {
        // ORDER BY values: [10,10,9,8,8]
        // DENSE_RANK:      [1, 1,2,3,3]
        let groups = [0usize, 0, 1, 2, 2];
        let cutoff = rank_like_cutoff(SortTopNType::DenseRank, 2, groups.len(), |l, r| {
            groups[l] == groups[r]
        });
        assert_eq!(cutoff, 3);
    }
}
