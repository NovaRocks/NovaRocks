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

use crate::runtime::fragment::ExecutionResult;

use std::sync::Arc;

use crate::exec::chunk::Chunk;
use crate::exec::expr::ExprArena;
use crate::exec::node::sort::{SortExpression, SortTopNType};
use crate::exec::operators::sort::{
    ChunksSorter, ChunksSorterHeapSort, ChunksSorterPartitionTopN, ChunksSorterTopN,
};

use super::creation::{CreatedSortArray, CreatedSortColumns, SortCreation};
use crate::exec::pipeline::operator::{
    ConstructionFailure, InputConstructionFactory, Operator, ProcessorOperator,
};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::RuntimeState;

use arrow::array::ArrayRef;
use arrow::compute::{SortColumn, SortOptions};

/// Factory for sort processors that materialize ORDER BY output chunks.
///
/// Design: docs/adr/ADR-0162-in-memory-query-execution-boundary.md
pub struct SortProcessorFactory {
    input_layout: Option<novarocks_local_program::StaticLayout>,
    #[cfg(test)]
    fixture_input_schema: Option<crate::exec::chunk::ChunkSchemaRef>,
    name: String,
    arena: Arc<ExprArena>,
    order_by: Vec<SortExpression>,
    use_top_n: bool,
    limit: Option<usize>,
    offset: usize,
    topn_type: SortTopNType,
    max_buffered_rows: Option<usize>,
    max_buffered_bytes: Option<usize>,
    partition_exprs: Vec<SortExpression>,
    partition_limit: Option<usize>,
}

impl SortProcessorFactory {
    #[cfg(test)]
    pub(crate) fn with_fixture_input_schema(
        mut self,
        schema: crate::exec::chunk::ChunkSchemaRef,
    ) -> Self {
        self.fixture_input_schema = Some(schema);
        self
    }
    pub(crate) fn with_input_layout(
        mut self,
        input: novarocks_local_program::StaticLayout,
    ) -> Self {
        self.input_layout = Some(input);
        self
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "The sort factory mirrors the frozen physical sort-node contract."
    )]
    pub fn new(
        node_id: i32,
        arena: Arc<ExprArena>,
        order_by: Vec<SortExpression>,
        limit: Option<usize>,
        offset: usize,
        topn_type: SortTopNType,
        max_buffered_rows: Option<usize>,
        max_buffered_bytes: Option<usize>,
        partition_exprs: Vec<SortExpression>,
        partition_limit: Option<usize>,
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
            partition_exprs,
            partition_limit,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "The TopN factory mirrors the frozen physical sort-node contract."
    )]
    pub fn new_topn(
        node_id: i32,
        arena: Arc<ExprArena>,
        order_by: Vec<SortExpression>,
        limit: Option<usize>,
        offset: usize,
        topn_type: SortTopNType,
        max_buffered_rows: Option<usize>,
        max_buffered_bytes: Option<usize>,
        partition_exprs: Vec<SortExpression>,
        partition_limit: Option<usize>,
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
            partition_exprs,
            partition_limit,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "The internal constructor preserves the complete sort configuration without an allocation-only options type."
    )]
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
        partition_exprs: Vec<SortExpression>,
        partition_limit: Option<usize>,
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
            input_layout: None,
            #[cfg(test)]
            fixture_input_schema: None,
            name,
            arena,
            order_by,
            use_top_n,
            limit,
            offset,
            topn_type,
            max_buffered_rows,
            max_buffered_bytes,
            partition_exprs,
            partition_limit,
        }
    }
}

impl OperatorFactory for SortProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(SortProcessorOperator {
            input_layout: self.input_layout.clone(),
            #[cfg(test)]
            fixture_input_schema: self.fixture_input_schema.clone(),
            output_schema: None,
            name: self.name.clone(),
            arena: Arc::clone(&self.arena),
            order_by: self.order_by.clone(),
            use_top_n: self.use_top_n,
            limit: self.limit,
            offset: self.offset,
            topn_type: self.topn_type,
            max_buffered_rows: self.max_buffered_rows,
            max_buffered_bytes: self.max_buffered_bytes,
            partition_exprs: self.partition_exprs.clone(),
            partition_limit: self.partition_limit,
            buffered: Vec::new(),
            buffered_bytes: 0,
            buffered_rows: 0,
            pending_output: None,
            finishing: false,
            finished: false,
            profile_initialized: false,
            profiles: None,
            construction: None,
            evaluation_control: None,
            prepared: false,
        })
    }
}

struct SortProcessorOperator {
    input_layout: Option<novarocks_local_program::StaticLayout>,
    #[cfg(test)]
    fixture_input_schema: Option<crate::exec::chunk::ChunkSchemaRef>,
    output_schema: Option<crate::exec::chunk::ChunkSchemaRef>,
    construction: Option<Arc<dyn InputConstructionFactory>>,
    evaluation_control: Option<crate::exec::operators::compiled_expression::RuntimeKernelControl>,
    prepared: bool,
    name: String,
    arena: Arc<ExprArena>,
    order_by: Vec<SortExpression>,
    use_top_n: bool,
    limit: Option<usize>,
    offset: usize,
    topn_type: SortTopNType,
    max_buffered_rows: Option<usize>,
    max_buffered_bytes: Option<usize>,
    partition_exprs: Vec<SortExpression>,
    partition_limit: Option<usize>,
    buffered: Vec<Chunk>,
    buffered_bytes: i64,
    buffered_rows: i64,
    pending_output: Option<Chunk>,
    finishing: bool,
    finished: bool,
    profile_initialized: bool,
    profiles: Option<crate::runtime::profile::OperatorProfiles>,
}

impl Operator for SortProcessorOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.construction = Some(factory);
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        let mut control = crate::exec::operators::compiled_expression::RuntimeKernelControl::new(
            state.error_state(),
        );
        control.bind_runtime_memory(state);
        control.bind_construction_factory(
            self.construction
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?
                .clone(),
        );
        self.evaluation_control = Some(control);
        Ok(())
    }

    fn prepare(&mut self) -> ExecutionResult<()> {
        let factory = self
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        if let Some(layout) = self.input_layout.as_ref() {
            self.output_schema = Some(factory.create_compiled_chunk_schema(layout)?);
        } else {
            #[cfg(test)]
            if let Some(schema) = self.fixture_input_schema.as_ref() {
                // The old functional carrier supplies a predeclared schema;
                // the real installed creator validates it before preparation.
                let _owner = factory.begin_typed_construction(schema)?;
                self.output_schema = Some(schema.clone());
                self.prepared = true;
                return Ok(());
            }
            return Err(ConstructionFailure::InvalidSource(
                "sort lacks its frozen complete input layout".into(),
            )
            .into());
        }
        self.prepared = true;
        Ok(())
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

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        if !self.prepared {
            return Err(ConstructionFailure::MissingOwner.into());
        }
        if self.finished {
            return Ok(());
        }
        self.bind_runtime_state(_state)?;
        self.init_profile_if_needed();
        if self.pending_output.is_some() {
            return Err("sort received input while output buffer is full"
                .to_string()
                .into());
        }
        self.creation()?
            .verify_chunks(std::slice::from_ref(&chunk))?;
        if !chunk.is_empty() {
            let chunk_bytes = i64::try_from(chunk.estimated_bytes()).unwrap_or(i64::MAX);
            let chunk_rows = i64::try_from(chunk.len()).unwrap_or(i64::MAX);
            self.buffered_bytes = self.buffered_bytes.saturating_add(chunk_bytes);
            self.buffered_rows = self.buffered_rows.saturating_add(chunk_rows);
            self.buffered.push(chunk);
        }
        self.maybe_prune_topn_buffered()?;
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        let out = self.pending_output.take();
        if self.finishing && self.pending_output.is_none() {
            self.finished = true;
        }
        Ok(out)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        if self.finishing || self.finished {
            return Ok(());
        }
        self.bind_runtime_state(_state)?;
        self.init_profile_if_needed();
        self.finishing = true;

        let out = self.build_final_output_from_chunks(&self.buffered)?;
        self.clear_buffered();
        self.pending_output = out;
        if self.pending_output.is_none() {
            self.finished = true;
        }
        Ok(())
    }
}

impl SortProcessorOperator {
    fn creation(&self) -> ExecutionResult<SortCreation<'_>> {
        if !self.prepared {
            return Err(ConstructionFailure::MissingOwner.into());
        }
        Ok(SortCreation::new(
            self.construction
                .as_deref()
                .ok_or(ConstructionFailure::MissingOwner)?,
            self.output_schema
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?,
        )
        .with_evaluation_control(
            self.evaluation_control
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?,
        ))
    }

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

    fn build_topn_sorter(&self) -> Option<Box<dyn ChunksSorter>> {
        // Partition-TopN must be checked FIRST: a partition-topn node has
        // use_top_n=true and no global limit, so rows_to_keep_for_topn() /
        // rank_like_limit_for_topn() both return None and would fall through
        // to the `None` arm, silently bypassing per-partition truncation.
        // Checking is_partition_topn() first ensures the correct path always
        // fires regardless of global-limit and topn_type combinations.
        let sorter: Box<dyn ChunksSorter> = if self.is_partition_topn() {
            let partition_limit = self
                .partition_limit
                .expect("is_partition_topn implies Some");
            Box::new(ChunksSorterPartitionTopN::new(
                Arc::clone(&self.arena),
                self.partition_exprs.clone(),
                self.order_by.clone(),
                self.topn_type,
                partition_limit,
            ))
        } else if let Some(rows_to_keep) = self.rows_to_keep_for_topn() {
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
        Some(sorter)
    }

    fn sort_chunks_for_topn_mode(
        &self,
        chunks: &[Chunk],
    ) -> crate::runtime::fragment::ExecutionResult<Option<Chunk>> {
        let Some(sorter) = self.build_topn_sorter() else {
            return Ok(None);
        };
        sorter.sort_chunks(chunks, &self.creation()?)
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

    fn truncate_buffered_prefix(&mut self, rows_to_keep: usize) -> ExecutionResult<()> {
        if rows_to_keep == 0 {
            self.buffered.clear();
            self.buffered_bytes = 0;
            self.buffered_rows = 0;
            return Ok(());
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
                kept.push(self.creation()?.slice(chunk, 0, remain)?);
                remain = 0;
            }
        }
        self.buffered = kept;
        self.recompute_buffered_stats();
        Ok(())
    }

    fn maybe_prune_topn_buffered(&mut self) -> crate::runtime::fragment::ExecutionResult<()> {
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
                self.truncate_buffered_prefix(cutoff_base)?;
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

    fn clear_buffered(&mut self) {
        self.buffered.clear();
        self.buffered_bytes = 0;
        self.buffered_rows = 0;
    }

    fn is_partition_topn(&self) -> bool {
        self.partition_limit.is_some() && !self.partition_exprs.is_empty()
    }

    fn build_final_output_from_chunks(
        &self,
        chunks: &[Chunk],
    ) -> crate::runtime::fragment::ExecutionResult<Option<Chunk>> {
        if chunks.is_empty() {
            return Ok(None);
        }
        // Per-partition TopN: must return early before the ordinary limit/rank/full-sort
        // fallbacks below, none of which know about partition grouping.
        if self.is_partition_topn() {
            return self.sort_chunks_for_topn_mode(chunks);
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
            return Ok(Some(self.creation()?.slice(
                &topn_chunk,
                self.offset,
                take_len,
            )?));
        }

        let creation = self.creation()?;
        creation.verify_chunks(chunks)?;
        let owner = creation.begin(&chunks[0])?;
        let batch = creation.concat(chunks, owner.as_ref())?;
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
            let merged = creation.publish(batch, &chunks[0], owner.as_ref())?;
            return Ok(Some(creation.slice(&merged, self.offset, take_len)?));
        }
        let indices = self.sort_indices(&batch, &chunks[0])?;
        let len = indices.array().len();
        if self.offset >= len {
            return Ok(None);
        }
        let take_len = if let Some(l) = self.limit {
            l.min(len - self.offset)
        } else {
            len - self.offset
        };
        let index_slice_owner = creation.begin(&chunks[0])?;
        index_slice_owner.request(
            novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<arrow::array::UInt32Array>(),
            )
            .map_err(ConstructionFailure::from)?,
        )?;
        let sliced_indices = indices.array().slice(self.offset, take_len);
        let sorted_batch =
            creation.take_batch(&batch, sliced_indices.as_ref(), &chunks[0], owner.as_ref())?;
        Ok(Some(creation.publish(
            sorted_batch,
            &chunks[0],
            owner.as_ref(),
        )?))
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
    ) -> crate::runtime::fragment::ExecutionResult<Option<Chunk>> {
        if self.offset != 0 {
            return Err(format!(
                "topn_type {:?} requires offset=0, got {}",
                self.topn_type, self.offset
            )
            .into());
        }
        let rank_limit = self.limit.unwrap_or(0);
        if rank_limit == 0 {
            return Ok(None);
        }

        let creation = self.creation()?;
        creation.verify_chunks(chunks)?;
        let owner = creation.begin(&chunks[0])?;
        let batch = creation.concat(chunks, owner.as_ref())?;
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        if self.order_by.is_empty() {
            // Without order-by keys all rows belong to one peer group (rank = 1).
            return Ok(Some(creation.publish(batch, &chunks[0], owner.as_ref())?));
        }

        let indices = self.sort_indices(&batch, &chunks[0])?;
        let sorted_batch =
            creation.take_batch(&batch, indices.array().as_ref(), &chunks[0], owner.as_ref())?;
        let key_columns = self.eval_order_by_columns(&sorted_batch, &chunks[0])?;
        let converter = creation.row_converter(
            &key_columns,
            key_columns.storage_lifetime(),
            self.order_by.iter().map(|expr| SortOptions {
                descending: !expr.asc,
                nulls_first: expr.nulls_first,
            }),
        )?;
        let key_rows = converter
            .converter()
            .convert_columns(&key_columns)
            .map_err(|cause| cause.to_string())?;
        let cutoff = rank_like_cutoff(
            self.topn_type,
            rank_limit,
            sorted_batch.num_rows(),
            |lhs, rhs| key_rows.row(lhs) == key_rows.row(rhs),
        );
        if cutoff == 0 {
            return Ok(None);
        }
        let sorted = creation.publish(sorted_batch, &chunks[0], owner.as_ref())?;
        Ok(Some(creation.slice(&sorted, 0, cutoff)?))
    }

    fn sort_indices(
        &self,
        batch: &arrow::record_batch::RecordBatch,
        source: &Chunk,
    ) -> ExecutionResult<CreatedSortArray> {
        let creation = self.creation()?;
        let owner = creation.begin(source)?;
        let key_columns = self.eval_order_by_columns(batch, source)?;
        let sort_columns = self.build_sort_columns(&key_columns, &creation, owner.as_ref())?;
        creation.lexsort(&sort_columns, None, source)
    }

    fn eval_order_by_columns(
        &self,
        batch: &arrow::record_batch::RecordBatch,
        source: &Chunk,
    ) -> ExecutionResult<CreatedSortColumns> {
        let creation = self.creation()?;
        let owner = creation.begin(source)?;
        let types = creation.begin(source)?;
        owner.observe(
            0,
            &[],
            crate::exec::chunk::ArrayNormalizationStep::CreateChunkColumns {
                channels: batch.num_columns(),
            },
        )?;
        let chunk = creation.publish(batch.clone(), source, owner.as_ref())?;
        owner.request(std::alloc::Layout::array::<ArrayRef>(self.order_by.len()).map_err(|_| ConstructionFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?)?;
        let mut columns = Vec::with_capacity(self.order_by.len());
        for sort_expr in &self.order_by {
            let evaluation = creation.expression_context(&self.arena, owner.as_ref())?;
            let values = evaluation.eval(sort_expr.expr, &chunk)?;
            let declared = self.arena.value_type(sort_expr.expr).ok_or_else(|| {
                ConstructionFailure::InvalidSource(
                    "sort key lacks its complete declared logical type".into(),
                )
            })?;
            columns.push(creation.normalize_key(
                &values,
                declared.value_type(),
                owner.as_ref(),
                types.as_ref(),
            )?);
        }
        Ok(CreatedSortColumns::new(
            columns,
            creation.source_type_storage(source, types.as_ref())?,
            owner.storage_lifetime(),
        ))
    }

    fn build_sort_columns(
        &self,
        key_columns: &[ArrayRef],
        creation: &SortCreation<'_>,
        owner: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
    ) -> ExecutionResult<Vec<SortColumn>> {
        let mut columns = creation.vector::<SortColumn>(key_columns.len(), owner)?;
        for (values, sort_expr) in key_columns.iter().zip(&self.order_by) {
            columns.push(SortColumn {
                values: values.clone(),
                options: Some(SortOptions {
                    descending: !sort_expr.asc,
                    nulls_first: sort_expr.nulls_first,
                }),
            });
        }
        if let Some(first) = key_columns.first() {
            creation.stable_index(&mut columns, first.len(), owner)?;
        }
        Ok(columns)
    }
}

/// Compute output row count for rank-based topn.
///
/// `peer_equal(i, j)` returns whether sorted row `i` and `j` belong to the same
/// ORDER BY peer group.
pub(crate) fn rank_like_cutoff<F>(
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory;
    use arrow::array::{Array, Decimal128Array, Int32Array, make_array};
    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_local_program::StaticLayout;
    use novarocks_type_contract::{
        CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
        TypedValueBinding, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime,
    };
    use novarocks_types::SlotId;
    use std::alloc::Layout;

    struct DeclarationControl;
    impl PureCompileControl for DeclarationControl {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }
    struct TestSource {
        layout: StaticLayout,
        creator: Arc<dyn InputConstructionFactory>,
    }
    fn source_for(state: &RuntimeState, fields: &[(&str, DataType, bool)]) -> TestSource {
        let bindings = fields
            .iter()
            .map(|(_, ty, nullable)| {
                let storage = ValueTypeStorageLifetime::pure();
                TypedValueBinding::try_new(
                    ValueTypeRef::try_new(
                        ValueKind::Scalar(ty.clone()),
                        *nullable,
                        storage.clone(),
                    )
                    .unwrap(),
                    ValueLayout::try_new(ty, storage).unwrap(),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(
            fields
                .iter()
                .map(|(name, ty, nullable)| Field::new(*name, ty.clone(), *nullable))
                .collect::<Vec<_>>(),
        ));
        let layout = StaticLayout::try_new_typed_for_compile(
            schema,
            Arc::from(
                (1..=fields.len())
                    .map(|id| SlotId::new(u32::try_from(id).unwrap()))
                    .collect::<Vec<_>>(),
            ),
            bindings
                .into_iter()
                .map(|binding| (binding, None))
                .collect(),
            ValueTypeStorageLifetime::pure(),
            |_| Ok(()),
            &DeclarationControl,
        )
        .unwrap();
        let creator = try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            crate::runtime::mem_tracker::MemTracker::new_root("original-sort-test-scaffolds"),
            state.error_state(),
        )
        .unwrap();
        TestSource { layout, creator }
    }
    fn source_chunk<const N: usize>(
        source: &TestSource,
        arrays: [ArrayRef; N],
        rows: usize,
    ) -> Chunk {
        let schema = source
            .creator
            .create_compiled_chunk_schema(&source.layout)
            .unwrap();
        let owner = source.creator.begin_typed_construction(&schema).unwrap();
        owner
            .request(Layout::array::<ArrayRef>(N).unwrap())
            .unwrap();
        Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
            schema,
            arrays.into_iter().collect(),
            rows,
            owner.storage_lifetime(),
            |step| owner.observe(0, &[], step),
        )
        .unwrap()
    }
    fn make_chunk(source: &TestSource, values: Vec<Option<i32>>) -> Chunk {
        let rows = values.len();
        source_chunk(
            source,
            [Arc::new(Int32Array::from(values)) as ArrayRef],
            rows,
        )
    }
    fn make_chunk_with_decimal_payload(
        source: &TestSource,
        ids: Vec<i32>,
        prices: Vec<i128>,
    ) -> Chunk {
        let rows = ids.len();
        let wide_prices = Decimal128Array::from(prices.into_iter().map(Some).collect::<Vec<_>>())
            .with_precision_and_scale(38, 2)
            .expect("wide decimal prices");
        let price_data = wide_prices
            .to_data()
            .into_builder()
            .data_type(DataType::Decimal128(20, 2))
            .build()
            .expect("decimal20 price data");
        source_chunk(
            source,
            [
                Arc::new(Int32Array::from(ids)) as ArrayRef,
                make_array(price_data),
            ],
            rows,
        )
    }
    #[test]
    fn full_sort_reconciles_nullable_columns_across_chunks() {
        let state = RuntimeState::default();
        // One declared nullable channel covers both batches, independent of observed NULL rows.
        let source = source_for(&state, &[("v", DataType::Int32, true)]);
        let chunks = vec![
            make_chunk(&source, vec![Some(1), Some(2)]),
            make_chunk(&source, vec![None, Some(3)]),
        ];
        let mut processor = processor_for(&source, false, None, 0, SortTopNType::RowNumber, None);
        for chunk in chunks {
            processor
                .push_chunk(&state, chunk)
                .expect("push full-sort input");
        }
        processor.set_finishing(&state).expect("finish full sort");
        let out = processor
            .pull_chunk(&state)
            .expect("pull sorted output")
            .expect("chunk");
        assert!(out.record_batch().schema().field(0).is_nullable());
        let col = out
            .record_batch()
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32");
        assert_eq!(col.len(), 4);
        assert!(col.is_null(0));
        assert_eq!(col.value(1), 1);
        assert_eq!(col.value(2), 2);
        assert_eq!(col.value(3), 3);
    }

    #[test]
    fn full_sort_preserves_decimal_payload_exceeding_declared_precision() {
        let overflow = 100_000_000_000_000_000_000_i128;
        let state = RuntimeState::default();
        let source = source_for(
            &state,
            &[
                ("id", DataType::Int32, true),
                ("price", DataType::Decimal128(20, 2), true),
            ],
        );
        let chunks = vec![make_chunk_with_decimal_payload(
            &source,
            vec![2, 1],
            vec![overflow, 12_345_i128],
        )];
        let mut processor = processor_for(&source, false, None, 0, SortTopNType::RowNumber, None);
        for chunk in chunks {
            processor
                .push_chunk(&state, chunk)
                .expect("push full-sort input");
        }
        processor.set_finishing(&state).expect("finish full sort");
        let out = processor
            .pull_chunk(&state)
            .expect("pull sorted output")
            .expect("chunk");
        let ids = out
            .record_batch()
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id int32");
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
        let prices = out
            .record_batch()
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("price decimal");
        // Sort no longer widens decimal precision (it uses the array's own type;
        // planning makes cross-fragment decimals canonical). The i128 payload is
        // preserved regardless of the precision tag, so the over-precision value
        // survives the sort/concat round trip with the declared Decimal128(20, 2).
        assert_eq!(prices.data_type(), &DataType::Decimal128(20, 2));
        assert_eq!(prices.value(0), 12_345_i128);
        assert!(!prices.is_null(1));
        assert_eq!(prices.value(1), overflow);
    }

    fn processor_for(
        source: &TestSource,
        use_top_n: bool,
        limit: Option<usize>,
        offset: usize,
        topn_type: SortTopNType,
        max_buffered_rows: Option<usize>,
    ) -> SortProcessorOperator {
        let mut arena = ExprArena::default();
        let key = arena.push_value_typed(
            crate::exec::expr::ExprNode::SlotId(SlotId::new(1)),
            FunctionValueType::try_from_typed_binding(
                source.layout.value_binding_at(0).unwrap().clone(),
            )
            .unwrap(),
        );
        let mut processor = SortProcessorOperator {
            input_layout: Some(source.layout.clone()),
            fixture_input_schema: None,
            output_schema: None,
            construction: Some(source.creator.clone()),
            evaluation_control: None,
            prepared: false,
            name: "test sort".to_string(),
            arena: Arc::new(arena),
            order_by: vec![SortExpression {
                expr: key,
                asc: true,
                nulls_first: true,
            }],
            use_top_n,
            limit,
            offset,
            topn_type,
            max_buffered_rows,
            max_buffered_bytes: None,
            partition_exprs: Vec::new(),
            partition_limit: None,
            buffered: Vec::new(),
            buffered_bytes: 0,
            buffered_rows: 0,
            pending_output: None,
            finishing: false,
            finished: false,
            profile_initialized: false,
            profiles: None,
        };
        processor.prepare().expect("actual installed sort prepare");
        processor
    }
    fn chunk_from_array(source: &TestSource, array: ArrayRef) -> Chunk {
        let rows = array.len();
        source_chunk(source, [array], rows)
    }
    fn int_chunk(source: &TestSource, values: Vec<i32>) -> Chunk {
        chunk_from_array(source, Arc::new(Int32Array::from(values)))
    }
    fn int_values(chunk: &Chunk) -> Vec<i32> {
        chunk.columns()[0]
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .expect("int output")
            .values()
            .to_vec()
    }

    #[test]
    fn small_and_large_topn_prune_inputs_preserve_offset_and_release_candidates_at_finish() {
        for limit in [3, 1025] {
            let offset = 2;
            let rows_to_keep = limit + offset;
            let state = RuntimeState::default();
            let source = source_for(&state, &[("v", DataType::Int32, true)]);
            let mut processor = processor_for(
                &source,
                true,
                Some(limit),
                offset,
                SortTopNType::RowNumber,
                Some(rows_to_keep + 1),
            );
            let tracker = crate::runtime::mem_tracker::MemTracker::new_root("sort input");
            let original = (0..(rows_to_keep * 2) as i32).rev().collect::<Vec<_>>();
            let mut chunk = int_chunk(&source, original.clone());
            let schema = chunk.chunk_schema_ref();
            let input_array = Arc::downgrade(&chunk.columns()[0]);
            chunk.transfer_to(&tracker);
            assert!(tracker.current() > 0);
            processor
                .push_chunk(&state, chunk)
                .expect("push first batch");
            assert_eq!(processor.buffered_rows, rows_to_keep as i64);
            assert!(
                input_array.upgrade().is_none(),
                "pruning releases the input Arrow owner"
            );
            assert_eq!(
                tracker.current(),
                0,
                "pruning releases the original input charge"
            );
            processor
                .push_chunk(&state, int_chunk(&source, (-6..0).collect()))
                .expect("push late best rows");
            assert_eq!(processor.buffered_rows, rows_to_keep as i64);
            let candidate_array = Arc::downgrade(&processor.buffered[0].columns()[0]);
            processor.set_finishing(&state).expect("finish");
            assert!(processor.buffered.is_empty());
            assert_eq!((processor.buffered_rows, processor.buffered_bytes), (0, 0));
            assert!(
                candidate_array.upgrade().is_none(),
                "finish releases the retained candidate"
            );
            let output = processor.pull_chunk(&state).expect("pull").expect("output");
            let mut expected = original;
            expected.extend(-6..0);
            expected.sort_unstable();
            assert_eq!(
                int_values(&output),
                expected
                    .into_iter()
                    .skip(offset)
                    .take(limit)
                    .collect::<Vec<_>>()
            );
            assert_eq!(output.chunk_schema(), schema.as_ref());
            assert!(processor.is_finished());
            assert!(
                processor
                    .pull_chunk(&state)
                    .expect("pull after finish")
                    .is_none()
            );
        }
    }

    #[test]
    fn rank_and_dense_rank_pruning_recompute_ties_across_batches() {
        for (kind, expected) in [
            (SortTopNType::Rank, vec![1, 1]),
            (SortTopNType::DenseRank, vec![1, 1, 2, 2, 2]),
        ] {
            let state = RuntimeState::default();
            let source = source_for(&state, &[("v", DataType::Int32, true)]);
            let mut processor = processor_for(&source, true, Some(2), 0, kind, Some(3));
            processor
                .push_chunk(&state, int_chunk(&source, vec![4, 2, 2, 1]))
                .expect("first peer groups");
            processor
                .push_chunk(&state, int_chunk(&source, vec![2, 1, 3]))
                .expect("late boundary peers");
            processor.set_finishing(&state).expect("finish");
            let output = processor.pull_chunk(&state).expect("pull").expect("output");
            assert_eq!(int_values(&output), expected);
            assert!(processor.buffered.is_empty());
        }
    }

    #[test]
    fn full_sort_preserves_null_nan_order_and_releases_input_before_output_is_pulled() {
        use arrow::array::{Array, Float64Array};
        let state = RuntimeState::default();
        let source = source_for(&state, &[("v", DataType::Float64, true)]);
        let mut processor = processor_for(&source, false, None, 0, SortTopNType::RowNumber, None);
        let mut chunk = chunk_from_array(
            &source,
            Arc::new(Float64Array::from(vec![
                Some(f64::NAN),
                Some(2.0),
                None,
                Some(-1.0),
            ])),
        );
        let input_array = Arc::downgrade(&chunk.columns()[0]);
        let tracker = crate::runtime::mem_tracker::MemTracker::new_root("full sort input");
        chunk.transfer_to(&tracker);
        processor.push_chunk(&state, chunk).expect("push");
        assert!(input_array.upgrade().is_some());
        assert!(tracker.current() > 0);
        processor.set_finishing(&state).expect("finish");
        assert!(input_array.upgrade().is_none());
        assert_eq!(tracker.current(), 0);
        assert_eq!((processor.buffered_rows, processor.buffered_bytes), (0, 0));
        let output = processor.pull_chunk(&state).expect("pull").expect("output");
        let values = output.columns()[0]
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float output");
        assert_eq!(values.len(), 4);
        assert!(values.is_null(0));
        assert_eq!(values.value(1), -1.0);
        assert_eq!(values.value(2), 2.0);
        assert!(values.value(3).is_nan());
    }

    #[test]
    fn empty_sort_finishes_without_output_for_full_sort_and_topn() {
        for use_top_n in [false, true] {
            let state = RuntimeState::default();
            let source = source_for(&state, &[("v", DataType::Int32, true)]);
            let mut processor = processor_for(
                &source,
                use_top_n,
                Some(3),
                0,
                SortTopNType::RowNumber,
                None,
            );
            processor.set_finishing(&state).expect("finish empty input");
            assert!(processor.is_finished());
            assert!(!processor.has_output());
            assert!(!processor.need_input());
            assert!(processor.pull_chunk(&state).expect("pull empty").is_none());
        }
    }

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

    /// End-to-end integration test: per-partition TopN with topn_type=Rank,
    /// partition_limit=Some(2), and NO global limit (limit=None).
    ///
    /// This proves the CONTRACT:
    ///  - use_top_n=true + limit=None + partition_limit=Some(2) routes to the
    ///    partition sorter, not the global sort path.
    ///  - Output is 4 rows (per-partition top-2), NOT all 6 (limit=None=all)
    ///    and NOT global top-2 rows.
    ///
    /// Input:  p=[1,1,1,2,2,2], o=[10,20,30,5,6,7]
    /// Expected: partition 1 keeps o=[10,20], partition 2 keeps o=[5,6]  → 4 rows
    #[test]
    fn partition_topn_rank_limit_none_routes_per_partition_not_global() {
        use crate::exec::expr::{ExprArena, ExprNode};
        use crate::exec::node::sort::{SortExpression, SortTopNType};
        use arrow::array::{Array, Int32Array};
        use arrow::datatypes::DataType;
        use novarocks_types::SlotId;
        use std::sync::Arc;

        let state = RuntimeState::default();
        let source = source_for(
            &state,
            &[("p", DataType::Int32, true), ("o", DataType::Int32, true)],
        );
        let chunk = source_chunk(
            &source,
            [
                Arc::new(Int32Array::from(vec![
                    Some(1),
                    Some(1),
                    Some(1),
                    Some(2),
                    Some(2),
                    Some(2),
                ])),
                Arc::new(Int32Array::from(vec![
                    Some(10),
                    Some(20),
                    Some(30),
                    Some(5),
                    Some(6),
                    Some(7),
                ])),
            ],
            6,
        );

        let mut arena = ExprArena::default();
        let p_expr = arena.push_value_typed(
            ExprNode::SlotId(SlotId::new(1)),
            FunctionValueType::try_from_typed_binding(
                source.layout.value_binding_at(0).unwrap().clone(),
            )
            .unwrap(),
        );
        let o_expr = arena.push_value_typed(
            ExprNode::SlotId(SlotId::new(2)),
            FunctionValueType::try_from_typed_binding(
                source.layout.value_binding_at(1).unwrap().clone(),
            )
            .unwrap(),
        );
        let partition_exprs = vec![SortExpression {
            expr: p_expr,
            asc: true,
            nulls_first: true,
        }];
        let order_by = vec![SortExpression {
            expr: o_expr,
            asc: true,
            nulls_first: true,
        }];
        let arena = Arc::new(arena);

        // KEY ASPECT: use_top_n=true, limit=None, topn_type=Rank, partition_limit=Some(2)
        let factory = SortProcessorFactory::new_topn(
            -1,
            Arc::clone(&arena),
            order_by,
            None, // NO global limit — partition-topn is decoupled from global limit
            0,
            SortTopNType::Rank, // Rank type — exercises the partition branch precedence
            None,
            None,
            partition_exprs,
            Some(2), // per-partition cap = 2
        );

        let factory = factory.with_input_layout(source.layout.clone());
        let mut op = factory.create(1, 0);
        op.set_construction_factory(source.creator.clone());
        op.prepare()
            .expect("actual installed partition-sort prepare");
        let proc = op.as_processor_mut().expect("processor");

        proc.push_chunk(&state, chunk).expect("push chunk");
        proc.set_finishing(&state).expect("set finishing");

        let result = proc
            .pull_chunk(&state)
            .expect("pull chunk")
            .expect("non-empty output");

        // Must be exactly 4 rows: top-2 from each of 2 partitions.
        // Proves it is PER-PARTITION (4), not global (all 6 since limit=None),
        // and not global top-2 (which would be 2 rows).
        assert_eq!(
            result.len(),
            4,
            "expected 4 rows (per-partition top-2 with Rank), got {}",
            result.len()
        );

        let o_col = result
            .record_batch()
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 order column");
        let o_vals: Vec<Option<i32>> = (0..o_col.len())
            .map(|i| {
                if o_col.is_null(i) {
                    None
                } else {
                    Some(o_col.value(i))
                }
            })
            .collect();

        // Partition 1 (p=1): Rank top-2 ascending → o=[10,20]
        // Partition 2 (p=2): Rank top-2 ascending → o=[5,6]
        assert_eq!(
            o_vals,
            vec![Some(10), Some(20), Some(5), Some(6)],
            "per-partition Rank order values mismatch: {:?}",
            o_vals
        );
    }

    /// Verify that `build_topn_sorter` selects the partition path when
    /// `partition_exprs` is non-empty AND `partition_limit` is `Some`, and that
    /// the resulting sorter yields the correct per-partition top-2 output.
    ///
    /// Input: two partitions (p=1 → o=[10,20,30], p=2 → o=[5,6,7])
    /// Expected after RowNumber partition_limit=2:
    ///   partition 1 keeps o=[10,20]; partition 2 keeps o=[5,6]
    #[test]
    fn build_topn_sorter_uses_partition_path_and_yields_per_partition_result() {
        use crate::exec::expr::{ExprArena, ExprNode};
        use crate::exec::node::sort::{SortExpression, SortTopNType};
        use arrow::array::{Array, Int32Array};
        use arrow::datatypes::DataType;
        use novarocks_types::SlotId;
        use std::sync::Arc;

        // Build a two-column chunk: col 0 = partition key (SlotId 1), col 1 = order key (SlotId 2).
        let state = RuntimeState::default();
        let source = source_for(
            &state,
            &[("p", DataType::Int32, true), ("o", DataType::Int32, true)],
        );
        let chunk = source_chunk(
            &source,
            [
                Arc::new(Int32Array::from(vec![
                    Some(1),
                    Some(1),
                    Some(1),
                    Some(2),
                    Some(2),
                    Some(2),
                ])),
                Arc::new(Int32Array::from(vec![
                    Some(10),
                    Some(20),
                    Some(30),
                    Some(5),
                    Some(6),
                    Some(7),
                ])),
            ],
            6,
        );

        let mut arena = ExprArena::default();
        let p_expr = arena.push_value_typed(
            ExprNode::SlotId(SlotId::new(1)),
            FunctionValueType::try_from_typed_binding(
                source.layout.value_binding_at(0).unwrap().clone(),
            )
            .unwrap(),
        );
        let o_expr = arena.push_value_typed(
            ExprNode::SlotId(SlotId::new(2)),
            FunctionValueType::try_from_typed_binding(
                source.layout.value_binding_at(1).unwrap().clone(),
            )
            .unwrap(),
        );
        let partition_exprs = vec![SortExpression {
            expr: p_expr,
            asc: true,
            nulls_first: true,
        }];
        let order_by = vec![SortExpression {
            expr: o_expr,
            asc: true,
            nulls_first: true,
        }];
        let arena = Arc::new(arena);

        // Build SortProcessorFactory with partition_exprs + partition_limit=2.
        let factory = SortProcessorFactory::new_topn(
            -1,
            Arc::clone(&arena),
            order_by,
            None, // limit (not used for partition path)
            0,
            SortTopNType::RowNumber,
            None,
            None,
            partition_exprs,
            Some(2),
        );

        // Instantiate the operator and drive it through the push/set_finishing/pull cycle.
        let factory = factory.with_input_layout(source.layout.clone());
        let mut op = factory.create(1, 0);
        op.set_construction_factory(source.creator.clone());
        op.prepare()
            .expect("actual installed partition-sort prepare");
        let proc = op.as_processor_mut().expect("processor");

        // Drive the ordinary runtime state through the processor lifecycle.

        proc.push_chunk(&state, chunk).expect("push chunk");
        proc.set_finishing(&state).expect("set finishing");

        let result = proc
            .pull_chunk(&state)
            .expect("pull chunk")
            .expect("non-empty output");

        // The output must have exactly 4 rows: top-2 from each partition.
        assert_eq!(
            result.len(),
            4,
            "expected 4 rows (top-2 per partition), got {}",
            result.len()
        );

        // Extract the order-key column (col 1) from the result.
        let o_col = result
            .record_batch()
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 order column");
        let o_vals: Vec<Option<i32>> = (0..o_col.len())
            .map(|i| {
                if o_col.is_null(i) {
                    None
                } else {
                    Some(o_col.value(i))
                }
            })
            .collect();

        // Partition 1 (p=1): top-2 ascending => o=[10, 20]
        // Partition 2 (p=2): top-2 ascending => o=[5, 6]
        assert_eq!(
            o_vals,
            vec![Some(10), Some(20), Some(5), Some(6)],
            "per-partition order values mismatch: {:?}",
            o_vals
        );
    }
}
