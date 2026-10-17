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
//! Heap-sort style topn chunks sorter.
//!
//! This sorter is a small-limit specialization for ROW_NUMBER topn.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use super::creation::{CreatedSortConverter, SortCreation};
use crate::exec::chunk::Chunk;
use crate::exec::expr::ExprArena;
use crate::exec::node::sort::SortExpression;
use crate::exec::operators::sort::ChunksSorter;

use arrow::array::ArrayRef;
use arrow::compute::SortOptions;
use arrow::row::OwnedRow;

#[derive(Debug)]
struct TopNHeapEntry {
    key: OwnedRow,
    seq: u64,
    chunk: Arc<Chunk>,
    _chunk_header: novarocks_type_contract::ValueTypeStorageLifetime,
    row_idx: usize,
}

impl PartialEq for TopNHeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.seq == other.seq
    }
}

impl Eq for TopNHeapEntry {}

impl PartialOrd for TopNHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TopNHeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap keeps the largest element on top.
        // "Largest" here means "worst row" for top-n eviction.
        self.key
            .cmp(&other.key)
            .then_with(|| self.seq.cmp(&other.seq))
    }
}

struct TopNHeapKernel {
    order_by: Vec<SortExpression>,
    rows_to_keep: usize,
    // OwnedRow configs in the heap share converter type backing. Drop heap
    // before the guarded converter, so its finite responsibility outlives them.
    heap: BinaryHeap<TopNHeapEntry>,
    row_converter: Option<CreatedSortConverter>,
    next_seq: u64,
}

impl TopNHeapKernel {
    fn new(order_by: &[SortExpression], rows_to_keep: usize) -> Self {
        Self {
            order_by: order_by.to_vec(),
            rows_to_keep,
            row_converter: None,
            heap: BinaryHeap::new(),
            next_seq: 0,
        }
    }

    fn update(
        &mut self,
        arena: &ExprArena,
        chunk: Chunk,
        creation: &SortCreation<'_>,
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        if self.rows_to_keep == 0 || chunk.is_empty() {
            return Ok(());
        }
        if self.order_by.is_empty() {
            return Err("topn sorter requires at least one order-by key".into());
        }

        let owner = creation.begin(&chunk)?;
        let types = creation.begin(&chunk)?;
        let chunk_header = creation.begin(&chunk)?;
        chunk_header.request(
            novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<Chunk>(),
            )
            .map_err(crate::exec::pipeline::operator::ConstructionFailure::from)?,
        )?;
        let chunk = Arc::new(chunk);
        let mut key_columns = creation.vector::<ArrayRef>(self.order_by.len(), owner.as_ref())?;
        for sort_expr in &self.order_by {
            let evaluation = creation.expression_context(arena, owner.as_ref())?;
            let key = evaluation.eval(sort_expr.expr, &chunk)?;
            let declared = arena.value_type(sort_expr.expr).ok_or_else(|| {
                crate::exec::pipeline::operator::ConstructionFailure::InvalidSource(
                    "sort key lacks its complete declared logical type".into(),
                )
            })?;
            key_columns.push(creation.normalize_key(
                &key,
                declared.value_type(),
                owner.as_ref(),
                types.as_ref(),
            )?);
        }

        let key_columns = super::creation::CreatedSortColumns::new(
            key_columns,
            creation.source_type_storage(&chunk, types.as_ref())?,
            owner.storage_lifetime(),
        );
        if self.row_converter.is_none() {
            let converter = creation.row_converter(
                key_columns.columns(),
                key_columns.storage_lifetime(),
                self.order_by.iter().map(|expr| SortOptions {
                    descending: !expr.asc,
                    nulls_first: expr.nulls_first,
                }),
            )?;
            self.row_converter = Some(converter);
        }

        let rows = self
            .row_converter
            .as_ref()
            .expect("row converter initialized")
            .convert_columns(&key_columns)
            .map_err(|e| e.to_string())?;

        for row_idx in 0..chunk.len() {
            let entry = TopNHeapEntry {
                key: rows.row(row_idx).owned(),
                seq: self.next_seq,
                chunk: Arc::clone(&chunk),
                _chunk_header: chunk_header.storage_lifetime(),
                row_idx,
            };
            self.next_seq = self.next_seq.saturating_add(1);

            if self.heap.len() < self.rows_to_keep {
                self.heap.push(entry);
                continue;
            }
            if let Some(worst) = self.heap.peek()
                && entry < *worst
            {
                let _ = self.heap.pop();
                self.heap.push(entry);
            }
        }

        Ok(())
    }

    fn finish(
        self,
        creation: &SortCreation<'_>,
    ) -> crate::runtime::fragment::ExecutionResult<Option<Chunk>> {
        if self.heap.is_empty() {
            return Ok(None);
        }

        let mut selected = self.heap.into_vec();
        selected.sort_unstable();

        let source_chunk = selected[0].chunk.clone();
        let row_chunks = selected
            .into_iter()
            .map(|entry| creation.slice(entry.chunk.as_ref(), entry.row_idx, 1))
            .collect::<crate::runtime::fragment::ExecutionResult<Vec<_>>>()?;
        let owner = creation.begin(&source_chunk)?;
        let batch = creation.concat(&row_chunks, owner.as_ref())?;
        let chunk = creation.publish(batch, &source_chunk, owner.as_ref())?;
        Ok(Some(chunk))
    }
}

pub(crate) fn sort_chunks_topn_heap(
    arena: &ExprArena,
    order_by: &[SortExpression],
    rows_to_keep: usize,
    chunks: &[Chunk],
    creation: &SortCreation<'_>,
) -> crate::runtime::fragment::ExecutionResult<Option<Chunk>> {
    if rows_to_keep == 0 || chunks.is_empty() {
        return Ok(None);
    }
    if order_by.is_empty() {
        let owner = creation.begin(&chunks[0])?;
        let batch = creation.concat(chunks, owner.as_ref())?;
        let keep = rows_to_keep.min(batch.num_rows());
        if keep == 0 {
            return Ok(None);
        }
        let chunk = creation.publish(batch, &chunks[0], owner.as_ref())?;
        return Ok(Some(creation.slice(&chunk, 0, keep)?));
    }

    let mut sorter = TopNHeapKernel::new(order_by, rows_to_keep);
    for chunk in chunks {
        sorter.update(arena, chunk.clone(), creation)?;
    }
    sorter.finish(creation)
}

/// Heap-sort specialization for topn with small `rows_to_keep`.
pub(crate) struct ChunksSorterHeapSort {
    arena: Arc<ExprArena>,
    order_by: Vec<SortExpression>,
    rows_to_keep: usize,
}

impl ChunksSorterHeapSort {
    pub(crate) fn new(
        arena: Arc<ExprArena>,
        order_by: Vec<SortExpression>,
        rows_to_keep: usize,
    ) -> Self {
        Self {
            arena,
            order_by,
            rows_to_keep,
        }
    }
}

impl ChunksSorter for ChunksSorterHeapSort {
    fn sort_chunks(
        &self,
        chunks: &[Chunk],
        creation: &SortCreation<'_>,
    ) -> crate::runtime::fragment::ExecutionResult<Option<Chunk>> {
        sort_chunks_topn_heap(
            self.arena.as_ref(),
            &self.order_by,
            self.rows_to_keep,
            chunks,
            creation,
        )
    }
}
