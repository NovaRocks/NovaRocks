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

use crate::exec::chunk::Chunk;
use crate::exec::expr::ExprArena;
use crate::exec::node::sort::SortExpression;
use crate::exec::operators::sort::{ChunksSorter, normalize_sort_key_array};

use arrow::array::ArrayRef;
use arrow::compute::{SortOptions, concat_batches};
use arrow::row::{OwnedRow, RowConverter, SortField};

#[derive(Debug)]
struct TopNHeapEntry {
    key: OwnedRow,
    seq: u64,
    chunk: Arc<Chunk>,
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
    row_converter: Option<RowConverter>,
    heap: BinaryHeap<TopNHeapEntry>,
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

    fn update(&mut self, arena: &ExprArena, chunk: Chunk) -> Result<(), String> {
        if self.rows_to_keep == 0 || chunk.is_empty() {
            return Ok(());
        }
        if self.order_by.is_empty() {
            return Err("topn sorter requires at least one order-by key".to_string());
        }

        let chunk = Arc::new(chunk);
        let mut key_columns = Vec::<ArrayRef>::with_capacity(self.order_by.len());
        for sort_expr in &self.order_by {
            let key = arena
                .eval(sort_expr.expr, &chunk)
                .map_err(|e| e.to_string())?;
            key_columns.push(normalize_sort_key_array(&key)?);
        }

        if self.row_converter.is_none() {
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

    fn finish(self) -> Result<Option<Chunk>, String> {
        if self.heap.is_empty() {
            return Ok(None);
        }

        let mut selected = self.heap.into_vec();
        selected.sort_unstable();

        let schema = selected[0].chunk.schema();
        let row_batches = selected
            .into_iter()
            .map(|entry| entry.chunk.batch.slice(entry.row_idx, 1))
            .collect::<Vec<_>>();
        let batch = concat_batches(&schema, &row_batches).map_err(|e| e.to_string())?;
        let chunk = Chunk::try_new(batch).map_err(|e| e.to_string())?;
        Ok(Some(chunk))
    }
}

pub(crate) fn sort_chunks_topn_heap(
    arena: &ExprArena,
    order_by: &[SortExpression],
    rows_to_keep: usize,
    chunks: &[Chunk],
) -> Result<Option<Chunk>, String> {
    if rows_to_keep == 0 || chunks.is_empty() {
        return Ok(None);
    }
    if order_by.is_empty() {
        let schema = chunks[0].schema();
        let batches = chunks.iter().map(|c| c.batch.clone()).collect::<Vec<_>>();
        let batch = concat_batches(&schema, &batches).map_err(|e| e.to_string())?;
        let keep = rows_to_keep.min(batch.num_rows());
        if keep == 0 {
            return Ok(None);
        }
        return Chunk::try_new(batch.slice(0, keep))
            .map(Some)
            .map_err(|e| e.to_string());
    }

    let mut sorter = TopNHeapKernel::new(order_by, rows_to_keep);
    for chunk in chunks {
        sorter.update(arena, chunk.clone())?;
    }
    sorter.finish()
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
    fn sort_chunks(&self, chunks: &[Chunk]) -> Result<Option<Chunk>, String> {
        sort_chunks_topn_heap(
            self.arena.as_ref(),
            &self.order_by,
            self.rows_to_keep,
            chunks,
        )
    }
}
