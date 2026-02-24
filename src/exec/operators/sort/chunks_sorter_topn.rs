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
//! Top-n sorter kernels.
//!
//! This module hosts generic topn logic and rank-like semantics.
//! Heap specialization lives in `chunks_sorter_heap_sort`.

use std::cmp::Ordering;
use std::sync::Arc;

use crate::exec::chunk::Chunk;
use crate::exec::expr::ExprArena;
use crate::exec::node::sort::{SortExpression, SortTopNType};
use crate::exec::operators::sort::ChunksSorter;
use crate::exec::operators::sort::chunks_sorter_heap_sort::sort_chunks_topn_heap;
use crate::exec::operators::sort::normalize_sort_key_array;

use arrow::array::{ArrayRef, UInt32Array};
use arrow::compute::{SortColumn, SortOptions, concat_batches, lexsort_to_indices, take};
use arrow::record_batch::RecordBatch;
use arrow::row::{OwnedRow, RowConverter, SortField};

/// Build row-number topn output through boundary filtering + global sort.
///
/// Steps:
/// 1. Find the current top-k boundary with heap kernel.
/// 2. Keep rows `<= boundary_key`.
/// 3. Globally sort kept rows and truncate to `k`.
///
/// This keeps heap-sort specialization (`sort_chunks_topn_heap`) separate from
/// the large-k generic topn path.
pub(crate) fn sort_chunks_topn(
    arena: &ExprArena,
    order_by: &[SortExpression],
    rows_to_keep: usize,
    chunks: &[Chunk],
) -> Result<Option<Chunk>, String> {
    if rows_to_keep == 0 || chunks.is_empty() {
        return Ok(None);
    }
    let total_rows = chunks.iter().map(Chunk::len).sum::<usize>();
    let rows_to_keep = rows_to_keep.min(total_rows);
    if rows_to_keep == 0 {
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

    let Some(boundary_chunk) = sort_chunks_topn_heap(arena, order_by, rows_to_keep, chunks)? else {
        return Ok(None);
    };
    if boundary_chunk.is_empty() {
        return Ok(None);
    }
    let boundary_keys = eval_order_by_columns(arena, order_by, &boundary_chunk)?;
    let converter = build_row_converter(order_by, &boundary_keys)?;
    let boundary_rows = converter
        .convert_columns(&boundary_keys)
        .map_err(|e| e.to_string())?;
    let boundary_idx = boundary_chunk.len() - 1;
    let boundary_key = boundary_rows.row(boundary_idx).owned();

    let mut filtered = Vec::new();
    for chunk in chunks {
        if let Some(kept) =
            filter_chunk_by_boundary(arena, order_by, chunk, &converter, &boundary_key)?
        {
            filtered.push(kept);
        }
    }
    if filtered.is_empty() {
        return Ok(None);
    }

    let sorted = sort_chunks_by_order(arena, order_by, &filtered)?;
    if sorted.is_empty() {
        return Ok(None);
    }
    let keep = rows_to_keep.min(sorted.len());
    Ok(Some(sorted.slice(0, keep)))
}

/// Build rank-based topn output:
/// keep rows whose SQL `RANK()` is within `rank_limit`.
///
/// This preserves trailing ties at the boundary by:
/// 1. finding the row-number top `rank_limit` boundary key
/// 2. keeping all rows with key <= boundary key
/// 3. globally sorting the kept rows
pub(crate) fn sort_chunks_rank(
    arena: &ExprArena,
    order_by: &[SortExpression],
    rank_limit: usize,
    chunks: &[Chunk],
) -> Result<Option<Chunk>, String> {
    if rank_limit == 0 || chunks.is_empty() {
        return Ok(None);
    }
    if order_by.is_empty() {
        let schema = chunks[0].schema();
        let batches = chunks.iter().map(|c| c.batch.clone()).collect::<Vec<_>>();
        let batch = concat_batches(&schema, &batches).map_err(|e| e.to_string())?;
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        return Chunk::try_new(batch).map(Some).map_err(|e| e.to_string());
    }

    let Some(boundary_chunk) = sort_chunks_topn(arena, order_by, rank_limit, chunks)? else {
        return Ok(None);
    };
    if boundary_chunk.is_empty() {
        return Ok(None);
    }
    let boundary_keys = eval_order_by_columns(arena, order_by, &boundary_chunk)?;
    let converter = build_row_converter(order_by, &boundary_keys)?;
    let boundary_rows = converter
        .convert_columns(&boundary_keys)
        .map_err(|e| e.to_string())?;
    let boundary_idx = boundary_chunk.len() - 1;
    let boundary_key = boundary_rows.row(boundary_idx).owned();

    let mut filtered = Vec::new();
    for chunk in chunks {
        if let Some(kept) =
            filter_chunk_by_boundary(arena, order_by, chunk, &converter, &boundary_key)?
        {
            filtered.push(kept);
        }
    }
    if filtered.is_empty() {
        return Ok(None);
    }
    let sorted = sort_chunks_by_order(arena, order_by, &filtered)?;
    Ok(Some(sorted))
}

/// Build dense-rank-based topn output:
/// keep rows whose SQL `DENSE_RANK()` is within `rank_limit`.
///
/// Current StarRocks FE does not rewrite ranking-window queries to
/// `TOP-N type: DENSE_RANK` yet, but this path is kept for executor
/// completeness and direct plan coverage.
pub(crate) fn sort_chunks_dense_rank(
    arena: &ExprArena,
    order_by: &[SortExpression],
    rank_limit: usize,
    chunks: &[Chunk],
) -> Result<Option<Chunk>, String> {
    if rank_limit == 0 || chunks.is_empty() {
        return Ok(None);
    }
    if order_by.is_empty() {
        let schema = chunks[0].schema();
        let batches = chunks.iter().map(|c| c.batch.clone()).collect::<Vec<_>>();
        let batch = concat_batches(&schema, &batches).map_err(|e| e.to_string())?;
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        return Chunk::try_new(batch).map(Some).map_err(|e| e.to_string());
    }

    let sorted = sort_chunks_by_order(arena, order_by, chunks)?;
    if sorted.is_empty() {
        return Ok(None);
    }
    let key_columns = eval_order_by_columns(arena, order_by, &sorted)?;
    let converter = build_row_converter(order_by, &key_columns)?;
    let rows = converter
        .convert_columns(&key_columns)
        .map_err(|e| e.to_string())?;
    let mut dense_rank = 1usize;
    let mut cutoff = 0usize;
    for idx in 0..sorted.len() {
        if idx > 0 && rows.row(idx - 1) != rows.row(idx) {
            dense_rank = dense_rank.saturating_add(1);
        }
        if dense_rank > rank_limit {
            break;
        }
        cutoff = idx + 1;
    }
    if cutoff == 0 {
        return Ok(None);
    }
    Ok(Some(sorted.slice(0, cutoff)))
}

/// Topn sorter implementation that supports ROW_NUMBER, RANK and DENSE_RANK modes.
pub(crate) struct ChunksSorterTopN {
    arena: Arc<ExprArena>,
    order_by: Vec<SortExpression>,
    topn_type: SortTopNType,
    limit: usize,
}

impl ChunksSorterTopN {
    pub(crate) fn new(
        arena: Arc<ExprArena>,
        order_by: Vec<SortExpression>,
        topn_type: SortTopNType,
        limit: usize,
    ) -> Self {
        Self {
            arena,
            order_by,
            topn_type,
            limit,
        }
    }
}

impl ChunksSorter for ChunksSorterTopN {
    fn sort_chunks(&self, chunks: &[Chunk]) -> Result<Option<Chunk>, String> {
        match self.topn_type {
            SortTopNType::RowNumber => {
                sort_chunks_topn(self.arena.as_ref(), &self.order_by, self.limit, chunks)
            }
            SortTopNType::Rank => {
                sort_chunks_rank(self.arena.as_ref(), &self.order_by, self.limit, chunks)
            }
            SortTopNType::DenseRank => {
                sort_chunks_dense_rank(self.arena.as_ref(), &self.order_by, self.limit, chunks)
            }
        }
    }
}

fn eval_order_by_columns(
    arena: &ExprArena,
    order_by: &[SortExpression],
    chunk: &Chunk,
) -> Result<Vec<ArrayRef>, String> {
    let mut key_columns = Vec::with_capacity(order_by.len());
    for sort_expr in order_by {
        let key = arena
            .eval(sort_expr.expr, chunk)
            .map_err(|e| e.to_string())?;
        key_columns.push(normalize_sort_key_array(&key)?);
    }
    Ok(key_columns)
}

fn build_row_converter(
    order_by: &[SortExpression],
    key_columns: &[ArrayRef],
) -> Result<RowConverter, String> {
    let fields = key_columns
        .iter()
        .zip(order_by.iter())
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
    RowConverter::new(fields).map_err(|e| e.to_string())
}

fn build_sort_columns(order_by: &[SortExpression], key_columns: &[ArrayRef]) -> Vec<SortColumn> {
    key_columns
        .iter()
        .zip(order_by.iter())
        .map(|(values, expr)| SortColumn {
            values: values.clone(),
            options: Some(SortOptions {
                descending: !expr.asc,
                nulls_first: expr.nulls_first,
            }),
        })
        .collect()
}

fn sort_chunks_by_order(
    arena: &ExprArena,
    order_by: &[SortExpression],
    chunks: &[Chunk],
) -> Result<Chunk, String> {
    if chunks.is_empty() {
        return Err("sort_chunks_by_order requires non-empty chunks".to_string());
    }
    let schema = chunks[0].schema();
    let batches = chunks.iter().map(|c| c.batch.clone()).collect::<Vec<_>>();
    let batch = concat_batches(&schema, &batches).map_err(|e| e.to_string())?;
    if batch.num_rows() == 0 || order_by.is_empty() {
        return Chunk::try_new(batch).map_err(|e| e.to_string());
    }

    let key_chunk = Chunk::new(batch.clone());
    let key_columns = eval_order_by_columns(arena, order_by, &key_chunk)?;
    let sort_columns = build_sort_columns(order_by, &key_columns);
    let indices = lexsort_to_indices(&sort_columns, None).map_err(|e| e.to_string())?;
    let columns = batch
        .columns()
        .iter()
        .map(|col| take(col.as_ref(), &indices, None))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    let sorted_batch = RecordBatch::try_new(batch.schema(), columns).map_err(|e| e.to_string())?;
    Chunk::try_new(sorted_batch).map_err(|e| e.to_string())
}

fn filter_chunk_by_boundary(
    arena: &ExprArena,
    order_by: &[SortExpression],
    chunk: &Chunk,
    converter: &RowConverter,
    boundary_key: &OwnedRow,
) -> Result<Option<Chunk>, String> {
    if chunk.is_empty() {
        return Ok(None);
    }
    let key_columns = eval_order_by_columns(arena, order_by, chunk)?;
    let rows = converter
        .convert_columns(&key_columns)
        .map_err(|e| e.to_string())?;
    let mut indices = Vec::<u32>::new();
    for row_idx in 0..chunk.len() {
        if rows.row(row_idx).owned().cmp(boundary_key) != Ordering::Greater {
            let idx = u32::try_from(row_idx)
                .map_err(|_| format!("row index {} exceeds UInt32Array range", row_idx))?;
            indices.push(idx);
        }
    }
    if indices.is_empty() {
        return Ok(None);
    }
    if indices.len() == chunk.len() {
        return Ok(Some(chunk.clone()));
    }

    let selection = UInt32Array::from(indices);
    let columns = chunk
        .batch
        .columns()
        .iter()
        .map(|col| take(col.as_ref(), &selection, None))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    let filtered = RecordBatch::try_new(chunk.schema(), columns).map_err(|e| e.to_string())?;
    Chunk::try_new(filtered)
        .map(Some)
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::field_with_slot_id;
    use crate::exec::expr::{ExprArena, ExprNode};
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn make_chunk(values: Vec<Option<i32>>) -> Chunk {
        let schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("v", DataType::Int32, true),
            SlotId::new(1),
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))])
            .expect("record batch");
        Chunk::new(batch)
    }

    fn single_key_order_by(asc: bool, nulls_first: bool) -> (ExprArena, Vec<SortExpression>) {
        let mut arena = ExprArena::default();
        let expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let order_by = vec![SortExpression {
            expr,
            asc,
            nulls_first,
        }];
        (arena, order_by)
    }

    fn collect_i32(chunk: &Chunk) -> Vec<Option<i32>> {
        let col = chunk
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32");
        (0..col.len())
            .map(|i| {
                if col.is_null(i) {
                    None
                } else {
                    Some(col.value(i))
                }
            })
            .collect()
    }

    #[test]
    fn topn_ascending_keeps_smallest_rows() {
        let (arena, order_by) = single_key_order_by(true, true);
        let chunks = vec![
            make_chunk(vec![Some(7), Some(2), Some(5)]),
            make_chunk(vec![Some(1), Some(9), Some(3)]),
        ];

        let out = sort_chunks_topn(&arena, &order_by, 3, &chunks)
            .expect("topn")
            .expect("chunk");
        assert_eq!(collect_i32(&out), vec![Some(1), Some(2), Some(3)]);
    }

    #[test]
    fn topn_heap_kernel_keeps_smallest_rows() {
        let (arena, order_by) = single_key_order_by(true, true);
        let chunks = vec![
            make_chunk(vec![Some(7), Some(2), Some(5)]),
            make_chunk(vec![Some(1), Some(9), Some(3)]),
        ];

        let out = sort_chunks_topn_heap(&arena, &order_by, 3, &chunks)
            .expect("topn heap")
            .expect("chunk");
        assert_eq!(collect_i32(&out), vec![Some(1), Some(2), Some(3)]);
    }

    #[test]
    fn topn_descending_respects_nulls_last() {
        let (arena, order_by) = single_key_order_by(false, false);
        let chunks = vec![make_chunk(vec![
            Some(2),
            None,
            Some(9),
            Some(4),
            None,
            Some(7),
        ])];

        let out = sort_chunks_topn(&arena, &order_by, 4, &chunks)
            .expect("topn")
            .expect("chunk");
        assert_eq!(collect_i32(&out), vec![Some(9), Some(7), Some(4), Some(2)]);
    }

    #[test]
    fn rank_topn_expands_boundary_ties() {
        let (arena, order_by) = single_key_order_by(false, false);
        let chunks = vec![make_chunk(vec![
            Some(10),
            Some(10),
            Some(9),
            Some(8),
            Some(8),
            Some(7),
        ])];

        // Rank sequence is [1,1,3,4,4,6], so rank<=4 keeps five rows.
        let out = sort_chunks_rank(&arena, &order_by, 4, &chunks)
            .expect("rank topn")
            .expect("chunk");
        assert_eq!(
            collect_i32(&out),
            vec![Some(10), Some(10), Some(9), Some(8), Some(8)]
        );
    }

    #[test]
    fn rank_topn_with_empty_order_by_keeps_all_rows() {
        let mut arena = ExprArena::default();
        let _expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let chunks = vec![make_chunk(vec![Some(1), Some(2), Some(3)])];

        // Without ORDER BY keys, all rows are in one peer group (rank=1).
        let out = sort_chunks_rank(&arena, &[], 1, &chunks)
            .expect("rank topn")
            .expect("chunk");
        assert_eq!(collect_i32(&out), vec![Some(1), Some(2), Some(3)]);
    }

    #[test]
    fn dense_rank_topn_keeps_first_distinct_peer_groups() {
        let (arena, order_by) = single_key_order_by(false, false);
        let chunks = vec![make_chunk(vec![
            Some(10),
            Some(10),
            Some(9),
            Some(8),
            Some(8),
            Some(7),
        ])];

        // DENSE_RANK sequence is [1,1,2,3,3,4], so dense_rank<=2 keeps three rows.
        let out = sort_chunks_dense_rank(&arena, &order_by, 2, &chunks)
            .expect("dense rank topn")
            .expect("chunk");
        assert_eq!(collect_i32(&out), vec![Some(10), Some(10), Some(9)]);
    }

    #[test]
    fn dense_rank_topn_with_empty_order_by_keeps_all_rows() {
        let mut arena = ExprArena::default();
        let _expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let chunks = vec![make_chunk(vec![Some(1), Some(2), Some(3)])];

        // Without ORDER BY keys, all rows are in one peer group (dense_rank=1).
        let out = sort_chunks_dense_rank(&arena, &[], 1, &chunks)
            .expect("dense rank topn")
            .expect("chunk");
        assert_eq!(collect_i32(&out), vec![Some(1), Some(2), Some(3)]);
    }

    #[test]
    fn topn_rows_to_keep_larger_than_input_does_not_overflow() {
        let (arena, order_by) = single_key_order_by(true, true);
        let chunks = vec![make_chunk(vec![
            Some(5),
            Some(1),
            Some(3),
            Some(2),
            Some(4),
        ])];

        let out = sort_chunks_topn(&arena, &order_by, usize::MAX, &chunks)
            .expect("topn")
            .expect("chunk");
        assert_eq!(
            collect_i32(&out),
            vec![Some(1), Some(2), Some(3), Some(4), Some(5)]
        );
    }
}
