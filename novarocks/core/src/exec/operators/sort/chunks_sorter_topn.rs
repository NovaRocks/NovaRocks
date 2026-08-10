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
use crate::exec::operators::analytic_shared::{compute_partitions, row_equal_on_keys};
use crate::exec::operators::sort::chunks_sorter_heap_sort::sort_chunks_topn_heap;
use crate::exec::operators::sort::sort_processor::rank_like_cutoff;
use crate::exec::operators::sort::{ChunksSorter, concat_sort_chunks};
use crate::exec::operators::sort::{
    append_stable_row_index_sort_column, merged_sort_schema_for_chunks,
    normalize_sort_batch_for_schema, normalize_sort_key_array,
};

use arrow::array::{ArrayRef, UInt32Array};
use arrow::compute::{SortColumn, SortOptions, lexsort_to_indices, take};
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
        let batch = concat_sort_chunks(chunks)?;
        let keep = rows_to_keep.min(batch.num_rows());
        if keep == 0 {
            return Ok(None);
        }
        return Chunk::try_new_like(batch.slice(0, keep), &chunks[0])
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
        let batch = concat_sort_chunks(chunks)?;
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        return Chunk::try_new_like(batch, &chunks[0])
            .map(Some)
            .map_err(|e| e.to_string());
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
        let batch = concat_sort_chunks(chunks)?;
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        return Chunk::try_new_like(batch, &chunks[0])
            .map(Some)
            .map_err(|e| e.to_string());
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

/// Per-partition rank-TopN sorter.
///
/// Groups input rows by `partition_exprs`, then within each group keeps the top
/// `partition_limit` rows according to `topn_type` and `order_by`.
pub(crate) struct ChunksSorterPartitionTopN {
    arena: Arc<ExprArena>,
    partition_exprs: Vec<SortExpression>,
    order_by: Vec<SortExpression>,
    topn_type: SortTopNType,
    partition_limit: usize,
}

impl ChunksSorterPartitionTopN {
    pub(crate) fn new(
        arena: Arc<ExprArena>,
        partition_exprs: Vec<SortExpression>,
        order_by: Vec<SortExpression>,
        topn_type: SortTopNType,
        partition_limit: usize,
    ) -> Self {
        Self {
            arena,
            partition_exprs,
            order_by,
            topn_type,
            partition_limit,
        }
    }
}

impl ChunksSorter for ChunksSorterPartitionTopN {
    fn sort_chunks(&self, chunks: &[Chunk]) -> Result<Option<Chunk>, String> {
        sort_chunks_partition_topn(
            self.arena.as_ref(),
            &self.partition_exprs,
            &self.order_by,
            self.topn_type,
            self.partition_limit,
            chunks,
        )
    }
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
    let mut sort_columns: Vec<SortColumn> = key_columns
        .iter()
        .zip(order_by.iter())
        .map(|(values, expr)| SortColumn {
            values: values.clone(),
            options: Some(SortOptions {
                descending: !expr.asc,
                nulls_first: expr.nulls_first,
            }),
        })
        .collect();
    if let Some(first) = key_columns.first() {
        append_stable_row_index_sort_column(&mut sort_columns, first.len());
    }
    sort_columns
}

fn sort_chunks_by_order(
    arena: &ExprArena,
    order_by: &[SortExpression],
    chunks: &[Chunk],
) -> Result<Chunk, String> {
    if chunks.is_empty() {
        return Err("sort_chunks_by_order requires non-empty chunks".to_string());
    }
    let batch = concat_sort_chunks(chunks)?;
    if batch.num_rows() == 0 || order_by.is_empty() {
        return Chunk::try_new_like(batch, &chunks[0]).map_err(|e| e.to_string());
    }

    let key_chunk = Chunk::new_like(batch.clone(), &chunks[0]);
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
    Chunk::try_new_like(sorted_batch, &chunks[0]).map_err(|e| e.to_string())
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
    take_rows(chunk, &indices)
}

/// Take selected rows from `chunk` by absolute row indices.
///
/// Returns `None` if `indices` is empty; returns a clone of `chunk` if all
/// rows are selected; otherwise performs a physical take and rebuilds the chunk.
fn take_rows(chunk: &Chunk, indices: &[u32]) -> Result<Option<Chunk>, String> {
    if indices.is_empty() {
        return Ok(None);
    }
    if indices.len() == chunk.len() {
        return Ok(Some(chunk.clone()));
    }
    let selection = UInt32Array::from(indices.to_vec());
    let schema = merged_sort_schema_for_chunks(std::slice::from_ref(chunk))?;
    let batch = normalize_sort_batch_for_schema(chunk, &schema, 0)?;
    let columns = batch
        .columns()
        .iter()
        .map(|col| take(col.as_ref(), &selection, None))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    let filtered = RecordBatch::try_new(schema, columns).map_err(|e| e.to_string())?;
    Chunk::try_new_like(filtered, chunk)
        .map(Some)
        .map_err(|e| e.to_string())
}

/// Per-partition rank-TopN.
///
/// Sorts by `(partition_exprs ASC, order_by)` so rows of the same partition are
/// adjacent and ordered, then keeps, within each partition segment, the rows
/// whose `RowNumber`/`Rank`/`DenseRank` is `<= partition_limit`.
///
/// Output stays sorted by `(partition, order)`. Empty `partition_exprs` behaves
/// like a single global group, identical to `sort_chunks_rank`/`sort_chunks_topn`.
pub(crate) fn sort_chunks_partition_topn(
    arena: &ExprArena,
    partition_exprs: &[SortExpression],
    order_by: &[SortExpression],
    topn_type: SortTopNType,
    partition_limit: usize,
    chunks: &[Chunk],
) -> Result<Option<Chunk>, String> {
    if partition_limit == 0 || chunks.is_empty() {
        return Ok(None);
    }
    // Build a combined sort key: partition keys first (preserving their sort direction), then order keys.
    let mut combined: Vec<SortExpression> = partition_exprs.to_vec();
    combined.extend_from_slice(order_by);
    let sorted = sort_chunks_by_order(arena, &combined, chunks)?;
    if sorted.is_empty() {
        return Ok(None);
    }
    // Materialize partition-key columns from the sorted chunk for grouping.
    let part_keys = eval_order_by_columns(arena, partition_exprs, &sorted)?;
    // Materialize order-key columns for peer-equality comparisons inside each partition.
    let order_keys = eval_order_by_columns(arena, order_by, &sorted)?;
    let partitions = compute_partitions(&part_keys, sorted.len())?;
    let mut keep_indices = Vec::<u32>::new();
    for (start, end) in partitions {
        let seg_rows = end - start;
        // peer_equal compares ORDER-BY columns at ABSOLUTE indices within the sorted chunk.
        let cutoff = rank_like_cutoff(topn_type, partition_limit, seg_rows, |a, b| {
            row_equal_on_keys(&order_keys, start + a, start + b).unwrap_or(false)
        });
        for local in 0..cutoff {
            keep_indices.push(
                u32::try_from(start + local).map_err(|_| {
                    format!("row index {} exceeds UInt32Array range", start + local)
                })?,
            );
        }
    }
    if keep_indices.is_empty() {
        return Ok(None);
    }
    if keep_indices.len() == sorted.len() {
        return Ok(Some(sorted));
    }
    take_rows(&sorted, &keep_indices)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::{ExprArena, ExprNode};
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;
    use std::sync::Arc;

    /// Build a two-column Chunk: col 0 = partition key (SlotId 1), col 1 = order key (SlotId 2).
    fn make_two_col_chunk(p_values: Vec<Option<i32>>, o_values: Vec<Option<i32>>) -> Chunk {
        let schema = Arc::new(Schema::new(vec![
            Field::new("p", DataType::Int32, true),
            Field::new("o", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(p_values)),
                Arc::new(Int32Array::from(o_values)),
            ],
        )
        .expect("record batch");
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1), SlotId::new(2)],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    /// Build partition + order SortExpressions for the two-column chunk.
    fn two_col_sort_exprs(
        asc: bool,
        nulls_first: bool,
    ) -> (ExprArena, Vec<SortExpression>, Vec<SortExpression>) {
        let mut arena = ExprArena::default();
        let p_expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let o_expr = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);
        let partition_exprs = vec![SortExpression {
            expr: p_expr,
            asc,
            nulls_first,
        }];
        let order_exprs = vec![SortExpression {
            expr: o_expr,
            asc,
            nulls_first,
        }];
        (arena, partition_exprs, order_exprs)
    }

    fn collect_col_i32(chunk: &Chunk, col_idx: usize) -> Vec<Option<i32>> {
        let col = chunk
            .batch
            .column(col_idx)
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
    fn partition_topn_row_number_keeps_exactly_k_per_partition() {
        // p=[1,1,2,2,2], o=[10,20,5,6,7]; limit=1 (ROW_NUMBER)
        // Partition 1: row 10 kept (row_number=1 <= 1)
        // Partition 2: row 5 kept (row_number=1 <= 1)
        let (arena, partition_exprs, order_by) = two_col_sort_exprs(true, true);
        let chunks = vec![make_two_col_chunk(
            vec![Some(1), Some(1), Some(2), Some(2), Some(2)],
            vec![Some(10), Some(20), Some(5), Some(6), Some(7)],
        )];

        let out = sort_chunks_partition_topn(
            &arena,
            &partition_exprs,
            &order_by,
            SortTopNType::RowNumber,
            1,
            &chunks,
        )
        .expect("partition_topn")
        .expect("non-empty result");

        // Sorted by (partition, order): p=1 → o=10, p=2 → o=5
        assert_eq!(out.len(), 2);
        assert_eq!(collect_col_i32(&out, 1), vec![Some(10), Some(5)]);
    }

    #[test]
    fn partition_topn_dense_rank_keeps_distinct_peer_groups_per_partition() {
        // p=[1,1,1,1,2,2,2], o=[10,10,20,30,5,5,7]
        // Partition 1: dense_ranks = [1,1,2,3] → dense_rank<=2 keeps [10,10,20] (3 rows)
        // Partition 2: dense_ranks = [1,1,2] → dense_rank<=2 keeps [5,5,7] (3 rows)
        let (arena, partition_exprs, order_by) = two_col_sort_exprs(true, true);
        let chunks = vec![make_two_col_chunk(
            vec![
                Some(1),
                Some(1),
                Some(1),
                Some(1),
                Some(2),
                Some(2),
                Some(2),
            ],
            vec![
                Some(10),
                Some(10),
                Some(20),
                Some(30),
                Some(5),
                Some(5),
                Some(7),
            ],
        )];

        let out = sort_chunks_partition_topn(
            &arena,
            &partition_exprs,
            &order_by,
            SortTopNType::DenseRank,
            2,
            &chunks,
        )
        .expect("partition_topn")
        .expect("non-empty result");

        assert_eq!(out.len(), 6);
        // Partition 1 o=[10,10,20], Partition 2 o=[5,5,7]
        assert_eq!(
            collect_col_i32(&out, 1),
            vec![Some(10), Some(10), Some(20), Some(5), Some(5), Some(7)]
        );
    }

    #[test]
    fn partition_topn_null_partition_key_groups_nulls_together() {
        // p=[None,None,1], o=[5,10,20]
        // NULL partition key rows grouped together: rank<=1 → o=5 kept from null-group
        // Partition p=1: rank<=1 → o=20 kept
        let (arena, partition_exprs, order_by) = two_col_sort_exprs(true, true);
        let chunks = vec![make_two_col_chunk(
            vec![None, None, Some(1)],
            vec![Some(5), Some(10), Some(20)],
        )];

        let out = sort_chunks_partition_topn(
            &arena,
            &partition_exprs,
            &order_by,
            SortTopNType::RowNumber,
            1,
            &chunks,
        )
        .expect("partition_topn")
        .expect("non-empty result");

        // Null-key group keeps first row (o=5), p=1 group keeps o=20
        assert_eq!(out.len(), 2);
        let o_vals = collect_col_i32(&out, 1);
        assert!(
            o_vals.contains(&Some(5)),
            "expected o=5 from null-key partition, got {:?}",
            o_vals
        );
        assert!(
            o_vals.contains(&Some(20)),
            "expected o=20 from p=1 partition, got {:?}",
            o_vals
        );
    }

    #[test]
    fn partition_topn_empty_partition_exprs_matches_global() {
        // Empty partition_exprs → one global group; same result as sort_chunks_rank global.
        let mut arena = ExprArena::default();
        let o_expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let order_by = vec![SortExpression {
            expr: o_expr,
            asc: true,
            nulls_first: true,
        }];

        let chunks = vec![make_chunk(vec![
            Some(10),
            Some(10),
            Some(9),
            Some(8),
            Some(8),
            Some(7),
        ])];

        // Global rank with limit=4 keeps [7,8,8,9,10,10] minus rank>4 → [7,8,8,9] = 4 rows
        // rank sequence asc: [1,2,2,4,4,6] → rank<=4 keeps rows 7,8,8,9 (5 rows incl boundary ties)
        // Actually: sorted asc = [7,8,8,9,10,10], rank = [1,2,2,4,4,6]
        // rank<=4 keeps [7,8,8,9] = 4 rows
        let global_out = sort_chunks_rank(&arena, &order_by, 4, &chunks)
            .expect("global rank")
            .expect("non-empty");

        let partition_out = sort_chunks_partition_topn(
            &arena,
            &[], // no partition keys → one global group
            &order_by,
            SortTopNType::Rank,
            4,
            &chunks,
        )
        .expect("partition_topn")
        .expect("non-empty result");

        assert_eq!(partition_out.len(), global_out.len());
        assert_eq!(collect_i32(&partition_out), collect_i32(&global_out));
    }

    #[test]
    fn partition_topn_resets_rank_per_partition() {
        // p=[1,1,1,2,2,2], o=[10,20,30,5,6,7]
        // After partition sort: p=[1,1,1,2,2,2], o=[10,20,30,5,6,7] (already sorted)
        // Partition 1 ranks: 1,2,3 → keep rank<=2 → rows (1,10),(1,20)
        // Partition 2 ranks: 1,2,3 → keep rank<=2 → rows (2,5),(2,6)
        // Expected o values: [10,20,5,6]
        let (arena, partition_exprs, order_by) = two_col_sort_exprs(true, true);
        let chunks = vec![make_two_col_chunk(
            vec![Some(1), Some(1), Some(1), Some(2), Some(2), Some(2)],
            vec![Some(10), Some(20), Some(30), Some(5), Some(6), Some(7)],
        )];

        let out = sort_chunks_partition_topn(
            &arena,
            &partition_exprs,
            &order_by,
            SortTopNType::Rank,
            2,
            &chunks,
        )
        .expect("partition_topn")
        .expect("non-empty result");

        assert_eq!(out.len(), 4);
        assert_eq!(
            collect_col_i32(&out, 1),
            vec![Some(10), Some(20), Some(5), Some(6)]
        );
    }

    #[test]
    fn partition_topn_limit_exceeds_partition_size_keeps_entire_partition() {
        // p=[1,1,2,2,2], o=[10,20,5,6,7], Rank, partition_limit=10
        // Partition 1 has 2 rows (< 10); all 2 rows must be kept.
        // Partition 2 has 3 rows (< 10); all 3 rows must be kept.
        let (arena, partition_exprs, order_by) = two_col_sort_exprs(true, true);
        let chunks = vec![make_two_col_chunk(
            vec![Some(1), Some(1), Some(2), Some(2), Some(2)],
            vec![Some(10), Some(20), Some(5), Some(6), Some(7)],
        )];

        let out = sort_chunks_partition_topn(
            &arena,
            &partition_exprs,
            &order_by,
            SortTopNType::Rank,
            10,
            &chunks,
        )
        .expect("partition_topn")
        .expect("non-empty result");

        // All 5 rows must be kept — the limit is larger than either partition.
        assert_eq!(out.len(), 5);
        assert_eq!(
            collect_col_i32(&out, 1),
            vec![Some(10), Some(20), Some(5), Some(6), Some(7)]
        );
    }

    #[test]
    fn partition_topn_handles_multiple_input_chunks() {
        // Two separate Chunks; rows of the same partition are split across them:
        //   chunk1: (p=1,o=10), (p=2,o=5)
        //   chunk2: (p=1,o=20), (p=1,o=30), (p=2,o=6)
        // Rank limit=2:
        //   Partition 1: sorted o=[10,20,30] → rank sequence [1,2,3] → keep [10,20]
        //   Partition 2: sorted o=[5,6]      → rank sequence [1,2]   → keep [5,6]
        let (arena, partition_exprs, order_by) = two_col_sort_exprs(true, true);
        let chunk1 = make_two_col_chunk(vec![Some(1), Some(2)], vec![Some(10), Some(5)]);
        let chunk2 = make_two_col_chunk(
            vec![Some(1), Some(1), Some(2)],
            vec![Some(20), Some(30), Some(6)],
        );
        let chunks = vec![chunk1, chunk2];

        let out = sort_chunks_partition_topn(
            &arena,
            &partition_exprs,
            &order_by,
            SortTopNType::Rank,
            2,
            &chunks,
        )
        .expect("partition_topn")
        .expect("non-empty result");

        assert_eq!(out.len(), 4);
        assert_eq!(
            collect_col_i32(&out, 1),
            vec![Some(10), Some(20), Some(5), Some(6)]
        );
    }

    fn make_chunk(values: Vec<Option<i32>>) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))])
            .expect("record batch");
        {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        }
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
