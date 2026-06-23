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
//! Utility functions for hash-join probe output construction.
//!
//! Responsibilities:
//! - Keeps existing probe utility entrypoints for cross/keyed join callers.
//! - Delegates output gather and null-fill wrappers to `join_hash_map::gather` during migration.
//!
//! Key exported interfaces:
//! - Functions: `cross_join_chunk`, `keyed_join_chunk`, `lookup_group_ids`, `build_join_batch`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use super::join_hash_table::{JoinHashTable, row_has_forbidden_null};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::hash_table::key_builder::{
    GroupKeyArrayView, build_compressed_flags, build_group_key_hashes, build_group_key_views,
    build_one_number_hashes,
};
use crate::exec::hash_table::key_strategy::GroupKeyStrategy;
use crate::runtime::profile::{CounterRef, clamp_u128_to_i64};

#[inline]
fn record_timer_ns(timer: Option<&CounterRef>, start: std::time::Instant) {
    if let Some(timer) = timer {
        timer.add(clamp_u128_to_i64(start.elapsed().as_nanos()));
    }
}

/// Produce cross-join output rows by combining each left row with all right rows.
pub(crate) fn cross_join_chunk(
    left: &Chunk,
    right: &Chunk,
    output_schema: &SchemaRef,
) -> Result<Option<RecordBatch>, String> {
    let left_rows = left.len();
    let right_rows = right.len();
    if left_rows == 0 || right_rows == 0 {
        return Ok(None);
    }

    let mut left_indices = Vec::with_capacity(left_rows * right_rows);
    let mut right_indices = Vec::with_capacity(left_rows * right_rows);

    for l in 0..left_rows {
        for r in 0..right_rows {
            left_indices.push(l as u32);
            right_indices.push(r as u32);
        }
    }

    build_join_batch(left, right, &left_indices, &right_indices, output_schema)
}

/// Build joined rows from matched left/right row index pairs for keyed join probing.
pub(crate) fn keyed_join_chunk(
    arena: &ExprArena,
    probe_keys: &[ExprId],
    left: &Chunk,
    right_batches: &[Chunk],
    table: &JoinHashTable,
    output_schema: &SchemaRef,
    search_timer: Option<&CounterRef>,
    output_timer: Option<&CounterRef>,
) -> Result<Vec<RecordBatch>, String> {
    let left_len = left.len();
    if left_len == 0 {
        return Ok(Vec::new());
    }

    let mut probe_key_arrays = Vec::with_capacity(probe_keys.len());
    for expr in probe_keys {
        let array = arena.eval(*expr, left).map_err(|e| e.to_string())?;
        probe_key_arrays.push(array);
    }
    if right_batches.is_empty() {
        return Ok(Vec::new());
    }

    let key_views = build_group_key_views(&probe_key_arrays).map_err(|e| e.to_string())?;
    let nulls = build_nulls(&key_views, left_len, table.null_safe_eq());
    let search_start = std::time::Instant::now();
    let group_ids = match table.key_strategy() {
        GroupKeyStrategy::OneNumber => {
            let view = key_views
                .first()
                .ok_or_else(|| "join one number key view missing".to_string())?;
            let hashes = build_one_number_hashes(view, left_len, table.hash_seed())
                .map_err(|e| e.to_string())?;
            table
                .lookup_one_number_batch(view, &hashes, &nulls)
                .map_err(|e| e.to_string())?
        }
        GroupKeyStrategy::OneString => {
            let view = key_views
                .first()
                .ok_or_else(|| "join one string key view missing".to_string())?;
            let hashes = build_group_key_hashes(&key_views, left_len, table.hash_seed())
                .map_err(|e| e.to_string())?;
            table
                .lookup_one_string_batch(view, &hashes, &nulls)
                .map_err(|e| e.to_string())?
        }
        GroupKeyStrategy::FixedSize => {
            let hashes = build_group_key_hashes(&key_views, left_len, table.hash_seed())
                .map_err(|e| e.to_string())?;
            table
                .lookup_fixed_size_batch(&key_views, &hashes, &nulls)
                .map_err(|e| e.to_string())?
        }
        GroupKeyStrategy::CompressedFixed => {
            let ctx = table
                .compressed_ctx()
                .ok_or_else(|| "join compressed key context missing".to_string())?;
            let keys =
                build_compressed_flags(ctx, &key_views, left_len).map_err(|e| e.to_string())?;
            let hashes = build_group_key_hashes(&key_views, left_len, table.hash_seed())
                .map_err(|e| e.to_string())?;
            let need_rows = keys
                .iter()
                .zip(nulls.iter())
                .any(|(key, is_null)| !*is_null && !*key);
            let rows = if need_rows {
                Some(
                    table
                        .build_rows_or_fallback(&probe_key_arrays)
                        .map_err(|e| e.to_string())?,
                )
            } else {
                None
            };
            table
                .lookup_compressed_batch(&key_views, &keys, rows.as_ref(), &hashes, &nulls)
                .map_err(|e| e.to_string())?
        }
        GroupKeyStrategy::Serialized => {
            let rows = table
                .build_rows_or_fallback(&probe_key_arrays)
                .map_err(|e| e.to_string())?;
            let hashes = build_group_key_hashes(&key_views, left_len, table.hash_seed())
                .map_err(|e| e.to_string())?;
            table
                .lookup_serialized_batch(&rows, &hashes, &nulls)
                .map_err(|e| e.to_string())?
        }
        GroupKeyStrategy::Scalar => {
            return Err("join hash table does not support empty keys".to_string());
        }
    };
    record_timer_ns(search_timer, search_start);

    let mut counts_by_batch = vec![0usize; right_batches.len()];
    for group_id_opt in group_ids.iter() {
        let Some(group_id) = group_id_opt else {
            continue;
        };
        let rows = table
            .group_rows_slice(*group_id)
            .map_err(|e| e.to_string())?;
        for &right_row in rows {
            let (batch_idx, _row_idx) = table.row_location(right_row).map_err(|e| e.to_string())?;
            let batch_slot = batch_idx as usize;
            if batch_slot >= counts_by_batch.len() {
                return Err("join row batch index out of bounds".to_string());
            }
            counts_by_batch[batch_slot] = counts_by_batch[batch_slot]
                .checked_add(1)
                .ok_or_else(|| "join row batch count overflow".to_string())?;
        }
    }

    let mut left_indices_by_batch = Vec::with_capacity(right_batches.len());
    let mut right_indices_by_batch = Vec::with_capacity(right_batches.len());
    for count in &counts_by_batch {
        if *count == 0 {
            left_indices_by_batch.push(Vec::new());
            right_indices_by_batch.push(Vec::new());
        } else {
            left_indices_by_batch.push(vec![0u32; *count]);
            right_indices_by_batch.push(vec![0u32; *count]);
        }
    }

    let mut write_pos = vec![0usize; right_batches.len()];
    for (row, group_id_opt) in group_ids.iter().enumerate() {
        let Some(group_id) = group_id_opt else {
            continue;
        };
        let rows = table
            .group_rows_slice(*group_id)
            .map_err(|e| e.to_string())?;
        for &right_row in rows {
            let (batch_idx, row_idx) = table.row_location(right_row).map_err(|e| e.to_string())?;
            let batch_slot = batch_idx as usize;
            if batch_slot >= left_indices_by_batch.len() {
                return Err("join row batch index out of bounds".to_string());
            }
            let slot = write_pos[batch_slot];
            if slot >= left_indices_by_batch[batch_slot].len() {
                return Err("join row batch slot out of bounds".to_string());
            }
            left_indices_by_batch[batch_slot][slot] = row as u32;
            right_indices_by_batch[batch_slot][slot] = row_idx;
            write_pos[batch_slot] = slot + 1;
        }
    }

    let mut output_batches = Vec::new();
    for (batch_idx, right) in right_batches.iter().enumerate() {
        let left_indices = &left_indices_by_batch[batch_idx];
        if left_indices.is_empty() {
            continue;
        }
        let right_indices = &right_indices_by_batch[batch_idx];
        let output_start = std::time::Instant::now();
        let batches = build_join_batches(left, right, left_indices, right_indices, output_schema)?;
        record_timer_ns(output_timer, output_start);
        output_batches.extend(batches);
    }

    Ok(output_batches)
}

/// Lookup hash-table group ids for probe rows and report matched/unmatched classification.
pub(crate) fn lookup_group_ids(
    arena: &ExprArena,
    probe_keys: &[ExprId],
    probe: &Chunk,
    table: &JoinHashTable,
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
        let array = arena.eval(*expr, probe).map_err(|e| e.to_string())?;
        probe_key_arrays.push(array);
    }

    let key_views = build_group_key_views(&probe_key_arrays).map_err(|e| e.to_string())?;
    let nulls = build_nulls(&key_views, probe_len, table.null_safe_eq());

    let group_ids = match table.key_strategy() {
        GroupKeyStrategy::OneNumber => {
            let view = key_views
                .first()
                .ok_or_else(|| "join one number key view missing".to_string())?;
            let hashes = build_one_number_hashes(view, probe_len, table.hash_seed())
                .map_err(|e| e.to_string())?;
            table
                .lookup_one_number_batch(view, &hashes, &nulls)
                .map_err(|e| e.to_string())?
        }
        GroupKeyStrategy::OneString => {
            let view = key_views
                .first()
                .ok_or_else(|| "join one string key view missing".to_string())?;
            let hashes = build_group_key_hashes(&key_views, probe_len, table.hash_seed())
                .map_err(|e| e.to_string())?;
            table
                .lookup_one_string_batch(view, &hashes, &nulls)
                .map_err(|e| e.to_string())?
        }
        GroupKeyStrategy::FixedSize => {
            let hashes = build_group_key_hashes(&key_views, probe_len, table.hash_seed())
                .map_err(|e| e.to_string())?;
            table
                .lookup_fixed_size_batch(&key_views, &hashes, &nulls)
                .map_err(|e| e.to_string())?
        }
        GroupKeyStrategy::CompressedFixed => {
            let ctx = table
                .compressed_ctx()
                .ok_or_else(|| "join compressed key context missing".to_string())?;
            let keys =
                build_compressed_flags(ctx, &key_views, probe_len).map_err(|e| e.to_string())?;
            let hashes = build_group_key_hashes(&key_views, probe_len, table.hash_seed())
                .map_err(|e| e.to_string())?;
            let need_rows = keys
                .iter()
                .zip(nulls.iter())
                .any(|(key, is_null)| !*is_null && !*key);
            let rows = if need_rows {
                Some(
                    table
                        .build_rows_or_fallback(&probe_key_arrays)
                        .map_err(|e| e.to_string())?,
                )
            } else {
                None
            };
            table
                .lookup_compressed_batch(&key_views, &keys, rows.as_ref(), &hashes, &nulls)
                .map_err(|e| e.to_string())?
        }
        GroupKeyStrategy::Serialized => {
            let rows = table
                .build_rows_or_fallback(&probe_key_arrays)
                .map_err(|e| e.to_string())?;
            let hashes = build_group_key_hashes(&key_views, probe_len, table.hash_seed())
                .map_err(|e| e.to_string())?;
            table
                .lookup_serialized_batch(&rows, &hashes, &nulls)
                .map_err(|e| e.to_string())?
        }
        GroupKeyStrategy::Scalar => {
            return Err("join hash table does not support empty keys".to_string());
        }
    };

    Ok(group_ids)
}

/// Assemble one joined output batch from matched row id pairs and requested schemas.
pub(crate) fn build_join_batch(
    left: &Chunk,
    right: &Chunk,
    left_indices: &[u32],
    right_indices: &[u32],
    output_schema: &SchemaRef,
) -> Result<Option<RecordBatch>, String> {
    super::join_hash_map::gather::gather_join_batch(
        left,
        right,
        left_indices,
        right_indices,
        output_schema,
    )
}

pub(crate) fn build_join_batches(
    left: &Chunk,
    right: &Chunk,
    left_indices: &[u32],
    right_indices: &[u32],
    output_schema: &SchemaRef,
) -> Result<Vec<RecordBatch>, String> {
    super::join_hash_map::gather::gather_join_batches(
        left,
        right,
        left_indices,
        right_indices,
        output_schema,
    )
}

/// Build left-preserving rows with null-filled right side for outer/semi join paths.
pub(crate) fn build_left_with_null_right(
    left: &Chunk,
    left_indices: &[u32],
    right_schema: &SchemaRef,
    output_schema: &SchemaRef,
) -> Result<Option<RecordBatch>, String> {
    super::join_hash_map::gather::gather_left_with_null_right(
        left,
        left_indices,
        right_schema,
        output_schema,
    )
}

/// Build right-preserving rows with null-filled left side for right/full join paths.
pub(crate) fn build_null_left_with_right(
    right: &Chunk,
    right_indices: &[u32],
    left_schema: &SchemaRef,
    output_schema: &SchemaRef,
) -> Result<Option<RecordBatch>, String> {
    super::join_hash_map::gather::gather_null_left_with_right(
        right,
        right_indices,
        left_schema,
        output_schema,
    )
}

fn build_nulls(views: &[GroupKeyArrayView<'_>], len: usize, null_safe_eq: &[bool]) -> Vec<bool> {
    let mut nulls = Vec::with_capacity(len);
    for row in 0..len {
        nulls.push(row_has_forbidden_null(views, row, null_safe_eq));
    }
    nulls
}

/// Concatenate left and right schemas into the joined output schema order.
pub(crate) fn concat_schemas(left: SchemaRef, right: SchemaRef) -> SchemaRef {
    super::join_hash_map::gather::concat_schemas(left, right)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;

    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, ChunkSchema};

    use super::super::join_hash_map::gather::MAX_JOIN_OUTPUT_ROWS_PER_BATCH;
    use super::build_join_batches;

    fn one_column_chunk(name: &str, slot_id: SlotId, values: Vec<i32>) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, false)]));
        let array = Arc::new(Int32Array::from(values)) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[slot_id])
                .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    fn join_schema(left_name: &str, right_name: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(left_name, DataType::Int32, false),
            Field::new(right_name, DataType::Int32, false),
        ]))
    }

    #[test]
    fn build_join_batches_splits_large_candidate_output() {
        let rows = MAX_JOIN_OUTPUT_ROWS_PER_BATCH + 1;
        let left = one_column_chunk("l", SlotId::new(1), (0..rows).map(|i| i as i32).collect());
        let right = one_column_chunk("r", SlotId::new(2), vec![7]);
        let left_indices = (0..rows).map(|i| i as u32).collect::<Vec<_>>();
        let right_indices = vec![0u32; rows];

        let batches = build_join_batches(
            &left,
            &right,
            &left_indices,
            &right_indices,
            &join_schema("l", "r"),
        )
        .expect("join batches");

        assert_eq!(batches.len(), 2);
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            rows
        );
    }
}
