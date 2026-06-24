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
//! - Keeps cross join output construction reusable for probe paths.
//!
//! Key exported interfaces:
//! - Functions: `cross_join_batches`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::exec::chunk::Chunk;

pub(crate) use super::join_hash_map::gather::{
    MAX_JOIN_OUTPUT_ROWS_PER_BATCH, gather_join_batch, gather_left_with_null_right,
    gather_null_left_with_right,
};

/// Produce cross-join output rows by combining each left row with all right rows.
pub(crate) fn cross_join_batches(
    left: &Chunk,
    right: &Chunk,
    output_schema: &SchemaRef,
) -> Result<Vec<RecordBatch>, String> {
    let left_rows = left.len();
    let right_rows = right.len();
    if left_rows == 0 || right_rows == 0 {
        return Ok(Vec::new());
    }

    let initial_capacity = left_rows
        .saturating_mul(right_rows)
        .min(MAX_JOIN_OUTPUT_ROWS_PER_BATCH);
    let mut left_indices = Vec::with_capacity(initial_capacity);
    let mut right_indices = Vec::with_capacity(initial_capacity);
    let mut output_batches = Vec::new();

    for l in 0..left_rows {
        let left_row = u32::try_from(l).map_err(|_| "join left row id overflow".to_string())?;
        for r in 0..right_rows {
            left_indices.push(left_row);
            right_indices
                .push(u32::try_from(r).map_err(|_| "join right row id overflow".to_string())?);
            if left_indices.len() == MAX_JOIN_OUTPUT_ROWS_PER_BATCH {
                if let Some(batch) =
                    gather_join_batch(left, right, &left_indices, &right_indices, output_schema)?
                {
                    output_batches.push(batch);
                }
                left_indices.clear();
                right_indices.clear();
            }
        }
    }

    if !left_indices.is_empty() {
        if let Some(batch) =
            gather_join_batch(left, right, &left_indices, &right_indices, output_schema)?
        {
            output_batches.push(batch);
        }
    }

    Ok(output_batches)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, ChunkSchema};

    use super::{MAX_JOIN_OUTPUT_ROWS_PER_BATCH, cross_join_batches};

    fn one_column_chunk(name: &str, slot_id: SlotId, values: Vec<i32>) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, false)]));
        let array = Arc::new(Int32Array::from(values)) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[slot_id])
                .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    #[test]
    fn cross_join_batches_splits_large_candidate_output() {
        let left = one_column_chunk(
            "l",
            SlotId::new(1),
            (0..(MAX_JOIN_OUTPUT_ROWS_PER_BATCH + 1))
                .map(|i| i as i32)
                .collect(),
        );
        let right = one_column_chunk("r", SlotId::new(2), vec![7]);
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("l", DataType::Int32, false),
            Field::new("r", DataType::Int32, false),
        ]));

        let batches = cross_join_batches(&left, &right, &output_schema).expect("cross join");

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), MAX_JOIN_OUTPUT_ROWS_PER_BATCH);
        assert_eq!(batches[1].num_rows(), 1);
    }
}
