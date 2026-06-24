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
//! Hash join output gather and null-fill construction helpers.

use std::sync::Arc;

use arrow::array::{ArrayRef, UInt32Array, new_null_array};
use arrow::compute::take;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::exec::chunk::Chunk;
use crate::exec::schema_compat::align_schema_to_arrays;

pub(crate) const MAX_JOIN_OUTPUT_ROWS_PER_BATCH: usize = 16 * 1024;

fn build_output_record_batch(
    output_schema: &SchemaRef,
    columns: Vec<ArrayRef>,
    context: &str,
) -> Result<RecordBatch, String> {
    let output_schema = align_schema_to_arrays(output_schema, &columns, context)?;
    RecordBatch::try_new(output_schema, columns).map_err(|e| e.to_string())
}

pub(crate) fn gather_join_batch(
    left: &Chunk,
    right: &Chunk,
    left_indices: &[u32],
    right_indices: &[u32],
    output_schema: &SchemaRef,
) -> Result<Option<RecordBatch>, String> {
    if left_indices.len() != right_indices.len() {
        return Err(format!(
            "join index length mismatch: left={} right={}",
            left_indices.len(),
            right_indices.len()
        ));
    }
    if left_indices.is_empty() || right_indices.is_empty() {
        return Ok(None);
    }
    let left_idx_array = UInt32Array::from(left_indices.to_vec());
    let right_idx_array = UInt32Array::from(right_indices.to_vec());
    let left_idx_ref = Arc::new(left_idx_array) as ArrayRef;
    let right_idx_ref = Arc::new(right_idx_array) as ArrayRef;

    let mut columns = Vec::with_capacity(left.batch.num_columns() + right.batch.num_columns());
    for col in left.batch.columns() {
        let taken = take(col.as_ref(), &left_idx_ref, None).map_err(|e| e.to_string())?;
        columns.push(taken);
    }
    for col in right.batch.columns() {
        let taken = take(col.as_ref(), &right_idx_ref, None).map_err(|e| e.to_string())?;
        columns.push(taken);
    }

    let batch = build_output_record_batch(output_schema, columns, "join output")?;
    Ok(Some(batch))
}

pub(crate) fn gather_join_batches(
    left: &Chunk,
    right: &Chunk,
    left_indices: &[u32],
    right_indices: &[u32],
    output_schema: &SchemaRef,
) -> Result<Vec<RecordBatch>, String> {
    if left_indices.len() != right_indices.len() {
        return Err(format!(
            "join index length mismatch: left={} right={}",
            left_indices.len(),
            right_indices.len()
        ));
    }
    if left_indices.is_empty() {
        return Ok(Vec::new());
    }

    let mut batches = Vec::new();
    let mut offset = 0usize;
    while offset < left_indices.len() {
        let end = (offset + MAX_JOIN_OUTPUT_ROWS_PER_BATCH).min(left_indices.len());
        if let Some(batch) = gather_join_batch(
            left,
            right,
            &left_indices[offset..end],
            &right_indices[offset..end],
            output_schema,
        )? {
            batches.push(batch);
        }
        offset = end;
    }
    Ok(batches)
}

pub(crate) fn gather_left_with_null_right(
    left: &Chunk,
    left_indices: &[u32],
    right_schema: &SchemaRef,
    output_schema: &SchemaRef,
) -> Result<Option<RecordBatch>, String> {
    if left_indices.is_empty() {
        return Ok(None);
    }
    let len = left_indices.len();
    let left_idx_array = UInt32Array::from(left_indices.to_vec());
    let left_idx_ref = Arc::new(left_idx_array) as ArrayRef;

    let mut columns = Vec::with_capacity(left.batch.num_columns() + right_schema.fields().len());
    for col in left.batch.columns() {
        let taken = take(col.as_ref(), &left_idx_ref, None).map_err(|e| e.to_string())?;
        columns.push(taken);
    }
    for field in right_schema.fields().iter() {
        columns.push(new_null_array(field.data_type(), len));
    }

    let batch = build_output_record_batch(output_schema, columns, "join left outer output")?;
    Ok(Some(batch))
}

pub(crate) fn gather_null_left_with_right(
    right: &Chunk,
    right_indices: &[u32],
    left_schema: &SchemaRef,
    output_schema: &SchemaRef,
) -> Result<Option<RecordBatch>, String> {
    if right_indices.is_empty() {
        return Ok(None);
    }
    let len = right_indices.len();
    let right_idx_array = UInt32Array::from(right_indices.to_vec());
    let right_idx_ref = Arc::new(right_idx_array) as ArrayRef;

    let mut columns = Vec::with_capacity(left_schema.fields().len() + right.batch.num_columns());
    for field in left_schema.fields().iter() {
        columns.push(new_null_array(field.data_type(), len));
    }
    for col in right.batch.columns() {
        let taken = take(col.as_ref(), &right_idx_ref, None).map_err(|e| e.to_string())?;
        columns.push(taken);
    }

    let batch = build_output_record_batch(output_schema, columns, "join right outer output")?;
    Ok(Some(batch))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;

    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, ChunkSchema};

    use super::{
        MAX_JOIN_OUTPUT_ROWS_PER_BATCH, gather_join_batches, gather_left_with_null_right,
        gather_null_left_with_right,
    };

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
            Field::new(right_name, DataType::Int32, true),
        ]))
    }

    #[test]
    fn gather_join_batches_splits_large_candidate_output() {
        let rows = MAX_JOIN_OUTPUT_ROWS_PER_BATCH + 1;
        let left = one_column_chunk("l", SlotId::new(1), (0..rows).map(|i| i as i32).collect());
        let right = one_column_chunk("r", SlotId::new(2), vec![7]);
        let left_indices = (0..rows).map(|i| i as u32).collect::<Vec<_>>();
        let right_indices = vec![0u32; rows];

        let batches = gather_join_batches(
            &left,
            &right,
            &left_indices,
            &right_indices,
            &join_schema("l", "r"),
        )
        .expect("join batches");

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), MAX_JOIN_OUTPUT_ROWS_PER_BATCH);
        assert_eq!(batches[1].num_rows(), 1);
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            rows
        );
    }

    #[test]
    fn gather_left_with_null_right_preserves_row_count() {
        let left = one_column_chunk("l", SlotId::new(1), vec![10, 20, 30, 40]);
        let right_schema = Arc::new(Schema::new(vec![Field::new("r", DataType::Int32, true)]));
        let left_indices = vec![3, 0, 3];

        let batch = gather_left_with_null_right(
            &left,
            &left_indices,
            &right_schema,
            &join_schema("l", "r"),
        )
        .expect("left null right")
        .expect("batch");

        assert_eq!(batch.num_rows(), left_indices.len());
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.column(1).len(), left_indices.len());
        assert_eq!(batch.column(1).null_count(), left_indices.len());
    }

    #[test]
    fn gather_null_left_with_right_preserves_row_count() {
        let right = one_column_chunk("r", SlotId::new(2), vec![100, 200, 300, 400]);
        let left_schema = Arc::new(Schema::new(vec![Field::new("l", DataType::Int32, true)]));
        let right_indices = vec![2, 1, 2];

        let batch = gather_null_left_with_right(
            &right,
            &right_indices,
            &left_schema,
            &join_schema("l", "r"),
        )
        .expect("null left right")
        .expect("batch");

        assert_eq!(batch.num_rows(), right_indices.len());
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.column(0).len(), right_indices.len());
        assert_eq!(batch.column(0).null_count(), right_indices.len());
    }
}
