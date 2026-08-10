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
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::exec::chunk::Chunk;
use crate::exec::chunk::type_compatibility::{check_exact, retag_column};
use crate::runtime::profile::clamp_u128_to_i64;

pub(crate) const MAX_JOIN_OUTPUT_ROWS_PER_BATCH: usize = 16 * 1024;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct GatherTimings {
    pub(crate) build_ns: i64,
    pub(crate) probe_ns: i64,
}

#[derive(Debug)]
pub(crate) struct GatherBatches {
    pub(crate) batches: Vec<RecordBatch>,
    pub(crate) timings: GatherTimings,
}

impl GatherTimings {
    fn add_build(&mut self, start: std::time::Instant) {
        self.build_ns = self
            .build_ns
            .saturating_add(clamp_u128_to_i64(start.elapsed().as_nanos()));
    }

    fn add_probe(&mut self, start: std::time::Instant) {
        self.probe_ns = self
            .probe_ns
            .saturating_add(clamp_u128_to_i64(start.elapsed().as_nanos()));
    }
}

fn normalize_columns_to_schema(
    output_schema: &SchemaRef,
    columns: Vec<ArrayRef>,
    context: &str,
) -> Result<Vec<ArrayRef>, String> {
    if output_schema.fields().len() != columns.len() {
        return Err(format!(
            "{context} column count mismatch: schema_fields={} arrays={}",
            output_schema.fields().len(),
            columns.len()
        ));
    }

    let mut normalized = Vec::with_capacity(columns.len());
    for (idx, column) in columns.into_iter().enumerate() {
        let descriptor = output_schema.field(idx).data_type();
        if descriptor == column.data_type() {
            normalized.push(column);
            continue;
        }

        let actual = column.data_type().clone();
        if let Err(_mismatch) = check_exact(descriptor, &actual) {
            return Err(format!(
                "{context} type mismatch at column {idx}: descriptor={:?} actual={:?}",
                descriptor, actual,
            ));
        }

        let column = retag_column(&column, descriptor).map_err(|mismatch| {
            format!(
                "{context} type retag failed at column {idx}: descriptor={:?} actual={:?} mismatch={:?}",
                descriptor, actual, mismatch
            )
        })?;
        normalized.push(column);
    }
    Ok(normalized)
}

fn build_output_record_batch(
    output_schema: &SchemaRef,
    columns: Vec<ArrayRef>,
    context: &str,
) -> Result<RecordBatch, String> {
    let columns = normalize_columns_to_schema(output_schema, columns, context)?;
    let schema = output_schema_for_columns(output_schema, &columns);
    RecordBatch::try_new(schema, columns).map_err(|e| e.to_string())
}

fn output_schema_for_columns(output_schema: &SchemaRef, columns: &[ArrayRef]) -> SchemaRef {
    let mut changed = false;
    let fields = output_schema
        .fields()
        .iter()
        .zip(columns.iter())
        .map(|(field, column)| {
            if !field.is_nullable() && column.null_count() > 0 {
                changed = true;
                field.as_ref().clone().with_nullable(true)
            } else {
                field.as_ref().clone()
            }
        })
        .collect::<Vec<_>>();
    if !changed {
        return Arc::clone(output_schema);
    }
    Arc::new(Schema::new_with_metadata(
        fields,
        output_schema.metadata().clone(),
    ))
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

pub(crate) fn gather_probe_build_batches(
    probe: &Chunk,
    build: &Chunk,
    probe_indices: &[u32],
    build_indices: &[u32],
    output_schema: &SchemaRef,
    probe_is_left: bool,
    all_match_one: bool,
) -> Result<GatherBatches, String> {
    if probe_indices.len() != build_indices.len() {
        return Err(format!(
            "join index length mismatch: probe={} build={}",
            probe_indices.len(),
            build_indices.len()
        ));
    }
    if probe_indices.is_empty() {
        return Ok(GatherBatches {
            batches: Vec::new(),
            timings: GatherTimings::default(),
        });
    }

    if all_match_one {
        for (row, probe_row) in probe_indices.iter().enumerate() {
            if *probe_row != row as u32 {
                return Err(format!(
                    "ALL_MATCH_ONE probe index mismatch: row={} probe_row={}",
                    row, probe_row
                ));
            }
        }
    }

    let mut batches = Vec::new();
    let mut timings = GatherTimings::default();
    let mut offset = 0usize;
    while offset < probe_indices.len() {
        let end = (offset + MAX_JOIN_OUTPUT_ROWS_PER_BATCH).min(probe_indices.len());
        let row_count = end - offset;
        let build_idx_array = UInt32Array::from(build_indices[offset..end].to_vec());
        let build_idx_ref = Arc::new(build_idx_array) as ArrayRef;
        let mut columns = Vec::with_capacity(probe.batch.num_columns() + build.batch.num_columns());
        let take_probe_columns = |columns: &mut Vec<ArrayRef>,
                                  timings: &mut GatherTimings|
         -> Result<(), String> {
            if all_match_one {
                if probe.batch.num_columns() > 0 {
                    let start = std::time::Instant::now();
                    for col in probe.batch.columns() {
                        columns.push(col.slice(offset, row_count));
                    }
                    timings.add_probe(start);
                }
            } else {
                let probe_idx_array = UInt32Array::from(probe_indices[offset..end].to_vec());
                let probe_idx_ref = Arc::new(probe_idx_array) as ArrayRef;
                if probe.batch.num_columns() > 0 {
                    let start = std::time::Instant::now();
                    for col in probe.batch.columns() {
                        columns.push(
                            take(col.as_ref(), &probe_idx_ref, None).map_err(|e| e.to_string())?,
                        );
                    }
                    timings.add_probe(start);
                }
            }
            Ok(())
        };
        let take_build_columns = |columns: &mut Vec<ArrayRef>,
                                  timings: &mut GatherTimings|
         -> Result<(), String> {
            if build.batch.num_columns() > 0 {
                let start = std::time::Instant::now();
                for col in build.batch.columns() {
                    columns
                        .push(take(col.as_ref(), &build_idx_ref, None).map_err(|e| e.to_string())?);
                }
                timings.add_build(start);
            }
            Ok(())
        };

        if probe_is_left {
            take_probe_columns(&mut columns, &mut timings)?;
            take_build_columns(&mut columns, &mut timings)?;
        } else {
            take_build_columns(&mut columns, &mut timings)?;
            take_probe_columns(&mut columns, &mut timings)?;
        }
        batches.push(build_output_record_batch(
            output_schema,
            columns,
            "join all-match-one output",
        )?);
        offset = end;
    }
    Ok(GatherBatches { batches, timings })
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
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, Decimal128Array, Int32Array, ListArray};
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::{RecordBatch, RecordBatchOptions};

    use crate::exec::chunk::{Chunk, ChunkSchema};
    use novarocks_types::SlotId;

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

    fn decimal_chunk(name: &str, slot_id: SlotId, precision: u8, scale: i8) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new(
            name,
            DataType::Decimal128(precision, scale),
            false,
        )]));
        let array = Arc::new(
            Decimal128Array::from(vec![Some(123_i128)])
                .with_precision_and_scale(precision, scale)
                .expect("decimal type"),
        ) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[slot_id])
                .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    fn empty_chunk(row_count: usize) -> Chunk {
        let schema = Arc::new(Schema::empty());
        let options = RecordBatchOptions::new().with_row_count(Some(row_count));
        let batch = RecordBatch::try_new_with_options(Arc::clone(&schema), Vec::new(), &options)
            .expect("empty record batch");
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[])
            .expect("empty chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    fn join_schema(left_name: &str, right_name: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(left_name, DataType::Int32, true),
            Field::new(right_name, DataType::Int32, true),
        ]))
    }

    fn int32_values(batch: &RecordBatch, column: usize) -> Vec<Option<i32>> {
        let array = batch
            .column(column)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 column");
        (0..batch.num_rows())
            .map(|row| {
                if array.is_null(row) {
                    None
                } else {
                    Some(array.value(row))
                }
            })
            .collect()
    }

    #[test]
    fn gather_probe_build_batches_directs_probe_when_all_match_one() {
        let left = one_column_chunk("l", SlotId::new(1), vec![1, 2, 3]);
        let right = one_column_chunk("r", SlotId::new(2), vec![10, 20, 30]);
        let output_schema = join_schema("l", "r");
        let probe_indices = vec![0, 1, 2];
        let build_indices = vec![2, 0, 1];

        let out = super::gather_probe_build_batches(
            &left,
            &right,
            &probe_indices,
            &build_indices,
            &output_schema,
            true,
            true,
        )
        .expect("gather");

        assert_eq!(out.batches.len(), 1);
        assert_eq!(out.batches[0].num_rows(), 3);
        assert_eq!(
            int32_values(&out.batches[0], 0),
            vec![Some(1), Some(2), Some(3)]
        );
        assert_eq!(
            int32_values(&out.batches[0], 1),
            vec![Some(30), Some(10), Some(20)]
        );
    }

    #[test]
    fn gather_probe_build_batches_preserves_output_order_when_probe_is_right() {
        let probe = one_column_chunk("r", SlotId::new(2), vec![10, 20, 30]);
        let build = one_column_chunk("l", SlotId::new(1), vec![1, 2, 3]);
        let output_schema = join_schema("l", "r");
        let probe_indices = vec![0, 1, 2];
        let build_indices = vec![2, 0, 1];

        let out = super::gather_probe_build_batches(
            &probe,
            &build,
            &probe_indices,
            &build_indices,
            &output_schema,
            false,
            true,
        )
        .expect("gather");

        assert_eq!(out.batches.len(), 1);
        assert_eq!(out.batches[0].num_rows(), 3);
        assert_eq!(
            int32_values(&out.batches[0], 0),
            vec![Some(3), Some(1), Some(2)]
        );
        assert_eq!(
            int32_values(&out.batches[0], 1),
            vec![Some(10), Some(20), Some(30)]
        );
    }

    #[test]
    fn gather_probe_build_batches_does_not_charge_empty_build_side_when_all_match_one() {
        let probe = one_column_chunk("l", SlotId::new(1), vec![1, 2, 3]);
        let build = empty_chunk(3);
        let output_schema = Arc::new(Schema::new(vec![Field::new("l", DataType::Int32, true)]));
        let probe_indices = vec![0, 1, 2];
        let build_indices = vec![0, 1, 2];

        let out = super::gather_probe_build_batches(
            &probe,
            &build,
            &probe_indices,
            &build_indices,
            &output_schema,
            true,
            true,
        )
        .expect("gather");

        assert_eq!(out.batches.len(), 1);
        assert_eq!(out.batches[0].num_rows(), 3);
        assert_eq!(out.timings.build_ns, 0);
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
    fn gather_join_batch_rejects_decimal_precision_drift() {
        let left = decimal_chunk("l", SlotId::new(1), 38, 2);
        let right = one_column_chunk("r", SlotId::new(2), vec![7]);
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("l", DataType::Decimal128(10, 2), false),
            Field::new("r", DataType::Int32, true),
        ]));

        let err = super::gather_join_batch(&left, &right, &[0], &[0], &output_schema)
            .expect_err("join output must reject actual-widen decimal");

        assert!(
            err.contains("join output type mismatch at column 0"),
            "err={err}"
        );
        assert!(err.contains("Decimal128(10, 2)"), "err={err}");
        assert!(err.contains("Decimal128(38, 2)"), "err={err}");
    }

    #[test]
    fn build_output_record_batch_retags_nested_metadata_to_descriptor() {
        let actual_item = Arc::new(Field::new("item", DataType::Int32, true).with_metadata(
            HashMap::from([("PARQUET:field_id".to_string(), "3".to_string())]),
        ));
        let actual = Arc::new(ListArray::new(
            actual_item,
            OffsetBuffer::from_lengths([2]),
            Arc::new(Int32Array::from(vec![1, 2])),
            None::<NullBuffer>,
        )) as ArrayRef;
        let descriptor_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "arr",
            descriptor_type.clone(),
            true,
        )]));

        let batch = super::build_output_record_batch(&output_schema, vec![actual], "join output")
            .expect("retagged output");

        assert_eq!(batch.schema().field(0).data_type(), &descriptor_type);
        assert_eq!(batch.column(0).data_type(), &descriptor_type);
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
    fn gather_left_with_null_right_widens_null_filled_output_field() {
        let left = one_column_chunk("l", SlotId::new(1), vec![10, 20, 30, 40]);
        let right_schema = Arc::new(Schema::new(vec![Field::new("r", DataType::Int32, false)]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("l", DataType::Int32, false),
            Field::new("r", DataType::Int32, false),
        ]));
        let left_indices = vec![3, 0, 3];

        let batch =
            gather_left_with_null_right(&left, &left_indices, &right_schema, &output_schema)
                .expect("left null right")
                .expect("batch");

        assert!(!batch.schema().field(0).is_nullable());
        assert!(batch.schema().field(1).is_nullable());
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
