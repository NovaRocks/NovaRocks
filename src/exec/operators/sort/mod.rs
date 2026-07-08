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
//! Sorter kernels used by sort operators.
//!
//! Responsibilities:
//! - Host reusable sorter implementations so the pipeline operator can choose
//!   full-sort vs. top-n behavior explicitly.
//! - Keep sorting algorithms isolated from operator state transitions.

use crate::exec::chunk::Chunk;
use crate::exec::chunk::type_compatibility::{check_exact, retag_column};
use arrow::array::{
    Array, ArrayRef, Decimal128Array, FixedSizeBinaryArray, Int8Array, ListArray, MapArray,
    StructArray, UInt64Array,
};
use arrow::compute::{SortColumn, SortOptions, concat_batches};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_buffer::OffsetBuffer;
use std::sync::Arc;

use crate::common::largeint;

mod chunks_sorter_full_sort;
mod chunks_sorter_heap_sort;
mod chunks_sorter_topn;
mod sort_processor;
mod spillable_chunks_sorter;

/// Shared sorter abstraction for sort/topn operator implementations.
pub(crate) trait ChunksSorter: Send + Sync {
    fn sort_chunks(&self, chunks: &[Chunk]) -> Result<Option<Chunk>, String>;
}

pub(crate) fn concat_sort_chunks(chunks: &[Chunk]) -> Result<RecordBatch, String> {
    if chunks.is_empty() {
        return Err("sort concat requires non-empty chunks".to_string());
    }
    let schema = merged_sort_schema_for_chunks(chunks)?;
    let batches = chunks
        .iter()
        .enumerate()
        .map(|(idx, chunk)| normalize_sort_batch_for_schema(chunk, &schema, idx))
        .collect::<Result<Vec<_>, _>>()?;
    concat_batches(&schema, &batches).map_err(|e| e.to_string())
}

fn sort_field_from_array(field: &Field, array: &ArrayRef) -> Field {
    Field::new(
        field.name(),
        array.data_type().clone(),
        field.is_nullable() || array.null_count() > 0,
    )
    .with_metadata(field.metadata().clone())
}

pub(crate) fn merged_sort_schema_for_chunks(chunks: &[Chunk]) -> Result<SchemaRef, String> {
    let first_schema = chunks
        .first()
        .ok_or_else(|| "sort schema merge requires non-empty chunks".to_string())?
        .schema();
    let field_count = first_schema.fields().len();
    let mut fields = first_schema
        .fields()
        .iter()
        .zip(chunks[0].batch.columns().iter())
        .map(|(field, array)| sort_field_from_array(field.as_ref(), array))
        .collect::<Vec<_>>();
    let mut changed = first_schema
        .fields()
        .iter()
        .zip(fields.iter())
        .any(|(original, merged)| original.as_ref() != merged);

    for chunk in chunks {
        let schema = chunk.schema();
        if schema.fields().len() != field_count {
            return Err(format!(
                "sort schema field count mismatch: expected={} actual={}",
                field_count,
                schema.fields().len()
            ));
        }
        for idx in 0..field_count {
            let expected = &fields[idx];
            let actual = schema.field(idx);
            let actual = sort_field_from_array(actual, chunk.batch.column(idx));
            if expected.name() != actual.name()
                || check_exact(expected.data_type(), actual.data_type()).is_err()
            {
                return Err(format!(
                    "sort schema field mismatch at index {}: expected=({}, {:?}) actual=({}, {:?})",
                    idx,
                    expected.name(),
                    expected.data_type(),
                    actual.name(),
                    actual.data_type()
                ));
            }
            let nullable = expected.is_nullable()
                || actual.is_nullable()
                || chunk.batch.column(idx).null_count() > 0;
            if nullable != expected.is_nullable() {
                fields[idx] = expected.clone().with_nullable(nullable);
                changed = true;
            }
        }
    }

    if !changed {
        return Ok(first_schema);
    }
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        first_schema.metadata().clone(),
    )))
}

fn normalize_sort_array_for_field(array: &ArrayRef, field: &Field) -> Result<ArrayRef, String> {
    if let Err(mismatch) = check_exact(field.data_type(), array.data_type()) {
        return Err(format!(
            "sort payload type mismatch for field {}: array={:?} field={:?} ({:?})",
            field.name(),
            array.data_type(),
            field.data_type(),
            mismatch.kind
        ));
    }
    retag_column(array, field.data_type()).map_err(|m| {
        format!(
            "sort payload retag failed for field {}: array={:?} field={:?} ({:?})",
            field.name(),
            array.data_type(),
            field.data_type(),
            m.kind
        )
    })
}

pub(crate) fn normalize_sort_batch_for_schema(
    chunk: &Chunk,
    schema: &SchemaRef,
    chunk_index: usize,
) -> Result<RecordBatch, String> {
    if chunk.batch.num_columns() != schema.fields().len() {
        return Err(format!(
            "sort chunk column count mismatch at index {}: batch_columns={} schema_fields={}",
            chunk_index,
            chunk.batch.num_columns(),
            schema.fields().len()
        ));
    }
    let columns = chunk
        .batch
        .columns()
        .iter()
        .zip(schema.fields().iter())
        .map(|(array, field)| normalize_sort_array_for_field(array, field.as_ref()))
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(|e| {
        format!(
            "failed to normalize sort chunk schema at index {}: {e}",
            chunk_index
        )
    })
}

pub(crate) fn normalize_sort_key_array(values: &ArrayRef) -> Result<ArrayRef, String> {
    match values.data_type() {
        DataType::Null => {
            return Ok(Arc::new(Int8Array::from(vec![None::<i8>; values.len()])) as ArrayRef);
        }
        DataType::List(field) => {
            let list = values
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| "LIST sort key is not ListArray".to_string())?;
            let normalized_values = normalize_sort_key_array(list.values())?;
            if normalized_values.data_type() == list.values().data_type() {
                return Ok(values.clone());
            }
            let normalized_field = Arc::new(Field::new(
                field.name(),
                normalized_values.data_type().clone(),
                field.is_nullable(),
            ));
            let normalized = ListArray::new(
                normalized_field,
                OffsetBuffer::new(list.value_offsets().to_vec().into()),
                normalized_values,
                list.nulls().cloned(),
            );
            return Ok(Arc::new(normalized) as ArrayRef);
        }
        DataType::Struct(fields) => {
            let struct_arr = values
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| "STRUCT sort key is not StructArray".to_string())?;
            let mut changed = false;
            let mut normalized_columns = Vec::with_capacity(struct_arr.num_columns());
            for column in struct_arr.columns() {
                let normalized = normalize_sort_key_array(column)?;
                changed |= normalized.data_type() != column.data_type();
                normalized_columns.push(normalized);
            }
            if !changed {
                return Ok(values.clone());
            }
            let normalized_fields = Fields::from(
                fields
                    .iter()
                    .zip(normalized_columns.iter())
                    .map(|(field, column)| {
                        Arc::new(Field::new(
                            field.name(),
                            column.data_type().clone(),
                            field.is_nullable(),
                        ))
                    })
                    .collect::<Vec<_>>(),
            );
            let normalized = StructArray::new(
                normalized_fields,
                normalized_columns,
                struct_arr.nulls().cloned(),
            );
            return Ok(Arc::new(normalized) as ArrayRef);
        }
        DataType::Map(entries_field, ordered) => {
            let map = values
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| "MAP sort key is not MapArray".to_string())?;
            let entries = Arc::new(map.entries().clone()) as ArrayRef;
            let normalized_entries = normalize_sort_key_array(&entries)?;
            if normalized_entries.data_type() == entries.data_type() {
                return Ok(values.clone());
            }
            let normalized_entries = normalized_entries
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| "normalized MAP entries sort key is not StructArray".to_string())?
                .clone();
            let normalized_field = Arc::new(Field::new(
                entries_field.name(),
                DataType::Struct(normalized_entries.fields().clone()),
                entries_field.is_nullable(),
            ));
            let normalized = MapArray::new(
                normalized_field,
                OffsetBuffer::new(map.value_offsets().to_vec().into()),
                normalized_entries,
                map.nulls().cloned(),
                *ordered,
            );
            return Ok(Arc::new(normalized) as ArrayRef);
        }
        _ => {}
    }

    if !largeint::is_largeint_data_type(values.data_type()) {
        return Ok(values.clone());
    }
    let array = values
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| "LARGEINT sort key is not FixedSizeBinaryArray".to_string())?;
    if array.value_length() != largeint::LARGEINT_BYTE_WIDTH {
        return Err(format!(
            "LARGEINT sort key width mismatch: expected {}, got {}",
            largeint::LARGEINT_BYTE_WIDTH,
            array.value_length()
        ));
    }

    let mut decoded = Vec::with_capacity(array.len());
    for row in 0..array.len() {
        if array.is_null(row) {
            decoded.push(None);
        } else {
            decoded.push(Some(largeint::i128_from_be_bytes(array.value(row))?));
        }
    }

    let decimal = Decimal128Array::from(decoded)
        .with_precision_and_scale(38, 0)
        .map_err(|e| format!("normalize LARGEINT sort key failed: {e}"))?;
    Ok(Arc::new(decimal) as ArrayRef)
}

pub(crate) fn append_stable_row_index_sort_column(
    sort_columns: &mut Vec<SortColumn>,
    num_rows: usize,
) {
    let row_ids = UInt64Array::from_iter_values((0..num_rows).map(|idx| idx as u64));
    sort_columns.push(SortColumn {
        values: Arc::new(row_ids) as ArrayRef,
        options: Some(SortOptions {
            descending: false,
            nulls_first: true,
        }),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::ChunkSchema;
    use arrow::array::{Int32Builder, ListBuilder, StringArray};
    use arrow::compute::lexsort_to_indices;

    fn decimal_chunk(precision: u8, scale: i8, value: i128) -> Chunk {
        let data_type = DataType::Decimal128(precision, scale);
        let array = Decimal128Array::from(vec![Some(value)])
            .with_precision_and_scale(precision, scale)
            .expect("decimal array");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            data_type.clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array) as ArrayRef])
            .expect("record batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(1)])
                .expect("chunk schema");
        Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk")
    }

    #[test]
    fn merged_sort_schema_rejects_decimal_precision_drift() {
        let left = decimal_chunk(10, 2, 1234);
        let right = decimal_chunk(38, 2, 5678);

        let err = merged_sort_schema_for_chunks(&[left, right])
            .expect_err("sort concat must reject decimal precision drift");

        assert!(err.contains("sort schema field mismatch"), "err={err}");
        assert!(err.contains("Decimal128(10, 2)"), "err={err}");
        assert!(err.contains("Decimal128(38, 2)"), "err={err}");
    }

    #[test]
    fn normalize_sort_batch_rejects_utf8_binary_type_drift() {
        let source_schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&source_schema),
            vec![Arc::new(StringArray::from(vec![Some("abc")])) as ArrayRef],
        )
        .expect("record batch");
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            source_schema.as_ref(),
            &[SlotId::new(7)],
        )
        .expect("chunk schema");
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk");
        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Binary,
            true,
        )]));

        let err = normalize_sort_batch_for_schema(&chunk, &target_schema, 0)
            .expect_err("sort normalize must reject Utf8/Binary descriptor drift");

        assert!(err.contains("sort payload type mismatch"), "err={err}");
        assert!(err.contains("Utf8"), "err={err}");
        assert!(err.contains("Binary"), "err={err}");
    }

    fn append_inner_list(
        builder: &mut ListBuilder<ListBuilder<Int32Builder>>,
        values: &[Option<i32>],
    ) {
        for value in values {
            builder.values().values().append_option(*value);
        }
        builder.values().append(true);
    }

    #[test]
    fn complex_sort_keeps_parent_nulls_first_and_inner_nulls_first() {
        let mut builder = ListBuilder::new(ListBuilder::new(Int32Builder::new()));

        builder.append(false);

        append_inner_list(&mut builder, &[Some(1)]);
        builder.values().append(false);
        append_inner_list(&mut builder, &[Some(2)]);
        builder.append(true);

        append_inner_list(&mut builder, &[Some(1)]);
        append_inner_list(&mut builder, &[Some(2)]);
        builder.append(true);

        let values = Arc::new(builder.finish()) as ArrayRef;
        let values = normalize_sort_key_array(&values).expect("normalize sort key");
        let mut sort_columns = vec![SortColumn {
            values,
            options: Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
        }];
        append_stable_row_index_sort_column(&mut sort_columns, 3);

        let indices = lexsort_to_indices(&sort_columns, None).expect("sort indices");
        let actual = (0..indices.len())
            .map(|idx| indices.value(idx))
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![0, 1, 2]);
    }
}

pub(crate) use chunks_sorter_full_sort::ChunksSorterFullSort;
pub(crate) use chunks_sorter_heap_sort::ChunksSorterHeapSort;
pub(crate) use chunks_sorter_topn::{ChunksSorterPartitionTopN, ChunksSorterTopN};
pub use sort_processor::SortProcessorFactory;
pub(crate) use spillable_chunks_sorter::SpillableChunksSorter;
