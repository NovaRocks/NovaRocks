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
use arrow::array::{
    Array, ArrayRef, Decimal128Array, Decimal256Array, FixedSizeBinaryArray, Int8Array, ListArray,
    MapArray, StructArray, UInt64Array, make_array,
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

fn is_compatible_sort_field_type(expected: &DataType, actual: &DataType) -> bool {
    match (expected, actual) {
        (DataType::Decimal128(_, expected_scale), DataType::Decimal128(_, actual_scale)) => {
            expected_scale == actual_scale
        }
        (DataType::Decimal256(_, expected_scale), DataType::Decimal256(_, actual_scale)) => {
            expected_scale == actual_scale
        }
        _ => expected == actual,
    }
}

fn sort_payload_data_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Decimal128(_, scale) => DataType::Decimal128(38, *scale),
        DataType::Decimal256(_, scale) => DataType::Decimal256(76, *scale),
        _ => data_type.clone(),
    }
}

fn sort_field_from_array(field: &Field, array: &ArrayRef) -> Field {
    Field::new(
        field.name(),
        sort_payload_data_type(array.data_type()),
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
                || !is_compatible_sort_field_type(expected.data_type(), actual.data_type())
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

fn retag_decimal128_array_for_sort(
    array: &Decimal128Array,
    target_precision: u8,
    target_scale: i8,
) -> Result<ArrayRef, String> {
    let data = array
        .to_data()
        .into_builder()
        .data_type(DataType::Decimal128(target_precision, target_scale))
        .build()
        .map_err(|e| e.to_string())?;
    Ok(make_array(data))
}

fn retag_decimal256_array_for_sort(
    array: &Decimal256Array,
    target_precision: u8,
    target_scale: i8,
) -> Result<ArrayRef, String> {
    let data = array
        .to_data()
        .into_builder()
        .data_type(DataType::Decimal256(target_precision, target_scale))
        .build()
        .map_err(|e| e.to_string())?;
    Ok(make_array(data))
}

fn normalize_sort_array_for_field(array: &ArrayRef, field: &Field) -> Result<ArrayRef, String> {
    if array.data_type() == field.data_type() {
        return Ok(array.clone());
    }
    match (array.data_type(), field.data_type()) {
        (DataType::Decimal128(_, source_scale), DataType::Decimal128(precision, target_scale))
            if source_scale == target_scale =>
        {
            let decimal = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "sort Decimal128 payload downcast failed".to_string())?;
            retag_decimal128_array_for_sort(decimal, *precision, *target_scale)
        }
        (DataType::Decimal256(_, source_scale), DataType::Decimal256(precision, target_scale))
            if source_scale == target_scale =>
        {
            let decimal = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| "sort Decimal256 payload downcast failed".to_string())?;
            retag_decimal256_array_for_sort(decimal, *precision, *target_scale)
        }
        _ => Err(format!(
            "sort payload type mismatch for field {}: array={:?} field={:?}",
            field.name(),
            array.data_type(),
            field.data_type()
        )),
    }
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

pub(crate) use chunks_sorter_full_sort::ChunksSorterFullSort;
pub(crate) use chunks_sorter_heap_sort::ChunksSorterHeapSort;
pub(crate) use chunks_sorter_topn::ChunksSorterTopN;
pub use sort_processor::SortProcessorFactory;
pub(crate) use spillable_chunks_sorter::SpillableChunksSorter;
