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
    Array, ArrayRef, Decimal128Array, FixedSizeBinaryArray, Int8Array, ListArray, MapArray,
    StructArray,
};
use arrow::datatypes::{DataType, Field, Fields};
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

pub(crate) use chunks_sorter_full_sort::ChunksSorterFullSort;
pub(crate) use chunks_sorter_heap_sort::ChunksSorterHeapSort;
pub(crate) use chunks_sorter_topn::ChunksSorterTopN;
pub use sort_processor::SortProcessorFactory;
pub(crate) use spillable_chunks_sorter::SpillableChunksSorter;
