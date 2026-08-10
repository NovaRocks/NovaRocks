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

//! Storage-agnostic global-dictionary encode helpers.
//!
//! They are shared here so the Iceberg/HDFS Parquet scan path can reuse them
//! without creating a `formats → connector` dependency cycle.

use crate::exec::chunk::ChunkSchemaRef;
use arrow::array::{Array, ArrayRef, Int32Builder, LargeStringArray, ListArray, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use novarocks_types::SlotId;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

/// Maps a slot id to the global-dictionary encode map for that slot.
///
/// Each inner map translates a raw UTF-8 byte string to its integer dict-id.
pub type QueryGlobalDictEncodeMap = HashMap<SlotId, Arc<HashMap<Vec<u8>, i32>>>;

fn is_integer_dict_code_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

pub fn build_scan_schema_for_global_dict_encoding(
    output_schema: &SchemaRef,
    output_chunk_schema: &ChunkSchemaRef,
    query_global_dicts: &QueryGlobalDictEncodeMap,
) -> Result<(SchemaRef, bool), String> {
    if query_global_dicts.is_empty() {
        return Ok((output_schema.clone(), false));
    }
    if output_schema.fields().len() != output_chunk_schema.slots().len() {
        return Err(format!(
            "output schema/chunk schema length mismatch while building dict scan schema: fields={} slots={}",
            output_schema.fields().len(),
            output_chunk_schema.slots().len()
        ));
    }
    let mut fields = Vec::with_capacity(output_schema.fields().len());
    let mut changed = false;
    for (field_ref, slot) in output_schema
        .fields()
        .iter()
        .zip(output_chunk_schema.slots().iter())
    {
        let field = field_ref.as_ref();
        let slot_id = slot.slot_id();
        let needs_dict_encode = query_global_dicts.contains_key(&slot_id);
        if needs_dict_encode {
            if let Some(scan_type) = dict_scan_data_type_for_output(field.data_type()) {
                changed = true;
                info!(
                    "starrocks native dict scan type rewrite: field={} slot_id={:?} output_type={:?} scan_type={:?}",
                    field.name(),
                    Some(slot_id),
                    field.data_type(),
                    scan_type
                );
                fields.push(Arc::new(field.clone().with_data_type(scan_type)));
                continue;
            }
            info!(
                "starrocks native dict scan type rewrite skipped: field={} slot_id={:?} output_type={:?}",
                field.name(),
                Some(slot_id),
                field.data_type()
            );
        }
        fields.push(field_ref.clone());
    }
    if !changed {
        return Ok((output_schema.clone(), false));
    }
    let scan_schema = Arc::new(Schema::new(fields));
    Ok((scan_schema, true))
}

pub fn encode_batch_with_query_global_dicts(
    scan_batch: RecordBatch,
    output_schema: &SchemaRef,
    output_chunk_schema: &ChunkSchemaRef,
    query_global_dicts: &QueryGlobalDictEncodeMap,
) -> Result<RecordBatch, String> {
    if query_global_dicts.is_empty() {
        return Ok(scan_batch);
    }
    if scan_batch.num_columns() != output_schema.fields().len() {
        return Err(format!(
            "native starrocks dict encode output column mismatch: scan_columns={}, output_columns={}",
            scan_batch.num_columns(),
            output_schema.fields().len()
        ));
    }
    if output_schema.fields().len() != output_chunk_schema.slots().len() {
        return Err(format!(
            "output schema/chunk schema length mismatch while dict-encoding native batch: fields={} slots={}",
            output_schema.fields().len(),
            output_chunk_schema.slots().len()
        ));
    }
    let mut arrays = Vec::with_capacity(scan_batch.num_columns());
    for ((idx, field_ref), slot) in output_schema
        .fields()
        .iter()
        .enumerate()
        .zip(output_chunk_schema.slots().iter())
    {
        let output_field = field_ref.as_ref();
        let slot_id = slot.slot_id();
        let Some(dict_map) = query_global_dicts.get(&slot_id) else {
            arrays.push(scan_batch.column(idx).clone());
            continue;
        };
        let encoded = encode_column_to_dict_ids(
            scan_batch.column(idx),
            output_field.data_type(),
            dict_map,
            output_field.name(),
            slot_id,
        )?;
        arrays.push(encoded);
    }
    RecordBatch::try_new(output_schema.clone(), arrays)
        .map_err(|e| format!("build dict-encoded native starrocks batch failed: {e}"))
}

fn encode_utf8_column_to_dict_ids(
    array: &ArrayRef,
    output_type: &DataType,
    dict_map: &HashMap<Vec<u8>, i32>,
    output_name: &str,
    slot_id: SlotId,
) -> Result<ArrayRef, String> {
    let mut builder = Int32Builder::with_capacity(array.len());
    let mut non_null_count = 0usize;
    match array.data_type() {
        DataType::Utf8 => {
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "downcast Utf8 array for dict encode failed".to_string())?;
            for row in 0..values.len() {
                if values.is_null(row) {
                    builder.append_null();
                } else {
                    non_null_count += 1;
                    let code = lookup_global_dict_code(
                        dict_map,
                        values.value(row).as_bytes(),
                        slot_id,
                        output_name,
                    )?;
                    builder.append_value(code);
                }
            }
        }
        DataType::LargeUtf8 => {
            let values = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| "downcast LargeUtf8 array for dict encode failed".to_string())?;
            for row in 0..values.len() {
                if values.is_null(row) {
                    builder.append_null();
                } else {
                    non_null_count += 1;
                    let code = lookup_global_dict_code(
                        dict_map,
                        values.value(row).as_bytes(),
                        slot_id,
                        output_name,
                    )?;
                    builder.append_value(code);
                }
            }
        }
        other => {
            return Err(format!(
                "native starrocks dict encode expects Utf8 source column: slot_id={}, output_column={}, source_type={:?}",
                slot_id, output_name, other
            ));
        }
    }
    if non_null_count > 0 {
        info!(
            "starrocks global dict encode stats: slot_id={} output_column={} non_null={} dict_size={}",
            slot_id,
            output_name,
            non_null_count,
            dict_map.len()
        );
    }
    let encoded_i32: ArrayRef = Arc::new(builder.finish());
    if output_type == &DataType::Int32 {
        return Ok(encoded_i32);
    }
    cast(encoded_i32.as_ref(), output_type).map_err(|e| {
        format!(
            "cast dict-encoded column to output type failed: slot_id={}, output_column={}, output_type={:?}, error={}",
            slot_id, output_name, output_type, e
        )
    })
}

fn lookup_global_dict_code(
    dict_map: &HashMap<Vec<u8>, i32>,
    value: &[u8],
    slot_id: SlotId,
    output_name: &str,
) -> Result<i32, String> {
    dict_map.get(value).copied().ok_or_else(|| {
        let sample = String::from_utf8_lossy(value);
        format!(
            "value not found in query global dict: slot_id={}, output_column={}, value_sample='{}', dict_size={}",
            slot_id,
            output_name,
            sample,
            dict_map.len()
        )
    })
}

/// Map a dict-encoded output type to the type the scan must READ from storage:
/// an integer dict-code column (`Int32`, etc.) is stored as `Utf8`, and a
/// `List<int>` of dict codes is stored as `List<Utf8>`. Returns `None` for
/// types that are not a dict-code shape. Callers use this to rewrite a dict
/// column's read-side type before the storage read, then encode back to the
/// declared output type.
pub fn dict_scan_data_type_for_output(output_type: &DataType) -> Option<DataType> {
    if is_integer_dict_code_type(output_type) {
        return Some(DataType::Utf8);
    }
    match output_type {
        DataType::List(item) => {
            let scan_item = dict_scan_data_type_for_output(item.data_type())?;
            Some(DataType::List(Arc::new(
                item.as_ref().clone().with_data_type(scan_item),
            )))
        }
        _ => None,
    }
}

fn encode_column_to_dict_ids(
    array: &ArrayRef,
    output_type: &DataType,
    dict_map: &HashMap<Vec<u8>, i32>,
    output_name: &str,
    slot_id: SlotId,
) -> Result<ArrayRef, String> {
    if is_integer_dict_code_type(output_type) {
        return encode_utf8_column_to_dict_ids(array, output_type, dict_map, output_name, slot_id);
    }

    match output_type {
        DataType::List(output_item) => {
            let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                format!(
                    "native starrocks dict encode expects ListArray for output column '{}' (slot_id={}), got {:?}",
                    output_name,
                    slot_id,
                    array.data_type()
                )
            })?;
            let encoded_values = encode_column_to_dict_ids(
                &list.values().clone(),
                output_item.data_type(),
                dict_map,
                output_name,
                slot_id,
            )?;
            Ok(Arc::new(ListArray::new(
                output_item.clone(),
                list.offsets().clone(),
                encoded_values,
                list.nulls().cloned(),
            )))
        }
        _ => Ok(array.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::{QueryGlobalDictEncodeMap, encode_batch_with_query_global_dicts};
    use arrow::array::{Array, Int32Array, LargeStringArray, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
    use novarocks_types::SlotId;

    #[test]
    fn file_read_dictionary_adapter_maps_utf8_to_ids() {
        let schema = Arc::new(Schema::new(vec![Field::new("v1", DataType::Int32, true)]));
        let scan_schema = Arc::new(Schema::new(vec![Field::new("v1", DataType::Utf8, true)]));
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(7),
                Field::new("v1", DataType::Utf8, true),
                None,
                None,
            )])
            .expect("chunk schema"),
        );
        let scan_batch = RecordBatch::try_new(
            scan_schema,
            vec![Arc::new(StringArray::from(vec![Some("a"), None]))],
        )
        .expect("scan batch");
        let mut dict_values = HashMap::new();
        dict_values.insert(b"a".to_vec(), 11);
        let mut dict_map = QueryGlobalDictEncodeMap::new();
        dict_map.insert(SlotId::new(7), Arc::new(dict_values));

        let encoded =
            encode_batch_with_query_global_dicts(scan_batch, &schema, &chunk_schema, &dict_map)
                .expect("encode");
        let values = encoded
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 column");
        assert_eq!(values.value(0), 11);
        assert!(values.is_null(1));
    }

    #[test]
    fn encode_batch_with_query_global_dicts_rejects_missing_value() {
        let schema = Arc::new(Schema::new(vec![Field::new("v1", DataType::Int32, true)]));
        let scan_schema = Arc::new(Schema::new(vec![Field::new("v1", DataType::Utf8, true)]));
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(7),
                Field::new("v1", DataType::Utf8, true),
                None,
                None,
            )])
            .expect("chunk schema"),
        );
        let scan_batch = RecordBatch::try_new(
            scan_schema,
            vec![Arc::new(StringArray::from(vec![Some("missing")]))],
        )
        .expect("scan batch");
        let mut dict_values = HashMap::new();
        dict_values.insert(b"a".to_vec(), 11);
        let mut dict_map = QueryGlobalDictEncodeMap::new();
        dict_map.insert(SlotId::new(7), Arc::new(dict_values));

        let err =
            encode_batch_with_query_global_dicts(scan_batch, &schema, &chunk_schema, &dict_map)
                .unwrap_err();
        assert!(
            err.contains("value not found in query global dict")
                && err.contains("slot_id=7")
                && err.contains("output_column=v1"),
            "{err}"
        );
    }

    #[test]
    fn encode_batch_with_query_global_dicts_maps_large_utf8_to_ids() {
        let schema = Arc::new(Schema::new(vec![Field::new("v1", DataType::Int32, true)]));
        let scan_schema = Arc::new(Schema::new(vec![Field::new(
            "v1",
            DataType::LargeUtf8,
            true,
        )]));
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(7),
                Field::new("v1", DataType::LargeUtf8, true),
                None,
                None,
            )])
            .expect("chunk schema"),
        );
        let scan_batch = RecordBatch::try_new(
            scan_schema,
            vec![Arc::new(LargeStringArray::from(vec![Some("a"), None]))],
        )
        .expect("scan batch");
        let mut dict_values = HashMap::new();
        dict_values.insert(b"a".to_vec(), 11);
        let mut dict_map = QueryGlobalDictEncodeMap::new();
        dict_map.insert(SlotId::new(7), Arc::new(dict_values));

        let encoded =
            encode_batch_with_query_global_dicts(scan_batch, &schema, &chunk_schema, &dict_map)
                .expect("encode");
        let values = encoded
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 column");
        assert_eq!(values.value(0), 11);
        assert!(values.is_null(1));
    }

    #[test]
    fn encode_batch_with_query_global_dicts_rejects_missing_large_utf8_value() {
        let schema = Arc::new(Schema::new(vec![Field::new("v1", DataType::Int32, true)]));
        let scan_schema = Arc::new(Schema::new(vec![Field::new(
            "v1",
            DataType::LargeUtf8,
            true,
        )]));
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(7),
                Field::new("v1", DataType::LargeUtf8, true),
                None,
                None,
            )])
            .expect("chunk schema"),
        );
        let scan_batch = RecordBatch::try_new(
            scan_schema,
            vec![Arc::new(LargeStringArray::from(vec![Some("missing")]))],
        )
        .expect("scan batch");
        let mut dict_values = HashMap::new();
        dict_values.insert(b"a".to_vec(), 11);
        let mut dict_map = QueryGlobalDictEncodeMap::new();
        dict_map.insert(SlotId::new(7), Arc::new(dict_values));

        let err =
            encode_batch_with_query_global_dicts(scan_batch, &schema, &chunk_schema, &dict_map)
                .unwrap_err();
        assert!(
            err.contains("value not found in query global dict")
                && err.contains("slot_id=7")
                && err.contains("output_column=v1"),
            "{err}"
        );
    }
}
