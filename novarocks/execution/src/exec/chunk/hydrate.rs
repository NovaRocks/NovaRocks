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

use std::sync::Arc;

use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use novarocks_types::SlotId;

use super::{Chunk, ChunkSchema};

pub(crate) fn assert_no_dictionary(batch: &RecordBatch) -> Result<(), String> {
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        if matches!(field.data_type(), DataType::Dictionary(_, _)) {
            return Err(format!(
                "dictionary column must be hydrated before flat boundary: column {} field {} has type {:?}",
                idx,
                field.name(),
                field.data_type()
            ));
        }
        if let Some(column) = batch.columns().get(idx) {
            if matches!(column.data_type(), DataType::Dictionary(_, _)) {
                return Err(format!(
                    "dictionary column must be hydrated before flat boundary: column {} field {} has array type {:?}",
                    idx,
                    field.name(),
                    column.data_type()
                ));
            }
        }
    }
    Ok(())
}

// Design: ADR-0005 (docs/adr/ADR-0005-low-cardinality-runtime-carrier-first.md)
pub(crate) fn hydrate_dictionary_columns(chunk: &Chunk) -> Result<Chunk, String> {
    hydrate_dictionary_columns_except(chunk, |_, _| false)
}

pub fn hydrate_dictionary_columns_except(
    chunk: &Chunk,
    keep_encoded: impl Fn(SlotId, &DataType) -> bool,
) -> Result<Chunk, String> {
    let mut changed = false;
    let mut columns = Vec::with_capacity(chunk.columns().len());
    let mut slots = Vec::with_capacity(chunk.chunk_schema().slots().len());

    for (idx, ((column, field), slot)) in chunk
        .columns()
        .iter()
        .zip(chunk.schema().fields().iter())
        .zip(chunk.chunk_schema().slots().iter())
        .enumerate()
    {
        let column_type = column.data_type();
        match column_type {
            DataType::Dictionary(_, value_type) if !keep_encoded(slot.slot_id(), column_type) => {
                changed = true;
                let value_type = value_type.as_ref().clone();
                let flat_field = field.as_ref().clone().with_data_type(value_type.clone());
                let flat = cast(column.as_ref(), &value_type).map_err(|e| {
                    format!(
                        "hydrate dictionary chunk column {} to value type {:?} failed: {e}",
                        idx, value_type
                    )
                })?;
                columns.push(flat);
                slots.push(slot.with_field(flat_field)?);
            }
            _ => {
                columns.push(Arc::clone(column));
                slots.push(slot.clone());
            }
        }
    }

    if !changed {
        return Ok(chunk.clone());
    }

    let chunk_schema = Arc::new(ChunkSchema::try_new_with_schema_metadata(
        slots,
        chunk.schema().metadata().clone(),
    )?);
    let batch = RecordBatch::try_new(chunk_schema.arrow_schema_ref(), columns)
        .map_err(|e| format!("build hydrated chunk record batch failed: {e}"))?;
    Chunk::try_new_with_chunk_schema(batch, chunk_schema)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, DictionaryArray, LargeStringArray, LargeStringDictionaryBuilder,
        StringArray,
    };
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;

    use super::{
        assert_no_dictionary, hydrate_dictionary_columns, hydrate_dictionary_columns_except,
    };
    use crate::exec::chunk::{Chunk, ChunkFieldSchema, ChunkSchema, ChunkSlotSchema};
    use novarocks_types::SlotId;
    use novarocks_types::logical::{LogicalType, field_with_logical_type};

    fn dict_utf8_with_nulls_and_empty() -> ArrayRef {
        Arc::new(
            vec![Some("PAID"), None, Some(""), Some("NEW")]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        )
    }

    fn dict_large_utf8_with_nulls_and_empty() -> ArrayRef {
        let mut builder = LargeStringDictionaryBuilder::<Int32Type>::new();
        builder.append_value("PAID");
        builder.append_null();
        builder.append_value("");
        builder.append_value("NEW");
        Arc::new(builder.finish())
    }

    fn chunk_with_column(slot_id: SlotId, field: Field, column: ArrayRef) -> Chunk {
        chunk_with_slot(
            ChunkSlotSchema::new_with_field(slot_id, field, None, None),
            column,
        )
    }

    fn chunk_with_slot(slot: ChunkSlotSchema, column: ArrayRef) -> Chunk {
        let schema = Arc::new(ChunkSchema::try_new(vec![slot]).expect("chunk schema"));
        Chunk::try_new_with_columns(schema, vec![column]).expect("chunk")
    }

    fn json_field_schema() -> ChunkFieldSchema {
        ChunkFieldSchema::from_field(&field_with_logical_type(
            Field::new("logical_payload", DataType::Utf8, true),
            LogicalType::Json,
        ))
        .expect("logical field schema")
    }

    #[test]
    fn hydrate_dictionary_columns_flattens_utf8_dictionary_preserving_slot_contract() {
        let slot_id = SlotId::new(7);
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("source".to_string(), "dict".to_string());
        let chunk = chunk_with_column(
            slot_id,
            Field::new("status", DataType::Utf8, false).with_metadata(metadata.clone()),
            dict_utf8_with_nulls_and_empty(),
        );

        let hydrated = hydrate_dictionary_columns(&chunk).expect("hydrate");

        assert_eq!(hydrated.columns()[0].data_type(), &DataType::Utf8);
        let values = hydrated.columns()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        assert_eq!(values.value(0), "PAID");
        assert!(values.is_null(1));
        assert_eq!(values.value(2), "");
        assert_eq!(values.value(3), "NEW");
        let field = hydrated
            .chunk_schema()
            .field_by_slot(slot_id)
            .expect("slot field");
        assert_eq!(field.name(), "status");
        assert_eq!(field.data_type(), &DataType::Utf8);
        assert_eq!(field.metadata(), &metadata);
        assert_eq!(
            hydrated
                .chunk_schema()
                .slot(slot_id)
                .expect("slot schema")
                .slot_id(),
            slot_id
        );
    }

    #[test]
    fn hydrate_dictionary_columns_flattens_large_utf8_dictionary_with_nulls_and_empty_strings() {
        let slot_id = SlotId::new(11);
        let chunk = chunk_with_column(
            slot_id,
            Field::new("status_l", DataType::LargeUtf8, true),
            dict_large_utf8_with_nulls_and_empty(),
        );

        let hydrated = hydrate_dictionary_columns(&chunk).expect("hydrate");

        assert_eq!(hydrated.columns()[0].data_type(), &DataType::LargeUtf8);
        let values = hydrated.columns()[0]
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .expect("large string array");
        assert_eq!(values.value(0), "PAID");
        assert!(values.is_null(1));
        assert_eq!(values.value(2), "");
        assert_eq!(values.value(3), "NEW");
        assert_eq!(
            hydrated
                .chunk_schema()
                .field_by_slot(slot_id)
                .expect("slot field")
                .data_type(),
            &DataType::LargeUtf8
        );
    }

    #[test]
    fn hydrate_dictionary_columns_preserves_slot_unique_id_and_field_schema() {
        let slot_id = SlotId::new(13);
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("source".to_string(), "dict".to_string());
        let slot = ChunkSlotSchema::new_with_field(
            slot_id,
            Field::new("payload", DataType::Utf8, true).with_metadata(metadata.clone()),
            Some(json_field_schema()),
            Some(77),
        );
        let chunk = chunk_with_slot(slot, dict_utf8_with_nulls_and_empty());

        let hydrated = hydrate_dictionary_columns(&chunk).expect("hydrate");

        let hydrated_slot = hydrated
            .chunk_schema()
            .slot(slot_id)
            .expect("hydrated slot");
        assert_eq!(hydrated_slot.unique_id(), Some(77));
        assert_eq!(hydrated_slot.field().metadata(), &metadata);
        assert_eq!(hydrated_slot.data_type(), &DataType::Utf8);
        assert!(hydrated_slot.field_schema().json_semantic());
    }

    #[test]
    fn hydrate_dictionary_columns_fast_path_preserves_plain_chunk_schema() {
        let slot_id = SlotId::new(17);
        let slot = ChunkSlotSchema::new_with_field(
            slot_id,
            Field::new("payload", DataType::Utf8, true),
            Some(json_field_schema()),
            Some(77),
        );
        let chunk = chunk_with_slot(
            slot,
            Arc::new(StringArray::from(vec![Some("PAID"), None, Some("")])),
        );

        let hydrated = hydrate_dictionary_columns(&chunk).expect("hydrate");

        assert_eq!(hydrated.columns()[0].data_type(), &DataType::Utf8);
        let hydrated_slot = hydrated
            .chunk_schema()
            .slot(slot_id)
            .expect("hydrated slot");
        assert_eq!(hydrated_slot.unique_id(), Some(77));
        assert_eq!(hydrated_slot.data_type(), &DataType::Utf8);
        assert!(hydrated_slot.field_schema().json_semantic());
    }

    #[test]
    fn hydrate_dictionary_columns_except_keeps_selected_slots_and_matches_default_hydration() {
        let slot_id = SlotId::new(1);
        let chunk = chunk_with_column(
            slot_id,
            Field::new("status", DataType::Utf8, true),
            dict_utf8_with_nulls_and_empty(),
        );

        let kept = hydrate_dictionary_columns_except(&chunk, |slot, _dt| slot == SlotId::new(1))
            .expect("keep selected slot");

        assert!(matches!(
            kept.columns()[0].data_type(),
            DataType::Dictionary(_, value_type) if value_type.as_ref() == &DataType::Utf8
        ));
        assert!(matches!(
            kept.schema().fields()[0].data_type(),
            DataType::Dictionary(_, value_type) if value_type.as_ref() == &DataType::Utf8
        ));
        assert!(matches!(
            kept.chunk_schema()
                .slot(slot_id)
                .expect("kept slot")
                .data_type(),
            DataType::Dictionary(_, value_type) if value_type.as_ref() == &DataType::Utf8
        ));

        let keep_none =
            hydrate_dictionary_columns_except(&chunk, |_, _| false).expect("hydrate all");
        let default_hydrated = hydrate_dictionary_columns(&chunk).expect("default hydrate");

        assert_eq!(keep_none.columns()[0].data_type(), &DataType::Utf8);
        assert_eq!(
            keep_none.columns()[0].data_type(),
            default_hydrated.columns()[0].data_type()
        );
        let keep_none_values = keep_none.columns()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("keep-none string array");
        let default_values = default_hydrated.columns()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("default string array");
        assert_eq!(keep_none_values, default_values);
        assert_eq!(
            keep_none
                .chunk_schema()
                .slot(slot_id)
                .expect("keep-none slot")
                .data_type(),
            default_hydrated
                .chunk_schema()
                .slot(slot_id)
                .expect("default slot")
                .data_type()
        );
    }

    #[test]
    fn hydrate_dictionary_columns_except_keeps_one_dictionary_slot_and_hydrates_another() {
        let keep_slot_id = SlotId::new(1);
        let hydrate_slot_id = SlotId::new(2);
        let keep_column = dict_utf8_with_nulls_and_empty();
        let hydrate_column = dict_utf8_with_nulls_and_empty();
        let mut schema_metadata = std::collections::HashMap::new();
        schema_metadata.insert("source".to_string(), "mixed-dict".to_string());
        let chunk_schema = Arc::new(
            ChunkSchema::try_new_with_schema_metadata(
                vec![
                    ChunkSlotSchema::new_with_field(
                        keep_slot_id,
                        Field::new("keep_status", keep_column.data_type().clone(), true),
                        None,
                        None,
                    ),
                    ChunkSlotSchema::new_with_field(
                        hydrate_slot_id,
                        Field::new("hydrate_status", hydrate_column.data_type().clone(), true),
                        None,
                        None,
                    ),
                ],
                schema_metadata.clone(),
            )
            .expect("chunk schema"),
        );
        let batch = RecordBatch::try_new(
            chunk_schema.arrow_schema_ref(),
            vec![keep_column, hydrate_column],
        )
        .expect("record batch");
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk");
        assert_eq!(chunk.schema().metadata(), &schema_metadata);
        assert_eq!(
            chunk.chunk_schema().arrow_schema_ref().metadata(),
            &schema_metadata
        );

        let hydrated = hydrate_dictionary_columns_except(&chunk, |slot, _dt| slot == keep_slot_id)
            .expect("hydrate except kept slot");

        assert!(matches!(
            hydrated.columns()[0].data_type(),
            DataType::Dictionary(key_type, value_type)
                if key_type.as_ref() == &DataType::Int32 && value_type.as_ref() == &DataType::Utf8
        ));
        assert!(matches!(
            hydrated.schema().fields()[0].data_type(),
            DataType::Dictionary(key_type, value_type)
                if key_type.as_ref() == &DataType::Int32 && value_type.as_ref() == &DataType::Utf8
        ));
        let kept_slot = hydrated
            .chunk_schema()
            .slot(keep_slot_id)
            .expect("kept slot schema");
        assert_eq!(kept_slot.slot_id(), keep_slot_id);
        assert!(matches!(
            kept_slot.data_type(),
            DataType::Dictionary(key_type, value_type)
                if key_type.as_ref() == &DataType::Int32 && value_type.as_ref() == &DataType::Utf8
        ));

        assert_eq!(hydrated.columns()[1].data_type(), &DataType::Utf8);
        let hydrated_values = hydrated.columns()[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("hydrated string array");
        assert_eq!(hydrated_values.value(0), "PAID");
        assert!(hydrated_values.is_null(1));
        assert_eq!(hydrated_values.value(2), "");
        assert_eq!(hydrated_values.value(3), "NEW");
        assert_eq!(hydrated.schema().fields()[1].data_type(), &DataType::Utf8);
        let hydrated_slot = hydrated
            .chunk_schema()
            .slot(hydrate_slot_id)
            .expect("hydrated slot schema");
        assert_eq!(hydrated_slot.slot_id(), hydrate_slot_id);
        assert_eq!(hydrated_slot.data_type(), &DataType::Utf8);
        assert_eq!(
            hydrated.chunk_schema().slot_ids(),
            &[keep_slot_id, hydrate_slot_id]
        );
        assert_eq!(hydrated.schema().metadata(), &schema_metadata);
        assert_eq!(
            hydrated.chunk_schema().arrow_schema_ref().metadata(),
            &schema_metadata
        );
    }

    #[test]
    fn hydrate_dictionary_columns_except_preserves_schema_metadata_when_hydrating() {
        let slot_id = SlotId::new(23);
        let column = dict_utf8_with_nulls_and_empty();
        let mut chunk = chunk_with_column(
            slot_id,
            Field::new("status", DataType::Utf8, true),
            Arc::clone(&column),
        );
        let mut schema_metadata = std::collections::HashMap::new();
        schema_metadata.insert("tenant".to_string(), "analytics".to_string());
        let batch_schema = Arc::new(Schema::new_with_metadata(
            vec![Arc::new(Field::new(
                "status",
                column.data_type().clone(),
                true,
            ))],
            schema_metadata.clone(),
        ));
        chunk.batch = RecordBatch::try_new(batch_schema, vec![column]).expect("record batch");
        assert_eq!(chunk.schema().metadata(), &schema_metadata);

        let hydrated =
            hydrate_dictionary_columns_except(&chunk, |_, _| false).expect("hydrate all");

        assert_eq!(hydrated.schema().metadata(), &schema_metadata);
        assert_eq!(
            hydrated.chunk_schema().arrow_schema_ref().metadata(),
            &schema_metadata
        );
    }

    #[test]
    fn assert_no_dictionary_allows_flat_utf8_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![Some("PAID"), None]))],
        )
        .expect("record batch");

        assert_no_dictionary(&batch).expect("flat batch should pass");
    }

    #[test]
    fn assert_no_dictionary_rejects_dictionary_batch_with_field_name() {
        let column = dict_utf8_with_nulls_and_empty();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            column.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![column]).expect("record batch");

        let err = assert_no_dictionary(&batch).expect_err("dictionary batch should fail");

        assert!(err.contains("status"), "error should identify field: {err}");
        assert!(
            err.contains("Dictionary"),
            "error should identify dictionary type: {err}"
        );
    }
}
