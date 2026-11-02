// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! ScalarValueV1 container records produced from one Native row decode back
//! to the same typed value, and every shape, type, metadata, nullability and
//! size violation is refused before a record is emitted.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Int32Array, Int64Array, ListArray, MapArray, RecordBatch, StringArray, StructArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Fields};
use novarocks_execution::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
use novarocks_native_adapter::root_scalar_container_codec::NativeScalarContainerEncoder;
use novarocks_native_adapter::root_scalar_leaf_codec::NativeScalarLeafError;
use novarocks_result_contract::{
    NamedScalarField, RootProfileV1, ScalarField, ScalarLeafError, ScalarProfileV1, ScalarRecord,
    ScalarSchema, ScalarValue, ScalarValueType,
};
use novarocks_result_render::RenderTurnStatus;
use novarocks_types::SlotId;
use novarocks_types::logical::{LogicalType, NR_LOGICAL_TYPE_KEY};

const SLOT: u32 = 7;

fn leaf(nullable: bool, value_type: ScalarValueType) -> ScalarField {
    ScalarField {
        nullable,
        value_type,
    }
}
fn int32(nullable: bool) -> ScalarField {
    leaf(nullable, ScalarValueType::SignedInteger(32))
}
fn list_of(item: ScalarField, nullable: bool) -> ScalarField {
    leaf(nullable, ScalarValueType::List(Box::new(item)))
}
fn schema(field: ScalarField) -> ScalarSchema {
    ScalarSchema::try_new(field)
        .unwrap()
        .bind_native_slots(&[SLOT])
        .unwrap()
}
fn chunk_with_slot(array: ArrayRef, nullable: bool, slot: u32) -> Chunk {
    let field = Field::new("value", array.data_type().clone(), nullable);
    let slot = ChunkSlotSchema::try_new_with_field(SlotId::new(slot), field, None, None).unwrap();
    let schema = Arc::new(ChunkSchema::try_new(vec![slot]).unwrap());
    let batch = RecordBatch::try_new(schema.arrow_schema_ref(), vec![array]).unwrap();
    Chunk::try_new_with_chunk_schema(batch, schema).unwrap()
}
fn chunk(array: ArrayRef, nullable: bool) -> Chunk {
    chunk_with_slot(array, nullable, SLOT)
}
fn item_field(data_type: DataType, nullable: bool) -> Arc<Field> {
    Arc::new(Field::new("item", data_type, nullable))
}
fn int_list(rows: &[Option<Vec<Option<i32>>>], item_nullable: bool) -> ArrayRef {
    let mut values = Vec::new();
    let mut lengths = Vec::new();
    let mut validity = Vec::new();
    for row in rows {
        validity.push(row.is_some());
        let items = row.clone().unwrap_or_default();
        lengths.push(items.len());
        values.extend(items);
    }
    Arc::new(
        ListArray::try_new(
            item_field(DataType::Int32, item_nullable),
            OffsetBuffer::from_lengths(lengths),
            Arc::new(Int32Array::from(values)),
            Some(NullBuffer::from(validity)),
        )
        .unwrap(),
    )
}
fn prepaid() -> usize {
    NativeScalarContainerEncoder::scratch_capacity_bytes()
}
fn emit(mut encoder: NativeScalarContainerEncoder) -> Vec<u8> {
    let mut record = Vec::new();
    let mut output = vec![0_u8; RootProfileV1::EMIT_BYTES_PER_TURN];
    loop {
        let turn = encoder.step(&mut output);
        record.extend_from_slice(&output[..turn.emitted_bytes]);
        if turn.status == RenderTurnStatus::InputComplete {
            assert_eq!(turn.completed_rows, 1);
            return record;
        }
    }
}
fn encode(chunk: &Chunk, schema: &ScalarSchema) -> Result<ScalarRecord, NativeScalarLeafError> {
    let encoder = NativeScalarContainerEncoder::try_encode(chunk, schema, prepaid())?;
    let record = emit(encoder);
    Ok(ScalarRecord::decode_owned(schema, &record).unwrap())
}
fn i32_value(value: i64) -> ScalarValue {
    ScalarValue::SignedInteger { bits: 32, value }
}

#[test]
fn list_with_null_element_round_trips() {
    let schema = schema(list_of(int32(true), true));
    let chunk = chunk(int_list(&[Some(vec![Some(1), None, Some(3)])], true), true);
    assert_eq!(
        encode(&chunk, &schema).unwrap(),
        ScalarRecord::Value(ScalarValue::List(vec![
            i32_value(1),
            ScalarValue::Null,
            i32_value(3)
        ]))
    );
}

#[test]
fn empty_list_is_a_value_not_absence() {
    let schema = schema(list_of(int32(true), true));
    let chunk = chunk(int_list(&[Some(vec![])], true), true);
    assert_eq!(
        encode(&chunk, &schema).unwrap(),
        ScalarRecord::Value(ScalarValue::List(Vec::new()))
    );
}

#[test]
fn top_level_null_is_one_null_value() {
    let schema = schema(list_of(int32(true), true));
    let chunk = chunk(int_list(&[None], true), true);
    assert_eq!(
        encode(&chunk, &schema).unwrap(),
        ScalarRecord::Value(ScalarValue::Null)
    );
}

#[test]
fn sliced_row_reads_its_own_offsets() {
    let schema = schema(list_of(int32(true), true));
    let rows = int_list(&[Some(vec![Some(9)]), Some(vec![Some(4), Some(5)])], true);
    let chunk = chunk(rows.slice(1, 1), true);
    assert_eq!(
        encode(&chunk, &schema).unwrap(),
        ScalarRecord::Value(ScalarValue::List(vec![i32_value(4), i32_value(5)]))
    );
}

fn map_type(value_nullable: bool) -> (Arc<Field>, Fields) {
    let entry_fields = Fields::from(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Int64, value_nullable),
    ]);
    let entries = Arc::new(Field::new(
        "entries",
        DataType::Struct(entry_fields.clone()),
        false,
    ));
    (entries, entry_fields)
}

#[test]
fn map_entries_round_trip_in_order() {
    let (entries, entry_fields) = map_type(true);
    let keys = Arc::new(StringArray::from(vec!["b", "a"])) as ArrayRef;
    let values = Arc::new(Int64Array::from(vec![Some(2), None])) as ArrayRef;
    let map = MapArray::try_new(
        entries,
        OffsetBuffer::from_lengths([2]),
        StructArray::try_new(entry_fields, vec![keys, values], None).unwrap(),
        None,
        false,
    )
    .unwrap();
    let schema = schema(leaf(
        false,
        ScalarValueType::Map {
            key: Box::new(leaf(false, ScalarValueType::String)),
            value: Box::new(leaf(true, ScalarValueType::SignedInteger(64))),
        },
    ));
    let chunk = chunk(Arc::new(map), false);
    assert_eq!(
        encode(&chunk, &schema).unwrap(),
        ScalarRecord::Value(ScalarValue::Map(vec![
            (
                ScalarValue::String("b".into()),
                ScalarValue::SignedInteger { bits: 64, value: 2 }
            ),
            (ScalarValue::String("a".into()), ScalarValue::Null),
        ]))
    );
}

#[test]
fn struct_with_nested_list_and_null_member_round_trips() {
    let tags = int_list(&[Some(vec![Some(5)])], true);
    let fields = Fields::from(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("tags", tags.data_type().clone(), true),
    ]);
    let structure = StructArray::try_new(
        fields,
        vec![
            Arc::new(Int32Array::from(vec![11])) as ArrayRef,
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            tags,
        ],
        None,
    )
    .unwrap();
    let schema = schema(leaf(
        false,
        ScalarValueType::Struct(vec![
            NamedScalarField {
                name: "id".into(),
                field: int32(false),
            },
            NamedScalarField {
                name: "name".into(),
                field: leaf(true, ScalarValueType::String),
            },
            NamedScalarField {
                name: "tags".into(),
                field: list_of(int32(true), true),
            },
        ]),
    ));
    let chunk = chunk(Arc::new(structure), false);
    assert_eq!(
        encode(&chunk, &schema).unwrap(),
        ScalarRecord::Value(ScalarValue::Struct(vec![
            i32_value(11),
            ScalarValue::Null,
            ScalarValue::List(vec![i32_value(5)]),
        ]))
    );
}

#[test]
fn nested_json_leaf_requires_its_logical_metadata() {
    let schema = schema(list_of(leaf(true, ScalarValueType::Json), true));
    let make = |metadata: bool| {
        let mut item = Field::new("item", DataType::Utf8, true);
        if metadata {
            item = item.with_metadata(std::collections::HashMap::<String, String>::from([(
                NR_LOGICAL_TYPE_KEY.to_string(),
                LogicalType::Json.metadata_value().to_string(),
            )]));
        }
        let list = ListArray::try_new(
            Arc::new(item),
            OffsetBuffer::from_lengths([1]),
            Arc::new(StringArray::from(vec!["{\"a\":1}"])),
            None,
        )
        .unwrap();
        chunk(Arc::new(list), true)
    };
    assert_eq!(
        encode(&make(true), &schema).unwrap(),
        ScalarRecord::Value(ScalarValue::List(vec![ScalarValue::Json(
            "{\"a\":1}".into()
        )]))
    );
    assert!(matches!(
        NativeScalarContainerEncoder::try_encode(&make(false), &schema, prepaid()),
        Err(NativeScalarLeafError::LogicalMetadata)
    ));
}

#[test]
fn whole_record_including_header_is_limited_to_one_record_allowance() {
    let schema = schema(list_of(leaf(true, ScalarValueType::String), true));
    let text = "x".repeat(1024);
    let build = |count: usize| {
        let list = ListArray::try_new(
            item_field(DataType::Utf8, true),
            OffsetBuffer::from_lengths([count]),
            Arc::new(StringArray::from(vec![text.as_str(); count])),
            None,
        )
        .unwrap();
        chunk(Arc::new(list), true)
    };
    // Header(24) + count(4) + n * (presence 1 + length 4 + 1024 bytes)
    let fits = (ScalarProfileV1::RECORD_PAYLOAD_BYTES - 4) / (1 + 4 + 1024);
    let encoder = NativeScalarContainerEncoder::try_encode(&build(fits), &schema, prepaid())
        .expect("a record within the allowance encodes");
    assert!(encoder.encoded_len() <= novarocks_result_contract::SCALAR_RECORD_MAX_BYTES);
    assert!(matches!(
        NativeScalarContainerEncoder::try_encode(&build(fits + 1), &schema, prepaid()),
        Err(NativeScalarLeafError::Leaf(ScalarLeafError::ValueLimit))
    ));
}

#[test]
fn member_nullability_must_match_the_frozen_type() {
    // Arrow keeps the member nullable; the frozen scalar type does not.
    let schema = schema(list_of(int32(false), true));
    let chunk = chunk(int_list(&[Some(vec![Some(1), None])], true), true);
    assert!(matches!(
        NativeScalarContainerEncoder::try_encode(&chunk, &schema, prepaid()),
        Err(NativeScalarLeafError::Type)
    ));
}

#[test]
fn shape_slot_carrier_and_scratch_violations_are_refused() {
    let schema = schema(list_of(int32(true), true));
    let one = int_list(&[Some(vec![Some(1)])], true);
    assert!(matches!(
        NativeScalarContainerEncoder::try_encode(
            &chunk_with_slot(one.clone(), true, 8),
            &schema,
            prepaid()
        ),
        Err(NativeScalarLeafError::Slot)
    ));
    assert!(matches!(
        NativeScalarContainerEncoder::try_encode(&chunk(one.clone(), true), &schema, prepaid() - 1),
        Err(NativeScalarLeafError::ScratchLimit)
    ));
    let two = int_list(&[Some(vec![Some(1)]), Some(vec![])], true);
    assert!(matches!(
        NativeScalarContainerEncoder::try_encode(&chunk(two, true), &schema, prepaid()),
        Err(NativeScalarLeafError::Shape)
    ));
    let empty = int_list(&[], true);
    assert!(matches!(
        NativeScalarContainerEncoder::try_encode(&chunk(empty.clone(), true), &schema, prepaid()),
        Err(NativeScalarLeafError::EmptyInput)
    ));
    NativeScalarContainerEncoder::validate_empty(&chunk(empty, true), &schema)
        .expect("an empty batch validates without a record");
    let wide = crate::schema(list_of(
        leaf(true, ScalarValueType::SignedInteger(64)),
        true,
    ));
    assert!(matches!(
        NativeScalarContainerEncoder::try_encode(&chunk(one, true), &wide, prepaid()),
        Err(NativeScalarLeafError::Type)
    ));
}

#[test]
fn leaf_schema_is_not_a_container_input() {
    let schema = schema(int32(true));
    let chunk = chunk(int_list(&[Some(vec![Some(1)])], true), true);
    assert!(matches!(
        NativeScalarContainerEncoder::try_encode(&chunk, &schema, prepaid()),
        Err(NativeScalarLeafError::Type)
    ));
}

#[test]
fn small_output_yields_needs_output_until_complete() {
    let schema = schema(list_of(int32(true), true));
    let chunk = chunk(int_list(&[Some(vec![Some(1), Some(2)])], true), true);
    let mut encoder = NativeScalarContainerEncoder::try_encode(&chunk, &schema, prepaid()).unwrap();
    let total = encoder.encoded_len();
    let mut record = Vec::new();
    let mut output = [0_u8; 5];
    let mut turns = 0;
    loop {
        let turn = encoder.step(&mut output);
        turns += 1;
        record.extend_from_slice(&output[..turn.emitted_bytes]);
        if turn.status == RenderTurnStatus::InputComplete {
            break;
        }
        assert_eq!(turn.status, RenderTurnStatus::NeedsOutput);
        assert_eq!(turn.completed_rows, 0);
    }
    assert_eq!(record.len(), total);
    assert_eq!(turns, total.div_ceil(output.len()));
    assert_eq!(
        ScalarRecord::decode_owned(&schema, &record).unwrap(),
        ScalarRecord::Value(ScalarValue::List(vec![i32_value(1), i32_value(2)]))
    );
}
