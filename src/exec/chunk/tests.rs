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

use arrow::array::BinaryArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use super::{Chunk, ChunkSchema, ChunkSlotSchema};
use crate::common::ids::SlotId;
use crate::types::logical::{LogicalType, field_with_logical_type, logical_type_of_field};

#[test]
fn strict_rejects_duplicate_slot_id() {
    let err = ChunkSchema::try_new(vec![
        ChunkSlotSchema::new_with_field(
            SlotId::new(1),
            Field::new("a", DataType::Int32, true),
            None,
            None,
        ),
        ChunkSlotSchema::new_with_field(
            SlotId::new(1),
            Field::new("b", DataType::Int32, true),
            None,
            None,
        ),
    ])
    .expect_err("duplicate slot ids should fail");
    assert!(err.contains("duplicate slot id"), "err={}", err);
}

#[test]
fn chunk_schema_recovers_logical_metadata_and_unique_id() {
    let hll_field =
        field_with_logical_type(Field::new("a", DataType::Binary, true), LogicalType::Hll);
    let schema = Arc::new(Schema::new(vec![hll_field.clone()]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(BinaryArray::from(vec![Some(b"x".as_slice())]))],
    )
    .expect("record batch");
    let chunk = Chunk::try_new_with_chunk_schema(
        batch,
        Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(7),
                hll_field,
                None,
                Some(77),
            )])
            .expect("chunk schema"),
        ),
    )
    .expect("chunk");
    let slot = chunk
        .chunk_schema()
        .slot(SlotId::new(7))
        .expect("slot schema");
    assert_eq!(slot.data_type(), &DataType::Binary);
    assert_eq!(slot.field_schema().logical_type(), Some(LogicalType::Hll));
    assert_eq!(logical_type_of_field(slot.field()), Some(LogicalType::Hll));
    assert_eq!(slot.name(), "a");
    assert_eq!(slot.unique_id(), Some(77));
}
