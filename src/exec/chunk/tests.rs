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

use arrow::array::{BinaryArray, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use super::{Chunk, ChunkSchema, ChunkSlotSchema};
use crate::common::ids::SlotId;
use crate::lower::type_lowering::scalar_type_desc;
use crate::types::TPrimitiveType;

#[test]
fn try_new_with_slot_ids_requires_explicit_slot_ids() {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2]))])
        .expect("record batch");
    let err = Chunk::try_new_with_slot_ids(batch, &[]).expect_err("expected explicit slot ids");
    assert!(err.contains("slot id length mismatch"), "err={}", err);
}

#[test]
fn strict_rejects_duplicate_slot_id() {
    let err = ChunkSchema::try_new(vec![
        ChunkSlotSchema::new(SlotId::new(1), "a", true, None, None),
        ChunkSlotSchema::new(SlotId::new(1), "b", true, None, None),
    ])
    .expect_err("duplicate slot ids should fail");
    assert!(err.contains("duplicate slot id"), "err={}", err);
}

#[test]
fn chunk_schema_sidecar_recovers_type_desc_and_unique_id() {
    let desc = scalar_type_desc(TPrimitiveType::HLL);
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Binary, true)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(BinaryArray::from(vec![Some(b"x".as_slice())]))],
    )
    .expect("record batch");
    let chunk = Chunk::try_new_with_chunk_schema(
        batch,
        Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new(
                SlotId::new(7),
                "a",
                true,
                Some(desc.clone()),
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
    assert_eq!(slot.type_desc(), Some(&desc));
    assert_eq!(slot.primitive_type(), Some(TPrimitiveType::HLL));
    assert_eq!(slot.name(), "a");
    assert_eq!(slot.unique_id(), Some(77));
}
