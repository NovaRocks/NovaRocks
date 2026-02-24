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
use crate::common::ids::SlotId;
use crate::exec::chunk::{Chunk, field_with_slot_id};
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

pub fn chunk_len_1() -> Chunk {
    let array = Arc::new(Int64Array::from(vec![1])) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![field_with_slot_id(
        Field::new("dummy", DataType::Int64, false),
        SlotId::new(1),
    )]));
    let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
    Chunk::new(batch)
}

pub fn literal_string(arena: &mut ExprArena, v: &str) -> ExprId {
    arena.push(ExprNode::Literal(LiteralValue::Utf8(v.to_string())))
}

pub fn typed_null(arena: &mut ExprArena, data_type: DataType) -> ExprId {
    arena.push_typed(ExprNode::Literal(LiteralValue::Null), data_type)
}
