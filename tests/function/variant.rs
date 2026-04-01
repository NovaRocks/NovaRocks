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
#![allow(unused_imports)]

use crate::common;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, LargeBinaryArray, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks::common::ids::SlotId;
use novarocks::exec::chunk::Chunk;
use novarocks::exec::chunk::ChunkSchema;
use novarocks::exec::expr::ExprId;
use novarocks::exec::expr::function::FunctionKind;
use novarocks::exec::expr::function::variant::{eval_variant_function, eval_variant_query};
use novarocks::exec::expr::{ExprArena, ExprNode, LiteralValue};
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn slot_id_expr(arena: &mut ExprArena, slot: i32, data_type: DataType) -> ExprId {
    arena.push_typed(ExprNode::SlotId(SlotId::new(slot as u32)), data_type)
}

fn variant_primitive_serialized(type_id: u8, payload: &[u8]) -> Vec<u8> {
    use novarocks::exec::variant::VariantMetadata;
    use novarocks::exec::variant::VariantValue;
    let metadata = VariantMetadata::empty();
    let mut value = vec![type_id << 2];
    value.extend_from_slice(payload);
    VariantValue::create(metadata.raw(), &value)
        .unwrap()
        .serialize()
}

fn make_variant_chunk(variant_bytes: Vec<u8>) -> (Chunk, ExprId, ExprArena) {
    let variant_arr =
        Arc::new(LargeBinaryArray::from(vec![Some(variant_bytes.as_slice())])) as ArrayRef;
    let variant_type = DataType::LargeBinary;
    let field = Field::new("v", variant_type.clone(), true);
    let batch =
        RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![variant_arr]).unwrap();
    let chunk = {
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1)],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    };
    let mut arena = ExprArena::default();
    let arg0 = slot_id_expr(&mut arena, 1, variant_type);
    (chunk, arg0, arena)
}

// ---------------------------------------------------------------------------
// Tests migrated from variant/dispatch.rs
// ---------------------------------------------------------------------------

#[test]
fn test_register_variant_functions() {
    use novarocks::exec::expr::function::variant::register;
    let mut m = HashMap::new();
    register(&mut m);
    assert_eq!(
        m.get("json_query"),
        Some(&FunctionKind::Variant("json_query"))
    );
    assert_eq!(
        m.get("variant_typeof"),
        Some(&FunctionKind::Variant("variant_typeof"))
    );
    assert_eq!(
        m.get("get_json_object"),
        Some(&FunctionKind::Variant("get_variant_string"))
    );
    assert_eq!(
        m.get("get_json_int"),
        Some(&FunctionKind::Variant("get_variant_int"))
    );
}

// ---------------------------------------------------------------------------
// Tests migrated from variant/get_variant.rs
// ---------------------------------------------------------------------------

#[test]
fn test_variant_query_root_path() {
    let variant = variant_primitive_serialized(6, &123_i64.to_le_bytes());
    let (chunk, arg0, mut arena) = make_variant_chunk(variant);
    let arg1 = arena.push(ExprNode::Literal(LiteralValue::Utf8("$".to_string())));
    let expr = common::typed_null(&mut arena, DataType::LargeBinary);
    let out = eval_variant_query(&arena, expr, &[arg0, arg1], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    assert!(!out.is_null(0));
}

#[test]
fn test_get_variant_bool() {
    let variant = variant_primitive_serialized(1, &[]);
    let (chunk, arg0, mut arena) = make_variant_chunk(variant);
    let arg1 = arena.push(ExprNode::Literal(LiteralValue::Utf8("$".to_string())));
    let expr = common::typed_null(&mut arena, DataType::Boolean);
    let out =
        eval_variant_function("get_variant_bool", &arena, expr, &[arg0, arg1], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(out.value(0));
}

#[test]
fn test_get_variant_int() {
    let variant = variant_primitive_serialized(6, &123_i64.to_le_bytes());
    let (chunk, arg0, mut arena) = make_variant_chunk(variant);
    let arg1 = arena.push(ExprNode::Literal(LiteralValue::Utf8("$".to_string())));
    let expr = common::typed_null(&mut arena, DataType::Int64);
    let out =
        eval_variant_function("get_variant_int", &arena, expr, &[arg0, arg1], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(out.value(0), 123);
}

#[test]
fn test_get_variant_double() {
    let payload = 3.5_f64.to_le_bytes();
    let variant = variant_primitive_serialized(7, &payload);
    let (chunk, arg0, mut arena) = make_variant_chunk(variant);
    let arg1 = arena.push(ExprNode::Literal(LiteralValue::Utf8("$".to_string())));
    let expr = common::typed_null(&mut arena, DataType::Float64);
    let out =
        eval_variant_function("get_variant_double", &arena, expr, &[arg0, arg1], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((out.value(0) - 3.5).abs() < 1e-12);
}

#[test]
fn test_get_variant_string() {
    let variant = variant_primitive_serialized(6, &123_i64.to_le_bytes());
    let (chunk, arg0, mut arena) = make_variant_chunk(variant);
    let arg1 = arena.push(ExprNode::Literal(LiteralValue::Utf8("$".to_string())));
    let expr = common::typed_null(&mut arena, DataType::Utf8);
    let out =
        eval_variant_function("get_variant_string", &arena, expr, &[arg0, arg1], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(out.value(0), "123");
}

#[test]
fn test_json_query_quotes_string_and_null_for_missing() {
    let json_arr = Arc::new(StringArray::from(vec![
        Some("{\"name\":\"abc\",\"age\":23}"),
        Some("{\"age\":23}"),
    ])) as ArrayRef;
    let field = Field::new("j", DataType::Utf8, true);
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![json_arr]).unwrap();
    let chunk = {
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1)],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    };

    let mut arena = ExprArena::default();
    let arg0 = slot_id_expr(&mut arena, 1, DataType::Utf8);
    let arg1 = arena.push(ExprNode::Literal(LiteralValue::Utf8("$.name".to_string())));
    let expr = common::typed_null(&mut arena, DataType::Utf8);
    let out = eval_variant_function("json_query", &arena, expr, &[arg0, arg1], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(out.value(0), "\"abc\"");
    assert!(out.is_null(1));
}

// ---------------------------------------------------------------------------
// Tests migrated from variant/variant_typeof.rs
// ---------------------------------------------------------------------------

#[test]
fn test_variant_typeof_int64() {
    let variant = variant_primitive_serialized(6, &123_i64.to_le_bytes());
    let (chunk, arg0, mut arena) = make_variant_chunk(variant);
    let expr = common::typed_null(&mut arena, DataType::Utf8);
    let out = eval_variant_function("variant_typeof", &arena, expr, &[arg0], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(out.value(0), "Int64");
}
