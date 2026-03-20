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
use novarocks::common::ids::SlotId;
use novarocks::exec::chunk::Chunk;
use novarocks::exec::expr::function::map::eval_map_function;
use novarocks::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
use arrow::array::{
    Array, ArrayRef, BooleanBuilder, Int32Array, Int64Array, Int64Builder, ListArray, ListBuilder,
    MapArray, MapBuilder, StringArray, StringBuilder, StructArray,
};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers copied from map/test_utils.rs
// ---------------------------------------------------------------------------

fn slot_id_expr(arena: &mut ExprArena, slot: i32, data_type: DataType) -> ExprId {
    arena.push_typed(
        ExprNode::SlotId(SlotId::new(slot as u32)),
        data_type,
    )
}

fn typed_null(arena: &mut ExprArena, data_type: DataType) -> ExprId {
    arena.push_typed(ExprNode::Literal(LiteralValue::Null), data_type)
}

fn make_chunk_from_arrays(
    fields: Vec<Field>,
    arrays: Vec<ArrayRef>,
    slot_ids: &[u32],
) -> Chunk {
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    let ids: Vec<SlotId> = slot_ids.iter().map(|&id| SlotId::new(id)).collect();
    let chunk_schema =
        novarocks::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &ids,
        )
        .expect("chunk schema");
    Chunk::new_with_chunk_schema(batch, chunk_schema)
}

// ---------------------------------------------------------------------------
// Tests from map_keys.rs
// ---------------------------------------------------------------------------

fn single_row_map_i64() -> (Chunk, DataType) {
    let mut builder = MapBuilder::new(None, Int64Builder::new(), Int64Builder::new());
    builder.keys().append_value(1);
    builder.values().append_value(10);
    builder.keys().append_value(2);
    builder.values().append_value(20);
    builder.append(true).unwrap();
    let map = Arc::new(builder.finish()) as ArrayRef;
    let map_type = map.data_type().clone();
    let field = Field::new("m", map_type.clone(), true);
    (
        make_chunk_from_arrays(vec![field], vec![map], &[1]),
        map_type,
    )
}

#[test]
fn test_map_keys() {
    let (chunk, map_type) = single_row_map_i64();
    let mut arena = ExprArena::default();
    let arg = slot_id_expr(&mut arena, 1, map_type);
    let expr = typed_null(
        &mut arena,
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
    );
    let out = eval_map_function("map_keys", &arena, expr, &[arg], &chunk).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.value(0), 1);
    assert_eq!(values.value(1), 2);
}

// ---------------------------------------------------------------------------
// Tests from map_values.rs
// ---------------------------------------------------------------------------

#[test]
fn test_map_values() {
    let (chunk, map_type) = single_row_map_i64();
    let mut arena = ExprArena::default();
    let arg = slot_id_expr(&mut arena, 1, map_type);
    let expr = typed_null(
        &mut arena,
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
    );
    let out = eval_map_function("map_values", &arena, expr, &[arg], &chunk).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.value(0), 10);
    assert_eq!(values.value(1), 20);
}

// ---------------------------------------------------------------------------
// Tests from map_size.rs
// ---------------------------------------------------------------------------

#[test]
fn test_map_size() {
    let (chunk, map_type) = single_row_map_i64();
    let mut arena = ExprArena::default();
    let arg = slot_id_expr(&mut arena, 1, map_type);
    let expr = typed_null(&mut arena, DataType::Int32);
    let out = eval_map_function("map_size", &arena, expr, &[arg], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(out.value(0), 2);
}

// ---------------------------------------------------------------------------
// Tests from map_filter.rs
// ---------------------------------------------------------------------------

fn chunk_with_map_and_filter() -> (Chunk, DataType, DataType) {
    let mut map_builder = MapBuilder::new(None, Int64Builder::new(), Int64Builder::new());
    map_builder.keys().append_value(1);
    map_builder.values().append_value(10);
    map_builder.keys().append_value(2);
    map_builder.values().append_value(20);
    map_builder.keys().append_value(3);
    map_builder.values().append_value(30);
    map_builder.append(true).unwrap();
    map_builder.append(false).unwrap();
    let map = Arc::new(map_builder.finish()) as ArrayRef;
    let map_type = map.data_type().clone();

    let mut filter_builder = ListBuilder::new(BooleanBuilder::new());
    filter_builder.values().append_value(true);
    filter_builder.values().append_null();
    filter_builder.values().append_value(false);
    filter_builder.append(true);
    filter_builder.append(false);
    let filter = Arc::new(filter_builder.finish()) as ArrayRef;
    let filter_type = filter.data_type().clone();

    let fields = vec![
        Field::new("m", map_type.clone(), true),
        Field::new("f", filter_type.clone(), true),
    ];
    (
        make_chunk_from_arrays(fields, vec![map, filter], &[1, 2]),
        map_type,
        filter_type,
    )
}

#[test]
fn test_map_filter_basic() {
    let (chunk, map_type, filter_type) = chunk_with_map_and_filter();
    let mut arena = ExprArena::default();
    let map_arg = slot_id_expr(&mut arena, 1, map_type.clone());
    let filter_arg = slot_id_expr(&mut arena, 2, filter_type);
    let expr = typed_null(&mut arena, map_type);
    let out =
        eval_map_function("map_filter", &arena, expr, &[map_arg, filter_arg], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(out.value_length(0), 1);
    let keys = out.keys().as_any().downcast_ref::<Int64Array>().unwrap();
    let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(keys.value(0), 1);
    assert_eq!(values.value(0), 10);
}

#[test]
fn test_map_filter_keeps_map_nullability() {
    let (chunk, map_type, filter_type) = chunk_with_map_and_filter();
    let mut arena = ExprArena::default();
    let map_arg = slot_id_expr(&mut arena, 1, map_type.clone());
    let filter_arg = slot_id_expr(&mut arena, 2, filter_type);
    let expr = typed_null(&mut arena, map_type);
    let out =
        eval_map_function("map_filter", &arena, expr, &[map_arg, filter_arg], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<MapArray>().unwrap();
    assert!(out.is_null(1));
}

// ---------------------------------------------------------------------------
// Tests from map_entries.rs
// ---------------------------------------------------------------------------

#[test]
fn test_map_entries() {
    let (chunk, map_type) = single_row_map_i64();
    let mut arena = ExprArena::default();
    let arg = slot_id_expr(&mut arena, 1, map_type);
    let entry_type = {
        let DataType::Map(entry_field, _) = arena.data_type(arg).unwrap() else {
            panic!("map_entries input must be Map");
        };
        entry_field.data_type().clone()
    };
    let expr = typed_null(
        &mut arena,
        DataType::List(Arc::new(Field::new("item", entry_type, true))),
    );
    let out = eval_map_function("map_entries", &arena, expr, &[arg], &chunk).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let entries = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let values = entries
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(keys.value(0), 1);
    assert_eq!(values.value(0), 10);
}

#[test]
fn test_map_entries_cast_nullable_struct_key() {
    let mut builder = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    builder.keys().append_value("a");
    builder.values().append_value(1);
    builder.keys().append_value("b");
    builder.values().append_value(2);
    builder.append(true).unwrap();

    let map = Arc::new(builder.finish()) as ArrayRef;
    let map_type = map.data_type().clone();
    let field = Field::new("m", map_type.clone(), true);
    let chunk = make_chunk_from_arrays(vec![field], vec![map], &[1]);

    let mut arena = ExprArena::default();
    let arg = slot_id_expr(&mut arena, 1, map_type);
    let expr = typed_null(
        &mut arena,
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![
                Arc::new(Field::new("key", DataType::Utf8, true)),
                Arc::new(Field::new("value", DataType::Int64, true)),
            ])),
            true,
        ))),
    );

    let out = eval_map_function("map_entries", &arena, expr, &[arg], &chunk).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let entries = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(keys.value(0), "a");
    assert_eq!(keys.value(1), "b");
}

// ---------------------------------------------------------------------------
// Tests from map_concat.rs
// ---------------------------------------------------------------------------

fn chunk_with_two_maps() -> (Chunk, DataType, DataType) {
    let mut m1 = MapBuilder::new(None, Int64Builder::new(), Int64Builder::new());
    m1.keys().append_value(1);
    m1.values().append_value(10);
    m1.keys().append_value(2);
    m1.values().append_value(20);
    m1.append(true).unwrap();
    m1.append(false).unwrap();
    let map1 = Arc::new(m1.finish()) as ArrayRef;
    let map1_type = map1.data_type().clone();

    let mut m2 = MapBuilder::new(None, Int64Builder::new(), Int64Builder::new());
    m2.keys().append_value(2);
    m2.values().append_value(200);
    m2.keys().append_value(3);
    m2.values().append_value(300);
    m2.append(true).unwrap();
    m2.append(false).unwrap();
    let map2 = Arc::new(m2.finish()) as ArrayRef;
    let map2_type = map2.data_type().clone();

    let fields = vec![
        Field::new("m1", map1_type.clone(), true),
        Field::new("m2", map2_type.clone(), true),
    ];
    (
        make_chunk_from_arrays(fields, vec![map1, map2], &[1, 2]),
        map1_type,
        map2_type,
    )
}

#[test]
fn test_map_concat_rightmost_overrides() {
    let (chunk, map1_type, map2_type) = chunk_with_two_maps();
    let mut arena = ExprArena::default();
    let map1 = slot_id_expr(&mut arena, 1, map1_type.clone());
    let map2 = slot_id_expr(&mut arena, 2, map2_type);
    let expr = typed_null(&mut arena, map1_type);
    let out = eval_map_function("map_concat", &arena, expr, &[map1, map2], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<MapArray>().unwrap();

    assert_eq!(out.value_length(0), 3);
    let keys = out.keys().as_any().downcast_ref::<Int64Array>().unwrap();
    let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
    let start = out.value_offsets()[0] as usize;
    let end = out.value_offsets()[1] as usize;
    let mut kv = HashMap::new();
    for idx in start..end {
        kv.insert(keys.value(idx), values.value(idx));
    }
    assert_eq!(kv.get(&1), Some(&10));
    assert_eq!(kv.get(&2), Some(&200));
    assert_eq!(kv.get(&3), Some(&300));
}

#[test]
fn test_map_concat_all_null_row_is_null() {
    let (chunk, map1_type, map2_type) = chunk_with_two_maps();
    let mut arena = ExprArena::default();
    let map1 = slot_id_expr(&mut arena, 1, map1_type.clone());
    let map2 = slot_id_expr(&mut arena, 2, map2_type);
    let expr = typed_null(&mut arena, map1_type);
    let out = eval_map_function("map_concat", &arena, expr, &[map1, map2], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<MapArray>().unwrap();
    assert!(out.is_null(1));
}

// ---------------------------------------------------------------------------
// Tests from map_apply.rs
// ---------------------------------------------------------------------------

fn map_type_i64_i64() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Arc::new(Field::new("key", DataType::Int64, false)),
                Arc::new(Field::new("value", DataType::Int64, true)),
            ])),
            false,
        )),
        false,
    )
}

fn single_map_chunk() -> (Chunk, DataType) {
    let mut b = MapBuilder::new(None, Int64Builder::new(), Int64Builder::new());
    b.keys().append_value(1);
    b.values().append_value(10);
    b.keys().append_value(2);
    b.values().append_value(20);
    b.append(true).unwrap();
    b.keys().append_value(3);
    b.values().append_value(30);
    b.append(true).unwrap();
    let map = Arc::new(b.finish()) as ArrayRef;
    let map_type = map.data_type().clone();
    let fields = vec![Field::new("m", map_type.clone(), true)];
    (
        make_chunk_from_arrays(fields, vec![map], &[1]),
        map_type,
    )
}

fn build_lambda_identity_map(arena: &mut ExprArena) -> ExprId {
    use novarocks::exec::expr::function::FunctionKind;
    let list_i64 = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let map_i64_i64 = map_type_i64_i64();

    let key_slot = arena.push_typed(ExprNode::SlotId(SlotId::new(101)), DataType::Int64);
    let value_slot = arena.push_typed(ExprNode::SlotId(SlotId::new(102)), DataType::Int64);
    let key_arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![key_slot],
        },
        list_i64.clone(),
    );
    let value_arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![value_slot],
        },
        list_i64,
    );
    let body = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Map("map_from_arrays"),
            args: vec![key_arr, value_arr],
        },
        map_i64_i64,
    );
    arena.push_typed(
        ExprNode::LambdaFunction {
            body,
            arg_slots: vec![SlotId::new(101), SlotId::new(102)],
            common_sub_exprs: Vec::new(),
            is_nondeterministic: false,
        },
        DataType::Null,
    )
}

#[test]
fn test_map_apply_identity() {
    let (chunk, map_type) = single_map_chunk();
    let mut arena = ExprArena::default();
    let lambda = build_lambda_identity_map(&mut arena);
    let map_arg = slot_id_expr(&mut arena, 1, map_type.clone());
    let expr = typed_null(&mut arena, map_type.clone());

    let out = eval_map_function("map_apply", &arena, expr, &[lambda, map_arg], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(out.value_length(0), 2);
    assert_eq!(out.value_length(1), 1);
    let keys = out.keys().as_any().downcast_ref::<Int64Array>().unwrap();
    let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(keys.value(0), 1);
    assert_eq!(values.value(0), 10);
    assert_eq!(keys.value(1), 2);
    assert_eq!(values.value(1), 20);
}

#[test]
fn test_map_apply_lambda_output_must_be_map() {
    let (chunk, map_type) = single_map_chunk();
    let mut arena = ExprArena::default();
    let key_slot = arena.push_typed(ExprNode::SlotId(SlotId::new(101)), DataType::Int64);
    let lambda = arena.push_typed(
        ExprNode::LambdaFunction {
            body: key_slot,
            arg_slots: vec![SlotId::new(101), SlotId::new(102)],
            common_sub_exprs: Vec::new(),
            is_nondeterministic: false,
        },
        DataType::Null,
    );
    let map_arg = slot_id_expr(&mut arena, 1, map_type.clone());
    let expr = typed_null(&mut arena, map_type);

    let err = eval_map_function("map_apply", &arena, expr, &[lambda, map_arg], &chunk)
        .expect_err("must fail");
    assert!(err.contains("must return MAP"));
}

#[test]
fn test_map_apply_transform_values_alias() {
    use novarocks::exec::expr::function::FunctionKind;
    let (chunk, map_type) = single_map_chunk();
    let mut arena = ExprArena::default();
    let list_i64 = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));

    let key_slot = arena.push_typed(ExprNode::SlotId(SlotId::new(101)), DataType::Int64);
    let value_slot = arena.push_typed(ExprNode::SlotId(SlotId::new(102)), DataType::Int64);
    let one = arena.push_typed(
        ExprNode::Literal(LiteralValue::Int64(1)),
        DataType::Int64,
    );
    let value_plus_one = arena.push_typed(ExprNode::Add(value_slot, one), DataType::Int64);
    let key_arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![key_slot],
        },
        list_i64.clone(),
    );
    let value_arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![value_plus_one],
        },
        list_i64,
    );
    let body = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Map("map_from_arrays"),
            args: vec![key_arr, value_arr],
        },
        map_type.clone(),
    );
    let lambda = arena.push_typed(
        ExprNode::LambdaFunction {
            body,
            arg_slots: vec![SlotId::new(101), SlotId::new(102)],
            common_sub_exprs: Vec::new(),
            is_nondeterministic: false,
        },
        DataType::Null,
    );
    let map_arg = slot_id_expr(&mut arena, 1, map_type.clone());
    let expr = typed_null(&mut arena, map_type);

    let out = eval_map_function("transform_values", &arena, expr, &[lambda, map_arg], &chunk)
        .unwrap();
    let out = out.as_any().downcast_ref::<MapArray>().unwrap();
    let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.value(0), 11);
    assert_eq!(values.value(1), 21);
    assert_eq!(values.value(2), 31);
}

// ---------------------------------------------------------------------------
// Tests from element_at.rs
// ---------------------------------------------------------------------------

use novarocks::exec::expr::function::map::eval_element_at;

#[test]
fn test_element_at_found() {
    let (chunk, map_type) = single_row_map_i64();
    let mut arena = ExprArena::default();
    let map_arg = arena.push_typed(
        ExprNode::SlotId(SlotId::new(1)),
        map_type,
    );
    let key = arena.push(ExprNode::Literal(LiteralValue::Int64(2)));
    let expr = typed_null(&mut arena, DataType::Int64);
    let out = eval_map_function("element_at", &arena, expr, &[map_arg, key], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(out.value(0), 20);
}

#[test]
fn test_element_at_not_found() {
    let (chunk, map_type) = single_row_map_i64();
    let mut arena = ExprArena::default();
    let map_arg = arena.push_typed(
        ExprNode::SlotId(SlotId::new(1)),
        map_type,
    );
    let key = arena.push(ExprNode::Literal(LiteralValue::Int64(9)));
    let expr = typed_null(&mut arena, DataType::Int64);
    let out = eval_element_at(&arena, expr, &[map_arg, key], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(out.is_null(0));
}

#[test]
fn test_element_at_not_found_with_check_errors() {
    let (chunk, map_type) = single_row_map_i64();
    let mut arena = ExprArena::default();
    let map_arg = arena.push_typed(
        ExprNode::SlotId(SlotId::new(1)),
        map_type,
    );
    let key = arena.push(ExprNode::Literal(LiteralValue::Int64(9)));
    let check_true = arena.push_typed(
        ExprNode::Literal(LiteralValue::Bool(true)),
        DataType::Boolean,
    );
    let expr = typed_null(&mut arena, DataType::Int64);
    let err = eval_element_at(&arena, expr, &[map_arg, key, check_true], &chunk).unwrap_err();
    assert!(err.contains("Key not present in map"));
}

// ---------------------------------------------------------------------------
// Tests from distinct_map_keys.rs
// ---------------------------------------------------------------------------

fn chunk_with_map_rows() -> (Chunk, DataType) {
    let mut builder = MapBuilder::new(None, Int64Builder::new(), Int64Builder::new());
    builder.keys().append_value(1);
    builder.values().append_value(10);
    builder.keys().append_value(2);
    builder.values().append_value(20);
    builder.keys().append_value(1);
    builder.values().append_value(30);
    builder.append(true).unwrap();
    builder.append(false).unwrap();

    let map = Arc::new(builder.finish()) as ArrayRef;
    let map_type = map.data_type().clone();
    let field = Field::new("m", map_type.clone(), true);
    (
        make_chunk_from_arrays(vec![field], vec![map], &[1]),
        map_type,
    )
}

#[test]
fn test_distinct_map_keys_keep_last_value() {
    let (chunk, map_type) = chunk_with_map_rows();
    let mut arena = ExprArena::default();
    let arg = slot_id_expr(&mut arena, 1, map_type.clone());
    let expr = typed_null(&mut arena, map_type);
    let out = eval_map_function("distinct_map_keys", &arena, expr, &[arg], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(out.value_length(0), 2);
    let keys = out.keys().as_any().downcast_ref::<Int64Array>().unwrap();
    let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(keys.value(0), 2);
    assert_eq!(values.value(0), 20);
    assert_eq!(keys.value(1), 1);
    assert_eq!(values.value(1), 30);
}

#[test]
fn test_distinct_map_keys_keep_null_row() {
    let (chunk, map_type) = chunk_with_map_rows();
    let mut arena = ExprArena::default();
    let arg = slot_id_expr(&mut arena, 1, map_type.clone());
    let expr = typed_null(&mut arena, map_type);
    let out = eval_map_function("distinct_map_keys", &arena, expr, &[arg], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<MapArray>().unwrap();
    assert!(out.is_null(1));
}

// ---------------------------------------------------------------------------
// Tests from cardinality.rs
// ---------------------------------------------------------------------------

#[test]
fn test_cardinality_array() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = typed_null(&mut arena, DataType::Int32);
    let e1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let e2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![e1, e2],
        },
        list_type,
    );

    let out = eval_map_function("cardinality", &arena, expr, &[arr], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(out.value(0), 2);
}

#[test]
fn test_cardinality_map() {
    let mut builder = MapBuilder::new(None, Int64Builder::new(), Int64Builder::new());
    builder.keys().append_value(1);
    builder.values().append_value(10);
    builder.keys().append_value(2);
    builder.values().append_value(20);
    builder.append(true).unwrap();
    let map = Arc::new(builder.finish()) as ArrayRef;
    let map_type = map.data_type().clone();
    let field = Field::new("m", map_type.clone(), true);
    let chunk = make_chunk_from_arrays(vec![field], vec![map], &[1]);

    let mut arena = ExprArena::default();
    let arg = slot_id_expr(&mut arena, 1, map_type);
    let expr = typed_null(&mut arena, DataType::Int32);
    let out = eval_map_function("cardinality", &arena, expr, &[arg], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(out.value(0), 2);
}

// ---------------------------------------------------------------------------
// Tests from arrays_zip.rs
// ---------------------------------------------------------------------------

fn chunk_with_two_lists() -> (Chunk, DataType, DataType) {
    let mut a = ListBuilder::new(Int64Builder::new());
    a.values().append_value(1);
    a.values().append_value(2);
    a.append(true);
    a.values().append_value(3);
    a.append(true);
    a.append(false);
    let a = Arc::new(a.finish()) as ArrayRef;
    let a_type = a.data_type().clone();

    let mut b = ListBuilder::new(StringBuilder::new());
    b.values().append_value("a");
    b.append(true);
    b.values().append_value("b");
    b.values().append_value("c");
    b.append(true);
    b.values().append_value("z");
    b.append(true);
    let b = Arc::new(b.finish()) as ArrayRef;
    let b_type = b.data_type().clone();

    let fields = vec![
        Field::new("a", a_type.clone(), true),
        Field::new("b", b_type.clone(), true),
    ];
    (
        make_chunk_from_arrays(fields, vec![a, b], &[1, 2]),
        a_type,
        b_type,
    )
}

#[test]
fn test_arrays_zip_basic_and_padding() {
    let (chunk, a_type, b_type) = chunk_with_two_lists();
    let mut arena = ExprArena::default();
    let a = slot_id_expr(&mut arena, 1, a_type);
    let b = slot_id_expr(&mut arena, 2, b_type);
    let out_type = DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("col1", DataType::Int64, true)),
            Arc::new(Field::new("col2", DataType::Utf8, true)),
        ])),
        true,
    )));
    let expr = typed_null(&mut arena, out_type);

    let out = eval_map_function("arrays_zip", &arena, expr, &[a, b], &chunk).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.value_length(0), 2);
    assert_eq!(list.value_length(1), 2);
    assert!(list.is_null(2));

    let values = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let col1 = values
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let col2 = values
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(col1.value(0), 1);
    assert_eq!(col2.value(0), "a");
    assert_eq!(col1.value(2), 3);
    assert_eq!(col2.value(2), "b");
    assert!(col1.is_null(3));
    assert_eq!(col2.value(3), "c");
}

// ---------------------------------------------------------------------------
// Tests from map_from_arrays.rs
// ---------------------------------------------------------------------------

use novarocks::exec::expr::function::map::eval_map_from_arrays;

#[test]
fn test_map_from_arrays_basic() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();

    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let map_entry_type = DataType::Struct(Fields::from(vec![
        Arc::new(Field::new("key", DataType::Int64, false)),
        Arc::new(Field::new("value", DataType::Int64, true)),
    ]));
    let map_type = DataType::Map(
        Arc::new(Field::new("entries", map_entry_type, false)),
        false,
    );

    let key1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let key2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let val1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(10)), DataType::Int64);
    let val2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(20)), DataType::Int64);

    let keys = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![key1, key2],
        },
        list_type.clone(),
    );
    let values = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![val1, val2],
        },
        list_type,
    );
    let expr = typed_null(&mut arena, map_type);

    let out =
        eval_map_function("map_from_arrays", &arena, expr, &[keys, values], &chunk).unwrap();
    let map = out.as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(map.value_length(0), 2);
}

#[test]
fn test_map_from_arrays_length_mismatch() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let map_type = DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Arc::new(Field::new("key", DataType::Int64, false)),
                Arc::new(Field::new("value", DataType::Int64, true)),
            ])),
            false,
        )),
        false,
    );

    let key = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let val1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(10)), DataType::Int64);
    let val2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(20)), DataType::Int64);
    let keys = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![key],
        },
        list_type.clone(),
    );
    let values = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![val1, val2],
        },
        list_type,
    );
    let expr = typed_null(&mut arena, map_type);

    let err =
        eval_map_from_arrays(&arena, expr, &[keys, values], &chunk).expect_err("must fail");
    assert!(err.contains("same length"));
}

#[test]
fn test_map_from_arrays_keeps_null_key_entries() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let map_type = DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Arc::new(Field::new("key", DataType::Int64, false)),
                Arc::new(Field::new("value", DataType::Int64, true)),
            ])),
            false,
        )),
        false,
    );

    let null_key = typed_null(&mut arena, DataType::Int64);
    let key2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let val1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(10)), DataType::Int64);
    let val2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(20)), DataType::Int64);

    let keys = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![null_key, key2],
        },
        list_type.clone(),
    );
    let values = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![val1, val2],
        },
        list_type,
    );
    let expr = typed_null(&mut arena, map_type);

    let out =
        eval_map_function("map_from_arrays", &arena, expr, &[keys, values], &chunk).unwrap();
    let map = out.as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(map.value_length(0), 2);
    let key_arr = map.keys();
    assert!(key_arr.is_null(0));
}

// ---------------------------------------------------------------------------
// Tests from dispatch.rs
// ---------------------------------------------------------------------------

use novarocks::exec::expr::function::FunctionKind;

#[test]
fn test_register_map_functions() {
    use std::collections::HashMap as StdHashMap;
    use novarocks::exec::expr::function::map::register;

    let mut m = StdHashMap::new();
    register(&mut m);
    assert_eq!(m.get("map"), Some(&FunctionKind::Map("map_from_arrays")));
    assert_eq!(m.get("map_size"), Some(&FunctionKind::Map("map_size")));
    assert_eq!(
        m.get("transform_keys"),
        Some(&FunctionKind::Map("map_apply"))
    );
    assert_eq!(
        m.get("transform_values"),
        Some(&FunctionKind::Map("map_apply"))
    );
    assert_eq!(
        m.get("cardinality"),
        Some(&FunctionKind::Map("cardinality"))
    );
}
