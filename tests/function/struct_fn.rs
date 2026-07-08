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
    Array, ArrayRef, Decimal128Array, Int32Array, Int64Array, ListArray, StringArray, StructArray,
};
use arrow::datatypes::{DataType, Field, Fields};
use arrow::record_batch::RecordBatch;
use novarocks::common::ids::SlotId;
use novarocks::exec::chunk::{Chunk, ChunkSchema};
use novarocks::exec::expr::ExprId;
use novarocks::exec::expr::function::FunctionKind;
use novarocks::exec::expr::function::struct_fn::eval_struct_function;
use novarocks::exec::expr::{ExprArena, ExprNode, LiteralValue};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn literal_i64(arena: &mut ExprArena, v: i64) -> ExprId {
    arena.push(ExprNode::Literal(LiteralValue::Int64(v)))
}

fn literal_string(arena: &mut ExprArena, v: &str) -> ExprId {
    arena.push(ExprNode::Literal(LiteralValue::Utf8(v.to_string())))
}

fn chunk_from_array(name: &str, slot_id: SlotId, array: ArrayRef) -> Chunk {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
        name,
        array.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
    let chunk_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[slot_id])
            .expect("chunk schema");
    Chunk::new_with_chunk_schema(batch, chunk_schema)
}

fn decimal_array(values: Vec<i128>, precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values.into_iter().map(Some).collect::<Vec<_>>())
            .with_precision_and_scale(precision, scale)
            .expect("decimal type"),
    )
}

// ---------------------------------------------------------------------------
// Tests migrated from struct_fn/dispatch.rs
// ---------------------------------------------------------------------------

#[test]
fn test_struct_register() {
    use novarocks::exec::expr::function::struct_fn::register;
    use std::collections::HashMap;
    let mut m = HashMap::new();
    register(&mut m);
    assert_eq!(m.get("row"), Some(&FunctionKind::StructFn("row")));
    assert_eq!(m.get("struct"), Some(&FunctionKind::StructFn("row")));
}

// ---------------------------------------------------------------------------
// Tests migrated from struct_fn/struct_func.rs
// ---------------------------------------------------------------------------

#[test]
fn test_row() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let struct_type = DataType::Struct(Fields::from(vec![
        Arc::new(Field::new("c0", DataType::Int64, true)),
        Arc::new(Field::new("c1", DataType::Utf8, true)),
    ]));
    let expr = common::typed_null(&mut arena, struct_type);
    let a = literal_i64(&mut arena, 1);
    let b = literal_string(&mut arena, "x");
    let out = eval_struct_function("row", &arena, expr, &[a, b], &chunk).unwrap();
    let st = out.as_any().downcast_ref::<StructArray>().unwrap();
    let c0 = st.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let c1 = st.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(c0.value(0), 1);
    assert_eq!(c1.value(0), "x");
}

#[test]
fn test_struct_alias() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let struct_type = DataType::Struct(Fields::from(vec![Arc::new(Field::new(
        "c0",
        DataType::Int64,
        true,
    ))]));
    let expr = common::typed_null(&mut arena, struct_type);
    let a = literal_i64(&mut arena, 7);
    let out = eval_struct_function("struct", &arena, expr, &[a], &chunk).unwrap();
    let st = out.as_any().downcast_ref::<StructArray>().unwrap();
    let c0 = st.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(c0.value(0), 7);
}

#[test]
fn test_row_rejects_decimal_precision_drift() {
    let mut arena = ExprArena::default();
    let slot_id = SlotId::new(11);
    let chunk = chunk_from_array("d", slot_id, decimal_array(vec![1234], 38, 2));
    let struct_type = DataType::Struct(Fields::from(vec![Arc::new(Field::new(
        "d",
        DataType::Decimal128(10, 2),
        true,
    ))]));
    let expr = common::typed_null(&mut arena, struct_type);
    let value = arena.push_typed(ExprNode::SlotId(slot_id), DataType::Decimal128(38, 2));

    let err = eval_struct_function("row", &arena, expr, &[value], &chunk)
        .expect_err("row must reject actual-widen decimal child");

    assert!(
        err.contains("row field type mismatch at field[0]"),
        "err={err}"
    );
    assert!(err.contains("Decimal128(10, 2)"), "err={err}");
    assert!(err.contains("Decimal128(38, 2)"), "err={err}");
}

#[test]
fn test_struct_expr_nested_list_casts_declared_item_type() {
    let mut arena = ExprArena::default();
    let slot_id = SlotId::new(12);
    let list = Arc::new(ListArray::from_iter_primitive::<
        arrow::datatypes::Int64Type,
        _,
        _,
    >(vec![Some(vec![Some(7_i64)])])) as ArrayRef;
    let chunk = chunk_from_array("xs", slot_id, list);
    let expected_list = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
    let struct_type = DataType::Struct(Fields::from(vec![Arc::new(Field::new(
        "xs",
        expected_list,
        true,
    ))]));
    let child = arena.push_typed(
        ExprNode::SlotId(slot_id),
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
    );
    let expr = arena.push_typed(
        ExprNode::StructExpr {
            fields: vec![child],
        },
        struct_type,
    );

    let out = arena
        .eval(expr, &chunk)
        .expect("struct expr must cast nested list item drift");

    let out = out.as_any().downcast_ref::<StructArray>().unwrap();
    let list = out.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.values().data_type(), &DataType::Int32);
    let values = list.values().as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(values.value(0), 7);
}

#[test]
fn test_named_struct() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let struct_type = DataType::Struct(Fields::from(vec![
        Arc::new(Field::new("a", DataType::Int64, true)),
        Arc::new(Field::new("b", DataType::Utf8, true)),
    ]));
    let expr = common::typed_null(&mut arena, struct_type);
    let name_a = literal_string(&mut arena, "a");
    let value_a = literal_i64(&mut arena, 42);
    let name_b = literal_string(&mut arena, "b");
    let value_b = literal_string(&mut arena, "ok");
    let out = eval_struct_function(
        "named_struct",
        &arena,
        expr,
        &[name_a, value_a, name_b, value_b],
        &chunk,
    )
    .unwrap();
    let st = out.as_any().downcast_ref::<StructArray>().unwrap();
    let c0 = st.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let c1 = st.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(c0.value(0), 42);
    assert_eq!(c1.value(0), "ok");
}

#[test]
fn test_named_struct_arg_validation() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let struct_type = DataType::Struct(Fields::from(vec![Arc::new(Field::new(
        "a",
        DataType::Int64,
        true,
    ))]));
    let expr = common::typed_null(&mut arena, struct_type);
    let name_a = literal_string(&mut arena, "a");
    let value_a = literal_i64(&mut arena, 42);
    let extra = literal_string(&mut arena, "dangling");
    let err = eval_struct_function(
        "named_struct",
        &arena,
        expr,
        &[name_a, value_a, extra],
        &chunk,
    )
    .expect_err("must fail");
    assert!(err.contains("even number"));
}

// ---------------------------------------------------------------------------
// Tests migrated from struct_fn/subfield.rs
// ---------------------------------------------------------------------------

#[test]
fn test_subfield_extracts_field() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();

    let struct_type = DataType::Struct(Fields::from(vec![
        Arc::new(Field::new("a", DataType::Int64, true)),
        Arc::new(Field::new("b", DataType::Utf8, true)),
    ]));
    let out_expr = common::typed_null(&mut arena, DataType::Utf8);

    let f0 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(7)), DataType::Int64);
    let f1 = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8("x".to_string())),
        DataType::Utf8,
    );
    let struct_expr = arena.push_typed(
        ExprNode::StructExpr {
            fields: vec![f0, f1],
        },
        struct_type,
    );
    let field_name = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8("b".to_string())),
        DataType::Utf8,
    );

    let out = eval_struct_function(
        "subfield",
        &arena,
        out_expr,
        &[struct_expr, field_name],
        &chunk,
    )
    .unwrap();
    let out = out.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(out.value(0), "x");
}

#[test]
fn test_subfield_parent_null_propagates() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();

    let struct_type = DataType::Struct(Fields::from(vec![Arc::new(Field::new(
        "a",
        DataType::Int64,
        true,
    ))]));
    let out_expr = common::typed_null(&mut arena, DataType::Int64);
    let struct_expr = common::typed_null(&mut arena, struct_type);
    let field_name = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8("a".to_string())),
        DataType::Utf8,
    );

    let out = eval_struct_function(
        "subfield",
        &arena,
        out_expr,
        &[struct_expr, field_name],
        &chunk,
    )
    .unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(out.is_null(0));
}
