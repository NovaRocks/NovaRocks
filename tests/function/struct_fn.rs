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
use novarocks::exec::expr::function::struct_fn::eval_struct_function;
use novarocks::exec::expr::function::FunctionKind;
use novarocks::exec::expr::{ExprArena, ExprNode, LiteralValue};
use novarocks::exec::expr::ExprId;
use arrow::array::{Array, ArrayRef, Int64Array, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Fields};
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
    let err = eval_struct_function("named_struct", &arena, expr, &[name_a, value_a, extra], &chunk)
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

    let out = eval_struct_function("subfield", &arena, out_expr, &[struct_expr, field_name], &chunk).unwrap();
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

    let out = eval_struct_function("subfield", &arena, out_expr, &[struct_expr, field_name], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(out.is_null(0));
}
