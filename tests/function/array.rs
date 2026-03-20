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
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, LargeListArray, ListArray,
    StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use novarocks::exec::expr::function::array::*;
use novarocks::exec::expr::{ExprArena, ExprNode, LiteralValue};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// all_match tests
// ---------------------------------------------------------------------------

#[test]
fn test_all_match_basic() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr = common::typed_null(&mut arena, DataType::Boolean);
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Boolean, true)));

    let t = arena.push_typed(
        ExprNode::Literal(LiteralValue::Bool(true)),
        DataType::Boolean,
    );
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![t, t],
        },
        list_type,
    );

    let out = eval_array_function("all_match", &arena, expr, &[arr], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(out.value(0));
}

#[test]
fn test_all_match_false_and_null() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr = common::typed_null(&mut arena, DataType::Boolean);
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Boolean, true)));

    let t = arena.push_typed(
        ExprNode::Literal(LiteralValue::Bool(true)),
        DataType::Boolean,
    );
    let f = arena.push_typed(
        ExprNode::Literal(LiteralValue::Bool(false)),
        DataType::Boolean,
    );
    let null_elem = common::typed_null(&mut arena, DataType::Boolean);

    let arr_false = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![t, f, null_elem],
        },
        list_type.clone(),
    );
    let out_false =
        eval_array_function("all_match", &arena, expr, &[arr_false], &chunk).unwrap();
    let out_false = out_false.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(!out_false.value(0));

    let expr2 = common::typed_null(&mut arena, DataType::Boolean);
    let arr_null = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![t, null_elem],
        },
        list_type,
    );
    let out_null =
        eval_array_function("all_match", &arena, expr2, &[arr_null], &chunk).unwrap();
    let out_null = out_null.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(out_null.is_null(0));
}

// ---------------------------------------------------------------------------
// any_match tests
// ---------------------------------------------------------------------------

#[test]
fn test_any_match_basic() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr = common::typed_null(&mut arena, DataType::Boolean);
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Boolean, true)));

    let t = arena.push_typed(
        ExprNode::Literal(LiteralValue::Bool(true)),
        DataType::Boolean,
    );
    let f = arena.push_typed(
        ExprNode::Literal(LiteralValue::Bool(false)),
        DataType::Boolean,
    );
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![f, t],
        },
        list_type,
    );

    let out = eval_array_function("any_match", &arena, expr, &[arr], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(out.value(0));
}

#[test]
fn test_any_match_false_and_null() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr = common::typed_null(&mut arena, DataType::Boolean);
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Boolean, true)));

    let f = arena.push_typed(
        ExprNode::Literal(LiteralValue::Bool(false)),
        DataType::Boolean,
    );
    let null_elem = common::typed_null(&mut arena, DataType::Boolean);

    let arr_false = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![f, f],
        },
        list_type.clone(),
    );
    let out_false =
        eval_array_function("any_match", &arena, expr, &[arr_false], &chunk).unwrap();
    let out_false = out_false.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(!out_false.value(0));

    let expr2 = common::typed_null(&mut arena, DataType::Boolean);
    let arr_null = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![f, null_elem],
        },
        list_type,
    );
    let out_null =
        eval_array_function("any_match", &arena, expr2, &[arr_null], &chunk).unwrap();
    let out_null = out_null.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(out_null.is_null(0));
}

// ---------------------------------------------------------------------------
// array_append tests
// ---------------------------------------------------------------------------

#[test]
fn test_array_append_basic() {
    use arrow::array::Int64Array;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, list_type.clone());

    let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let v2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v1, v2],
        },
        list_type,
    );
    let target = common::literal_i64(&mut arena, 3);

    let out =
        eval_array_function("array_append", &arena, expr, &[arr, target], &chunk).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.values(), &[1, 2, 3]);
}

#[test]
fn test_array_append_null_array_and_null_element() {
    use arrow::array::Int64Array;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));

    let expr = common::typed_null(&mut arena, list_type.clone());
    let arr_null = common::typed_null(&mut arena, list_type.clone());
    let target = common::literal_i64(&mut arena, 1);
    let out_null =
        eval_array_function("array_append", &arena, expr, &[arr_null, target], &chunk).unwrap();
    let out_null = out_null.as_any().downcast_ref::<ListArray>().unwrap();
    assert!(out_null.is_null(0));

    let expr2 = common::typed_null(&mut arena, list_type.clone());
    let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let arr = arena.push_typed(ExprNode::ArrayExpr { elements: vec![v1] }, list_type);
    let target_null = common::typed_null(&mut arena, DataType::Int64);
    let out = eval_array_function("array_append", &arena, expr2, &[arr, target_null], &chunk)
        .unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!(values.value(0), 1);
    assert!(values.is_null(1));
}

// ---------------------------------------------------------------------------
// array_avg tests
// ---------------------------------------------------------------------------

#[test]
fn test_array_avg_int64() {
    use arrow::array::Decimal128Array;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr = common::typed_null(&mut arena, DataType::Float64);
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let v3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v1, v3],
        },
        list_type,
    );

    let out = eval_array_function("array_avg", &arena, expr, &[arr], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((out.value(0) - 2.0).abs() < 1e-12);
}

#[test]
fn test_array_avg_null_counts_in_denominator_and_empty() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr = common::typed_null(&mut arena, DataType::Float64);
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let null_elem = common::typed_null(&mut arena, DataType::Int64);
    let v2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![null_elem, v2],
        },
        list_type.clone(),
    );

    let out = eval_array_function("array_avg", &arena, expr, &[arr], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((out.value(0) - 1.0).abs() < 1e-12);

    let expr2 = common::typed_null(&mut arena, DataType::Float64);
    let empty = arena.push_typed(ExprNode::ArrayExpr { elements: vec![] }, list_type);
    let out_empty = eval_array_function("array_avg", &arena, expr2, &[empty], &chunk).unwrap();
    let out_empty = out_empty.as_any().downcast_ref::<Float64Array>().unwrap();
    assert!(out_empty.is_null(0));
}

#[test]
fn test_array_avg_all_null_returns_null() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr = common::typed_null(&mut arena, DataType::Float64);
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let null_elem = common::typed_null(&mut arena, DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![null_elem, null_elem],
        },
        list_type,
    );
    let out = eval_array_function("array_avg", &arena, expr, &[arr], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
    assert!(out.is_null(0));
}

#[test]
fn test_array_avg_decimal_rounds_exactly() {
    use arrow::array::Decimal128Array;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr = common::typed_null(&mut arena, DataType::Decimal128(18, 8));
    let list_type = DataType::List(Arc::new(Field::new(
        "item",
        DataType::Decimal128(9, 2),
        true,
    )));
    let v1 = arena.push_typed(
        ExprNode::Literal(LiteralValue::Decimal128 {
            value: 100,
            precision: 9,
            scale: 2,
        }),
        DataType::Decimal128(9, 2),
    );
    let v2 = arena.push_typed(
        ExprNode::Literal(LiteralValue::Decimal128 {
            value: 200,
            precision: 9,
            scale: 2,
        }),
        DataType::Decimal128(9, 2),
    );
    let v3 = arena.push_typed(
        ExprNode::Literal(LiteralValue::Decimal128 {
            value: 200,
            precision: 9,
            scale: 2,
        }),
        DataType::Decimal128(9, 2),
    );
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v1, v2, v3],
        },
        list_type,
    );

    let out = eval_array_function("array_avg", &arena, expr, &[arr], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<Decimal128Array>().unwrap();
    assert_eq!(out.value(0), 166_666_667);
}

// ---------------------------------------------------------------------------
// array_concat tests
// ---------------------------------------------------------------------------

#[test]
fn test_array_concat_basic() {
    use arrow::array::Int64Array;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, list_type.clone());

    let a1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let a2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let b1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
    let arr1 = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![a1, a2],
        },
        list_type.clone(),
    );
    let arr2 = arena.push_typed(ExprNode::ArrayExpr { elements: vec![b1] }, list_type);

    let out = eval_array_function("array_concat", &arena, expr, &[arr1, arr2], &chunk).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.len(), 3);
    assert_eq!(values.value(0), 1);
    assert_eq!(values.value(1), 2);
    assert_eq!(values.value(2), 3);
}

// ---------------------------------------------------------------------------
// array_contains tests
// ---------------------------------------------------------------------------

#[test]
fn test_array_contains_true_false() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr = common::typed_null(&mut arena, DataType::Boolean);

    let e1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let e2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![e1, e2],
        },
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
    );
    let target = common::literal_i64(&mut arena, 2);

    let out =
        eval_array_function("array_contains", &arena, expr, &[arr, target], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(out.value(0));
}

#[test]
fn test_array_contains_null_target() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr = common::typed_null(&mut arena, DataType::Boolean);
    let e1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr { elements: vec![e1] },
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
    );
    let target = common::typed_null(&mut arena, DataType::Int64);

    let out = eval_array_contains(&arena, expr, &[arr, target], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(!out.is_null(0));
    assert!(!out.value(0));
}

// ---------------------------------------------------------------------------
// array_contains_all tests
// ---------------------------------------------------------------------------

#[test]
fn test_array_contains_all_basic() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, DataType::Boolean);

    let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let v2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let v3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
    let left = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v1, v2, v3],
        },
        list_type.clone(),
    );
    let right = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v2, v1],
        },
        list_type,
    );

    let out = eval_array_function("array_contains_all", &arena, expr, &[left, right], &chunk)
        .unwrap();
    let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(out.value(0));
}

#[test]
fn test_array_contains_all_with_null_element() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, DataType::Boolean);

    let null_elem = common::typed_null(&mut arena, DataType::Int64);
    let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let left = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v1, null_elem],
        },
        list_type.clone(),
    );
    let right = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![null_elem],
        },
        list_type,
    );

    let out = eval_array_function("array_contains_all", &arena, expr, &[left, right], &chunk)
        .unwrap();
    let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(out.value(0));
}

// ---------------------------------------------------------------------------
// array_contains_seq tests
// ---------------------------------------------------------------------------

#[test]
fn test_array_contains_seq_basic() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, DataType::Boolean);

    let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let v2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let v3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
    let v4 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(4)), DataType::Int64);
    let left = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v1, v2, v3, v4],
        },
        list_type.clone(),
    );
    let right = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v2, v3],
        },
        list_type,
    );

    let out = eval_array_function("array_contains_seq", &arena, expr, &[left, right], &chunk)
        .unwrap();
    let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(out.value(0));
}

#[test]
fn test_array_contains_seq_with_nulls() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, DataType::Boolean);

    let null_elem = common::typed_null(&mut arena, DataType::Int64);
    let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let left = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v1, null_elem],
        },
        list_type.clone(),
    );
    let right = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![null_elem],
        },
        list_type.clone(),
    );
    let out = eval_array_function("array_contains_seq", &arena, expr, &[left, right], &chunk)
        .unwrap();
    let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(out.value(0));

    let v2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let left2 = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v1, v2, null_elem],
        },
        list_type.clone(),
    );
    let right2 = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v2, v1],
        },
        list_type,
    );
    let expr2 = common::typed_null(&mut arena, DataType::Boolean);
    let out2 = eval_array_function(
        "array_contains_seq",
        &arena,
        expr2,
        &[left2, right2],
        &chunk,
    )
    .unwrap();
    let out2 = out2.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(!out2.value(0));
}

// ---------------------------------------------------------------------------
// array_cum_sum tests
// ---------------------------------------------------------------------------

#[test]
fn test_array_cum_sum_int64() {
    use arrow::array::Int64Array;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, list_type.clone());

    let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let v2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let v3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v1, v2, v3],
        },
        list_type,
    );

    let out = eval_array_function("array_cum_sum", &arena, expr, &[arr], &chunk).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.values(), &[1, 3, 6]);
}

#[test]
fn test_array_cum_sum_with_nulls() {
    use arrow::array::Int64Array;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, list_type.clone());
    let null_elem = common::typed_null(&mut arena, DataType::Int64);
    let v2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let v3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v2, null_elem, v3],
        },
        list_type,
    );

    let out = eval_array_function("array_cum_sum", &arena, expr, &[arr], &chunk).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.len(), 3);
    assert_eq!(values.value(0), 2);
    assert!(values.is_null(1));
    assert_eq!(values.value(2), 5);
}

// ---------------------------------------------------------------------------
// array_difference tests
// ---------------------------------------------------------------------------

#[test]
fn test_array_difference_int64() {
    use arrow::array::Int64Array;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let out_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, out_type.clone());
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));

    let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let v3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
    let v6 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(6)), DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v1, v3, v6],
        },
        list_type,
    );

    let out = eval_array_function("array_difference", &arena, expr, &[arr], &chunk).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.values(), &[0, 2, 3]);
}

#[test]
fn test_array_difference_with_nulls() {
    use arrow::array::Int64Array;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let out_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, out_type.clone());
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let null_elem = common::typed_null(&mut arena, DataType::Int64);
    let v2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let v4 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(4)), DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![null_elem, v2, v4],
        },
        list_type,
    );

    let out = eval_array_function("array_difference", &arena, expr, &[arr], &chunk).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.len(), 3);
    assert!(values.is_null(0));
    assert!(values.is_null(1));
    assert_eq!(values.value(2), 2);
}

// ---------------------------------------------------------------------------
// array_distinct tests
// ---------------------------------------------------------------------------

#[test]
fn test_array_distinct_basic() {
    use arrow::array::Int64Array;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, list_type.clone());

    let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let v2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v1, v2, v1, v2],
        },
        list_type,
    );

    let out = eval_array_function("array_distinct", &arena, expr, &[arr], &chunk).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!(values.value(0), 1);
    assert_eq!(values.value(1), 2);
}

#[test]
fn test_array_distinct_keep_single_null() {
    use arrow::array::Int64Array;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, list_type.clone());

    let null_elem = common::typed_null(&mut arena, DataType::Int64);
    let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let v2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![null_elem, v1, null_elem, v2, v1],
        },
        list_type,
    );

    let out = eval_array_function("array_distinct", &arena, expr, &[arr], &chunk).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.len(), 3);
    assert!(values.is_null(0));
    assert_eq!(values.value(1), 1);
    assert_eq!(values.value(2), 2);
}

// ---------------------------------------------------------------------------
// array_filter tests
// ---------------------------------------------------------------------------

#[test]
fn test_array_filter_basic() {
    use arrow::array::Int64Array;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let src_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let filter_type = DataType::List(Arc::new(Field::new("item", DataType::Boolean, true)));
    let expr = common::typed_null(&mut arena, src_type.clone());

    let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let v2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let v3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
    let src = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v1, v2, v3],
        },
        src_type,
    );
    let t = arena.push_typed(
        ExprNode::Literal(LiteralValue::Bool(true)),
        DataType::Boolean,
    );
    let f = arena.push_typed(
        ExprNode::Literal(LiteralValue::Bool(false)),
        DataType::Boolean,
    );
    let filter = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![t, f, t],
        },
        filter_type,
    );

    let out =
        eval_array_function("array_filter", &arena, expr, &[src, filter], &chunk).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.values(), &[1, 3]);
}

#[test]
fn test_array_filter_filter_null_row_returns_empty() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let src_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let filter_type = DataType::List(Arc::new(Field::new("item", DataType::Boolean, true)));
    let expr = common::typed_null(&mut arena, src_type.clone());

    let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let src = arena.push_typed(ExprNode::ArrayExpr { elements: vec![v1] }, src_type);
    let filter = common::typed_null(&mut arena, filter_type);

    let out =
        eval_array_function("array_filter", &arena, expr, &[src, filter], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(out.value_length(0), 0);
    assert!(!out.is_null(0));
}

// ---------------------------------------------------------------------------
// array_flatten tests
// ---------------------------------------------------------------------------

#[test]
fn test_array_flatten_basic() {
    use arrow::array::Int64Array;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let inner_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let outer_type = DataType::List(Arc::new(Field::new("item", inner_type.clone(), true)));
    let expr = common::typed_null(&mut arena, inner_type.clone());

    let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let v2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let v3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
    let inner1 = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v1, v2],
        },
        inner_type.clone(),
    );
    let inner2 = arena.push_typed(
        ExprNode::ArrayExpr { elements: vec![v3] },
        inner_type.clone(),
    );
    let outer = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![inner1, inner2],
        },
        outer_type,
    );

    let out = eval_array_function("array_flatten", &arena, expr, &[outer], &chunk).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.values(), &[1, 2, 3]);
}

#[test]
fn test_array_flatten_skip_null_inner_and_keep_null_outer() {
    use arrow::array::Int64Array;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let inner_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let outer_type = DataType::List(Arc::new(Field::new("item", inner_type.clone(), true)));
    let expr = common::typed_null(&mut arena, inner_type.clone());

    let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let inner1 = arena.push_typed(
        ExprNode::ArrayExpr { elements: vec![v1] },
        inner_type.clone(),
    );
    let null_inner = common::typed_null(&mut arena, inner_type.clone());
    let outer = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![inner1, null_inner],
        },
        outer_type.clone(),
    );
    let out = eval_array_function("array_flatten", &arena, expr, &[outer], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.values(), &[1]);

    let expr2 = common::typed_null(&mut arena, inner_type);
    let outer_null = common::typed_null(&mut arena, outer_type);
    let out_null =
        eval_array_function("array_flatten", &arena, expr2, &[outer_null], &chunk).unwrap();
    let out_null = out_null.as_any().downcast_ref::<ListArray>().unwrap();
    assert!(out_null.is_null(0));
}

// ---------------------------------------------------------------------------
// array_generate tests
// ---------------------------------------------------------------------------

#[test]
fn test_array_generate_one_arg() {
    use arrow::datatypes::TimeUnit;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, list_type);
    let stop = common::literal_i64(&mut arena, 3);

    let out = eval_array_function("array_generate", &arena, expr, &[stop], &chunk).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.values(), &[1, 2, 3]);
}

#[test]
fn test_array_generate_two_args_desc() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, list_type);
    let start = common::literal_i64(&mut arena, 3);
    let stop = common::literal_i64(&mut arena, 1);

    let out =
        eval_array_function("array_generate", &arena, expr, &[start, stop], &chunk).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.values(), &[3, 2, 1]);
}

#[test]
fn test_array_generate_three_args_and_empty() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, list_type.clone());
    let start = common::literal_i64(&mut arena, 1);
    let stop = common::literal_i64(&mut arena, 5);
    let step = common::literal_i64(&mut arena, 2);

    let out = eval_array_function("array_generate", &arena, expr, &[start, stop, step], &chunk)
        .unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.values(), &[1, 3, 5]);

    let expr2 = common::typed_null(&mut arena, list_type);
    let start2 = common::literal_i64(&mut arena, 3);
    let stop2 = common::literal_i64(&mut arena, 2);
    let step2 = common::literal_i64(&mut arena, 1);
    let out2 = eval_array_function(
        "array_generate",
        &arena,
        expr2,
        &[start2, stop2, step2],
        &chunk,
    )
    .unwrap();
    let list2 = out2.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list2.value_length(0), 0);
}

#[test]
fn test_array_generate_date_with_unit_arg() {
    use arrow::datatypes::TimeUnit;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new(
        "item",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        true,
    )));
    let expr = common::typed_null(&mut arena, list_type);
    let start = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8(
            "2025-10-01".to_string(),
        )),
        DataType::Utf8,
    );
    let stop = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8(
            "2025-10-05".to_string(),
        )),
        DataType::Utf8,
    );
    let step = common::literal_i64(&mut arena, 1);
    let unit = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8(
            "day".to_string(),
        )),
        DataType::Utf8,
    );
    let out = eval_array_function(
        "array_generate",
        &arena,
        expr,
        &[start, stop, step, unit],
        &chunk,
    )
    .unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.value_length(0), 5);
}

// ---------------------------------------------------------------------------
// array_intersect tests
// ---------------------------------------------------------------------------

#[test]
fn test_array_intersect_basic() {
    use arrow::array::{Decimal128Array, Int16Array};
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, list_type.clone());

    let a1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let a2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let a3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let b1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let b2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
    let arr1 = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![a1, a2, a3],
        },
        list_type.clone(),
    );
    let arr2 = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![b1, b2],
        },
        list_type,
    );

    let out =
        eval_array_function("array_intersect", &arena, expr, &[arr1, arr2], &chunk).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.len(), 1);
    assert_eq!(values.value(0), 2);
}

#[test]
fn test_array_intersect_keeps_shared_null_once() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, list_type.clone());

    let a1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let an = common::typed_null(&mut arena, DataType::Int64);
    let a2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let bn = common::typed_null(&mut arena, DataType::Int64);
    let b2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let arr1 = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![a1, an, a2],
        },
        list_type.clone(),
    );
    let arr2 = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![bn, b2],
        },
        list_type,
    );

    let out =
        eval_array_function("array_intersect", &arena, expr, &[arr1, arr2], &chunk).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!(values.value(0), 2);
    assert!(values.is_null(1));
}

#[test]
fn test_array_intersect_preserves_starrocks_hash_order() {
    use arrow::array::Int16Array;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int16, true)));
    let expr = common::typed_null(&mut arena, list_type.clone());

    let v100_a = arena.push_typed(ExprNode::Literal(LiteralValue::Int16(100)), DataType::Int16);
    let v200 = arena.push_typed(ExprNode::Literal(LiteralValue::Int16(200)), DataType::Int16);
    let v300_a = arena.push_typed(ExprNode::Literal(LiteralValue::Int16(300)), DataType::Int16);
    let v100_b = arena.push_typed(ExprNode::Literal(LiteralValue::Int16(100)), DataType::Int16);
    let v300_b = arena.push_typed(ExprNode::Literal(LiteralValue::Int16(300)), DataType::Int16);
    let v900 = arena.push_typed(ExprNode::Literal(LiteralValue::Int16(900)), DataType::Int16);
    let arr1 = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v100_a, v200, v300_a],
        },
        list_type.clone(),
    );
    let arr2 = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v100_b, v300_b, v900],
        },
        list_type,
    );

    let out =
        eval_array_function("array_intersect", &arena, expr, &[arr1, arr2], &chunk).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list.values().as_any().downcast_ref::<Int16Array>().unwrap();
    assert_eq!(values.values(), &[300, 100]);
}

#[test]
fn test_array_intersect_preserves_decimalv2_hash_order() {
    use arrow::array::Decimal128Array;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let decimal_type = DataType::Decimal128(27, 9);
    let list_type = DataType::List(Arc::new(Field::new("item", decimal_type.clone(), true)));
    let expr = common::typed_null(&mut arena, list_type.clone());

    let v123_a = arena.push_typed(
        ExprNode::Literal(LiteralValue::Decimal128 {
            value: 123_450_000_000,
            precision: 27,
            scale: 9,
        }),
        decimal_type.clone(),
    );
    let v456_a = arena.push_typed(
        ExprNode::Literal(LiteralValue::Decimal128 {
            value: 456_780_000_000,
            precision: 27,
            scale: 9,
        }),
        decimal_type.clone(),
    );
    let v789_a = arena.push_typed(
        ExprNode::Literal(LiteralValue::Decimal128 {
            value: 789_010_000_000,
            precision: 27,
            scale: 9,
        }),
        decimal_type.clone(),
    );
    let v123_b = arena.push_typed(
        ExprNode::Literal(LiteralValue::Decimal128 {
            value: 123_450_000_000,
            precision: 27,
            scale: 9,
        }),
        decimal_type.clone(),
    );
    let v456_b = arena.push_typed(
        ExprNode::Literal(LiteralValue::Decimal128 {
            value: 456_780_000_000,
            precision: 27,
            scale: 9,
        }),
        decimal_type.clone(),
    );
    let v789_b = arena.push_typed(
        ExprNode::Literal(LiteralValue::Decimal128 {
            value: 789_010_000_000,
            precision: 27,
            scale: 9,
        }),
        decimal_type.clone(),
    );
    let arr1 = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v123_a, v456_a, v789_a],
        },
        list_type.clone(),
    );
    let arr2 = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v123_b, v456_b, v789_b],
        },
        list_type,
    );

    let out =
        eval_array_function("array_intersect", &arena, expr, &[arr1, arr2], &chunk).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list
        .values()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(
        values.values(),
        &[123_450_000_000, 789_010_000_000, 456_780_000_000]
    );
}

// ---------------------------------------------------------------------------
// array_join tests
// ---------------------------------------------------------------------------

#[test]
fn test_array_join_ignore_null() {
    use novarocks::exec::expr::ExprId;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, DataType::Utf8);

    let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let v3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
    let null_elem = common::typed_null(&mut arena, DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v1, null_elem, v3],
        },
        list_type,
    );
    let sep = arena.push(ExprNode::Literal(LiteralValue::Utf8("-".to_string())));

    let out = eval_array_function("array_join", &arena, expr, &[arr, sep], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(out.value(0), "1-3");
}

#[test]
fn test_array_join_replace_null() {
    use novarocks::exec::expr::ExprId;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, DataType::Utf8);

    let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let v3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
    let null_elem = common::typed_null(&mut arena, DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v1, null_elem, v3],
        },
        list_type,
    );
    let sep = arena.push(ExprNode::Literal(LiteralValue::Utf8("-".to_string())));
    let null_repl = arena.push(ExprNode::Literal(LiteralValue::Utf8("X".to_string())));

    let out = eval_array_function("array_join", &arena, expr, &[arr, sep, null_repl], &chunk)
        .unwrap();
    let out = out.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(out.value(0), "1-X-3");
}

// ---------------------------------------------------------------------------
// array_length tests
// ---------------------------------------------------------------------------

#[test]
fn test_array_length_basic() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr = common::typed_null(&mut arena, DataType::Int32);
    let values =
        vec![arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64)];
    let arr = arena.push_typed(
        ExprNode::ArrayExpr { elements: values },
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
    );

    let out = eval_array_function("array_length", &arena, expr, &[arr], &chunk).unwrap();
    let out = out
        .as_any()
        .downcast_ref::<arrow::array::Int32Array>()
        .unwrap();
    assert_eq!(out.value(0), 1);
}

#[test]
fn test_array_length_null() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr = common::typed_null(&mut arena, DataType::Int64);
    let arr = common::typed_null(
        &mut arena,
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
    );
    let out = eval_array_length(&arena, expr, &[arr], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(out.is_null(0));
}

#[test]
fn test_array_length_const_cast_json_array() {
    use arrow::array::ArrayRef;
    use arrow::record_batch::RecordBatch;
    use novarocks::common::ids::SlotId;
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Int64);
    let lit = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8("[\"a\", \"b\", null]".to_string())),
        DataType::Utf8,
    );
    let arr_type = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
    let cast_expr = arena.push_typed(ExprNode::Cast(lit), arr_type);

    let array = Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "dummy",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
    let chunk = {
        let batch = batch;
        let chunk_schema = novarocks::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1)],
        )
        .expect("chunk schema");
        novarocks::exec::chunk::Chunk::new_with_chunk_schema(batch, chunk_schema)
    };

    let out = eval_array_length(&arena, expr, &[cast_expr], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(out.len(), 4);
    for i in 0..out.len() {
        assert_eq!(out.value(i), 3);
    }
}

// ---------------------------------------------------------------------------
// array_map tests
// ---------------------------------------------------------------------------

#[test]
fn eval_array_map_with_lambda() {
    use arrow::array::{ArrayRef, Int64Builder, ListBuilder};
    use arrow::record_batch::RecordBatch;
    use novarocks::common::ids::SlotId;

    let mut arena = ExprArena::default();
    let item_field = Arc::new(Field::new("item", DataType::Int64, true));
    let list_type = DataType::List(item_field.clone());

    // Lambda args slot refs
    let slot_x = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
    let slot_y = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int64);

    // Lambda body: x + y
    let add_expr = arena.push_typed(ExprNode::Add(slot_x, slot_y), DataType::Int64);

    let lambda = arena.push_typed(
        ExprNode::LambdaFunction {
            body: add_expr,
            arg_slots: vec![SlotId::new(1), SlotId::new(2), SlotId::new(3)],
            common_sub_exprs: Vec::new(),
            is_nondeterministic: false,
        },
        DataType::Utf8,
    );

    // Array arguments as slot refs
    let arr1 = arena.push_typed(ExprNode::SlotId(SlotId::new(10)), list_type.clone());
    let arr2 = arena.push_typed(ExprNode::SlotId(SlotId::new(11)), list_type.clone());
    let arr3 = arena.push_typed(ExprNode::SlotId(SlotId::new(12)), list_type.clone());

    let func = arena.push_typed(
        ExprNode::FunctionCall {
            kind: novarocks::exec::expr::function::FunctionKind::ArrayMap,
            args: vec![lambda, arr1, arr2, arr3],
        },
        list_type.clone(),
    );

    let col1 = {
        let mut builder = ListBuilder::new(Int64Builder::new());
        builder.values().append_value(1);
        builder.append(true);
        Arc::new(builder.finish()) as ArrayRef
    };
    let col2 = {
        let mut builder = ListBuilder::new(Int64Builder::new());
        builder.values().append_value(2);
        builder.append(true);
        Arc::new(builder.finish()) as ArrayRef
    };
    let col3 = {
        let mut builder = ListBuilder::new(Int64Builder::new());
        builder.values().append_value(4);
        builder.append(true);
        Arc::new(builder.finish()) as ArrayRef
    };

    let fields = vec![
        Field::new("a", list_type.clone(), true),
        Field::new("b", list_type.clone(), true),
        Field::new("c", list_type.clone(), true),
    ];
    let batch =
        RecordBatch::try_new(Arc::new(Schema::new(fields)), vec![col1, col2, col3]).unwrap();
    let chunk = {
        let batch = batch;
        let chunk_schema = novarocks::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(10), SlotId::new(11), SlotId::new(12)],
        )
        .expect("chunk schema");
        novarocks::exec::chunk::Chunk::new_with_chunk_schema(batch, chunk_schema)
    };

    let result = arena.eval(func, &chunk).unwrap();
    let list = result.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list.values();
    let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(values.len(), 1);
    assert_eq!(values.value(0), 3);
}

// ---------------------------------------------------------------------------
// array_max tests
// ---------------------------------------------------------------------------

#[test]
fn test_array_max_basic() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, DataType::Int64);
    let v5 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(5)), DataType::Int64);
    let v3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
    let v7 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(7)), DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v5, v3, v7],
        },
        list_type,
    );

    let out = eval_array_function("array_max", &arena, expr, &[arr], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(out.value(0), 7);
}

#[test]
fn test_array_max_empty_or_all_null() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, DataType::Int64);

    let empty = arena.push_typed(ExprNode::ArrayExpr { elements: vec![] }, list_type.clone());
    let out_empty = eval_array_function("array_max", &arena, expr, &[empty], &chunk).unwrap();
    let out_empty = out_empty.as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(out_empty.is_null(0));

    let null_elem = common::typed_null(&mut arena, DataType::Int64);
    let arr_all_null = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![null_elem, null_elem],
        },
        list_type,
    );
    let expr2 = common::typed_null(&mut arena, DataType::Int64);
    let out_null =
        eval_array_function("array_max", &arena, expr2, &[arr_all_null], &chunk).unwrap();
    let out_null = out_null.as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(out_null.is_null(0));
}

// ---------------------------------------------------------------------------
// array_min tests
// ---------------------------------------------------------------------------

#[test]
fn test_array_min_basic() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, DataType::Int64);
    let v5 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(5)), DataType::Int64);
    let v3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
    let v7 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(7)), DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v5, v3, v7],
        },
        list_type,
    );

    let out = eval_array_function("array_min", &arena, expr, &[arr], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(out.value(0), 3);
}

#[test]
fn test_array_min_empty_or_all_null() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, DataType::Int64);

    let empty = arena.push_typed(ExprNode::ArrayExpr { elements: vec![] }, list_type.clone());
    let out_empty = eval_array_function("array_min", &arena, expr, &[empty], &chunk).unwrap();
    let out_empty = out_empty.as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(out_empty.is_null(0));

    let null_elem = common::typed_null(&mut arena, DataType::Int64);
    let arr_all_null = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![null_elem, null_elem],
        },
        list_type,
    );
    let expr2 = common::typed_null(&mut arena, DataType::Int64);
    let out_null =
        eval_array_function("array_min", &arena, expr2, &[arr_all_null], &chunk).unwrap();
    let out_null = out_null.as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(out_null.is_null(0));
}

// ---------------------------------------------------------------------------
// array_position tests
// ---------------------------------------------------------------------------

#[test]
fn test_array_position_found() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr = common::typed_null(&mut arena, DataType::Int32);

    let e1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let e2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let e3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![e1, e2, e3],
        },
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
    );
    let target = common::literal_i64(&mut arena, 2);

    let out =
        eval_array_function("array_position", &arena, expr, &[arr, target], &chunk).unwrap();
    let out = out
        .as_any()
        .downcast_ref::<arrow::array::Int32Array>()
        .unwrap();
    assert_eq!(out.value(0), 2);
}

#[test]
fn test_array_position_not_found() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr = common::typed_null(&mut arena, DataType::Int64);

    let e1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr { elements: vec![e1] },
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
    );
    let target = common::literal_i64(&mut arena, 9);

    let out = eval_array_position(&arena, expr, &[arr, target], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(out.value(0), 0);
}

// ---------------------------------------------------------------------------
// array_remove tests
// ---------------------------------------------------------------------------

#[test]
fn test_array_remove_basic() {
    use arrow::array::Int64Array;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, list_type.clone());

    let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let v2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let v3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v1, v2, v3, v2],
        },
        list_type,
    );
    let target = common::literal_i64(&mut arena, 2);

    let out =
        eval_array_function("array_remove", &arena, expr, &[arr, target], &chunk).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!(values.value(0), 1);
    assert_eq!(values.value(1), 3);
}

#[test]
fn test_array_remove_null_target_removes_null_elements() {
    use arrow::array::Int64Array;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, list_type.clone());

    let null_elem = common::typed_null(&mut arena, DataType::Int64);
    let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![null_elem, v1, null_elem],
        },
        list_type,
    );
    let target = common::typed_null(&mut arena, DataType::Int64);

    let out =
        eval_array_function("array_remove", &arena, expr, &[arr, target], &chunk).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.len(), 1);
    assert_eq!(values.value(0), 1);
}

// ---------------------------------------------------------------------------
// array_repeat tests
// ---------------------------------------------------------------------------

#[test]
fn test_array_repeat_basic_and_negative() {
    use arrow::array::Int64Array;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));

    let expr = common::typed_null(&mut arena, list_type.clone());
    let source = common::literal_i64(&mut arena, 1);
    let count = common::literal_i64(&mut arena, 5);
    let out = eval_array_function("array_repeat", &arena, expr, &[source, count], &chunk)
        .expect("array_repeat should succeed");
    let out = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.values(), &[1, 1, 1, 1, 1]);

    let expr2 = common::typed_null(&mut arena, list_type);
    let neg_count = common::literal_i64(&mut arena, -1);
    let out2 = eval_array_function("array_repeat", &arena, expr2, &[source, neg_count], &chunk)
        .expect("array_repeat with negative should succeed");
    let out2 = out2.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(out2.value_length(0), 0);
}

#[test]
fn test_array_repeat_null_behaviour() {
    use arrow::array::Int64Array;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));

    let expr = common::typed_null(&mut arena, list_type.clone());
    let source_null = common::typed_null(&mut arena, DataType::Int64);
    let count = common::literal_i64(&mut arena, 3);
    let out = eval_array_function("array_repeat", &arena, expr, &[source_null, count], &chunk)
        .expect("array_repeat should succeed");
    let out = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.len(), 3);
    assert!(values.is_null(0));
    assert!(values.is_null(1));
    assert!(values.is_null(2));

    let expr2 = common::typed_null(&mut arena, list_type);
    let source = common::literal_i64(&mut arena, 2);
    let count_null = common::typed_null(&mut arena, DataType::Int64);
    let out2 =
        eval_array_function("array_repeat", &arena, expr2, &[source, count_null], &chunk)
            .expect("array_repeat should succeed");
    let out2 = out2.as_any().downcast_ref::<ListArray>().unwrap();
    assert!(out2.is_null(0));
}

// ---------------------------------------------------------------------------
// array_slice tests
// ---------------------------------------------------------------------------

#[test]
fn test_array_slice_two_args() {
    use arrow::array::Int64Array;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, list_type.clone());

    let e1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let e2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let e3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![e1, e2, e3],
        },
        list_type.clone(),
    );
    let offset = common::literal_i64(&mut arena, 2);

    let out = eval_array_function("array_slice", &arena, expr, &[arr, offset], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!(values.value(0), 2);
    assert_eq!(values.value(1), 3);
}

#[test]
fn test_array_slice_three_args() {
    use arrow::array::Int64Array;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, list_type.clone());

    let e1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let e2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let e3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![e1, e2, e3],
        },
        list_type.clone(),
    );
    let offset = common::literal_i64(&mut arena, 1);
    let len = common::literal_i64(&mut arena, 2);

    let out =
        eval_array_function("array_slice", &arena, expr, &[arr, offset, len], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!(values.value(0), 1);
    assert_eq!(values.value(1), 2);
}

#[test]
fn test_array_slice_zero_offset_returns_empty() {
    use arrow::array::Int64Array;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, list_type.clone());

    let e1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let e2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let e3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![e1, e2, e3],
        },
        list_type.clone(),
    );
    let offset = common::literal_i64(&mut arena, 0);
    let len = common::literal_i64(&mut arena, 2);

    let out =
        eval_array_function("array_slice", &arena, expr, &[arr, offset, len], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.len(), 0);
}

#[test]
fn test_array_slice_null_input() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, list_type.clone());
    let arr = common::typed_null(&mut arena, list_type);
    let offset = common::literal_i64(&mut arena, 1);
    let out = eval_array_slice(&arena, expr, &[arr, offset], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert!(out.is_null(0));
}

// ---------------------------------------------------------------------------
// array_sort tests
// ---------------------------------------------------------------------------

#[test]
fn test_array_sort_basic() {
    use arrow::array::Int64Array;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, list_type.clone());

    let v3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
    let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let v2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v3, v1, v2],
        },
        list_type,
    );

    let out = eval_array_function("array_sort", &arena, expr, &[arr], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.values(), &[1, 2, 3]);
}

#[test]
fn test_array_sort_null_first() {
    use arrow::array::Int64Array;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, list_type.clone());
    let null_elem = common::typed_null(&mut arena, DataType::Int64);
    let v2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v2, null_elem, v1],
        },
        list_type,
    );

    let out = eval_array_function("array_sort", &arena, expr, &[arr], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(values.is_null(0));
    assert_eq!(values.value(1), 1);
    assert_eq!(values.value(2), 2);
}

// ---------------------------------------------------------------------------
// array_sort_lambda tests
// ---------------------------------------------------------------------------

#[test]
fn test_array_sort_lambda_descending() {
    use arrow::array::Int64Array;
    use novarocks::common::ids::SlotId;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let item_type = DataType::Int64;
    let list_type = DataType::List(Arc::new(Field::new("item", item_type.clone(), true)));
    let expr = common::typed_null(&mut arena, list_type.clone());

    let x_slot = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), item_type.clone());
    let y_slot = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), item_type.clone());
    let body = arena.push_typed(ExprNode::Sub(y_slot, x_slot), item_type.clone());
    let lambda = arena.push_typed(
        ExprNode::LambdaFunction {
            body,
            arg_slots: vec![SlotId::new(1), SlotId::new(2)],
            common_sub_exprs: Vec::new(),
            is_nondeterministic: false,
        },
        DataType::Utf8,
    );

    let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), item_type.clone());
    let v2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), item_type.clone());
    let v5 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(5)), item_type);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v1, v5, v2],
        },
        list_type.clone(),
    );

    let out =
        eval_array_function("array_sort_lambda", &arena, expr, &[arr, lambda], &chunk).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.values(), &[5, 2, 1]);
}

// ---------------------------------------------------------------------------
// array_sortby tests
// ---------------------------------------------------------------------------

#[test]
fn test_array_sortby_basic() {
    use arrow::array::{Int64Array, StringArray};
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let src_type = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
    let key_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, src_type.clone());

    let a = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8("b".to_string())),
        DataType::Utf8,
    );
    let b = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8("a".to_string())),
        DataType::Utf8,
    );
    let c = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8("c".to_string())),
        DataType::Utf8,
    );
    let src = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![a, b, c],
        },
        src_type,
    );

    let k2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let k1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let k3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
    let key = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![k2, k1, k3],
        },
        key_type,
    );

    let out = eval_array_function("array_sortby", &arena, expr, &[src, key], &chunk).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(0), "a");
    assert_eq!(values.value(1), "b");
    assert_eq!(values.value(2), "c");
}

#[test]
fn test_array_sortby_key_null_keeps_source() {
    use arrow::array::Int64Array;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let src_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let key_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, src_type.clone());

    let v2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let src = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v2, v1],
        },
        src_type,
    );
    let key_null = common::typed_null(&mut arena, key_type);

    let out =
        eval_array_function("array_sortby", &arena, expr, &[src, key_null], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.values(), &[2, 1]);
}

// ---------------------------------------------------------------------------
// array_sum tests
// ---------------------------------------------------------------------------

#[test]
fn test_array_sum_int64() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr = common::typed_null(&mut arena, DataType::Int64);
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let v2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let v3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![v1, v2, v3],
        },
        list_type,
    );

    let out = eval_array_function("array_sum", &arena, expr, &[arr], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(out.value(0), 6);
}

#[test]
fn test_array_sum_skip_null_and_all_null() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr = common::typed_null(&mut arena, DataType::Int64);
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let null_elem = common::typed_null(&mut arena, DataType::Int64);
    let v2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![null_elem, v2, null_elem],
        },
        list_type.clone(),
    );
    let out = eval_array_function("array_sum", &arena, expr, &[arr], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(out.value(0), 2);

    let expr2 = common::typed_null(&mut arena, DataType::Int64);
    let arr_all_null = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![null_elem, null_elem],
        },
        list_type,
    );
    let out2 =
        eval_array_function("array_sum", &arena, expr2, &[arr_all_null], &chunk).unwrap();
    let out2 = out2.as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(out2.is_null(0));
}

#[test]
fn test_array_sum_bool() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr = common::typed_null(&mut arena, DataType::Int64);
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Boolean, true)));
    let t = arena.push_typed(
        ExprNode::Literal(LiteralValue::Bool(true)),
        DataType::Boolean,
    );
    let f = arena.push_typed(
        ExprNode::Literal(LiteralValue::Bool(false)),
        DataType::Boolean,
    );
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![t, f, t],
        },
        list_type,
    );

    let out = eval_array_function("array_sum", &arena, expr, &[arr], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(out.value(0), 2);
}

// ---------------------------------------------------------------------------
// arrays_overlap tests
// ---------------------------------------------------------------------------

#[test]
fn test_arrays_overlap() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let expr = common::typed_null(&mut arena, DataType::Boolean);

    let a1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
    let a2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let b1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let b2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
    let arr1 = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![a1, a2],
        },
        list_type.clone(),
    );
    let arr2 = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![b1, b2],
        },
        list_type,
    );

    let out =
        eval_array_function("arrays_overlap", &arena, expr, &[arr1, arr2], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(out.value(0));
}

// ---------------------------------------------------------------------------
// element_at tests
// ---------------------------------------------------------------------------

#[test]
fn test_element_at_returns_value() {
    use novarocks::exec::expr::ExprId;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let item_type = DataType::Int64;
    let list_type = DataType::List(Arc::new(Field::new("item", item_type.clone(), true)));
    let expr = common::typed_null(&mut arena, item_type.clone());

    let e1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(10)), DataType::Int64);
    let e2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(20)), DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![e1, e2],
        },
        list_type,
    );
    let idx_ok = arena.push_typed(ExprNode::Literal(LiteralValue::Int32(2)), DataType::Int32);

    let out = eval_array_function("element_at", &arena, expr, &[arr, idx_ok], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(out.value(0), 20);
}

#[test]
fn test_element_at_out_of_bounds_returns_null_without_check() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let item_type = DataType::Int64;
    let list_type = DataType::List(Arc::new(Field::new("item", item_type.clone(), true)));
    let expr = common::typed_null(&mut arena, item_type.clone());

    let e1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(10)), DataType::Int64);
    let e2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(20)), DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![e1, e2],
        },
        list_type,
    );
    let idx_oob = arena.push_typed(ExprNode::Literal(LiteralValue::Int32(3)), DataType::Int32);

    let out = eval_array_function("element_at", &arena, expr, &[arr, idx_oob], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(out.is_null(0));
}

#[test]
fn test_element_at_out_of_bounds_errors_with_check() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let item_type = DataType::Int64;
    let list_type = DataType::List(Arc::new(Field::new("item", item_type.clone(), true)));
    let expr = common::typed_null(&mut arena, item_type.clone());

    let e1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(10)), DataType::Int64);
    let e2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(20)), DataType::Int64);
    let arr = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![e1, e2],
        },
        list_type,
    );
    let idx_oob = arena.push_typed(ExprNode::Literal(LiteralValue::Int32(3)), DataType::Int32);
    let check_true = arena.push_typed(
        ExprNode::Literal(LiteralValue::Bool(true)),
        DataType::Boolean,
    );

    let err = eval_element_at(&arena, expr, &[arr, idx_oob, check_true], &chunk).unwrap_err();
    assert!(err.contains("Array subscript must be less than or equal to array length"));
}

// ---------------------------------------------------------------------------
// dispatch tests
// ---------------------------------------------------------------------------

#[test]
fn test_register_array_functions() {
    use novarocks::exec::expr::function::FunctionKind;
    use std::collections::HashMap;
    let mut m = HashMap::new();
    register(&mut m);
    assert_eq!(
        m.get("array_length"),
        Some(&FunctionKind::Array("array_length"))
    );
    assert_eq!(
        m.get("array_contains"),
        Some(&FunctionKind::Array("array_contains"))
    );
}

#[test]
fn test_array_metadata() {
    let meta = metadata("array_slice").unwrap();
    assert_eq!(meta.min_args, 2);
    assert_eq!(meta.max_args, 3);
}
