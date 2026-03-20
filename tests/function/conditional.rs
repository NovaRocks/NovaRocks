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
use novarocks::exec::expr::function::conditional::{
    eval_assert_true, eval_coalesce, eval_if, eval_ifnull, eval_is_not_null, eval_is_null,
    eval_nullif,
};
use novarocks::exec::expr::function::FunctionKind;
use novarocks::exec::expr::{ExprArena, ExprNode, LiteralValue};
use novarocks::exec::expr::ExprId;
use novarocks::common::ids::SlotId;
use novarocks::exec::chunk::Chunk;
use novarocks::exec::chunk::ChunkSchema;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Int8Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn bool_chunk(values: Vec<Option<bool>>) -> Chunk {
    let field = Field::new("cond", DataType::Boolean, true);
    let schema = Arc::new(Schema::new(vec![field]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(BooleanArray::from(values))]).unwrap();
    let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
        batch.schema().as_ref(),
        &[SlotId::new(1)],
    )
    .expect("chunk schema");
    Chunk::new_with_chunk_schema(batch, chunk_schema)
}

fn create_test_chunk_int(values: Vec<Option<i64>>) -> Chunk {
    let array = Arc::new(Int64Array::from(values)) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![Field::new("col0", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
    let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
        batch.schema().as_ref(),
        &[SlotId::new(1)],
    )
    .expect("chunk schema");
    Chunk::new_with_chunk_schema(batch, chunk_schema)
}

fn create_test_chunk_string(values: Vec<Option<String>>) -> Chunk {
    let array = Arc::new(StringArray::from(values)) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![Field::new("col0", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
    let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
        batch.schema().as_ref(),
        &[SlotId::new(1)],
    )
    .expect("chunk schema");
    Chunk::new_with_chunk_schema(batch, chunk_schema)
}

// ---------------------------------------------------------------------------
// Tests migrated from conditional/assert_true.rs
// ---------------------------------------------------------------------------

#[test]
fn assert_true_returns_true_column() {
    let chunk = bool_chunk(vec![Some(true), Some(true), Some(true)]);
    let mut arena = ExprArena::default();
    let condition = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Boolean);

    let result = eval_assert_true(&arena, &[condition], &chunk).unwrap();
    let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();

    assert_eq!(result.len(), 3);
    assert!(result.value(0));
    assert!(result.value(1));
    assert!(result.value(2));
}

#[test]
fn assert_true_reports_false() {
    let chunk = bool_chunk(vec![Some(true), Some(false)]);
    let mut arena = ExprArena::default();
    let condition = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Boolean);

    let err = eval_assert_true(&arena, &[condition], &chunk).unwrap_err();
    assert!(err.contains("assert_true failed"));
}

#[test]
fn assert_true_reports_null() {
    let chunk = bool_chunk(vec![Some(true), None]);
    let mut arena = ExprArena::default();
    let condition = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Boolean);

    let err = eval_assert_true(&arena, &[condition], &chunk).unwrap_err();
    assert!(err.contains("null"));
}

#[test]
fn assert_true_uses_custom_message() {
    let chunk = bool_chunk(vec![Some(false)]);
    let mut arena = ExprArena::default();
    let condition = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Boolean);
    let message = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8("custom assert".to_string())),
        DataType::Utf8,
    );

    let err = eval_assert_true(&arena, &[condition, message], &chunk).unwrap_err();
    assert_eq!(err, "custom assert");
}

// ---------------------------------------------------------------------------
// Tests migrated from conditional/coalesce.rs
// ---------------------------------------------------------------------------

#[test]
fn test_coalesce_two_args_first_not_null() {
    let mut arena = ExprArena::default();
    let lit1 = arena.push(ExprNode::Literal(LiteralValue::Int64(10)));
    let lit2 = arena.push(ExprNode::Literal(LiteralValue::Int64(20)));
    let coalesce = arena.push_typed(
        ExprNode::FunctionCall {
            kind: novarocks::exec::expr::function::FunctionKind::Coalesce,
            args: vec![lit1, lit2],
        },
        DataType::Int64,
    );

    let chunk = create_test_chunk_int(vec![Some(1)]);

    let result = arena.eval(coalesce, &chunk).unwrap();
    let result_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();

    assert_eq!(result_arr.value(0), 10);
}

#[test]
fn test_coalesce_two_args_first_null() {
    let mut arena = ExprArena::default();
    let lit1 = arena.push(ExprNode::Literal(LiteralValue::Null));
    let lit2 = arena.push(ExprNode::Literal(LiteralValue::Int64(20)));
    let coalesce = arena.push_typed(
        ExprNode::FunctionCall {
            kind: novarocks::exec::expr::function::FunctionKind::Coalesce,
            args: vec![lit1, lit2],
        },
        DataType::Int64,
    );

    let chunk = create_test_chunk_int(vec![Some(1)]);

    let result = arena.eval(coalesce, &chunk).unwrap();
    let result_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();

    assert_eq!(result_arr.value(0), 20);
}

#[test]
fn test_coalesce_three_args() {
    let mut arena = ExprArena::default();
    let lit1 = arena.push(ExprNode::Literal(LiteralValue::Null));
    let lit2 = arena.push(ExprNode::Literal(LiteralValue::Null));
    let lit3 = arena.push(ExprNode::Literal(LiteralValue::Int64(30)));
    let coalesce = arena.push_typed(
        ExprNode::FunctionCall {
            kind: novarocks::exec::expr::function::FunctionKind::Coalesce,
            args: vec![lit1, lit2, lit3],
        },
        DataType::Int64,
    );

    let chunk = create_test_chunk_int(vec![Some(1)]);

    let result = arena.eval(coalesce, &chunk).unwrap();
    let result_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();

    assert_eq!(result_arr.value(0), 30);
}

#[test]
fn test_coalesce_all_null() {
    let mut arena = ExprArena::default();
    let lit1 = arena.push(ExprNode::Literal(LiteralValue::Null));
    let lit2 = arena.push(ExprNode::Literal(LiteralValue::Null));
    let coalesce = arena.push_typed(
        ExprNode::FunctionCall {
            kind: novarocks::exec::expr::function::FunctionKind::Coalesce,
            args: vec![lit1, lit2],
        },
        DataType::Int64,
    );

    let chunk = create_test_chunk_int(vec![Some(1)]);

    let result = arena.eval(coalesce, &chunk).unwrap();
    let result_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();

    assert!(result_arr.is_null(0));
}

#[test]
fn test_coalesce_string() {
    let mut arena = ExprArena::default();
    let lit1 = arena.push(ExprNode::Literal(LiteralValue::Null));
    let lit2 = arena.push(ExprNode::Literal(LiteralValue::Utf8("hello".to_string())));
    let coalesce = arena.push_typed(
        ExprNode::FunctionCall {
            kind: novarocks::exec::expr::function::FunctionKind::Coalesce,
            args: vec![lit1, lit2],
        },
        DataType::Utf8,
    );

    let chunk = create_test_chunk_string(vec![Some("test".to_string())]);

    let result = arena.eval(coalesce, &chunk).unwrap();
    let result_arr = result.as_any().downcast_ref::<StringArray>().unwrap();

    assert_eq!(result_arr.value(0), "hello");
}

// ---------------------------------------------------------------------------
// Tests migrated from conditional/nullif.rs
// ---------------------------------------------------------------------------

#[test]
fn test_nullif_int_values() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Int64);
    let chunk = common::chunk_len_1();

    let a = common::literal_i64(&mut arena, 1);
    let b = common::literal_i64(&mut arena, 1);
    let arr = eval_nullif(&arena, expr, a, b, &chunk).unwrap();
    let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(arr.is_null(0));

    let c = common::literal_i64(&mut arena, 1);
    let d = common::literal_i64(&mut arena, 2);
    let arr2 = eval_nullif(&arena, expr, c, d, &chunk).unwrap();
    let arr2 = arr2.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(arr2.value(0), 1);
}

// ---------------------------------------------------------------------------
// Tests migrated from conditional/if_func.rs
// ---------------------------------------------------------------------------

fn chunk_int64_nullable(values: Vec<Option<i64>>) -> Chunk {
    let array = Arc::new(Int64Array::from(values)) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![Field::new("c0", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
    let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
        batch.schema().as_ref(),
        &[SlotId::new(1)],
    )
    .expect("chunk schema");
    Chunk::new_with_chunk_schema(batch, chunk_schema)
}

#[test]
fn if_then_int_else_null_ok() {
    let chunk = chunk_int64_nullable(vec![Some(1), Some(0), None]);
    let mut arena = ExprArena::default();

    let cond = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
    let then_v = arena.push_typed(ExprNode::Literal(LiteralValue::Int8(1)), DataType::Int8);
    let else_v = arena.push_typed(ExprNode::Literal(LiteralValue::Null), DataType::Null);

    let if_expr = arena.push_typed(
        ExprNode::FunctionCall {
            kind: novarocks::exec::expr::function::FunctionKind::If,
            args: vec![cond, then_v, else_v],
        },
        DataType::Int8,
    );

    let result = arena.eval(if_expr, &chunk).unwrap();
    let arr = result.as_any().downcast_ref::<Int8Array>().unwrap();

    assert_eq!(arr.value(0), 1);
    assert!(arr.is_null(1));
    assert!(arr.is_null(2));
}

#[test]
fn if_null_condition_treated_as_false() {
    let chunk = chunk_int64_nullable(vec![Some(1)]);
    let mut arena = ExprArena::default();

    let cond = arena.push_typed(ExprNode::Literal(LiteralValue::Null), DataType::Null);
    let then_v = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(10)), DataType::Int64);
    let else_v = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(20)), DataType::Int64);

    let if_expr = arena.push_typed(
        ExprNode::FunctionCall {
            kind: novarocks::exec::expr::function::FunctionKind::If,
            args: vec![cond, then_v, else_v],
        },
        DataType::Int64,
    );

    let result = arena.eval(if_expr, &chunk).unwrap();
    let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(arr.value(0), 20);
}

#[test]
fn if_does_not_eagerly_eval_then_branch() {
    // Guard against errors like division-by-zero in the untaken branch:
    // if(false, 1/0, 1) => 1 (no error).
    let schema = Arc::new(Schema::empty());
    let options = RecordBatchOptions::new().with_row_count(Some(1));
    let batch = RecordBatch::try_new_with_options(schema, vec![], &options).unwrap();
    let chunk = {
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    };

    let mut arena = ExprArena::default();
    let cond = arena.push_typed(
        ExprNode::Literal(LiteralValue::Bool(false)),
        DataType::Boolean,
    );
    let one = arena.push_typed(
        ExprNode::Literal(LiteralValue::Decimal128 {
            value: 100,
            precision: 7,
            scale: 2,
        }),
        DataType::Decimal128(7, 2),
    );
    let zero = arena.push_typed(
        ExprNode::Literal(LiteralValue::Decimal128 {
            value: 0,
            precision: 7,
            scale: 2,
        }),
        DataType::Decimal128(7, 2),
    );
    let then_div = arena.push_typed(ExprNode::Div(one, zero), DataType::Decimal128(7, 2));
    let else_v = arena.push_typed(
        ExprNode::Literal(LiteralValue::Decimal128 {
            value: 100,
            precision: 7,
            scale: 2,
        }),
        DataType::Decimal128(7, 2),
    );

    let if_expr = arena.push_typed(
        ExprNode::FunctionCall {
            kind: novarocks::exec::expr::function::FunctionKind::If,
            args: vec![cond, then_div, else_v],
        },
        DataType::Decimal128(7, 2),
    );

    let result = arena.eval(if_expr, &chunk).unwrap();
    let arr = result
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(arr.value(0), 100);
}
