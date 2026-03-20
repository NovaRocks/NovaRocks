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
use novarocks::exec::expr::function::math::eval_math_function;
use novarocks::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
use arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers copied from math/test_utils.rs (adapted to use common:: helpers)
// ---------------------------------------------------------------------------

fn math_eval_f64(
    name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &novarocks::exec::chunk::Chunk,
) -> f64 {
    let arr = eval_math_function(name, arena, expr, args, chunk).unwrap();
    let arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
    arr.value(0)
}

fn math_eval_i64(
    name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &novarocks::exec::chunk::Chunk,
) -> i64 {
    let arr = eval_math_function(name, arena, expr, args, chunk).unwrap();
    let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
    arr.value(0)
}

fn math_eval_str(
    name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &novarocks::exec::chunk::Chunk,
) -> String {
    let arr = eval_math_function(name, arena, expr, args, chunk).unwrap();
    let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
    arr.value(0).to_string()
}

pub fn assert_math_function_logic(name: &str) {
    let canonical = match name {
        "ceiling" | "dceil" => "ceil",
        "dexp" => "exp",
        "dfloor" => "floor",
        "dlog10" => "log10",
        "dpow" | "fpow" | "power" => "pow",
        "dsqrt" => "sqrt",
        "random" => "rand",
        other => other,
    };

    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_f64 = common::typed_null(&mut arena, DataType::Float64);
    let expr_i64 = common::typed_null(&mut arena, DataType::Int64);
    let expr_str = common::typed_null(&mut arena, DataType::Utf8);

    match canonical {
        "acos" => {
            let x = common::literal_f64(&mut arena, 1.0);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 0.0).abs() < 1e-12);
        }
        "asin" => {
            let x = common::literal_f64(&mut arena, 0.0);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 0.0).abs() < 1e-12);
        }
        "atan" => {
            let x = common::literal_f64(&mut arena, 0.0);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 0.0).abs() < 1e-12);
        }
        "atan2" => {
            let y = common::literal_f64(&mut arena, 1.0);
            let x = common::literal_f64(&mut arena, 1.0);
            assert!(
                (math_eval_f64(name, &arena, expr_f64, &[y, x], &chunk)
                    - std::f64::consts::FRAC_PI_4)
                    .abs()
                    < 1e-12
            );
        }
        "bin" => {
            let x = common::literal_i64(&mut arena, 5);
            assert_eq!(math_eval_str(name, &arena, expr_str, &[x], &chunk), "101");
        }
        "cbrt" => {
            let x = common::literal_f64(&mut arena, 27.0);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 3.0).abs() < 1e-12);
        }
        "ceil" => {
            let x = common::literal_f64(&mut arena, 1.2);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 2.0).abs() < 1e-12);
        }
        "conv" => {
            let x = common::literal_string(&mut arena, "A");
            let from = common::literal_i64(&mut arena, 16);
            let to = common::literal_i64(&mut arena, 10);
            assert_eq!(
                math_eval_str(name, &arena, expr_str, &[x, from, to], &chunk),
                "10"
            );
        }
        "cos" => {
            let x = common::literal_f64(&mut arena, 0.0);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 1.0).abs() < 1e-12);
        }
        "cot" => {
            let x = common::literal_f64(&mut arena, std::f64::consts::FRAC_PI_4);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 1.0).abs() < 1e-12);
        }
        "degress" => {
            let x = common::literal_f64(&mut arena, std::f64::consts::PI);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 180.0).abs() < 1e-9);
        }
        "dlog1" => {
            let x = common::literal_f64(&mut arena, 0.0);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 0.0).abs() < 1e-12);
        }
        "dround" => {
            let x = common::literal_f64(&mut arena, 1.6);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 2.0).abs() < 1e-12);
        }
        "e" => {
            assert!(
                (math_eval_f64(name, &arena, expr_f64, &[], &chunk) - std::f64::consts::E).abs()
                    < 1e-12
            );
        }
        "exp" => {
            let x = common::literal_f64(&mut arena, 1.0);
            assert!(
                (math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - std::f64::consts::E).abs()
                    < 1e-12
            );
        }
        "floor" => {
            let x = common::literal_f64(&mut arena, 1.8);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 1.0).abs() < 1e-12);
        }
        "fmod" => {
            let x = common::literal_f64(&mut arena, 5.5);
            let y = common::literal_f64(&mut arena, 2.0);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x, y], &chunk) - 1.5).abs() < 1e-12);
        }
        "greatest" => {
            let a = common::literal_f64(&mut arena, 1.0);
            let b = common::literal_f64(&mut arena, 3.0);
            let c = common::literal_f64(&mut arena, 2.0);
            assert!(
                (math_eval_f64(name, &arena, expr_f64, &[a, b, c], &chunk) - 3.0).abs() < 1e-12
            );
        }
        "least" => {
            let a = common::literal_f64(&mut arena, 1.0);
            let b = common::literal_f64(&mut arena, 3.0);
            let c = common::literal_f64(&mut arena, 2.0);
            assert!(
                (math_eval_f64(name, &arena, expr_f64, &[a, b, c], &chunk) - 1.0).abs() < 1e-12
            );
        }
        "ln" => {
            let x = common::literal_f64(&mut arena, std::f64::consts::E);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 1.0).abs() < 1e-12);
        }
        "log" => {
            let base = common::literal_f64(&mut arena, 2.0);
            let x = common::literal_f64(&mut arena, 8.0);
            assert!(
                (math_eval_f64(name, &arena, expr_f64, &[base, x], &chunk) - 3.0).abs() < 1e-12
            );
        }
        "log10" => {
            let x = common::literal_f64(&mut arena, 100.0);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 2.0).abs() < 1e-12);
        }
        "log2" => {
            let x = common::literal_f64(&mut arena, 8.0);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 3.0).abs() < 1e-12);
        }
        "mod" => {
            let a = common::literal_i64(&mut arena, -7);
            let b = common::literal_i64(&mut arena, 3);
            assert_eq!(math_eval_i64(name, &arena, expr_i64, &[a, b], &chunk), -1);
        }
        "pi" => {
            assert!(
                (math_eval_f64(name, &arena, expr_f64, &[], &chunk) - std::f64::consts::PI).abs()
                    < 1e-12
            );
        }
        "pmod" => {
            let a = common::literal_i64(&mut arena, -7);
            let b = common::literal_i64(&mut arena, 3);
            assert_eq!(math_eval_i64(name, &arena, expr_i64, &[a, b], &chunk), 2);
        }
        "pow" => {
            let base = common::literal_f64(&mut arena, 2.0);
            let exp = common::literal_f64(&mut arena, 3.0);
            assert!(
                (math_eval_f64(name, &arena, expr_f64, &[base, exp], &chunk) - 8.0).abs() < 1e-12
            );
        }
        "positive" => {
            let x = common::literal_f64(&mut arena, -12.5);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) + 12.5).abs() < 1e-12);
        }
        "radians" => {
            let x = common::literal_f64(&mut arena, 180.0);
            assert!(
                (math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - std::f64::consts::PI).abs()
                    < 1e-9
            );
        }
        "rand" => {
            let seed = common::literal_i64(&mut arena, 42);
            let v = math_eval_f64(name, &arena, expr_f64, &[seed], &chunk);
            assert!((0.0..1.0).contains(&v));
        }
        "sign" => {
            let x = common::literal_f64(&mut arena, -3.0);
            assert_eq!(math_eval_i64(name, &arena, expr_i64, &[x], &chunk), -1);
        }
        "sin" => {
            let x = common::literal_f64(&mut arena, 0.0);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 0.0).abs() < 1e-12);
        }
        "sqrt" => {
            let x = common::literal_f64(&mut arena, 9.0);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 3.0).abs() < 1e-12);
        }
        "square" => {
            let x = common::literal_f64(&mut arena, 3.0);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 9.0).abs() < 1e-12);
        }
        "tan" => {
            let x = common::literal_f64(&mut arena, 0.0);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 0.0).abs() < 1e-12);
        }
        "truncate" => {
            let x = common::literal_f64(&mut arena, 12.345);
            let scale = common::literal_i64(&mut arena, 2);
            assert!(
                (math_eval_f64(name, &arena, expr_f64, &[x, scale], &chunk) - 12.34).abs() < 1e-12
            );
        }
        other => panic!(
            "unsupported high-priority math function in helper: {}",
            other
        ),
    }
}

// ---------------------------------------------------------------------------
// Tests from mod_ops.rs
// ---------------------------------------------------------------------------

#[test]
fn test_mod_logic() {
    assert_math_function_logic("mod");
}

#[test]
fn test_pmod_logic() {
    assert_math_function_logic("pmod");
}

// ---------------------------------------------------------------------------
// Tests from rand_ops.rs
// ---------------------------------------------------------------------------

#[test]
fn test_rand_logic() {
    assert_math_function_logic("rand");
}

#[test]
fn test_random_logic() {
    assert_math_function_logic("random");
}

// ---------------------------------------------------------------------------
// Tests from sign.rs
// ---------------------------------------------------------------------------

#[test]
fn test_sign_logic() {
    assert_math_function_logic("sign");
}

// ---------------------------------------------------------------------------
// Tests from truncate_ops.rs
// ---------------------------------------------------------------------------

#[test]
fn test_truncate_logic() {
    assert_math_function_logic("truncate");
}

#[test]
fn test_dround_logic() {
    assert_math_function_logic("dround");
}

// ---------------------------------------------------------------------------
// Tests from unary_ops.rs
// ---------------------------------------------------------------------------

#[test]
fn test_unary_math_logic() {
    for name in [
        "acos", "asin", "atan", "ceil", "ceiling", "dceil", "cos", "cot", "degress", "dexp",
        "dlog1", "dlog10", "exp", "floor", "dfloor", "ln", "log10", "log2", "radians", "sin",
        "sqrt", "dsqrt", "square", "tan", "cbrt", "positive",
    ] {
        assert_math_function_logic(name);
    }
}

// ---------------------------------------------------------------------------
// Tests from vector_ops.rs
// ---------------------------------------------------------------------------

fn float_array_literal(arena: &mut ExprArena, values: &[f32]) -> ExprId {
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Float32, true)));
    let elements = values
        .iter()
        .map(|v| {
            arena.push_typed(
                ExprNode::Literal(LiteralValue::Float32(*v)),
                DataType::Float32,
            )
        })
        .collect::<Vec<_>>();
    arena.push_typed(ExprNode::ArrayExpr { elements }, list_type)
}

fn eval_single(
    name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &novarocks::exec::chunk::Chunk,
) -> f64 {
    let out = eval_math_function(name, arena, expr, args, chunk).unwrap();
    let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
    out.value(0)
}

#[test]
fn test_vector_math_values() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr = common::typed_null(&mut arena, DataType::Float64);

    let a = float_array_literal(&mut arena, &[0.1, 0.2, 0.3]);
    let b = float_array_literal(&mut arena, &[0.2, 0.1, 0.3]);

    let cosine = eval_single("cosine_similarity", &arena, expr, &[a, b], &chunk);
    assert!((cosine - 0.92857142857).abs() < 1e-8);

    let cosine_norm = eval_single("cosine_similarity_norm", &arena, expr, &[a, a], &chunk);
    assert!((cosine_norm - 0.14).abs() < 1e-7);

    let l2 = eval_single("l2_distance", &arena, expr, &[a, b], &chunk);
    assert!((l2 - 0.02).abs() < 1e-8);
}

#[test]
fn test_vector_math_empty_array_rejected() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr = common::typed_null(&mut arena, DataType::Float64);
    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Float32, true)));
    let empty = arena.push_typed(ExprNode::ArrayExpr { elements: vec![] }, list_type.clone());

    let err =
        eval_math_function("l2_distance", &arena, expr, &[empty, empty], &chunk).unwrap_err();
    assert!(err.contains("requires non-empty arrays"));
}

// ---------------------------------------------------------------------------
// Tests from bin.rs
// ---------------------------------------------------------------------------

#[test]
fn test_bin_logic() {
    assert_math_function_logic("bin");
}

// ---------------------------------------------------------------------------
// Tests from binary_ops.rs
// ---------------------------------------------------------------------------

#[test]
fn test_binary_math_logic() {
    for name in ["atan2", "fmod", "pow", "power", "dpow", "fpow"] {
        assert_math_function_logic(name);
    }
}

// ---------------------------------------------------------------------------
// Tests from const_ops.rs
// ---------------------------------------------------------------------------

#[test]
fn test_const_logic() {
    assert_math_function_logic("e");
    assert_math_function_logic("pi");
}

// ---------------------------------------------------------------------------
// Tests from conv.rs
// ---------------------------------------------------------------------------

#[test]
fn test_conv_logic() {
    assert_math_function_logic("conv");
}

// ---------------------------------------------------------------------------
// Tests from equiwidth_bucket.rs
// ---------------------------------------------------------------------------

use novarocks::exec::expr::function::math::eval_equiwidth_bucket;

#[test]
fn test_equiwidth_bucket_basic() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Int64);
    let value = common::literal_i64(&mut arena, 10);
    let min = common::literal_i64(&mut arena, 0);
    let max = common::literal_i64(&mut arena, 10);
    let buckets = common::literal_i64(&mut arena, 20);

    let out =
        eval_equiwidth_bucket(&arena, expr, &[value, min, max, buckets], &common::chunk_len_1())
            .unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(out.value(0), 10);
}

#[test]
fn test_equiwidth_bucket_invalid_range() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Int64);
    let value = common::literal_i64(&mut arena, 1);
    let min = common::literal_i64(&mut arena, 2);
    let max = common::literal_i64(&mut arena, 2);
    let buckets = common::literal_i64(&mut arena, 1);
    let err =
        eval_equiwidth_bucket(&arena, expr, &[value, min, max, buckets], &common::chunk_len_1())
            .unwrap_err();
    assert!(err.contains("min < max"));
}

#[test]
fn test_equiwidth_bucket_out_of_bounds() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Int64);
    let value = common::literal_i64(&mut arena, 11);
    let min = common::literal_i64(&mut arena, 0);
    let max = common::literal_i64(&mut arena, 10);
    let buckets = common::literal_i64(&mut arena, 20);
    let err =
        eval_equiwidth_bucket(&arena, expr, &[value, min, max, buckets], &common::chunk_len_1())
            .unwrap_err();
    assert!(err.contains("size <= max"));
}

// ---------------------------------------------------------------------------
// Tests from extrema_ops.rs
// ---------------------------------------------------------------------------

#[test]
fn test_extrema_logic() {
    assert_math_function_logic("greatest");
    assert_math_function_logic("least");
}

// ---------------------------------------------------------------------------
// Tests from log.rs
// ---------------------------------------------------------------------------

#[test]
fn test_log_logic() {
    assert_math_function_logic("log");
}

// ---------------------------------------------------------------------------
// Tests from abs.rs
// ---------------------------------------------------------------------------

use novarocks::exec::expr::function::FunctionKind;
use novarocks::common::largeint;

fn create_test_chunk_int(values: Vec<i64>) -> novarocks::exec::chunk::Chunk {
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks::common::ids::SlotId;
    use novarocks::exec::chunk::ChunkSchema;
    use std::sync::Arc;

    let array = Arc::new(Int64Array::from(values)) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![Field::new("col0", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
    let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
        batch.schema().as_ref(),
        &[SlotId::new(1)],
    )
    .expect("chunk schema");
    novarocks::exec::chunk::Chunk::new_with_chunk_schema(batch, chunk_schema)
}

fn create_test_chunk_float(values: Vec<f64>) -> novarocks::exec::chunk::Chunk {
    use arrow::array::{ArrayRef, Float64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks::common::ids::SlotId;
    use novarocks::exec::chunk::ChunkSchema;
    use std::sync::Arc;

    let array = Arc::new(Float64Array::from(values)) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "col0",
        DataType::Float64,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
    let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
        batch.schema().as_ref(),
        &[SlotId::new(1)],
    )
    .expect("chunk schema");
    novarocks::exec::chunk::Chunk::new_with_chunk_schema(batch, chunk_schema)
}

#[test]
fn test_abs_int64_positive() {
    let mut arena = ExprArena::default();
    let lit = arena.push(ExprNode::Literal(LiteralValue::Int64(42)));
    let abs = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Abs,
            args: vec![lit],
        },
        DataType::Int64,
    );

    let chunk = create_test_chunk_int(vec![1]);
    let result = arena.eval(abs, &chunk).unwrap();
    let result_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(result_arr.value(0), 42);
}

#[test]
fn test_abs_int64_negative() {
    let mut arena = ExprArena::default();
    let lit = arena.push(ExprNode::Literal(LiteralValue::Int64(-42)));
    let abs = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Abs,
            args: vec![lit],
        },
        DataType::Int64,
    );

    let chunk = create_test_chunk_int(vec![1]);
    let result = arena.eval(abs, &chunk).unwrap();
    let result_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(result_arr.value(0), 42);
}

#[test]
fn test_abs_float64_positive() {
    let pi = std::f64::consts::PI;
    let mut arena = ExprArena::default();
    let lit = arena.push(ExprNode::Literal(LiteralValue::Float64(pi)));
    let abs = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Abs,
            args: vec![lit],
        },
        DataType::Float64,
    );

    let chunk = create_test_chunk_float(vec![0.0]);
    let result = arena.eval(abs, &chunk).unwrap();
    let result_arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((result_arr.value(0) - pi).abs() < 0.0001);
}

#[test]
fn test_abs_float64_negative() {
    let pi = std::f64::consts::PI;
    let mut arena = ExprArena::default();
    let lit = arena.push(ExprNode::Literal(LiteralValue::Float64(-pi)));
    let abs = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Abs,
            args: vec![lit],
        },
        DataType::Float64,
    );

    let chunk = create_test_chunk_float(vec![0.0]);
    let result = arena.eval(abs, &chunk).unwrap();
    let result_arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((result_arr.value(0) - pi).abs() < 0.0001);
}

#[test]
fn test_abs_int64_zero() {
    let mut arena = ExprArena::default();
    let lit = arena.push(ExprNode::Literal(LiteralValue::Int64(0)));
    let abs = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Abs,
            args: vec![lit],
        },
        DataType::Int64,
    );

    let chunk = create_test_chunk_int(vec![1]);
    let result = arena.eval(abs, &chunk).unwrap();
    let result_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(result_arr.value(0), 0);
}

#[test]
fn test_abs_int32_output() {
    let mut arena = ExprArena::default();
    let lit = arena.push(ExprNode::Literal(LiteralValue::Int32(-7)));
    let abs = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Abs,
            args: vec![lit],
        },
        DataType::Int32,
    );

    let chunk = create_test_chunk_int(vec![1]);
    let result = arena.eval(abs, &chunk).unwrap();
    let result_arr = result
        .as_any()
        .downcast_ref::<arrow::array::Int32Array>()
        .expect("must be Int32Array");
    assert_eq!(result_arr.value(0), 7);
}

#[test]
fn test_abs_int64_min() {
    let mut arena = ExprArena::default();
    let lit = arena.push(ExprNode::Literal(LiteralValue::Int64(i64::MIN)));
    let abs = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Abs,
            args: vec![lit],
        },
        DataType::Int64,
    );

    let chunk = create_test_chunk_int(vec![1]);
    let err = arena
        .eval(abs, &chunk)
        .expect_err("int64 overflow should fail");
    assert!(err.contains("abs overflow"));
}

#[test]
fn test_abs_int64_min_largeint_output() {
    let mut arena = ExprArena::default();
    let lit = arena.push_typed(
        ExprNode::Literal(LiteralValue::Int64(i64::MIN)),
        DataType::Int64,
    );
    let abs = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Abs,
            args: vec![lit],
        },
        DataType::FixedSizeBinary(16),
    );

    let chunk = create_test_chunk_int(vec![1]);
    let result = arena.eval(abs, &chunk).unwrap();
    let result_arr = result
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("must be FixedSizeBinaryArray");
    let parsed = largeint::i128_from_be_bytes(result_arr.value(0)).unwrap();
    assert_eq!(parsed, 9_223_372_036_854_775_808_i128);
}

#[test]
fn test_abs_largeint_min_wraps() {
    let mut arena = ExprArena::default();
    let lit = arena.push_typed(
        ExprNode::Literal(LiteralValue::LargeInt(i128::MIN)),
        DataType::FixedSizeBinary(16),
    );
    let abs = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Abs,
            args: vec![lit],
        },
        DataType::FixedSizeBinary(16),
    );

    let chunk = create_test_chunk_int(vec![1]);
    let result = arena.eval(abs, &chunk).unwrap();
    let result_arr = result
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("must be FixedSizeBinaryArray");
    let parsed = largeint::i128_from_be_bytes(result_arr.value(0)).unwrap();
    assert_eq!(parsed, i128::MIN);
}

// ---------------------------------------------------------------------------
// Tests from dispatch.rs
// ---------------------------------------------------------------------------

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

#[test]
fn test_trig_and_log() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_f64 = common::typed_null(&mut arena, DataType::Float64);

    let one = common::literal_f64(&mut arena, 1.0);
    let zero = common::literal_f64(&mut arena, 0.0);
    let pi = std::f64::consts::PI;
    let pi_lit = common::literal_f64(&mut arena, pi);

    assert!((math_eval_f64("acos", &arena, expr_f64, &[one], &chunk) - 0.0).abs() < 1e-12);
    assert!((math_eval_f64("asin", &arena, expr_f64, &[zero], &chunk) - 0.0).abs() < 1e-12);
    assert!((math_eval_f64("atan", &arena, expr_f64, &[zero], &chunk) - 0.0).abs() < 1e-12);
    assert!(
        (math_eval_f64("atan2", &arena, expr_f64, &[one, one], &chunk) - (pi / 4.0)).abs() < 1e-12
    );
    assert!((math_eval_f64("cos", &arena, expr_f64, &[zero], &chunk) - 1.0).abs() < 1e-12);
    assert!((math_eval_f64("sin", &arena, expr_f64, &[zero], &chunk) - 0.0).abs() < 1e-12);
    assert!((math_eval_f64("tan", &arena, expr_f64, &[zero], &chunk) - 0.0).abs() < 1e-12);
    let pi_quarter = common::literal_f64(&mut arena, pi / 4.0);
    assert!(
        (math_eval_f64("cot", &arena, expr_f64, &[pi_quarter], &chunk) - 1.0).abs() < 1e-12
    );
    let deg180 = common::literal_f64(&mut arena, 180.0);
    assert!(
        (math_eval_f64("radians", &arena, expr_f64, &[deg180], &chunk) - pi).abs() < 1e-9
    );
    assert!(
        (math_eval_f64("degress", &arena, expr_f64, &[pi_lit], &chunk) - 180.0).abs() < 1e-9
    );

    let e_val = common::literal_f64(&mut arena, std::f64::consts::E);
    let log10_val = common::literal_f64(&mut arena, 100.0);
    let log2_val = common::literal_f64(&mut arena, 8.0);
    let log_base = common::literal_f64(&mut arena, 2.0);
    let log_val = common::literal_f64(&mut arena, 8.0);
    assert!((math_eval_f64("ln", &arena, expr_f64, &[e_val], &chunk) - 1.0).abs() < 1e-12);
    assert!(
        (math_eval_f64("log10", &arena, expr_f64, &[log10_val], &chunk) - 2.0).abs() < 1e-12
    );
    assert!(
        (math_eval_f64("dlog10", &arena, expr_f64, &[log10_val], &chunk) - 2.0).abs() < 1e-12
    );
    assert!(
        (math_eval_f64("log2", &arena, expr_f64, &[log2_val], &chunk) - 3.0).abs() < 1e-12
    );
    assert!(
        (math_eval_f64("log", &arena, expr_f64, &[log_base, log_val], &chunk) - 3.0).abs()
            < 1e-12
    );
    assert!((math_eval_f64("dlog1", &arena, expr_f64, &[zero], &chunk) - 0.0).abs() < 1e-12);
}

#[test]
fn test_pow_and_roots() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_f64 = common::typed_null(&mut arena, DataType::Float64);

    let four = common::literal_f64(&mut arena, 4.0);
    let nine = common::literal_f64(&mut arena, 9.0);
    let one = common::literal_f64(&mut arena, 1.0);
    let three = common::literal_f64(&mut arena, 3.0);
    assert!((math_eval_f64("sqrt", &arena, expr_f64, &[four], &chunk) - 2.0).abs() < 1e-12);
    assert!((math_eval_f64("dsqrt", &arena, expr_f64, &[nine], &chunk) - 3.0).abs() < 1e-12);
    assert!(
        (math_eval_f64("exp", &arena, expr_f64, &[one], &chunk) - std::f64::consts::E).abs()
            < 1e-12
    );
    assert!(
        (math_eval_f64("dexp", &arena, expr_f64, &[one], &chunk) - std::f64::consts::E).abs()
            < 1e-12
    );
    assert!(
        (math_eval_f64("square", &arena, expr_f64, &[three], &chunk) - 9.0).abs() < 1e-12
    );

    let base = common::literal_f64(&mut arena, 2.0);
    let exp = common::literal_f64(&mut arena, 3.0);
    assert!(
        (math_eval_f64("pow", &arena, expr_f64, &[base, exp], &chunk) - 8.0).abs() < 1e-12
    );
    assert!(
        (math_eval_f64("power", &arena, expr_f64, &[base, exp], &chunk) - 8.0).abs() < 1e-12
    );
    assert!(
        (math_eval_f64("dpow", &arena, expr_f64, &[base, exp], &chunk) - 8.0).abs() < 1e-12
    );
    assert!(
        (math_eval_f64("fpow", &arena, expr_f64, &[base, exp], &chunk) - 8.0).abs() < 1e-12
    );
}

#[test]
fn test_non_finite_math_results_are_null() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_f64 = common::typed_null(&mut arena, DataType::Float64);

    let neg_one = common::literal_f64(&mut arena, -1.0);
    let zero = common::literal_f64(&mut arena, 0.0);
    let two = common::literal_f64(&mut arena, 2.0);
    let huge = common::literal_f64(&mut arena, 1000.0);

    let assert_null = |name: &str, args: &[ExprId]| {
        let out = eval_math_function(name, &arena, expr_f64, args, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(out.is_null(0), "{name} should convert non-finite to NULL");
    };

    assert_null("sqrt", &[neg_one]);
    assert_null("log", &[neg_one]);
    assert_null("ln", &[zero]);
    assert_null("acos", &[two]);
    assert_null("exp", &[huge]);
}

#[test]
fn test_rounding_and_floor() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_f64 = common::typed_null(&mut arena, DataType::Float64);

    let val = common::literal_f64(&mut arena, 1.2);
    assert!(
        (math_eval_f64("ceil", &arena, expr_f64, &[val], &chunk) - 2.0).abs() < 1e-12
    );
    assert!(
        (math_eval_f64("ceiling", &arena, expr_f64, &[val], &chunk) - 2.0).abs() < 1e-12
    );
    assert!(
        (math_eval_f64("dceil", &arena, expr_f64, &[val], &chunk) - 2.0).abs() < 1e-12
    );

    let val2 = common::literal_f64(&mut arena, 1.8);
    assert!(
        (math_eval_f64("floor", &arena, expr_f64, &[val2], &chunk) - 1.0).abs() < 1e-12
    );
    assert!(
        (math_eval_f64("dfloor", &arena, expr_f64, &[val2], &chunk) - 1.0).abs() < 1e-12
    );

    let val3 = common::literal_f64(&mut arena, 12.345);
    let scale = common::literal_i64(&mut arena, 2);
    assert!(
        (math_eval_f64("truncate", &arena, expr_f64, &[val3, scale], &chunk) - 12.34).abs()
            < 1e-12
    );
    let round_val = common::literal_f64(&mut arena, 1.6);
    assert!(
        (math_eval_f64("dround", &arena, expr_f64, &[round_val], &chunk) - 2.0).abs() < 1e-12
    );
}

#[test]
fn test_mod_sign_greatest_least() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_i64 = common::typed_null(&mut arena, DataType::Int64);
    let expr_f64 = common::typed_null(&mut arena, DataType::Float64);

    let a = common::literal_i64(&mut arena, -7);
    let b = common::literal_i64(&mut arena, 3);
    assert_eq!(math_eval_i64("mod", &arena, expr_i64, &[a, b], &chunk), -1);
    assert_eq!(math_eval_i64("pmod", &arena, expr_i64, &[a, b], &chunk), 2);

    let fa = common::literal_f64(&mut arena, 5.5);
    let fb = common::literal_f64(&mut arena, 2.0);
    assert!(
        (math_eval_f64("fmod", &arena, expr_f64, &[fa, fb], &chunk) - 1.5).abs() < 1e-12
    );

    let neg = common::literal_f64(&mut arena, -3.0);
    assert_eq!(math_eval_i64("sign", &arena, expr_i64, &[neg], &chunk), -1);

    let g1 = common::literal_f64(&mut arena, 1.0);
    let g2 = common::literal_f64(&mut arena, 3.0);
    let g3 = common::literal_f64(&mut arena, 2.0);
    assert!(
        (math_eval_f64("greatest", &arena, expr_f64, &[g1, g2, g3], &chunk) - 3.0).abs() < 1e-12
    );
    assert!(
        (math_eval_f64("least", &arena, expr_f64, &[g1, g2, g3], &chunk) - 1.0).abs() < 1e-12
    );
}

#[test]
fn test_bin_and_conv() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_str = common::typed_null(&mut arena, DataType::Utf8);

    let v = common::literal_i64(&mut arena, 5);
    assert_eq!(math_eval_str("bin", &arena, expr_str, &[v], &chunk), "101");

    let num = common::literal_string(&mut arena, "A");
    let from = common::literal_i64(&mut arena, 16);
    let to = common::literal_i64(&mut arena, 10);
    assert_eq!(
        math_eval_str("conv", &arena, expr_str, &[num, from, to], &chunk),
        "10"
    );
}

#[test]
fn test_constants_and_rand() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_f64 = common::typed_null(&mut arena, DataType::Float64);

    let pi = math_eval_f64("pi", &arena, expr_f64, &[], &chunk);
    assert!((pi - std::f64::consts::PI).abs() < 1e-12);
    let e = math_eval_f64("e", &arena, expr_f64, &[], &chunk);
    assert!((e - std::f64::consts::E).abs() < 1e-12);

    let seed = common::literal_i64(&mut arena, 42);
    let expected = {
        let mut rng = StdRng::seed_from_u64(42);
        rng.r#gen::<f64>()
    };
    let got = math_eval_f64("rand", &arena, expr_f64, &[seed], &chunk);
    assert!((got - expected).abs() < 1e-12);

    let seed2 = common::literal_i64(&mut arena, 7);
    let expected2 = {
        let mut rng = StdRng::seed_from_u64(7);
        rng.r#gen::<f64>()
    };
    let got2 = math_eval_f64("random", &arena, expr_f64, &[seed2], &chunk);
    assert!((got2 - expected2).abs() < 1e-12);
}
