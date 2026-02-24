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
use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use super::eval_math_function;

pub fn chunk_len_1() -> Chunk {
    let array = Arc::new(Int64Array::from(vec![1])) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![field_with_slot_id(
        Field::new("dummy", DataType::Int64, false),
        SlotId::new(1),
    )]));
    let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
    Chunk::new(batch)
}

pub fn literal_i64(arena: &mut ExprArena, v: i64) -> ExprId {
    arena.push(ExprNode::Literal(LiteralValue::Int64(v)))
}

pub fn literal_f64(arena: &mut ExprArena, v: f64) -> ExprId {
    arena.push(ExprNode::Literal(LiteralValue::Float64(v)))
}

pub fn literal_string(arena: &mut ExprArena, v: &str) -> ExprId {
    arena.push(ExprNode::Literal(LiteralValue::Utf8(v.to_string())))
}

pub fn typed_null(arena: &mut ExprArena, data_type: DataType) -> ExprId {
    arena.push_typed(ExprNode::Literal(LiteralValue::Null), data_type)
}

fn math_eval_f64(
    name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
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
    chunk: &Chunk,
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
    chunk: &Chunk,
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
    let chunk = chunk_len_1();
    let expr_f64 = typed_null(&mut arena, DataType::Float64);
    let expr_i64 = typed_null(&mut arena, DataType::Int64);
    let expr_str = typed_null(&mut arena, DataType::Utf8);

    match canonical {
        "acos" => {
            let x = literal_f64(&mut arena, 1.0);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 0.0).abs() < 1e-12);
        }
        "asin" => {
            let x = literal_f64(&mut arena, 0.0);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 0.0).abs() < 1e-12);
        }
        "atan" => {
            let x = literal_f64(&mut arena, 0.0);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 0.0).abs() < 1e-12);
        }
        "atan2" => {
            let y = literal_f64(&mut arena, 1.0);
            let x = literal_f64(&mut arena, 1.0);
            assert!(
                (math_eval_f64(name, &arena, expr_f64, &[y, x], &chunk)
                    - std::f64::consts::FRAC_PI_4)
                    .abs()
                    < 1e-12
            );
        }
        "bin" => {
            let x = literal_i64(&mut arena, 5);
            assert_eq!(math_eval_str(name, &arena, expr_str, &[x], &chunk), "101");
        }
        "cbrt" => {
            let x = literal_f64(&mut arena, 27.0);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 3.0).abs() < 1e-12);
        }
        "ceil" => {
            let x = literal_f64(&mut arena, 1.2);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 2.0).abs() < 1e-12);
        }
        "conv" => {
            let x = literal_string(&mut arena, "A");
            let from = literal_i64(&mut arena, 16);
            let to = literal_i64(&mut arena, 10);
            assert_eq!(
                math_eval_str(name, &arena, expr_str, &[x, from, to], &chunk),
                "10"
            );
        }
        "cos" => {
            let x = literal_f64(&mut arena, 0.0);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 1.0).abs() < 1e-12);
        }
        "cot" => {
            let x = literal_f64(&mut arena, std::f64::consts::FRAC_PI_4);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 1.0).abs() < 1e-12);
        }
        "degress" => {
            let x = literal_f64(&mut arena, std::f64::consts::PI);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 180.0).abs() < 1e-9);
        }
        "dlog1" => {
            let x = literal_f64(&mut arena, 0.0);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 0.0).abs() < 1e-12);
        }
        "dround" => {
            let x = literal_f64(&mut arena, 1.6);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 2.0).abs() < 1e-12);
        }
        "e" => {
            assert!(
                (math_eval_f64(name, &arena, expr_f64, &[], &chunk) - std::f64::consts::E).abs()
                    < 1e-12
            );
        }
        "exp" => {
            let x = literal_f64(&mut arena, 1.0);
            assert!(
                (math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - std::f64::consts::E).abs()
                    < 1e-12
            );
        }
        "floor" => {
            let x = literal_f64(&mut arena, 1.8);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 1.0).abs() < 1e-12);
        }
        "fmod" => {
            let x = literal_f64(&mut arena, 5.5);
            let y = literal_f64(&mut arena, 2.0);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x, y], &chunk) - 1.5).abs() < 1e-12);
        }
        "greatest" => {
            let a = literal_f64(&mut arena, 1.0);
            let b = literal_f64(&mut arena, 3.0);
            let c = literal_f64(&mut arena, 2.0);
            assert!(
                (math_eval_f64(name, &arena, expr_f64, &[a, b, c], &chunk) - 3.0).abs() < 1e-12
            );
        }
        "least" => {
            let a = literal_f64(&mut arena, 1.0);
            let b = literal_f64(&mut arena, 3.0);
            let c = literal_f64(&mut arena, 2.0);
            assert!(
                (math_eval_f64(name, &arena, expr_f64, &[a, b, c], &chunk) - 1.0).abs() < 1e-12
            );
        }
        "ln" => {
            let x = literal_f64(&mut arena, std::f64::consts::E);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 1.0).abs() < 1e-12);
        }
        "log" => {
            let base = literal_f64(&mut arena, 2.0);
            let x = literal_f64(&mut arena, 8.0);
            assert!(
                (math_eval_f64(name, &arena, expr_f64, &[base, x], &chunk) - 3.0).abs() < 1e-12
            );
        }
        "log10" => {
            let x = literal_f64(&mut arena, 100.0);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 2.0).abs() < 1e-12);
        }
        "log2" => {
            let x = literal_f64(&mut arena, 8.0);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 3.0).abs() < 1e-12);
        }
        "mod" => {
            let a = literal_i64(&mut arena, -7);
            let b = literal_i64(&mut arena, 3);
            assert_eq!(math_eval_i64(name, &arena, expr_i64, &[a, b], &chunk), -1);
        }
        "pi" => {
            assert!(
                (math_eval_f64(name, &arena, expr_f64, &[], &chunk) - std::f64::consts::PI).abs()
                    < 1e-12
            );
        }
        "pmod" => {
            let a = literal_i64(&mut arena, -7);
            let b = literal_i64(&mut arena, 3);
            assert_eq!(math_eval_i64(name, &arena, expr_i64, &[a, b], &chunk), 2);
        }
        "pow" => {
            let base = literal_f64(&mut arena, 2.0);
            let exp = literal_f64(&mut arena, 3.0);
            assert!(
                (math_eval_f64(name, &arena, expr_f64, &[base, exp], &chunk) - 8.0).abs() < 1e-12
            );
        }
        "positive" => {
            let x = literal_f64(&mut arena, -12.5);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) + 12.5).abs() < 1e-12);
        }
        "radians" => {
            let x = literal_f64(&mut arena, 180.0);
            assert!(
                (math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - std::f64::consts::PI).abs()
                    < 1e-9
            );
        }
        "rand" => {
            let seed = literal_i64(&mut arena, 42);
            let v = math_eval_f64(name, &arena, expr_f64, &[seed], &chunk);
            assert!((0.0..1.0).contains(&v));
        }
        "sign" => {
            let x = literal_f64(&mut arena, -3.0);
            assert_eq!(math_eval_i64(name, &arena, expr_i64, &[x], &chunk), -1);
        }
        "sin" => {
            let x = literal_f64(&mut arena, 0.0);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 0.0).abs() < 1e-12);
        }
        "sqrt" => {
            let x = literal_f64(&mut arena, 9.0);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 3.0).abs() < 1e-12);
        }
        "square" => {
            let x = literal_f64(&mut arena, 3.0);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 9.0).abs() < 1e-12);
        }
        "tan" => {
            let x = literal_f64(&mut arena, 0.0);
            assert!((math_eval_f64(name, &arena, expr_f64, &[x], &chunk) - 0.0).abs() < 1e-12);
        }
        "truncate" => {
            let x = literal_f64(&mut arena, 12.345);
            let scale = literal_i64(&mut arena, 2);
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
