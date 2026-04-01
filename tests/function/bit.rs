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

use crate::common;
use arrow::array::{Array, FixedSizeBinaryArray, Int64Array};
use arrow::datatypes::DataType;
use novarocks::common::largeint;
use novarocks::exec::expr::function::bit::{
    eval_bit_shift_left, eval_bit_shift_right, eval_bit_shift_right_logical, eval_bitand,
    eval_bitnot, eval_bitor, eval_bitxor, eval_xx_hash3_128,
};
use novarocks::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};

fn literal_largeint(arena: &mut ExprArena, v: i128) -> ExprId {
    arena.push_typed(
        ExprNode::Literal(LiteralValue::LargeInt(v)),
        DataType::FixedSizeBinary(16),
    )
}

fn split_high_low(v: i128) -> (i64, u64) {
    let high = (v >> 64) as i64;
    let low = ((v as u128) & ((1u128 << 64) - 1)) as u64;
    (high, low)
}

// ---------------------------------------------------------------------------
// bit_ops tests
// ---------------------------------------------------------------------------

#[test]
fn test_bit_shift_left_basic() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Int64);
    let a = common::literal_i64(&mut arena, 3);
    let b = common::literal_i64(&mut arena, 2);

    let out = eval_bit_shift_left(&arena, expr, &[a, b], &common::chunk_len_1()).unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(out.value(0), 12);
}

#[test]
fn test_bit_shift_right_basic() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Int64);
    let a = common::literal_i64(&mut arena, -8);
    let b = common::literal_i64(&mut arena, 1);

    let out = eval_bit_shift_right(&arena, expr, &[a, b], &common::chunk_len_1()).unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(out.value(0), -4);
}

#[test]
fn test_bit_shift_right_logical_basic() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Int64);
    let a = common::literal_i64(&mut arena, -1);
    let b = common::literal_i64(&mut arena, 1);

    let out = eval_bit_shift_right_logical(&arena, expr, &[a, b], &common::chunk_len_1()).unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(out.value(0), 0x7fff_ffff_ffff_ffff_u64 as i64);
}

#[test]
fn test_bitand_basic() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Int64);
    let a = common::literal_i64(&mut arena, 6);
    let b = common::literal_i64(&mut arena, 3);

    let out = eval_bitand(&arena, expr, &[a, b], &common::chunk_len_1()).unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(out.value(0), 2);
}

#[test]
fn test_bitnot_basic() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Int64);
    let a = common::literal_i64(&mut arena, 6);

    let out = eval_bitnot(&arena, expr, &[a], &common::chunk_len_1()).unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(out.value(0), !6i64);
}

#[test]
fn test_bitor_basic() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Int64);
    let a = common::literal_i64(&mut arena, 6);
    let b = common::literal_i64(&mut arena, 3);

    let out = eval_bitor(&arena, expr, &[a, b], &common::chunk_len_1()).unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(out.value(0), 7);
}

#[test]
fn test_bitxor_basic() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Int64);
    let a = common::literal_i64(&mut arena, 6);
    let b = common::literal_i64(&mut arena, 3);

    let out = eval_bitxor(&arena, expr, &[a, b], &common::chunk_len_1()).unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(out.value(0), 5);
}

#[test]
fn test_largeint_bit_shift_and_mask() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::FixedSizeBinary(16));
    let value = literal_largeint(&mut arena, -1_i128);
    let shift = common::literal_i64(&mut arena, 64);
    let mask = literal_largeint(&mut arena, 18446744073709551615_i128);

    let shifted =
        eval_bit_shift_right_logical(&arena, expr, &[value, shift], &common::chunk_len_1())
            .unwrap();
    let shifted = shifted
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let shifted = largeint::value_at(shifted, 0).unwrap();
    assert_eq!(shifted, 18446744073709551615_i128);

    let masked = eval_bitand(&arena, expr, &[value, mask], &common::chunk_len_1()).unwrap();
    let masked = masked
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let masked = largeint::value_at(masked, 0).unwrap();
    assert_eq!(masked, 18446744073709551615_i128);
}

// ---------------------------------------------------------------------------
// xx_hash3_128 tests
// ---------------------------------------------------------------------------

#[test]
fn test_xx_hash3_128_known_vectors() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::FixedSizeBinary(16));
    let hello = common::literal_string(&mut arena, "hello");
    let starrocks = common::literal_string(&mut arena, "starrocks");

    let out = eval_xx_hash3_128(&arena, expr, &[hello], &common::chunk_len_1()).unwrap();
    let out = out.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
    let value = largeint::value_at(out, 0).unwrap();
    let (high, low) = split_high_low(value);
    assert_eq!(high, -5338522934378283393);
    assert_eq!(low, 14373748016363485208u64);

    let out = eval_xx_hash3_128(&arena, expr, &[hello, starrocks], &common::chunk_len_1()).unwrap();
    let out = out.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
    let value = largeint::value_at(out, 0).unwrap();
    let (high, low) = split_high_low(value);
    assert_eq!(high, 1559307639436096304);
    assert_eq!(low, 8859976453967563600u64);
}

#[test]
fn test_xx_hash3_128_null_propagation() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::FixedSizeBinary(16));
    let null_arg = common::typed_null(&mut arena, DataType::Utf8);

    let out = eval_xx_hash3_128(&arena, expr, &[null_arg], &common::chunk_len_1()).unwrap();
    let out = out.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
    assert!(out.is_null(0));
}

// ---------------------------------------------------------------------------
// dispatch tests
// ---------------------------------------------------------------------------

#[test]
fn test_register_bit_functions() {
    use novarocks::exec::expr::function::FunctionKind;
    use novarocks::exec::expr::function::bit::register;
    use std::collections::HashMap;

    let mut m = HashMap::new();
    register(&mut m);
    assert_eq!(m.get("bitand"), Some(&FunctionKind::Bit("bitand")));
    assert_eq!(
        m.get("bit_shift_right_logical"),
        Some(&FunctionKind::Bit("bit_shift_right_logical"))
    );
    assert_eq!(
        m.get("xx_hash3_128"),
        Some(&FunctionKind::Bit("xx_hash3_128"))
    );
}
