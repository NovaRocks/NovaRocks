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
/// Integration tests for SUBSTRING/SUBSTR function.
///
/// Tests verify that SUBSTRING properly extracts substrings from strings
/// and aligns with StarRocks BE's substring behavior.
use novarocks::common::ids::SlotId;
use novarocks::exec::chunk::{Chunk, field_with_slot_id};
use novarocks::exec::expr::{ExprArena, ExprNode, LiteralValue, function::FunctionKind};

use arrow::array::{Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Helper function to create a test chunk with a dummy column
fn create_test_chunk() -> Chunk {
    let schema = Schema::new(vec![field_with_slot_id(
        Field::new("dummy", DataType::Int64, false),
        SlotId::new(1),
    )]);
    let array = arrow::array::Int64Array::from(vec![1]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();
    Chunk::new(batch)
}

/// Helper function to test substring with 3 arguments
fn test_substring_3args(arena: &mut ExprArena, str_val: &str, pos: i64, len: i64, expected: &str) {
    let str_expr = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8(str_val.to_string())),
        DataType::Utf8,
    );
    let pos_expr = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(pos)), DataType::Int64);
    let len_expr = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(len)), DataType::Int64);

    let func_id = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Substring,
            args: vec![str_expr, pos_expr, len_expr],
        },
        DataType::Utf8,
    );

    let chunk = create_test_chunk();
    let result = arena.eval(func_id, &chunk).unwrap();
    let str_array = result.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        str_array.value(0),
        expected,
        "substr({:?}, {}, {})",
        str_val,
        pos,
        len
    );
}

/// Helper function to test substring with 2 arguments (no length)
fn test_substring_2args(arena: &mut ExprArena, str_val: &str, pos: i64, expected: &str) {
    let str_expr = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8(str_val.to_string())),
        DataType::Utf8,
    );
    let pos_expr = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(pos)), DataType::Int64);

    let func_id = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Substring,
            args: vec![str_expr, pos_expr],
        },
        DataType::Utf8,
    );

    let chunk = create_test_chunk();
    let result = arena.eval(func_id, &chunk).unwrap();
    let str_array = result.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        str_array.value(0),
        expected,
        "substr({:?}, {})",
        str_val,
        pos
    );
}

#[test]
fn test_substring_basic_ascii() {
    let mut arena = ExprArena::default();

    // Basic positive position tests
    test_substring_3args(&mut arena, "123456789", 1, 2, "12");
    test_substring_3args(&mut arena, "123456789", 2, 3, "234");
    test_substring_3args(&mut arena, "123456789", 9, 1, "9");
    test_substring_3args(&mut arena, "123456789", 1, 9, "123456789");
}

#[test]
fn test_substring_negative_position() {
    let mut arena = ExprArena::default();

    // Negative position: count from right
    test_substring_3args(&mut arena, "123456789", -1, 1, "9");
    test_substring_3args(&mut arena, "123456789", -2, 1, "8");
    test_substring_3args(&mut arena, "123456789", -4, 2, "67");
    test_substring_3args(&mut arena, "123456789", -4, 4, "6789");
    test_substring_3args(&mut arena, "123456789", -9, 9, "123456789");
}

#[test]
fn test_substring_zero_position() {
    let mut arena = ExprArena::default();

    // pos = 0 should return empty string
    test_substring_3args(&mut arena, "123456789", 0, 1, "");
    test_substring_3args(&mut arena, "123456789", 0, 10, "");
}

#[test]
fn test_substring_zero_or_negative_length() {
    let mut arena = ExprArena::default();

    // len <= 0 should return empty string
    test_substring_3args(&mut arena, "123456789", 1, 0, "");
    test_substring_3args(&mut arena, "123456789", 1, -1, "");
    test_substring_3args(&mut arena, "123456789", 1, -10, "");
    test_substring_3args(&mut arena, "123456789", -1, -1, "");
}

#[test]
fn test_substring_out_of_bounds() {
    let mut arena = ExprArena::default();

    // Position beyond string length
    test_substring_3args(&mut arena, "123456789", 10, 1, "");
    test_substring_3args(&mut arena, "123456789", 100, 1, "");

    // Negative position beyond string length
    test_substring_3args(&mut arena, "123456789", -10, 1, "");
    test_substring_3args(&mut arena, "123456789", -100, 1, "");
}

#[test]
fn test_substring_length_exceeds_string() {
    let mut arena = ExprArena::default();

    // Length exceeds remaining string
    test_substring_3args(&mut arena, "123456789", 2, 100, "23456789");
    test_substring_3args(&mut arena, "123456789", 9, 100, "9");
    test_substring_3args(&mut arena, "123456789", -4, 10, "6789");
}

#[test]
fn test_substring_two_args() {
    let mut arena = ExprArena::default();

    // 2-arg form: substr(str, pos) - returns from pos to end
    test_substring_2args(&mut arena, "123456789", 1, "123456789");
    test_substring_2args(&mut arena, "123456789", 2, "23456789");
    test_substring_2args(&mut arena, "123456789", 5, "56789");
    test_substring_2args(&mut arena, "123456789", 9, "9");
    test_substring_2args(&mut arena, "123456789", -1, "9");
    test_substring_2args(&mut arena, "123456789", -4, "6789");
}

#[test]
fn test_substring_chinese_characters() {
    let mut arena = ExprArena::default();

    // Test with Chinese characters (UTF-8)
    // "我是中文字符串" = 我(1), 是(2), 中(3), 文(4), 字(5), 符(6), 串(7)
    // From right: 串(-1), 符(-2), 字(-3), 文(-4), 中(-5), 是(-6), 我(-7)
    test_substring_3args(&mut arena, "我是中文字符串", 1, 2, "我是");
    test_substring_3args(&mut arena, "我是中文字符串", 2, 3, "是中文");
    test_substring_3args(&mut arena, "我是中文字符串", 7, 1, "串");
    test_substring_3args(&mut arena, "我是中文字符串", -1, 1, "串");
    test_substring_3args(&mut arena, "我是中文字符串", -2, 1, "符");
    test_substring_3args(&mut arena, "我是中文字符串", -4, 4, "文字符串"); // -4 is "文", take 4 chars
    test_substring_3args(&mut arena, "我是中文字符串", -5, 5, "中文字符串"); // -5 is "中", take 5 chars
}

#[test]
fn test_substring_mixed_ascii_utf8() {
    let mut arena = ExprArena::default();

    // Mixed ASCII and UTF-8
    test_substring_3args(&mut arena, "test123测试", 1, 4, "test");
    test_substring_3args(&mut arena, "test123测试", 5, 3, "123");
    test_substring_3args(&mut arena, "test123测试", 8, 2, "测试");
    test_substring_3args(&mut arena, "test123测试", -2, 2, "测试");
}

#[test]
fn test_substring_empty_string() {
    let mut arena = ExprArena::default();

    // Empty string
    test_substring_3args(&mut arena, "", 1, 1, "");
    test_substring_3args(&mut arena, "", 1, 10, "");
    test_substring_3args(&mut arena, "", -1, 1, "");
}

#[test]
fn test_substring_single_character() {
    let mut arena = ExprArena::default();

    // Single character
    test_substring_3args(&mut arena, "a", 1, 1, "a");
    test_substring_3args(&mut arena, "a", 1, 10, "a");
    test_substring_3args(&mut arena, "a", -1, 1, "a");
    test_substring_3args(&mut arena, "中", 1, 1, "中");
    test_substring_3args(&mut arena, "中", -1, 1, "中");
}

#[test]
fn test_substring_multiple_rows() {
    let mut arena = ExprArena::default();

    // Test with multiple rows in chunk
    let str_expr = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8("test".to_string())),
        DataType::Utf8,
    );
    let pos_expr = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let len_expr = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);

    let func_id = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Substring,
            args: vec![str_expr, pos_expr, len_expr],
        },
        DataType::Utf8,
    );

    // Create chunk with 3 rows
    let schema = Schema::new(vec![field_with_slot_id(
        Field::new("dummy", DataType::Int64, false),
        SlotId::new(1),
    )]);
    let array = arrow::array::Int64Array::from(vec![1, 2, 3]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();
    let chunk = Chunk::new(batch);

    let result = arena.eval(func_id, &chunk).unwrap();
    let str_array = result.as_any().downcast_ref::<StringArray>().unwrap();

    assert_eq!(str_array.len(), 3);
    // All rows should have the same result
    assert_eq!(str_array.value(0), "es");
    assert_eq!(str_array.value(1), "es");
    assert_eq!(str_array.value(2), "es");
}

#[test]
fn test_substring_edge_cases() {
    let mut arena = ExprArena::default();

    // Edge cases from StarRocks test suite
    test_substring_3args(&mut arena, "123456789", 1, i64::MAX, "123456789");
    test_substring_3args(&mut arena, "123456789", 2, i64::MAX, "23456789");
    test_substring_3args(&mut arena, "123456789", -9, 9, "123456789");
    test_substring_3args(&mut arena, "123456789", -9, 10, "123456789");
    test_substring_3args(&mut arena, "123456789", -4, 5, "6789");

    // More edge cases
    test_substring_3args(&mut arena, "123456789", 1, 0, "");
    test_substring_3args(&mut arena, "123456789", 2, 100, "23456789");
    test_substring_3args(&mut arena, "123456789", 9, 1, "9");
    test_substring_3args(&mut arena, "123456789", 9, 100, "9");
    test_substring_3args(&mut arena, "123456789", 10, 1, "");
    test_substring_3args(&mut arena, "123456789", -9, 1, "1");
    test_substring_3args(&mut arena, "123456789", -4, 1, "6");
    test_substring_3args(&mut arena, "123456789", -4, 4, "6789");
    test_substring_3args(&mut arena, "123456789", -1, 1, "9");
    test_substring_3args(&mut arena, "123456789", -1, 2, "9");
}

#[test]
fn test_substring_unicode_various_lengths() {
    let mut arena = ExprArena::default();

    // Test various UTF-8 character lengths
    // 1-byte: 'a'
    // 2-byte: '中' (Chinese)
    // 3-byte: '한' (Korean)
    // 4-byte: emoji (if available)

    let test_str = "a中한";
    test_substring_3args(&mut arena, test_str, 1, 1, "a");
    test_substring_3args(&mut arena, test_str, 2, 1, "中");
    test_substring_3args(&mut arena, test_str, 3, 1, "한");
    test_substring_3args(&mut arena, test_str, 1, 3, "a中한");
    test_substring_3args(&mut arena, test_str, -1, 1, "한");
    test_substring_3args(&mut arena, test_str, -2, 2, "中한");
}

#[test]
fn test_substring_comprehensive_starrocks_cases() {
    let mut arena = ExprArena::default();

    // Comprehensive test cases aligned with StarRocks test suite
    // Test string: "123456789"
    test_substring_3args(&mut arena, "123456789", 1, 2, "12");
    test_substring_3args(&mut arena, "123456789", 1, 0, "");
    test_substring_3args(&mut arena, "123456789", 2, 100, "23456789");
    test_substring_3args(&mut arena, "123456789", 9, 1, "9");
    test_substring_3args(&mut arena, "123456789", 9, 100, "9");
    test_substring_3args(&mut arena, "123456789", 10, 1, "");
    test_substring_3args(&mut arena, "123456789", -9, 1, "1");
    test_substring_3args(&mut arena, "123456789", -9, 9, "123456789");
    test_substring_3args(&mut arena, "123456789", -9, 10, "123456789");
    test_substring_3args(&mut arena, "123456789", -4, 1, "6");
    test_substring_3args(&mut arena, "123456789", -4, 4, "6789");
    test_substring_3args(&mut arena, "123456789", -4, 5, "6789");
    test_substring_3args(&mut arena, "123456789", -1, 1, "9");
    test_substring_3args(&mut arena, "123456789", -1, 2, "9");
    test_substring_3args(&mut arena, "123456789", 0, 1, "");
    test_substring_3args(&mut arena, "123456789", 1, i64::MAX, "123456789");
    test_substring_3args(&mut arena, "123456789", 1, -2, "");
    test_substring_3args(&mut arena, "123456789", -3, -2, "");
}

#[test]
fn test_substring_chinese_comprehensive() {
    let mut arena = ExprArena::default();

    // Test with Chinese characters similar to StarRocks test
    // "壹贰叁肆伍陆柒捌玖" = 9 characters
    let test_str = "壹贰叁肆伍陆柒捌玖";
    test_substring_3args(&mut arena, test_str, 1, 2, "壹贰");
    test_substring_3args(&mut arena, test_str, 1, 0, "");
    test_substring_3args(&mut arena, test_str, 2, 100, "贰叁肆伍陆柒捌玖");
    test_substring_3args(&mut arena, test_str, 9, 1, "玖");
    test_substring_3args(&mut arena, test_str, 9, 100, "玖");
    test_substring_3args(&mut arena, test_str, 10, 1, "");
    test_substring_3args(&mut arena, test_str, -9, 1, "壹");
    test_substring_3args(&mut arena, test_str, -9, 9, "壹贰叁肆伍陆柒捌玖");
    test_substring_3args(&mut arena, test_str, -9, 10, "壹贰叁肆伍陆柒捌玖");
    test_substring_3args(&mut arena, test_str, -4, 1, "陆");
    test_substring_3args(&mut arena, test_str, -4, 4, "陆柒捌玖");
    test_substring_3args(&mut arena, test_str, -4, 5, "陆柒捌玖");
    test_substring_3args(&mut arena, test_str, -1, 1, "玖");
    test_substring_3args(&mut arena, test_str, -1, 2, "玖");
    test_substring_3args(&mut arena, test_str, 0, 1, "");
    test_substring_3args(&mut arena, test_str, 1, i64::MAX, "壹贰叁肆伍陆柒捌玖");
}
