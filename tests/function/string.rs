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
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int32Array, Int64Array, ListArray,
    MapArray, StringArray, StructArray,
};
use arrow::datatypes::DataType;
use novarocks::exec::expr::function::FunctionKind;
use novarocks::exec::expr::function::string::{eval_split, eval_string_function};
use novarocks::exec::expr::{ExprArena, ExprNode, LiteralValue};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers copied from string/test_utils.rs (updated to use novarocks:: prefix)
// ---------------------------------------------------------------------------

fn string_eval_str(
    name: &str,
    arena: &ExprArena,
    expr: novarocks::exec::expr::ExprId,
    args: &[novarocks::exec::expr::ExprId],
    chunk: &novarocks::exec::chunk::Chunk,
) -> String {
    let arr = eval_string_function(name, arena, expr, args, chunk).unwrap();
    let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
    arr.value(0).to_string()
}

fn string_eval_i64(
    name: &str,
    arena: &ExprArena,
    expr: novarocks::exec::expr::ExprId,
    args: &[novarocks::exec::expr::ExprId],
    chunk: &novarocks::exec::chunk::Chunk,
) -> i64 {
    let arr = eval_string_function(name, arena, expr, args, chunk).unwrap();
    let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
    arr.value(0)
}

fn string_eval_i32(
    name: &str,
    arena: &ExprArena,
    expr: novarocks::exec::expr::ExprId,
    args: &[novarocks::exec::expr::ExprId],
    chunk: &novarocks::exec::chunk::Chunk,
) -> i32 {
    let arr = eval_string_function(name, arena, expr, args, chunk).unwrap();
    let arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
    arr.value(0)
}

fn string_eval_bool(
    name: &str,
    arena: &ExprArena,
    expr: novarocks::exec::expr::ExprId,
    args: &[novarocks::exec::expr::ExprId],
    chunk: &novarocks::exec::chunk::Chunk,
) -> bool {
    let arr = eval_string_function(name, arena, expr, args, chunk).unwrap();
    let arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
    arr.value(0)
}

fn assert_string_function_logic(name: &str) {
    let canonical = match name {
        "lcase" => "lower",
        "strleft" => "left",
        "strright" => "right",
        other => other,
    };

    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_str = common::typed_null(&mut arena, DataType::Utf8);
    let expr_i64 = common::typed_null(&mut arena, DataType::Int64);
    let expr_i32 = common::typed_null(&mut arena, DataType::Int32);
    let expr_bool = common::typed_null(&mut arena, DataType::Boolean);

    match canonical {
        "append_trailing_char_if_absent" => {
            let s = common::literal_string(&mut arena, "path");
            let ch = common::literal_string(&mut arena, "/");
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[s, ch], &chunk),
                "path/"
            );
        }
        "ascii" => {
            let s = common::literal_string(&mut arena, "A");
            assert_eq!(string_eval_i32(name, &arena, expr_i32, &[s], &chunk), 65);
        }
        "bar" => {
            let size = common::literal_i64(&mut arena, 1);
            let min = common::literal_i64(&mut arena, 0);
            let max = common::literal_i64(&mut arena, 10);
            let width = common::literal_i64(&mut arena, 20);
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[size, min, max, width], &chunk),
                "▓▓"
            );
        }
        "char_length" => {
            let s = common::literal_string(&mut arena, "a\u{00E9}");
            assert_eq!(string_eval_i64(name, &arena, expr_i64, &[s], &chunk), 2);
        }
        "concat" => {
            let a = common::literal_string(&mut arena, "a");
            let b = common::literal_string(&mut arena, "b");
            let c = common::literal_string(&mut arena, "c");
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[a, b, c], &chunk),
                "abc"
            );
        }
        "concat_ws" => {
            let sep = common::literal_string(&mut arena, "-");
            let a = common::literal_string(&mut arena, "a");
            let b = common::literal_string(&mut arena, "b");
            let c = common::literal_string(&mut arena, "c");
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[sep, a, b, c], &chunk),
                "a-b-c"
            );
        }
        "ends_with" => {
            let s = common::literal_string(&mut arena, "hello");
            let suf = common::literal_string(&mut arena, "lo");
            assert!(string_eval_bool(name, &arena, expr_bool, &[s, suf], &chunk));
        }
        "field" => {
            let target = common::literal_string(&mut arena, "b");
            let a = common::literal_string(&mut arena, "a");
            let b = common::literal_string(&mut arena, "b");
            let c = common::literal_string(&mut arena, "c");
            assert_eq!(
                string_eval_i32(name, &arena, expr_i32, &[target, a, b, c], &chunk),
                2
            );
        }
        "find_in_set" => {
            let target = common::literal_string(&mut arena, "b");
            let set = common::literal_string(&mut arena, "a,b,c");
            assert_eq!(
                string_eval_i32(name, &arena, expr_i32, &[target, set], &chunk),
                2
            );
        }
        "format_bytes" => {
            let bytes = common::literal_i64(&mut arena, 1024);
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[bytes], &chunk),
                "1.00 KB"
            );
        }
        "group_concat" => {
            let s = common::literal_string(&mut arena, "AB");
            let err = eval_string_function(name, &arena, expr_str, &[s], &chunk)
                .expect_err("group_concat should not be evaluated in scalar path");
            assert!(err.contains("aggregate"));
        }
        "hex" => {
            let s = common::literal_string(&mut arena, "AB");
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[s], &chunk),
                "4142"
            );
        }
        "initcap" => {
            let s = common::literal_string(&mut arena, "hELLo wORLD");
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[s], &chunk),
                "Hello World"
            );
        }
        "instr" => {
            let expr_i32 = common::typed_null(&mut arena, DataType::Int32);
            let s = common::literal_string(&mut arena, "hello");
            let sub = common::literal_string(&mut arena, "lo");
            assert_eq!(
                string_eval_i32(name, &arena, expr_i32, &[s, sub], &chunk),
                4
            );
        }
        "left" => {
            let s = common::literal_string(&mut arena, "hello");
            let n = common::literal_i64(&mut arena, 2);
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[s, n], &chunk),
                "he"
            );
        }
        "length" => {
            let s = common::literal_string(&mut arena, "a\u{00E9}");
            assert_eq!(string_eval_i64(name, &arena, expr_i64, &[s], &chunk), 3);
        }
        "locate" => {
            let expr_i32 = common::typed_null(&mut arena, DataType::Int32);
            let s = common::literal_string(&mut arena, "hello");
            let sub = common::literal_string(&mut arena, "lo");
            assert_eq!(
                string_eval_i32(name, &arena, expr_i32, &[sub, s], &chunk),
                4
            );
        }
        "lower" => {
            let s = common::literal_string(&mut arena, "HeLLo");
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[s], &chunk),
                "hello"
            );
        }
        "lpad" => {
            let s = common::literal_string(&mut arena, "hi");
            let len = common::literal_i64(&mut arena, 4);
            let pad = common::literal_string(&mut arena, "x");
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[s, len, pad], &chunk),
                "xxhi"
            );
        }
        "ltrim" => {
            let s = common::literal_string(&mut arena, "  hi");
            assert_eq!(string_eval_str(name, &arena, expr_str, &[s], &chunk), "hi");
        }
        "money_format" => {
            let f = common::literal_f64(&mut arena, 1234.5);
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[f], &chunk),
                "1,234.50"
            );
        }
        "null_or_empty" => {
            let s = common::literal_string(&mut arena, "");
            assert!(string_eval_bool(name, &arena, expr_bool, &[s], &chunk));
        }
        "parse_url" => {
            let url = common::literal_string(&mut arena, "https://example.com/path?x=1#frag");
            let host = common::literal_string(&mut arena, "HOST");
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[url, host], &chunk),
                "example.com"
            );
        }
        "regexp_extract" => {
            let text = common::literal_string(&mut arena, "abc123");
            let pat = common::literal_string(&mut arena, "(\\d+)");
            let idx = common::literal_i64(&mut arena, 1);
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[text, pat, idx], &chunk),
                "123"
            );
        }
        "regexp_count" => {
            let text = common::literal_string(&mut arena, "abc123def456");
            let pat = common::literal_string(&mut arena, "[0-9]");
            assert_eq!(
                string_eval_i64(name, &arena, expr_i64, &[text, pat], &chunk),
                6
            );
        }
        "regexp_extract_all" => {
            let text = common::literal_string(&mut arena, "AbCdExCeF");
            let pat = common::literal_string(&mut arena, "([[:lower:]]+)C([[:lower:]]+)");
            let idx = common::literal_i64(&mut arena, 0);
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[text, pat, idx], &chunk),
                "[\"bCd\",\"xCe\"]"
            );
        }
        "regexp_position" => {
            let text = common::literal_string(&mut arena, "a1b2c3d");
            let pat = common::literal_string(&mut arena, "[0-9]");
            let pos = common::literal_i64(&mut arena, 4);
            let occ = common::literal_i64(&mut arena, 2);
            assert_eq!(
                string_eval_i32(name, &arena, expr_i32, &[text, pat, pos, occ], &chunk),
                6
            );
        }
        "regexp_replace" => {
            let text = common::literal_string(&mut arena, "abc123");
            let pat = common::literal_string(&mut arena, "\\d+");
            let rep = common::literal_string(&mut arena, "#");
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[text, pat, rep], &chunk),
                "abc#"
            );
        }
        "repeat" => {
            let s = common::literal_string(&mut arena, "ab");
            let n = common::literal_i64(&mut arena, 3);
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[s, n], &chunk),
                "ababab"
            );
        }
        "replace" => {
            let s = common::literal_string(&mut arena, "hello");
            let from = common::literal_string(&mut arena, "l");
            let to = common::literal_string(&mut arena, "x");
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[s, from, to], &chunk),
                "hexxo"
            );
        }
        "reverse" => {
            let s = common::literal_string(&mut arena, "abc");
            assert_eq!(string_eval_str(name, &arena, expr_str, &[s], &chunk), "cba");
        }
        "right" => {
            let s = common::literal_string(&mut arena, "hello");
            let n = common::literal_i64(&mut arena, 2);
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[s, n], &chunk),
                "lo"
            );
        }
        "rpad" => {
            let s = common::literal_string(&mut arena, "hi");
            let len = common::literal_i64(&mut arena, 4);
            let pad = common::literal_string(&mut arena, "x");
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[s, len, pad], &chunk),
                "hixx"
            );
        }
        "rtrim" => {
            let s = common::literal_string(&mut arena, "hi  ");
            assert_eq!(string_eval_str(name, &arena, expr_str, &[s], &chunk), "hi");
        }
        "space" => {
            let n = common::literal_i64(&mut arena, 3);
            assert_eq!(string_eval_str(name, &arena, expr_str, &[n], &chunk), "   ");
        }
        "split_part" => {
            let s = common::literal_string(&mut arena, "a,b,c");
            let delim = common::literal_string(&mut arena, ",");
            let idx = common::literal_i64(&mut arena, 2);
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[s, delim, idx], &chunk),
                "b"
            );
        }
        "starts_with" => {
            let s = common::literal_string(&mut arena, "hello");
            let pre = common::literal_string(&mut arena, "he");
            assert!(string_eval_bool(name, &arena, expr_bool, &[s, pre], &chunk));
        }
        "str_to_map" => {
            let s = common::literal_string(&mut arena, "a:1,b:2");
            let entry = common::literal_string(&mut arena, ",");
            let kv = common::literal_string(&mut arena, ":");
            let arr =
                eval_string_function(name, &arena, expr_str, &[s, entry, kv], &chunk).unwrap();
            assert_eq!(arr.len(), 1);
            assert!(!arr.is_null(0));
            assert!(matches!(arr.data_type(), DataType::Map(_, _)));
        }
        "substring_index" => {
            let s = common::literal_string(&mut arena, "a,b,c");
            let delim = common::literal_string(&mut arena, ",");
            let count = common::literal_i64(&mut arena, 2);
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[s, delim, count], &chunk),
                "a,b"
            );
        }
        "translate" => {
            let s = common::literal_string(&mut arena, "abc");
            let from = common::literal_string(&mut arena, "ab");
            let to = common::literal_string(&mut arena, "12");
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[s, from, to], &chunk),
                "12c"
            );
        }
        "trim" => {
            let s = common::literal_string(&mut arena, "  hi  ");
            assert_eq!(string_eval_str(name, &arena, expr_str, &[s], &chunk), "hi");
        }
        "unhex" => {
            let hex = common::literal_string(&mut arena, "4142");
            let arr = eval_string_function(name, &arena, expr_str, &[hex], &chunk).unwrap();
            let arr = arr.as_any().downcast_ref::<BinaryArray>().unwrap();
            assert_eq!(arr.value(0), b"AB");
        }
        other => panic!(
            "unsupported high-priority string function in helper: {}",
            other
        ),
    }
}

// ---------------------------------------------------------------------------
// Tests migrated from string/append_trailing_char_if_absent.rs
// ---------------------------------------------------------------------------

#[test]
fn test_append_trailing_char_if_absent_logic() {
    assert_string_function_logic("append_trailing_char_if_absent");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/ascii.rs
// ---------------------------------------------------------------------------

#[test]
fn test_ascii_logic() {
    assert_string_function_logic("ascii");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/bar.rs
// ---------------------------------------------------------------------------

#[test]
fn test_bar_logic() {
    assert_string_function_logic("bar");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/case_ops.rs
// ---------------------------------------------------------------------------

#[test]
fn test_lower_logic() {
    assert_string_function_logic("lower");
}

#[test]
fn test_lcase_logic() {
    assert_string_function_logic("lcase");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/concat.rs
// ---------------------------------------------------------------------------

#[test]
fn test_concat_logic() {
    assert_string_function_logic("concat");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/concat_ws.rs
// ---------------------------------------------------------------------------

#[test]
fn test_concat_ws_logic() {
    assert_string_function_logic("concat_ws");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/crc32.rs
// ---------------------------------------------------------------------------

#[test]
fn crc32_utf8_values_match_zlib() {
    use arrow::array::Int64Array;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks::common::ids::SlotId;
    use novarocks::exec::chunk::Chunk;
    use novarocks::exec::chunk::ChunkSchema;
    use std::sync::Arc;

    let mut arena = ExprArena::default();
    let arg = arena.push_typed(ExprNode::SlotId(SlotId(1)), DataType::Utf8);

    let input = Arc::new(StringArray::from(vec![Some("123"), Some("abc"), None])) as ArrayRef;
    let field = Field::new("c1", DataType::Utf8, true);
    let schema = Arc::new(Schema::new(vec![field]));
    let batch = RecordBatch::try_new(schema, vec![input]).expect("record batch");
    let chunk = {
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId(1)])
                .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    };

    let out = eval_string_function("crc32", &arena, arg, &[arg], &chunk).expect("eval crc32");
    let out = out
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 array");
    assert_eq!(out.value(0), 2_286_445_522);
    assert_eq!(out.value(1), 891_568_578);
    assert!(out.is_null(2));
}

// ---------------------------------------------------------------------------
// Tests migrated from string/field.rs
// ---------------------------------------------------------------------------

#[test]
fn test_field_logic() {
    assert_string_function_logic("field");
}

#[test]
fn test_field_null_first_argument_with_null_type_returns_zero() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Int32);
    let first = arena.push_typed(ExprNode::Literal(LiteralValue::Null), DataType::Null);
    let a = common::literal_string(&mut arena, "a");
    let b = common::literal_string(&mut arena, "b");
    let out = eval_string_function(
        "field",
        &arena,
        expr,
        &[first, a, b],
        &common::chunk_len_1(),
    )
    .unwrap();
    let out = out.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(out.value(0), 0);
}

// ---------------------------------------------------------------------------
// Tests migrated from string/find_in_set.rs
// ---------------------------------------------------------------------------

#[test]
fn test_find_in_set_logic() {
    assert_string_function_logic("find_in_set");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/format_bytes.rs
// ---------------------------------------------------------------------------

#[test]
fn test_format_bytes_logic() {
    assert_string_function_logic("format_bytes");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/group_concat.rs
// ---------------------------------------------------------------------------

#[test]
fn test_group_concat_logic() {
    assert_string_function_logic("group_concat");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/hex.rs
// ---------------------------------------------------------------------------

#[test]
fn test_hex_logic() {
    assert_string_function_logic("hex");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/initcap.rs
// ---------------------------------------------------------------------------

#[test]
fn test_initcap_logic() {
    assert_string_function_logic("initcap");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/left_right_ops.rs
// ---------------------------------------------------------------------------

#[test]
fn test_left_logic() {
    assert_string_function_logic("left");
}

#[test]
fn test_right_logic() {
    assert_string_function_logic("right");
}

#[test]
fn test_strleft_logic() {
    assert_string_function_logic("strleft");
}

#[test]
fn test_strright_logic() {
    assert_string_function_logic("strright");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/length_ops.rs
// ---------------------------------------------------------------------------

#[test]
fn test_length_logic() {
    assert_string_function_logic("length");
}

#[test]
fn test_char_length_logic() {
    assert_string_function_logic("char_length");
}

#[test]
fn test_length_respects_int_return_type() {
    use novarocks::exec::expr::function::string::eval_string_function;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_i32 = common::typed_null(&mut arena, DataType::Int32);
    let s = common::literal_string(&mut arena, "abc");
    let out = eval_string_function("length", &arena, expr_i32, &[s], &chunk).expect("eval length");
    let arr = out
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("length should produce Int32Array");
    assert_eq!(arr.value(0), 3);
}

// ---------------------------------------------------------------------------
// Tests migrated from string/locate_ops.rs
// ---------------------------------------------------------------------------

#[test]
fn test_locate_logic() {
    assert_string_function_logic("locate");
}

#[test]
fn test_instr_logic() {
    assert_string_function_logic("instr");
}

#[test]
fn test_strpos_impl_logic() {
    // strpos_impl is private; test via eval_string_function("strpos", ...)
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_i64 = common::typed_null(&mut arena, DataType::Int64);

    let haystack = common::literal_string(&mut arena, "abcabc");
    let needle = common::literal_string(&mut arena, "abc");
    let instance = common::literal_i64(&mut arena, 2);
    let out = eval_string_function(
        "strpos",
        &arena,
        expr_i64,
        &[haystack, needle, instance],
        &chunk,
    )
    .unwrap();
    let out = out.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(out.value(0), 4);
}

// ---------------------------------------------------------------------------
// Tests migrated from string/money_format.rs
// ---------------------------------------------------------------------------

#[test]
fn test_money_format_logic() {
    assert_string_function_logic("money_format");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/murmur_hash3_32.rs
// ---------------------------------------------------------------------------

#[test]
fn murmur_hash3_32_known_vectors() {
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks::common::ids::SlotId;
    use novarocks::exec::chunk::Chunk;
    use novarocks::exec::chunk::ChunkSchema;
    use std::sync::Arc;

    let mut arena = ExprArena::default();
    let arg0 = arena.push_typed(ExprNode::SlotId(SlotId(1)), DataType::Utf8);
    let arg1 = arena.push_typed(ExprNode::SlotId(SlotId(2)), DataType::Utf8);

    let c1 = Arc::new(StringArray::from(vec![Some("test1234567"), Some("hello")])) as ArrayRef;
    let c2 = Arc::new(StringArray::from(vec![Some("asdf213"), Some("world")])) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Utf8, true),
        Field::new("c2", DataType::Utf8, true),
    ]));
    let chunk = {
        let batch = RecordBatch::try_new(schema, vec![c1, c2]).expect("record batch");
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId(1), SlotId(2)],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    };

    let out_single = eval_string_function("murmur_hash3_32", &arena, arg0, &[arg0], &chunk)
        .expect("eval single");
    let out_single = out_single
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("int32 output");
    assert_eq!(out_single.value(0), -1_948_194_659);
    assert_eq!(out_single.value(1), 1_321_743_225);

    let out_multi = eval_string_function("murmur_hash3_32", &arena, arg0, &[arg0, arg1], &chunk)
        .expect("eval multi");
    let out_multi = out_multi
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("int32 output");
    assert_eq!(out_multi.value(0), -500_290_079);
    assert_eq!(out_multi.value(1), 984_713_481);
}

#[test]
fn murmur_hash3_32_null_propagation() {
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks::common::ids::SlotId;
    use novarocks::exec::chunk::Chunk;
    use novarocks::exec::chunk::ChunkSchema;
    use std::sync::Arc;

    let mut arena = ExprArena::default();
    let arg0 = arena.push_typed(ExprNode::SlotId(SlotId(1)), DataType::Utf8);
    let arg1 = arena.push_typed(ExprNode::SlotId(SlotId(2)), DataType::Utf8);

    let c1 = Arc::new(StringArray::from(vec![Some("hello"), Some("hello")])) as ArrayRef;
    let c2 = Arc::new(StringArray::from(vec![None::<&str>, Some("world")])) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Utf8, true),
        Field::new("c2", DataType::Utf8, true),
    ]));
    let chunk = {
        let batch = RecordBatch::try_new(schema, vec![c1, c2]).expect("record batch");
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId(1), SlotId(2)],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    };

    let out =
        eval_string_function("murmur_hash3_32", &arena, arg0, &[arg0, arg1], &chunk).expect("eval");
    let out = out
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("int32 output");
    assert!(out.is_null(0));
    assert_eq!(out.value(1), 984_713_481);
}

// ---------------------------------------------------------------------------
// Tests migrated from string/ngram_search.rs
// ---------------------------------------------------------------------------

#[test]
fn test_ngram_search_basic() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr = common::typed_null(&mut arena, DataType::Float64);
    let haystack = common::literal_string(&mut arena, "chinese");
    let needle = common::literal_string(&mut arena, "china");
    let gram_num = common::literal_i64(&mut arena, 4);
    let out = eval_string_function(
        "ngram_search",
        &arena,
        expr,
        &[haystack, needle, gram_num],
        &chunk,
    )
    .unwrap();
    let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(out.value(0), 0.5);
}

#[test]
fn test_ngram_search_case_insensitive_basic() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr = common::typed_null(&mut arena, DataType::Float64);
    let haystack = common::literal_string(&mut arena, "chinese");
    let needle = common::literal_string(&mut arena, "CHINESE");
    let gram_num = common::literal_i64(&mut arena, 4);
    let out = eval_string_function(
        "ngram_search_case_insensitive",
        &arena,
        expr,
        &[haystack, needle, gram_num],
        &chunk,
    )
    .unwrap();
    let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(out.value(0), 1.0);
}

// ---------------------------------------------------------------------------
// Tests migrated from string/null_or_empty.rs
// ---------------------------------------------------------------------------

#[test]
fn test_null_or_empty_logic() {
    assert_string_function_logic("null_or_empty");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/pad_ops.rs
// ---------------------------------------------------------------------------

#[test]
fn test_lpad_logic() {
    assert_string_function_logic("lpad");
}

#[test]
fn test_rpad_logic() {
    assert_string_function_logic("rpad");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/parse_url.rs
// ---------------------------------------------------------------------------

#[test]
fn test_parse_url_logic() {
    assert_string_function_logic("parse_url");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/prefix_suffix_ops.rs
// ---------------------------------------------------------------------------

#[test]
fn test_starts_with_logic() {
    assert_string_function_logic("starts_with");
}

#[test]
fn test_ends_with_logic() {
    assert_string_function_logic("ends_with");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/regexp_count.rs
// ---------------------------------------------------------------------------

#[test]
fn test_regexp_count_logic() {
    assert_string_function_logic("regexp_count");
}

#[test]
fn test_regexp_count_special_pattern_returns_zero() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Int64);
    let input = common::literal_string(&mut arena, "test string");
    let pat = common::literal_string(&mut arena, "a{,}");

    let out = eval_string_function(
        "regexp_count",
        &arena,
        expr,
        &[input, pat],
        &common::chunk_len_1(),
    )
    .unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(out.value(0), 0);
}

// ---------------------------------------------------------------------------
// Tests migrated from string/regexp_extract.rs
// ---------------------------------------------------------------------------

#[test]
fn test_regexp_extract_logic() {
    assert_string_function_logic("regexp_extract");
}

#[test]
fn test_regexp_extract_no_match_returns_empty_string() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Utf8);
    let input = common::literal_string(&mut arena, "foo=123");
    let pat = common::literal_string(&mut arena, "bar=([0-9]+)");
    let idx = common::literal_i64(&mut arena, 1);

    let out = eval_string_function(
        "regexp_extract",
        &arena,
        expr,
        &[input, pat, idx],
        &common::chunk_len_1(),
    )
    .unwrap();
    let out = out.as_any().downcast_ref::<StringArray>().unwrap();
    assert!(!out.is_null(0));
    assert_eq!(out.value(0), "");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/regexp_extract_all.rs
// ---------------------------------------------------------------------------

#[test]
fn test_regexp_extract_all_logic() {
    assert_string_function_logic("regexp_extract_all");
}

#[test]
fn test_regexp_extract_all_out_of_range_group_returns_empty_json_array() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Utf8);
    let input = common::literal_string(&mut arena, "AbCdExCeF");
    let pat = common::literal_string(&mut arena, "([[:lower:]]+)C([[:lower:]]+)");
    let idx = common::literal_i64(&mut arena, 3);

    let out = eval_string_function(
        "regexp_extract_all",
        &arena,
        expr,
        &[input, pat, idx],
        &common::chunk_len_1(),
    )
    .unwrap();
    let out = out.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(out.value(0), "[]");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/regexp_position.rs
// ---------------------------------------------------------------------------

#[test]
fn test_regexp_position_logic() {
    assert_string_function_logic("regexp_position");
}

#[test]
fn test_regexp_position_unicode_and_occurrence() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Int32);
    let input = common::literal_string(&mut arena, "有朋$%X自9远方9来");
    let pat = common::literal_string(&mut arena, "[0-9]");
    let start = common::literal_i64(&mut arena, 10);
    let occ = common::literal_i64(&mut arena, 2);

    let out = eval_string_function(
        "regexp_position",
        &arena,
        expr,
        &[input, pat, start, occ],
        &common::chunk_len_1(),
    )
    .unwrap();
    let out = out.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(out.value(0), -1);
}

// ---------------------------------------------------------------------------
// Tests migrated from string/regexp_replace.rs
// ---------------------------------------------------------------------------

#[test]
fn test_regexp_replace_logic() {
    assert_string_function_logic("regexp_replace");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/repeat.rs
// ---------------------------------------------------------------------------

#[test]
fn test_repeat_logic() {
    assert_string_function_logic("repeat");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/replace.rs
// ---------------------------------------------------------------------------

#[test]
fn test_replace_logic() {
    assert_string_function_logic("replace");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/reverse.rs
// ---------------------------------------------------------------------------

#[test]
fn test_reverse_logic() {
    assert_string_function_logic("reverse");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/space.rs
// ---------------------------------------------------------------------------

#[test]
fn test_space_logic() {
    assert_string_function_logic("space");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/split.rs
// ---------------------------------------------------------------------------

#[test]
fn test_split_basic() {
    use novarocks::exec::expr::function::string::eval_split;
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let s = common::literal_string(&mut arena, "a,b,c");
    let delim = common::literal_string(&mut arena, ",");

    let out = eval_split(&arena, s, delim, &chunk).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let start = list.offsets()[0] as usize;
    let end = list.offsets()[1] as usize;
    assert_eq!(end - start, 3);
    assert_eq!(values.value(start), "a");
    assert_eq!(values.value(start + 1), "b");
    assert_eq!(values.value(start + 2), "c");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/split_part.rs
// ---------------------------------------------------------------------------

#[test]
fn test_split_part_logic() {
    assert_string_function_logic("split_part");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/str_to_map.rs
// ---------------------------------------------------------------------------

#[test]
fn test_str_to_map_logic() {
    assert_string_function_logic("str_to_map");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/substring_index.rs
// ---------------------------------------------------------------------------

#[test]
fn test_substring_index_logic() {
    assert_string_function_logic("substring_index");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/translate.rs
// ---------------------------------------------------------------------------

#[test]
fn test_translate_logic() {
    assert_string_function_logic("translate");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/trim_ops.rs
// ---------------------------------------------------------------------------

#[test]
fn test_ltrim_logic() {
    assert_string_function_logic("ltrim");
}

#[test]
fn test_rtrim_logic() {
    assert_string_function_logic("rtrim");
}

#[test]
fn test_trim_logic() {
    assert_string_function_logic("trim");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/unhex.rs
// ---------------------------------------------------------------------------

#[test]
fn test_unhex_logic() {
    assert_string_function_logic("unhex");
}

#[test]
fn test_unhex_invalid_input_returns_empty_string() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Binary);
    let bad = common::literal_string(&mut arena, "ZZ");
    let odd = common::literal_string(&mut arena, "F");

    let out_bad =
        eval_string_function("unhex", &arena, expr, &[bad], &common::chunk_len_1()).unwrap();
    let out_bad = out_bad.as_any().downcast_ref::<BinaryArray>().unwrap();
    assert!(!out_bad.is_null(0));
    assert_eq!(out_bad.value(0), b"");

    let out_odd =
        eval_string_function("unhex", &arena, expr, &[odd], &common::chunk_len_1()).unwrap();
    let out_odd = out_odd.as_any().downcast_ref::<BinaryArray>().unwrap();
    assert!(!out_odd.is_null(0));
    assert_eq!(out_odd.value(0), b"");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/upper.rs
// ---------------------------------------------------------------------------

fn create_test_chunk_string(values: Vec<String>) -> novarocks::exec::chunk::Chunk {
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks::common::ids::SlotId;
    use novarocks::exec::chunk::ChunkSchema;
    use std::sync::Arc;

    let array = Arc::new(StringArray::from(values)) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![Field::new("col0", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
    let chunk_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId::new(1)])
            .expect("chunk schema");
    novarocks::exec::chunk::Chunk::new_with_chunk_schema(batch, chunk_schema)
}

#[test]
fn test_upper_lowercase() {
    let mut arena = ExprArena::default();
    let lit = arena.push(ExprNode::Literal(LiteralValue::Utf8("hello".to_string())));
    let upper = arena.push_typed(
        ExprNode::FunctionCall {
            kind: novarocks::exec::expr::function::FunctionKind::Upper,
            args: vec![lit],
        },
        DataType::Utf8,
    );

    let chunk = create_test_chunk_string(vec!["test".to_string()]);

    let result = arena.eval(upper, &chunk).unwrap();
    let result_arr = result.as_any().downcast_ref::<StringArray>().unwrap();

    assert_eq!(result_arr.value(0), "HELLO");
}

#[test]
fn test_upper_mixed_case() {
    let mut arena = ExprArena::default();
    let lit = arena.push(ExprNode::Literal(LiteralValue::Utf8(
        "HeLLo WoRLd".to_string(),
    )));
    let upper = arena.push_typed(
        ExprNode::FunctionCall {
            kind: novarocks::exec::expr::function::FunctionKind::Upper,
            args: vec![lit],
        },
        DataType::Utf8,
    );

    let chunk = create_test_chunk_string(vec!["test".to_string()]);

    let result = arena.eval(upper, &chunk).unwrap();
    let result_arr = result.as_any().downcast_ref::<StringArray>().unwrap();

    assert_eq!(result_arr.value(0), "HELLO WORLD");
}

#[test]
fn test_upper_already_uppercase() {
    let mut arena = ExprArena::default();
    let lit = arena.push(ExprNode::Literal(LiteralValue::Utf8("HELLO".to_string())));
    let upper = arena.push_typed(
        ExprNode::FunctionCall {
            kind: novarocks::exec::expr::function::FunctionKind::Upper,
            args: vec![lit],
        },
        DataType::Utf8,
    );

    let chunk = create_test_chunk_string(vec!["test".to_string()]);

    let result = arena.eval(upper, &chunk).unwrap();
    let result_arr = result.as_any().downcast_ref::<StringArray>().unwrap();

    assert_eq!(result_arr.value(0), "HELLO");
}

#[test]
fn test_upper_empty_string() {
    let mut arena = ExprArena::default();
    let lit = arena.push(ExprNode::Literal(LiteralValue::Utf8("".to_string())));
    let upper = arena.push_typed(
        ExprNode::FunctionCall {
            kind: novarocks::exec::expr::function::FunctionKind::Upper,
            args: vec![lit],
        },
        DataType::Utf8,
    );

    let chunk = create_test_chunk_string(vec!["test".to_string()]);

    let result = arena.eval(upper, &chunk).unwrap();
    let result_arr = result.as_any().downcast_ref::<StringArray>().unwrap();

    assert_eq!(result_arr.value(0), "");
}

#[test]
fn test_upper_utf8() {
    let mut arena = ExprArena::default();
    let lit = arena.push(ExprNode::Literal(LiteralValue::Utf8("café".to_string())));
    let upper = arena.push_typed(
        ExprNode::FunctionCall {
            kind: novarocks::exec::expr::function::FunctionKind::Upper,
            args: vec![lit],
        },
        DataType::Utf8,
    );

    let chunk = create_test_chunk_string(vec!["test".to_string()]);

    let result = arena.eval(upper, &chunk).unwrap();
    let result_arr = result.as_any().downcast_ref::<StringArray>().unwrap();

    assert_eq!(result_arr.value(0), "CAFÉ");
}

#[test]
fn test_upper_with_numbers() {
    let mut arena = ExprArena::default();
    let lit = arena.push(ExprNode::Literal(LiteralValue::Utf8(
        "hello123".to_string(),
    )));
    let upper = arena.push_typed(
        ExprNode::FunctionCall {
            kind: novarocks::exec::expr::function::FunctionKind::Upper,
            args: vec![lit],
        },
        DataType::Utf8,
    );

    let chunk = create_test_chunk_string(vec!["test".to_string()]);

    let result = arena.eval(upper, &chunk).unwrap();
    let result_arr = result.as_any().downcast_ref::<StringArray>().unwrap();

    assert_eq!(result_arr.value(0), "HELLO123");
}

// ---------------------------------------------------------------------------
// Tests migrated from string/url_ops.rs
// ---------------------------------------------------------------------------

#[test]
fn test_url_encode_decode_logic() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_str = common::typed_null(&mut arena, DataType::Utf8);

    let raw = common::literal_string(
        &mut arena,
        "https://docs.starrocks.io/en-us/latest/quick_start/Deploy",
    );
    let encoded_arr = eval_string_function("url_encode", &arena, expr_str, &[raw], &chunk).unwrap();
    let encoded = encoded_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
        .to_string();
    assert_eq!(
        encoded,
        "https%3A%2F%2Fdocs.starrocks.io%2Fen-us%2Flatest%2Fquick_start%2FDeploy"
    );

    let encoded_expr = common::literal_string(&mut arena, &encoded);
    let decoded_arr =
        eval_string_function("url_decode", &arena, expr_str, &[encoded_expr], &chunk).unwrap();
    let decoded = decoded_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
        .to_string();
    assert_eq!(
        decoded,
        "https://docs.starrocks.io/en-us/latest/quick_start/Deploy"
    );
}

#[test]
fn test_url_extract_host_logic() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_str = common::typed_null(&mut arena, DataType::Utf8);

    let url = common::literal_string(&mut arena, "https://starrocks.com/test/api/v1");
    let out = eval_string_function("url_extract_host", &arena, expr_str, &[url], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(out.value(0), "starrocks.com");

    let url2 = common::literal_string(&mut arena, "https://starrocks.快速.com/test/api/v1");
    let out2 = eval_string_function("url_extract_host", &arena, expr_str, &[url2], &chunk).unwrap();
    let out2 = out2.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(out2.value(0), "starrocks.快速.com");
}

// ---------------------------------------------------------------------------
// Tests migrated from dev/test/sql/test_string_functions
// ---------------------------------------------------------------------------

#[test]
fn test_initcap_non_space_delimiters() {
    // Non-whitespace punctuation must act as word separators.
    // Digits do NOT trigger capitalization of the next letter.
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_str = common::typed_null(&mut arena, DataType::Utf8);

    let cases: &[(&str, &str)] = &[
        ("one,two.three;four", "One,Two.Three;Four"),
        ("alpha-beta_gamma", "Alpha-Beta_Gamma"),
        ("1st place", "1st Place"),
        ("word1 word2", "Word1 Word2"),
        ("123abc456", "123abc456"),
        ("macDonald", "Macdonald"),
        ("hello world", "Hello World"),
        ("HELLO WORLD", "Hello World"),
    ];
    for (input, expected) in cases {
        let s = common::literal_string(&mut arena, input);
        let out = eval_string_function("initcap", &arena, expr_str, &[s], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), *expected, "initcap({input:?})");
    }

    // NULL input → NULL output
    let null_val = common::typed_null(&mut arena, DataType::Utf8);
    let out = eval_string_function("initcap", &arena, expr_str, &[null_val], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<StringArray>().unwrap();
    assert!(out.is_null(0));
}

#[test]
fn test_raise_error_non_null_returns_err() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_str = common::typed_null(&mut arena, DataType::Utf8);

    let msg = common::literal_string(&mut arena, "custom error message");
    let err = eval_string_function("raise_error", &arena, expr_str, &[msg], &chunk)
        .expect_err("raise_error with non-null should return Err");
    assert!(err.contains("custom error message"), "err={err:?}");
}

#[test]
fn test_raise_error_null_returns_null() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();
    let expr_str = common::typed_null(&mut arena, DataType::Utf8);

    let null_val = common::typed_null(&mut arena, DataType::Utf8);
    let out = eval_string_function("raise_error", &arena, expr_str, &[null_val], &chunk).unwrap();
    assert!(out.is_null(0));
}
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
/// Integration tests for SUBSTRING/SUBSTR function.
///
/// Tests verify that SUBSTRING properly extracts substrings from strings
/// and aligns with StarRocks BE's substring behavior.
use novarocks::common::ids::SlotId;
use novarocks::exec::chunk::Chunk;

/// Helper function to create a test chunk with a dummy column
fn create_test_chunk() -> Chunk {
    let schema = Schema::new(vec![Field::new("dummy", DataType::Int64, false)]);
    let array = arrow::array::Int64Array::from(vec![1]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();
    let chunk_schema = novarocks::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
        batch.schema().as_ref(),
        &[SlotId::new(1)],
    )
    .expect("chunk schema");
    Chunk::new_with_chunk_schema(batch, chunk_schema)
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
    let schema = Schema::new(vec![Field::new("dummy", DataType::Int64, false)]);
    let array = arrow::array::Int64Array::from(vec![1, 2, 3]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();
    let chunk_schema = novarocks::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
        batch.schema().as_ref(),
        &[SlotId::new(1)],
    )
    .expect("chunk schema");
    let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);

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

// ---------------------------------------------------------------------------
// test_split — migrated from dev/test/sql/test_function/R/test_split
// ---------------------------------------------------------------------------

#[test]
fn test_split_unicode_and_empty_delimiter() {
    let mut arena = ExprArena::default();
    let chunk = common::chunk_len_1();

    macro_rules! split_eq {
        ($s:expr, $d:expr, [$($e:expr),*]) => {{
            // We need a fresh arena state since clone_push_str is used per call
            let sa = common::literal_string(&mut arena, $s);
            let da = common::literal_string(&mut arena, $d);
            let arr = eval_split(&arena, sa, da, &chunk).unwrap();
            let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
            let values = list.value(0);
            let strs = values.as_any().downcast_ref::<StringArray>().unwrap();
            let got: Vec<&str> = (0..strs.len()).map(|i| strs.value(i)).collect();
            assert_eq!(got, vec![$($e),*], "split('{}', '{}')", $s, $d);
        }};
    }

    // empty delimiter splits into UTF-8 characters
    split_eq!("测隔试隔试", "", ["测", "隔", "试", "隔", "试"]);
    split_eq!(
        "测abc隔试隔试",
        "",
        ["测", "a", "b", "c", "隔", "试", "隔", "试"]
    );
    split_eq!("a|b|c|d", "", ["a", "|", "b", "|", "c", "|", "d"]);

    // Chinese delimiter
    split_eq!("测隔试隔试", "隔", ["测", "试", "试"]);
    split_eq!("测abc隔试隔试", "隔", ["测abc", "试", "试"]);
    split_eq!("a|b|c|d", "隔", ["a|b|c|d"]);

    // ASCII delimiter
    split_eq!("测abc隔试abc隔试", "a", ["测", "bc隔试", "bc隔试"]);
    split_eq!("a|b|c|d", "|", ["a", "b", "c", "d"]);

    // delimiter not found
    split_eq!("测隔试隔试", "a", ["测隔试隔试"]);
}
