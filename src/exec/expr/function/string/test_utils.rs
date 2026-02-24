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
use arrow::array::{Array, ArrayRef, BinaryArray, BooleanArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use super::eval_string_function;

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

fn string_eval_str(
    name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> String {
    let arr = eval_string_function(name, arena, expr, args, chunk).unwrap();
    let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
    arr.value(0).to_string()
}

fn string_eval_i64(
    name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> i64 {
    let arr = eval_string_function(name, arena, expr, args, chunk).unwrap();
    let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
    arr.value(0)
}

fn string_eval_bool(
    name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> bool {
    let arr = eval_string_function(name, arena, expr, args, chunk).unwrap();
    let arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
    arr.value(0)
}

pub fn assert_string_function_logic(name: &str) {
    let canonical = match name {
        "lcase" => "lower",
        "strleft" => "left",
        "strright" => "right",
        other => other,
    };

    let mut arena = ExprArena::default();
    let chunk = chunk_len_1();
    let expr_str = typed_null(&mut arena, DataType::Utf8);
    let expr_i64 = typed_null(&mut arena, DataType::Int64);
    let expr_bool = typed_null(&mut arena, DataType::Boolean);

    match canonical {
        "append_trailing_char_if_absent" => {
            let s = literal_string(&mut arena, "path");
            let ch = literal_string(&mut arena, "/");
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[s, ch], &chunk),
                "path/"
            );
        }
        "ascii" => {
            let s = literal_string(&mut arena, "A");
            assert_eq!(string_eval_i64(name, &arena, expr_i64, &[s], &chunk), 65);
        }
        "bar" => {
            let size = literal_i64(&mut arena, 1);
            let min = literal_i64(&mut arena, 0);
            let max = literal_i64(&mut arena, 10);
            let width = literal_i64(&mut arena, 20);
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[size, min, max, width], &chunk),
                "▓▓"
            );
        }
        "char_length" => {
            let s = literal_string(&mut arena, "a\u{00E9}");
            assert_eq!(string_eval_i64(name, &arena, expr_i64, &[s], &chunk), 2);
        }
        "concat" => {
            let a = literal_string(&mut arena, "a");
            let b = literal_string(&mut arena, "b");
            let c = literal_string(&mut arena, "c");
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[a, b, c], &chunk),
                "abc"
            );
        }
        "concat_ws" => {
            let sep = literal_string(&mut arena, "-");
            let a = literal_string(&mut arena, "a");
            let b = literal_string(&mut arena, "b");
            let c = literal_string(&mut arena, "c");
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[sep, a, b, c], &chunk),
                "a-b-c"
            );
        }
        "ends_with" => {
            let s = literal_string(&mut arena, "hello");
            let suf = literal_string(&mut arena, "lo");
            assert!(string_eval_bool(name, &arena, expr_bool, &[s, suf], &chunk));
        }
        "field" => {
            let target = literal_string(&mut arena, "b");
            let a = literal_string(&mut arena, "a");
            let b = literal_string(&mut arena, "b");
            let c = literal_string(&mut arena, "c");
            assert_eq!(
                string_eval_i64(name, &arena, expr_i64, &[target, a, b, c], &chunk),
                2
            );
        }
        "find_in_set" => {
            let target = literal_string(&mut arena, "b");
            let set = literal_string(&mut arena, "a,b,c");
            assert_eq!(
                string_eval_i64(name, &arena, expr_i64, &[target, set], &chunk),
                2
            );
        }
        "format_bytes" => {
            let bytes = literal_i64(&mut arena, 1024);
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[bytes], &chunk),
                "1.00 KB"
            );
        }
        "group_concat" => {
            let s = literal_string(&mut arena, "AB");
            let err = eval_string_function(name, &arena, expr_str, &[s], &chunk)
                .expect_err("group_concat should not be evaluated in scalar path");
            assert!(err.contains("aggregate"));
        }
        "hex" => {
            let s = literal_string(&mut arena, "AB");
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[s], &chunk),
                "4142"
            );
        }
        "initcap" => {
            let s = literal_string(&mut arena, "hELLo wORLD");
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[s], &chunk),
                "Hello World"
            );
        }
        "instr" => {
            let s = literal_string(&mut arena, "hello");
            let sub = literal_string(&mut arena, "lo");
            assert_eq!(
                string_eval_i64(name, &arena, expr_i64, &[s, sub], &chunk),
                4
            );
        }
        "left" => {
            let s = literal_string(&mut arena, "hello");
            let n = literal_i64(&mut arena, 2);
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[s, n], &chunk),
                "he"
            );
        }
        "length" => {
            let s = literal_string(&mut arena, "a\u{00E9}");
            assert_eq!(string_eval_i64(name, &arena, expr_i64, &[s], &chunk), 3);
        }
        "locate" => {
            let s = literal_string(&mut arena, "hello");
            let sub = literal_string(&mut arena, "lo");
            assert_eq!(
                string_eval_i64(name, &arena, expr_i64, &[sub, s], &chunk),
                4
            );
        }
        "lower" => {
            let s = literal_string(&mut arena, "HeLLo");
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[s], &chunk),
                "hello"
            );
        }
        "lpad" => {
            let s = literal_string(&mut arena, "hi");
            let len = literal_i64(&mut arena, 4);
            let pad = literal_string(&mut arena, "x");
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[s, len, pad], &chunk),
                "xxhi"
            );
        }
        "ltrim" => {
            let s = literal_string(&mut arena, "  hi");
            assert_eq!(string_eval_str(name, &arena, expr_str, &[s], &chunk), "hi");
        }
        "money_format" => {
            let f = literal_f64(&mut arena, 1234.5);
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[f], &chunk),
                "1,234.50"
            );
        }
        "null_or_empty" => {
            let s = literal_string(&mut arena, "");
            assert!(string_eval_bool(name, &arena, expr_bool, &[s], &chunk));
        }
        "parse_url" => {
            let url = literal_string(&mut arena, "https://example.com/path?x=1#frag");
            let host = literal_string(&mut arena, "HOST");
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[url, host], &chunk),
                "example.com"
            );
        }
        "regexp_extract" => {
            let text = literal_string(&mut arena, "abc123");
            let pat = literal_string(&mut arena, "(\\d+)");
            let idx = literal_i64(&mut arena, 1);
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[text, pat, idx], &chunk),
                "123"
            );
        }
        "regexp_count" => {
            let text = literal_string(&mut arena, "abc123def456");
            let pat = literal_string(&mut arena, "[0-9]");
            assert_eq!(
                string_eval_i64(name, &arena, expr_i64, &[text, pat], &chunk),
                6
            );
        }
        "regexp_extract_all" => {
            let text = literal_string(&mut arena, "AbCdExCeF");
            let pat = literal_string(&mut arena, "([[:lower:]]+)C([[:lower:]]+)");
            let idx = literal_i64(&mut arena, 0);
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[text, pat, idx], &chunk),
                "[\"bCd\",\"xCe\"]"
            );
        }
        "regexp_position" => {
            let text = literal_string(&mut arena, "a1b2c3d");
            let pat = literal_string(&mut arena, "[0-9]");
            let pos = literal_i64(&mut arena, 4);
            let occ = literal_i64(&mut arena, 2);
            assert_eq!(
                string_eval_i64(name, &arena, expr_i64, &[text, pat, pos, occ], &chunk),
                6
            );
        }
        "regexp_replace" => {
            let text = literal_string(&mut arena, "abc123");
            let pat = literal_string(&mut arena, "\\d+");
            let rep = literal_string(&mut arena, "#");
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[text, pat, rep], &chunk),
                "abc#"
            );
        }
        "repeat" => {
            let s = literal_string(&mut arena, "ab");
            let n = literal_i64(&mut arena, 3);
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[s, n], &chunk),
                "ababab"
            );
        }
        "replace" => {
            let s = literal_string(&mut arena, "hello");
            let from = literal_string(&mut arena, "l");
            let to = literal_string(&mut arena, "x");
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[s, from, to], &chunk),
                "hexxo"
            );
        }
        "reverse" => {
            let s = literal_string(&mut arena, "abc");
            assert_eq!(string_eval_str(name, &arena, expr_str, &[s], &chunk), "cba");
        }
        "right" => {
            let s = literal_string(&mut arena, "hello");
            let n = literal_i64(&mut arena, 2);
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[s, n], &chunk),
                "lo"
            );
        }
        "rpad" => {
            let s = literal_string(&mut arena, "hi");
            let len = literal_i64(&mut arena, 4);
            let pad = literal_string(&mut arena, "x");
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[s, len, pad], &chunk),
                "hixx"
            );
        }
        "rtrim" => {
            let s = literal_string(&mut arena, "hi  ");
            assert_eq!(string_eval_str(name, &arena, expr_str, &[s], &chunk), "hi");
        }
        "space" => {
            let n = literal_i64(&mut arena, 3);
            assert_eq!(string_eval_str(name, &arena, expr_str, &[n], &chunk), "   ");
        }
        "split_part" => {
            let s = literal_string(&mut arena, "a,b,c");
            let delim = literal_string(&mut arena, ",");
            let idx = literal_i64(&mut arena, 2);
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[s, delim, idx], &chunk),
                "b"
            );
        }
        "starts_with" => {
            let s = literal_string(&mut arena, "hello");
            let pre = literal_string(&mut arena, "he");
            assert!(string_eval_bool(name, &arena, expr_bool, &[s, pre], &chunk));
        }
        "str_to_map" => {
            let s = literal_string(&mut arena, "a:1,b:2");
            let entry = literal_string(&mut arena, ",");
            let kv = literal_string(&mut arena, ":");
            let arr =
                eval_string_function(name, &arena, expr_str, &[s, entry, kv], &chunk).unwrap();
            assert_eq!(arr.len(), 1);
            assert!(!arr.is_null(0));
            assert!(matches!(arr.data_type(), DataType::Map(_, _)));
        }
        "substring_index" => {
            let s = literal_string(&mut arena, "a,b,c");
            let delim = literal_string(&mut arena, ",");
            let count = literal_i64(&mut arena, 2);
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[s, delim, count], &chunk),
                "a,b"
            );
        }
        "translate" => {
            let s = literal_string(&mut arena, "abc");
            let from = literal_string(&mut arena, "ab");
            let to = literal_string(&mut arena, "12");
            assert_eq!(
                string_eval_str(name, &arena, expr_str, &[s, from, to], &chunk),
                "12c"
            );
        }
        "trim" => {
            let s = literal_string(&mut arena, "  hi  ");
            assert_eq!(string_eval_str(name, &arena, expr_str, &[s], &chunk), "hi");
        }
        "unhex" => {
            let hex = literal_string(&mut arena, "4142");
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
