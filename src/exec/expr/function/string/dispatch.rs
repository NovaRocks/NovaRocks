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
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::ArrayRef;
use std::collections::HashMap;

#[derive(Clone, Copy)]
pub struct FunctionMeta {
    pub name: &'static str,
    pub min_args: usize,
    pub max_args: usize,
}

pub fn register(map: &mut HashMap<&'static str, crate::exec::expr::function::FunctionKind>) {
    for (name, canonical) in STRING_FUNCTIONS {
        map.insert(
            *name,
            crate::exec::expr::function::FunctionKind::String(*canonical),
        );
    }
}

pub fn metadata(name: &str) -> Option<FunctionMeta> {
    STRING_METADATA.iter().find(|m| m.name == name).copied()
}

pub fn eval_string_function(
    name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let canonical = STRING_FUNCTIONS
        .iter()
        .find_map(|(alias, target)| (*alias == name).then_some(*target))
        .unwrap_or(name);

    match canonical {
        "append_trailing_char_if_absent" => {
            super::append_trailing_char_if_absent::eval_append_trailing_char_if_absent(
                arena, expr, args, chunk,
            )
        }
        "ascii" => super::ascii::eval_ascii(arena, expr, args, chunk),
        "bar" => super::bar::eval_bar(arena, expr, args, chunk),
        "char_length" => super::length_ops::eval_char_length(arena, expr, args, chunk),
        "concat" => super::concat::eval_concat(arena, expr, args, chunk),
        "concat_ws" => super::concat_ws::eval_concat_ws(arena, expr, args, chunk),
        "crc32" => super::crc32::eval_crc32(arena, expr, args, chunk),
        "murmur_hash3_32" => super::murmur_hash3_32::eval_murmur_hash3_32(arena, expr, args, chunk),
        "ends_with" => super::prefix_suffix_ops::eval_ends_with(arena, expr, args, chunk),
        "field" => super::field::eval_field(arena, expr, args, chunk),
        "find_in_set" => super::find_in_set::eval_find_in_set(arena, expr, args, chunk),
        "format_bytes" => super::format_bytes::eval_format_bytes(arena, expr, args, chunk),
        "group_concat" => super::group_concat::eval_group_concat(arena, expr, args, chunk),
        "hex" => super::hex::eval_hex(arena, expr, args, chunk),
        "initcap" => super::initcap::eval_initcap(arena, expr, args, chunk),
        "instr" => super::locate_ops::eval_instr(arena, expr, args, chunk),
        "left" => super::left_right_ops::eval_left(arena, expr, args, chunk),
        "length" => super::length_ops::eval_length(arena, expr, args, chunk),
        "locate" => super::locate_ops::eval_locate(arena, expr, args, chunk),
        "ngram_search" => super::ngram_search::eval_ngram_search(arena, expr, args, chunk),
        "ngram_search_case_insensitive" => {
            super::ngram_search::eval_ngram_search_case_insensitive(arena, expr, args, chunk)
        }
        "lower" => super::case_ops::eval_lower(arena, expr, args, chunk),
        "lpad" => super::pad_ops::eval_lpad(arena, expr, args, chunk),
        "ltrim" => super::trim_ops::eval_ltrim(arena, expr, args, chunk),
        "money_format" => super::money_format::eval_money_format(arena, expr, args, chunk),
        "null_or_empty" => super::null_or_empty::eval_null_or_empty(arena, expr, args, chunk),
        "parse_url" => super::parse_url::eval_parse_url(arena, expr, args, chunk),
        "regexp_count" => super::regexp_count::eval_regexp_count(arena, expr, args, chunk),
        "regexp_extract" => super::regexp_extract::eval_regexp_extract(arena, expr, args, chunk),
        "regexp_extract_all" => {
            super::regexp_extract_all::eval_regexp_extract_all(arena, expr, args, chunk)
        }
        "regexp_position" => super::regexp_position::eval_regexp_position(arena, expr, args, chunk),
        "regexp_replace" => super::regexp_replace::eval_regexp_replace(arena, expr, args, chunk),
        "repeat" => super::repeat::eval_repeat(arena, expr, args, chunk),
        "replace" => super::replace::eval_replace(arena, expr, args, chunk),
        "reverse" => super::reverse::eval_reverse(arena, expr, args, chunk),
        "right" => super::left_right_ops::eval_right(arena, expr, args, chunk),
        "rpad" => super::pad_ops::eval_rpad(arena, expr, args, chunk),
        "rtrim" => super::trim_ops::eval_rtrim(arena, expr, args, chunk),
        "space" => super::space::eval_space(arena, expr, args, chunk),
        "split_part" => super::split_part::eval_split_part(arena, expr, args, chunk),
        "strpos" => super::locate_ops::eval_strpos(arena, expr, args, chunk),
        "starts_with" => super::prefix_suffix_ops::eval_starts_with(arena, expr, args, chunk),
        "str_to_map" => super::str_to_map::eval_str_to_map(arena, expr, args, chunk),
        "substring_index" => super::substring_index::eval_substring_index(arena, expr, args, chunk),
        "translate" => super::translate::eval_translate(arena, expr, args, chunk),
        "trim" => super::trim_ops::eval_trim(arena, expr, args, chunk),
        "unhex" => super::unhex::eval_unhex(arena, expr, args, chunk),
        "url_decode" => super::url_ops::eval_url_decode(arena, expr, args, chunk),
        "url_encode" => super::url_ops::eval_url_encode(arena, expr, args, chunk),
        "url_extract_host" => super::url_ops::eval_url_extract_host(arena, expr, args, chunk),
        "uuid" => super::uuid::eval_uuid(arena, expr, args, chunk),
        other => Err(format!("unsupported string function: {}", other)),
    }
}
static STRING_FUNCTIONS: &[(&str, &str)] = &[
    (
        "append_trailing_char_if_absent",
        "append_trailing_char_if_absent",
    ),
    ("ascii", "ascii"),
    ("bar", "bar"),
    ("char_length", "char_length"),
    ("concat", "concat"),
    ("concat_ws", "concat_ws"),
    ("crc32", "crc32"),
    ("murmur_hash3_32", "murmur_hash3_32"),
    ("ends_with", "ends_with"),
    ("field", "field"),
    ("find_in_set", "find_in_set"),
    ("format_bytes", "format_bytes"),
    ("group_concat", "group_concat"),
    ("hex", "hex"),
    ("initcap", "initcap"),
    ("instr", "instr"),
    ("lcase", "lower"),
    ("left", "left"),
    ("length", "length"),
    ("locate", "locate"),
    ("ngram_search", "ngram_search"),
    (
        "ngram_search_case_insensitive",
        "ngram_search_case_insensitive",
    ),
    ("lower", "lower"),
    ("lpad", "lpad"),
    ("ltrim", "ltrim"),
    ("money_format", "money_format"),
    ("null_or_empty", "null_or_empty"),
    ("parse_url", "parse_url"),
    ("regexp_count", "regexp_count"),
    ("regexp_extract", "regexp_extract"),
    ("regexp_extract_all", "regexp_extract_all"),
    ("regexp_position", "regexp_position"),
    ("regexp_replace", "regexp_replace"),
    ("repeat", "repeat"),
    ("replace", "replace"),
    ("reverse", "reverse"),
    ("right", "right"),
    ("rpad", "rpad"),
    ("rtrim", "rtrim"),
    ("space", "space"),
    ("split_part", "split_part"),
    ("strpos", "strpos"),
    ("strpos_instance", "strpos"),
    ("starts_with", "starts_with"),
    ("str_to_map", "str_to_map"),
    ("strleft", "left"),
    ("strright", "right"),
    ("substring_index", "substring_index"),
    ("translate", "translate"),
    ("trim", "trim"),
    ("unhex", "unhex"),
    ("url_decode", "url_decode"),
    ("url_encode", "url_encode"),
    ("url_extract_", "url_extract_host"),
    ("url_extract_host", "url_extract_host"),
    ("uuid", "uuid"),
];

static STRING_METADATA: &[FunctionMeta] = &[
    FunctionMeta {
        name: "append_trailing_char_if_absent",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "ascii",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "bar",
        min_args: 4,
        max_args: 4,
    },
    FunctionMeta {
        name: "char_length",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "concat",
        min_args: 1,
        max_args: usize::MAX,
    },
    FunctionMeta {
        name: "concat_ws",
        min_args: 2,
        max_args: usize::MAX,
    },
    FunctionMeta {
        name: "crc32",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "murmur_hash3_32",
        min_args: 1,
        max_args: usize::MAX,
    },
    FunctionMeta {
        name: "ends_with",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "field",
        min_args: 2,
        max_args: usize::MAX,
    },
    FunctionMeta {
        name: "find_in_set",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "format_bytes",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "group_concat",
        min_args: 1,
        max_args: usize::MAX,
    },
    FunctionMeta {
        name: "hex",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "initcap",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "instr",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "lower",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "left",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "length",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "locate",
        min_args: 2,
        max_args: 3,
    },
    FunctionMeta {
        name: "ngram_search",
        min_args: 3,
        max_args: 3,
    },
    FunctionMeta {
        name: "ngram_search_case_insensitive",
        min_args: 3,
        max_args: 3,
    },
    FunctionMeta {
        name: "lpad",
        min_args: 3,
        max_args: 3,
    },
    FunctionMeta {
        name: "ltrim",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "money_format",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "null_or_empty",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "parse_url",
        min_args: 2,
        max_args: 3,
    },
    FunctionMeta {
        name: "regexp_count",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "regexp_extract",
        min_args: 3,
        max_args: 3,
    },
    FunctionMeta {
        name: "regexp_extract_all",
        min_args: 3,
        max_args: 3,
    },
    FunctionMeta {
        name: "regexp_position",
        min_args: 2,
        max_args: 4,
    },
    FunctionMeta {
        name: "regexp_replace",
        min_args: 3,
        max_args: 3,
    },
    FunctionMeta {
        name: "repeat",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "replace",
        min_args: 3,
        max_args: 3,
    },
    FunctionMeta {
        name: "reverse",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "right",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "rpad",
        min_args: 3,
        max_args: 3,
    },
    FunctionMeta {
        name: "rtrim",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "space",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "split_part",
        min_args: 3,
        max_args: 3,
    },
    FunctionMeta {
        name: "strpos",
        min_args: 2,
        max_args: 3,
    },
    FunctionMeta {
        name: "starts_with",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "str_to_map",
        min_args: 3,
        max_args: 3,
    },
    FunctionMeta {
        name: "substring_index",
        min_args: 3,
        max_args: 3,
    },
    FunctionMeta {
        name: "translate",
        min_args: 3,
        max_args: 3,
    },
    FunctionMeta {
        name: "trim",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "unhex",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "url_decode",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "url_encode",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "url_extract_host",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "uuid",
        min_args: 0,
        max_args: 0,
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::string::test_utils::*;
    use arrow::array::Array;
    use arrow::array::{BinaryArray, BooleanArray, Int64Array, MapArray, StringArray, StructArray};
    use arrow::datatypes::DataType;

    fn eval_str(
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

    fn eval_i64(
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

    fn eval_f64(
        name: &str,
        arena: &ExprArena,
        expr: ExprId,
        args: &[ExprId],
        chunk: &Chunk,
    ) -> f64 {
        let arr = eval_string_function(name, arena, expr, args, chunk).unwrap();
        let arr = arr
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        arr.value(0)
    }

    fn eval_bool(
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

    #[test]
    fn test_append_ascii_length() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_str = typed_null(&mut arena, DataType::Utf8);
        let expr_i64 = typed_null(&mut arena, DataType::Int64);

        let s = literal_string(&mut arena, "path");
        let ch = literal_string(&mut arena, "/");
        assert_eq!(
            eval_str(
                "append_trailing_char_if_absent",
                &arena,
                expr_str,
                &[s, ch],
                &chunk
            ),
            "path/"
        );

        let a = literal_string(&mut arena, "A");
        assert_eq!(eval_i64("ascii", &arena, expr_i64, &[a], &chunk), 65);

        let utf = literal_string(&mut arena, "a\u{00E9}");
        assert_eq!(eval_i64("char_length", &arena, expr_i64, &[utf], &chunk), 2);
        assert_eq!(eval_i64("length", &arena, expr_i64, &[utf], &chunk), 3);
    }

    #[test]
    fn test_concat_and_case() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_str = typed_null(&mut arena, DataType::Utf8);

        let a = literal_string(&mut arena, "a");
        let b = literal_string(&mut arena, "b");
        let c = literal_string(&mut arena, "c");
        assert_eq!(
            eval_str("concat", &arena, expr_str, &[a, b, c], &chunk),
            "abc"
        );

        let sep = literal_string(&mut arena, "-");
        let x = literal_string(&mut arena, "a");
        let y = literal_string(&mut arena, "b");
        let z = literal_string(&mut arena, "c");
        assert_eq!(
            eval_str("concat_ws", &arena, expr_str, &[sep, x, y, z], &chunk),
            "a-b-c"
        );

        let mixed = literal_string(&mut arena, "HeLLo");
        assert_eq!(
            eval_str("lower", &arena, expr_str, &[mixed], &chunk),
            "hello"
        );
        let mixed2 = literal_string(&mut arena, "HeLLo");
        assert_eq!(
            eval_str("lcase", &arena, expr_str, &[mixed2], &chunk),
            "hello"
        );

        let title = literal_string(&mut arena, "hELLo wORLD");
        assert_eq!(
            eval_str("initcap", &arena, expr_str, &[title], &chunk),
            "Hello World"
        );
    }

    #[test]
    fn test_starts_ends_locate_instr() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_bool = typed_null(&mut arena, DataType::Boolean);
        let expr_i64 = typed_null(&mut arena, DataType::Int64);

        let s = literal_string(&mut arena, "hello");
        let he = literal_string(&mut arena, "he");
        assert!(eval_bool(
            "starts_with",
            &arena,
            expr_bool,
            &[s, he],
            &chunk
        ));

        let s2 = literal_string(&mut arena, "hello");
        let lo = literal_string(&mut arena, "lo");
        assert!(eval_bool("ends_with", &arena, expr_bool, &[s2, lo], &chunk));

        let s3 = literal_string(&mut arena, "hello");
        let sub = literal_string(&mut arena, "lo");
        assert_eq!(eval_i64("locate", &arena, expr_i64, &[sub, s3], &chunk), 4);
        let s3_2 = literal_string(&mut arena, "hello");
        let sub_2 = literal_string(&mut arena, "l");
        let start = literal_i64(&mut arena, 4);
        assert_eq!(
            eval_i64("locate", &arena, expr_i64, &[sub_2, s3_2, start], &chunk),
            4
        );
        let s3_3 = literal_string(&mut arena, "aébc");
        let sub_3 = literal_string(&mut arena, "é");
        assert_eq!(
            eval_i64("locate", &arena, expr_i64, &[sub_3, s3_3], &chunk),
            2
        );
        let s4 = literal_string(&mut arena, "hello");
        let sub2 = literal_string(&mut arena, "lo");
        assert_eq!(eval_i64("instr", &arena, expr_i64, &[s4, sub2], &chunk), 4);
    }

    #[test]
    fn test_left_right_variants() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_str = typed_null(&mut arena, DataType::Utf8);
        let n = literal_i64(&mut arena, 2);

        let s = literal_string(&mut arena, "hello");
        assert_eq!(eval_str("left", &arena, expr_str, &[s, n], &chunk), "he");
        let s2 = literal_string(&mut arena, "hello");
        assert_eq!(eval_str("right", &arena, expr_str, &[s2, n], &chunk), "lo");
        let s3 = literal_string(&mut arena, "hello");
        assert_eq!(
            eval_str("strleft", &arena, expr_str, &[s3, n], &chunk),
            "he"
        );
        let s4 = literal_string(&mut arena, "hello");
        assert_eq!(
            eval_str("strright", &arena, expr_str, &[s4, n], &chunk),
            "lo"
        );
    }

    #[test]
    fn test_pad_trim_space() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_str = typed_null(&mut arena, DataType::Utf8);
        let n = literal_i64(&mut arena, 4);
        let pad = literal_string(&mut arena, "x");

        let s = literal_string(&mut arena, "hi");
        assert_eq!(
            eval_str("lpad", &arena, expr_str, &[s, n, pad], &chunk),
            "xxhi"
        );
        let s2 = literal_string(&mut arena, "hi");
        assert_eq!(
            eval_str("rpad", &arena, expr_str, &[s2, n, pad], &chunk),
            "hixx"
        );

        let s3 = literal_string(&mut arena, "  hi");
        assert_eq!(eval_str("ltrim", &arena, expr_str, &[s3], &chunk), "hi");
        let s4 = literal_string(&mut arena, "hi  ");
        assert_eq!(eval_str("rtrim", &arena, expr_str, &[s4], &chunk), "hi");
        let s5 = literal_string(&mut arena, "  hi  ");
        assert_eq!(eval_str("trim", &arena, expr_str, &[s5], &chunk), "hi");

        let spaces = literal_i64(&mut arena, 3);
        assert_eq!(
            eval_str("space", &arena, expr_str, &[spaces], &chunk),
            "   "
        );
    }

    #[test]
    fn test_field_find_in_set() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_i64 = typed_null(&mut arena, DataType::Int64);

        let target = literal_string(&mut arena, "b");
        let a = literal_string(&mut arena, "a");
        let b = literal_string(&mut arena, "b");
        let c = literal_string(&mut arena, "c");
        assert_eq!(
            eval_i64("field", &arena, expr_i64, &[target, a, b, c], &chunk),
            2
        );

        let target2 = literal_string(&mut arena, "b");
        let set = literal_string(&mut arena, "a,b,c");
        assert_eq!(
            eval_i64("find_in_set", &arena, expr_i64, &[target2, set], &chunk),
            2
        );
    }

    #[test]
    fn test_format_bytes_money_null_or_empty() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_str = typed_null(&mut arena, DataType::Utf8);
        let expr_bool = typed_null(&mut arena, DataType::Boolean);

        let bytes = literal_i64(&mut arena, 1024);
        assert_eq!(
            eval_str("format_bytes", &arena, expr_str, &[bytes], &chunk),
            "1.00 KB"
        );

        let money = literal_f64(&mut arena, 1234.5);
        assert_eq!(
            eval_str("money_format", &arena, expr_str, &[money], &chunk),
            "1,234.50"
        );
        let money_i = literal_i64(&mut arena, 12345);
        assert_eq!(
            eval_str("money_format", &arena, expr_str, &[money_i], &chunk),
            "12,345.00"
        );

        let empty = literal_string(&mut arena, "");
        assert!(eval_bool(
            "null_or_empty",
            &arena,
            expr_bool,
            &[empty],
            &chunk
        ));
    }

    #[test]
    fn test_parse_url_and_regex() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_str = typed_null(&mut arena, DataType::Utf8);

        let url = literal_string(&mut arena, "https://example.com/path?x=1#frag");
        let host = literal_string(&mut arena, "HOST");
        assert_eq!(
            eval_str("parse_url", &arena, expr_str, &[url, host], &chunk),
            "example.com"
        );

        let url2 = literal_string(&mut arena, "https://example.com/path?x=1#frag");
        let query = literal_string(&mut arena, "QUERY");
        let key = literal_string(&mut arena, "x");
        assert_eq!(
            eval_str("parse_url", &arena, expr_str, &[url2, query, key], &chunk),
            "1"
        );

        let text = literal_string(&mut arena, "abc123");
        let pat = literal_string(&mut arena, "(\\d+)");
        let idx = literal_i64(&mut arena, 1);
        assert_eq!(
            eval_str(
                "regexp_extract",
                &arena,
                expr_str,
                &[text, pat, idx],
                &chunk
            ),
            "123"
        );
        let text_count = literal_string(&mut arena, "abc123");
        let pat_count = literal_string(&mut arena, "[0-9]");
        let expr_i64_count = typed_null(&mut arena, DataType::Int64);
        assert_eq!(
            eval_i64(
                "regexp_count",
                &arena,
                expr_i64_count,
                &[text_count, pat_count],
                &chunk
            ),
            3
        );
        let text_all = literal_string(&mut arena, "abc123");
        let pat_all = literal_string(&mut arena, "(\\d+)");
        let idx_all = literal_i64(&mut arena, 1);
        assert_eq!(
            eval_str(
                "regexp_extract_all",
                &arena,
                expr_str,
                &[text_all, pat_all, idx_all],
                &chunk
            ),
            "[\"123\"]"
        );
        let text_pos = literal_string(&mut arena, "a1b2c3d");
        let pat_pos = literal_string(&mut arena, "[0-9]");
        let start_pos = literal_i64(&mut arena, 4);
        let occ = literal_i64(&mut arena, 2);
        let expr_i64 = typed_null(&mut arena, DataType::Int64);
        assert_eq!(
            eval_i64(
                "regexp_position",
                &arena,
                expr_i64,
                &[text_pos, pat_pos, start_pos, occ],
                &chunk
            ),
            6
        );

        let text2 = literal_string(&mut arena, "abc123");
        let pat2 = literal_string(&mut arena, "\\d+");
        let rep = literal_string(&mut arena, "#");
        assert_eq!(
            eval_str(
                "regexp_replace",
                &arena,
                expr_str,
                &[text2, pat2, rep],
                &chunk
            ),
            "abc#"
        );
    }

    #[test]
    fn test_repeat_replace_reverse() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_str = typed_null(&mut arena, DataType::Utf8);

        let s = literal_string(&mut arena, "ab");
        let n = literal_i64(&mut arena, 3);
        assert_eq!(
            eval_str("repeat", &arena, expr_str, &[s, n], &chunk),
            "ababab"
        );

        let s2 = literal_string(&mut arena, "hello");
        let from = literal_string(&mut arena, "l");
        let to = literal_string(&mut arena, "x");
        assert_eq!(
            eval_str("replace", &arena, expr_str, &[s2, from, to], &chunk),
            "hexxo"
        );

        let s3 = literal_string(&mut arena, "abc");
        assert_eq!(eval_str("reverse", &arena, expr_str, &[s3], &chunk), "cba");
    }

    #[test]
    fn test_split_part_str_to_map_substring_index() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_str = typed_null(&mut arena, DataType::Utf8);

        let s = literal_string(&mut arena, "a,b,c");
        let delim = literal_string(&mut arena, ",");
        let idx = literal_i64(&mut arena, 2);
        assert_eq!(
            eval_str("split_part", &arena, expr_str, &[s, delim, idx], &chunk),
            "b"
        );
        let s_overlap = literal_string(&mut arena, "abc##567###234");
        let delim_overlap = literal_string(&mut arena, "##");
        let idx_overlap = literal_i64(&mut arena, -1);
        assert_eq!(
            eval_str(
                "split_part",
                &arena,
                expr_str,
                &[s_overlap, delim_overlap, idx_overlap],
                &chunk
            ),
            "234"
        );

        let s2 = literal_string(&mut arena, "a,b,c");
        let delim2 = literal_string(&mut arena, ",");
        let count = literal_i64(&mut arena, 2);
        assert_eq!(
            eval_str(
                "substring_index",
                &arena,
                expr_str,
                &[s2, delim2, count],
                &chunk
            ),
            "a,b"
        );

        let s3 = literal_string(&mut arena, "a:1,b:2");
        let entry = literal_string(&mut arena, ",");
        let kv = literal_string(&mut arena, ":");
        let out =
            eval_string_function("str_to_map", &arena, expr_str, &[s3, entry, kv], &chunk).unwrap();
        let map = out.as_any().downcast_ref::<MapArray>().unwrap();
        let entry_arr = map.value(0);
        let struct_arr = entry_arr.as_any().downcast_ref::<StructArray>().unwrap();
        let keys = struct_arr
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let vals = struct_arr
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(keys.value(0), "a");
        assert_eq!(vals.value(0), "1");
        assert_eq!(keys.value(1), "b");
        assert_eq!(vals.value(1), "2");
    }

    #[test]
    fn test_hex_unhex_and_group_concat() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_str = typed_null(&mut arena, DataType::Utf8);

        let s = literal_string(&mut arena, "AB");
        assert_eq!(eval_str("hex", &arena, expr_str, &[s], &chunk), "4142");
        let hex = literal_string(&mut arena, "4142");
        let unhex_out = eval_string_function("unhex", &arena, expr_str, &[hex], &chunk).unwrap();
        let unhex_out = unhex_out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(unhex_out.value(0), b"AB");

        let err = eval_string_function("group_concat", &arena, expr_str, &[s], &chunk)
            .expect_err("group_concat should error in string dispatcher");
        assert!(err.contains("aggregate"));
    }

    #[test]
    fn test_uuid_format() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_str = typed_null(&mut arena, DataType::Utf8);
        let out = eval_string_function("uuid", &arena, expr_str, &[], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        let v = out.value(0);
        assert_eq!(v.len(), 36);
        let b = v.as_bytes();
        assert_eq!(b[8], b'-');
        assert_eq!(b[13], b'-');
        assert_eq!(b[18], b'-');
        assert_eq!(b[23], b'-');
        assert_eq!(b[14], b'4');
        assert!(matches!(b[19], b'8' | b'9' | b'a' | b'b'));
    }

    #[test]
    fn test_strpos_url_and_ngram() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_i64 = typed_null(&mut arena, DataType::Int64);
        let expr_str = typed_null(&mut arena, DataType::Utf8);
        let expr_f64 = typed_null(&mut arena, DataType::Float64);

        let haystack = literal_string(&mut arena, "hello world hello");
        let needle = literal_string(&mut arena, "hello");
        let instance = literal_i64(&mut arena, -1);
        assert_eq!(
            eval_i64(
                "strpos",
                &arena,
                expr_i64,
                &[haystack, needle, instance],
                &chunk
            ),
            13
        );

        let url = literal_string(&mut arena, "https://starrocks.快速.com/test/api/v1");
        assert_eq!(
            eval_str("url_extract_host", &arena, expr_str, &[url], &chunk),
            "starrocks.快速.com"
        );
        let raw = literal_string(
            &mut arena,
            "https://docs.starrocks.io/en-us/latest/quick_start/Deploy",
        );
        let encoded = eval_str("url_encode", &arena, expr_str, &[raw], &chunk);
        assert_eq!(
            encoded,
            "https%3A%2F%2Fdocs.starrocks.io%2Fen-us%2Flatest%2Fquick_start%2FDeploy"
        );
        let encoded_expr = literal_string(&mut arena, &encoded);
        assert_eq!(
            eval_str("url_decode", &arena, expr_str, &[encoded_expr], &chunk),
            "https://docs.starrocks.io/en-us/latest/quick_start/Deploy"
        );

        let h = literal_string(&mut arena, "chinese");
        let n = literal_string(&mut arena, "china");
        let gram = literal_i64(&mut arena, 4);
        assert_eq!(
            eval_f64("ngram_search", &arena, expr_f64, &[h, n, gram], &chunk),
            0.5
        );
    }
}
