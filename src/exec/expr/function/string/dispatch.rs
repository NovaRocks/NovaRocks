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
        "raise_error" => super::raise_error::eval_raise_error(arena, expr, args, chunk),
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
    ("raise_error", "raise_error"),
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
    FunctionMeta {
        name: "raise_error",
        min_args: 1,
        max_args: 1,
    },
];
