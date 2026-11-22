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

//! Functions-owned, source-audited builtin implementation admission facts.
//! Legacy type inspection does not establish selected implementation availability.

use novarocks_type_contract::FunctionIntrinsicRowError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BuiltinDisposition {
    InstalledScalar(FunctionIntrinsicRowError),
    AggregateBoundary,
    WindowBoundary,
    LoweredOnly,
    Unavailable,
}

// Exactly the advertised canonical registry plus owner-derived dynamic vocabulary.
// New declarations require an explicit selected-owner review; unknown has no fact.
const BUILTIN_DISPOSITIONS: &[(&str, BuiltinDisposition)] = &[
    (
        "__array_element_at",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "__array_literal",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "__array_struct_subfield",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "__json_canonical_key",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        crate::native_like::ESCAPE_FUNCTION_NAME,
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "__map_element_at",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "__map_subscript",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        super::negation_owner::NAME,
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "__struct_subfield",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "abs",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "acos",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "add_months",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "adddate",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("all_match", BuiltinDisposition::Unavailable),
    ("any_match", BuiltinDisposition::Unavailable),
    ("any_value", BuiltinDisposition::AggregateBoundary),
    (
        "approx_count_distinct",
        BuiltinDisposition::AggregateBoundary,
    ),
    (
        "approx_count_distinct_state_union",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "approx_count_distinct_state_visible",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "array_distinct",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "array_intersect",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "array_join",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "array_max",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "array_min",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "array_position",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "array_remove",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "array_sort",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "arrays_overlap",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "ascii",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "asin",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "assert_true",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "atan",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "atan2",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "avg_state_union",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "avg_state_visible",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "bar",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "bit_shift_left",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "bit_shift_right",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "bit_shift_right_logical",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "bitand",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "bitnot",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "bitor",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "bitxor",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("bool_and", BuiltinDisposition::AggregateBoundary),
    (
        "bool_and_state_union",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "bool_and_state_visible",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    ("bool_or", BuiltinDisposition::AggregateBoundary),
    (
        "bool_or_state_union",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "bool_or_state_visible",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "cardinality",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("case", BuiltinDisposition::LoweredOnly),
    ("cast", BuiltinDisposition::LoweredOnly),
    (
        "cbrt",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "ceil",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "ceiling",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("char", BuiltinDisposition::Unavailable),
    (
        "char_length",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("character_length", BuiltinDisposition::Unavailable),
    (
        "coalesce",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "concat",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "concat_ws",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "contains",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "contains_sequence",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "convert_tz",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("corr", BuiltinDisposition::AggregateBoundary),
    (
        "cos",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("cosh", BuiltinDisposition::Unavailable),
    (
        "cosine_similarity",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "count_distinct_state_union",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "count_distinct_state_visible",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "count_state_union",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "count_state_visible",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    ("covar_pop", BuiltinDisposition::AggregateBoundary),
    ("covar_samp", BuiltinDisposition::AggregateBoundary),
    (
        "crc32",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("cume_dist", BuiltinDisposition::WindowBoundary),
    (
        "curdate",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "current_date",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "current_time",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "current_timestamp",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("current_user", BuiltinDisposition::Unavailable),
    (
        "curtime",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("database", BuiltinDisposition::Unavailable),
    (
        "date",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "date_add",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "date_format",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "date_sub",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "date_trunc",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "datediff",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "day",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "dayofmonth",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "dayofweek",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "dayofyear",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "days_add",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "days_diff",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "days_sub",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "dceil",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("degrees", BuiltinDisposition::Unavailable),
    (
        "degress",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("dense_rank", BuiltinDisposition::WindowBoundary),
    (
        "dexp",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "dfloor",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "dlog1",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "dlog10",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "dpow",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "dround",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "dsqrt",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "e",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "element_at",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "equiwidth_bucket",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    ("every", BuiltinDisposition::AggregateBoundary),
    (
        "exp",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("filter", BuiltinDisposition::Unavailable),
    ("first_value", BuiltinDisposition::WindowBoundary),
    (
        "flatten",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "floor",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "fmod",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("format", BuiltinDisposition::Unavailable),
    (
        "fpow",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "from_base64",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "from_binary",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "from_unixtime",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "get_json_bool",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "get_json_double",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "get_json_int",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "get_json_object",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "get_json_string",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "get_variant_bool",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "get_variant_double",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "get_variant_int",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "get_variant_string",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "greatest",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("group_concat", BuiltinDisposition::AggregateBoundary),
    ("grouping", BuiltinDisposition::LoweredOnly),
    (
        "hex",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "hour",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "hours_add",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "hours_diff",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "hours_sub",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "if",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "ifnull",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "initcap",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "instr",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("is_not_null", BuiltinDisposition::Unavailable),
    ("is_null", BuiltinDisposition::Unavailable),
    ("isnotnull", BuiltinDisposition::Unavailable),
    ("isnull", BuiltinDisposition::Unavailable),
    (
        "join_row_key",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("json_array", BuiltinDisposition::Unavailable),
    (
        "json_exists",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("json_extract", BuiltinDisposition::Unavailable),
    (
        "json_keys",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "json_length",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "json_object",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "json_query",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "l2_distance",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    ("lag", BuiltinDisposition::WindowBoundary),
    (
        "last_day",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("last_value", BuiltinDisposition::WindowBoundary),
    ("lead", BuiltinDisposition::WindowBoundary),
    (
        "least",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "left",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "length",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "ln",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "localtime",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "localtimestamp",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "locate",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "log",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "log10",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "log2",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "lower",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "lpad",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "ltrim",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "map",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "map_concat",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "map_entries",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("map_filter", BuiltinDisposition::Unavailable),
    (
        "map_keys",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "map_values",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("max_by", BuiltinDisposition::AggregateBoundary),
    ("max_n", BuiltinDisposition::AggregateBoundary),
    (
        "max_state_union",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "max_state_visible",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "md5",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("min_by", BuiltinDisposition::AggregateBoundary),
    ("min_n", BuiltinDisposition::AggregateBoundary),
    (
        "min_state_union",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "min_state_visible",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "minute",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "minutes_add",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "minutes_diff",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "minutes_sub",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "mod",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "month",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "months_add",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "months_diff",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "months_sub",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "murmur_hash3_32",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "mv_group_row_id",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "named_struct",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("ndv", BuiltinDisposition::AggregateBoundary),
    ("negative", BuiltinDisposition::LoweredOnly),
    (
        "now",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("ntile", BuiltinDisposition::WindowBoundary),
    (
        "nullif",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("nvl", BuiltinDisposition::Unavailable),
    (
        "parse_json",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "parse_url",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("percent_rank", BuiltinDisposition::WindowBoundary),
    ("percentile_approx", BuiltinDisposition::AggregateBoundary),
    (
        "percentile_approx_weighted",
        BuiltinDisposition::AggregateBoundary,
    ),
    (
        "pi",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("position", BuiltinDisposition::Unavailable),
    (
        "positive",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "pow",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "power",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "quarter",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "radians",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "rand",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "random",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("rank", BuiltinDisposition::WindowBoundary),
    (
        "regexp_count",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "regexp_extract",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "regexp_extract_all",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "regexp_position",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "regexp_replace",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "repeat",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "replace",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "reverse",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "right",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "round",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "row",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("row_number", BuiltinDisposition::WindowBoundary),
    (
        "rpad",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "rtrim",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "second",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "seconds_add",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "seconds_diff",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "seconds_sub",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "sequence",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "sha2",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "sign",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "sin",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("sinh", BuiltinDisposition::Unavailable),
    (
        "sleep",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "slice",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "split",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "split_part",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "split_to_map",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "sqrt",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "state_all_zero",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    ("std", BuiltinDisposition::AggregateBoundary),
    ("stddev", BuiltinDisposition::AggregateBoundary),
    ("stddev_pop", BuiltinDisposition::AggregateBoundary),
    ("stddev_samp", BuiltinDisposition::AggregateBoundary),
    (
        "str_to_date",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("string_agg", BuiltinDisposition::AggregateBoundary),
    (
        "strleft",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "strright",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "subdate",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "substr",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "substring",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "sum_state_union",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "sum_state_visible",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "tan",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("tanh", BuiltinDisposition::Unavailable),
    (
        "timestamp",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "timestampdiff",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "to_base64",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "to_binary",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "to_datetime",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("to_json", BuiltinDisposition::Unavailable),
    ("to_unix_timestamp", BuiltinDisposition::Unavailable),
    ("transform", BuiltinDisposition::Unavailable),
    ("transform_keys", BuiltinDisposition::Unavailable),
    ("transform_values", BuiltinDisposition::Unavailable),
    (
        "translate",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "trim",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "truncate",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "try_variant_get",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    ("typeof", BuiltinDisposition::LoweredOnly),
    (
        "unhex",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "unix_timestamp",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "upper",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "url_decode",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "url_encode",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("user", BuiltinDisposition::Unavailable),
    (
        "uuid",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("var_pop", BuiltinDisposition::AggregateBoundary),
    ("var_samp", BuiltinDisposition::AggregateBoundary),
    ("variance", BuiltinDisposition::AggregateBoundary),
    ("variance_pop", BuiltinDisposition::AggregateBoundary),
    ("variance_samp", BuiltinDisposition::AggregateBoundary),
    (
        "variant_get",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "variant_typeof",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("version", BuiltinDisposition::Unavailable),
    (
        "weekofyear",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "weeks_add",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "weeks_diff",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "weeks_sub",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "year",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "years_add",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "years_diff",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "years_sub",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "zip",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
];

pub fn builtin_disposition(name: &str) -> Option<BuiltinDisposition> {
    BUILTIN_DISPOSITIONS
        .binary_search_by_key(&name, |(name, _)| *name)
        .ok()
        .map(|index| BUILTIN_DISPOSITIONS[index].1)
}

/// Borrow the complete source-audited disposition manifest in canonical order.
/// This is declaration inventory; it does not install a CPU implementation.
pub fn builtin_dispositions() -> &'static [(&'static str, BuiltinDisposition)] {
    BUILTIN_DISPOSITIONS
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn disposition_manifest_is_unique_ordered_and_unknown_has_no_fact() {
        let entries = builtin_dispositions();
        let names = entries
            .iter()
            .map(|(name, _)| *name)
            .collect::<BTreeSet<_>>();
        assert!(!entries.is_empty());
        assert_eq!(names.len(), entries.len());
        assert!(entries.windows(2).all(|pair| pair[0].0 < pair[1].0));
        for &(name, disposition) in entries {
            assert_eq!(builtin_disposition(name), Some(disposition));
        }
        assert_eq!(builtin_disposition("not_an_advertised_builtin"), None);
    }
}
