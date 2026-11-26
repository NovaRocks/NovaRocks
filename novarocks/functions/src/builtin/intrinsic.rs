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
    InstalledHigherOrder,
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
        super::array_element_at_owner::SUBSCRIPT_NAME,
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
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
        "__row_field",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "__struct_subfield",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "abs",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "acos",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("all_match", BuiltinDisposition::InstalledHigherOrder),
    ("any_match", BuiltinDisposition::InstalledHigherOrder),
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
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
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
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "array_remove",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "array_sort",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "arrays_overlap",
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
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "bitwise_and",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "bitwise_left_shift",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "bitwise_not",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "bitwise_or",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "bitwise_right_shift",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "bitwise_right_shift_arithmetic",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "bitwise_xor",
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
    (
        "chr",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "coalesce",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "codepoint",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "concat",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "concat_ws",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "contains",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "contains_sequence",
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
        "current_date",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "date",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "date_add",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "date_diff",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "date_format",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "date_trunc",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "day",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "day_of_month",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "day_of_week",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "day_of_year",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "degrees",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("dense_rank", BuiltinDisposition::WindowBoundary),
    (
        "dow",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "doy",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
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
        "euclidean_distance",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    ("every", BuiltinDisposition::AggregateBoundary),
    (
        "exp",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("filter", BuiltinDisposition::InstalledHigherOrder),
    ("first_value", BuiltinDisposition::WindowBoundary),
    (
        "flatten",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "floor",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("format", BuiltinDisposition::Unavailable),
    (
        "from_base64",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "from_hex",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "from_unixtime",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "from_utf8",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
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
        "hour",
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
    ("lag", BuiltinDisposition::WindowBoundary),
    (
        "last_day_of_month",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    ("last_value", BuiltinDisposition::WindowBoundary),
    ("lead", BuiltinDisposition::WindowBoundary),
    (
        "least",
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
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
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
    ("map_filter", BuiltinDisposition::InstalledHigherOrder),
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
        "mod",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "month",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
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
        "normalize",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "now",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("ntile", BuiltinDisposition::WindowBoundary),
    (
        "nullif",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    ("nvl", BuiltinDisposition::Unavailable),
    (
        "parse_json",
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
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "random",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
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
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
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
        "sequence",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "sha256",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "sha512",
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
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "split",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "split_part",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "split_to_map",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
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
    ("string_agg", BuiltinDisposition::AggregateBoundary),
    (
        "strpos",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
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
        "title_case",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "to_base64",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "to_big_endian_64",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "to_hex",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("to_json", BuiltinDisposition::Unavailable),
    ("to_unix_timestamp", BuiltinDisposition::Unavailable),
    (
        "to_utf8",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    ("transform", BuiltinDisposition::InstalledHigherOrder),
    ("transform_keys", BuiltinDisposition::InstalledHigherOrder),
    ("transform_values", BuiltinDisposition::InstalledHigherOrder),
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
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "try",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "try_variant_get",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "typeof",
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
    (
        "url_extract_fragment",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "url_extract_host",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "url_extract_parameter",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "url_extract_path",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "url_extract_port",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "url_extract_protocol",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
    (
        "url_extract_query",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::NoRowError),
    ),
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
        "week",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "week_of_year",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "width_bucket",
        BuiltinDisposition::InstalledScalar(FunctionIntrinsicRowError::MayRaise),
    ),
    (
        "year",
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
