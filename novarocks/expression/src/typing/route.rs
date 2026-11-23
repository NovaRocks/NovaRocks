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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScalarTypingRoute {
    Shared,
    Legacy,
}
/// Callers supply a resolved, lowercase scalar name. A name's entire
/// family stays on the SQL owner as long as that owner applies any special
/// typing rule to it. Ordinary resolution still distinguishes function kinds.
pub fn scalar_typing_route(name: &str) -> ScalarTypingRoute {
    match name {
        // SQL-specific rewrites or validation.
        "strpos" | "normalize" | "__array_element_at" | "__map_element_at" | "typeof"
       | "flatten"  | "if" | "variant_get" | "try_variant_get" | "named_struct"
       | "arrays_overlap"
       | "contains_sequence" | "map_keys" | "map_values" | "array_min"
       | "array_max" | "__array_struct_subfield" | "translate"
        // SQL's implicit string conversion.
       | "concat_ws" | "group_concat" | "string_agg"
        | "instr" | "locate" | "split" | "starts_with" | "ends_with"
       | "regexp_extract" | "regexp_extract_all" | "regexp_replace" | "lpad" | "rpad"
       | "replace" | "ascii" | "char_length" | "character_length" | "initcap" | "left"
       | "length" | "lower" | "ltrim" | "reverse" | "right" | "rtrim"
       | "strleft" | "strright" | "substr" | "substring" | "trim" | "upper" | "btrim"
       | "trim_whitespace"
        // High-order calls need SQL's lambda argument author.
       | "transform" | "any_match" | "all_match" | "none_match"
       | "filter" | "array_sort" | "map_filter"
       | "transform_keys" | "transform_values" | "reduce" | "zip_with" | "map_zip_with"
        // Materialized view state families use dedicated SQL validation.
       | "count_state_union" | "count_state_visible"
       | "count_distinct_state_union" | "count_distinct_state_visible"
       | "approx_count_distinct_state_union" | "approx_count_distinct_state_visible"
       | "avg_state_union" | "avg_state_visible" | "sum_state_union" | "sum_state_visible"
       | "min_state_union" | "min_state_visible" | "max_state_union" | "max_state_visible"
       | "bool_or_state_union" | "bool_or_state_visible" | "bool_and_state_union"
       | "bool_and_state_visible" | "state_all_zero" | "mv_group_row_id" => ScalarTypingRoute::Legacy,
        _ => ScalarTypingRoute::Shared,
    }
}
