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

use super::{ChildTyping, TypingError};
use arrow::datatypes::DataType;
use novarocks_functions::FunctionBindingError;
use novarocks_type_contract::{
    CharacterKind, FunctionOverloadId, ValueKind, trino_can_coerce_scalar,
};

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
        "__array_element_at" | "__map_element_at"
       | "flatten" | "variant_get" | "try_variant_get" | "named_struct"
       | "arrays_overlap"
       | "contains_sequence" | "map_keys" | "map_values" | "array_min"
       | "array_max" | "__array_struct_subfield"
        // SQL's implicit string conversion.
       | "group_concat" | "string_agg" | "starts_with" | "ends_with" | "btrim" | "trim_whitespace"
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

/// Recognize frozen string signatures with no installed implementation after
/// the catalogue has declined the actual arguments.
pub(super) fn unavailable_string_binding(
    name: &str,
    children: &[ChildTyping],
    error: FunctionBindingError,
) -> TypingError {
    if !matches!(
        error,
        FunctionBindingError::UnknownFunction | FunctionBindingError::NoMatchingOverload
    ) {
        return error.into();
    }
    let text = |child: &ChildTyping| {
        trino_can_coerce_scalar(child.value_type.value_type().kind(), &ValueKind::String)
    };
    let bigint = |child: &ChildTyping| {
        trino_can_coerce_scalar(
            child.value_type.value_type().kind(),
            &ValueKind::Scalar(DataType::Int64),
        )
    };
    let overload = match (name, children) {
        ("format", [template, _, ..]) => {
            let actual = template.value_type.value_type().kind();
            let varchar = matches!(actual, ValueKind::String)
                || matches!(actual, ValueKind::BoundedCharacter(character)
                    if character.kind() == CharacterKind::Varchar);
            if !varchar {
                return TypingError::TypeMismatch(format!(
                    "Type of first argument to format() must be VARCHAR (actual: {actual:?})"
                ));
            }
            "builtin.scalar/format/trino-variadic-v1"
        }
        ("$overlay", [source, replacement, start])
            if text(source) && text(replacement) && bigint(start) =>
        {
            "builtin.scalar/$overlay/trino-varchar-three-v1"
        }
        ("$overlay", [source, replacement, start, length])
            if text(source) && text(replacement) && bigint(start) && bigint(length) =>
        {
            "builtin.scalar/$overlay/trino-varchar-four-v1"
        }
        ("trim" | "ltrim" | "rtrim", [source, characters]) if text(source) && text(characters) => {
            let character = matches!(
                source.value_type.value_type().kind(),
                ValueKind::BoundedCharacter(source) if source.kind() == CharacterKind::Char
            );
            match (name, character) {
                ("trim", false) => "builtin.scalar/trim/trino-varchar-codepoints-v1",
                ("trim", true) => "builtin.scalar/trim/trino-char-codepoints-v1",
                ("ltrim", false) => "builtin.scalar/ltrim/trino-varchar-codepoints-v1",
                ("ltrim", true) => "builtin.scalar/ltrim/trino-char-codepoints-v1",
                ("rtrim", false) => "builtin.scalar/rtrim/trino-varchar-codepoints-v1",
                ("rtrim", true) => "builtin.scalar/rtrim/trino-char-codepoints-v1",
                _ => unreachable!("checked trim name"),
            }
        }
        ("format" | "$overlay", _) => return FunctionBindingError::NoMatchingOverload.into(),
        _ => return error.into(),
    };
    FunctionBindingError::UnavailableImplementation(
        FunctionOverloadId::try_new(overload).expect("valid unavailable string overload identity"),
    )
    .into()
}
