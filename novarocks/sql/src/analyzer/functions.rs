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

use std::sync::Arc;

use arrow::datatypes::DataType;

use crate::analysis::TypedExpr;
use novarocks_types::wider_type;

pub(super) fn is_window_only_function(name: &str) -> bool {
    matches!(
        name,
        "row_number"
            | "rank"
            | "dense_rank"
            | "cume_dist"
            | "percent_rank"
            | "ntile"
            | "lag"
            | "lead"
            | "first_value"
            | "last_value"
    )
}

/// Ordinary aggregates with a concrete implementation in the native analytic
/// operator. Catalog membership alone is not enough to admit `OVER`: the BE
/// intentionally supports a smaller, explicit set of aggregate window shapes.
pub(super) fn is_supported_aggregate_window_function(name: &str) -> bool {
    matches!(
        name,
        "count"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "max_by"
            | "min_by"
            | "variance"
            | "var_samp"
            | "stddev"
            | "stddev_samp"
            | "bool_or"
            | "covar_pop"
            | "covar_samp"
            | "corr"
            | "array_agg"
    )
}

pub(super) fn aggregate_window_supports_distinct(name: &str) -> bool {
    name == "array_agg"
}

pub(super) fn aggregate_window_supports_function_order_by(name: &str) -> bool {
    name == "array_agg"
}

fn format_signature_type(data_type: &DataType, map_value_context: bool) -> String {
    match data_type {
        DataType::Null => "null_type".to_string(),
        DataType::Boolean => "boolean".to_string(),
        DataType::Int8 => "tinyint(4)".to_string(),
        DataType::Int16 => "smallint(6)".to_string(),
        DataType::Int32 => "int(11)".to_string(),
        DataType::Int64 => "bigint(20)".to_string(),
        DataType::Float32 => "float".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 => {
            if map_value_context {
                "varchar(20)".to_string()
            } else {
                "varchar(255)".to_string()
            }
        }
        DataType::Binary | DataType::LargeBinary => "varbinary".to_string(),
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            format!("decimal({precision},{scale})")
        }
        DataType::List(item) => {
            format!("array<{}>", format_signature_type(item.data_type(), false))
        }
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return "map<unknown,unknown>".to_string();
            };
            if fields.len() != 2 {
                return "map<unknown,unknown>".to_string();
            }
            format!(
                "map<{},{}>",
                format_signature_type(fields[0].data_type(), false),
                format_signature_type(fields[1].data_type(), true)
            )
        }
        DataType::Struct(fields) => format!(
            "struct<{}>",
            fields
                .iter()
                .map(|field| format_signature_type(field.data_type(), false))
                .collect::<Vec<_>>()
                .join(",")
        ),
        other => format!("{other:?}").to_lowercase(),
    }
}

pub(super) fn no_matching_signature(name: &str, arg_types: &[DataType]) -> String {
    format!(
        "No matching function with signature: {}({}).",
        name,
        arg_types
            .iter()
            .map(|arg| format_signature_type(arg, false))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

#[cfg(test)]
pub(super) fn validate_scalar_function_call(
    name: &str,
    arg_types: &[DataType],
) -> Result<(), String> {
    validate_scalar_function_call_impl(name, arg_types)
}

pub(super) fn validate_scalar_function_call_typed(
    name: &str,
    args: &[TypedExpr],
) -> Result<(), String> {
    let arg_types = args
        .iter()
        .map(|arg| arg.value_type.data_type().clone())
        .collect::<Vec<_>>();
    validate_scalar_function_call_impl(name, &arg_types)
}

fn validate_scalar_function_call_impl(name: &str, arg_types: &[DataType]) -> Result<(), String> {
    if is_mv_state_scalar_function(name) {
        validate_mv_state_scalar_function(name, arg_types)?;
    }
    let expected_arity = match name {
        "cardinality" | "map_keys" | "map_values" => Some(1usize),
        "__array_struct_subfield" => Some(2usize),
        _ => None,
    };
    if let Some(expected) = expected_arity
        && arg_types.len() != expected
    {
        return Err(no_matching_signature(name, arg_types));
    }
    Ok(())
}

fn is_mv_state_scalar_function(name: &str) -> bool {
    matches!(
        name,
        "count_state_union"
            | "count_state_visible"
            | "count_distinct_state_union"
            | "count_distinct_state_visible"
            | "approx_count_distinct_state_union"
            | "approx_count_distinct_state_visible"
            | "avg_state_union"
            | "avg_state_visible"
            | "sum_state_union"
            | "sum_state_visible"
            | "min_state_union"
            | "min_state_visible"
            | "max_state_union"
            | "max_state_visible"
            | "bool_or_state_union"
            | "bool_or_state_visible"
            | "bool_and_state_union"
            | "bool_and_state_visible"
            | "state_all_zero"
            | "mv_group_row_id"
    )
}

fn validate_mv_state_scalar_function(name: &str, arg_types: &[DataType]) -> Result<(), String> {
    let binary_arg = |ty: &DataType| matches!(ty, DataType::Binary | DataType::LargeBinary);
    let zero_check_arg = |ty: &DataType| binary_arg(ty) || matches!(ty, DataType::Int64);
    if name == "mv_group_row_id" {
        if arg_types.is_empty() {
            return Err(no_matching_signature(name, arg_types));
        }
        return Ok(());
    }
    let expected = match name {
        "count_state_union"
        | "count_distinct_state_union"
        | "approx_count_distinct_state_union"
        | "avg_state_union"
        | "sum_state_union"
        | "min_state_union"
        | "max_state_union"
        | "bool_or_state_union"
        | "bool_and_state_union" => 2,
        "avg_state_visible" if (2..=4).contains(&arg_types.len()) => arg_types.len(),
        "sum_state_visible" | "min_state_visible" | "max_state_visible" if arg_types.len() == 2 => {
            2
        }
        "count_state_visible"
        | "count_distinct_state_visible"
        | "approx_count_distinct_state_visible"
        | "avg_state_visible"
        | "sum_state_visible"
        | "min_state_visible"
        | "max_state_visible"
        | "bool_or_state_visible"
        | "bool_and_state_visible"
        | "state_all_zero" => 1,
        _ => return Ok(()),
    };
    let valid_args = if name == "avg_state_visible" && (2..=4).contains(&arg_types.len()) {
        binary_arg(&arg_types[0])
            && binary_arg(&arg_types[1])
            && (arg_types.len() == 2 || matches!(arg_types[2], DataType::Int64))
    } else if matches!(
        name,
        "sum_state_visible" | "min_state_visible" | "max_state_visible"
    ) && arg_types.len() == 2
    {
        binary_arg(&arg_types[0])
    } else if name == "state_all_zero" {
        arg_types.iter().all(zero_check_arg)
    } else {
        arg_types.iter().all(binary_arg)
    };
    if arg_types.len() != expected || !valid_args {
        return Err(no_matching_signature(name, arg_types));
    }
    Ok(())
}

pub(super) fn is_aggregate_function(
    function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    name: &str,
) -> bool {
    function_catalog.contains_aggregate(name)
}

// ---------------------------------------------------------------------------
// Scalar function return type inference
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(super) fn infer_scalar_return_type(name: &str, arg_types: &[DataType]) -> DataType {
    infer_scalar_return_type_with_catalog(
        crate::functions::builtin_sql_function_catalog(),
        name,
        arg_types,
        &crate::compiler::SqlCompileControl::unbounded(),
    )
    .unwrap()
}

/// Infer an unregistered scalar return type using the function snapshot that
/// belongs to the current compiler request. Registered signatures always take
/// precedence; the match below remains the deliberately narrow legacy
/// fallback for complex functions not yet representable in the registry.
pub(super) fn infer_scalar_return_type_with_catalog(
    function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    name: &str,
    arg_types: &[DataType],
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<DataType, crate::analyze_error::AnalyzeError> {
    Ok(
        legacy_scalar_return_type_with_catalog(function_catalog, name, arg_types, control)?
            // This compatibility helper is retained for staged non-analysis
            // consumers. SQL analysis checks the Option-returning function below
            // before it accepts an arbitrary scalar function name.
            .unwrap_or(DataType::Utf8),
    )
}

/// Return the legacy scalar type only when the name is known by either the
/// signature registry or the deliberately limited legacy matcher.
pub(super) fn legacy_scalar_return_type_with_catalog(
    function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    name: &str,
    arg_types: &[DataType],
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<DataType>, crate::analyze_error::AnalyzeError> {
    // Step B: the central signature registry is the single source of
    // truth for nearly all scalar built-ins (~250 names) including the
    // `coalesce`/`if`/`ifnull` cast-match family. If the registry knows
    // the function, its answer wins. The legacy match below stays as a
    // fallback for corner cases the registry can't model: lambda
    // functions (`array_map`, `transform_*`), composite-type-aware
    // extractors (`__array_element_at`, `map_keys`), and functions
    // whose permissive type fallbacks haven't been formalised as
    // signatures yet.
    match function_catalog.resolve_scalar_signature(name, arg_types, control) {
        Ok(resolved) => return Ok(Some(resolved.return_type)),
        Err(error @ novarocks_functions::FunctionResolutionError::Control(_)) => {
            return Err(crate::analyze_error::AnalyzeError::function_resolution(
                error,
            ));
        }
        Err(_) => {}
    }
    Ok(dynamic_scalar_return_type(name, arg_types))
}

/// Type-only portion of the dynamic builtin binder. Literal-dependent result
/// details are applied by the function catalog's exact resolver.
pub(crate) fn dynamic_scalar_return_type(name: &str, arg_types: &[DataType]) -> Option<DataType> {
    Some(match name {
        "repeat" | "hex" | "unhex" | "md5" | "sha2" | "to_base64" | "from_base64"
        | "url_encode" | "url_decode" | "bar" | "char" | "group_concat" | "string_agg"
        | "parse_url" => DataType::Utf8,
        "split_to_map" => DataType::Map(
            Arc::new(arrow::datatypes::Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(arrow::datatypes::Field::new("key", DataType::Utf8, true)),
                        Arc::new(arrow::datatypes::Field::new("value", DataType::Utf8, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ),

        "positive" => DataType::Float64,
        "equiwidth_bucket" => DataType::Int64,

        // Conditional functions -> widened type of value args.
        "if" if arg_types.len() >= 2 => {
            let mut result = arg_types[1].clone();
            for t in &arg_types[2..] {
                result = wider_type(&result, t);
            }
            result
        }
        "if" | "ifnull" | "nullif" | "coalesce" | "nvl" | "greatest" | "least" => {
            if arg_types.is_empty() {
                DataType::Null
            } else {
                let mut result = arg_types[0].clone();
                for t in &arg_types[1..] {
                    result = wider_type(&result, t);
                }
                // StarRocks only registers DATETIME signatures for
                // greatest/least, so pure DATE inputs are widened to DATETIME.
                if matches!(name, "greatest" | "least") && matches!(result, DataType::Date32) {
                    result = DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None);
                }
                result
            }
        }

        // These call sites without a bound catalogue can only describe the
        // physically fixed results. Temporal precision is decided by the owner.
        "date" | "last_day_of_month" => DataType::Date32,
        "date_format" => DataType::Utf8,
        "date_diff" | "year" | "month" | "day" | "day_of_month" | "hour" | "minute" | "second"
        | "day_of_week" | "dow" | "day_of_year" | "doy" | "week" | "week_of_year" | "quarter" => {
            DataType::Int64
        }
        "to_unixtime" => DataType::Float64,

        // Misc
        "version" | "database" | "current_user" | "user" | "uuid" => DataType::Utf8,
        "sleep" => DataType::Boolean,
        // Symmetric ciphers / hashes: aes_*/encode_* return BE-style VARCHAR
        // (raw bytes interpreted as latin-1); to_base64 wraps them in tests.
        // Iceberg internal transform functions.
        "to_binary" => DataType::Binary,
        "count_state_union"
        | "count_distinct_state_union"
        | "approx_count_distinct_state_union"
        | "avg_state_union"
        | "sum_state_union"
        | "min_state_union"
        | "max_state_union"
        | "bool_or_state_union"
        | "bool_and_state_union" => DataType::Binary,
        "count_state_visible"
        | "count_distinct_state_visible"
        | "approx_count_distinct_state_visible"
        | "sum_state_visible"
        | "min_state_visible"
        | "max_state_visible" => DataType::Int64,
        "avg_state_visible" if arg_types.len() == 4 => {
            arg_types.get(3).cloned().unwrap_or(DataType::Float64)
        }
        "avg_state_visible" => DataType::Float64,
        "bool_or_state_visible" | "bool_and_state_visible" => DataType::Boolean,
        "mv_group_row_id" => DataType::Utf8,
        "state_all_zero" => DataType::Boolean,
        "array_position" | "cardinality" => DataType::Int64,
        "grouping" => DataType::Int64,
        "array_min" | "array_max" => array_item_type(arg_types.first()).unwrap_or(DataType::Null),
        "all_match" | "any_match" | "contains" | "contains_sequence" | "arrays_overlap" => {
            DataType::Boolean
        }
        "array_sort" | "slice" | "array_remove" | "array_distinct" => {
            arg_types.first().cloned().unwrap_or(DataType::Null)
        }
        "flatten" => infer_array_flatten_return_type(arg_types),
        "array_intersect" => infer_array_intersect_return_type(arg_types),
        "map_keys" => match arg_types.first() {
            Some(DataType::Map(entries, _)) => match entries.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => DataType::List(Arc::new(
                    arrow::datatypes::Field::new("item", fields[0].data_type().clone(), true),
                )),
                _ => DataType::Null,
            },
            _ => DataType::Null,
        },
        "map_values" => match arg_types.first() {
            Some(DataType::Map(entries, _)) => match entries.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => DataType::List(Arc::new(
                    arrow::datatypes::Field::new("item", fields[1].data_type().clone(), true),
                )),
                _ => DataType::Null,
            },
            _ => DataType::Null,
        },
        "map_entries" => infer_map_entries_return_type(arg_types),
        "zip" => infer_arrays_zip_return_type(arg_types),
        "map_concat" => infer_map_concat_return_type(arg_types),
        "map_filter" | "transform_keys" | "transform_values" => {
            arg_types.first().cloned().unwrap_or(DataType::Null)
        }
        "map" => infer_map_constructor_return_type(arg_types),
        "row" => infer_struct_constructor_return_type(arg_types),
        "named_struct" => infer_named_struct_return_type(arg_types),
        "get_json_bool" | "get_variant_bool" | "json_exists" => DataType::Boolean,
        "get_json_int" | "get_variant_int" => DataType::Int64,
        "get_json_double" | "get_variant_double" => DataType::Float64,
        "variant_get" | "try_variant_get" => DataType::LargeBinary,
        "json_query" | "json_extract" | "get_json_string" | "get_json_object" | "json_object"
        | "json_array" | "to_json" | "parse_json" | "variant_typeof" => DataType::Utf8,
        "__array_struct_subfield" => DataType::Null,
        "__array_element_at" => match arg_types.first() {
            Some(DataType::List(item)) => item.data_type().clone(),
            _ => DataType::Null,
        },
        "__map_element_at" => match arg_types.first() {
            Some(DataType::Map(entries, _)) => match entries.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => fields[1].data_type().clone(),
                _ => DataType::Null,
            },
            _ => DataType::Null,
        },

        _ => return None,
    })
}

fn list_type(item_type: DataType) -> DataType {
    DataType::List(Arc::new(arrow::datatypes::Field::new(
        "item", item_type, true,
    )))
}

fn array_item_type(data_type: Option<&DataType>) -> Option<DataType> {
    match data_type {
        Some(DataType::List(item)) => Some(item.data_type().clone()),
        _ => None,
    }
}

fn infer_array_flatten_return_type(arg_types: &[DataType]) -> DataType {
    match arg_types.first() {
        Some(DataType::List(outer)) => match outer.data_type() {
            DataType::List(inner) => list_type(inner.data_type().clone()),
            _ => arg_types[0].clone(),
        },
        _ => DataType::Null,
    }
}

fn infer_array_intersect_return_type(arg_types: &[DataType]) -> DataType {
    let item_type = arg_types
        .iter()
        .filter_map(|ty| array_item_type(Some(ty)))
        .reduce(|acc, ty| wider_type(&acc, &ty));
    item_type.map(list_type).unwrap_or(DataType::Null)
}

fn map_key_value_types(data_type: &DataType) -> Option<(DataType, DataType)> {
    let DataType::Map(entries, _) = data_type else {
        return None;
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return None;
    };
    if fields.len() != 2 {
        return None;
    }
    Some((fields[0].data_type().clone(), fields[1].data_type().clone()))
}

fn map_type(key_type: DataType, value_type: DataType) -> DataType {
    DataType::Map(
        Arc::new(arrow::datatypes::Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(arrow::datatypes::Field::new("key", key_type, true)),
                    Arc::new(arrow::datatypes::Field::new("value", value_type, true)),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}

fn infer_map_entries_return_type(arg_types: &[DataType]) -> DataType {
    match arg_types.first() {
        Some(DataType::Map(entries, _)) => list_type(entries.data_type().clone()),
        _ => DataType::Null,
    }
}

fn infer_arrays_zip_return_type(arg_types: &[DataType]) -> DataType {
    let fields = arg_types
        .iter()
        .enumerate()
        .map(|(idx, data_type)| {
            let item_type = array_item_type(Some(data_type)).unwrap_or(DataType::Null);
            Arc::new(arrow::datatypes::Field::new(
                format!("col{}", idx + 1),
                item_type,
                true,
            ))
        })
        .collect::<Vec<_>>();
    list_type(DataType::Struct(arrow::datatypes::Fields::from(fields)))
}

fn infer_map_concat_return_type(arg_types: &[DataType]) -> DataType {
    let mut iter = arg_types.iter().filter_map(map_key_value_types);
    let Some((mut key_type, mut value_type)) = iter.next() else {
        return DataType::Null;
    };
    for (next_key, next_value) in iter {
        key_type = wider_type(&key_type, &next_key);
        value_type = wider_type(&value_type, &next_value);
    }
    map_type(key_type, value_type)
}

fn infer_map_constructor_return_type(arg_types: &[DataType]) -> DataType {
    let key_type = arg_types
        .iter()
        .step_by(2)
        .cloned()
        .reduce(|acc, ty| wider_type(&acc, &ty))
        .unwrap_or(DataType::Null);
    let value_type = arg_types
        .iter()
        .skip(1)
        .step_by(2)
        .cloned()
        .reduce(|acc, ty| wider_type(&acc, &ty))
        .unwrap_or(DataType::Null);
    DataType::Map(
        Arc::new(arrow::datatypes::Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(arrow::datatypes::Field::new("key", key_type, true)),
                    Arc::new(arrow::datatypes::Field::new("value", value_type, true)),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}

fn infer_struct_constructor_return_type(arg_types: &[DataType]) -> DataType {
    let fields = arg_types
        .iter()
        .enumerate()
        .map(|(idx, data_type)| {
            Arc::new(arrow::datatypes::Field::new(
                format!("col{}", idx + 1),
                data_type.clone(),
                true,
            ))
        })
        .collect::<Vec<_>>();
    DataType::Struct(arrow::datatypes::Fields::from(fields))
}

fn infer_named_struct_return_type(arg_types: &[DataType]) -> DataType {
    let fields = arg_types
        .iter()
        .skip(1)
        .step_by(2)
        .enumerate()
        .map(|(idx, data_type)| {
            Arc::new(arrow::datatypes::Field::new(
                format!("col{}", idx + 1),
                data_type.clone(),
                true,
            ))
        })
        .collect::<Vec<_>>();
    DataType::Struct(arrow::datatypes::Fields::from(fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_types::largeint;

    fn resolve_aggregate(name: &str, arg_types: &[DataType]) -> DataType {
        let arguments = arg_types
            .iter()
            .cloned()
            .map(|data_type| novarocks_functions::FunctionArgument::Value {
                value_type: novarocks_functions::FunctionValueType::new(data_type, true),
                constant: None,
            })
            .collect::<Vec<_>>();
        let binding = crate::functions::builtin_engine_function_catalog()
            .resolve_bound_user(
                name,
                novarocks_functions::FunctionKind::Aggregate,
                novarocks_functions::FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: arguments.len(),
                    expected_result_type: None,
                },
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap_or_else(|error| panic!("aggregate {name} must resolve: {error}"));
        crate::functions::aggregate_result_type(&binding)
            .data_type()
            .clone()
    }

    #[test]
    fn aggregate_catalog_decimal_return_type_is_canonical() {
        assert_eq!(
            resolve_aggregate("sum", &[DataType::Decimal128(20, 2)]),
            DataType::Decimal128(38, 2)
        );
        assert_eq!(
            resolve_aggregate("avg", &[DataType::Decimal128(10, 3)]),
            DataType::Decimal128(38, 9)
        );
    }

    fn array_type(item_type: DataType) -> DataType {
        DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item", item_type, true,
        )))
    }

    #[test]
    fn infer_scalar_return_type_for_collection_count_functions() {
        let int_array = array_type(DataType::Int32);

        assert_eq!(
            infer_scalar_return_type("cardinality", std::slice::from_ref(&int_array)),
            DataType::Int64
        );
        assert_eq!(
            infer_scalar_return_type("array_position", &[int_array, DataType::Int32]),
            DataType::Int64
        );
    }

    #[test]
    fn infer_scalar_return_type_for_to_binary_is_binary() {
        assert_eq!(
            infer_scalar_return_type("to_binary", &[DataType::Utf8, DataType::Utf8]),
            DataType::Binary
        );
    }

    #[test]
    fn infer_scalar_return_type_for_join_row_key_is_utf8() {
        assert_eq!(
            infer_scalar_return_type(
                "join_row_key",
                &[
                    DataType::Binary,
                    DataType::Int64,
                    DataType::LargeBinary,
                    DataType::Int64,
                ],
            ),
            DataType::Utf8
        );
    }

    #[test]
    fn count_state_scalar_functions_require_binary_inputs() {
        assert_eq!(
            infer_scalar_return_type("count_state_union", &[DataType::Binary, DataType::Binary]),
            DataType::Binary
        );
        assert_eq!(
            infer_scalar_return_type("count_state_visible", &[DataType::Binary]),
            DataType::Int64
        );
        validate_scalar_function_call("count_state_union", &[DataType::Binary, DataType::Binary])
            .unwrap();
        validate_scalar_function_call("count_state_visible", &[DataType::Binary]).unwrap();

        let err = validate_scalar_function_call("count_state_union", &[DataType::Utf8])
            .expect_err("count_state_union should reject non-binary arity/type");
        assert_eq!(
            err,
            "No matching function with signature: count_state_union(varchar(255))."
        );
    }

    #[test]
    fn state_all_zero_scalar_function_accepts_binary_or_int64_count_input() {
        assert_eq!(
            crate::functions::resolve_scalar_function("state_all_zero", &[DataType::Binary]),
            Ok(DataType::Boolean)
        );
        assert_eq!(
            crate::functions::resolve_scalar_function("state_all_zero", &[DataType::Int64]),
            Ok(DataType::Boolean)
        );
        assert_eq!(
            infer_scalar_return_type("state_all_zero", &[DataType::LargeBinary]),
            DataType::Boolean
        );
        validate_scalar_function_call("state_all_zero", &[DataType::Binary]).unwrap();
        validate_scalar_function_call("state_all_zero", &[DataType::LargeBinary]).unwrap();
        validate_scalar_function_call("state_all_zero", &[DataType::Int64]).unwrap();

        let err = validate_scalar_function_call("state_all_zero", &[DataType::Utf8])
            .expect_err("state_all_zero should reject non-state/count input");
        assert_eq!(
            err,
            "No matching function with signature: state_all_zero(varchar(255))."
        );
    }

    #[test]
    fn bool_state_scalar_functions_require_binary_inputs() {
        for name in ["bool_or_state_union", "bool_and_state_union"] {
            assert_eq!(
                infer_scalar_return_type(name, &[DataType::Binary, DataType::Binary]),
                DataType::Binary
            );
            validate_scalar_function_call(name, &[DataType::LargeBinary, DataType::Binary])
                .unwrap();
        }

        for name in ["bool_or_state_visible", "bool_and_state_visible"] {
            assert_eq!(
                infer_scalar_return_type(name, &[DataType::LargeBinary]),
                DataType::Boolean
            );
            validate_scalar_function_call(name, &[DataType::Binary]).unwrap();
        }

        let err = validate_scalar_function_call("bool_or_state_union", &[DataType::Utf8])
            .expect_err("bool_or_state_union should reject non-binary arity/type");
        assert_eq!(
            err,
            "No matching function with signature: bool_or_state_union(varchar(255))."
        );
    }

    #[test]
    fn sum_state_scalar_functions_require_binary_inputs() {
        assert_eq!(
            infer_scalar_return_type("sum_state_union", &[DataType::Binary, DataType::Binary]),
            DataType::Binary
        );
        assert_eq!(
            infer_scalar_return_type("sum_state_visible", &[DataType::LargeBinary]),
            DataType::Int64
        );
        validate_scalar_function_call(
            "sum_state_union",
            &[DataType::LargeBinary, DataType::Binary],
        )
        .unwrap();
        validate_scalar_function_call("sum_state_visible", &[DataType::Binary]).unwrap();

        let err = validate_scalar_function_call("sum_state_visible", &[DataType::Utf8])
            .expect_err("sum_state_visible should reject non-binary type");
        assert_eq!(
            err,
            "No matching function with signature: sum_state_visible(varchar(255))."
        );
    }

    #[test]
    fn avg_state_scalar_functions_require_binary_inputs() {
        assert_eq!(
            infer_scalar_return_type("avg_state_union", &[DataType::Binary, DataType::Binary]),
            DataType::Binary
        );
        assert_eq!(
            infer_scalar_return_type(
                "avg_state_visible",
                &[DataType::LargeBinary, DataType::Binary],
            ),
            DataType::Float64
        );
        validate_scalar_function_call(
            "avg_state_union",
            &[DataType::LargeBinary, DataType::Binary],
        )
        .unwrap();
        validate_scalar_function_call(
            "avg_state_visible",
            &[DataType::Binary, DataType::LargeBinary],
        )
        .unwrap();
        validate_scalar_function_call(
            "avg_state_visible",
            &[DataType::Binary, DataType::LargeBinary, DataType::Int64],
        )
        .unwrap();

        let err =
            validate_scalar_function_call("avg_state_visible", &[DataType::Utf8, DataType::Binary])
                .expect_err("avg_state_visible should reject non-binary type");
        assert_eq!(
            err,
            "No matching function with signature: avg_state_visible(varchar(255), varbinary)."
        );

        let err = validate_scalar_function_call(
            "avg_state_visible",
            &[DataType::Binary, DataType::Binary, DataType::Utf8],
        )
        .expect_err("avg_state_visible should reject non-integer decimal scale");
        assert_eq!(
            err,
            "No matching function with signature: avg_state_visible(varbinary, varbinary, varchar(255))."
        );
    }

    #[test]
    fn min_max_state_scalar_functions_require_binary_inputs() {
        for name in ["min_state_union", "max_state_union"] {
            assert_eq!(
                infer_scalar_return_type(name, &[DataType::Binary, DataType::Binary]),
                DataType::Binary
            );
            validate_scalar_function_call(name, &[DataType::LargeBinary, DataType::Binary])
                .unwrap();
        }

        for name in ["min_state_visible", "max_state_visible"] {
            assert_eq!(
                infer_scalar_return_type(name, &[DataType::LargeBinary]),
                DataType::Int64
            );
            validate_scalar_function_call(name, &[DataType::Binary]).unwrap();
        }

        let err = validate_scalar_function_call("min_state_union", &[DataType::Utf8])
            .expect_err("min_state_union should reject non-binary arity/type");
        assert_eq!(
            err,
            "No matching function with signature: min_state_union(varchar(255))."
        );
    }

    #[test]
    fn distinct_state_scalar_functions_require_binary_inputs() {
        for (union, visible) in [
            ("count_distinct_state_union", "count_distinct_state_visible"),
            (
                "approx_count_distinct_state_union",
                "approx_count_distinct_state_visible",
            ),
        ] {
            assert_eq!(
                infer_scalar_return_type(union, &[DataType::Binary, DataType::Binary]),
                DataType::Binary
            );
            assert_eq!(
                infer_scalar_return_type(visible, &[DataType::LargeBinary]),
                DataType::Int64
            );
            validate_scalar_function_call(union, &[DataType::LargeBinary, DataType::Binary])
                .unwrap();
            validate_scalar_function_call(visible, &[DataType::Binary]).unwrap();

            let err = validate_scalar_function_call(visible, &[DataType::Utf8])
                .expect_err("distinct state visible should reject non-binary input");
            assert_eq!(
                err,
                format!("No matching function with signature: {visible}(varchar(255)).")
            );
        }
    }

    #[test]
    fn infer_scalar_return_type_for_json_getters() {
        assert_eq!(
            infer_scalar_return_type("get_json_bool", &[DataType::Utf8, DataType::Utf8]),
            DataType::Boolean
        );
        assert_eq!(
            infer_scalar_return_type("get_json_int", &[DataType::Utf8, DataType::Utf8]),
            DataType::Int64
        );
        assert_eq!(
            infer_scalar_return_type("get_json_double", &[DataType::Utf8, DataType::Utf8]),
            DataType::Float64
        );
        assert_eq!(
            infer_scalar_return_type("get_json_string", &[DataType::Utf8, DataType::Utf8]),
            DataType::Utf8
        );
    }

    #[test]
    fn infer_scalar_return_type_for_row_constructor() {
        let actual = infer_scalar_return_type("row", &[DataType::Int32, DataType::Float64]);
        let DataType::Struct(fields) = actual else {
            panic!("row() should infer a struct type");
        };
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].data_type(), &DataType::Int32);
        assert_eq!(fields[1].data_type(), &DataType::Float64);
    }

    #[test]
    fn sum_largeint_returns_largeint() {
        let largeint_type = DataType::FixedSizeBinary(largeint::LARGEINT_BYTE_WIDTH);
        // This fixture declares a SQL LARGEINT source; the carrier alone does
        // not distinguish numeric values from UUID or ordinary fixed binary.
        let source_type = novarocks_functions::FunctionValueType::try_with_logical_type(
            largeint_type.clone(),
            true,
            novarocks_type_contract::ValueLogicalType::LargeInt,
        )
        .unwrap();
        let arguments = [novarocks_functions::FunctionArgument::Value {
            value_type: source_type,
            constant: None,
        }];
        let binding = crate::functions::builtin_engine_function_catalog()
            .resolve_bound_user(
                "sum",
                novarocks_functions::FunctionKind::Aggregate,
                novarocks_functions::FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: arguments.len(),
                    expected_result_type: None,
                },
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
        let result = crate::functions::aggregate_result_type(&binding);
        assert_eq!(
            result.logical_type(),
            novarocks_type_contract::ValueLogicalType::LargeInt
        );
        assert_eq!(result.data_type().clone(), largeint_type);
    }

    #[test]
    fn state_combinator_aggregate_functions_return_binary_state() {
        for name in [
            "count_state",
            "count_state_signed",
            "count_state_merge",
            "sum_state",
            "sum_state_merge",
            "sum_state_signed",
            "avg_state",
            "avg_state_merge",
            "avg_state_signed",
            "min_state",
            "min_state_merge",
            "min_state_signed",
            "max_state",
            "max_state_merge",
            "max_state_signed",
            "bool_or_state",
            "bool_or_state_merge",
            "bool_or_state_signed",
            "bool_and_state",
            "bool_and_state_merge",
            "bool_and_state_signed",
            "count_distinct_state",
            "count_distinct_state_merge",
            "count_distinct_state_signed",
            "approx_count_distinct_state",
            "approx_count_distinct_state_merge",
            "approx_count_distinct_state_signed",
        ] {
            assert!(
                is_aggregate_function(crate::functions::builtin_sql_function_catalog(), name),
                "{name}"
            );
            let arg_types = if name.ends_with("_signed") {
                vec![DataType::Struct(
                    vec![
                        Arc::new(arrow::datatypes::Field::new("value", DataType::Int64, true)),
                        Arc::new(arrow::datatypes::Field::new(
                            "change_op",
                            DataType::Int8,
                            false,
                        )),
                    ]
                    .into(),
                )]
            } else {
                vec![DataType::Int64]
            };
            assert_eq!(
                resolve_aggregate(name, &arg_types),
                DataType::Binary,
                "{name}"
            );
        }
    }

    #[test]
    fn array_intersection_keeps_string_common_type_for_float_and_datetime() {
        for scalar in [
            DataType::Float32,
            DataType::Float64,
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        ] {
            let scalar_array = list_type(scalar);
            let string_array = list_type(DataType::Utf8);
            assert_eq!(
                infer_array_intersect_return_type(&[scalar_array.clone(), string_array.clone()]),
                string_array
            );
            assert_eq!(
                infer_array_intersect_return_type(&[string_array.clone(), scalar_array.clone()]),
                string_array
            );
            validate_scalar_function_call("arrays_overlap", &[scalar_array, string_array]).unwrap();
        }
    }
}
