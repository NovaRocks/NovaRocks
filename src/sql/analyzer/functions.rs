use std::sync::Arc;

use arrow::datatypes::DataType;

use crate::sql::types::wider_type;

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
            | "session_number"
    )
}

pub(super) fn infer_window_return_type(name: &str, arg_types: &[DataType]) -> DataType {
    match name {
        "row_number" | "rank" | "dense_rank" | "ntile" => DataType::Int64,
        "cume_dist" | "percent_rank" => DataType::Float64,
        "lag" | "lead" | "first_value" | "last_value" => {
            arg_types.first().cloned().unwrap_or(DataType::Null)
        }
        "session_number" => DataType::Int64,
        _ => arg_types.first().cloned().unwrap_or(DataType::Null),
    }
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

fn no_matching_signature(name: &str, arg_types: &[DataType]) -> String {
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

pub(super) fn validate_scalar_function_call(
    name: &str,
    arg_types: &[DataType],
) -> Result<(), String> {
    if name == "map" && !arg_types.len().is_multiple_of(2) {
        return Err(no_matching_signature(name, arg_types));
    }
    let expected_arity = match name {
        "cardinality" | "array_length" | "map_size" | "map_keys" | "map_values" | "array_min"
        | "array_max" => Some(1usize),
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

pub(super) fn validate_aggregate_function_call(
    name: &str,
    arg_types: &[DataType],
) -> Result<(), String> {
    match name {
        "sum_map" => validate_sum_map_arguments(arg_types),
        _ => Ok(()),
    }
}

fn infer_date_trunc_return_type(arg_types: &[DataType]) -> DataType {
    match arg_types.get(1) {
        Some(DataType::Date32) => DataType::Date32,
        Some(DataType::Timestamp(unit, tz)) => DataType::Timestamp(*unit, tz.clone()),
        _ => DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
    }
}

fn validate_sum_map_arguments(arg_types: &[DataType]) -> Result<(), String> {
    let Some(arg_type) = arg_types.first() else {
        return Ok(());
    };
    if matches!(arg_type, DataType::Null) {
        return Ok(());
    }
    let DataType::Map(entries, _) = arg_type else {
        return Ok(());
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return Ok(());
    };
    if fields.len() != 2 {
        return Ok(());
    }
    if is_sum_map_scalar_kv_type(fields[0].data_type())
        && is_sum_map_scalar_kv_type(fields[1].data_type())
    {
        if is_sum_map_supported_value_type(fields[1].data_type()) {
            Ok(())
        } else {
            Err(format!(
                "unsupported value type:{}",
                sum_map_value_type_name(fields[1].data_type())
            ))
        }
    } else {
        Err("sum_map only support scalar KV".to_string())
    }
}

fn is_sum_map_scalar_kv_type(data_type: &DataType) -> bool {
    !matches!(
        data_type,
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Struct(_)
            | DataType::Map(_, _)
            | DataType::Union(_, _)
    )
}

fn is_sum_map_supported_value_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::FixedSizeBinary(_)
    )
}

fn sum_map_value_type_name(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Null => "NULL_TYPE",
        DataType::Boolean => "BOOLEAN",
        DataType::Int8 => "TINYINT",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INT",
        DataType::Int64 => "BIGINT",
        DataType::Float32 => "FLOAT",
        DataType::Float64 => "DOUBLE",
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "DECIMAL",
        DataType::Date32 => "DATE",
        DataType::Timestamp(_, _) => "DATETIME",
        DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR",
        DataType::FixedSizeBinary(_) => "LARGEINT",
        DataType::Binary | DataType::LargeBinary => "VARBINARY",
        DataType::List(_) => "ARRAY",
        DataType::LargeList(_) | DataType::FixedSizeList(_, _) => "ARRAY",
        DataType::Struct(_) => "STRUCT",
        DataType::Map(_, _) => "MAP",
        DataType::Union(_, _) => "UNION",
        _ => "UNKNOWN",
    }
}

pub(super) fn is_aggregate_function(name: &str) -> bool {
    // Keep in sync with expr_compiler::is_aggregate_function.
    matches!(
        name,
        "count"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "count_if"
            | "any_value"
            | "group_concat"
            | "string_agg"
            | "bitmap_agg"
            | "bitmap_union"
            | "bitmap_union_count"
            | "bitmap_union_int"
            | "multi_distinct_count"
            | "array_agg"
            | "array_agg_distinct"
            | "array_unique_agg"
            | "sum_map"
            | "map_agg"
            | "percentile_approx"
            | "percentile_approx_weighted"
            | "percentile_cont"
            | "percentile_disc"
            | "percentile_disc_lc"
            | "percentile_union"
            | "approx_count_distinct"
            | "approx_count_distinct_hll_sketch"
            | "approx_top_k"
            | "ds_hll_accumulate"
            | "ds_hll_combine"
            | "ds_hll_estimate"
            | "ds_hll_count_distinct"
            | "ds_hll_count_distinct_union"
            | "ds_hll_count_distinct_merge"
            | "hll_union"
            | "hll_union_agg"
            | "hll_raw_agg"
            | "hll_cardinality"
            | "ndv"
            | "variance"
            | "variance_samp"
            | "variance_pop"
            | "var_samp"
            | "var_pop"
            | "stddev"
            | "stddev_samp"
            | "stddev_pop"
            | "covar_samp"
            | "covar_pop"
            | "corr"
            | "max_by"
            | "min_by"
            | "mann_whitney_u_test"
            | "bool_or"
            | "bool_and"
            | "boolor_agg"
            | "booland_agg"
            | "every"
            | "min_n"
            | "max_n"
            | "dict_merge"
    )
}

// ---------------------------------------------------------------------------
// Scalar function return type inference
// ---------------------------------------------------------------------------

pub(super) fn infer_scalar_return_type(name: &str, arg_types: &[DataType]) -> DataType {
    match name {
        // String functions
        "upper"
        | "lower"
        | "trim"
        | "ltrim"
        | "rtrim"
        | "reverse"
        | "replace"
        | "lpad"
        | "rpad"
        | "concat"
        | "concat_ws"
        | "substr"
        | "substring"
        | "left"
        | "right"
        | "repeat"
        | "space"
        | "hex"
        | "unhex"
        | "md5"
        | "sha2"
        | "to_base64"
        | "from_base64"
        | "url_encode"
        | "url_decode"
        | "translate"
        | "initcap"
        | "regexp_extract"
        | "regexp_replace"
        | "append_trailing_char_if_absent"
        | "money_format"
        | "char"
        | "elt"
        | "format"
        | "strleft"
        | "strright"
        | "md5sum"
        | "sm3"
        | "group_concat"
        | "string_agg"
        | "substring_index"
        | "parse_url"
        | "str_to_map" => DataType::Utf8,

        // Math functions that return the same type as input
        "abs" => arg_types.first().cloned().unwrap_or(DataType::Float64),

        // Math functions that return Int64
        "ceil" | "ceiling" | "floor" => DataType::Int64,

        // round/truncate:
        // - Decimal input -> Decimal128 with adjusted scale
        // - Non-decimal without explicit scale -> Int64
        // - Non-decimal with explicit scale -> Float64
        "round" | "truncate" => match arg_types.first() {
            Some(DataType::Decimal128(_, s)) => {
                // If second arg is an integer type, the value is the target
                // decimal places.  We can't see the value here (only the type),
                // so we keep the original scale.  The execution ROUND already
                // handles the conversion.  To match StarRocks display, we'll
                // rely on the execution layer's output scale adjustment.
                DataType::Decimal128(38, *s)
            }
            _ if arg_types.len() >= 2 => DataType::Float64,
            _ => DataType::Int64,
        },

        // Math functions that return Float64
        "mod" | "pow" | "power" | "sqrt" | "exp" | "ln" | "log" | "log2" | "log10" | "sin"
        | "cos" | "tan" | "asin" | "acos" | "atan" | "atan2" | "radians" | "degrees" | "pi"
        | "e" | "sign" | "rand" | "random" => DataType::Float64,

        // String length/position -> Int32
        "length" | "char_length" | "character_length" | "bit_length" | "instr" | "locate"
        | "position" | "find_in_set" | "strcmp" | "ascii" | "ord" => DataType::Int32,

        // Conditional functions -> widened type of args
        "if" | "ifnull" | "nullif" | "coalesce" | "nvl" => {
            if arg_types.is_empty() {
                DataType::Null
            } else {
                let mut result = arg_types[0].clone();
                for t in &arg_types[1..] {
                    result = wider_type(&result, t);
                }
                result
            }
        }

        // Date/time
        "now" | "current_timestamp" | "current_date" | "curdate" => {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        }
        "date_format" | "from_unixtime" | "time_format" => DataType::Utf8,
        "date_add" | "date_sub" | "adddate" | "subdate" | "days_add" | "days_sub" | "weeks_add"
        | "weeks_sub" | "months_add" | "months_sub" | "years_add" | "years_sub"
        | "timestampadd" | "sec_to_time" | "hours_add" | "hours_sub" | "minutes_add"
        | "minutes_sub" | "seconds_add" | "seconds_sub" | "microseconds_add"
        | "microseconds_sub" => {
            // Return the same type as the date/timestamp input argument.
            if let Some(dt) = arg_types.first() {
                match dt {
                    DataType::Date32 => DataType::Date32,
                    DataType::Timestamp(u, tz) => DataType::Timestamp(*u, tz.clone()),
                    _ => DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                }
            } else {
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
            }
        }
        "date_trunc" => infer_date_trunc_return_type(arg_types),
        "year" | "month" | "day" | "dayofmonth" | "hour" | "minute" | "second" | "dayofweek"
        | "yearweek" | "dayofyear" | "weekofyear" | "quarter" | "hour_from_unixtime" => {
            DataType::Int32
        }
        "unix_timestamp" | "to_unix_timestamp" | "datediff" | "timestampdiff" | "months_diff"
        | "years_diff" | "weeks_diff" | "days_diff" | "hours_diff" | "minutes_diff"
        | "seconds_diff" | "to_days" | "time_to_sec" => DataType::Int64,
        "to_date" | "str_to_date" | "from_days" | "makedate" | "last_day" | "next_day" => {
            DataType::Date32
        }

        // Misc
        "version" | "database" | "current_user" | "user" | "uuid" | "bitmap_to_string" => {
            DataType::Utf8
        }
        "sleep" => DataType::Boolean,
        "murmur_hash3_32" => DataType::Int32,
        "md5sum_numeric" => DataType::FixedSizeBinary(crate::common::largeint::LARGEINT_BYTE_WIDTH),
        "hll_hash"
        | "ds_hll_count_distinct_state"
        | "to_bitmap"
        | "to_binary"
        | "encode_row_id" => DataType::Binary,
        "array_length" | "array_position" | "cardinality" | "map_size" => DataType::Int32,
        "grouping" | "grouping_id" => DataType::Int64,
        "split" => DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Utf8,
            true,
        ))),
        "array_min" | "array_max" => match arg_types.first() {
            Some(DataType::List(item)) => item.data_type().clone(),
            _ => DataType::Null,
        },
        "array_sort" | "array_sortby" | "array_reverse" | "array_slice" | "array_remove"
        | "array_filter" | "array_map" | "array_flatten" | "array_concat" => {
            arg_types.first().cloned().unwrap_or(DataType::Null)
        }
        "array_generate" => infer_array_generate_return_type(arg_types),
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
        "map" => infer_map_constructor_return_type(arg_types),
        "row" | "struct" => infer_struct_constructor_return_type(arg_types),
        "named_struct" => infer_named_struct_return_type(arg_types),
        "map_from_arrays" => match (arg_types.first(), arg_types.get(1)) {
            (Some(DataType::List(keys)), Some(DataType::List(values))) => DataType::Map(
                Arc::new(arrow::datatypes::Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(arrow::datatypes::Field::new(
                                "key",
                                keys.data_type().clone(),
                                true,
                            )),
                            Arc::new(arrow::datatypes::Field::new(
                                "value",
                                values.data_type().clone(),
                                true,
                            )),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            _ => DataType::Null,
        },
        "percentile_hash" | "percentile_empty" => DataType::Binary,
        "percentile_approx_raw" => DataType::Float64,
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

        // Default for unknown functions -> Utf8 (permissive)
        _ => DataType::Utf8,
    }
}

fn infer_array_generate_return_type(arg_types: &[DataType]) -> DataType {
    let is_datetime = arg_types.iter().any(|ty| {
        matches!(
            ty,
            DataType::Date32 | DataType::Timestamp(_, _) | DataType::Utf8
        )
    });
    let item_type = if is_datetime {
        arg_types
            .iter()
            .find_map(|ty| match ty {
                DataType::Date32 => Some(DataType::Date32),
                DataType::Timestamp(unit, tz) => Some(DataType::Timestamp(*unit, tz.clone())),
                _ => None,
            })
            .unwrap_or(DataType::Date32)
    } else {
        DataType::Int64
    };
    DataType::List(Arc::new(arrow::datatypes::Field::new(
        "item", item_type, true,
    )))
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

fn null_map_type() -> DataType {
    DataType::Map(
        Arc::new(arrow::datatypes::Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(arrow::datatypes::Field::new("key", DataType::Null, true)),
                    Arc::new(arrow::datatypes::Field::new("value", DataType::Null, true)),
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

// ---------------------------------------------------------------------------
// Aggregate function return type inference
// ---------------------------------------------------------------------------

pub(super) fn infer_agg_return_type(name: &str, arg_types: &[DataType]) -> DataType {
    let first_arg = arg_types.first().cloned().unwrap_or(DataType::Null);
    let float_array = || {
        DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Float64,
            true,
        )))
    };
    let approx_top_k_array = |item_type: DataType| {
        DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Struct(
                vec![
                    Arc::new(arrow::datatypes::Field::new("item", item_type, true)),
                    Arc::new(arrow::datatypes::Field::new("count", DataType::Int64, true)),
                ]
                .into(),
            ),
            true,
        )))
    };
    let array_output = |item_type: DataType| {
        DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item", item_type, true,
        )))
    };
    match name {
        "count"
        | "count_if"
        | "bitmap_union_count"
        | "bitmap_union_int"
        | "approx_count_distinct"
        | "approx_count_distinct_hll_sketch"
        | "ds_hll_count_distinct"
        | "ds_hll_count_distinct_merge"
        | "ndv"
        | "hll_union_agg"
        | "multi_distinct_count" => DataType::Int64,

        "sum" => match &first_arg {
            DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64 => DataType::Int64,
            DataType::Float32 | DataType::Float64 => DataType::Float64,
            DataType::Decimal128(_p, s) => DataType::Decimal128(38, *s),
            _ => DataType::Float64,
        },

        "avg" => match &first_arg {
            DataType::Decimal128(_p, s) => {
                // StarRocks computes avg as sum/count. Division scale rule:
                // s <= 6  => result_scale = s + 6
                // s <= 12 => result_scale = 12
                // else    => result_scale = s
                let new_scale = if *s <= 6 {
                    *s + 6
                } else if *s <= 12 {
                    12
                } else {
                    *s
                };
                DataType::Decimal128(38, new_scale)
            }
            _ => DataType::Float64,
        },
        "min" | "max" | "any_value" => first_arg,
        "group_concat" | "string_agg" => DataType::Utf8,
        "dict_merge" => DataType::Utf8,
        "mann_whitney_u_test" => DataType::Utf8,
        "bitmap_agg"
        | "bitmap_union"
        | "ds_hll_count_distinct_union"
        | "hll_union"
        | "hll_raw_agg" => DataType::Binary,
        "array_agg" | "array_agg_distinct" => array_output(first_arg),
        "array_unique_agg" => first_arg,
        "sum_map" => {
            if first_arg == DataType::Null {
                null_map_type()
            } else {
                first_arg
            }
        }
        "map_agg" => {
            let key_type = arg_types.first().cloned().unwrap_or(DataType::Null);
            let value_type = arg_types.get(1).cloned().unwrap_or(DataType::Null);
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

        "variance" | "variance_samp" | "variance_pop" | "var_samp" | "var_pop" | "stddev"
        | "stddev_samp" | "stddev_pop" | "covar_samp" | "covar_pop" | "corr" => DataType::Float64,
        "bool_or" | "bool_and" | "boolor_agg" | "booland_agg" | "every" => DataType::Boolean,

        "percentile_approx" => {
            if matches!(arg_types.get(1), Some(DataType::List(_))) {
                float_array()
            } else {
                DataType::Float64
            }
        }
        "percentile_approx_weighted" => {
            if matches!(arg_types.get(2), Some(DataType::List(_))) {
                float_array()
            } else {
                DataType::Float64
            }
        }
        "approx_top_k" => approx_top_k_array(first_arg),
        "min_n" | "max_n" => array_output(first_arg),

        // Default: same as first arg
        _ => {
            if arg_types.is_empty() {
                DataType::Int64
            } else {
                first_arg
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn array_type(item_type: DataType) -> DataType {
        DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item", item_type, true,
        )))
    }

    #[test]
    fn infer_scalar_return_type_for_collection_length_functions() {
        let int_array = array_type(DataType::Int32);

        assert_eq!(
            infer_scalar_return_type("array_length", std::slice::from_ref(&int_array)),
            DataType::Int32
        );
        assert_eq!(
            infer_scalar_return_type("cardinality", std::slice::from_ref(&int_array)),
            DataType::Int32
        );
        assert_eq!(
            infer_scalar_return_type("array_position", &[int_array, DataType::Int32]),
            DataType::Int32
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
    fn sum_map_is_treated_as_aggregate_map_output() {
        let map_type = DataType::Map(
            Arc::new(arrow::datatypes::Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(arrow::datatypes::Field::new("key", DataType::Int32, true)),
                        Arc::new(arrow::datatypes::Field::new("value", DataType::Int64, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        assert!(is_aggregate_function("sum_map"));
        assert_eq!(
            infer_agg_return_type("sum_map", std::slice::from_ref(&map_type)),
            map_type
        );
    }

    #[test]
    fn sum_map_rejects_non_scalar_values() {
        let map_type = DataType::Map(
            Arc::new(arrow::datatypes::Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(arrow::datatypes::Field::new("key", DataType::Int32, true)),
                        Arc::new(arrow::datatypes::Field::new(
                            "value",
                            DataType::List(Arc::new(arrow::datatypes::Field::new(
                                "item",
                                DataType::Int32,
                                true,
                            ))),
                            true,
                        )),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );

        let err = validate_aggregate_function_call("sum_map", &[map_type])
            .expect_err("sum_map should reject non-scalar map values");
        assert_eq!(err, "sum_map only support scalar KV");
    }

    #[test]
    fn sum_map_rejects_unsupported_scalar_value_types() {
        let map_type = DataType::Map(
            Arc::new(arrow::datatypes::Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(arrow::datatypes::Field::new("key", DataType::Int32, true)),
                        Arc::new(arrow::datatypes::Field::new(
                            "value",
                            DataType::Date32,
                            true,
                        )),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );

        let err = validate_aggregate_function_call("sum_map", &[map_type])
            .expect_err("sum_map should reject date values");
        assert_eq!(err, "unsupported value type:DATE");
    }
}
