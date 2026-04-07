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
            | "bitmap_union_count"
            | "bitmap_union_int"
            | "multi_distinct_count"
            | "array_agg"
            | "array_unique_agg"
            | "percentile_approx"
            | "percentile_approx_weighted"
            | "percentile_cont"
            | "percentile_disc"
            | "percentile_disc_lc"
            | "percentile_union"
            | "approx_count_distinct"
            | "approx_top_k"
            | "hll_union_agg"
            | "hll_raw_agg"
            | "hll_cardinality"
            | "ndv"
            | "variance"
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
        "upper" | "lower" | "trim" | "ltrim" | "rtrim" | "reverse" | "replace" | "lpad"
        | "rpad" | "concat" | "concat_ws" | "substr" | "substring" | "left" | "right"
        | "repeat" | "space" | "hex" | "unhex" | "md5" | "sha2" | "to_base64" | "from_base64"
        | "url_encode" | "url_decode" | "translate" | "initcap" => DataType::Utf8,

        // Math functions that return the same type as input
        "abs" => arg_types.first().cloned().unwrap_or(DataType::Float64),

        // Math functions that return Int64
        "ceil" | "ceiling" | "floor" => DataType::Int64,

        // round/truncate: Decimal input → Decimal128(38, scale); otherwise Float64
        // When the second argument is Int (constant decimal places), clamp to
        // a reasonable output scale to avoid excess trailing zeros.
        "round" | "truncate" => match arg_types.first() {
            Some(DataType::Decimal128(_, s)) => {
                // If second arg is an integer type, the value is the target
                // decimal places.  We can't see the value here (only the type),
                // so we keep the original scale.  The execution ROUND already
                // handles the conversion.  To match StarRocks display, we'll
                // rely on the execution layer's output scale adjustment.
                DataType::Decimal128(38, *s)
            }
            _ => DataType::Float64,
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
        "date_format" | "from_unixtime" => DataType::Utf8,
        "date_add" | "date_sub" | "adddate" | "subdate" | "days_add" | "days_sub" | "weeks_add"
        | "weeks_sub" | "months_add" | "months_sub" | "years_add" | "years_sub" => {
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
        "year" | "month" | "day" | "dayofmonth" | "hour" | "minute" | "second" | "dayofweek"
        | "dayofyear" | "weekofyear" | "quarter" => DataType::Int32,
        "unix_timestamp" | "to_unix_timestamp" | "datediff" | "timestampdiff" => DataType::Int64,
        "to_date" | "str_to_date" => DataType::Date32,

        // Misc
        "version" | "database" | "current_user" | "user" | "uuid" => DataType::Utf8,
        "sleep" => DataType::Boolean,
        "murmur_hash3_32" => DataType::Int32,

        // Default for unknown functions -> Utf8 (permissive)
        _ => DataType::Utf8,
    }
}

// ---------------------------------------------------------------------------
// Aggregate function return type inference
// ---------------------------------------------------------------------------

pub(super) fn infer_agg_return_type(name: &str, arg_types: &[DataType]) -> DataType {
    let first_arg = arg_types.first().cloned().unwrap_or(DataType::Null);
    match name {
        "count"
        | "count_if"
        | "bitmap_union_count"
        | "bitmap_union_int"
        | "approx_count_distinct"
        | "ndv"
        | "hll_union_agg"
        | "multi_distinct_count" => DataType::Int64,

        "sum" => match &first_arg {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => DataType::Int64,
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
        "array_agg" => {
            let elem = first_arg;
            DataType::List(Arc::new(arrow::datatypes::Field::new("item", elem, true)))
        }

        "variance" | "var_samp" | "var_pop" | "stddev" | "stddev_samp" | "stddev_pop"
        | "covar_samp" | "covar_pop" | "corr" => DataType::Float64,

        "percentile_approx" => DataType::Float64,

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
