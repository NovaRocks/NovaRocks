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

//! Native parser AST -> NovaRocks `Literal` conversion, plus literal
//! utilities (compare, cast, arithmetic, encoding, and keying) used across
//! SQL planning and standalone execution.
//!
//! All items here are pure functions with no standalone-runtime state. They
//! translate between typed SQL expressions and NovaRocks types.

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, TimeUnit};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};

use crate::semantic::Literal;
use novarocks_parser::{ast, printer};
use novarocks_types::schema::{ColumnDefault, SqlType, validate_column_default};

#[path = "literal_failure.rs"]
mod failure;
pub use failure::LiteralFailure;

/// Tags accepted by the original typed-string parser and its value consumers.
/// GenericLiteral retains raw tag text, so quoted tags are not guessed aliases.
pub(crate) fn is_supported_generic_literal(value: &ast::GenericLiteral) -> bool {
    matches!(
        value.type_name.to_ascii_lowercase().as_str(),
        "date" | "datetime" | "timestamp" | "time" | "json"
    )
}

fn native_literal_to_literal(literal: &ast::Literal) -> Result<Literal, LiteralFailure> {
    match &literal.kind {
        ast::LiteralKind::Null => Ok(Literal::Null),
        ast::LiteralKind::Boolean(value) => Ok(Literal::Bool(*value)),
        ast::LiteralKind::Long { parsed_value, .. } => Ok(Literal::Int(*parsed_value)),
        ast::LiteralKind::Decimal(value) => Ok(sql_number_literal(value)),
        ast::LiteralKind::Double(value) => Ok(Literal::Float(value.get())),
        ast::LiteralKind::String(value) => Ok(Literal::String(value.clone())),
        ast::LiteralKind::Binary(value) => Ok(Literal::String(bytes_to_latin1_string(value))),
        ast::LiteralKind::Generic(value) => {
            if !is_supported_generic_literal(value) {
                return Err(LiteralFailure::rejected(format_args!(
                    "generic literal type {} is not supported",
                    value.type_name
                )));
            }
            // The original typed-string INSERT fast path delegates its decoded
            // value to the sink's existing declaration conversion.
            Ok(Literal::String(value.value.clone()))
        }
        ast::LiteralKind::Interval(_) => Err(LiteralFailure::rejected(
            "INTERVAL literals are not supported",
        )),
    }
}

fn object_name_lower(name: &ast::ObjectName) -> Result<String, LiteralFailure> {
    name.parts
        .last()
        .map(|part| part.value.to_ascii_lowercase())
        .ok_or_else(|| LiteralFailure::rejected("function name cannot be empty"))
}

pub fn bytes_to_latin1_string(bytes: &[u8]) -> String {
    bytes.iter().map(|b| char::from(*b)).collect()
}

pub fn latin1_string_to_bytes(value: &str) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(value.len());
    for ch in value.chars() {
        if (ch as u32) > 0xff {
            return Err(format!("literal contains non-LATIN1 character: {value:?}"));
        }
        out.push(ch as u8);
    }
    Ok(out)
}

pub fn parse_date_string_to_days(s: &str) -> Result<i32, String> {
    use chrono::NaiveDate;
    let date = NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d")
        .map_err(|e| format!("invalid date literal `{s}`: {e}"))?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    Ok((date - epoch).num_days() as i32)
}

pub fn parse_datetime_string_to_micros(s: &str) -> Result<i64, String> {
    use chrono::NaiveDateTime;
    let s = s.trim();
    // Try datetime first, then date-only
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Ok(dt.and_utc().timestamp_micros());
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
        return Ok(dt.and_utc().timestamp_micros());
    }
    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let dt = d.and_hms_opt(0, 0, 0).expect("midnight");
        return Ok(dt.and_utc().timestamp_micros());
    }
    Err(format!("invalid datetime literal `{s}`"))
}

/// Parse a `YYYY-MM-DD HH:MM:SS[.fffffffff]` literal into nanoseconds since the
/// Unix epoch. Mirrors `parse_datetime_string_to_micros` but keeps nanosecond
/// precision for Iceberg v3 `timestamp_ns` columns. Errors if the value is
/// outside the nanosecond-representable range (~1677-09-21 .. 2262-04-11).
#[allow(
    dead_code,
    reason = "Retained for the staged Iceberg v3 timestamp-nanosecond write path."
)]
pub(crate) fn parse_datetime_string_to_nanos(s: &str) -> Result<i64, String> {
    use chrono::NaiveDateTime;
    let s = s.trim();
    // Try datetime first, then date-only
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return dt.and_utc().timestamp_nanos_opt().ok_or_else(|| {
            format!("DATETIME literal '{s}' out of nanosecond representable range")
        });
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
        return dt.and_utc().timestamp_nanos_opt().ok_or_else(|| {
            format!("DATETIME literal '{s}' out of nanosecond representable range")
        });
    }
    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let dt = d.and_hms_opt(0, 0, 0).expect("midnight");
        return dt.and_utc().timestamp_nanos_opt().ok_or_else(|| {
            format!("DATETIME literal '{s}' out of nanosecond representable range")
        });
    }
    Err(format!("invalid datetime literal `{s}`"))
}

/// Convert a native expression to a Literal for INSERT VALUES.
pub fn expr_to_literal(expr: &ast::Expr) -> Result<Literal, LiteralFailure> {
    match expr {
        ast::Expr::Literal(literal) => native_literal_to_literal(literal),
        ast::Expr::Unary(unary) if unary.operator == ast::UnaryOperator::Minus => {
            negate_literal(expr_to_literal(&unary.expression)?)
        }
        ast::Expr::Nested(nested) => expr_to_literal(&nested.expression),
        // Handle CAST(expr AS type): peel the CAST and evaluate the inner literal,
        // EXCEPT for DECIMAL targets. CAST to a DECIMAL type carries an explicit
        // (precision, scale) that the literal fast-path ignores — it always writes
        // the raw literal value against the *sink* column's scale, which may be
        // narrower and would produce a false "too many fractional digits" error.
        // Returning Err here causes select_projection_requires_pipeline to route
        // the INSERT through the full query pipeline instead, where the CAST is
        // evaluated with its declared type and the narrowing to the sink's
        // DECIMAL(p,s) is handled at write time (with rounding).
        ast::Expr::Cast(cast) => {
            if cast_type_is_decimal(&cast.data_type) {
                Err(LiteralFailure::rejected(format_args!(
                    "CAST to DECIMAL in INSERT SELECT requires pipeline evaluation: {}",
                    printer::print_expr(expr)
                )))
            } else {
                expr_to_literal(&cast.expression)
            }
        }
        // In MySQL mode, "value" is parsed as an identifier — treat as string literal
        ast::Expr::Identifier(ident) => Ok(Literal::String(ident.value.clone())),
        // Handle binary operations like 10000 - 1
        ast::Expr::Binary(binary) => {
            let l = expr_to_literal(&binary.left)?;
            let r = expr_to_literal(&binary.right)?;
            match (l, binary.operator, r) {
                (Literal::Int(a), ast::BinaryOperator::Add, Literal::Int(b)) => a
                    .checked_add(b)
                    .map(Literal::Int)
                    .ok_or_else(|| LiteralFailure::numeric("integer literal arithmetic overflow")),
                (Literal::Int(a), ast::BinaryOperator::Subtract, Literal::Int(b)) => a
                    .checked_sub(b)
                    .map(Literal::Int)
                    .ok_or_else(|| LiteralFailure::numeric("integer literal arithmetic overflow")),
                (Literal::Int(a), ast::BinaryOperator::Multiply, Literal::Int(b)) => a
                    .checked_mul(b)
                    .map(Literal::Int)
                    .ok_or_else(|| LiteralFailure::numeric("integer literal arithmetic overflow")),
                (Literal::Float(a), ast::BinaryOperator::Add, Literal::Float(b)) => {
                    Ok(Literal::Float(a + b))
                }
                (Literal::Float(a), ast::BinaryOperator::Subtract, Literal::Float(b)) => {
                    Ok(Literal::Float(a - b))
                }
                _ => Err(LiteralFailure::rejected(format_args!(
                    "unsupported expression in INSERT VALUES: {}",
                    printer::print_expr(expr)
                ))),
            }
        }
        // Handle array literal [1, 2, 3]
        ast::Expr::Array(array) => Ok(Literal::Array(
            array
                .values
                .iter()
                .map(expr_to_literal)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        ast::Expr::FunctionCall(func) => function_to_literal(func),
        ast::Expr::Row(struct_expr) => Ok(Literal::Struct(
            struct_expr
                .fields
                .iter()
                .map(|field| expr_to_literal(&field.expression))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        _ => Err(LiteralFailure::rejected(format_args!(
            "unsupported expression in INSERT VALUES: {}",
            printer::print_expr(expr)
        ))),
    }
}

/// Returns true if the given native TypeName is a DECIMAL variant (including
/// StarRocks-style DECIMAL32/DECIMAL64/DECIMAL128 names). Used to decide
/// whether a CAST-to-DECIMAL expression should be routed through the full query
/// pipeline rather than being folded into a bare literal.
fn cast_type_is_decimal(data_type: &ast::TypeName) -> bool {
    matches!(&data_type.kind, ast::TypeKind::Generic(value) if matches!(value.name.value.to_ascii_lowercase().as_str(), "decimal" | "dec" | "numeric" | "decimal32" | "decimal64" | "decimal128"))
}

fn type_name_to_sql_type(data_type: &ast::TypeName) -> Result<SqlType, LiteralFailure> {
    let generic = match &data_type.kind {
        ast::TypeKind::Generic(generic) => generic,
        ast::TypeKind::Row(row) => {
            return row
                .fields
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    Ok((
                        field
                            .name
                            .as_ref()
                            .map(|name| name.value.clone())
                            .unwrap_or_else(|| format!("f{}", index + 1)),
                        type_name_to_sql_type(&field.data_type)?,
                    ))
                })
                .collect::<Result<Vec<_>, LiteralFailure>>()
                .map(SqlType::Struct);
        }
        ast::TypeKind::DateTime(value) => {
            if value.with_time_zone || value.precision.as_deref().is_some_and(|parameter| !matches!(parameter, ast::DataTypeParameter::Numeric(number) if number.parsed_value == 6)) {
                return Err(LiteralFailure::rejected("TIME/TIMESTAMP only support microsecond precision without time zone"));
            }
            return Ok(match value.kind {
                ast::DateTimeType::Time => SqlType::Time,
                ast::DateTimeType::Timestamp => SqlType::DateTime,
            });
        }
        ast::TypeKind::Interval(_) => {
            return Err(LiteralFailure::rejected(
                "INTERVAL data types are not supported",
            ));
        }
        ast::TypeKind::ExplicitArray(_) => {
            return Err(LiteralFailure::rejected(
                "explicit ARRAY lengths are not supported",
            ));
        }
    };
    let name = generic.name.value.to_ascii_lowercase();
    match name.as_str() {
        "tinyint" => Ok(SqlType::TinyInt),
        "smallint" => Ok(SqlType::SmallInt),
        "int" | "integer" => Ok(SqlType::Int),
        "bigint" => Ok(SqlType::BigInt),
        "largeint" => Ok(SqlType::LargeInt),
        "float" | "real" => Ok(SqlType::Float),
        "double" | "double precision" => Ok(SqlType::Double),
        "boolean" | "bool" => Ok(SqlType::Boolean),
        "varchar" | "char" | "character" | "string" | "text" => Ok(SqlType::String),
        "json" | "jsonb" => Ok(SqlType::Json),
        "varbinary" | "binary" => Ok(SqlType::Binary),
        "bitmap" => Ok(SqlType::Bitmap),
        "hll" => Ok(SqlType::Hll),
        "date" => Ok(SqlType::Date),
        "datetime" | "timestamp" | "timestamptz" => Ok(SqlType::DateTime),
        "datetime_ns" | "timestamp_ns" | "timestamptz_ns" => Ok(SqlType::DateTimeNs),
        "time" => Ok(SqlType::Time),
        "variant" => Ok(SqlType::Variant),
        "decimal" | "dec" | "numeric" | "decimal32" | "decimal64" | "decimal128" => {
            let precision = type_numeric_argument(&generic.arguments, 0)?.unwrap_or(38);
            let scale = type_numeric_argument(&generic.arguments, 1)?.unwrap_or(0);
            let precision = u8::try_from(precision).map_err(|_| {
                LiteralFailure::rejected(
                    "literal DECIMAL precision exceeds its supported parameter domain",
                )
            })?;
            let scale = i8::try_from(scale).map_err(|_| {
                LiteralFailure::rejected(
                    "literal DECIMAL scale exceeds its supported parameter domain",
                )
            })?;
            arrow::array::types::validate_decimal_precision_and_scale::<
                arrow::array::types::Decimal128Type,
            >(precision, scale)
            .map_err(LiteralFailure::decimal_parameters)?;
            Ok(SqlType::Decimal { precision, scale })
        }
        "array" => Ok(SqlType::Array(Box::new(type_type_argument(
            &generic.arguments,
            0,
            "ARRAY",
        )?))),
        "map" => Ok(SqlType::Map(
            Box::new(type_type_argument(&generic.arguments, 0, "MAP")?),
            Box::new(type_type_argument(&generic.arguments, 1, "MAP")?),
        )),
        _ => Err(LiteralFailure::rejected(format_args!(
            "unsupported SQL type: {name}"
        ))),
    }
}

fn type_numeric_argument(
    arguments: &[ast::DataTypeParameter],
    index: usize,
) -> Result<Option<u64>, LiteralFailure> {
    let Some(argument) = arguments.get(index) else {
        return Ok(None);
    };
    let ast::DataTypeParameter::Numeric(value) = argument else {
        return Err(LiteralFailure::rejected(
            "numeric type parameter must be an integer",
        ));
    };
    u64::try_from(value.parsed_value)
        .map(Some)
        .map_err(|_| LiteralFailure::rejected("numeric type parameter must be nonnegative"))
}

fn type_type_argument(
    arguments: &[ast::DataTypeParameter],
    index: usize,
    kind: &str,
) -> Result<SqlType, LiteralFailure> {
    let Some(ast::DataTypeParameter::Type(value)) = arguments.get(index) else {
        return Err(LiteralFailure::rejected(format_args!(
            "{kind} type requires a type parameter"
        )));
    };
    type_name_to_sql_type(&value.data_type)
}

pub(crate) fn sql_number_literal(input: &str) -> Literal {
    if is_integral_sql_number(input) {
        input
            .parse::<i64>()
            .map(Literal::Int)
            .unwrap_or_else(|_| Literal::String(input.to_string()))
    } else {
        input
            .parse::<f64>()
            .map(Literal::Float)
            .unwrap_or_else(|_| Literal::String(input.to_string()))
    }
}

pub(crate) fn is_integral_sql_number(input: &str) -> bool {
    !input.contains(['.', 'e', 'E'])
}

pub(crate) fn negate_literal(literal: Literal) -> Result<Literal, LiteralFailure> {
    match literal {
        Literal::Int(i) => i
            .checked_neg()
            .map(Literal::Int)
            .ok_or_else(|| LiteralFailure::numeric("integer literal negation overflow")),
        Literal::Float(f) => Ok(Literal::Float(-f)),
        Literal::String(s) if is_integral_sql_number(s.trim()) => {
            Ok(Literal::String(format!("-{}", s.trim())))
        }
        other => Err(LiteralFailure::rejected(format_args!(
            "cannot negate {other:?}"
        ))),
    }
}

fn literal_to_json_key(literal: Literal) -> Result<Option<String>, LiteralFailure> {
    Ok(match literal {
        Literal::Null => None,
        Literal::Bool(v) => Some(if v { "true" } else { "false" }.to_string()),
        Literal::Int(v) => Some(v.to_string()),
        Literal::Float(v) => Some(v.to_string()),
        Literal::String(v) | Literal::Date(v) => Some(v),
        Literal::Array(_) | Literal::Map(_) | Literal::Struct(_) => {
            return Err(LiteralFailure::rejected(
                "json_object key does not support complex type",
            ));
        }
    })
}

fn literal_to_json_value(literal: Literal) -> Result<JsonValue, LiteralFailure> {
    Ok(match literal {
        Literal::Null => JsonValue::Null,
        Literal::Bool(v) => JsonValue::Bool(v),
        Literal::Int(v) => JsonValue::Number(JsonNumber::from(v)),
        Literal::Float(v) => JsonNumber::from_f64(v)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Literal::String(v) | Literal::Date(v) => {
            serde_json::from_str::<JsonValue>(&v).unwrap_or(JsonValue::String(v))
        }
        Literal::Array(items) => JsonValue::Array(
            items
                .into_iter()
                .map(literal_to_json_value)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Literal::Map(entries) => {
            let mut map = JsonMap::new();
            for (key, value) in entries {
                if let Some(key) = literal_to_json_key(key)? {
                    map.insert(key, literal_to_json_value(value)?);
                } else {
                    return Ok(JsonValue::Null);
                }
            }
            JsonValue::Object(map)
        }
        Literal::Struct(fields) => JsonValue::Array(
            fields
                .into_iter()
                .map(literal_to_json_value)
                .collect::<Result<Vec<_>, _>>()?,
        ),
    })
}

fn json_object_literal(args: &[&ast::Expr]) -> Result<Literal, LiteralFailure> {
    let mut object = JsonMap::new();
    let mut idx = 0usize;
    while idx < args.len() {
        let key = expr_to_literal(args[idx])?;
        let Some(key) = literal_to_json_key(key)? else {
            return Ok(Literal::Null);
        };
        let value = if let Some(value_expr) = args.get(idx + 1) {
            literal_to_json_value(expr_to_literal(value_expr)?)?
        } else {
            JsonValue::Null
        };
        object.insert(key, value);
        idx += 2;
    }
    let json_text =
        serde_json::to_string(&JsonValue::Object(object)).map_err(LiteralFailure::json)?;
    let bytes =
        novarocks_types::value::variant_encode::encode_json_text_to_variant_bytes(&json_text)
            .map_err(|source| LiteralFailure::variant("json_object failed", source))?;
    Ok(Literal::String(bytes_to_latin1_string(&bytes)))
}

pub(crate) fn function_to_literal(func: &ast::FunctionCall) -> Result<Literal, LiteralFailure> {
    if func.processing_mode.is_some() {
        return Err(LiteralFailure::rejected(
            "function processing modes are not supported in literals",
        ));
    }
    let args = function_expr_args(&func.arguments)?;
    let name = object_name_lower(&func.name)?;
    if let Some(value) = try_array_map_cast_string_literal(&name, &args)? {
        return Ok(value);
    }
    match name.as_str() {
        "array_generate" => {
            let values = args
                .iter()
                .map(|arg| expr_to_literal(arg))
                .collect::<Result<Vec<_>, _>>()?;
            eval_array_generate_literal(&values)
        }
        "array_repeat" => {
            if args.len() != 2 {
                return Err(LiteralFailure::rejected("array_repeat expects 2 arguments"));
            }
            let value = expr_to_literal(args[0])?;
            let repeat = match expr_to_literal(args[1])? {
                Literal::Int(v) => v,
                other => {
                    return Err(LiteralFailure::rejected(format_args!(
                        "array_repeat expects integer count, got {other:?}"
                    )));
                }
            };
            if repeat <= 0 {
                return Ok(Literal::Array(Vec::new()));
            }
            let required = repeat as u128 * std::mem::size_of::<Literal>() as u128;
            let repeat = usize::try_from(repeat)
                .map_err(|source| LiteralFailure::address(required, source))?;
            std::alloc::Layout::array::<Literal>(repeat)
                .map_err(|source| LiteralFailure::layout(required, source))?;
            let mut values = Vec::new();
            values
                .try_reserve_exact(repeat)
                .map_err(|source| LiteralFailure::allocation(required, source))?;
            values.resize(repeat, value);
            Ok(Literal::Array(values))
        }
        "json_object" => json_object_literal(&args),
        "array_append" => {
            if args.len() != 2 {
                return Err(LiteralFailure::rejected("array_append expects 2 arguments"));
            }
            let array = expr_to_literal(args[0])?;
            let value = expr_to_literal(args[1])?;
            match array {
                Literal::Null => Ok(Literal::Null),
                Literal::Array(mut values) => {
                    values.push(value);
                    Ok(Literal::Array(values))
                }
                other => Err(LiteralFailure::rejected(format_args!(
                    "array_append expects ARRAY argument, got {other:?}"
                ))),
            }
        }
        "bitmap_empty" => {
            if !args.is_empty() {
                return Err(LiteralFailure::rejected("bitmap_empty expects 0 arguments"));
            }
            // SeriV2 empty bitmap encoding: a single BITMAP_TYPE_EMPTY (=0) byte,
            // matching `eval_bitmap_empty` runtime output.
            Ok(Literal::String(bytes_to_latin1_string(&[
                novarocks_types::value::bitmap::BITMAP_TYPE_EMPTY,
            ])))
        }
        "hll_hash" => {
            if args.len() != 1 {
                return Err(LiteralFailure::rejected("hll_hash expects 1 argument"));
            }
            // Reject explicit narrowing CAST since this const-fold path always
            // hashes Int64 little-endian bytes, while the runtime path hashes
            // the cast's native (narrower) width. Allowing the unwrap would
            // produce values that disagree with `eval_hll_hash` at runtime.
            if let ast::Expr::Cast(cast) = args[0] {
                let narrowing = match &cast.data_type.kind {
                    ast::TypeKind::Generic(generic) => Some(&generic.name),
                    _ => None,
                }
                .is_some_and(|part| {
                    matches!(
                        part.value.to_ascii_lowercase().as_str(),
                        "tinyint"
                            | "smallint"
                            | "int"
                            | "integer"
                            | "int2"
                            | "int4"
                            | "mediumint"
                            | "float"
                    )
                });
                if narrowing {
                    return Err(LiteralFailure::rejected(
                        "hll_hash with narrowing CAST argument is not supported in INSERT VALUES; \
                         wrap the value directly without CAST",
                    ));
                }
            }
            use novarocks_functions::hll_hash_core::encode_scalar_bytes;
            let arg = expr_to_literal(args[0])?;
            // Preserve this INSERT author's original byte projection. SELECT
            // analyzer coercion is a distinct source contract, not a default here.
            let bytes = match arg {
                Literal::Null => encode_scalar_bytes(None),
                Literal::Int(v) => encode_scalar_bytes(Some(&v.to_le_bytes())),
                Literal::Float(v) => encode_scalar_bytes(Some(&v.to_le_bytes())),
                Literal::String(s) => encode_scalar_bytes(Some(s.as_bytes())),
                Literal::Bool(b) => encode_scalar_bytes(Some(&[if b { 1u8 } else { 0u8 }])),
                other => {
                    return Err(LiteralFailure::rejected(format_args!(
                        "hll_hash unsupported literal: {other:?}"
                    )));
                }
            };
            Ok(Literal::String(bytes_to_latin1_string(&bytes)))
        }
        "to_binary" => {
            if args.len() != 1 && args.len() != 2 {
                return Err(LiteralFailure::rejected(
                    "to_binary expects 1 or 2 arguments",
                ));
            }

            let Literal::String(input) = expr_to_literal(args[0])? else {
                return Err(LiteralFailure::rejected(
                    "to_binary expects VARCHAR as first argument",
                ));
            };

            let format = if args.len() == 2 {
                let Literal::String(format) = expr_to_literal(args[1])? else {
                    return Err(LiteralFailure::rejected(
                        "to_binary expects VARCHAR format argument",
                    ));
                };
                format
            } else {
                "hex".to_string()
            };

            let bytes = match format.to_ascii_lowercase().as_str() {
                "encode64" => {
                    if input.is_empty() {
                        return Ok(Literal::Null);
                    }
                    use base64::Engine;
                    base64::engine::general_purpose::STANDARD
                        .decode(input.as_bytes())
                        .map_err(LiteralFailure::base64)?
                }
                "utf8" => input.into_bytes(),
                _ => hex::decode(input)
                    .map_err(|source| LiteralFailure::hex("to_binary hex decode failed", source))?,
            };

            Ok(Literal::String(
                bytes.iter().map(|b| char::from(*b)).collect(),
            ))
        }
        "bitmap_from_string" => {
            if args.len() != 1 {
                return Err(LiteralFailure::rejected(
                    "bitmap_from_string expects 1 argument",
                ));
            }
            let arg = expr_to_literal(args[0])?;
            let text = match arg {
                Literal::Null => return Ok(Literal::Null),
                Literal::String(s) => s,
                other => {
                    return Err(LiteralFailure::rejected(format_args!(
                        "bitmap_from_string expects VARCHAR argument, got {other:?}"
                    )));
                }
            };
            // Mirror runtime semantics: malformed string -> NULL (not error).
            let values = match novarocks_types::value::bitmap::parse_bitmap_string(&text) {
                Ok(v) => v,
                Err(source) if source.is_data() => return Ok(Literal::Null),
                Err(source) => return Err(source.into()),
            };
            // Use the EXTERNAL (storage / SeriV1-style) encoding here —
            // that's the format the StarRocks table bitmap column
            // reader expects, matching `bitmap_empty` / `to_bitmap`'s
            // const-fold output. The internal varint format only round-
            // trips through the runtime expression layer.
            let bytes = novarocks_types::value::bitmap::encode_external_bitmap(&values)?;
            Ok(Literal::String(bytes_to_latin1_string(&bytes)))
        }
        "to_bitmap" => {
            if args.len() != 1 {
                return Err(LiteralFailure::rejected("to_bitmap expects 1 argument"));
            }
            use novarocks_types::value::bitmap::encode_bitmap_single;
            let arg = expr_to_literal(args[0])?;
            // Mirror `eval_to_bitmap` runtime semantics for scalar literals:
            //   - NULL or negative integer → NULL
            //   - Int  → encode as u64 (Int64 runtime arm uses i128::from then casts)
            //   - Bool → 1 or 0
            //   - String → parse as unsigned decimal; non-numeric → NULL
            let value: u64 = match arg {
                Literal::Null => return Ok(Literal::Null),
                Literal::Int(v) if v >= 0 => v as u64,
                Literal::Int(_) => return Ok(Literal::Null),
                Literal::Bool(b) => {
                    if b {
                        1
                    } else {
                        0
                    }
                }
                Literal::String(s) => match s.trim().parse::<u64>() {
                    Ok(v) => v,
                    Err(_) => return Ok(Literal::Null),
                },
                other => {
                    return Err(LiteralFailure::rejected(format_args!(
                        "to_bitmap unsupported literal: {other:?}"
                    )));
                }
            };
            let bytes = encode_bitmap_single(value);
            Ok(Literal::String(bytes_to_latin1_string(&bytes)))
        }
        "md5sum" => {
            use md5::Digest;
            let mut hasher = md5::Md5::new();
            for arg in args {
                let literal = expr_to_literal(arg)?;
                let Some(bytes) = literal_to_varchar_bytes(&literal)? else {
                    continue;
                };
                hasher.update(bytes);
            }
            Ok(Literal::String(hex::encode(hasher.finalize())))
        }
        "parse_json" => {
            if args.len() != 1 {
                return Err(LiteralFailure::rejected("parse_json expects 1 argument"));
            }
            let Literal::String(json_text) = expr_to_literal(args[0])? else {
                return Err(LiteralFailure::rejected(
                    "parse_json expects VARCHAR argument",
                ));
            };
            let bytes = novarocks_types::value::variant_encode::encode_json_text_to_variant_bytes(
                &json_text,
            )
            .map_err(|source| LiteralFailure::variant("parse_json failed", source))?;
            // Pack raw variant bytes into Literal::String via Latin-1 (matches
            // `to_binary` convention; INSERT VALUES decodes via
            // `latin1_string_to_bytes`).
            Ok(Literal::String(bytes_to_latin1_string(&bytes)))
        }
        "row" => Ok(Literal::Struct(
            args.into_iter()
                .map(expr_to_literal)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        "named_struct" => {
            if args.len() % 2 != 0 {
                return Err(LiteralFailure::rejected(format_args!(
                    "named_struct literal requires an even number of arguments, got {}",
                    args.len()
                )));
            }
            Ok(Literal::Struct(
                args.into_iter()
                    .skip(1)
                    .step_by(2)
                    .map(expr_to_literal)
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
        "map" => {
            if args.len() % 2 != 0 {
                return Err(LiteralFailure::rejected(format_args!(
                    "MAP literal requires an even number of arguments, got {}",
                    args.len()
                )));
            }
            let mut entries = Vec::with_capacity(args.len() / 2);
            for pair in args.as_chunks::<2>().0 {
                entries.push((expr_to_literal(pair[0])?, expr_to_literal(pair[1])?));
            }
            Ok(Literal::Map(entries))
        }
        _ => Err(LiteralFailure::rejected(format_args!(
            "unsupported expression in INSERT VALUES: {}",
            printer::print_expr(&ast::Expr::FunctionCall(ast::AstBox::new(func.clone())))
        ))),
    }
}

fn literal_to_varchar_bytes(value: &Literal) -> Result<Option<Vec<u8>>, LiteralFailure> {
    match value {
        Literal::Null => Ok(None),
        Literal::Bool(v) => Ok(Some(if *v { b"1".to_vec() } else { b"0".to_vec() })),
        Literal::Int(v) => Ok(Some(v.to_string().into_bytes())),
        Literal::Float(v) => Ok(Some(v.to_string().into_bytes())),
        Literal::String(v) | Literal::Date(v) => Ok(Some(v.as_bytes().to_vec())),
        Literal::Array(_) | Literal::Map(_) | Literal::Struct(_) => Err(LiteralFailure::rejected(
            "md5sum literal folding does not support complex arguments",
        )),
    }
}

fn try_array_map_cast_string_literal(
    name: &str,
    args: &[&ast::Expr],
) -> Result<Option<Literal>, LiteralFailure> {
    if name != "array_map" && name != "transform" {
        return Ok(None);
    }
    if !array_map_cast_string_lambda_matches(args)? {
        return Ok(None);
    }
    let array_value = expr_to_literal(args[1])?;
    match array_value {
        Literal::Null => Ok(Some(Literal::Null)),
        Literal::Array(values) => values
            .into_iter()
            .map(|value| cast_literal(value, &SqlType::String))
            .collect::<Result<Vec<_>, _>>()
            .map(Literal::Array)
            .map(Some),
        other => Err(LiteralFailure::rejected(format_args!(
            "array_map expects ARRAY input, got {other:?}"
        ))),
    }
}

fn array_map_cast_string_lambda_matches(args: &[&ast::Expr]) -> Result<bool, LiteralFailure> {
    if args.len() != 2 {
        return Ok(false);
    }
    let Some((param_name, body)) = parse_single_arrow_lambda(args[0]) else {
        return Ok(false);
    };
    lambda_body_casts_param_to_string(body, &param_name)
}

fn parse_single_arrow_lambda(expr: &ast::Expr) -> Option<(String, &ast::Expr)> {
    match expr {
        ast::Expr::Lambda(lambda) => lambda
            .arguments
            .first()
            .map(|argument| (argument.name.value.to_lowercase(), &lambda.body)),
        ast::Expr::Nested(nested) => parse_single_arrow_lambda(&nested.expression),
        _ => None,
    }
}

fn lambda_body_casts_param_to_string(
    expr: &ast::Expr,
    param_name: &str,
) -> Result<bool, LiteralFailure> {
    match expr {
        ast::Expr::Nested(nested) => {
            lambda_body_casts_param_to_string(&nested.expression, param_name)
        }
        ast::Expr::Cast(cast) if lambda_expr_is_param(&cast.expression, param_name) => {
            let sql_type = type_name_to_sql_type(&cast.data_type)?;
            Ok(matches!(sql_type, SqlType::String))
        }
        _ => Ok(false),
    }
}

fn lambda_expr_is_param(expr: &ast::Expr, param_name: &str) -> bool {
    match expr {
        ast::Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case(param_name),
        ast::Expr::Nested(nested) => lambda_expr_is_param(&nested.expression, param_name),
        _ => false,
    }
}

pub(crate) fn function_expr_args(
    args: &[ast::CallArgument],
) -> Result<Vec<&ast::Expr>, LiteralFailure> {
    if args.iter().any(|argument| argument.name.is_some()) {
        return Err(LiteralFailure::rejected(
            "named function arguments are not supported in literals",
        ));
    }
    Ok(args.iter().map(|argument| &argument.value).collect())
}

/// Cast a `Literal` to the given SQL type without `ManualValue`.
pub(crate) fn cast_literal(
    value: Literal,
    data_type: &novarocks_types::schema::SqlType,
) -> Result<Literal, LiteralFailure> {
    use novarocks_types::schema::SqlType;
    match data_type {
        SqlType::String | SqlType::Json => match &value {
            Literal::Null => Ok(Literal::Null),
            Literal::Bool(v) => Ok(Literal::String(if *v {
                "1".to_string()
            } else {
                "0".to_string()
            })),
            Literal::Int(v) => Ok(Literal::String(v.to_string())),
            Literal::Float(v) => Ok(Literal::String(v.to_string())),
            Literal::String(_) | Literal::Date(_) => Ok(value),
            Literal::Array(_) | Literal::Map(_) | Literal::Struct(_) => Err(
                LiteralFailure::rejected("cannot cast complex literal to string"),
            ),
        },
        SqlType::Binary | SqlType::Bitmap | SqlType::Hll => match &value {
            Literal::Null => Ok(Literal::Null),
            Literal::Bool(v) => Ok(Literal::String(if *v {
                "1".to_string()
            } else {
                "0".to_string()
            })),
            Literal::Int(v) => Ok(Literal::String(v.to_string())),
            Literal::Float(v) => Ok(Literal::String(v.to_string())),
            Literal::String(_) | Literal::Date(_) => Ok(value),
            Literal::Array(_) | Literal::Map(_) | Literal::Struct(_) => Err(
                LiteralFailure::rejected("cannot cast complex literal to binary"),
            ),
        },
        SqlType::Int | SqlType::BigInt | SqlType::TinyInt | SqlType::SmallInt => match &value {
            Literal::Null => Ok(Literal::Null),
            Literal::Int(_) => Ok(value),
            Literal::Float(v) => Ok(Literal::Int(*v as i64)),
            other => Err(LiteralFailure::rejected(format_args!(
                "cannot cast {:?} to integer",
                other
            ))),
        },
        SqlType::Float | SqlType::Double => match &value {
            Literal::Null => Ok(Literal::Null),
            Literal::Int(v) => Ok(Literal::Float(*v as f64)),
            Literal::Float(_) => Ok(value),
            other => Err(LiteralFailure::rejected(format_args!(
                "cannot cast {:?} to floating point",
                other
            ))),
        },
        SqlType::Array(inner) => match value {
            Literal::Null => Ok(Literal::Null),
            Literal::Array(values) => values
                .into_iter()
                .map(|item| cast_literal(item, inner))
                .collect::<Result<Vec<_>, _>>()
                .map(Literal::Array),
            other => Err(LiteralFailure::rejected(format_args!(
                "cannot cast {:?} to array",
                other
            ))),
        },
        other => Err(LiteralFailure::rejected(format_args!(
            "standalone generate_series does not support CAST to {:?}",
            other
        ))),
    }
}

pub(crate) fn eval_array_generate_literal(args: &[Literal]) -> Result<Literal, LiteralFailure> {
    if args.is_empty() || args.len() > 3 {
        return Err(LiteralFailure::rejected(
            "array_generate expects 1 to 3 numeric arguments",
        ));
    }
    // SQL NULL propagation: if any argument is NULL, the whole call is NULL.
    if args.iter().any(|a| matches!(a, Literal::Null)) {
        return Ok(Literal::Null);
    }
    let literal_to_i64 = |value: &Literal| match value {
        Literal::Int(v) => Ok(*v),
        other => Err(LiteralFailure::rejected(format_args!(
            "array_generate expects integer arguments, got {other:?}"
        ))),
    };
    let (start, stop, step) = match args.len() {
        1 => (1, literal_to_i64(&args[0])?, 1),
        2 => (literal_to_i64(&args[0])?, literal_to_i64(&args[1])?, 1),
        3 => (
            literal_to_i64(&args[0])?,
            literal_to_i64(&args[1])?,
            literal_to_i64(&args[2])?,
        ),
        _ => unreachable!(),
    };
    if step == 0 {
        return Err(LiteralFailure::rejected(
            "array_generate step must not be zero",
        ));
    }

    let count = if (step > 0 && start > stop) || (step < 0 && start < stop) {
        0u128
    } else {
        i128::from(start).abs_diff(i128::from(stop)) / i128::from(step).unsigned_abs() + 1
    };
    let required = count * std::mem::size_of::<Literal>() as u128;
    let count =
        usize::try_from(count).map_err(|source| LiteralFailure::address(required, source))?;
    std::alloc::Layout::array::<Literal>(count)
        .map_err(|source| LiteralFailure::layout(required, source))?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|source| LiteralFailure::allocation(required, source))?;
    let mut current = start;
    for ordinal in 0..count {
        values.push(Literal::Int(current));
        if ordinal + 1 < count {
            current = current
                .checked_add(step)
                .ok_or_else(|| LiteralFailure::numeric("array_generate value overflow"))?;
        }
    }
    Ok(Literal::Array(values))
}

// ---------------------------------------------------------------------------
// Local parquet table helpers
// ---------------------------------------------------------------------------

/// Convert a SQL type to an Arrow DataType.
pub fn sql_type_to_arrow_type(sql_type: &SqlType) -> Result<DataType, String> {
    match sql_type {
        SqlType::TinyInt => Ok(DataType::Int8),
        SqlType::SmallInt => Ok(DataType::Int16),
        SqlType::Int => Ok(DataType::Int32),
        SqlType::BigInt => Ok(DataType::Int64),
        SqlType::LargeInt => Ok(DataType::FixedSizeBinary(
            novarocks_types::largeint::LARGEINT_BYTE_WIDTH,
        )),
        SqlType::Float => Ok(DataType::Float32),
        SqlType::Double => Ok(DataType::Float64),
        SqlType::String | SqlType::Json => Ok(DataType::Utf8),
        SqlType::Binary
        | SqlType::Bitmap
        | SqlType::Hll
        | SqlType::Object
        | SqlType::Percentile => Ok(DataType::Binary),
        SqlType::Boolean => Ok(DataType::Boolean),
        SqlType::Date => Ok(DataType::Date32),
        SqlType::DateTime => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        SqlType::DateTimeNs => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        SqlType::Time => Ok(DataType::Time64(TimeUnit::Microsecond)),
        SqlType::Decimal { precision, scale } => Ok(DataType::Decimal128(*precision, *scale)),
        SqlType::Array(inner) => Ok(DataType::List(Arc::new(sql_type_to_arrow_field(
            "item", inner, true,
        )?))),
        SqlType::Map(key, value) => {
            let entries = DataType::Struct(
                vec![
                    Arc::new(sql_type_to_arrow_field("key", key, true)?),
                    Arc::new(sql_type_to_arrow_field("value", value, true)?),
                ]
                .into(),
            );
            Ok(DataType::Map(
                Arc::new(Field::new("entries", entries, false)),
                false,
            ))
        }
        SqlType::Struct(fields) => Ok(DataType::Struct(
            fields
                .iter()
                .map(|(name, data_type)| {
                    Ok(Arc::new(sql_type_to_arrow_field(name, data_type, true)?))
                })
                .collect::<Result<Vec<_>, String>>()?
                .into(),
        )),
        SqlType::Variant => Ok(DataType::LargeBinary),
        SqlType::Uuid => Ok(DataType::FixedSizeBinary(16)),
    }
}

fn sql_type_to_arrow_field(
    name: &str,
    sql_type: &SqlType,
    nullable: bool,
) -> Result<Field, String> {
    use novarocks_types::logical::{LogicalType as L, field_with_logical_type};
    let field = Field::new(name, sql_type_to_arrow_type(sql_type)?, nullable);
    let logical = match sql_type {
        SqlType::Json => Some(L::Json),
        SqlType::Hll => Some(L::Hll),
        SqlType::Bitmap => Some(L::Bitmap),
        SqlType::Object => Some(L::Object),
        SqlType::Percentile => Some(L::Percentile),
        _ => None,
    };
    Ok(match logical {
        Some(logical) => field_with_logical_type(field, logical),
        None => field,
    })
}

// Ownership: this is the exact inverse of `sql_type_to_arrow_type` above and is
// a pure type-system mapping — it carries no catalog, connector, or Iceberg
// facts. It belongs beside its inverse in the SQL crate rather than inside a
// query-assembly module, so both the frontend assembly path and core catalog
// consumers can share one definition without depending on each other.
/// Recursive Arrow DataType -> SqlType conversion for CTAS schema inference.
pub fn arrow_data_type_to_sql_type(dt: &DataType) -> Result<SqlType, String> {
    Ok(match dt {
        DataType::Boolean => SqlType::Boolean,
        DataType::Int8 => SqlType::TinyInt,
        DataType::Int16 => SqlType::SmallInt,
        DataType::Int32 => SqlType::Int,
        DataType::Int64 => SqlType::BigInt,
        DataType::Float32 => SqlType::Float,
        DataType::Float64 => SqlType::Double,
        DataType::Decimal128(precision, scale) => SqlType::Decimal {
            precision: *precision,
            scale: *scale,
        },
        DataType::Utf8 | DataType::LargeUtf8 => SqlType::String,
        DataType::Binary | DataType::LargeBinary => SqlType::Binary,
        // StarRocks LARGEINT is stored as a fixed 16-byte signed integer.
        DataType::FixedSizeBinary(w) if *w == novarocks_types::largeint::LARGEINT_BYTE_WIDTH => {
            SqlType::LargeInt
        }
        DataType::Date32 => SqlType::Date,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => SqlType::DateTimeNs,
        DataType::Timestamp(_, _) => SqlType::DateTime,
        DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond) => SqlType::Time,
        DataType::List(elem) => {
            SqlType::Array(Box::new(arrow_data_type_to_sql_type(elem.data_type())?))
        }
        DataType::Struct(fields) => SqlType::Struct(
            fields
                .iter()
                .map(|f| {
                    Ok((
                        f.name().clone(),
                        arrow_data_type_to_sql_type(f.data_type())?,
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?,
        ),
        DataType::Map(entries, _) => {
            // Arrow MAP is encoded as List<Struct{key, value}>.
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(
                    "CTAS: MAP column has unexpected Arrow encoding (expected struct entries)"
                        .to_string(),
                );
            };
            let (_, key_field) = fields
                .find("key")
                .ok_or_else(|| "CTAS: MAP column missing 'key' field".to_string())?;
            let (_, val_field) = fields
                .find("value")
                .ok_or_else(|| "CTAS: MAP column missing 'value' field".to_string())?;
            SqlType::Map(
                Box::new(arrow_data_type_to_sql_type(key_field.data_type())?),
                Box::new(arrow_data_type_to_sql_type(val_field.data_type())?),
            )
        }
        other => {
            return Err(format!(
                "CTAS: arrow type {other:?} not supported; \
                 use CREATE TABLE then INSERT for variant/geometry/geography or \
                 unsupported numeric types (Float16, Decimal256, Interval, etc.)"
            ));
        }
    })
}

/// Compare two Arrow [`DataType`]s for structural equality while ignoring
/// nested [`Field`] metadata and nested-field nullability.
///
/// Motivation: Maps / Structs / Lists scanned from Iceberg parquet carry
/// `PARQUET:field_id` metadata on every inner Field, and the Iceberg map
/// convention uses non-null map keys, whereas the layout-derived expected
/// type produced by [`sql_type_to_arrow_type`] does not carry any metadata
/// and conservatively marks every nested field nullable. The two are
/// semantically the same shape; the strict `PartialEq` on `DataType` rejects
/// them.
///
/// This helper recurses through the container types (Map, Struct, List,
/// LargeList, FixedSizeList, Dictionary, Union, RunEndEncoded) and compares
/// only inner `DataType`s — never inner `Field` metadata, names, or
/// nullability. Scalar types fall through to strict equality.
///
/// Callers that need top-level column nullability enforcement must keep
/// their own `Field::is_nullable()` check; this helper deliberately operates
/// on `DataType` only.
pub use novarocks_type_contract::arrow_type_equals_ignoring_metadata;

pub fn compare_literals(left: &Literal, right: &Literal) -> Result<std::cmp::Ordering, String> {
    use std::cmp::Ordering;
    match (left, right) {
        (Literal::Int(l), Literal::Int(r)) => Ok(l.cmp(r)),
        (Literal::Float(l), Literal::Float(r)) => Ok(l.partial_cmp(r).unwrap_or(Ordering::Equal)),
        (Literal::Int(l), Literal::Float(r)) => {
            Ok((*l as f64).partial_cmp(r).unwrap_or(Ordering::Equal))
        }
        (Literal::Float(l), Literal::Int(r)) => {
            Ok(l.partial_cmp(&(*r as f64)).unwrap_or(Ordering::Equal))
        }
        (Literal::String(l), Literal::String(r)) => Ok(l.cmp(r)),
        (Literal::Bool(l), Literal::Bool(r)) => Ok(l.cmp(r)),
        (l, r) => Err(format!(
            "cannot compare {:?} and {:?} for aggregate merge",
            l, r
        )),
    }
}

/// Extract a `Literal` from a batch column at a specific row.
pub fn literal_from_batch(column: &ArrayRef, row_idx: usize) -> Result<Literal, String> {
    use arrow::array::*;
    use arrow::datatypes::TimeUnit;

    if column.is_null(row_idx) {
        return Ok(Literal::Null);
    }
    match column.data_type() {
        DataType::Boolean => {
            let arr = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or("downcast BooleanArray")?;
            Ok(Literal::Bool(arr.value(row_idx)))
        }
        DataType::Int8 => {
            let arr = column
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or("downcast Int8Array")?;
            Ok(Literal::Int(i64::from(arr.value(row_idx))))
        }
        DataType::Int16 => {
            let arr = column
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or("downcast Int16Array")?;
            Ok(Literal::Int(i64::from(arr.value(row_idx))))
        }
        DataType::Int32 => {
            let arr = column
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or("downcast Int32Array")?;
            Ok(Literal::Int(i64::from(arr.value(row_idx))))
        }
        DataType::Int64 => {
            let arr = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("downcast Int64Array")?;
            Ok(Literal::Int(arr.value(row_idx)))
        }
        DataType::Float32 => {
            let arr = column
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or("downcast Float32Array")?;
            Ok(Literal::Float(f64::from(arr.value(row_idx))))
        }
        DataType::Float64 => {
            let arr = column
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or("downcast Float64Array")?;
            Ok(Literal::Float(arr.value(row_idx)))
        }
        DataType::Decimal128(_, scale) => {
            let arr = column
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or("downcast Decimal128Array")?;
            let value = arr.value(row_idx);
            if *scale == 0 {
                i64::try_from(value)
                    .map(Literal::Int)
                    .map_err(|_| format!("decimal value {value} is out of range for INT64"))
            } else {
                Ok(Literal::String(format_decimal128_value(value, *scale)?))
            }
        }
        DataType::Utf8 => {
            let arr = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("downcast StringArray")?;
            Ok(Literal::String(arr.value(row_idx).to_string()))
        }
        DataType::Binary => {
            let arr = column
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or("downcast BinaryArray")?;
            Ok(Literal::String(bytes_to_latin1_string(arr.value(row_idx))))
        }
        DataType::LargeBinary => {
            let arr = column
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or("downcast LargeBinaryArray")?;
            Ok(Literal::String(bytes_to_latin1_string(arr.value(row_idx))))
        }
        DataType::Date32 => {
            use chrono::{Duration as ChronoDuration, NaiveDate};
            let arr = column
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or("downcast Date32Array")?;
            let days = arr.value(row_idx);
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
            let formatted = (epoch + ChronoDuration::days(i64::from(days)))
                .format("%Y-%m-%d")
                .to_string();
            Ok(Literal::Date(formatted))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            use chrono::DateTime;
            let arr = column
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or("downcast TimestampMicrosecondArray")?;
            let micros = arr.value(row_idx);
            let formatted = DateTime::from_timestamp_micros(micros)
                .expect("timestamp micros should be valid")
                .naive_utc()
                .format("%Y-%m-%d %H:%M:%S")
                .to_string();
            Ok(Literal::String(formatted))
        }
        DataType::List(_) => {
            let list = column
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or("downcast ListArray")?;
            let values = list.value(row_idx);
            let mut items = Vec::with_capacity(values.len());
            for idx in 0..values.len() {
                items.push(literal_from_batch(&values, idx)?);
            }
            Ok(Literal::Array(items))
        }
        DataType::Struct(_) => {
            let struct_array = column
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or("downcast StructArray")?;
            let mut items = Vec::with_capacity(struct_array.num_columns());
            for child_idx in 0..struct_array.num_columns() {
                items.push(literal_from_batch(struct_array.column(child_idx), row_idx)?);
            }
            Ok(Literal::Struct(items))
        }
        DataType::Map(_, _) => {
            let map = column
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or("downcast MapArray")?;
            let entries = map.value(row_idx);
            let entries = entries
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or("downcast StructArray for map entries")?;
            if entries.num_columns() != 2 {
                return Err(format!(
                    "map entries must contain 2 fields, got {}",
                    entries.num_columns()
                ));
            }
            let keys = entries.column(0);
            let values = entries.column(1);
            let mut out = Vec::with_capacity(entries.len());
            for idx in 0..entries.len() {
                out.push((
                    literal_from_batch(keys, idx)?,
                    literal_from_batch(values, idx)?,
                ));
            }
            Ok(Literal::Map(out))
        }
        other => Err(format!(
            "literal_from_batch does not support column type {:?}",
            other
        )),
    }
}

pub(crate) fn format_decimal128_value(value: i128, scale: i8) -> Result<String, String> {
    if scale < 0 {
        return Err(format!("unsupported decimal scale: {scale}"));
    }
    let scale = u32::try_from(scale).map_err(|_| format!("unsupported decimal scale: {scale}"))?;
    if scale == 0 {
        return Ok(value.to_string());
    }
    let factor = 10_u128
        .checked_pow(scale)
        .ok_or_else(|| format!("unsupported decimal scale: {scale}"))?;
    let negative = value.is_negative();
    let abs = value.unsigned_abs();
    let whole = abs / factor;
    let fraction = abs % factor;
    Ok(format!(
        "{}{}.{:0width$}",
        if negative { "-" } else { "" },
        whole,
        fraction,
        width = scale as usize
    ))
}

pub fn column_default_to_ast_literal(
    value: &ColumnDefault,
    column_type: &SqlType,
) -> Result<Literal, String> {
    validate_column_default(value)?;
    if let ColumnDefault::Decimal { scale, .. } = value
        && *scale < 0
    {
        return Err(format!("negative DECIMAL scale {scale} is not supported"));
    }
    if let SqlType::Decimal { scale, .. } = column_type
        && *scale < 0
    {
        return Err(format!("negative DECIMAL scale {scale} is not supported"));
    }

    match (value, column_type) {
        (ColumnDefault::Boolean(value), SqlType::Boolean) => Ok(Literal::Bool(*value)),
        (ColumnDefault::Int32(value), SqlType::TinyInt | SqlType::SmallInt | SqlType::Int) => {
            Ok(Literal::Int(i64::from(*value)))
        }
        (ColumnDefault::Int64(value), SqlType::BigInt) => Ok(Literal::Int(*value)),
        (ColumnDefault::Float32 { bits }, SqlType::Float) => {
            Ok(Literal::Float(f64::from(f32::from_bits(*bits))))
        }
        (ColumnDefault::Float64 { bits }, SqlType::Double) => {
            Ok(Literal::Float(f64::from_bits(*bits)))
        }
        (
            ColumnDefault::Decimal {
                unscaled,
                precision,
                scale,
            },
            SqlType::Decimal {
                precision: column_precision,
                scale: column_scale,
            },
        ) if precision == column_precision && scale == column_scale => {
            Ok(Literal::String(format_decimal128_value(*unscaled, *scale)?))
        }
        (ColumnDefault::String(value), SqlType::String | SqlType::Json) => {
            Ok(Literal::String(value.clone()))
        }
        (ColumnDefault::Binary(value), SqlType::Binary | SqlType::Bitmap | SqlType::Hll) => {
            Ok(Literal::String(bytes_to_latin1_string(value)))
        }
        (ColumnDefault::Date { days_since_epoch }, SqlType::Date) => {
            use chrono::NaiveDate;
            const UNIX_EPOCH_DAY_OFFSET: i32 = 719_163;
            let ce_days = UNIX_EPOCH_DAY_OFFSET
                .checked_add(*days_since_epoch)
                .ok_or_else(|| {
                    format!(
                        "write-default date value {days_since_epoch} is out of representable range"
                    )
                })?;
            let date = NaiveDate::from_num_days_from_ce_opt(ce_days).ok_or_else(|| {
                format!("write-default date value {days_since_epoch} is out of representable range")
            })?;
            Ok(Literal::Date(date.format("%Y-%m-%d").to_string()))
        }
        (
            ColumnDefault::TimestampMicros { micros_since_epoch }
            | ColumnDefault::TimestamptzMicros { micros_since_epoch },
            SqlType::DateTime,
        ) => {
            use chrono::DateTime as ChronoDateTime;
            let datetime = ChronoDateTime::from_timestamp_micros(*micros_since_epoch).ok_or_else(
                || {
                    format!(
                        "write-default datetime value {micros_since_epoch} µs is out of representable range"
                    )
                },
            )?;
            Ok(Literal::String(
                datetime.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string(),
            ))
        }
        (
            ColumnDefault::TimestampNanos { nanos_since_epoch }
            | ColumnDefault::TimestamptzNanos { nanos_since_epoch },
            SqlType::DateTimeNs,
        ) => {
            use chrono::DateTime as ChronoDateTime;
            let datetime = ChronoDateTime::from_timestamp_nanos(*nanos_since_epoch);
            Ok(Literal::String(
                datetime
                    .naive_utc()
                    .format("%Y-%m-%d %H:%M:%S%.9f")
                    .to_string(),
            ))
        }
        (ColumnDefault::Array(elements), SqlType::Array(_)) => {
            if !elements.is_empty() {
                return Err(format!(
                    "non-empty ARRAY write-default is not yet supported ({} elements)",
                    elements.len()
                ));
            }
            Ok(Literal::Array(Vec::new()))
        }
        (ColumnDefault::Map(entries), SqlType::Map(_, _)) => {
            if !entries.is_empty() {
                return Err(format!(
                    "non-empty MAP write-default is not yet supported ({} entries)",
                    entries.len()
                ));
            }
            Ok(Literal::Map(Vec::new()))
        }
        (value, column_type) => Err(format!(
            "write-default literal type does not match column type: literal={value:?} column={column_type:?}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::semantic::Literal;

    #[test]
    fn m07_internal_opaque_facts_do_not_admit_public_casts_or_defaults() {
        use novarocks_types::logical::{LogicalType, logical_type_of_field};
        for (sql_type, logical, spelling) in [
            (SqlType::Object, LogicalType::Object, "OBJECT"),
            (SqlType::Percentile, LogicalType::Percentile, "PERCENTILE"),
        ] {
            assert_eq!(sql_type_to_arrow_type(&sql_type).unwrap(), DataType::Binary);
            let nested =
                sql_type_to_arrow_type(&SqlType::Array(Box::new(sql_type.clone()))).unwrap();
            let DataType::List(item) = nested else {
                panic!("expected List")
            };
            assert_eq!(logical_type_of_field(&item), Some(logical));
            assert!(
                column_default_to_ast_literal(&ColumnDefault::Binary(vec![1]), &sql_type).is_err()
            );
            assert!(cast_literal(Literal::Int(1), &sql_type).is_err());
            let ast::Expr::Cast(cast) = parse_expr(&format!("CAST(1 AS {spelling})")) else {
                panic!("expected Cast")
            };
            assert!(type_name_to_sql_type(&cast.data_type).is_err());
        }
    }

    fn parse_expr(sql: &str) -> novarocks_parser::ast::Expr {
        let statements =
            novarocks_parser::parse(&format!("SELECT {sql}")).expect("parse expression query");
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("expected query");
        };
        let novarocks_parser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select");
        };
        let [novarocks_parser::ast::SelectItem::SingleColumn(projection_column)] =
            select.projection.as_slice()
        else {
            panic!("expected expression projection");
        };
        assert!(projection_column.alias.is_none(), "anonymous projection");
        let expr = &projection_column.expression;
        expr.clone()
    }

    #[test]
    fn parse_date_string_to_days_uses_unix_epoch() {
        assert_eq!(parse_date_string_to_days("1970-01-01").unwrap(), 0);
        assert_eq!(parse_date_string_to_days("1970-01-02").unwrap(), 1);
    }

    #[test]
    fn nanosecond_arrow_timestamp_maps_to_datetimens() {
        assert_eq!(
            arrow_data_type_to_sql_type(&DataType::Timestamp(TimeUnit::Nanosecond, None)).unwrap(),
            SqlType::DateTimeNs
        );
        assert_eq!(
            arrow_data_type_to_sql_type(&DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap(),
            SqlType::DateTime
        );
    }

    #[test]
    fn parse_datetime_string_to_micros_accepts_seconds_fraction_and_date_only() {
        assert_eq!(
            parse_datetime_string_to_micros("1970-01-01 00:00:01").unwrap(),
            1_000_000
        );
        assert_eq!(
            parse_datetime_string_to_micros("1970-01-01 00:00:01.123456").unwrap(),
            1_123_456
        );
        assert_eq!(
            parse_datetime_string_to_micros("1970-01-02").unwrap(),
            86_400_000_000
        );
    }

    #[test]
    fn parse_datetime_string_to_nanos_keeps_nanoseconds() {
        let nanos = parse_datetime_string_to_nanos("2024-01-02 03:04:05.123456789").unwrap();
        assert_eq!(nanos % 1_000, 789);
    }

    #[test]
    fn parse_datetime_string_to_nanos_handles_no_fraction() {
        let a = parse_datetime_string_to_nanos("2024-01-02 03:04:05").unwrap();
        let b = parse_datetime_string_to_nanos("2024-01-02").unwrap();
        assert_eq!(a % 1_000_000_000, 0);
        assert_eq!(b % 1_000_000_000, 0);
    }

    #[test]
    fn parse_datetime_string_to_nanos_rejects_out_of_range() {
        assert_eq!(
            parse_datetime_string_to_nanos("2262-04-12 00:00:00")
                .unwrap_err()
                .to_string(),
            "DATETIME literal '2262-04-12 00:00:00' out of nanosecond representable range"
        );
    }

    #[test]
    fn column_default_to_ast_literal_preserves_omitted_behavior() {
        assert_eq!(
            column_default_to_ast_literal(
                &ColumnDefault::TimestampMicros {
                    micros_since_epoch: 1_704_110_400_123_456,
                },
                &SqlType::DateTime,
            )
            .unwrap(),
            Literal::String("2024-01-01 12:00:00".to_string())
        );
        assert_eq!(
            column_default_to_ast_literal(
                &ColumnDefault::TimestamptzMicros {
                    micros_since_epoch: 0,
                },
                &SqlType::DateTime,
            )
            .unwrap(),
            Literal::String("1970-01-01 00:00:00".to_string())
        );
        assert_eq!(
            column_default_to_ast_literal(
                &ColumnDefault::TimestampNanos {
                    nanos_since_epoch: 1_704_164_645_123_456_789,
                },
                &SqlType::DateTimeNs,
            )
            .unwrap(),
            Literal::String("2024-01-02 03:04:05.123456789".to_string())
        );
        assert_eq!(
            column_default_to_ast_literal(
                &ColumnDefault::Binary((0_u16..=255).map(|byte| byte as u8).collect()),
                &SqlType::Binary,
            )
            .unwrap(),
            Literal::String((0_u16..=255).map(|byte| char::from(byte as u8)).collect())
        );
        assert_eq!(
            column_default_to_ast_literal(
                &ColumnDefault::Array(Vec::new()),
                &SqlType::Array(Box::new(SqlType::Int))
            )
            .unwrap(),
            Literal::Array(Vec::new())
        );
        assert_eq!(
            column_default_to_ast_literal(
                &ColumnDefault::Array(vec![ColumnDefault::Int32(1)]),
                &SqlType::Array(Box::new(SqlType::Int)),
            )
            .unwrap_err()
            .to_string(),
            "non-empty ARRAY write-default is not yet supported (1 elements)"
        );
        assert_eq!(
            column_default_to_ast_literal(
                &ColumnDefault::Map(Vec::new()),
                &SqlType::Map(Box::new(SqlType::String), Box::new(SqlType::Int)),
            )
            .unwrap(),
            Literal::Map(Vec::new())
        );
        assert_eq!(
            column_default_to_ast_literal(
                &ColumnDefault::Map(vec![(
                    ColumnDefault::String("key".to_string()),
                    ColumnDefault::Int32(1),
                )]),
                &SqlType::Map(Box::new(SqlType::String), Box::new(SqlType::Int)),
            )
            .unwrap_err()
            .to_string(),
            "non-empty MAP write-default is not yet supported (1 entries)"
        );
        assert_eq!(
            column_default_to_ast_literal(
                &ColumnDefault::Decimal {
                    unscaled: 1,
                    precision: 10,
                    scale: -1,
                },
                &SqlType::Decimal {
                    precision: 10,
                    scale: 0,
                },
            )
            .unwrap_err()
            .to_string(),
            "negative DECIMAL scale -1 is not supported"
        );
    }

    #[test]
    fn column_default_to_ast_literal_maps_positive_decimal_date_and_primitives() {
        let cases = [
            (
                ColumnDefault::Boolean(true),
                SqlType::Boolean,
                Literal::Bool(true),
            ),
            (ColumnDefault::Int32(7), SqlType::TinyInt, Literal::Int(7)),
            (
                ColumnDefault::Int64(i64::MAX),
                SqlType::BigInt,
                Literal::Int(i64::MAX),
            ),
            (
                ColumnDefault::Float32 {
                    bits: 1.5_f32.to_bits(),
                },
                SqlType::Float,
                Literal::Float(1.5),
            ),
            (
                ColumnDefault::Float64 {
                    bits: 2.5_f64.to_bits(),
                },
                SqlType::Double,
                Literal::Float(2.5),
            ),
            (
                ColumnDefault::Decimal {
                    unscaled: 12_345,
                    precision: 10,
                    scale: 2,
                },
                SqlType::Decimal {
                    precision: 10,
                    scale: 2,
                },
                Literal::String("123.45".to_string()),
            ),
            (
                ColumnDefault::String("value".to_string()),
                SqlType::String,
                Literal::String("value".to_string()),
            ),
            (
                ColumnDefault::Date {
                    days_since_epoch: -1,
                },
                SqlType::Date,
                Literal::Date("1969-12-31".to_string()),
            ),
            (
                ColumnDefault::Binary(vec![0x00, 0xff]),
                SqlType::Bitmap,
                Literal::String(bytes_to_latin1_string(&[0x00, 0xff])),
            ),
            (
                ColumnDefault::Binary(vec![0x01, 0xfe]),
                SqlType::Hll,
                Literal::String(bytes_to_latin1_string(&[0x01, 0xfe])),
            ),
        ];

        for (value, sql_type, expected) in cases {
            assert_eq!(
                column_default_to_ast_literal(&value, &sql_type),
                Ok(expected),
                "value={value:?} sql_type={sql_type:?}"
            );
        }
    }

    #[test]
    fn column_default_to_ast_literal_rejects_neutral_type_mismatch() {
        let error = column_default_to_ast_literal(&ColumnDefault::Int32(1), &SqlType::String)
            .unwrap_err()
            .to_string();
        assert!(error.starts_with("write-default literal type does not match column type:"));
        assert!(error.contains("Int32(1)"));
        assert!(error.contains("column=String"));
    }

    #[test]
    fn temporal_literal_parsers_preserve_invalid_error_text() {
        let date_error = parse_date_string_to_days("not-a-date")
            .unwrap_err()
            .to_string();
        assert!(date_error.starts_with("invalid date literal `not-a-date`:"));
        assert_eq!(
            parse_datetime_string_to_micros("not-a-datetime")
                .unwrap_err()
                .to_string(),
            "invalid datetime literal `not-a-datetime`"
        );
        assert_eq!(
            parse_datetime_string_to_nanos("not-a-datetime")
                .unwrap_err()
                .to_string(),
            "invalid datetime literal `not-a-datetime`"
        );
    }

    #[test]
    fn parse_json_folds_to_variant_bytes_via_latin1_string() {
        // The native parser builds a FunctionCall node for `parse_json('{"a":1}')`.
        let raw = parse_expr(r#"parse_json('{"a":1}')"#);
        let novarocks_parser::ast::Expr::FunctionCall(ref func) = raw else {
            panic!("expected Function node, got {raw:?}");
        };

        let lit = function_to_literal(func).expect("parse_json fold");
        let Literal::String(packed) = lit else {
            panic!("expected Literal::String");
        };
        let unpacked = latin1_string_to_bytes(&packed).expect("latin1 decode");

        // Must equal the encoder's output for the same JSON.
        let expected =
            novarocks_types::value::variant_encode::encode_json_text_to_variant_bytes(r#"{"a":1}"#)
                .expect("encode");
        assert_eq!(unpacked, expected);
    }

    #[test]
    fn parse_json_rejects_invalid_argument_count() {
        let raw = parse_expr(r#"parse_json('{"a":1}', 'extra')"#);
        let novarocks_parser::ast::Expr::FunctionCall(ref func) = raw else {
            panic!("expected Function node");
        };
        let err = function_to_literal(func).expect_err("must fail");
        assert!(
            err.to_string().contains("parse_json expects 1 argument"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn bitmap_constant_folds_round_trip_through_runtime_codec() {
        use std::collections::BTreeSet;

        for (sql, expected) in [
            ("bitmap_empty()", BTreeSet::new()),
            ("to_bitmap(7)", BTreeSet::from([7])),
            ("bitmap_from_string('7, 9, 7')", BTreeSet::from([7, 9])),
        ] {
            let raw = parse_expr(sql);
            let novarocks_parser::ast::Expr::FunctionCall(ref func) = raw else {
                panic!("expected Function node for `{sql}`");
            };
            let Literal::String(packed) = function_to_literal(func).expect("bitmap constant fold")
            else {
                panic!("expected packed bitmap literal for `{sql}`");
            };
            let bytes = latin1_string_to_bytes(&packed).expect("latin1 decode");

            assert_eq!(
                novarocks_types::value::bitmap::decode_bitmap(&bytes)
                    .expect("runtime bitmap decode"),
                expected,
                "runtime codec disagrees with constant fold for `{sql}`"
            );
        }
    }

    #[test]
    fn hll_hash_const_fold_rejects_narrowing_cast() {
        // The const-fold path always hashes the literal as Int64 little-endian
        // bytes; CAST(5 AS TINYINT) would silently produce the wrong bytes
        // (8-byte vs 1-byte) compared to the runtime path. We must reject
        // the explicit narrowing CAST rather than silently diverge.
        for cast_type in ["TINYINT", "SMALLINT", "INT", "INTEGER", "FLOAT"] {
            let sql = format!("hll_hash(CAST(5 AS {cast_type}))");
            let raw = parse_expr(&sql);
            let novarocks_parser::ast::Expr::FunctionCall(ref func) = raw else {
                panic!("expected Function node for `{sql}`");
            };
            let err = function_to_literal(func).expect_err(&format!("must reject `{sql}`"));
            assert!(
                err.to_string().contains("hll_hash with narrowing CAST"),
                "unexpected error for `{sql}`: {err}"
            );
        }
    }

    #[test]
    fn hll_hash_const_fold_accepts_bigint_cast() {
        // CAST to BIGINT is the runtime path's native width for integer
        // literals, so it must continue to fold cleanly.
        let raw = parse_expr("hll_hash(CAST(5 AS BIGINT))");
        let novarocks_parser::ast::Expr::FunctionCall(ref func) = raw else {
            panic!("expected Function node");
        };
        let lit = function_to_literal(func).expect("BIGINT cast must fold");
        assert!(matches!(lit, Literal::String(_)));
    }

    #[test]
    fn arrow_type_equals_ignoring_metadata_handles_scalar_and_nested_shapes() {
        use std::collections::HashMap;
        let mut meta = HashMap::new();
        meta.insert("PARQUET:field_id".to_string(), "1".to_string());

        // Scalars compare by equality.
        assert!(arrow_type_equals_ignoring_metadata(
            &DataType::Int64,
            &DataType::Int64
        ));
        assert!(!arrow_type_equals_ignoring_metadata(
            &DataType::Int64,
            &DataType::Int32
        ));

        // Map: entries-field name and metadata differ; inner-key nullability
        // differs. Helper must still report equal.
        let actual = DataType::Map(
            Arc::new(Field::new(
                "key_value",
                DataType::Struct(arrow::datatypes::Fields::from(vec![
                    Field::new("key", DataType::Int64, false).with_metadata(meta.clone()),
                    Field::new("value", DataType::Int64, true).with_metadata(meta.clone()),
                ])),
                false,
            )),
            false,
        );
        let expected = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(arrow::datatypes::Fields::from(vec![
                    Field::new("key", DataType::Int64, true),
                    Field::new("value", DataType::Int64, true),
                ])),
                false,
            )),
            false,
        );
        assert!(arrow_type_equals_ignoring_metadata(&actual, &expected));

        // Map: differing inner value DataType must still be rejected.
        let mismatched_value = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(arrow::datatypes::Fields::from(vec![
                    Field::new("key", DataType::Int64, true),
                    Field::new("value", DataType::Int32, true), // differs
                ])),
                false,
            )),
            false,
        );
        assert!(!arrow_type_equals_ignoring_metadata(
            &actual,
            &mismatched_value
        ));

        // List nesting: same shape with and without metadata compare equal.
        let actual_list = DataType::List(Arc::new(
            Field::new("item", DataType::Int64, true).with_metadata(meta.clone()),
        ));
        let expected_list = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        assert!(arrow_type_equals_ignoring_metadata(
            &actual_list,
            &expected_list
        ));

        // Map keys_sorted flag must still differentiate.
        let sorted = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(arrow::datatypes::Fields::from(vec![
                    Field::new("key", DataType::Int64, true),
                    Field::new("value", DataType::Int64, true),
                ])),
                false,
            )),
            true,
        );
        assert!(!arrow_type_equals_ignoring_metadata(&expected, &sorted));
    }

    /// CAST(literal AS DECIMAL(...)) must NOT fold to a bare literal — it must
    /// return Err so that the INSERT fast-path routes via the query pipeline
    /// instead of writing the raw literal against the (possibly narrower) sink
    /// scale and producing a spurious "too many fractional digits" error.
    #[test]
    fn cast_to_decimal_returns_err_to_force_pipeline_routing() {
        // Standard DECIMAL(p,s) forms
        let exprs = &[
            "CAST(1.2344 AS DECIMAL(10, 4))",
            "CAST(1.2344 AS DEC(10, 4))",
            "CAST(1.2344 AS NUMERIC(10, 4))",
        ];
        for sql in exprs {
            let expr = parse_expr(sql);
            let result = expr_to_literal(&expr);
            assert!(
                result.is_err(),
                "Expected Err for `{sql}` but got {:?}",
                result
            );
        }

        // Non-DECIMAL CASTs must still fold successfully.
        let non_decimal = &[
            ("CAST(5 AS BIGINT)", Literal::Int(5)),
            ("CAST(5 AS INT)", Literal::Int(5)),
        ];
        for (sql, expected) in non_decimal {
            let expr = parse_expr(sql);
            let result = expr_to_literal(&expr)
                .unwrap_or_else(|e| panic!("Expected Ok for `{sql}` but got Err: {e}"));
            assert_eq!(
                result, *expected,
                "CAST to non-DECIMAL `{sql}` folded to wrong literal"
            );
        }
    }

    #[test]
    fn literal_from_batch_extracts_large_binary_as_latin1_string() {
        let raw = b"\x04\x00\x00\x00meta";
        let array = Arc::new(arrow::array::LargeBinaryArray::from_iter_values([
            raw.as_slice()
        ])) as ArrayRef;

        let literal = literal_from_batch(&array, 0).expect("large binary literal");

        assert_eq!(literal, Literal::String(bytes_to_latin1_string(raw)));
    }
}

#[cfg(test)]
mod source_failure_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use std::error::Error;
    fn fold(source: &str) -> Result<Literal, LiteralFailure> {
        let tokens = novarocks_parser::lex(source).unwrap();
        let expr = novarocks_parser::parser::parse_expression(source, &tokens).unwrap();
        expr_to_literal(&expr)
    }
    #[test]
    fn literal_source_keeps_original_bitmap_and_variant_facts_without_text_classification() {
        if let Some(entries) = (u32::MAX as usize).checked_add(1) {
            let bitmap = novarocks_types::value::bitmap::BitmapValueFailure::EntryCount { entries };
            let expected = bitmap.failure_fact();
            let error = LiteralFailure::from(bitmap);
            assert_eq!(error.failure_fact(), expected);
            assert!(
                error
                    .source()
                    .unwrap()
                    .is::<novarocks_types::value::bitmap::BitmapValueFailure>()
            );
        }
        assert_eq!(fold("bitmap_from_string('bad')").unwrap(), Literal::Null);
        let error = fold("parse_json('not-json')").unwrap_err();
        assert!(
            error
                .source()
                .unwrap()
                .is::<novarocks_types::value::variant_encode::VariantEncodeFailure>()
        );
        assert_eq!(error.failure_fact().class(), FailureClass::Data);
        let error = LiteralFailure::rejected("cancelled resource exhausted numeric overflow");
        assert_eq!(error.failure_fact().class(), FailureClass::Data);
        assert_ne!(
            error.failure_fact(),
            novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact()
        );
    }
    #[test]
    fn literal_source_integer_overflow_and_wide_repeat_are_explicit_causes() {
        for sql in [
            "9223372036854775807 + 1",
            "(-9223372036854775807 - 1) - 1",
            "9223372036854775807 * 2",
        ] {
            let error = fold(sql).unwrap_err();
            assert_eq!(
                error.failure_fact(),
                novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact()
            );
        }
        let error = negate_literal(Literal::Int(i64::MIN)).unwrap_err();
        assert_eq!(
            error.failure_fact(),
            novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact()
        );
        let error = fold("array_repeat(1, 9223372036854775807)").unwrap_err();
        assert_eq!(error.failure_fact().class(), FailureClass::Resource);
        assert!(error.source().unwrap().is::<std::alloc::LayoutError>());
        assert_eq!(
            error.failure_fact().resource().unwrap().limit,
            Some(isize::MAX as u64)
        );
    }
    #[test]
    fn literal_source_inclusive_series_never_advances_past_its_last_value() {
        assert_eq!(
            eval_array_generate_literal(&[
                Literal::Int(i64::MAX),
                Literal::Int(i64::MAX),
                Literal::Int(1)
            ])
            .unwrap(),
            Literal::Array(vec![Literal::Int(i64::MAX)])
        );
        assert_eq!(
            eval_array_generate_literal(&[
                Literal::Int(i64::MIN),
                Literal::Int(i64::MIN),
                Literal::Int(-1)
            ])
            .unwrap(),
            Literal::Array(vec![Literal::Int(i64::MIN)])
        );
        let error = eval_array_generate_literal(&[
            Literal::Int(i64::MIN),
            Literal::Int(i64::MAX),
            Literal::Int(1),
        ])
        .unwrap_err();
        assert_eq!(error.failure_fact().class(), FailureClass::Resource);
        assert_eq!(error.failure_fact().resource().unwrap().requested, None);
        assert_eq!(
            error.failure_fact().resource().unwrap().limit,
            Some(isize::MAX as u64)
        );
    }
    #[test]
    fn literal_source_decimal_parameters_never_narrow_modulo_the_carrier() {
        for sql in [
            "CAST(1 AS DECIMAL(294,0))",
            "CAST(1 AS DECIMAL(38,256))",
            "CAST(1 AS DECIMAL(39,0))",
        ] {
            let tokens = novarocks_parser::lex(sql).unwrap();
            let ast::Expr::Cast(expr) =
                novarocks_parser::parser::parse_expression(sql, &tokens).unwrap()
            else {
                panic!("actual CAST source");
            };
            assert!(type_name_to_sql_type(&expr.data_type).is_err());
        }
    }
    #[test]
    fn generic_date_json_and_decoded_binary_keep_original_literal_producer_values() {
        for (source, expected) in [
            ("DATE '2024-01-02'", Literal::String("2024-01-02".into())),
            ("JSON '{\"a\":1}'", Literal::String("{\"a\":1}".into())),
            (
                "TIMESTAMP '2024-01-02 03:04:05.123456'",
                Literal::String("2024-01-02 03:04:05.123456".into()),
            ),
            (
                "X'00ff'",
                Literal::String(bytes_to_latin1_string(&[0, 255])),
            ),
        ] {
            let tokens = novarocks_parser::lex(source).unwrap();
            let expression = novarocks_parser::parser::parse_expression(source, &tokens).unwrap();
            assert_eq!(expr_to_literal(&expression).unwrap(), expected, "{source}");
        }
        let unknown = ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Generic(ast::GenericLiteral {
                type_name: "INTEGER".into(),
                value: "1".into(),
            }),
            span: novarocks_parser::Span::new(0, 11),
        });
        assert!(expr_to_literal(&unknown).is_err());
    }
}
