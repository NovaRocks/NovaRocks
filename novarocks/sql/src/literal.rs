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

use crate::semantic::Literal;
use novarocks_parser::ast;
use novarocks_types::schema::{ColumnDefault, SqlType, validate_column_default};

#[path = "literal_failure.rs"]
mod failure;
pub use failure::LiteralFailure;

/// Tags accepted by the original typed-string parser and its value consumers.
/// GenericLiteral retains raw tag text, so quoted tags are not guessed aliases.
pub(crate) fn is_supported_generic_literal(value: &ast::GenericLiteral) -> bool {
    novarocks_expression::typing::supports_typed_string(&value.type_name)
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
                    crate::parser::expression_for_diagnostic(expr)
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
                    crate::parser::expression_for_diagnostic(expr)
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
            crate::parser::expression_for_diagnostic(expr)
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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

pub(crate) fn function_to_literal(func: &ast::FunctionCall) -> Result<Literal, LiteralFailure> {
    if func.processing_mode.is_some() {
        return Err(LiteralFailure::rejected(
            "function processing modes are not supported in literals",
        ));
    }
    let args = function_expr_args(&func.arguments)?;
    let name = object_name_lower(&func.name)?;
    match name.as_str() {
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
        "row" => Ok(Literal::Struct(
            args.into_iter()
                .map(expr_to_literal)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        _ => Err(LiteralFailure::rejected(format_args!(
            "unsupported expression in INSERT VALUES: {}",
            crate::parser::expression_for_diagnostic(&ast::Expr::FunctionCall(ast::AstBox::new(
                func.clone()
            )))
        ))),
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
#[cfg(test)]
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
        SqlType::String | SqlType::BoundedCharacter(_) => Ok(DataType::Utf8),
        SqlType::Json => Ok(DataType::LargeBinary),
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
        SqlType::Temporal(ty) => Ok(ty.carrier()),
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
        let query = &novarocks_parser::parser::parse_query(&format!("SELECT {sql}"))
            .expect("parse expression query");
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
    fn sql_json_functions_are_owned_by_typed_evaluation() {
        for sql in [
            "json_parse('1')",
            "parse_json('1')",
            "JSON_OBJECT('k' VALUE 1)",
        ] {
            let expr = parse_expr(sql);
            assert!(expr_to_literal(&expr).is_err(), "{sql}");
        }
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
        let expr = novarocks_parser::parser::parse_expression(source).unwrap();
        expr_to_literal(&expr)
    }
    #[test]
    fn literal_source_keeps_original_bitmap_facts_without_text_classification() {
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
        let error = LiteralFailure::rejected("cancelled resource exhausted numeric overflow");
        assert_eq!(error.failure_fact().class(), FailureClass::Data);
        assert_ne!(
            error.failure_fact(),
            novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact()
        );
    }
    #[test]
    fn literal_source_integer_overflow_preserves_the_explicit_cause() {
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
    }
    #[test]
    fn literal_source_decimal_parameters_never_narrow_modulo_the_carrier() {
        for sql in [
            "CAST(1 AS DECIMAL(294,0))",
            "CAST(1 AS DECIMAL(38,256))",
            "CAST(1 AS DECIMAL(39,0))",
        ] {
            let ast::Expr::Cast(expr) = novarocks_parser::parser::parse_expression(sql).unwrap()
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
            ("INTEGER '1'", Literal::String("1".into())),
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
            let expression = novarocks_parser::parser::parse_expression(source).unwrap();
            assert_eq!(expr_to_literal(&expression).unwrap(), expected, "{source}");
        }
        let unknown = ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Generic(ast::GenericLiteral {
                type_name: "NOT_A_SQL_TYPE".into(),
                value: "1".into(),
            }),
            span: novarocks_parser::Span::new(0, 11),
        });
        assert!(expr_to_literal(&unknown).is_err());
    }
}
