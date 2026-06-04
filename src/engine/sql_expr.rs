//! sqlparser AST → NovaRocks `Expr`/`Literal` conversion, plus literal
//! utilities (compare, cast, arithmetic, encoding, keying) and small
//! property/tokenizer helpers used across the standalone engine.
//!
//! Extracted from `engine/mod.rs` during the PR1 refactor; all items here are
//! pure functions with no standalone-runtime state — they just translate
//! between sqlparser tokens/expressions and NovaRocks types.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};

use crate::engine::record_batch_to_chunk;
use crate::runtime::query_result::{QueryResult, QueryResultColumn};
use crate::sql::parser::ast::{ArithmeticOp, Expr, Literal, SqlType};

pub(crate) fn strip_optional_identifier_quotes(token: &str) -> &str {
    token.trim_end_matches(';').trim_matches('`')
}

pub(crate) fn canonicalize_sql_for_match(sql: &str) -> String {
    sql.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

pub(crate) fn sqlparser_expr_to_custom_expr(expr: &sqlparser::ast::Expr) -> Result<Expr, String> {
    use sqlparser::ast as sqlast;
    match expr {
        sqlast::Expr::Identifier(ident) => Ok(Expr::Column(crate::sql::parser::ast::ColumnRef {
            name: ident.value.clone(),
        })),
        sqlast::Expr::CompoundIdentifier(parts) => {
            Ok(Expr::Column(crate::sql::parser::ast::ColumnRef {
                name: parts
                    .last()
                    .map(|p| p.value.clone())
                    .ok_or_else(|| "empty column reference".to_string())?,
            }))
        }
        sqlast::Expr::Value(sqlast::ValueWithSpan { value, .. }) => {
            let lit = match value {
                sqlast::Value::Null => Literal::Null,
                sqlast::Value::Boolean(b) => Literal::Bool(*b),
                sqlast::Value::Number(n, _) => sql_number_literal(n),
                sqlast::Value::SingleQuotedString(s) | sqlast::Value::DoubleQuotedString(s) => {
                    Literal::String(s.clone())
                }
                _ => return Err(format!("unsupported value in expression: {value}")),
            };
            Ok(Expr::Literal(lit))
        }
        sqlast::Expr::BinaryOp { left, op, right } => {
            let left_expr = sqlparser_expr_to_custom_expr(left)?;
            let right_expr = sqlparser_expr_to_custom_expr(right)?;
            match op {
                sqlast::BinaryOperator::Plus => Ok(Expr::Arithmetic {
                    left: Box::new(left_expr),
                    op: ArithmeticOp::Add,
                    right: Box::new(right_expr),
                }),
                sqlast::BinaryOperator::Minus => Ok(Expr::Arithmetic {
                    left: Box::new(left_expr),
                    op: ArithmeticOp::Sub,
                    right: Box::new(right_expr),
                }),
                sqlast::BinaryOperator::Multiply => Ok(Expr::Arithmetic {
                    left: Box::new(left_expr),
                    op: ArithmeticOp::Mul,
                    right: Box::new(right_expr),
                }),
                sqlast::BinaryOperator::Divide => Ok(Expr::Arithmetic {
                    left: Box::new(left_expr),
                    op: ArithmeticOp::Div,
                    right: Box::new(right_expr),
                }),
                sqlast::BinaryOperator::Modulo => Ok(Expr::Arithmetic {
                    left: Box::new(left_expr),
                    op: ArithmeticOp::Mod,
                    right: Box::new(right_expr),
                }),
                other => Err(format!("unsupported operator in expression: {other}")),
            }
        }
        sqlast::Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            let inner_expr = sqlparser_expr_to_custom_expr(inner)?;
            let sql_type = crate::sql::parser::dialect::convert_sql_type(data_type.clone())?;
            Ok(Expr::Cast {
                expr: Box::new(inner_expr),
                data_type: sql_type,
            })
        }
        sqlast::Expr::UnaryOp {
            op: sqlast::UnaryOperator::Minus,
            expr: inner,
        } => Ok(Expr::Literal(negate_literal(sqlparser_expr_to_literal(
            inner,
        )?)?)),
        sqlast::Expr::Nested(inner) => sqlparser_expr_to_custom_expr(inner),
        sqlast::Expr::Array(sqlast::Array { elem, .. }) => Ok(Expr::Array(
            elem.iter()
                .map(sqlparser_expr_to_custom_expr)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        // Function calls: try constant-folding via the INSERT-VALUES literal
        // helper first (covers `row(...)`, `map(...)`, fully-constant
        // `to_binary(...)`, etc.). If folding fails (e.g. args reference a
        // column), fall back to a ScalarFunction node that the row-wise
        // evaluator can dispatch on.
        sqlast::Expr::Function(func) => {
            if let Some(expr) = try_array_map_cast_string_custom_expr(func)? {
                return Ok(expr);
            }
            if let Ok(lit) = sqlparser_function_to_literal(func) {
                return Ok(Expr::Literal(lit));
            }
            let name = func.name.to_string().to_ascii_lowercase();
            let args = function_expr_args(&func.args)?
                .into_iter()
                .map(sqlparser_expr_to_custom_expr)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Expr::ScalarFunction(
                crate::sql::parser::ast::ScalarFunctionExpr { name, args },
            ))
        }
        other => Err(format!("unsupported expression: {other}")),
    }
}

pub(crate) fn bytes_to_latin1_string(bytes: &[u8]) -> String {
    bytes.iter().map(|b| char::from(*b)).collect()
}

pub(crate) fn latin1_string_to_bytes(value: &str) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(value.len());
    for ch in value.chars() {
        if (ch as u32) > 0xff {
            return Err(format!("literal contains non-LATIN1 character: {value:?}"));
        }
        out.push(ch as u8);
    }
    Ok(out)
}

/// Convert a sqlparser expression to a Literal (for INSERT VALUES)
pub(crate) fn sqlparser_expr_to_literal(expr: &sqlparser::ast::Expr) -> Result<Literal, String> {
    use sqlparser::ast as sqlast;
    match expr {
        sqlast::Expr::Value(sqlast::ValueWithSpan { value, .. }) => match value {
            sqlast::Value::Null => Ok(Literal::Null),
            sqlast::Value::Number(n, _) => Ok(sql_number_literal(n)),
            sqlast::Value::SingleQuotedString(s) | sqlast::Value::DoubleQuotedString(s) => {
                Ok(Literal::String(s.clone()))
            }
            sqlast::Value::HexStringLiteral(s) => {
                let bytes =
                    hex::decode(s).map_err(|err| format!("invalid hex literal X'{s}': {err}"))?;
                Ok(Literal::String(bytes_to_latin1_string(&bytes)))
            }
            sqlast::Value::Boolean(b) => Ok(Literal::Bool(*b)),
            _ => Err(format!("unsupported literal in INSERT VALUES: {value}")),
        },
        sqlast::Expr::UnaryOp {
            op: sqlast::UnaryOperator::Minus,
            expr: inner,
        } => negate_literal(sqlparser_expr_to_literal(inner)?),
        sqlast::Expr::Nested(inner) => sqlparser_expr_to_literal(inner),
        // Handle CAST(expr AS type): peel the CAST and evaluate the inner literal,
        // EXCEPT for DECIMAL targets. CAST to a DECIMAL type carries an explicit
        // (precision, scale) that the literal fast-path ignores — it always writes
        // the raw literal value against the *sink* column's scale, which may be
        // narrower and would produce a false "too many fractional digits" error.
        // Returning Err here causes select_projection_requires_pipeline to route
        // the INSERT through the full query pipeline instead, where the CAST is
        // evaluated with its declared type and the narrowing to the sink's
        // DECIMAL(p,s) is handled at write time (with rounding).
        sqlast::Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            if cast_data_type_is_decimal(data_type) {
                Err(format!(
                    "CAST to DECIMAL in INSERT SELECT requires pipeline evaluation: {expr}"
                ))
            } else {
                sqlparser_expr_to_literal(inner)
            }
        }
        // Handle DATE '2024-01-01' typed strings
        sqlast::Expr::TypedString(typed) => Ok(Literal::String(typed.value.to_string())),
        // In MySQL mode, "value" is parsed as an identifier — treat as string literal
        sqlast::Expr::Identifier(ident) => Ok(Literal::String(ident.value.clone())),
        // Handle binary operations like 10000 - 1
        sqlast::Expr::BinaryOp { left, op, right } => {
            let l = sqlparser_expr_to_literal(left)?;
            let r = sqlparser_expr_to_literal(right)?;
            match (l, op, r) {
                (Literal::Int(a), sqlast::BinaryOperator::Plus, Literal::Int(b)) => {
                    Ok(Literal::Int(a + b))
                }
                (Literal::Int(a), sqlast::BinaryOperator::Minus, Literal::Int(b)) => {
                    Ok(Literal::Int(a - b))
                }
                (Literal::Int(a), sqlast::BinaryOperator::Multiply, Literal::Int(b)) => {
                    Ok(Literal::Int(a * b))
                }
                (Literal::Float(a), sqlast::BinaryOperator::Plus, Literal::Float(b)) => {
                    Ok(Literal::Float(a + b))
                }
                (Literal::Float(a), sqlast::BinaryOperator::Minus, Literal::Float(b)) => {
                    Ok(Literal::Float(a - b))
                }
                _ => Err(format!("unsupported expression in INSERT VALUES: {expr}")),
            }
        }
        // Handle array literal [1, 2, 3]
        sqlast::Expr::Array(sqlast::Array { elem, .. }) => Ok(Literal::Array(
            elem.iter()
                .map(sqlparser_expr_to_literal)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        sqlast::Expr::Function(func) => sqlparser_function_to_literal(func),
        sqlast::Expr::Tuple(values) => Ok(Literal::Struct(
            values
                .iter()
                .map(sqlparser_expr_to_literal)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        sqlast::Expr::Struct { values, .. } => Ok(Literal::Struct(
            values
                .iter()
                .map(sqlparser_expr_to_literal)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        sqlast::Expr::Map(map) => Ok(Literal::Map(
            map.entries
                .iter()
                .map(|entry| {
                    Ok((
                        sqlparser_expr_to_literal(&entry.key)?,
                        sqlparser_expr_to_literal(&entry.value)?,
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?,
        )),
        _ => Err(format!("unsupported expression in INSERT VALUES: {expr}")),
    }
}

/// Returns true if the given sqlparser DataType is a DECIMAL variant (including
/// StarRocks-style DECIMAL32/DECIMAL64/DECIMAL128 custom names). Used to decide
/// whether a CAST-to-DECIMAL expression should be routed through the full query
/// pipeline rather than being folded into a bare literal.
fn cast_data_type_is_decimal(data_type: &sqlparser::ast::DataType) -> bool {
    use sqlparser::ast::DataType as DT;
    match data_type {
        DT::Decimal(_) | DT::Dec(_) | DT::Numeric(_) => true,
        DT::Custom(name, _) => {
            let lower = name.to_string().to_lowercase();
            matches!(
                lower.as_str(),
                "decimal" | "decimal32" | "decimal64" | "decimal128"
            )
        }
        _ => false,
    }
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

pub(crate) fn negate_literal(literal: Literal) -> Result<Literal, String> {
    match literal {
        Literal::Int(i) => Ok(Literal::Int(-i)),
        Literal::Float(f) => Ok(Literal::Float(-f)),
        Literal::String(s) if is_integral_sql_number(s.trim()) => {
            Ok(Literal::String(format!("-{}", s.trim())))
        }
        other => Err(format!("cannot negate {other:?}")),
    }
}

fn literal_to_json_key(literal: Literal) -> Result<Option<String>, String> {
    Ok(match literal {
        Literal::Null => None,
        Literal::Bool(v) => Some(if v { "true" } else { "false" }.to_string()),
        Literal::Int(v) => Some(v.to_string()),
        Literal::Float(v) => Some(v.to_string()),
        Literal::String(v) | Literal::Date(v) => Some(v),
        Literal::Array(_) | Literal::Map(_) | Literal::Struct(_) => {
            return Err("json_object key does not support complex type".to_string());
        }
    })
}

fn literal_to_json_value(literal: Literal) -> Result<JsonValue, String> {
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

fn json_object_literal(args: &[&sqlparser::ast::Expr]) -> Result<Literal, String> {
    let mut object = JsonMap::new();
    let mut idx = 0usize;
    while idx < args.len() {
        let key = sqlparser_expr_to_literal(args[idx])?;
        let Some(key) = literal_to_json_key(key)? else {
            return Ok(Literal::Null);
        };
        let value = if let Some(value_expr) = args.get(idx + 1) {
            literal_to_json_value(sqlparser_expr_to_literal(value_expr)?)?
        } else {
            JsonValue::Null
        };
        object.insert(key, value);
        idx += 2;
    }
    let json_text = serde_json::to_string(&JsonValue::Object(object))
        .map_err(|e| format!("json_object stringify failed: {e}"))?;
    let bytes = crate::exec::variant_encode::encode_json_text_to_variant_bytes(&json_text)
        .map_err(|e| format!("json_object failed: {e}"))?;
    Ok(Literal::String(bytes_to_latin1_string(&bytes)))
}

pub(crate) fn literal_to_i128_for_integer(
    literal: &Literal,
    type_name: &str,
) -> Result<Option<i128>, String> {
    match literal {
        Literal::Null => Ok(None),
        Literal::Int(v) => Ok(Some(i128::from(*v))),
        Literal::Float(v) => {
            if !v.is_finite() {
                return Err(format!(
                    "literal {:?} is not valid for {type_name}",
                    literal
                ));
            }
            if *v < i128::MIN as f64 || *v > i128::MAX as f64 {
                return Err(format!(
                    "literal {:?} is out of range for {type_name}",
                    literal
                ));
            }
            // StarRocks/MySQL truncate fractional values when assigning floats
            // to integer columns (e.g. `INSERT INTO int_col SELECT 1/19` ->
            // 0). Match that lenient behaviour rather than failing fast.
            Ok(Some(v.trunc() as i128))
        }
        Literal::String(s) => {
            // StarRocks-compat: an empty / whitespace-only string in a slot
            // that wants an integer (e.g. inside a STRUCT or MAP literal
            // like `row(null, '')` / `map(1,'abc','',null)`) coerces to NULL
            // rather than erroring.
            if s.trim().is_empty() {
                Ok(None)
            } else {
                s.trim()
                    .parse::<i128>()
                    .map(Some)
                    .map_err(|_| format!("literal `{s}` is not valid for {type_name}"))
            }
        }
        other => Err(format!("literal {:?} is not valid for {type_name}", other)),
    }
}

pub(crate) fn sqlparser_function_to_literal(
    func: &sqlparser::ast::Function,
) -> Result<Literal, String> {
    use sqlparser::ast as sqlast;

    let args = function_expr_args(&func.args)?;
    let name = func.name.to_string().to_ascii_lowercase();
    if let Some(value) = try_array_map_cast_string_literal(&name, &args)? {
        return Ok(value);
    }
    match name.as_str() {
        "array_generate" => {
            let values = args
                .iter()
                .map(|arg| sqlparser_expr_to_literal(arg))
                .collect::<Result<Vec<_>, _>>()?;
            eval_array_generate_literal(&values)
        }
        "array_repeat" => {
            if args.len() != 2 {
                return Err("array_repeat expects 2 arguments".to_string());
            }
            let value = sqlparser_expr_to_literal(args[0])?;
            let repeat = match sqlparser_expr_to_literal(args[1])? {
                Literal::Int(v) => v,
                other => return Err(format!("array_repeat expects integer count, got {other:?}")),
            };
            if repeat <= 0 {
                return Ok(Literal::Array(Vec::new()));
            }
            let repeat = usize::try_from(repeat)
                .map_err(|_| "array_repeat count is too large".to_string())?;
            Ok(Literal::Array(vec![value; repeat]))
        }
        "json_object" => json_object_literal(&args),
        "array_append" => {
            if args.len() != 2 {
                return Err("array_append expects 2 arguments".to_string());
            }
            let array = sqlparser_expr_to_literal(args[0])?;
            let value = sqlparser_expr_to_literal(args[1])?;
            match array {
                Literal::Null => Ok(Literal::Null),
                Literal::Array(mut values) => {
                    values.push(value);
                    Ok(Literal::Array(values))
                }
                other => Err(format!(
                    "array_append expects ARRAY argument, got {other:?}"
                )),
            }
        }
        "bitmap_empty" => {
            if !args.is_empty() {
                return Err("bitmap_empty expects 0 arguments".to_string());
            }
            // SeriV2 empty bitmap encoding: a single BITMAP_TYPE_EMPTY (=0) byte,
            // matching `eval_bitmap_empty` runtime output.
            Ok(Literal::String(bytes_to_latin1_string(&[
                crate::exec::expr::function::object::bitmap_common::BITMAP_TYPE_EMPTY,
            ])))
        }
        "hll_hash" => {
            if args.len() != 1 {
                return Err("hll_hash expects 1 argument".to_string());
            }
            // Reject explicit narrowing CAST since this const-fold path always
            // hashes Int64 little-endian bytes, while the runtime path hashes
            // the cast's native (narrower) width. Allowing the unwrap would
            // produce values that disagree with `eval_hll_hash` at runtime.
            if let sqlast::Expr::Cast { data_type, .. } = args[0] {
                let narrowing = matches!(
                    data_type,
                    sqlast::DataType::TinyInt(_)
                        | sqlast::DataType::TinyIntUnsigned(_)
                        | sqlast::DataType::UTinyInt
                        | sqlast::DataType::SmallInt(_)
                        | sqlast::DataType::SmallIntUnsigned(_)
                        | sqlast::DataType::USmallInt
                        | sqlast::DataType::Int2(_)
                        | sqlast::DataType::Int2Unsigned(_)
                        | sqlast::DataType::MediumInt(_)
                        | sqlast::DataType::MediumIntUnsigned(_)
                        | sqlast::DataType::Int(_)
                        | sqlast::DataType::Integer(_)
                        | sqlast::DataType::IntUnsigned(_)
                        | sqlast::DataType::IntegerUnsigned(_)
                        | sqlast::DataType::Int4(_)
                        | sqlast::DataType::Int4Unsigned(_)
                        | sqlast::DataType::Int16
                        | sqlast::DataType::Int32
                        | sqlast::DataType::Float(_)
                        | sqlast::DataType::FloatUnsigned(_)
                );
                if narrowing {
                    return Err(
                        "hll_hash with narrowing CAST argument is not supported in INSERT VALUES; \
                         wrap the value directly without CAST"
                            .to_string(),
                    );
                }
            }
            use crate::exec::expr::function::object::hll_hash::{
                MURMUR_SEED, encode_hll_empty, encode_hll_single, murmur_hash64a,
            };
            let arg = sqlparser_expr_to_literal(args[0])?;
            // Mirror the runtime `eval_hll_hash` byte conversion exactly:
            //   - NULL  → encode_hll_empty()
            //   - Int   → Int64 little-endian (analyzer types integer literals as Int64)
            //   - Float → Float64 little-endian
            //   - String → raw UTF-8 bytes
            //   - Bool  → single byte 0/1
            let bytes = match arg {
                Literal::Null => encode_hll_empty(),
                Literal::Int(v) => {
                    let buf = v.to_le_bytes();
                    let hash = murmur_hash64a(&buf, MURMUR_SEED);
                    encode_hll_single(hash)
                }
                Literal::Float(v) => {
                    let buf = v.to_le_bytes();
                    let hash = murmur_hash64a(&buf, MURMUR_SEED);
                    encode_hll_single(hash)
                }
                Literal::String(s) => {
                    let hash = murmur_hash64a(s.as_bytes(), MURMUR_SEED);
                    encode_hll_single(hash)
                }
                Literal::Bool(b) => {
                    let buf = [if b { 1u8 } else { 0u8 }];
                    let hash = murmur_hash64a(&buf, MURMUR_SEED);
                    encode_hll_single(hash)
                }
                other => return Err(format!("hll_hash unsupported literal: {other:?}")),
            };
            Ok(Literal::String(bytes_to_latin1_string(&bytes)))
        }
        "to_binary" => {
            if args.len() != 1 && args.len() != 2 {
                return Err("to_binary expects 1 or 2 arguments".to_string());
            }

            let Literal::String(input) = sqlparser_expr_to_literal(args[0])? else {
                return Err("to_binary expects VARCHAR as first argument".to_string());
            };

            let format = if args.len() == 2 {
                let Literal::String(format) = sqlparser_expr_to_literal(args[1])? else {
                    return Err("to_binary expects VARCHAR format argument".to_string());
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
                        .map_err(|e| format!("to_binary encode64 decode failed: {e}"))?
                }
                "utf8" => input.into_bytes(),
                _ => hex::decode(input).map_err(|e| format!("to_binary hex decode failed: {e}"))?,
            };

            Ok(Literal::String(
                bytes.iter().map(|b| char::from(*b)).collect(),
            ))
        }
        "bitmap_from_string" => {
            if args.len() != 1 {
                return Err("bitmap_from_string expects 1 argument".to_string());
            }
            let arg = sqlparser_expr_to_literal(args[0])?;
            let text = match arg {
                Literal::Null => return Ok(Literal::Null),
                Literal::String(s) => s,
                other => {
                    return Err(format!(
                        "bitmap_from_string expects VARCHAR argument, got {other:?}"
                    ));
                }
            };
            // Mirror runtime semantics: malformed string -> NULL (not error).
            let values =
                match crate::exec::expr::function::object::bitmap_common::parse_bitmap_string(&text)
                {
                    Ok(v) => v,
                    Err(_) => return Ok(Literal::Null),
                };
            // Use the EXTERNAL (storage / SeriV1-style) encoding here —
            // that's the format the StarRocks table bitmap column
            // reader expects, matching `bitmap_empty` / `to_bitmap`'s
            // const-fold output. The internal varint format only round-
            // trips through the runtime expression layer.
            let bytes = crate::exec::expr::function::object::bitmap_common::encode_external_bitmap(
                &values,
            )?;
            Ok(Literal::String(bytes_to_latin1_string(&bytes)))
        }
        "to_bitmap" => {
            if args.len() != 1 {
                return Err("to_bitmap expects 1 argument".to_string());
            }
            use crate::exec::expr::function::object::to_bitmap::encode_bitmap_single;
            let arg = sqlparser_expr_to_literal(args[0])?;
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
                other => return Err(format!("to_bitmap unsupported literal: {other:?}")),
            };
            let bytes = encode_bitmap_single(value);
            Ok(Literal::String(bytes_to_latin1_string(&bytes)))
        }
        "md5sum" => {
            use md5::Digest;
            let mut hasher = md5::Md5::new();
            for arg in args {
                let literal = sqlparser_expr_to_literal(arg)?;
                let Some(bytes) = literal_to_varchar_bytes(&literal)? else {
                    continue;
                };
                hasher.update(bytes);
            }
            Ok(Literal::String(hex::encode(hasher.finalize())))
        }
        "parse_json" => {
            if args.len() != 1 {
                return Err("parse_json expects 1 argument".to_string());
            }
            let Literal::String(json_text) = sqlparser_expr_to_literal(args[0])? else {
                return Err("parse_json expects VARCHAR argument".to_string());
            };
            let bytes = crate::exec::variant_encode::encode_json_text_to_variant_bytes(&json_text)
                .map_err(|e| format!("parse_json failed: {e}"))?;
            // Pack raw variant bytes into Literal::String via Latin-1 (matches
            // `to_binary` convention; INSERT VALUES decodes via
            // `latin1_string_to_bytes`).
            Ok(Literal::String(bytes_to_latin1_string(&bytes)))
        }
        "row" => Ok(Literal::Struct(
            args.into_iter()
                .map(sqlparser_expr_to_literal)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        "named_struct" => {
            if args.len() % 2 != 0 {
                return Err(format!(
                    "named_struct literal requires an even number of arguments, got {}",
                    args.len()
                ));
            }
            Ok(Literal::Struct(
                args.into_iter()
                    .skip(1)
                    .step_by(2)
                    .map(sqlparser_expr_to_literal)
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
        "map" => {
            if args.len() % 2 != 0 {
                return Err(format!(
                    "MAP literal requires an even number of arguments, got {}",
                    args.len()
                ));
            }
            let mut entries = Vec::with_capacity(args.len() / 2);
            for pair in args.chunks_exact(2) {
                entries.push((
                    sqlparser_expr_to_literal(pair[0])?,
                    sqlparser_expr_to_literal(pair[1])?,
                ));
            }
            Ok(Literal::Map(entries))
        }
        _ => Err(format!(
            "unsupported expression in INSERT VALUES: {}",
            sqlast::Expr::Function(func.clone())
        )),
    }
}

fn literal_to_varchar_bytes(value: &Literal) -> Result<Option<Vec<u8>>, String> {
    match value {
        Literal::Null => Ok(None),
        Literal::Bool(v) => Ok(Some(if *v { b"1".to_vec() } else { b"0".to_vec() })),
        Literal::Int(v) => Ok(Some(v.to_string().into_bytes())),
        Literal::Float(v) => Ok(Some(v.to_string().into_bytes())),
        Literal::String(v) | Literal::Date(v) => Ok(Some(v.as_bytes().to_vec())),
        Literal::Array(_) | Literal::Map(_) | Literal::Struct(_) => {
            Err("md5sum literal folding does not support complex arguments".to_string())
        }
    }
}

fn try_array_map_cast_string_custom_expr(
    func: &sqlparser::ast::Function,
) -> Result<Option<Expr>, String> {
    let name = func.name.to_string().to_ascii_lowercase();
    if name != "array_map" && name != "transform" {
        return Ok(None);
    }
    let args = function_expr_args(&func.args)?;
    if !array_map_cast_string_lambda_matches(&args)? {
        return Ok(None);
    }
    Ok(Some(Expr::Cast {
        expr: Box::new(sqlparser_expr_to_custom_expr(args[1])?),
        data_type: SqlType::Array(Box::new(SqlType::String)),
    }))
}

fn try_array_map_cast_string_literal(
    name: &str,
    args: &[&sqlparser::ast::Expr],
) -> Result<Option<Literal>, String> {
    if name != "array_map" && name != "transform" {
        return Ok(None);
    }
    if !array_map_cast_string_lambda_matches(args)? {
        return Ok(None);
    }
    let array_value = sqlparser_expr_to_literal(args[1])?;
    match array_value {
        Literal::Null => Ok(Some(Literal::Null)),
        Literal::Array(values) => values
            .into_iter()
            .map(|value| cast_literal(value, &SqlType::String))
            .collect::<Result<Vec<_>, _>>()
            .map(Literal::Array)
            .map(Some),
        other => Err(format!("array_map expects ARRAY input, got {other:?}")),
    }
}

fn array_map_cast_string_lambda_matches(args: &[&sqlparser::ast::Expr]) -> Result<bool, String> {
    if args.len() != 2 {
        return Ok(false);
    }
    let Some((param_name, body)) = parse_single_arrow_lambda(args[0]) else {
        return Ok(false);
    };
    lambda_body_casts_param_to_string(body, &param_name)
}

fn parse_single_arrow_lambda(
    expr: &sqlparser::ast::Expr,
) -> Option<(String, &sqlparser::ast::Expr)> {
    use sqlparser::ast as sqlast;
    match expr {
        sqlast::Expr::Lambda(lambda) => lambda
            .params
            .first()
            .map(|ident| (ident.value.to_lowercase(), lambda.body.as_ref())),
        sqlast::Expr::BinaryOp {
            left,
            op: sqlast::BinaryOperator::Arrow,
            right,
        } => parse_single_lambda_param(left).map(|param| (param, right.as_ref())),
        sqlast::Expr::Nested(inner) => parse_single_arrow_lambda(inner),
        _ => None,
    }
}

fn parse_single_lambda_param(expr: &sqlparser::ast::Expr) -> Option<String> {
    match expr {
        sqlparser::ast::Expr::Identifier(ident) => Some(ident.value.to_lowercase()),
        sqlparser::ast::Expr::Nested(inner) => parse_single_lambda_param(inner),
        _ => None,
    }
}

fn lambda_body_casts_param_to_string(
    expr: &sqlparser::ast::Expr,
    param_name: &str,
) -> Result<bool, String> {
    use sqlparser::ast as sqlast;
    match expr {
        sqlast::Expr::Nested(inner) => lambda_body_casts_param_to_string(inner, param_name),
        sqlast::Expr::Cast {
            expr: inner,
            data_type,
            ..
        } if lambda_expr_is_param(inner, param_name) => {
            let sql_type = crate::sql::parser::dialect::convert_sql_type(data_type.clone())?;
            Ok(matches!(sql_type, SqlType::String))
        }
        _ => Ok(false),
    }
}

fn lambda_expr_is_param(expr: &sqlparser::ast::Expr, param_name: &str) -> bool {
    match expr {
        sqlparser::ast::Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case(param_name),
        sqlparser::ast::Expr::Nested(inner) => lambda_expr_is_param(inner, param_name),
        _ => false,
    }
}

pub(crate) fn function_expr_args(
    args: &sqlparser::ast::FunctionArguments,
) -> Result<Vec<&sqlparser::ast::Expr>, String> {
    use sqlparser::ast as sqlast;

    match args {
        sqlast::FunctionArguments::None => Ok(Vec::new()),
        sqlast::FunctionArguments::List(list) => list
            .args
            .iter()
            .map(|arg| match arg {
                sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(expr)) => Ok(expr),
                other => Err(format!(
                    "unsupported function argument in INSERT VALUES: {other}"
                )),
            })
            .collect(),
        other => Err(format!(
            "unsupported function argument form in INSERT VALUES: {other}"
        )),
    }
}

/// Evaluate arithmetic on `Literal` values without `ManualValue`.
pub(crate) fn eval_literal_arithmetic(
    op: ArithmeticOp,
    left: &Literal,
    right: &Literal,
) -> Result<Literal, String> {
    if matches!(left, Literal::Null) || matches!(right, Literal::Null) {
        return Ok(Literal::Null);
    }
    match (left, right) {
        (Literal::Int(l), Literal::Int(r)) => match op {
            ArithmeticOp::Add => Ok(Literal::Int(l + r)),
            ArithmeticOp::Sub => Ok(Literal::Int(l - r)),
            ArithmeticOp::Mul => Ok(Literal::Int(l * r)),
            ArithmeticOp::Div => Ok(Literal::Float(*l as f64 / *r as f64)),
            ArithmeticOp::Mod => Ok(Literal::Int(l % r)),
        },
        (Literal::Int(l), Literal::Float(r)) => {
            eval_literal_arithmetic(op, &Literal::Float(*l as f64), &Literal::Float(*r))
        }
        (Literal::Float(l), Literal::Int(r)) => {
            eval_literal_arithmetic(op, &Literal::Float(*l), &Literal::Float(*r as f64))
        }
        (Literal::Float(l), Literal::Float(r)) => match op {
            ArithmeticOp::Add => Ok(Literal::Float(l + r)),
            ArithmeticOp::Sub => Ok(Literal::Float(l - r)),
            ArithmeticOp::Mul => Ok(Literal::Float(l * r)),
            ArithmeticOp::Div => Ok(Literal::Float(l / r)),
            ArithmeticOp::Mod => {
                Err("MOD only supports integer inputs in standalone mode".to_string())
            }
        },
        (l, r) => Err(format!(
            "standalone arithmetic does not support {:?} and {:?}",
            l, r
        )),
    }
}

/// Cast a `Literal` to the given SQL type without `ManualValue`.
pub(crate) fn cast_literal(
    value: Literal,
    data_type: &crate::sql::SqlType,
) -> Result<Literal, String> {
    use crate::sql::SqlType;
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
            Literal::Array(_) | Literal::Map(_) | Literal::Struct(_) => {
                Err("cannot cast complex literal to string".to_string())
            }
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
            Literal::Array(_) | Literal::Map(_) | Literal::Struct(_) => {
                Err("cannot cast complex literal to binary".to_string())
            }
        },
        SqlType::Int | SqlType::BigInt | SqlType::TinyInt | SqlType::SmallInt => match &value {
            Literal::Null => Ok(Literal::Null),
            Literal::Int(_) => Ok(value),
            Literal::Float(v) => Ok(Literal::Int(*v as i64)),
            other => Err(format!("cannot cast {:?} to integer", other)),
        },
        SqlType::Float | SqlType::Double => match &value {
            Literal::Null => Ok(Literal::Null),
            Literal::Int(v) => Ok(Literal::Float(*v as f64)),
            Literal::Float(_) => Ok(value),
            other => Err(format!("cannot cast {:?} to floating point", other)),
        },
        SqlType::Array(inner) => match value {
            Literal::Null => Ok(Literal::Null),
            Literal::Array(values) => values
                .into_iter()
                .map(|item| cast_literal(item, inner))
                .collect::<Result<Vec<_>, _>>()
                .map(Literal::Array),
            other => Err(format!("cannot cast {:?} to array", other)),
        },
        other => Err(format!(
            "standalone generate_series does not support CAST to {:?}",
            other
        )),
    }
}

pub(crate) fn eval_array_generate_literal(args: &[Literal]) -> Result<Literal, String> {
    if args.is_empty() || args.len() > 3 {
        return Err("array_generate expects 1 to 3 numeric arguments".to_string());
    }
    // SQL NULL propagation: if any argument is NULL, the whole call is NULL.
    if args.iter().any(|a| matches!(a, Literal::Null)) {
        return Ok(Literal::Null);
    }
    let literal_to_i64 = |value: &Literal| match value {
        Literal::Int(v) => Ok(*v),
        other => Err(format!(
            "array_generate expects integer arguments, got {other:?}"
        )),
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
        return Err("array_generate step must not be zero".to_string());
    }

    let mut values = Vec::new();
    let mut current = start;
    if step > 0 {
        while current <= stop {
            values.push(Literal::Int(current));
            current = current
                .checked_add(step)
                .ok_or_else(|| "array_generate value overflow".to_string())?;
        }
    } else {
        while current >= stop {
            values.push(Literal::Int(current));
            current = current
                .checked_add(step)
                .ok_or_else(|| "array_generate value overflow".to_string())?;
        }
    }
    Ok(Literal::Array(values))
}

// ---------------------------------------------------------------------------
// SELECT without FROM helpers
// ---------------------------------------------------------------------------

/// Check if a query is a SELECT without any FROM clause.
pub(crate) fn is_select_without_from(query: &sqlparser::ast::Query) -> bool {
    if let sqlparser::ast::SetExpr::Select(ref select) = *query.body {
        select.from.is_empty()
    } else {
        false
    }
}

/// Evaluate a constant SELECT expression (no FROM) and return a single-row result.
pub(crate) fn evaluate_constant_select(
    query: &sqlparser::ast::Query,
) -> Result<QueryResult, String> {
    use sqlparser::ast as sqlast;

    let select = match query.body.as_ref() {
        sqlast::SetExpr::Select(s) => s.as_ref(),
        _ => return Err("only simple SELECT is supported for constant evaluation".into()),
    };

    let mut columns = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();

    for (idx, item) in select.projection.iter().enumerate() {
        match item {
            sqlast::SelectItem::UnnamedExpr(expr) => {
                let (col_name, array) = evaluate_const_expr(expr, idx)?;
                columns.push(QueryResultColumn {
                    name: col_name,
                    data_type: array.data_type().clone(),
                    nullable: true,
                    logical_type: None,
                });
                arrays.push(array);
            }
            sqlast::SelectItem::ExprWithAlias { expr, alias } => {
                let (_, array) = evaluate_const_expr(expr, idx)?;
                columns.push(QueryResultColumn {
                    name: alias.value.clone(),
                    data_type: array.data_type().clone(),
                    nullable: true,
                    logical_type: None,
                });
                arrays.push(array);
            }
            other => {
                return Err(format!(
                    "unsupported projection item in constant SELECT: {:?}",
                    other
                ));
            }
        }
    }

    let fields: Vec<Field> = columns
        .iter()
        .map(|c| Field::new(&c.name, c.data_type.clone(), c.nullable))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, arrays)
        .map_err(|e| format!("build constant SELECT batch failed: {e}"))?;
    let chunk = record_batch_to_chunk(batch)?;
    Ok(QueryResult {
        columns,
        chunks: vec![chunk],
    })
}

/// Evaluate a constant expression and return (column_name, single-element array).
pub(crate) fn evaluate_const_expr(
    expr: &sqlparser::ast::Expr,
    idx: usize,
) -> Result<(String, ArrayRef), String> {
    use arrow::array::*;
    use sqlparser::ast as sqlast;

    match expr {
        sqlast::Expr::Value(value_with_span) => match &value_with_span.value {
            sqlast::Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok((n.clone(), Arc::new(Int64Array::from(vec![i])) as ArrayRef))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok((n.clone(), Arc::new(Float64Array::from(vec![f])) as ArrayRef))
                } else {
                    Err(format!("cannot parse number literal `{n}`"))
                }
            }
            sqlast::Value::SingleQuotedString(s) | sqlast::Value::DoubleQuotedString(s) => Ok((
                s.clone(),
                Arc::new(StringArray::from(vec![s.as_str()])) as ArrayRef,
            )),
            sqlast::Value::Boolean(b) => Ok((
                b.to_string(),
                Arc::new(BooleanArray::from(vec![*b])) as ArrayRef,
            )),
            sqlast::Value::Null => Ok((
                "NULL".to_string(),
                Arc::new(arrow::array::NullArray::new(1)) as ArrayRef,
            )),
            other => Err(format!("unsupported constant value: {:?}", other)),
        },
        sqlast::Expr::BinaryOp { left, op, right } => {
            let (_, left_arr) = evaluate_const_expr(left, idx)?;
            let (_, right_arr) = evaluate_const_expr(right, idx)?;
            let left_val = extract_numeric_scalar(&left_arr)?;
            let right_val = extract_numeric_scalar(&right_arr)?;
            let result = match op {
                sqlast::BinaryOperator::Plus => left_val + right_val,
                sqlast::BinaryOperator::Minus => left_val - right_val,
                sqlast::BinaryOperator::Multiply => left_val * right_val,
                sqlast::BinaryOperator::Divide => {
                    if right_val == 0.0 {
                        return Err("division by zero".to_string());
                    }
                    left_val / right_val
                }
                sqlast::BinaryOperator::Modulo => left_val % right_val,
                other => return Err(format!("unsupported binary operator: {:?}", other)),
            };
            // Return as int if both inputs were int and result is whole
            if left_arr.data_type() == &DataType::Int64
                && right_arr.data_type() == &DataType::Int64
                && result.fract() == 0.0
                && !matches!(op, sqlast::BinaryOperator::Divide)
            {
                Ok((
                    format!("_col{idx}"),
                    Arc::new(Int64Array::from(vec![result as i64])) as ArrayRef,
                ))
            } else {
                Ok((
                    format!("_col{idx}"),
                    Arc::new(Float64Array::from(vec![result])) as ArrayRef,
                ))
            }
        }
        sqlast::Expr::UnaryOp {
            op: sqlast::UnaryOperator::Minus,
            expr: inner,
        } => {
            let (_, arr) = evaluate_const_expr(inner, idx)?;
            let val = extract_numeric_scalar(&arr)?;
            if arr.data_type() == &DataType::Int64 {
                Ok((
                    format!("_col{idx}"),
                    Arc::new(Int64Array::from(vec![(-val) as i64])) as ArrayRef,
                ))
            } else {
                Ok((
                    format!("_col{idx}"),
                    Arc::new(Float64Array::from(vec![-val])) as ArrayRef,
                ))
            }
        }
        sqlast::Expr::Nested(inner) => evaluate_const_expr(inner, idx),
        other => Err(format!(
            "unsupported expression in constant SELECT: {:?}",
            other
        )),
    }
}

/// Extract a numeric scalar value from a single-element array.
pub(crate) fn extract_numeric_scalar(arr: &ArrayRef) -> Result<f64, String> {
    use arrow::array::*;
    match arr.data_type() {
        DataType::Int64 => {
            let a = arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("downcast Int64Array")?;
            Ok(a.value(0) as f64)
        }
        DataType::Float64 => {
            let a = arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or("downcast Float64Array")?;
            Ok(a.value(0))
        }
        other => Err(format!("cannot extract numeric from {:?}", other)),
    }
}

// ---------------------------------------------------------------------------
// Local parquet table helpers
// ---------------------------------------------------------------------------

/// Convert a SQL type to an Arrow DataType.
pub(crate) fn sql_type_to_arrow_type(sql_type: &SqlType) -> Result<DataType, String> {
    match sql_type {
        SqlType::TinyInt => Ok(DataType::Int8),
        SqlType::SmallInt => Ok(DataType::Int16),
        SqlType::Int => Ok(DataType::Int32),
        SqlType::BigInt => Ok(DataType::Int64),
        SqlType::LargeInt => Ok(DataType::FixedSizeBinary(
            crate::common::largeint::LARGEINT_BYTE_WIDTH,
        )),
        SqlType::Float => Ok(DataType::Float32),
        SqlType::Double => Ok(DataType::Float64),
        SqlType::String | SqlType::Json => Ok(DataType::Utf8),
        SqlType::Binary | SqlType::Bitmap | SqlType::Hll => Ok(DataType::Binary),
        SqlType::Boolean => Ok(DataType::Boolean),
        SqlType::Date => Ok(DataType::Date32),
        SqlType::DateTime => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        SqlType::DateTimeNs => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        SqlType::Time => Ok(DataType::Time64(TimeUnit::Microsecond)),
        SqlType::Decimal { precision, scale } => Ok(DataType::Decimal128(*precision, *scale)),
        SqlType::Array(inner) => {
            let inner_type = sql_type_to_arrow_type(inner)?;
            Ok(DataType::List(Arc::new(Field::new(
                "item", inner_type, true,
            ))))
        }
        SqlType::Map(key, value) => {
            let key_type = sql_type_to_arrow_type(key)?;
            let value_type = sql_type_to_arrow_type(value)?;
            let entries = DataType::Struct(
                vec![
                    Arc::new(Field::new("key", key_type, true)),
                    Arc::new(Field::new("value", value_type, true)),
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
                    Ok(Arc::new(Field::new(
                        name,
                        sql_type_to_arrow_type(data_type)?,
                        true,
                    )))
                })
                .collect::<Result<Vec<_>, String>>()?
                .into(),
        )),
        SqlType::Variant => Ok(DataType::LargeBinary),
    }
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
pub(crate) fn arrow_type_equals_ignoring_metadata(a: &DataType, b: &DataType) -> bool {
    use DataType::*;
    match (a, b) {
        (List(a), List(b))
        | (LargeList(a), LargeList(b))
        | (ListView(a), ListView(b))
        | (LargeListView(a), LargeListView(b)) => {
            arrow_type_equals_ignoring_metadata(a.data_type(), b.data_type())
        }
        (FixedSizeList(a, a_size), FixedSizeList(b, b_size)) => {
            a_size == b_size && arrow_type_equals_ignoring_metadata(a.data_type(), b.data_type())
        }
        (Struct(a), Struct(b)) => {
            a.len() == b.len()
                && a.iter().zip(b.iter()).all(|(af, bf)| {
                    arrow_type_equals_ignoring_metadata(af.data_type(), bf.data_type())
                })
        }
        (Map(a_field, a_sorted), Map(b_field, b_sorted)) => {
            a_sorted == b_sorted
                && arrow_type_equals_ignoring_metadata(a_field.data_type(), b_field.data_type())
        }
        (Dictionary(a_key, a_value), Dictionary(b_key, b_value)) => {
            arrow_type_equals_ignoring_metadata(a_key, b_key)
                && arrow_type_equals_ignoring_metadata(a_value, b_value)
        }
        (RunEndEncoded(a_run_ends, a_values), RunEndEncoded(b_run_ends, b_values)) => {
            arrow_type_equals_ignoring_metadata(a_run_ends.data_type(), b_run_ends.data_type())
                && arrow_type_equals_ignoring_metadata(a_values.data_type(), b_values.data_type())
        }
        (Union(a_fields, a_mode), Union(b_fields, b_mode)) => {
            a_mode == b_mode
                && a_fields.len() == b_fields.len()
                && a_fields.iter().all(|(a_tag, a_field)| {
                    b_fields.iter().any(|(b_tag, b_field)| {
                        a_tag == b_tag
                            && arrow_type_equals_ignoring_metadata(
                                a_field.data_type(),
                                b_field.data_type(),
                            )
                    })
                })
        }
        _ => a == b,
    }
}

pub(crate) fn compare_literals(
    left: &Literal,
    right: &Literal,
) -> Result<std::cmp::Ordering, String> {
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

/// Hashable key derived from `Literal` for use in aggregate-table dedup maps.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum LiteralKey {
    Null,
    Bool(bool),
    Int(i64),
    Float(u64),
    String(String),
}

pub(crate) fn literal_to_key(literal: &Literal) -> LiteralKey {
    match literal {
        Literal::Null => LiteralKey::Null,
        Literal::Bool(v) => LiteralKey::Bool(*v),
        Literal::Int(v) => LiteralKey::Int(*v),
        Literal::Float(v) => LiteralKey::Float(v.to_bits()),
        Literal::String(v) | Literal::Date(v) => LiteralKey::String(v.clone()),
        Literal::Array(values) => {
            // Flatten to a string representation for hashing
            let s = values
                .iter()
                .map(|v| format!("{:?}", v))
                .collect::<Vec<_>>()
                .join(",");
            LiteralKey::String(s)
        }
        Literal::Map(entries) => LiteralKey::String(format!("{entries:?}")),
        Literal::Struct(values) => LiteralKey::String(format!("{values:?}")),
    }
}

/// Extract a `Literal` from a batch column at a specific row.
pub(crate) fn literal_from_batch(column: &ArrayRef, row_idx: usize) -> Result<Literal, String> {
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

pub(crate) fn parse_kv_properties(
    parser: &mut sqlparser::parser::Parser<'_>,
) -> Result<Vec<(String, String)>, String> {
    use sqlparser::tokenizer::Token;

    let mut props = Vec::new();
    if !parser.consume_token(&Token::LParen) {
        return Ok(props);
    }
    loop {
        if parser.consume_token(&Token::RParen) {
            break;
        }
        if !props.is_empty() {
            let _ = parser.consume_token(&Token::Comma);
            if parser.consume_token(&Token::RParen) {
                break;
            }
        }
        let key = parse_prop_string_or_ident(parser)?;
        let _ = parser.consume_token(&Token::Eq);
        let value = parse_prop_string_or_ident(parser)?;
        props.push((key, value));
    }
    Ok(props)
}

pub(crate) fn parse_prop_string_or_ident(
    parser: &mut sqlparser::parser::Parser<'_>,
) -> Result<String, String> {
    use sqlparser::tokenizer::Token;
    let token = parser.next_token();
    match token.token {
        Token::SingleQuotedString(s) | Token::DoubleQuotedString(s) => Ok(s),
        Token::Word(w) => Ok(w.value),
        Token::Number(n, _) => Ok(n),
        other => Err(format!("expected string or identifier, got {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::ast::{Expr, Literal};
    use crate::sql::parser::dialect::StarRocksDialect;

    fn parse_expr(sql: &str) -> sqlparser::ast::Expr {
        let mut parser = sqlparser::parser::Parser::new(&StarRocksDialect)
            .try_with_sql(sql)
            .expect("build parser");
        parser.parse_expr().expect("parse expression")
    }

    #[test]
    fn scalar_function_falls_back_when_literal_fold_fails() {
        // `concat` is not a constant-foldable function in `sqlparser_function_to_literal`,
        // so we expect a ScalarFunction node preserving the nested column ref and the
        // CAST around it.
        let raw = parse_expr("concat('value_', CAST(generate_series AS VARCHAR))");
        let converted = sqlparser_expr_to_custom_expr(&raw).expect("convert");
        match converted {
            Expr::ScalarFunction(func) => {
                assert_eq!(func.name, "concat");
                assert_eq!(func.args.len(), 2);
                assert!(
                    matches!(func.args[0], Expr::Literal(Literal::String(ref s)) if s == "value_")
                );
                assert!(matches!(func.args[1], Expr::Cast { .. }));
            }
            other => panic!("expected ScalarFunction, got {:?}", other),
        }
    }

    #[test]
    fn to_binary_with_column_ref_lowers_to_nested_scalar_function() {
        // The outer to_binary cannot literal-fold because the inner concat references
        // `generate_series`; expect nested ScalarFunction(to_binary -> ScalarFunction(concat)).
        let raw =
            parse_expr("to_binary(concat('value_', CAST(generate_series AS VARCHAR)), 'utf8')");
        let converted = sqlparser_expr_to_custom_expr(&raw).expect("convert");
        let Expr::ScalarFunction(outer) = converted else {
            panic!("expected outer ScalarFunction");
        };
        assert_eq!(outer.name, "to_binary");
        assert_eq!(outer.args.len(), 2);
        assert!(matches!(outer.args[0], Expr::ScalarFunction(ref f) if f.name == "concat"));
        assert!(matches!(outer.args[1], Expr::Literal(Literal::String(ref s)) if s == "utf8"));
    }

    #[test]
    fn constant_function_call_folds_to_literal() {
        // `row(100, 100)` and `map(1, 5.5)` should constant-fold through
        // `sqlparser_function_to_literal` when used as SELECT projections.
        let row = sqlparser_expr_to_custom_expr(&parse_expr("row(100, 100)")).expect("row");
        assert!(matches!(row, Expr::Literal(Literal::Struct(ref v)) if v.len() == 2));

        let map = sqlparser_expr_to_custom_expr(&parse_expr("map(1, 5.5)")).expect("map");
        assert!(matches!(map, Expr::Literal(Literal::Map(ref v)) if v.len() == 1));
    }

    #[test]
    fn constant_array_repeat_folds_to_array_literal() {
        let arr =
            sqlparser_expr_to_custom_expr(&parse_expr("array_repeat('abc', 3)")).expect("array");
        assert!(matches!(
            arr,
            Expr::Literal(Literal::Array(ref values))
                if values == &vec![
                    Literal::String("abc".to_string()),
                    Literal::String("abc".to_string()),
                    Literal::String("abc".to_string())
                ]
        ));
    }

    #[test]
    fn constant_named_struct_folds_values_positionally() {
        let value = sqlparser_expr_to_custom_expr(&parse_expr("named_struct('A', 1, 'B', 'x')"))
            .expect("named_struct");
        assert!(matches!(
            value,
            Expr::Literal(Literal::Struct(ref values))
                if values == &vec![Literal::Int(1), Literal::String("x".to_string())]
        ));
    }

    #[test]
    fn constant_array_append_folds_to_array_literal() {
        let value =
            sqlparser_expr_to_custom_expr(&parse_expr("array_append(array_generate(3), NULL)"))
                .expect("array_append");
        assert!(matches!(
            value,
            Expr::Literal(Literal::Array(ref values))
                if values
                    == &vec![
                        Literal::Int(1),
                        Literal::Int(2),
                        Literal::Int(3),
                        Literal::Null
                    ]
        ));
    }

    #[test]
    fn constant_md5sum_casts_scalar_to_varchar() {
        let value =
            sqlparser_expr_to_custom_expr(&parse_expr("md5sum(10000)")).expect("md5sum fold");
        let Expr::Literal(Literal::String(actual)) = value else {
            panic!("expected folded md5sum string literal");
        };

        use md5::{Digest, Md5};
        let mut hasher = Md5::new();
        hasher.update(b"10000");
        assert_eq!(actual, hex::encode(hasher.finalize()));
    }

    #[test]
    fn array_literal_lowers_to_array_expr() {
        let arr = sqlparser_expr_to_custom_expr(&parse_expr("[1, 2, 3]")).expect("array");
        let Expr::Array(items) = arr else {
            panic!("expected Expr::Array");
        };
        assert_eq!(items.len(), 3);
        assert!(matches!(items[0], Expr::Literal(Literal::Int(1))));
        assert!(matches!(items[2], Expr::Literal(Literal::Int(3))));
    }

    #[test]
    fn array_literal_preserves_column_ref_elements() {
        let arr = sqlparser_expr_to_custom_expr(&parse_expr("[generate_series]")).expect("array");
        let Expr::Array(items) = arr else {
            panic!("expected Expr::Array");
        };
        assert!(matches!(items[0], Expr::Column(ref c) if c.name == "generate_series"));
    }

    #[test]
    fn parse_json_folds_to_variant_bytes_via_latin1_string() {
        // sqlparser builds a Function node for `parse_json('{"a":1}')`.
        let raw = parse_expr(r#"parse_json('{"a":1}')"#);
        let sqlparser::ast::Expr::Function(ref func) = raw else {
            panic!("expected Function node, got {raw:?}");
        };

        let lit = sqlparser_function_to_literal(func).expect("parse_json fold");
        let Literal::String(packed) = lit else {
            panic!("expected Literal::String");
        };
        let unpacked = latin1_string_to_bytes(&packed).expect("latin1 decode");

        // Must equal the encoder's output for the same JSON.
        let expected = crate::exec::variant_encode::encode_json_text_to_variant_bytes(r#"{"a":1}"#)
            .expect("encode");
        assert_eq!(unpacked, expected);
    }

    #[test]
    fn parse_json_rejects_invalid_argument_count() {
        let raw = parse_expr(r#"parse_json('{"a":1}', 'extra')"#);
        let sqlparser::ast::Expr::Function(ref func) = raw else {
            panic!("expected Function node");
        };
        let err = sqlparser_function_to_literal(func).expect_err("must fail");
        assert!(
            err.contains("parse_json expects 1 argument"),
            "unexpected error: {err}"
        );
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
            let sqlparser::ast::Expr::Function(ref func) = raw else {
                panic!("expected Function node for `{sql}`");
            };
            let err =
                sqlparser_function_to_literal(func).expect_err(&format!("must reject `{sql}`"));
            assert!(
                err.contains("hll_hash with narrowing CAST"),
                "unexpected error for `{sql}`: {err}"
            );
        }
    }

    #[test]
    fn hll_hash_const_fold_accepts_bigint_cast() {
        // CAST to BIGINT is the runtime path's native width for integer
        // literals, so it must continue to fold cleanly.
        let raw = parse_expr("hll_hash(CAST(5 AS BIGINT))");
        let sqlparser::ast::Expr::Function(ref func) = raw else {
            panic!("expected Function node");
        };
        let lit = sqlparser_function_to_literal(func).expect("BIGINT cast must fold");
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
            let result = sqlparser_expr_to_literal(&expr);
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
            let result = sqlparser_expr_to_literal(&expr)
                .unwrap_or_else(|e| panic!("Expected Ok for `{sql}` but got Err: {e}"));
            assert_eq!(
                result, *expected,
                "CAST to non-DECIMAL `{sql}` folded to wrong literal"
            );
        }
    }
}
