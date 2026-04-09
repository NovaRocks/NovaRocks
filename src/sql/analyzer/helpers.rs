use arrow::datatypes::DataType;
use sqlparser::ast as sqlast;

use crate::sql::ir::JoinKind;

// ---------------------------------------------------------------------------
// SQL type -> Arrow type conversion
// ---------------------------------------------------------------------------

pub(super) fn sql_type_to_arrow(sql_type: &sqlast::DataType) -> Result<DataType, String> {
    match sql_type {
        sqlast::DataType::TinyInt(_) => Ok(DataType::Int8),
        sqlast::DataType::SmallInt(_) => Ok(DataType::Int16),
        sqlast::DataType::Int(_) | sqlast::DataType::Integer(_) => Ok(DataType::Int32),
        sqlast::DataType::BigInt(_) => Ok(DataType::Int64),
        sqlast::DataType::Float(_) => Ok(DataType::Float32),
        sqlast::DataType::Double(_) | sqlast::DataType::DoublePrecision => Ok(DataType::Float64),
        sqlast::DataType::Boolean => Ok(DataType::Boolean),
        sqlast::DataType::Varchar(_)
        | sqlast::DataType::CharVarying(_)
        | sqlast::DataType::Text => Ok(DataType::Utf8),
        sqlast::DataType::Char(_)
        | sqlast::DataType::Character(_)
        | sqlast::DataType::String(_) => Ok(DataType::Utf8),
        sqlast::DataType::Date => Ok(DataType::Date32),
        sqlast::DataType::Datetime(_) | sqlast::DataType::Timestamp(_, _) => Ok(
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        ),
        sqlast::DataType::Time(_, _) => {
            Ok(DataType::Time64(arrow::datatypes::TimeUnit::Microsecond))
        }
        sqlast::DataType::Decimal(info)
        | sqlast::DataType::Dec(info)
        | sqlast::DataType::Numeric(info) => match info {
            sqlast::ExactNumberInfo::PrecisionAndScale(p, s) => {
                Ok(DataType::Decimal128(*p as u8, *s as i8))
            }
            sqlast::ExactNumberInfo::Precision(p) => Ok(DataType::Decimal128(*p as u8, 0)),
            sqlast::ExactNumberInfo::None => Ok(DataType::Decimal128(38, 0)),
        },
        sqlast::DataType::Custom(name, _) => {
            let type_name = name.to_string().to_lowercase();
            match type_name.as_str() {
                "string" => Ok(DataType::Utf8),
                "largeint" => Ok(DataType::Int64),
                "json" | "jsonb" => Ok(DataType::Utf8),
                _ => Err(format!("unsupported SQL type: {name}")),
            }
        }
        other => Err(format!("unsupported CAST target type: {other:?}")),
    }
}

// ---------------------------------------------------------------------------
// Expression display name
// ---------------------------------------------------------------------------

pub(super) fn expr_display_name(expr: &sqlast::Expr) -> String {
    match expr {
        // Strip outer parentheses: `(col)` → display name of `col`.
        // This matches how `SELECT distinct(col)` is parsed: DISTINCT is
        // the SELECT modifier and `(col)` is a Nested expression.
        sqlast::Expr::Nested(inner) => expr_display_name(inner),
        sqlast::Expr::CompoundIdentifier(parts) if parts.len() >= 2 => parts
            .last()
            .map(|i| i.value.clone())
            .unwrap_or_else(|| format!("{expr}")),
        sqlast::Expr::Identifier(ident) => ident.value.clone(),
        sqlast::Expr::Function(f) => {
            // Lowercase function name to match StarRocks FE behavior.
            // sqlparser's Display may uppercase SQL keywords, so we
            // replace the function name portion with the lowercased form.
            let display = format!("{expr}");
            let func_name = f.name.to_string();
            let func_lower = func_name.to_lowercase();
            // Case-insensitive find and replace the function name prefix.
            if let Some(pos) = display.to_lowercase().find(&func_lower) {
                let mut result = String::with_capacity(display.len());
                result.push_str(&display[..pos]);
                result.push_str(&func_lower);
                result.push_str(&display[pos + func_name.len()..]);
                result
            } else {
                display
            }
        }
        // CAST: uppercase keyword, StarRocks-style type names (DECIMAL64/DECIMAL128),
        // wrap inner with parentheses if it's not a simple identifier or literal.
        sqlast::Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            let inner_str = expr_display_name_with_parens(inner);
            let type_str = format_cast_type(data_type);
            format!("CAST({inner_str} AS {type_str})")
        }
        // Binary ops: wrap each operand with parentheses unless it's a simple
        // identifier or literal, matching StarRocks AST2StringVisitor behavior.
        sqlast::Expr::BinaryOp { left, op, right } => {
            let left_str = expr_display_name_with_parens(left);
            let right_str = expr_display_name_with_parens(right);
            format!("{left_str} {op} {right_str}")
        }
        // Expressions like SUBSTR, EXTRACT are rendered in uppercase by
        // sqlparser's Display. Lowercase leading keyword to match StarRocks FE.
        other => {
            let s = format!("{other}");
            // Lowercase leading keyword (up to the first '(') if present.
            if let Some(paren) = s.find('(') {
                let prefix = &s[..paren];
                // Only lowercase if the prefix is all-ASCII-alpha (a keyword).
                if !prefix.is_empty() && prefix.chars().all(|c| c.is_ascii_alphabetic()) {
                    format!("{}{}", prefix.to_lowercase(), &s[paren..])
                } else {
                    s
                }
            } else {
                s
            }
        }
    }
}

/// Wraps `expr_display_name(expr)` in parentheses unless the expression is
/// a simple identifier or literal — matching StarRocks `printWithParentheses`.
fn expr_display_name_with_parens(expr: &sqlast::Expr) -> String {
    match expr {
        sqlast::Expr::Identifier(_) | sqlast::Expr::CompoundIdentifier(_) => {
            expr_display_name(expr)
        }
        sqlast::Expr::Value(_) => expr_display_name(expr),
        sqlast::Expr::Nested(inner) => expr_display_name_with_parens(inner),
        _ => format!("({})", expr_display_name(expr)),
    }
}

/// Format a CAST target type using StarRocks-style names.
/// DECIMAL(p,s) is promoted to DECIMAL32/DECIMAL64/DECIMAL128 to match
/// the analyzed type name that StarRocks FE emits in column aliases.
fn format_cast_type(data_type: &sqlast::DataType) -> String {
    match data_type {
        sqlast::DataType::Decimal(info)
        | sqlast::DataType::Dec(info)
        | sqlast::DataType::Numeric(info) => match info {
            sqlast::ExactNumberInfo::PrecisionAndScale(p, s) => {
                let kind = decimal_kind(*p);
                format!("{kind}({p},{s})")
            }
            sqlast::ExactNumberInfo::Precision(p) => {
                let kind = decimal_kind(*p);
                format!("{kind}({p},0)")
            }
            sqlast::ExactNumberInfo::None => "DECIMAL128(38,0)".to_string(),
        },
        other => format!("{other}"),
    }
}

fn decimal_kind(precision: u64) -> &'static str {
    if precision <= 9 {
        "DECIMAL32"
    } else if precision <= 18 {
        "DECIMAL64"
    } else {
        "DECIMAL128"
    }
}

// ---------------------------------------------------------------------------
// JOIN operator parsing
// ---------------------------------------------------------------------------

pub(super) fn parse_join_operator(
    op: &sqlast::JoinOperator,
) -> Result<(JoinKind, Option<&sqlast::JoinConstraint>), String> {
    match op {
        sqlast::JoinOperator::Join(c) | sqlast::JoinOperator::Inner(c) => {
            Ok((JoinKind::Inner, Some(c)))
        }
        sqlast::JoinOperator::Left(c) | sqlast::JoinOperator::LeftOuter(c) => {
            Ok((JoinKind::LeftOuter, Some(c)))
        }
        sqlast::JoinOperator::Right(c) | sqlast::JoinOperator::RightOuter(c) => {
            Ok((JoinKind::RightOuter, Some(c)))
        }
        sqlast::JoinOperator::FullOuter(c) => Ok((JoinKind::FullOuter, Some(c))),
        sqlast::JoinOperator::CrossJoin(_) => Ok((JoinKind::Cross, None)),
        sqlast::JoinOperator::LeftSemi(c) => Ok((JoinKind::LeftSemi, Some(c))),
        sqlast::JoinOperator::RightSemi(c) => Ok((JoinKind::RightSemi, Some(c))),
        sqlast::JoinOperator::LeftAnti(c) => Ok((JoinKind::LeftAnti, Some(c))),
        sqlast::JoinOperator::RightAnti(c) => Ok((JoinKind::RightAnti, Some(c))),
        other => Err(format!("unsupported join type: {other:?}")),
    }
}

// ---------------------------------------------------------------------------
// LIMIT / OFFSET extraction
// ---------------------------------------------------------------------------

pub(super) fn extract_limit(query: &sqlast::Query) -> Result<Option<i64>, String> {
    match &query.limit_clause {
        Some(sqlast::LimitClause::LimitOffset {
            limit:
                Some(sqlast::Expr::Value(sqlast::ValueWithSpan {
                    value: sqlast::Value::Number(n, _),
                    ..
                })),
            ..
        }) => n
            .parse::<i64>()
            .map(Some)
            .map_err(|e| format!("invalid LIMIT value: {e}")),
        Some(sqlast::LimitClause::LimitOffset { limit: None, .. }) => Ok(None),
        Some(sqlast::LimitClause::LimitOffset { .. }) => {
            Err("only constant LIMIT is supported".into())
        }
        Some(sqlast::LimitClause::OffsetCommaLimit {
            limit:
                sqlast::Expr::Value(sqlast::ValueWithSpan {
                    value: sqlast::Value::Number(n, _),
                    ..
                }),
            ..
        }) => n
            .parse::<i64>()
            .map(Some)
            .map_err(|e| format!("invalid LIMIT value: {e}")),
        Some(sqlast::LimitClause::OffsetCommaLimit { .. }) => {
            Err("only constant LIMIT is supported".into())
        }
        None => Ok(None),
    }
}

pub(super) fn extract_offset(query: &sqlast::Query) -> Result<Option<i64>, String> {
    match &query.limit_clause {
        Some(sqlast::LimitClause::LimitOffset {
            offset:
                Some(sqlast::Offset {
                    value:
                        sqlast::Expr::Value(sqlast::ValueWithSpan {
                            value: sqlast::Value::Number(n, _),
                            ..
                        }),
                    ..
                }),
            ..
        }) => n
            .parse::<i64>()
            .map(Some)
            .map_err(|e| format!("invalid OFFSET value: {e}")),
        Some(sqlast::LimitClause::LimitOffset { offset: None, .. }) => Ok(None),
        Some(sqlast::LimitClause::LimitOffset { .. }) => {
            Err("only constant OFFSET is supported".into())
        }
        Some(sqlast::LimitClause::OffsetCommaLimit {
            offset:
                sqlast::Expr::Value(sqlast::ValueWithSpan {
                    value: sqlast::Value::Number(n, _),
                    ..
                }),
            ..
        }) => n
            .parse::<i64>()
            .map(Some)
            .map_err(|e| format!("invalid OFFSET value: {e}")),
        Some(sqlast::LimitClause::OffsetCommaLimit { .. }) => {
            Err("only constant OFFSET is supported".into())
        }
        None => Ok(None),
    }
}

/// Evaluate a constant integer expression (literals and simple arithmetic).
pub(super) fn eval_const_i64(expr: &sqlast::Expr) -> Result<i64, String> {
    match expr {
        sqlast::Expr::Value(v) => match &v.value {
            sqlast::Value::Number(n, _) => n
                .parse::<i64>()
                .map_err(|e| format!("cannot parse integer literal `{n}`: {e}")),
            _ => Err(format!("expected integer literal, got: {v}")),
        },
        sqlast::Expr::UnaryOp {
            op: sqlast::UnaryOperator::Minus,
            expr: inner,
        } => Ok(-eval_const_i64(inner)?),
        sqlast::Expr::BinaryOp { left, op, right } => {
            let l = eval_const_i64(left)?;
            let r = eval_const_i64(right)?;
            match op {
                sqlast::BinaryOperator::Plus => Ok(l + r),
                sqlast::BinaryOperator::Minus => Ok(l - r),
                sqlast::BinaryOperator::Multiply => Ok(l * r),
                sqlast::BinaryOperator::Divide if r != 0 => Ok(l / r),
                sqlast::BinaryOperator::Modulo if r != 0 => Ok(l % r),
                _ => Err(format!("unsupported operator in constant expression: {op}")),
            }
        }
        sqlast::Expr::Nested(inner) => eval_const_i64(inner),
        _ => Err(format!("expected constant integer expression, got: {expr}")),
    }
}
