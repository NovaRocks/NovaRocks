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

use crate::runtime::query_result::{QueryResult, QueryResultColumn};
use crate::sql::parser::ast::{ArithmeticOp, Expr, Literal, SqlType};
use crate::standalone::engine::record_batch_to_chunk;

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
        // An array literal like `[1, 2, 3]` has no natural non-constant lowering
        // in this path (we don't run through the pipeline scalar-eval machinery
        // here), so fold it to a Literal::Array if every element is itself a
        // literal-convertible expression; otherwise fail fast with a clear error.
        sqlast::Expr::Array(_) => Ok(Expr::Literal(sqlparser_expr_to_literal(expr)?)),
        // Function calls: try constant-folding via the INSERT-VALUES literal
        // helper first (covers `row(...)`, `map(...)`, fully-constant
        // `to_binary(...)`, etc.). If folding fails (e.g. args reference a
        // column), fall back to a ScalarFunction node that the row-wise
        // evaluator can dispatch on.
        sqlast::Expr::Function(func) => {
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
        // Handle CAST(expr AS type) — evaluate inner and convert to string
        sqlast::Expr::Cast { expr: inner, .. } => sqlparser_expr_to_literal(inner),
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
            if v.fract() != 0.0 {
                return Err(format!(
                    "literal {:?} is not an integral value for {type_name}",
                    literal
                ));
            }
            if *v < i128::MIN as f64 || *v > i128::MAX as f64 {
                return Err(format!(
                    "literal {:?} is out of range for {type_name}",
                    literal
                ));
            }
            Ok(Some(*v as i128))
        }
        Literal::String(s) => s
            .trim()
            .parse::<i128>()
            .map(Some)
            .map_err(|_| format!("literal `{s}` is not valid for {type_name}")),
        other => Err(format!("literal {:?} is not valid for {type_name}", other)),
    }
}

pub(crate) fn sqlparser_function_to_literal(
    func: &sqlparser::ast::Function,
) -> Result<Literal, String> {
    use sqlparser::ast as sqlast;

    let args = function_expr_args(&func.args)?;
    let name = func.name.to_string().to_ascii_lowercase();
    match name.as_str() {
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
        "row" => Ok(Literal::Struct(
            args.into_iter()
                .map(sqlparser_expr_to_literal)
                .collect::<Result<Vec<_>, _>>()?,
        )),
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
        SqlType::String => match &value {
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
        SqlType::Binary => match &value {
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
        other => Err(format!(
            "standalone generate_series does not support CAST to {:?}",
            other
        )),
    }
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
        SqlType::String => Ok(DataType::Utf8),
        SqlType::Binary => Ok(DataType::Binary),
        SqlType::Boolean => Ok(DataType::Boolean),
        SqlType::Date => Ok(DataType::Date32),
        SqlType::DateTime => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
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
    fn array_literal_folds_to_literal_array() {
        let arr = sqlparser_expr_to_custom_expr(&parse_expr("[1, 2, 3]")).expect("array");
        let Expr::Literal(Literal::Array(items)) = arr else {
            panic!("expected Literal::Array");
        };
        assert_eq!(items.len(), 3);
        assert!(matches!(items[0], Literal::Int(1)));
        assert!(matches!(items[2], Literal::Int(3)));
    }
}
