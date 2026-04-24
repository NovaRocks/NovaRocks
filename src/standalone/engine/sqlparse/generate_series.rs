//! `generate_series(...)` lowering helpers for the standalone engine.
//!
//! These functions extract the `(start, end, step)` tuple from a sqlparser
//! function call, materialize the rows for both iceberg and local inserts,
//! and evaluate projection expressions row-by-row without going through the
//! full pipeline executor.

use sqlparser::ast as sqlast;

use crate::sql::parser::ast::{Expr, GenerateSeriesSelect, Literal};
use crate::standalone::engine::catalog::{ColumnDef, normalize_identifier};
use crate::standalone::engine::reorder_insert_rows;
use crate::standalone::iceberg::{IcebergCatalogEntry, insert_rows as insert_iceberg_rows};

use super::expr::{cast_literal, eval_literal_arithmetic, sqlparser_expr_to_literal};

pub(crate) fn parse_generate_series_function_expr(
    expr: &sqlast::Expr,
) -> Result<(i64, i64, i64), String> {
    let sqlast::Expr::Function(function) = expr else {
        return Err("expected generate_series function call".into());
    };
    let name = function
        .name
        .0
        .last()
        .map(|p| p.to_string().to_ascii_lowercase())
        .unwrap_or_default();
    if name != "generate_series" {
        return Err(format!("expected generate_series, got `{name}`"));
    }
    let sqlast::FunctionArguments::List(ref args) = function.args else {
        return Err("generate_series requires parenthesized arguments".into());
    };
    let values: Vec<i64> = args
        .args
        .iter()
        .map(|arg| match arg {
            sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(expr)) => {
                match sqlparser_expr_to_literal(expr)? {
                    Literal::Int(v) => Ok(v),
                    other => Err(format!(
                        "generate_series expects integer args, got {other:?}"
                    )),
                }
            }
            other => Err(format!(
                "generate_series expects positional args, got {other}"
            )),
        })
        .collect::<Result<_, _>>()?;
    match values.as_slice() {
        [start, end] => Ok((*start, *end, 1)),
        [start, end, step] => {
            if *step == 0 {
                return Err("generate_series step must not be zero".into());
            }
            Ok((*start, *end, *step))
        }
        _ => Err("generate_series expects 2 or 3 arguments".into()),
    }
}

pub(crate) fn insert_generate_series_rows(
    entry: &IcebergCatalogEntry,
    namespace: &str,
    table: &str,
    source: &GenerateSeriesSelect,
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<(), String> {
    const INSERT_CHUNK_SIZE: usize = 4096;
    let mut rows = Vec::with_capacity(INSERT_CHUNK_SIZE);
    let mut current = source.start;
    let ascending = source.step > 0;
    while if ascending {
        current <= source.end
    } else {
        current >= source.end
    } {
        let row = evaluate_generate_series_row(source, current)?;
        rows.extend(reorder_insert_rows(
            std::slice::from_ref(&row),
            insert_columns,
            target_columns,
        )?);
        if rows.len() >= INSERT_CHUNK_SIZE {
            insert_iceberg_rows(entry, namespace, table, &rows)?;
            rows.clear();
        }
        current = current.saturating_add(source.step);
    }
    if !rows.is_empty() {
        insert_iceberg_rows(entry, namespace, table, &rows)?;
    }
    Ok(())
}

pub(crate) fn insert_generate_series_rows_local(
    source: &GenerateSeriesSelect,
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<Vec<Vec<Literal>>, String> {
    let mut rows = Vec::new();
    let mut current = source.start;
    let ascending = source.step > 0;
    while if ascending {
        current <= source.end
    } else {
        current >= source.end
    } {
        let row = evaluate_generate_series_row(source, current)?;
        rows.extend(reorder_insert_rows(
            std::slice::from_ref(&row),
            insert_columns,
            target_columns,
        )?);
        current = current.saturating_add(source.step);
    }
    Ok(rows)
}

fn evaluate_generate_series_row(
    source: &GenerateSeriesSelect,
    current: i64,
) -> Result<Vec<Literal>, String> {
    source
        .projection
        .iter()
        .map(|expr| evaluate_generate_series_expr(expr, &source.column_name, current))
        .collect()
}

fn evaluate_generate_series_expr(
    expr: &Expr,
    column_name: &str,
    current: i64,
) -> Result<Literal, String> {
    match expr {
        Expr::Column(column) => {
            if normalize_identifier(&column.name)? == normalize_identifier(column_name)? {
                Ok(Literal::Int(current))
            } else {
                Err(format!(
                    "standalone generate_series source does not provide column `{}`",
                    column.name
                ))
            }
        }
        Expr::Literal(literal) => Ok(literal.clone()),
        Expr::Arithmetic { left, op, right } => {
            let left = evaluate_generate_series_expr(left, column_name, current)?;
            let right = evaluate_generate_series_expr(right, column_name, current)?;
            eval_literal_arithmetic(*op, &left, &right)
        }
        Expr::Cast { expr, data_type } => {
            let value = evaluate_generate_series_expr(expr, column_name, current)?;
            cast_literal(value, data_type)
        }
        Expr::ScalarFunction(func) => {
            let args = func
                .args
                .iter()
                .map(|arg| evaluate_generate_series_expr(arg, column_name, current))
                .collect::<Result<Vec<_>, _>>()?;
            evaluate_scalar_function(&func.name, &args)
        }
        Expr::Comparison { .. }
        | Expr::Logical { .. }
        | Expr::IsNull { .. }
        | Expr::Aggregate(_) => Err(
            "standalone generate_series insert-select only supports literal, column, arithmetic, CAST, and scalar function expressions"
                .to_string(),
        ),
    }
}

/// Row-wise scalar-function dispatcher for the generate_series INSERT-SELECT
/// path. Kept intentionally narrow — extend only when a concrete INSERT case
/// under `sql-tests` requires a new builtin, and prefer fail-fast otherwise.
fn evaluate_scalar_function(name: &str, args: &[Literal]) -> Result<Literal, String> {
    match name {
        "concat" => {
            // NULL propagation matches StarRocks' `concat`: any NULL argument
            // yields NULL; otherwise stringify each argument and join.
            let mut out = String::new();
            for arg in args {
                match arg {
                    Literal::Null => return Ok(Literal::Null),
                    Literal::String(s) | Literal::Date(s) => out.push_str(s),
                    Literal::Int(v) => out.push_str(&v.to_string()),
                    Literal::Float(v) => out.push_str(&v.to_string()),
                    Literal::Bool(v) => out.push_str(if *v { "1" } else { "0" }),
                    other => {
                        return Err(format!("concat does not support argument type: {other:?}"));
                    }
                }
            }
            Ok(Literal::String(out))
        }
        "to_binary" => {
            if args.is_empty() || args.len() > 2 {
                return Err("to_binary expects 1 or 2 arguments".to_string());
            }
            let input = match &args[0] {
                Literal::Null => return Ok(Literal::Null),
                Literal::String(s) => s.clone(),
                other => {
                    return Err(format!(
                        "to_binary expects VARCHAR as first argument, got: {other:?}"
                    ));
                }
            };
            let format = if args.len() == 2 {
                match &args[1] {
                    Literal::Null => return Ok(Literal::Null),
                    Literal::String(s) => s.clone(),
                    other => {
                        return Err(format!(
                            "to_binary expects VARCHAR format argument, got: {other:?}"
                        ));
                    }
                }
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
        other => Err(format!(
            "standalone generate_series insert-select does not support scalar function: {other}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::ast::{Expr, Literal, ScalarFunctionExpr, SqlType};

    fn col(name: &str) -> Expr {
        Expr::Column(crate::sql::parser::ast::ColumnRef {
            name: name.to_string(),
        })
    }

    #[test]
    fn scalar_function_concat_evaluates_with_column_ref() {
        // concat('value_', CAST(generate_series AS VARCHAR))
        let expr = Expr::ScalarFunction(ScalarFunctionExpr {
            name: "concat".to_string(),
            args: vec![
                Expr::Literal(Literal::String("value_".to_string())),
                Expr::Cast {
                    expr: Box::new(col("generate_series")),
                    data_type: SqlType::String,
                },
            ],
        });
        let out =
            evaluate_generate_series_expr(&expr, "generate_series", 7).expect("evaluate concat");
        assert_eq!(out, Literal::String("value_7".to_string()));
    }

    #[test]
    fn scalar_function_to_binary_utf8_round_trips_bytes() {
        // to_binary(concat('v_', CAST(generate_series AS VARCHAR)), 'utf8')
        let concat = Expr::ScalarFunction(ScalarFunctionExpr {
            name: "concat".to_string(),
            args: vec![
                Expr::Literal(Literal::String("v_".to_string())),
                Expr::Cast {
                    expr: Box::new(col("generate_series")),
                    data_type: SqlType::String,
                },
            ],
        });
        let to_bin = Expr::ScalarFunction(ScalarFunctionExpr {
            name: "to_binary".to_string(),
            args: vec![concat, Expr::Literal(Literal::String("utf8".to_string()))],
        });
        let out = evaluate_generate_series_expr(&to_bin, "generate_series", 42)
            .expect("evaluate to_binary(utf8)");
        assert_eq!(out, Literal::String("v_42".to_string()));
    }

    #[test]
    fn unsupported_scalar_function_returns_explicit_error() {
        let expr = Expr::ScalarFunction(ScalarFunctionExpr {
            name: "sqrt".to_string(),
            args: vec![col("generate_series")],
        });
        let err = evaluate_generate_series_expr(&expr, "generate_series", 1).unwrap_err();
        assert!(
            err.contains("does not support scalar function: sqrt"),
            "unexpected error: {err}"
        );
    }
}
