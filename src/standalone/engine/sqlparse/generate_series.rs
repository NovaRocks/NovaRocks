//! `generate_series(...)` lowering helpers for the standalone engine.
//!
//! These functions extract the `(start, end, step)` tuple from a sqlparser
//! function call, materialize the rows for both iceberg and local inserts,
//! and evaluate projection expressions row-by-row without going through the
//! full pipeline executor.

use sqlparser::ast as sqlast;

use crate::sql::parser::ast::{Expr, GenerateSeriesSelect, Literal};
use crate::standalone::engine::local::{ColumnDef, normalize_identifier};
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
        Expr::Comparison { .. }
        | Expr::Logical { .. }
        | Expr::IsNull { .. }
        | Expr::Aggregate(_)
        | Expr::ScalarFunction(_) => Err(
            "standalone generate_series insert-select only supports literal, column, arithmetic, and CAST expressions"
                .to_string(),
        ),
    }
}
