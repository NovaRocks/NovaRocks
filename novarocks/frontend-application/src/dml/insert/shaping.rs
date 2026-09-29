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

use std::collections::HashMap;

use arrow::datatypes::{DataType, TimeUnit};
use novarocks_parser::{Span, ast, printer};
use novarocks_types::naming::normalize_identifier;
use novarocks_types::schema::{ColumnDef, ColumnDefault, validate_column_default};

use super::command::{literal_query, plain_function};
use crate::query_execution::dml::iceberg_writer::arrow_data_type_to_sql_type_name;

/// Type direct row sources before their VALUES/UNION common-type analysis.
/// General query sources keep the original output projection boundary.
pub(crate) fn shape_insert_source(
    source: &ast::Query,
    insert_columns: &[String],
    source_columns: &[ColumnDef],
    write_columns: &[ColumnDef],
) -> Result<Option<ast::Query>, String> {
    let uncontrolled_values = matches!(source.body.as_ref(), ast::SetExpr::Values(_))
        && source.with.is_none()
        && source.order_by.is_empty()
        && source.limit.is_none()
        && source.offset.is_none()
        && source.fetch.is_none();
    if !uncontrolled_values && !literal_query(source) {
        return Ok(None);
    }
    if body_has_distinct_set(&source.body) {
        return Ok(None);
    }
    let mapping = column_mapping(insert_columns, source_columns, write_columns)?;
    let width = if insert_columns.is_empty() {
        source_columns.len()
    } else {
        insert_columns.len()
    };
    let mut query = source.clone();
    shape_body(&mut query.body, &mapping, width, write_columns)?;
    Ok(Some(query))
}

fn query_with_body(body: ast::SetExpr) -> ast::Query {
    ast::Query {
        with: None,
        body: Box::new(body),
        order_by: Vec::new(),
        limit: None,
        offset: None,
        limit_comma_offset: false,
        fetch: None,
        span: Span::new(0, 0),
    }
}

fn body_has_distinct_set(body: &ast::SetExpr) -> bool {
    match body {
        ast::SetExpr::SetOperation(operation) => {
            !matches!(operation.operator, ast::SetOperator::Union)
                || !matches!(operation.quantifier, ast::SetQuantifier::All)
                || body_has_distinct_set(&operation.left)
                || body_has_distinct_set(&operation.right)
        }
        ast::SetExpr::Query(query) => body_has_distinct_set(&query.body),
        _ => false,
    }
}

fn column_mapping(
    insert_columns: &[String],
    source_columns: &[ColumnDef],
    write_columns: &[ColumnDef],
) -> Result<Vec<Option<usize>>, String> {
    if insert_columns.is_empty() {
        return Ok(write_columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                source_columns
                    .iter()
                    .position(|source| source.name.eq_ignore_ascii_case(&column.name))
                    .or_else(|| (source_columns.len() == write_columns.len()).then_some(index))
            })
            .collect());
    }
    let mut indices = HashMap::new();
    for (index, name) in insert_columns.iter().enumerate() {
        if indices.insert(normalize_identifier(name)?, index).is_some() {
            return Err(format!("duplicate INSERT column `{name}`"));
        }
    }
    let mapping = write_columns
        .iter()
        .map(|column| Ok(indices.remove(&normalize_identifier(&column.name)?)))
        .collect::<Result<Vec<_>, String>>()?;
    if let Some((name, _)) = indices.into_iter().next() {
        return Err(format!("unknown INSERT column `{name}`"));
    }
    Ok(mapping)
}

fn shape_body(
    body: &mut ast::SetExpr,
    mapping: &[Option<usize>],
    width: usize,
    columns: &[ColumnDef],
) -> Result<(), String> {
    match body {
        ast::SetExpr::Values(values) => {
            for row in &mut values.rows {
                *row = shape_row(row, mapping, width, columns)?;
            }
        }
        ast::SetExpr::Select(select) => {
            let row = select
                .projection
                .iter()
                .map(|item| match item {
                    ast::SelectItem::UnnamedExpr(expr)
                    | ast::SelectItem::ExprWithAlias { expr, .. } => Ok(expr.clone()),
                    _ => Err("INSERT literal SELECT source only supports expressions".to_string()),
                })
                .collect::<Result<Vec<_>, _>>()?;
            let expressions = shape_row(&row, mapping, width, columns)?;
            // The eligible source projections are self-contained constants. Keep
            // the original SELECT as the row-producing relation so HAVING aliases,
            // filters and DISTINCT observe source types before sink conversion.
            // Each leaf's target projection still precedes UNION common typing.
            let inner_sql =
                printer::print_query(&query_with_body(ast::SetExpr::Select(select.clone())));
            let sql = format!(
                "SELECT {} FROM ({inner_sql}) AS __nr_insert_literal_src",
                expressions
                    .iter()
                    .map(printer::print_expr)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            let statements = novarocks_parser::parse(&sql)
                .map_err(|error| format!("INSERT literal projection: {error}"))?;
            let [ast::Statement::Query(query)] = statements.as_slice() else {
                return Err("INSERT literal projection expected query".into());
            };
            *body = ast::SetExpr::Query(Box::new(query.clone()));
        }
        ast::SetExpr::Query(query) => shape_body(&mut query.body, mapping, width, columns)?,
        ast::SetExpr::SetOperation(operation) => {
            if !matches!(operation.operator, ast::SetOperator::Union)
                || !matches!(operation.quantifier, ast::SetQuantifier::All)
            {
                return Err("INSERT literal source requires UNION ALL".to_string());
            }
            shape_body(&mut operation.left, mapping, width, columns)?;
            shape_body(&mut operation.right, mapping, width, columns)?;
        }
    }
    Ok(())
}

fn shape_row(
    row: &[ast::Expr],
    mapping: &[Option<usize>],
    width: usize,
    columns: &[ColumnDef],
) -> Result<Vec<ast::Expr>, String> {
    if row.len() != width {
        return Err(format!(
            "insert column count mismatch: expected {width} values for column list, got {}",
            row.len()
        ));
    }
    mapping
        .iter()
        .zip(columns)
        .map(|(index, column)| {
            let expr = match index {
                Some(index) => row[*index].clone(),
                None => omitted_insert_expr(column)?,
            };
            constrain_literal(expr, &column.data_type)
        })
        .collect()
}

/// Preserve a default's exact coefficient and bytes, including recursive values.
pub(crate) fn omitted_insert_expr(column: &ColumnDef) -> Result<ast::Expr, String> {
    if let Some(default) = &column.write_default {
        validate_column_default(default)
            .map_err(|error| format!("INSERT write-default for `{}`: {error}", column.name))?;
        return default_to_expr(default, &column.data_type)
            .map_err(|error| format!("INSERT write-default for `{}`: {error}", column.name));
    }
    if column.nullable {
        Ok(null_expr())
    } else {
        Err(format!(
            "INSERT omits required column `{}` without a write default",
            column.name
        ))
    }
}

fn constrain_literal(mut expr: ast::Expr, target: &DataType) -> Result<ast::Expr, String> {
    match &mut expr {
        // Explicit CAST is a semantic boundary. Only its own VARIANT declaration
        // authorizes the existing constant JSON representation conversion.
        ast::Expr::Cast(cast) => {
            if printer::print_object_name(&cast.data_type.name).eq_ignore_ascii_case("variant") {
                if let Some(encoded) = constant_variant(&cast.expr)? {
                    cast.expr = Box::new(encoded);
                }
            }
        }
        ast::Expr::Nested(nested) => {
            nested.expression = Box::new(constrain_literal((*nested.expression).clone(), target)?);
        }
        ast::Expr::Literal(value) => {
            if matches!(target, DataType::Binary | DataType::LargeBinary)
                && let ast::LiteralKind::String(text) = &value.kind
            {
                value.kind = ast::LiteralKind::HexString(hex::encode_upper(
                    novarocks_sql::literal::latin1_string_to_bytes(text)?,
                ));
            }
        }
        ast::Expr::Array(array) if array.element_type.is_none() => {
            if let DataType::List(field) = target {
                for element in &mut array.elements {
                    *element = constrain_literal(element.clone(), field.data_type())?;
                }
            }
        }
        ast::Expr::Map(map) => {
            if let Some((key, value)) = map_types(target) {
                for entry in &mut map.entries {
                    entry.key = constrain_literal(entry.key.clone(), key)?;
                    entry.value = constrain_literal(entry.value.clone(), value)?;
                }
            }
        }
        ast::Expr::Tuple(tuple) => {
            if let DataType::Struct(fields) = target
                && fields.len() == tuple.expressions.len()
            {
                for (value, field) in tuple.expressions.iter_mut().zip(fields) {
                    *value = constrain_literal(value.clone(), field.data_type())?;
                }
            }
        }
        ast::Expr::Struct(structure) => {
            if let DataType::Struct(fields) = target
                && fields.len() == structure.fields.len()
            {
                for (value, field) in structure.fields.iter_mut().zip(fields) {
                    value.value = constrain_literal(value.value.clone(), field.data_type())?;
                }
            }
        }
        ast::Expr::FunctionCall(function) if plain_function(function) => {
            let name = printer::print_object_name(&function.name).to_ascii_lowercase();
            match (name.as_str(), target) {
                ("array", DataType::List(field)) => {
                    for value in &mut function.arguments {
                        *value = constrain_literal(value.clone(), field.data_type())?;
                    }
                }
                ("row", DataType::Struct(fields)) if fields.len() == function.arguments.len() => {
                    for (value, field) in function.arguments.iter_mut().zip(fields) {
                        *value = constrain_literal(value.clone(), field.data_type())?;
                    }
                }
                ("named_struct", DataType::Struct(fields))
                    if fields.len() * 2 == function.arguments.len() =>
                {
                    for (pair, field) in function.arguments.chunks_exact_mut(2).zip(fields) {
                        pair[1] = constrain_literal(pair[1].clone(), field.data_type())?;
                    }
                }
                ("map", DataType::Map(..)) => {
                    if let Some((key, value)) = map_types(target) {
                        for pair in function.arguments.chunks_exact_mut(2) {
                            pair[0] = constrain_literal(pair[0].clone(), key)?;
                            pair[1] = constrain_literal(pair[1].clone(), value)?;
                        }
                    }
                }
                ("parse_json", DataType::LargeBinary) => {
                    if let Some(encoded) = constant_variant(&expr)? {
                        expr = encoded;
                    }
                }
                _ => {}
            }
        }
        _ => {}
    }
    cast_expr(expr, target)
}

fn map_types(target: &DataType) -> Option<(&DataType, &DataType)> {
    let DataType::Map(field, _) = target else {
        return None;
    };
    let DataType::Struct(fields) = field.data_type() else {
        return None;
    };
    (fields.len() == 2).then(|| (fields[0].data_type(), fields[1].data_type()))
}

fn constant_variant(expr: &ast::Expr) -> Result<Option<ast::Expr>, String> {
    if let ast::Expr::Nested(nested) = expr {
        return constant_variant(&nested.expression);
    }
    let ast::Expr::FunctionCall(function) = expr else {
        return Ok(None);
    };
    if !plain_function(function)
        || !printer::print_object_name(&function.name).eq_ignore_ascii_case("parse_json")
        || function.arguments.len() != 1
    {
        return Ok(None);
    }
    fn string_literal(expr: &ast::Expr) -> Option<&str> {
        match expr {
            ast::Expr::Literal(ast::Literal {
                kind: ast::LiteralKind::String(text),
                ..
            }) => Some(text),
            ast::Expr::Nested(nested) => string_literal(&nested.expression),
            ast::Expr::Cast(cast)
                if matches!(
                    printer::print_object_name(&cast.data_type.name)
                        .to_ascii_lowercase()
                        .as_str(),
                    "varchar" | "string" | "text"
                ) && cast.format.is_none() =>
            {
                string_literal(&cast.expr)
            }
            _ => None,
        }
    }
    let Some(text) = string_literal(&function.arguments[0]) else {
        return Ok(None);
    };
    let bytes = novarocks_types::value::variant_encode::encode_json_text_to_variant_bytes(text)
        .map_err(|error| format!("parse_json failed: {error}"))?;
    Ok(Some(ast::Expr::Literal(ast::Literal {
        kind: ast::LiteralKind::HexString(hex::encode_upper(bytes)),
        span: expr.span(),
    })))
}

fn cast_expr(expr: ast::Expr, target: &DataType) -> Result<ast::Expr, String> {
    let sql = format!(
        "SELECT CAST(NULL AS {})",
        arrow_data_type_to_sql_type_name(target)?
    );
    let statements =
        novarocks_parser::parse(&sql).map_err(|error| format!("INSERT target type: {error}"))?;
    let [ast::Statement::Query(query)] = statements.as_slice() else {
        return Err("INSERT target type expected query".into());
    };
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err("INSERT target type expected SELECT".into());
    };
    let ast::SelectItem::UnnamedExpr(ast::Expr::Cast(template)) = &select.projection[0] else {
        return Err("INSERT target type expected CAST".into());
    };
    let mut cast = template.clone();
    cast.span = expr.span();
    cast.expr = Box::new(expr);
    Ok(ast::Expr::Cast(cast))
}

fn literal(kind: ast::LiteralKind) -> ast::Expr {
    ast::Expr::Literal(ast::Literal {
        kind,
        span: Span::new(0, 0),
    })
}
fn null_expr() -> ast::Expr {
    literal(ast::LiteralKind::Null)
}
fn number(value: String) -> ast::Expr {
    literal(ast::LiteralKind::Number(value))
}
fn string(value: String) -> ast::Expr {
    literal(ast::LiteralKind::String(value))
}
fn binary(value: &[u8]) -> ast::Expr {
    literal(ast::LiteralKind::HexString(hex::encode_upper(value)))
}
fn array(elements: Vec<ast::Expr>) -> ast::Expr {
    ast::Expr::Array(ast::ArrayExpr {
        element_type: None,
        elements,
        span: Span::new(0, 0),
    })
}
fn tuple(expressions: Vec<ast::Expr>) -> ast::Expr {
    ast::Expr::Tuple(ast::TupleExpr {
        expressions,
        span: Span::new(0, 0),
    })
}
fn map(entries: Vec<(ast::Expr, ast::Expr)>) -> ast::Expr {
    ast::Expr::Map(ast::MapExpr {
        entries: entries
            .into_iter()
            .map(|(key, value)| ast::MapEntry {
                key,
                value,
                span: Span::new(0, 0),
            })
            .collect(),
        span: Span::new(0, 0),
    })
}

fn default_to_expr(default: &ColumnDefault, data_type: &DataType) -> Result<ast::Expr, String> {
    Ok(match (default, data_type) {
        (ColumnDefault::Boolean(value), DataType::Boolean) => {
            literal(ast::LiteralKind::Boolean(*value))
        }
        (
            ColumnDefault::Int32(value),
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64,
        ) => number(value.to_string()),
        (ColumnDefault::Int64(value), DataType::Int64) => number(value.to_string()),
        (ColumnDefault::Float32 { bits }, DataType::Float32) => {
            number(f32::from_bits(*bits).to_string())
        }
        (ColumnDefault::Float64 { bits }, DataType::Float64) => {
            number(f64::from_bits(*bits).to_string())
        }
        (
            ColumnDefault::Decimal {
                unscaled,
                precision,
                scale,
            },
            DataType::Decimal128(target_precision, target_scale),
        ) if precision == target_precision && scale == target_scale => {
            number(format_decimal(*unscaled, *scale)?)
        }
        (ColumnDefault::String(value), DataType::Utf8 | DataType::LargeUtf8) => {
            string(value.clone())
        }
        (ColumnDefault::Binary(value), DataType::Binary | DataType::LargeBinary) => binary(value),
        (ColumnDefault::Date { days_since_epoch }, DataType::Date32) => {
            string(format_date(*days_since_epoch)?)
        }
        (
            ColumnDefault::TimestampMicros { micros_since_epoch },
            DataType::Timestamp(TimeUnit::Microsecond, _),
        )
        | (
            ColumnDefault::TimestamptzMicros { micros_since_epoch },
            DataType::Timestamp(TimeUnit::Microsecond, _),
        ) => string(format_timestamp_micros(*micros_since_epoch)?),
        (
            ColumnDefault::TimestampNanos { nanos_since_epoch },
            DataType::Timestamp(TimeUnit::Nanosecond, _),
        )
        | (
            ColumnDefault::TimestamptzNanos { nanos_since_epoch },
            DataType::Timestamp(TimeUnit::Nanosecond, _),
        ) => string(format_timestamp_nanos(*nanos_since_epoch)),
        (ColumnDefault::Array(values), DataType::List(field)) => array(
            values
                .iter()
                .map(|value| {
                    if matches!(value, ColumnDefault::Null) {
                        Ok(null_expr())
                    } else {
                        default_to_expr(value, field.data_type())
                    }
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        (ColumnDefault::Map(entries), DataType::Map(field, _)) => {
            let DataType::Struct(fields) = field.data_type() else {
                return Err(format!(
                    "MAP has unexpected entries type {:?}",
                    field.data_type()
                ));
            };
            if fields.len() != 2 {
                return Err("MAP entries struct must contain key and value".to_string());
            }
            map(entries
                .iter()
                .map(|(key, value)| {
                    Ok((
                        default_to_expr(key, fields[0].data_type())?,
                        if matches!(value, ColumnDefault::Null) {
                            null_expr()
                        } else {
                            default_to_expr(value, fields[1].data_type())?
                        },
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?)
        }
        (ColumnDefault::Struct(values), DataType::Struct(fields))
            if values.len() == fields.len() =>
        {
            tuple(
                values
                    .iter()
                    .zip(fields)
                    .map(|((_, value), field)| {
                        if matches!(value, ColumnDefault::Null) {
                            Ok(null_expr())
                        } else {
                            default_to_expr(value, field.data_type())
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
        }
        (value, data_type) => {
            return Err(format!(
                "write-default literal type does not match column type: literal={value:?} column={data_type:?}"
            ));
        }
    })
}

fn format_decimal(unscaled: i128, scale: i8) -> Result<String, String> {
    if scale < 0 {
        return Err(format!("negative DECIMAL scale {scale} is not supported"));
    }
    let negative = unscaled.is_negative();
    let digits = unscaled.unsigned_abs().to_string();
    let scale = usize::try_from(scale).expect("non-negative scale");
    let value = if scale == 0 {
        digits
    } else if digits.len() <= scale {
        format!("0.{}{}", "0".repeat(scale - digits.len()), digits)
    } else {
        let split = digits.len() - scale;
        format!("{}.{}", &digits[..split], &digits[split..])
    };
    Ok(if negative { format!("-{value}") } else { value })
}

fn format_date(days_since_epoch: i32) -> Result<String, String> {
    const UNIX_EPOCH_DAY_OFFSET: i32 = 719_163;
    let ce_days = UNIX_EPOCH_DAY_OFFSET
        .checked_add(days_since_epoch)
        .ok_or_else(|| format!("write-default date value {days_since_epoch} is out of range"))?;
    chrono::NaiveDate::from_num_days_from_ce_opt(ce_days)
        .map(|date| date.format("%Y-%m-%d").to_string())
        .ok_or_else(|| format!("write-default date value {days_since_epoch} is out of range"))
}

fn format_timestamp_micros(value: i64) -> Result<String, String> {
    chrono::DateTime::from_timestamp_micros(value)
        .map(|datetime| datetime.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string())
        .ok_or_else(|| format!("write-default datetime value {value} is out of range"))
}

fn format_timestamp_nanos(value: i64) -> String {
    chrono::DateTime::from_timestamp_nanos(value)
        .naive_utc()
        .format("%Y-%m-%d %H:%M:%S%.9f")
        .to_string()
}

#[cfg(test)]
#[path = "shaping_tests.rs"]
mod tests;
