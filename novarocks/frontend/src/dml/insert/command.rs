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

use novarocks::engine::insert_engine::{InsertOverwriteMode, InsertTargetName, InsertValue};
use sqlparser::ast as sqlast;

const DYNAMIC_OVERWRITE_MARKER: &str = "__nr_op_dyn";

/// Frontend application command produced from one normalized sqlparser INSERT.
#[derive(Clone, Debug, PartialEq)]
pub struct InsertCommand {
    pub target: InsertTargetName,
    pub columns: Vec<String>,
    pub source: InsertCommandSource,
    pub overwrite_mode: InsertOverwriteMode,
}

/// Source form retained until backend dispatch and shaping.
#[derive(Clone, Debug, PartialEq)]
pub enum InsertCommandSource {
    Values(Vec<Vec<InsertValue>>),
    SelectLiteralRow(Vec<InsertValue>),
    FromQuery(Box<sqlast::Query>),
}

/// Convert a normalized sqlparser INSERT into the frontend-owned command.
pub fn convert_insert_command(insert: &sqlast::Insert) -> Result<InsertCommand, String> {
    let sqlast::TableObject::TableName(name) = &insert.table else {
        return Err(format!("unsupported INSERT target: {}", insert.table));
    };
    let mut target_parts = name
        .0
        .iter()
        .map(|part| {
            part.as_ident()
                .map(|ident| ident.value.clone())
                .ok_or_else(|| format!("unsupported INSERT target component: {part}"))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let overwrite_mode = if target_parts
        .first()
        .is_some_and(|part| part == DYNAMIC_OVERWRITE_MARKER)
    {
        if !insert.overwrite {
            return Err(
                "internal: __nr_op_dyn marker present without INSERT OVERWRITE \
                 (parser/normalizer mismatch)"
                    .to_string(),
            );
        }
        target_parts.remove(0);
        InsertOverwriteMode::DynamicPartitions
    } else if insert.overwrite {
        InsertOverwriteMode::FullTable
    } else {
        InsertOverwriteMode::Append
    };
    if target_parts.is_empty() {
        return Err("INSERT target is empty after overwrite normalization".to_string());
    }

    let source_query = insert
        .source
        .as_ref()
        .ok_or_else(|| "INSERT requires a source".to_string())?;
    let source = if should_route_insert_via_from_query(source_query) {
        InsertCommandSource::FromQuery(source_query.clone())
    } else {
        convert_set_expr_to_source(source_query.body.as_ref())?
    };

    Ok(InsertCommand {
        target: InsertTargetName {
            parts: target_parts,
        },
        columns: insert
            .columns
            .iter()
            .map(|column| column.value.clone())
            .collect(),
        source,
        overwrite_mode,
    })
}

fn convert_set_expr_to_source(body: &sqlast::SetExpr) -> Result<InsertCommandSource, String> {
    match body {
        sqlast::SetExpr::Values(values) => Ok(InsertCommandSource::Values(
            values
                .rows
                .iter()
                .map(|row| row.iter().map(expr_to_insert_value).collect())
                .collect::<Result<Vec<_>, _>>()?,
        )),
        sqlast::SetExpr::Select(select) => {
            if !select.from.is_empty() {
                return Err("INSERT SELECT with FROM must use the query pipeline".to_string());
            }
            Ok(InsertCommandSource::SelectLiteralRow(
                select
                    .projection
                    .iter()
                    .map(select_item_expr)
                    .map(|expr| expr.and_then(expr_to_insert_value))
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
        sqlast::SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            if !matches!(op, sqlast::SetOperator::Union) {
                return Err("INSERT SELECT set operation is only UNION ALL here".to_string());
            }
            if !matches!(
                set_quantifier,
                sqlast::SetQuantifier::All | sqlast::SetQuantifier::AllByName
            ) {
                return Err(
                    "INSERT SELECT UNION requires UNION ALL (UNION/UNION DISTINCT unsupported)"
                        .to_string(),
                );
            }
            let mut rows = Vec::new();
            flatten_literal_union_all(left, &mut rows)?;
            flatten_literal_union_all(right, &mut rows)?;
            Ok(InsertCommandSource::Values(rows))
        }
        sqlast::SetExpr::Query(query) => convert_set_expr_to_source(query.body.as_ref()),
        _ => Err("unsupported INSERT source".to_string()),
    }
}

fn flatten_literal_union_all(
    body: &sqlast::SetExpr,
    out: &mut Vec<Vec<InsertValue>>,
) -> Result<(), String> {
    if let sqlast::SetExpr::SetOperation {
        op: sqlast::SetOperator::Union,
        set_quantifier: sqlast::SetQuantifier::All | sqlast::SetQuantifier::AllByName,
        left,
        right,
    } = body
    {
        flatten_literal_union_all(left, out)?;
        flatten_literal_union_all(right, out)
    } else {
        match convert_set_expr_to_source(body)? {
            InsertCommandSource::Values(rows) => out.extend(rows),
            InsertCommandSource::SelectLiteralRow(row) => out.push(row),
            InsertCommandSource::FromQuery(_) => {
                return Err(
                    "internal: query-backed UNION ALL must use the query pipeline".to_string(),
                );
            }
        }
        Ok(())
    }
}

fn should_route_insert_via_from_query(query: &sqlast::Query) -> bool {
    query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || body_requires_pipeline(query.body.as_ref())
}

fn body_requires_pipeline(body: &sqlast::SetExpr) -> bool {
    match body {
        sqlast::SetExpr::Select(select) => {
            !select.from.is_empty()
                || select.projection.iter().any(|item| {
                    select_item_expr(item)
                        .and_then(expr_to_insert_value)
                        .is_err()
                })
        }
        sqlast::SetExpr::Values(values) => values
            .rows
            .iter()
            .flatten()
            .any(|expr| expr_to_insert_value(expr).is_err()),
        sqlast::SetExpr::Query(query) => should_route_insert_via_from_query(query),
        sqlast::SetExpr::SetOperation { left, right, .. } => {
            body_requires_pipeline(left) || body_requires_pipeline(right)
        }
        _ => false,
    }
}

fn select_item_expr(item: &sqlast::SelectItem) -> Result<&sqlast::Expr, String> {
    match item {
        sqlast::SelectItem::UnnamedExpr(expr) | sqlast::SelectItem::ExprWithAlias { expr, .. } => {
            Ok(expr)
        }
        _ => Err("INSERT SELECT source only supports expressions".to_string()),
    }
}

fn expr_to_insert_value(expr: &sqlast::Expr) -> Result<InsertValue, String> {
    match expr {
        sqlast::Expr::Value(sqlast::ValueWithSpan { value, .. }) => match value {
            sqlast::Value::Null => Ok(InsertValue::Null),
            sqlast::Value::Boolean(value) => Ok(InsertValue::Bool(*value)),
            sqlast::Value::Number(value, _) => Ok(number_to_insert_value(value)),
            sqlast::Value::SingleQuotedString(value) | sqlast::Value::DoubleQuotedString(value) => {
                Ok(InsertValue::String(value.clone()))
            }
            sqlast::Value::HexStringLiteral(value) => {
                let bytes = hex::decode(value)
                    .map_err(|error| format!("invalid hex literal X'{value}': {error}"))?;
                Ok(InsertValue::String(
                    bytes.into_iter().map(char::from).collect(),
                ))
            }
            _ => Err(format!("unsupported literal in INSERT VALUES: {value}")),
        },
        sqlast::Expr::UnaryOp {
            op: sqlast::UnaryOperator::Minus,
            expr,
        } => negate_insert_value(expr_to_insert_value(expr)?),
        sqlast::Expr::Nested(expr) => expr_to_insert_value(expr),
        sqlast::Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            if cast_data_type_is_decimal(data_type) {
                return Err(format!(
                    "CAST to DECIMAL in INSERT SELECT requires pipeline evaluation: {expr}"
                ));
            }
            expr_to_insert_value(inner)
        }
        sqlast::Expr::TypedString(typed) => Ok(InsertValue::String(typed.value.to_string())),
        sqlast::Expr::Identifier(ident) => Ok(InsertValue::String(ident.value.clone())),
        sqlast::Expr::BinaryOp { left, op, right } => {
            let left = expr_to_insert_value(left)?;
            let right = expr_to_insert_value(right)?;
            match (left, op, right) {
                (InsertValue::Int(left), sqlast::BinaryOperator::Plus, InsertValue::Int(right)) => {
                    left.checked_add(right)
                        .map(InsertValue::Int)
                        .ok_or_else(|| format!("integer literal overflow in `{expr}`"))
                }
                (
                    InsertValue::Int(left),
                    sqlast::BinaryOperator::Minus,
                    InsertValue::Int(right),
                ) => left
                    .checked_sub(right)
                    .map(InsertValue::Int)
                    .ok_or_else(|| format!("integer literal overflow in `{expr}`")),
                (
                    InsertValue::Int(left),
                    sqlast::BinaryOperator::Multiply,
                    InsertValue::Int(right),
                ) => left
                    .checked_mul(right)
                    .map(InsertValue::Int)
                    .ok_or_else(|| format!("integer literal overflow in `{expr}`")),
                (
                    InsertValue::Float(left),
                    sqlast::BinaryOperator::Plus,
                    InsertValue::Float(right),
                ) => Ok(InsertValue::Float(left + right)),
                (
                    InsertValue::Float(left),
                    sqlast::BinaryOperator::Minus,
                    InsertValue::Float(right),
                ) => Ok(InsertValue::Float(left - right)),
                _ => Err(format!("unsupported expression in INSERT VALUES: {expr}")),
            }
        }
        sqlast::Expr::Array(sqlast::Array { elem, .. }) => Ok(InsertValue::Array(
            elem.iter()
                .map(expr_to_insert_value)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        sqlast::Expr::Tuple(values) => Ok(InsertValue::Struct(
            values
                .iter()
                .map(expr_to_insert_value)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        sqlast::Expr::Struct { values, .. } => Ok(InsertValue::Struct(
            values
                .iter()
                .map(expr_to_insert_value)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        sqlast::Expr::Map(map) => Ok(InsertValue::Map(
            map.entries
                .iter()
                .map(|entry| {
                    Ok((
                        expr_to_insert_value(&entry.key)?,
                        expr_to_insert_value(&entry.value)?,
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?,
        )),
        sqlast::Expr::Function(function) => function_to_insert_value(function),
        _ => Err(format!("unsupported expression in INSERT VALUES: {expr}")),
    }
}

fn function_to_insert_value(function: &sqlast::Function) -> Result<InsertValue, String> {
    let args = function_expr_args(&function.args)?;
    let name = function.name.to_string().to_ascii_lowercase();
    match name.as_str() {
        "parse_json" => {
            if args.len() != 1 {
                return Err("parse_json expects 1 argument".to_string());
            }
            let InsertValue::String(json_text) = expr_to_insert_value(args[0])? else {
                return Err("parse_json expects VARCHAR argument".to_string());
            };
            let bytes = novarocks::engine::insert_engine::encode_insert_variant_json(&json_text)
                .map_err(|error| format!("parse_json failed: {error}"))?;
            Ok(InsertValue::String(
                bytes.into_iter().map(char::from).collect(),
            ))
        }
        "array" => Ok(InsertValue::Array(
            args.into_iter()
                .map(expr_to_insert_value)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        "row" => Ok(InsertValue::Struct(
            args.into_iter()
                .map(expr_to_insert_value)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        "named_struct" => {
            if args.len() % 2 != 0 {
                return Err(format!(
                    "named_struct literal requires an even number of arguments, got {}",
                    args.len()
                ));
            }
            Ok(InsertValue::Struct(
                args.into_iter()
                    .skip(1)
                    .step_by(2)
                    .map(expr_to_insert_value)
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
            args.chunks_exact(2)
                .map(|pair| {
                    Ok((
                        expr_to_insert_value(pair[0])?,
                        expr_to_insert_value(pair[1])?,
                    ))
                })
                .collect::<Result<Vec<_>, String>>()
                .map(InsertValue::Map)
        }
        _ => Err(format!(
            "unsupported expression in INSERT VALUES: {}",
            sqlast::Expr::Function(function.clone())
        )),
    }
}

fn function_expr_args(args: &sqlast::FunctionArguments) -> Result<Vec<&sqlast::Expr>, String> {
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

fn number_to_insert_value(value: &str) -> InsertValue {
    if !value.contains(['.', 'e', 'E']) {
        value
            .parse::<i64>()
            .map(InsertValue::Int)
            .unwrap_or_else(|_| InsertValue::String(value.to_string()))
    } else {
        value
            .parse::<f64>()
            .map(InsertValue::Float)
            .unwrap_or_else(|_| InsertValue::String(value.to_string()))
    }
}

fn negate_insert_value(value: InsertValue) -> Result<InsertValue, String> {
    match value {
        InsertValue::Int(value) => value
            .checked_neg()
            .map(InsertValue::Int)
            .ok_or_else(|| "integer literal overflow while negating".to_string()),
        InsertValue::Float(value) => Ok(InsertValue::Float(-value)),
        InsertValue::String(value) if !value.trim().contains(['.', 'e', 'E']) => {
            Ok(InsertValue::String(format!("-{}", value.trim())))
        }
        other => Err(format!("cannot negate {other:?}")),
    }
}

fn cast_data_type_is_decimal(data_type: &sqlast::DataType) -> bool {
    match data_type {
        sqlast::DataType::Decimal(_) | sqlast::DataType::Dec(_) | sqlast::DataType::Numeric(_) => {
            true
        }
        sqlast::DataType::Custom(name, _) => matches!(
            name.to_string().to_ascii_lowercase().as_str(),
            "decimal" | "decimal32" | "decimal64" | "decimal128"
        ),
        _ => false,
    }
}
