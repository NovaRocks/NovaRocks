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

use crate::query_execution::dml::insert::{InsertOverwriteMode, InsertTargetName, InsertValue};
use novarocks_parser::{
    ast::{self, Insert},
    printer,
};

/// Frontend application command produced from one SQLP-5 typed INSERT.
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
    FromQuery(Box<ast::Query>),
}

/// Convert the typed INSERT statement into the frontend-owned execution command.
pub fn convert_insert_command(insert: &Insert) -> Result<InsertCommand, String> {
    let target_parts = insert
        .target
        .parts
        .iter()
        .map(|part| part.value.clone())
        .collect::<Vec<_>>();

    let overwrite_mode = if insert
        .partitions
        .as_ref()
        .is_some_and(|partitions| partitions.dynamic)
    {
        if !insert.overwrite {
            return Err("dynamic INSERT partitions require INSERT OVERWRITE".to_string());
        }
        InsertOverwriteMode::DynamicPartitions
    } else if insert.overwrite {
        InsertOverwriteMode::FullTable
    } else {
        InsertOverwriteMode::Append
    };
    if target_parts.is_empty() {
        return Err("INSERT target is empty after overwrite normalization".to_string());
    }

    let source = if should_route_insert_via_from_query(&insert.source) {
        InsertCommandSource::FromQuery(Box::new(insert.source.clone()))
    } else {
        convert_set_expr_to_source(insert.source.body.as_ref())?
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

fn convert_set_expr_to_source(body: &ast::SetExpr) -> Result<InsertCommandSource, String> {
    match body {
        ast::SetExpr::Values(values) => Ok(InsertCommandSource::Values(
            values
                .rows
                .iter()
                .map(|row| row.iter().map(expr_to_insert_value).collect())
                .collect::<Result<Vec<_>, _>>()?,
        )),
        ast::SetExpr::Select(select) => {
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
        ast::SetExpr::SetOperation(operation) => {
            if !matches!(operation.operator, ast::SetOperator::Union) {
                return Err("INSERT SELECT set operation is only UNION ALL here".to_string());
            }
            if !matches!(operation.quantifier, ast::SetQuantifier::All) {
                return Err(
                    "INSERT SELECT UNION requires UNION ALL (UNION/UNION DISTINCT unsupported)"
                        .to_string(),
                );
            }
            let mut rows = Vec::new();
            flatten_literal_union_all(&operation.left, &mut rows)?;
            flatten_literal_union_all(&operation.right, &mut rows)?;
            Ok(InsertCommandSource::Values(rows))
        }
        ast::SetExpr::Query(query) => convert_set_expr_to_source(query.body.as_ref()),
    }
}

fn flatten_literal_union_all(
    body: &ast::SetExpr,
    out: &mut Vec<Vec<InsertValue>>,
) -> Result<(), String> {
    if let ast::SetExpr::SetOperation(operation) = body
        && matches!(operation.operator, ast::SetOperator::Union)
        && matches!(operation.quantifier, ast::SetQuantifier::All)
    {
        flatten_literal_union_all(&operation.left, out)?;
        flatten_literal_union_all(&operation.right, out)
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

fn should_route_insert_via_from_query(query: &ast::Query) -> bool {
    query.with.is_some()
        || !query.order_by.is_empty()
        || query.limit.is_some()
        || query.offset.is_some()
        || query.fetch.is_some()
        || body_requires_pipeline(query.body.as_ref())
}

fn body_requires_pipeline(body: &ast::SetExpr) -> bool {
    match body {
        ast::SetExpr::Select(select) => {
            !select.from.is_empty()
                || select.projection.iter().any(|item| {
                    select_item_expr(item)
                        .and_then(expr_to_insert_value)
                        .is_err()
                })
        }
        ast::SetExpr::Values(values) => values
            .rows
            .iter()
            .flatten()
            .any(|expr| expr_to_insert_value(expr).is_err()),
        ast::SetExpr::Query(query) => should_route_insert_via_from_query(query),
        ast::SetExpr::SetOperation(operation) => {
            body_requires_pipeline(&operation.left) || body_requires_pipeline(&operation.right)
        }
    }
}

fn select_item_expr(item: &ast::SelectItem) -> Result<&ast::Expr, String> {
    match item {
        ast::SelectItem::UnnamedExpr(expr) | ast::SelectItem::ExprWithAlias { expr, .. } => {
            Ok(expr)
        }
        _ => Err("INSERT SELECT source only supports expressions".to_string()),
    }
}

fn expr_to_insert_value(expr: &ast::Expr) -> Result<InsertValue, String> {
    match expr {
        ast::Expr::Literal(literal) => match &literal.kind {
            ast::LiteralKind::Null => Ok(InsertValue::Null),
            ast::LiteralKind::Boolean(value) => Ok(InsertValue::Bool(*value)),
            ast::LiteralKind::Number(value) => Ok(number_to_insert_value(value)),
            ast::LiteralKind::String(value) => Ok(InsertValue::String(value.clone())),
            ast::LiteralKind::HexString(value) => {
                let bytes = hex::decode(value)
                    .map_err(|error| format!("invalid hex literal X'{value}': {error}"))?;
                Ok(InsertValue::String(
                    bytes.into_iter().map(char::from).collect(),
                ))
            }
        },
        ast::Expr::Unary(unary) if matches!(unary.operator, ast::UnaryOperator::Minus) => {
            negate_insert_value(expr_to_insert_value(&unary.expression)?)
        }
        ast::Expr::Nested(nested) => expr_to_insert_value(&nested.expression),
        ast::Expr::Cast(cast) => {
            if cast_data_type_is_decimal(&cast.data_type) {
                return Err(format!(
                    "CAST to DECIMAL in INSERT SELECT requires pipeline evaluation: {}",
                    printer::print_expr(expr)
                ));
            }
            expr_to_insert_value(&cast.expr)
        }
        ast::Expr::TypedString(typed) => {
            expr_to_insert_value(&ast::Expr::Literal(typed.value.clone()))
        }
        ast::Expr::Identifier(ident) => Ok(InsertValue::String(ident.value.clone())),
        ast::Expr::Binary(binary) => {
            let left = expr_to_insert_value(&binary.left)?;
            let right = expr_to_insert_value(&binary.right)?;
            match (left, binary.operator, right) {
                (InsertValue::Int(left), ast::BinaryOperator::Add, InsertValue::Int(right)) => left
                    .checked_add(right)
                    .map(InsertValue::Int)
                    .ok_or_else(|| {
                        format!(
                            "integer literal overflow in `{}`",
                            printer::print_expr(expr)
                        )
                    }),
                (
                    InsertValue::Int(left),
                    ast::BinaryOperator::Subtract,
                    InsertValue::Int(right),
                ) => left
                    .checked_sub(right)
                    .map(InsertValue::Int)
                    .ok_or_else(|| {
                        format!(
                            "integer literal overflow in `{}`",
                            printer::print_expr(expr)
                        )
                    }),
                (
                    InsertValue::Int(left),
                    ast::BinaryOperator::Multiply,
                    InsertValue::Int(right),
                ) => left
                    .checked_mul(right)
                    .map(InsertValue::Int)
                    .ok_or_else(|| {
                        format!(
                            "integer literal overflow in `{}`",
                            printer::print_expr(expr)
                        )
                    }),
                (InsertValue::Float(left), ast::BinaryOperator::Add, InsertValue::Float(right)) => {
                    Ok(InsertValue::Float(left + right))
                }
                (
                    InsertValue::Float(left),
                    ast::BinaryOperator::Subtract,
                    InsertValue::Float(right),
                ) => Ok(InsertValue::Float(left - right)),
                _ => Err(format!(
                    "unsupported expression in INSERT VALUES: {}",
                    printer::print_expr(expr)
                )),
            }
        }
        ast::Expr::Array(array) => Ok(InsertValue::Array(
            array
                .elements
                .iter()
                .map(expr_to_insert_value)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        ast::Expr::Tuple(tuple) => Ok(InsertValue::Struct(
            tuple
                .expressions
                .iter()
                .map(expr_to_insert_value)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        ast::Expr::Struct(structure) => Ok(InsertValue::Struct(
            structure
                .fields
                .iter()
                .map(|field| expr_to_insert_value(&field.value))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        ast::Expr::Map(map) => Ok(InsertValue::Map(
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
        ast::Expr::FunctionCall(function) => function_to_insert_value(function),
        _ => Err(format!(
            "unsupported expression in INSERT VALUES: {}",
            printer::print_expr(expr)
        )),
    }
}

fn function_to_insert_value(function: &ast::FunctionCall) -> Result<InsertValue, String> {
    let args = function_expr_args(function)?;
    let name = printer::print_object_name(&function.name).to_ascii_lowercase();
    match name.as_str() {
        "parse_json" => {
            if args.len() != 1 {
                return Err("parse_json expects 1 argument".to_string());
            }
            let InsertValue::String(json_text) = expr_to_insert_value(args[0])? else {
                return Err("parse_json expects VARCHAR argument".to_string());
            };
            let bytes = crate::query_execution::dml::insert::encode_insert_variant_json(&json_text)
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
            printer::print_expr(&ast::Expr::FunctionCall(function.clone()))
        )),
    }
}

fn function_expr_args(function: &ast::FunctionCall) -> Result<Vec<&ast::Expr>, String> {
    if !matches!(function.quantifier, ast::FunctionQuantifier::None)
        || !function.order_by.is_empty()
        || function.separator.is_some()
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
    {
        return Err(format!(
            "unsupported function modifiers in INSERT VALUES: {}",
            printer::print_expr(&ast::Expr::FunctionCall(function.clone()))
        ));
    }
    Ok(function.arguments.iter().collect())
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

fn cast_data_type_is_decimal(data_type: &ast::TypeName) -> bool {
    matches!(
        printer::print_object_name(&data_type.name)
            .to_ascii_lowercase()
            .as_str(),
        "decimal" | "decimal32" | "decimal64" | "decimal128" | "dec" | "numeric"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_execution::dml::insert::{InsertOverwriteMode, InsertValue};

    fn parse_insert(sql: &str) -> Insert {
        let statements = novarocks_parser::parse(sql).expect("statement should parse");
        let [ast::Statement::Dml(ast::DmlStatement::Insert(statement))] = statements.as_slice()
        else {
            panic!("statement should be SQLP-5 INSERT");
        };
        statement.clone()
    }

    fn convert_insert(sql: &str) -> Result<InsertCommand, String> {
        convert_insert_command(&parse_insert(sql))
    }

    #[test]
    fn values_become_literal_rows() {
        let command =
            convert_insert("INSERT INTO db.t VALUES (1, 'a'), (2, NULL)").expect("convert command");
        assert_eq!(command.target.parts, vec!["db", "t"]);
        assert_eq!(
            command.source,
            InsertCommandSource::Values(vec![
                vec![InsertValue::Int(1), InsertValue::String("a".to_string())],
                vec![InsertValue::Int(2), InsertValue::Null],
            ])
        );
    }

    #[test]
    fn select_without_from_becomes_literal_row() {
        let command = convert_insert("INSERT INTO t SELECT 40 + 2, 'x'").expect("convert command");
        assert_eq!(
            command.source,
            InsertCommandSource::SelectLiteralRow(vec![
                InsertValue::Int(42),
                InsertValue::String("x".to_string()),
            ])
        );
    }

    #[test]
    fn select_with_from_uses_query_pipeline() {
        let command = convert_insert("INSERT INTO t SELECT id FROM src").expect("convert command");
        assert!(matches!(command.source, InsertCommandSource::FromQuery(_)));
    }

    #[test]
    fn non_constant_projection_uses_query_pipeline() {
        let command = convert_insert("INSERT INTO t SELECT value + 1").expect("convert command");
        assert!(matches!(command.source, InsertCommandSource::FromQuery(_)));
    }

    #[test]
    fn non_literal_values_function_uses_query_pipeline() {
        let command = convert_insert("INSERT INTO t VALUES (to_bitmap(11), hll_hash(5))")
            .expect("convert command");
        assert!(matches!(command.source, InsertCommandSource::FromQuery(_)));
    }

    #[test]
    fn parse_json_values_fold_to_packed_variant_literal() {
        let command = convert_insert(r#"INSERT INTO t VALUES (1, parse_json('{"a":1}'))"#)
            .expect("convert command");
        let InsertCommandSource::Values(rows) = command.source else {
            panic!("constant parse_json must stay on the literal VALUES path");
        };
        let InsertValue::String(packed) = &rows[0][1] else {
            panic!("parse_json must produce packed variant bytes");
        };
        assert!(
            packed.chars().all(|ch| u32::from(ch) <= 0xff),
            "packed variant must preserve every byte through the Latin-1 bridge"
        );
        let unpacked = packed.chars().map(|ch| ch as u8).collect::<Vec<_>>();
        let expected =
            crate::query_execution::dml::insert::encode_insert_variant_json(r#"{"a":1}"#)
                .expect("encode expected variant");
        assert_eq!(unpacked, expected);
    }

    #[test]
    fn union_all_flattens_in_source_order() {
        let command =
            convert_insert("INSERT INTO t SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3")
                .expect("convert command");
        let InsertCommandSource::Values(rows) = command.source else {
            panic!("expected UNION ALL literals to normalize into one VALUES source");
        };
        assert_eq!(
            rows,
            vec![
                vec![InsertValue::Int(1)],
                vec![InsertValue::Int(2)],
                vec![InsertValue::Int(3)],
            ]
        );
    }

    #[test]
    fn union_distinct_is_rejected() {
        let error = convert_insert("INSERT INTO t SELECT 1 UNION SELECT 2").unwrap_err();
        assert!(error.contains("requires UNION ALL"), "{error}");
    }

    #[test]
    fn typed_dynamic_overwrite_uses_partition_field() {
        let command = convert_insert("INSERT OVERWRITE PARTITIONS TABLE db.t VALUES (1)")
            .expect("convert dynamic overwrite");
        assert_eq!(command.target.parts, vec!["db", "t"]);
        assert_eq!(
            command.overwrite_mode,
            InsertOverwriteMode::DynamicPartitions
        );
    }

    #[test]
    fn typed_insert_target_preserves_object_name_components() {
        let statement = parse_insert("INSERT INTO db.t SELECT remote('localhost')");
        assert_eq!(
            statement
                .target
                .parts
                .iter()
                .map(|part| part.value.as_str())
                .collect::<Vec<_>>(),
            vec!["db", "t"]
        );
    }
}
