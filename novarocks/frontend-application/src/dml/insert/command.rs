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

use crate::query_execution::dml::insert::{InsertOverwriteMode, InsertTargetName};
use novarocks_parser::{
    ast::{self, Insert},
    printer,
};

/// Frontend command retaining the parser's complete source and numeric text.
#[derive(Clone, Debug, PartialEq)]
pub struct InsertCommand {
    pub target: InsertTargetName,
    pub columns: Vec<String>,
    pub source: Box<ast::Query>,
    pub overwrite_mode: InsertOverwriteMode,
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

    if legacy_literal_query(&insert.source) {
        validate_literal_set_operations(&insert.source.body)?;
    }
    let source = Box::new(insert.source.clone());

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

/// Classify literal projections by syntax, without evaluating values or CASTs.
pub(crate) fn is_literal_expr(expr: &ast::Expr) -> bool {
    match expr {
        ast::Expr::Literal(_) | ast::Expr::TypedString(_) => true,
        ast::Expr::Nested(nested) => is_literal_expr(&nested.expression),
        ast::Expr::Unary(unary) => {
            matches!(unary.operator, ast::UnaryOperator::Minus)
                && is_literal_expr(&unary.expression)
        }
        ast::Expr::Cast(cast) => is_literal_expr(&cast.expr),
        ast::Expr::Binary(binary) => {
            matches!(
                binary.operator,
                ast::BinaryOperator::Add
                    | ast::BinaryOperator::Subtract
                    | ast::BinaryOperator::Multiply
            ) && is_literal_expr(&binary.left)
                && is_literal_expr(&binary.right)
        }
        ast::Expr::Array(array) => array.elements.iter().all(is_literal_expr),
        ast::Expr::Map(map) => map
            .entries
            .iter()
            .all(|entry| is_literal_expr(&entry.key) && is_literal_expr(&entry.value)),
        ast::Expr::Tuple(tuple) => tuple.expressions.iter().all(is_literal_expr),
        ast::Expr::Struct(structure) => structure
            .fields
            .iter()
            .all(|field| is_literal_expr(&field.value)),
        ast::Expr::FunctionCall(function) => {
            plain_function(function)
                && matches!(
                    printer::print_object_name(&function.name)
                        .to_ascii_lowercase()
                        .as_str(),
                    "array" | "map" | "row" | "named_struct" | "parse_json"
                )
                && function.arguments.iter().all(is_literal_expr)
        }
        _ => false,
    }
}

pub(crate) fn plain_function(function: &ast::FunctionCall) -> bool {
    matches!(function.quantifier, ast::FunctionQuantifier::None)
        && function.order_by.is_empty()
        && function.separator.is_none()
        && function.filter.is_none()
        && function.null_treatment.is_none()
        && function.over.is_none()
}

pub(crate) fn literal_query(query: &ast::Query) -> bool {
    query.with.is_none()
        && query.order_by.is_empty()
        && query.limit.is_none()
        && query.offset.is_none()
        && query.fetch.is_none()
        && literal_body(&query.body)
}

fn literal_body(body: &ast::SetExpr) -> bool {
    match body {
        ast::SetExpr::Values(values) => values.rows.iter().flatten().all(is_literal_expr),
        ast::SetExpr::Select(select) => {
            select.from.is_empty()
                && select.projection.iter().all(|item| match item {
                    ast::SelectItem::UnnamedExpr(expr)
                    | ast::SelectItem::ExprWithAlias { expr, .. } => is_literal_expr(expr),
                    _ => false,
                })
        }
        ast::SetExpr::Query(query) => literal_query(query),
        ast::SetExpr::SetOperation(operation) => {
            literal_body(&operation.left) && literal_body(&operation.right)
        }
    }
}

// Admission preserves the former literal-source grammar without evaluating
// constants. Shaping is broader: SQL still owns all numeric values and CASTs.
#[derive(Clone, Copy, PartialEq)]
enum LiteralShape {
    Integer,
    Float,
    String,
    Other,
}

fn legacy_literal_shape(expr: &ast::Expr) -> Option<LiteralShape> {
    use LiteralShape::{Float, Integer, Other, String};
    match expr {
        ast::Expr::Literal(literal) => Some(match &literal.kind {
            ast::LiteralKind::Number(text) if text.contains(['.', 'e', 'E']) => Float,
            ast::LiteralKind::Number(text) if text.parse::<i64>().is_ok() => Integer,
            ast::LiteralKind::Number(_)
            | ast::LiteralKind::String(_)
            | ast::LiteralKind::HexString(_) => String,
            _ => Other,
        }),
        ast::Expr::TypedString(_) | ast::Expr::Identifier(_) => Some(String),
        ast::Expr::Nested(nested) => legacy_literal_shape(&nested.expression),
        ast::Expr::Cast(cast) => {
            if matches!(
                printer::print_object_name(&cast.data_type.name)
                    .to_ascii_lowercase()
                    .as_str(),
                "decimal" | "decimal32" | "decimal64" | "decimal128" | "dec" | "numeric"
            ) {
                None
            } else {
                legacy_literal_shape(&cast.expr)
            }
        }
        ast::Expr::Unary(unary) if matches!(unary.operator, ast::UnaryOperator::Minus) => {
            legacy_literal_shape(&unary.expression)
                .filter(|kind| matches!(kind, Integer | Float | String))
        }
        ast::Expr::Binary(binary) => match (
            legacy_literal_shape(&binary.left)?,
            binary.operator,
            legacy_literal_shape(&binary.right)?,
        ) {
            (
                Integer,
                ast::BinaryOperator::Add
                | ast::BinaryOperator::Subtract
                | ast::BinaryOperator::Multiply,
                Integer,
            ) => Some(Integer),
            (Float, ast::BinaryOperator::Add | ast::BinaryOperator::Subtract, Float) => Some(Float),
            _ => None,
        },
        ast::Expr::Array(array) => array
            .elements
            .iter()
            .all(|value| legacy_literal_shape(value).is_some())
            .then_some(Other),
        ast::Expr::Map(map) => map
            .entries
            .iter()
            .all(|entry| {
                legacy_literal_shape(&entry.key).is_some()
                    && legacy_literal_shape(&entry.value).is_some()
            })
            .then_some(Other),
        ast::Expr::Tuple(tuple) => tuple
            .expressions
            .iter()
            .all(|value| legacy_literal_shape(value).is_some())
            .then_some(Other),
        ast::Expr::Struct(structure) => structure
            .fields
            .iter()
            .all(|field| legacy_literal_shape(&field.value).is_some())
            .then_some(Other),
        ast::Expr::FunctionCall(function) if plain_function(function) => {
            let name = printer::print_object_name(&function.name).to_ascii_lowercase();
            if name == "parse_json" {
                return (function.arguments.len() == 1
                    && legacy_literal_shape(&function.arguments[0]) == Some(String))
                .then_some(String);
            }
            if !matches!(name.as_str(), "array" | "row" | "map" | "named_struct")
                || (matches!(name.as_str(), "map" | "named_struct")
                    && function.arguments.len() % 2 != 0)
            {
                return None;
            }
            function
                .arguments
                .iter()
                .all(|value| legacy_literal_shape(value).is_some())
                .then_some(Other)
        }
        _ => None,
    }
}

fn legacy_literal_query(query: &ast::Query) -> bool {
    if query.with.is_some()
        || !query.order_by.is_empty()
        || query.limit.is_some()
        || query.offset.is_some()
        || query.fetch.is_some()
    {
        return false;
    }
    fn body_is_literal(body: &ast::SetExpr) -> bool {
        match body {
            ast::SetExpr::Select(select) => {
                select.from.is_empty()
                    && select.projection.iter().all(|item| match item {
                        ast::SelectItem::UnnamedExpr(expr)
                        | ast::SelectItem::ExprWithAlias { expr, .. } => {
                            legacy_literal_shape(expr).is_some()
                        }
                        _ => false,
                    })
            }
            ast::SetExpr::Values(values) => values
                .rows
                .iter()
                .flatten()
                .all(|value| legacy_literal_shape(value).is_some()),
            ast::SetExpr::Query(query) => legacy_literal_query(query),
            ast::SetExpr::SetOperation(operation) => {
                body_is_literal(&operation.left) && body_is_literal(&operation.right)
            }
        }
    }
    body_is_literal(&query.body)
}

fn validate_literal_set_operations(body: &ast::SetExpr) -> Result<(), String> {
    match body {
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
            validate_literal_set_operations(&operation.left)?;
            validate_literal_set_operations(&operation.right)
        }
        ast::SetExpr::Query(query) => validate_literal_set_operations(&query.body),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn convert(sql: &str) -> Result<InsertCommand, String> {
        let statements = novarocks_parser::parse(sql).expect("parse INSERT");
        let [ast::Statement::Dml(ast::DmlStatement::Insert(insert))] = statements.as_slice() else {
            panic!("expected INSERT");
        };
        convert_insert_command(insert)
    }

    #[test]
    fn preserves_exact_numbers_and_explicit_casts() {
        let command = convert("INSERT INTO db.t(d) VALUES (123456789.123456789), (CAST(-123456789.123456789 AS DOUBLE))").unwrap();
        assert_eq!(command.target.parts, ["db", "t"]);
        assert_eq!(command.columns, ["d"]);
        let ast::SetExpr::Values(values) = command.source.body.as_ref() else {
            panic!("VALUES");
        };
        let ast::Expr::Literal(value) = &values.rows[0][0] else {
            panic!("numeric literal");
        };
        assert_eq!(
            value.kind,
            ast::LiteralKind::Number("123456789.123456789".into())
        );
        assert!(matches!(&values.rows[1][0], ast::Expr::Cast(_)));
    }

    #[test]
    fn retains_source_filters_limits_and_union_branches() {
        for sql in [
            "INSERT INTO t SELECT 40 + 2 WHERE false",
            "INSERT INTO t SELECT DISTINCT 1 LIMIT 0",
            "INSERT INTO t SELECT 1 UNION ALL SELECT 2",
            "INSERT INTO t SELECT id FROM src ORDER BY id LIMIT 3",
            "INSERT INTO t VALUES (to_bitmap(11), hll_hash(5))",
        ] {
            let statements = novarocks_parser::parse(sql).unwrap();
            let [ast::Statement::Dml(ast::DmlStatement::Insert(insert))] = statements.as_slice()
            else {
                panic!("INSERT");
            };
            assert_eq!(
                *convert_insert_command(insert).unwrap().source,
                insert.source
            );
        }
    }

    #[test]
    fn rejects_literal_union_distinct_but_preserves_general_query_boundary() {
        assert!(
            convert("INSERT INTO t SELECT 1 UNION SELECT 2")
                .unwrap_err()
                .contains("requires UNION ALL")
        );
        assert!(convert("INSERT INTO t SELECT id FROM src UNION SELECT id FROM other").is_ok());
        assert!(
            convert(
                "INSERT INTO t SELECT CAST(1 AS DECIMAL(5,2)) UNION SELECT CAST(1 AS DECIMAL(5,2))"
            )
            .is_ok()
        );
    }

    #[test]
    fn keeps_general_numeric_union_admission() {
        for sql in [
            "INSERT INTO t SELECT CAST(1 AS DECIMAL(5,2)) UNION SELECT CAST(1 AS DECIMAL(5,2))",
            "INSERT INTO t SELECT 1 + 1.5 UNION SELECT 2.5",
            "INSERT INTO t SELECT 1.5 * 2.0 UNION SELECT 3.0",
        ] {
            assert!(convert(sql).is_ok(), "{sql}");
        }
        for sql in [
            "INSERT INTO t SELECT 1 + 2 UNION SELECT 3",
            "INSERT INTO t SELECT 1.5 + 2.5 UNION SELECT 4.0",
        ] {
            assert!(
                convert(sql).unwrap_err().contains("requires UNION ALL"),
                "{sql}"
            );
        }
    }

    #[test]
    fn retains_variant_function_for_target_representation() {
        let command = convert(r#"INSERT INTO t VALUES (parse_json('{"a":1}'))"#).unwrap();
        let ast::SetExpr::Values(values) = command.source.body.as_ref() else {
            panic!("VALUES");
        };
        assert!(matches!(&values.rows[0][0], ast::Expr::FunctionCall(_)));
    }

    #[test]
    fn preserves_dynamic_overwrite_target() {
        let command = convert("INSERT OVERWRITE PARTITIONS TABLE db.t VALUES (1)").unwrap();
        assert_eq!(command.target.parts, ["db", "t"]);
        assert_eq!(
            command.overwrite_mode,
            InsertOverwriteMode::DynamicPartitions
        );
    }
}
