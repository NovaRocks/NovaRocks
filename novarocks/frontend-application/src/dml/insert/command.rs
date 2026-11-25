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

use crate::query_execution::dml::insert::InsertTargetName;
use novarocks_parser::{ast, printer};

/// Frontend command retaining the parser's complete source and numeric text.
#[derive(Clone, Debug, PartialEq)]
pub struct InsertCommand {
    pub target: InsertTargetName,
    pub columns: Vec<String>,
    pub source: Box<ast::Query>,
}

pub(super) fn validate_insert_source(
    target_parts: &[String],
    source: &ast::Query,
) -> Result<(), String> {
    if target_parts.is_empty() {
        return Err("INSERT target is empty after overwrite normalization".to_string());
    }
    if admission_literal_query(source) {
        validate_literal_set_operations(&source.body)?;
    }
    Ok(())
}

/// Classify literal projections by syntax, without evaluating values or CASTs.
pub(crate) fn is_literal_expr(expr: &ast::Expr) -> bool {
    match expr {
        ast::Expr::Literal(_) => true,
        ast::Expr::Unary(unary) => {
            matches!(unary.operator, ast::UnaryOperator::Minus)
                && is_literal_expr(&unary.expression)
        }
        ast::Expr::Cast(cast) => is_literal_expr(&cast.expression),
        ast::Expr::Binary(binary) => {
            matches!(
                binary.operator,
                ast::BinaryOperator::Add
                    | ast::BinaryOperator::Subtract
                    | ast::BinaryOperator::Multiply
            ) && is_literal_expr(&binary.left)
                && is_literal_expr(&binary.right)
        }
        ast::Expr::Array(array) => array.values.iter().all(is_literal_expr),
        ast::Expr::Row(structure) => structure
            .fields
            .iter()
            .all(|field| is_literal_expr(&field.expression)),
        ast::Expr::FunctionCall(function) => {
            plain_function(function)
                && matches!(
                    printer::print_object_name(&function.name)
                        .to_ascii_lowercase()
                        .as_str(),
                    "array" | "map" | "row" | "named_struct" | "json_parse"
                )
                && function
                    .arguments
                    .iter()
                    .all(|argument| is_literal_expr(&argument.value))
        }
        _ => false,
    }
}

pub(crate) fn plain_function(function: &ast::FunctionCall) -> bool {
    !function.distinct
        && function.order_by.is_none()
        && function.filter.is_none()
        && function.null_treatment.is_none()
        && function.window.is_none()
        && function.processing_mode.is_none()
        && function
            .arguments
            .iter()
            .all(|argument| argument.name.is_none())
}

pub(crate) fn literal_query(query: &ast::Query) -> bool {
    query.functions.is_empty()
        && query.with.is_none()
        && query.modifiers.order_by.is_none()
        && query.modifiers.limit.is_none()
        && query.modifiers.offset.is_none()
        && literal_body(&query.body)
}

fn literal_body(body: &ast::SetExpr) -> bool {
    match body {
        ast::SetExpr::Table(_) => false,
        ast::SetExpr::Values(values) => {
            !values.rows.is_empty()
                && values.rows.iter().all(|row| {
                    let cells = ast::ValuesRowCells::new(row);
                    !cells.is_empty()
                        && !cells.has_named_fields()
                        && cells.iter().all(is_literal_expr)
                })
        }
        ast::SetExpr::Select(select) => {
            select.modifiers.is_empty()
                && select.from.is_none()
                && select.projection.iter().all(|item| match item {
                    ast::SelectItem::SingleColumn(column) => is_literal_expr(&column.expression),
                    _ => false,
                })
        }
        ast::SetExpr::Query(subquery) => {
            let query = &subquery.query;
            literal_query(query)
        }
        ast::SetExpr::SetOperation(operation) => operation.inputs.iter().all(literal_body),
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

fn admission_literal_shape(expr: &ast::Expr) -> Option<LiteralShape> {
    use LiteralShape::{Float, Integer, Other, String};
    match expr {
        ast::Expr::Literal(literal) => Some(match &literal.kind {
            ast::LiteralKind::Long { .. } => Integer,
            ast::LiteralKind::Double(_) | ast::LiteralKind::Decimal(_) => Float,
            ast::LiteralKind::String(_)
            | ast::LiteralKind::Binary(_)
            | ast::LiteralKind::Generic(_) => String,
            _ => Other,
        }),
        ast::Expr::Identifier(_) => Some(String),
        ast::Expr::Cast(cast) => {
            if matches!(
                match &cast.data_type.kind {
                    ast::TypeKind::Generic(value) => value.name.value.to_ascii_lowercase(),
                    _ => std::string::String::new(),
                }
                .as_str(),
                "decimal" | "decimal32" | "decimal64" | "decimal128" | "dec" | "numeric"
            ) {
                None
            } else {
                admission_literal_shape(&cast.expression)
            }
        }
        ast::Expr::Unary(unary) if matches!(unary.operator, ast::UnaryOperator::Minus) => {
            admission_literal_shape(&unary.expression)
                .filter(|kind| matches!(kind, Integer | Float | String))
        }
        ast::Expr::Binary(binary) => match (
            admission_literal_shape(&binary.left)?,
            binary.operator,
            admission_literal_shape(&binary.right)?,
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
            .values
            .iter()
            .all(|value| admission_literal_shape(value).is_some())
            .then_some(Other),
        ast::Expr::Row(structure) => structure
            .fields
            .iter()
            .all(|field| admission_literal_shape(&field.expression).is_some())
            .then_some(Other),
        ast::Expr::FunctionCall(function) if plain_function(function) => {
            let name = printer::print_object_name(&function.name).to_ascii_lowercase();
            if name == "json_parse" {
                return (function.arguments.len() == 1
                    && admission_literal_shape(&function.arguments[0].value) == Some(String))
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
                .all(|argument| admission_literal_shape(&argument.value).is_some())
                .then_some(Other)
        }
        _ => None,
    }
}

fn admission_literal_query(query: &ast::Query) -> bool {
    if !query.functions.is_empty()
        || query.with.is_some()
        || query.modifiers.order_by.is_some()
        || query.modifiers.limit.is_some()
        || query.modifiers.offset.is_some()
    {
        return false;
    }
    fn body_is_literal(body: &ast::SetExpr) -> bool {
        match body {
            ast::SetExpr::Table(_) => false,
            ast::SetExpr::Select(select) => {
                select.modifiers.is_empty()
                    && select.from.is_none()
                    && select.projection.iter().all(|item| match item {
                        ast::SelectItem::SingleColumn(column) => {
                            admission_literal_shape(&column.expression).is_some()
                        }
                        _ => false,
                    })
            }
            ast::SetExpr::Values(values) => {
                !values.rows.is_empty()
                    && values.rows.iter().all(|row| {
                        let cells = ast::ValuesRowCells::new(row);
                        !cells.is_empty()
                            && !cells.has_named_fields()
                            && cells
                                .iter()
                                .all(|value| admission_literal_shape(value).is_some())
                    })
            }
            ast::SetExpr::Query(subquery) => {
                let query = &subquery.query;
                admission_literal_query(query)
            }
            ast::SetExpr::SetOperation(operation) => operation.inputs.iter().all(body_is_literal),
        }
    }
    body_is_literal(&query.body)
}

fn validate_literal_set_operations(body: &ast::SetExpr) -> Result<(), String> {
    match body {
        ast::SetExpr::Table(_) => {
            Err("TABLE query bodies are not literal INSERT sources".to_string())
        }
        ast::SetExpr::SetOperation(operation) => {
            if !matches!(operation.operator, ast::SetOperator::Union) {
                return Err("INSERT SELECT set operation is only UNION ALL here".to_string());
            }
            if operation.distinct {
                return Err(
                    "INSERT SELECT UNION requires UNION ALL (UNION/UNION DISTINCT unsupported)"
                        .to_string(),
                );
            }
            if operation.corresponding.is_some() {
                return Err("INSERT SELECT CORRESPONDING is unsupported".to_string());
            }
            for input in &operation.inputs {
                validate_literal_set_operations(input)?;
            }
            Ok(())
        }
        ast::SetExpr::Query(subquery) => {
            let query = &subquery.query;
            validate_literal_set_operations(&query.body)
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn convert(sql: &str) -> Result<InsertCommand, String> {
        let ast::TrinoStatement::Insert(insert) =
            novarocks_parser::parser::parse_statement(sql).expect("parse INSERT")
        else {
            panic!("expected INSERT");
        };
        super::super::prepare_trino_insert_shape(insert, sql)
            .map(|(command, _)| command)
            .map_err(|error| error.to_string())
    }

    #[test]
    fn preserves_exact_numbers_and_explicit_casts() {
        let command = convert("INSERT INTO db.t(d) VALUES (123456789.123456789), (CAST(-123456789.123456789 AS DOUBLE))").unwrap();
        assert_eq!(command.target.parts, ["db", "t"]);
        assert_eq!(command.columns, ["d"]);
        let ast::SetExpr::Values(values) = command.source.body.as_ref() else {
            panic!("VALUES");
        };
        let ast::Expr::Literal(value) = ast::ValuesRowCells::new(&values.rows[0]).get(0).unwrap()
        else {
            panic!("numeric literal");
        };
        assert_eq!(
            value.kind,
            ast::LiteralKind::Decimal("123456789.123456789".into())
        );
        assert!(matches!(
            ast::ValuesRowCells::new(&values.rows[1]).get(0).unwrap(),
            ast::Expr::Cast(_)
        ));
    }

    #[test]
    fn retains_source_filters_limits_and_union_branches() {
        for sql in [
            "INSERT INTO t SELECT 40 + 2 WHERE false",
            "INSERT INTO t SELECT DISTINCT 1 LIMIT 0",
            "INSERT INTO t SELECT 1 UNION ALL SELECT 2",
            "INSERT INTO t SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3",
            "INSERT INTO t SELECT 1 UNION ALL (SELECT 2 UNION ALL SELECT 3)",
            "INSERT INTO t SELECT id FROM src ORDER BY id LIMIT 3",
            "INSERT INTO t VALUES (to_bitmap(11), hll_hash(5))",
        ] {
            let ast::TrinoStatement::Insert(insert) =
                novarocks_parser::parser::parse_statement(sql).unwrap()
            else {
                panic!("INSERT");
            };
            assert_eq!(*convert(sql).unwrap().source, *insert.query.as_ref());
        }
    }

    #[test]
    fn rejects_literal_union_distinct_but_preserves_general_query_boundary() {
        assert!(
            convert("INSERT INTO t SELECT 1 UNION SELECT 2")
                .unwrap_err()
                .contains("requires UNION ALL")
        );
        let mut source = convert("INSERT INTO t SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3")
            .unwrap()
            .source;
        let ast::SetExpr::SetOperation(operation) = source.body.as_mut() else {
            panic!("set operation");
        };
        operation.corresponding = Some(ast::Corresponding {
            columns: Vec::new(),
            span: operation.span,
        });
        assert!(
            validate_literal_set_operations(&source.body)
                .unwrap_err()
                .contains("CORRESPONDING is unsupported")
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
        let command = convert(r#"INSERT INTO t VALUES (json_parse('{"a":1}'))"#).unwrap();
        let ast::SetExpr::Values(values) = command.source.body.as_ref() else {
            panic!("VALUES");
        };
        assert!(matches!(
            ast::ValuesRowCells::new(&values.rows[0]).get(0).unwrap(),
            ast::Expr::FunctionCall(_)
        ));
    }

    #[test]
    fn current_json_parser_uses_the_original_literal_select_admission() {
        for text in ["42", "{"] {
            let command = convert(&format!("INSERT INTO t SELECT json_parse('{text}')")).unwrap();
            assert!(literal_query(&command.source));
            assert!(admission_literal_query(&command.source));
        }
        let error = convert("INSERT INTO t SELECT json_parse('42') UNION SELECT json_parse('43')")
            .unwrap_err();
        assert!(error.contains("requires UNION ALL"), "{error}");

        let old = convert("INSERT INTO t SELECT parse_json('42')").unwrap();
        assert!(!literal_query(&old.source));
        assert!(!admission_literal_query(&old.source));
    }
}
