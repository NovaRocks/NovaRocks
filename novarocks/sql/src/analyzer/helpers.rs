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

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields};
use novarocks_parser::{ast, printer};

use crate::analyze_error::AnalyzeError;
use novarocks_types::logical::{LogicalType, field_with_logical_type};

// ---------------------------------------------------------------------------
// SQL type -> Arrow type conversion
// ---------------------------------------------------------------------------

/// Build the `Field` for a nested JSON cell, tagging its metadata so the
/// downstream type-desc walker can re-emit `TPrimitiveType::JSON`.
fn nested_field_with_logical_type(
    name: &str,
    sql_type: &novarocks_parser::ast::TypeName,
    nullable: bool,
) -> Result<Field, AnalyzeError> {
    let arrow = sql_type_to_arrow(sql_type)?;
    let mut field = Field::new(name, arrow, nullable);
    if is_json_sql_type(sql_type) {
        field = field_with_logical_type(field, LogicalType::Json);
    }
    Ok(field)
}

fn is_json_sql_type(sql_type: &novarocks_parser::ast::TypeName) -> bool {
    sql_type
        .name
        .parts
        .last()
        .is_some_and(|part| matches!(part.value.to_ascii_lowercase().as_str(), "json" | "jsonb"))
}

pub(super) fn sql_type_to_arrow(
    sql_type: &novarocks_parser::ast::TypeName,
) -> Result<DataType, AnalyzeError> {
    let type_name = sql_type
        .name
        .parts
        .last()
        .map(|part| part.value.to_ascii_lowercase())
        .ok_or_else(|| {
            AnalyzeError::invalid_query_shape("CAST target type has no name", sql_type.span)
        })?;
    let type_args = &sql_type.arguments;
    match type_name.as_str() {
        "tinyint" => Ok(DataType::Int8),
        "smallint" => Ok(DataType::Int16),
        "int" | "integer" => Ok(DataType::Int32),
        "bigint" => Ok(DataType::Int64),
        "float" | "real" => Ok(DataType::Float32),
        "double" | "double precision" => Ok(DataType::Float64),
        "boolean" | "bool" => Ok(DataType::Boolean),
        "varchar" | "char" | "character" | "string" | "text" => Ok(DataType::Utf8),
        "json" | "jsonb" => Ok(DataType::Utf8),
        "varbinary" | "binary" => Ok(DataType::Binary),
        "date" => Ok(DataType::Date32),
        "datetime" | "timestamp" | "timestamptz" => Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            None,
        )),
        "time" => Ok(DataType::Time64(arrow::datatypes::TimeUnit::Microsecond)),
        "largeint" => Ok(DataType::FixedSizeBinary(
            novarocks_types::largeint::LARGEINT_BYTE_WIDTH,
        )),
        "variant" => Ok(DataType::LargeBinary),
        "datetime_ns" | "timestamp_ns" | "timestamptz_ns" => Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Nanosecond,
            None,
        )),
        "decimal" | "dec" | "numeric" | "decimal32" | "decimal64" | "decimal128" => {
            if type_args.len() > 2 {
                return Err(AnalyzeError::invalid_argument(
                    "DECIMAL type accepts at most precision and scale",
                    sql_type.span,
                ));
            }
            // Generic SQL DECIMAL chooses its carrier by precision. Explicit
            // width aliases keep their declared domain; small decimals retain
            // the engine's existing Decimal128 representation.
            let (max_precision, default_precision) = match type_name.as_str() {
                "decimal32" => (9_u64, 9_u64),
                "decimal64" => (18, 18),
                "decimal128" => (38, 38),
                _ => (76, 38),
            };
            let precision = type_numeric_argument(type_args, 0)?.unwrap_or(default_precision);
            let scale = type_numeric_argument(type_args, 1)?.unwrap_or(0);
            if precision == 0 || precision > max_precision {
                return Err(AnalyzeError::invalid_argument(
                    format!(
                        "{type_name} precision {precision} must be between 1 and {max_precision}"
                    ),
                    sql_type.span,
                ));
            }
            if scale > precision {
                return Err(AnalyzeError::invalid_argument(
                    format!(
                        "{type_name} scale {scale} must be between 0 and precision {precision}"
                    ),
                    sql_type.span,
                ));
            }
            let precision = u8::try_from(precision).map_err(|_| {
                AnalyzeError::invalid_argument(
                    "DECIMAL precision exceeds its metadata domain",
                    sql_type.span,
                )
            })?;
            let scale = i8::try_from(scale).map_err(|_| {
                AnalyzeError::invalid_argument(
                    "DECIMAL scale exceeds its metadata domain",
                    sql_type.span,
                )
            })?;
            if precision > 38 {
                Ok(DataType::Decimal256(precision, scale))
            } else {
                Ok(DataType::Decimal128(precision, scale))
            }
        }
        "array" => {
            let element = type_type_argument(type_args, 0, "ARRAY", sql_type.span)?;
            Ok(DataType::List(Arc::new(nested_field_with_logical_type(
                "item", element, true,
            )?)))
        }
        "map" => {
            let key = type_type_argument(type_args, 0, "MAP", sql_type.span)?;
            let value = type_type_argument(type_args, 1, "MAP", sql_type.span)?;
            let key = nested_field_with_logical_type("key", key, true)?;
            let value = nested_field_with_logical_type("value", value, true)?;
            Ok(DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![Arc::new(key), Arc::new(value)])),
                    false,
                )),
                false,
            ))
        }
        "struct" => {
            let fields = type_args
                .iter()
                .enumerate()
                .map(|(index, argument)| match argument {
                    novarocks_parser::ast::TypeNameArgument::Field(field) => Ok(Arc::new(
                        nested_field_with_logical_type(&field.name.value, &field.data_type, true)?,
                    )),
                    novarocks_parser::ast::TypeNameArgument::Type(data_type) => {
                        Ok(Arc::new(nested_field_with_logical_type(
                            &format!("f{}", index + 1),
                            data_type,
                            true,
                        )?))
                    }
                    novarocks_parser::ast::TypeNameArgument::Literal(_) => {
                        Err(AnalyzeError::invalid_argument(
                            "STRUCT type field must include a type name",
                            type_name_argument_span(argument),
                        ))
                    }
                })
                .collect::<Result<Vec<_>, AnalyzeError>>()?;
            Ok(DataType::Struct(Fields::from(fields)))
        }
        _ => Err(AnalyzeError::unsupported_query_shape(
            format!("unsupported SQL type: {type_name}"),
            sql_type.span,
        )),
    }
}

fn type_numeric_argument(
    arguments: &[novarocks_parser::ast::TypeNameArgument],
    index: usize,
) -> Result<Option<u64>, AnalyzeError> {
    let Some(argument) = arguments.get(index) else {
        return Ok(None);
    };
    let novarocks_parser::ast::TypeNameArgument::Literal(literal) = argument else {
        return Err(AnalyzeError::invalid_literal(
            "numeric type parameter must be a literal",
            type_name_argument_span(argument),
        ));
    };
    let novarocks_parser::ast::LiteralKind::Number(value) = &literal.kind else {
        return Err(AnalyzeError::invalid_literal(
            "numeric type parameter must be an integer literal",
            literal.span,
        ));
    };
    value.parse::<u64>().map(Some).map_err(|error| {
        AnalyzeError::invalid_literal(
            format!("invalid numeric type parameter `{value}`: {error}"),
            literal.span,
        )
    })
}

fn type_type_argument<'a>(
    arguments: &'a [novarocks_parser::ast::TypeNameArgument],
    index: usize,
    kind: &str,
    type_span: novarocks_parser::Span,
) -> Result<&'a novarocks_parser::ast::TypeName, AnalyzeError> {
    let Some(novarocks_parser::ast::TypeNameArgument::Type(data_type)) = arguments.get(index)
    else {
        return Err(AnalyzeError::invalid_argument(
            format!("{kind} type requires a type parameter"),
            arguments
                .get(index)
                .map(type_name_argument_span)
                .unwrap_or(type_span),
        ));
    };
    Ok(data_type)
}

fn type_name_argument_span(
    argument: &novarocks_parser::ast::TypeNameArgument,
) -> novarocks_parser::Span {
    match argument {
        novarocks_parser::ast::TypeNameArgument::Type(type_name) => type_name.span,
        novarocks_parser::ast::TypeNameArgument::Literal(literal) => literal.span,
        novarocks_parser::ast::TypeNameArgument::Field(field) => field.span,
    }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Expression display name
// ---------------------------------------------------------------------------

pub(super) fn expr_display_name(expr: &ast::Expr) -> String {
    match expr {
        ast::Expr::Nested(nested) => expr_display_name(&nested.expression),
        ast::Expr::Literal(literal) => format_literal_display_name(literal),
        ast::Expr::Identifier(ident) => ident.value.clone(),
        ast::Expr::CompoundIdentifier(identifier) if !identifier.parts.is_empty() => identifier
            .parts
            .last()
            .expect("non-empty compound identifier")
            .value
            .clone(),
        ast::Expr::Access(access) => format_access_display_name(access),
        ast::Expr::Array(array) => format!(
            "[{}]",
            array
                .elements
                .iter()
                .map(expr_display_name)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        ast::Expr::Map(map) => format!(
            "map{{{}}}",
            map.entries
                .iter()
                .map(|entry| format!(
                    "{}:{}",
                    expr_display_name(&entry.key),
                    expr_display_name(&entry.value)
                ))
                .collect::<Vec<_>>()
                .join(",")
        ),
        ast::Expr::Lambda(lambda) => {
            let parameters = lambda
                .parameters
                .iter()
                .map(|parameter| parameter.value.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            let parameters = if lambda.parameters.len() == 1 {
                parameters
            } else {
                format!("({parameters})")
            };
            format!(
                "{parameters} -> {}",
                expr_display_name_preserve_path(&lambda.body)
            )
        }
        ast::Expr::FunctionCall(function) => format_function_display_name(function),
        ast::Expr::IsPredicate(predicate) => format_is_predicate_display_name(predicate),
        ast::Expr::Cast(cast) => {
            if type_name_is(&cast.data_type, "array")
                && matches!(cast.expr.as_ref(), ast::Expr::Array(_))
            {
                return expr_display_name(&cast.expr);
            }
            format!(
                "CAST({} AS {})",
                expr_display_name_with_parens(&cast.expr),
                format_cast_type_for_expression(&cast.data_type, &cast.expr)
            )
        }
        ast::Expr::Binary(binary) => format!(
            "{} {} {}",
            expr_display_name_with_parens(&binary.left),
            binary_operator_display(binary.operator),
            expr_display_name_with_parens(&binary.right)
        ),
        ast::Expr::InList(in_list) => {
            let not = if in_list.negated { " NOT" } else { "" };
            let items = in_list
                .list
                .iter()
                .map(expr_display_name_with_parens)
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "{}{} IN ({items})",
                expr_display_name_with_parens(&in_list.expr),
                not
            )
        }
        ast::Expr::InSubquery(in_subquery) => {
            let not = if in_subquery.negated { " NOT" } else { "" };
            format!(
                "{}{} IN ((({})))",
                expr_display_name_with_parens(&in_subquery.expr),
                not,
                format_subquery_display_name(&in_subquery.query)
            )
        }
        other => lowercase_leading_keyword(&printer::print_expr(other)),
    }
}

fn format_subquery_display_name(query: &ast::Query) -> String {
    let mut query = query.clone();
    mark_query_aliases_explicit(&mut query);
    printer::print_query(&query)
}

fn mark_query_aliases_explicit(query: &mut ast::Query) {
    if let Some(with) = &mut query.with {
        for cte in &mut with.ctes {
            mark_query_aliases_explicit(&mut cte.query);
        }
    }
    mark_set_expr_aliases_explicit(&mut query.body);
}

fn mark_set_expr_aliases_explicit(set_expr: &mut ast::SetExpr) {
    match set_expr {
        ast::SetExpr::Select(select) => {
            for table_with_joins in &mut select.from {
                mark_table_with_joins_aliases_explicit(table_with_joins);
            }
        }
        ast::SetExpr::Query(query) => mark_query_aliases_explicit(query),
        ast::SetExpr::SetOperation(operation) => {
            mark_set_expr_aliases_explicit(&mut operation.left);
            mark_set_expr_aliases_explicit(&mut operation.right);
        }
        ast::SetExpr::Values(_) => {}
    }
}

fn mark_table_with_joins_aliases_explicit(table_with_joins: &mut ast::TableWithJoins) {
    mark_table_factor_aliases_explicit(&mut table_with_joins.relation);
    for join in &mut table_with_joins.joins {
        mark_table_factor_aliases_explicit(&mut join.relation);
    }
}

fn mark_table_factor_aliases_explicit(factor: &mut ast::TableFactor) {
    match factor {
        ast::TableFactor::Table { alias, .. }
        | ast::TableFactor::TableFunction { alias, .. }
        | ast::TableFactor::Unnest { alias, .. } => {
            if let Some(alias) = alias {
                alias.explicit_as = true;
            }
        }
        ast::TableFactor::Derived {
            subquery, alias, ..
        } => {
            if let Some(alias) = alias {
                alias.explicit_as = true;
            }
            mark_query_aliases_explicit(subquery);
        }
        ast::TableFactor::NestedJoin {
            table_with_joins,
            alias,
            ..
        } => {
            if let Some(alias) = alias {
                alias.explicit_as = true;
            }
            mark_table_with_joins_aliases_explicit(table_with_joins);
        }
    }
}

fn format_access_display_name(access: &ast::AccessExpr) -> String {
    if let ast::AccessKind::Field(field) = &access.kind
        && matches!(
            access.expr.as_ref(),
            ast::Expr::Identifier(_) | ast::Expr::CompoundIdentifier(_)
        )
    {
        return field.value.clone();
    }

    let base = expr_display_name_preserve_path(&access.expr);
    match &access.kind {
        ast::AccessKind::Field(field) => format!("{base}.{}", field.value),
        ast::AccessKind::Subscript(index) => format!("{base}[{}]", expr_display_name(index)),
        ast::AccessKind::Json { operator, path } => {
            let operator = match operator {
                ast::JsonOperator::Arrow => "->",
                ast::JsonOperator::ArrowText => "->>",
            };
            format!(
                "{base} {operator} {}",
                expr_display_name_preserve_path(path)
            )
        }
    }
}
fn expr_display_name_preserve_path(expr: &ast::Expr) -> String {
    match expr {
        ast::Expr::Nested(nested) => expr_display_name_preserve_path(&nested.expression),
        ast::Expr::CompoundIdentifier(identifier) => identifier
            .parts
            .iter()
            .map(|part| part.value.as_str())
            .collect::<Vec<_>>()
            .join("."),
        ast::Expr::Access(access) => format_access_display_name_preserve_path(access),
        _ => expr_display_name(expr),
    }
}

fn format_access_display_name_preserve_path(access: &ast::AccessExpr) -> String {
    let base = expr_display_name_preserve_path(&access.expr);
    match &access.kind {
        ast::AccessKind::Field(field) => format!("{base}.{}", field.value),
        ast::AccessKind::Subscript(index) => format!("{base}[{}]", expr_display_name(index)),
        ast::AccessKind::Json { operator, path } => {
            let operator = match operator {
                ast::JsonOperator::Arrow => "->",
                ast::JsonOperator::ArrowText => "->>",
            };
            format!(
                "{base} {operator} {}",
                expr_display_name_preserve_path(path)
            )
        }
    }
}

fn format_literal_display_name(literal: &ast::Literal) -> String {
    match &literal.kind {
        ast::LiteralKind::String(value) => format!("'{}'", value.replace('\'', "''")),
        ast::LiteralKind::Boolean(true) => "TRUE".to_string(),
        ast::LiteralKind::Boolean(false) => "FALSE".to_string(),
        ast::LiteralKind::Null => "NULL".to_string(),
        ast::LiteralKind::Number(value) => value.clone(),
        ast::LiteralKind::HexString(value) => format!("X'{value}'"),
    }
}

fn expr_display_name_with_parens(expr: &ast::Expr) -> String {
    match expr {
        ast::Expr::Identifier(_) | ast::Expr::CompoundIdentifier(_) | ast::Expr::Literal(_) => {
            expr_display_name(expr)
        }
        ast::Expr::Unary(unary)
            if unary.operator == ast::UnaryOperator::Minus
                && matches!(unary.expression.as_ref(), ast::Expr::Literal(_)) =>
        {
            expr_display_name(expr)
        }
        ast::Expr::Nested(nested) => expr_display_name_with_parens(&nested.expression),
        _ => format!("({})", expr_display_name(expr)),
    }
}

fn format_is_predicate_display_name(predicate: &ast::IsPredicateExpr) -> String {
    let suffix = match predicate.predicate {
        ast::IsPredicate::Null => "IS NULL",
        ast::IsPredicate::NotNull => "IS NOT NULL",
        ast::IsPredicate::True => "IS TRUE",
        ast::IsPredicate::NotTrue => "IS NOT TRUE",
        ast::IsPredicate::False => "IS FALSE",
        ast::IsPredicate::NotFalse => "IS NOT FALSE",
        ast::IsPredicate::Unknown => "IS UNKNOWN",
        ast::IsPredicate::NotUnknown => "IS NOT UNKNOWN",
    };
    format!(
        "{} {suffix}",
        expr_display_name_with_parens(&predicate.expr)
    )
}

fn binary_operator_display(operator: ast::BinaryOperator) -> &'static str {
    match operator {
        ast::BinaryOperator::NamedArgument => "=>",
        ast::BinaryOperator::Or => "OR",
        ast::BinaryOperator::And => "AND",
        ast::BinaryOperator::Equal => "=",
        ast::BinaryOperator::NullSafeEqual => "<=>",
        ast::BinaryOperator::NotEqual => "!=",
        ast::BinaryOperator::LessThan => "<",
        ast::BinaryOperator::LessThanOrEqual => "<=",
        ast::BinaryOperator::GreaterThan => ">",
        ast::BinaryOperator::GreaterThanOrEqual => ">=",
        ast::BinaryOperator::Add => "+",
        ast::BinaryOperator::Subtract => "-",
        ast::BinaryOperator::Multiply => "*",
        ast::BinaryOperator::Divide => "/",
        ast::BinaryOperator::Modulo => "%",
        ast::BinaryOperator::BitwiseAnd => "&",
        ast::BinaryOperator::BitwiseOr => "|",
        ast::BinaryOperator::BitwiseXor => "^",
        ast::BinaryOperator::ShiftLeft => "<<",
        ast::BinaryOperator::ShiftRight => ">>",
        ast::BinaryOperator::StringConcat => "||",
        ast::BinaryOperator::IsDistinctFrom => "IS DISTINCT FROM",
        ast::BinaryOperator::IsNotDistinctFrom => "IS NOT DISTINCT FROM",
    }
}

fn type_name_is(data_type: &ast::TypeName, expected: &str) -> bool {
    data_type
        .name
        .parts
        .last()
        .is_some_and(|part| part.value.eq_ignore_ascii_case(expected))
}

fn format_cast_type(data_type: &ast::TypeName) -> String {
    let name = data_type
        .name
        .parts
        .last()
        .map(|part| part.value.to_ascii_lowercase())
        .unwrap_or_default();
    match name.as_str() {
        "decimal" | "dec" | "numeric" | "decimal32" | "decimal64" | "decimal128" => {
            let precision = numeric_type_argument(data_type, 0).unwrap_or(38);
            let scale = numeric_type_argument(data_type, 1).unwrap_or(0);
            format!("{}({precision},{scale})", decimal_kind(precision))
        }
        "tinyint" => "TINYINT".to_string(),
        "smallint" => "SMALLINT".to_string(),
        "bigint" => "BIGINT".to_string(),
        "largeint" => "LARGEINT".to_string(),
        "string" => "VARCHAR(65533)".to_string(),
        "binary" | "varbinary" => "VARBINARY".to_string(),
        "int" | "integer" => "INT".to_string(),
        "array" => format!(
            "ARRAY<{}>",
            data_type
                .arguments
                .first()
                .and_then(type_name_argument_as_type)
                .map(format_cast_type)
                .unwrap_or_default()
        ),
        "map" => format!(
            "MAP<{}>",
            data_type
                .arguments
                .iter()
                .filter_map(type_name_argument_as_type)
                .map(format_cast_type)
                .collect::<Vec<_>>()
                .join(",")
        ),
        "struct" => format!(
            "struct<{}>",
            data_type
                .arguments
                .iter()
                .map(format_struct_type_argument)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        _ => printer::print_type_name(data_type),
    }
}

fn format_cast_type_for_expression(data_type: &ast::TypeName, expression: &ast::Expr) -> String {
    if is_parse_json_call(expression) {
        format_json_cast_type(data_type)
    } else {
        format_cast_type(data_type)
    }
}

fn is_parse_json_call(expression: &ast::Expr) -> bool {
    let ast::Expr::FunctionCall(function) = expression else {
        return false;
    };
    function
        .name
        .parts
        .last()
        .is_some_and(|part| part.value.eq_ignore_ascii_case("parse_json"))
}

fn format_json_cast_type(data_type: &ast::TypeName) -> String {
    let name = data_type
        .name
        .parts
        .last()
        .map(|part| part.value.to_ascii_lowercase())
        .unwrap_or_default();
    match name.as_str() {
        "array" => format!(
            "ARRAY<{}>",
            data_type
                .arguments
                .first()
                .and_then(type_name_argument_as_type)
                .map(format_json_cast_type)
                .unwrap_or_default()
        ),
        "map" => format!(
            "MAP<{}>",
            data_type
                .arguments
                .iter()
                .filter_map(type_name_argument_as_type)
                .map(format_json_cast_type)
                .collect::<Vec<_>>()
                .join(",")
        ),
        "struct" => format!(
            "struct<{}>",
            data_type
                .arguments
                .iter()
                .map(format_json_struct_type_argument)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        _ => format_cast_type(data_type),
    }
}

fn numeric_type_argument(data_type: &ast::TypeName, index: usize) -> Option<u64> {
    match data_type.arguments.get(index) {
        Some(ast::TypeNameArgument::Literal(ast::Literal {
            kind: ast::LiteralKind::Number(value),
            ..
        })) => value.parse().ok(),
        _ => None,
    }
}

fn type_name_argument_as_type(argument: &ast::TypeNameArgument) -> Option<&ast::TypeName> {
    match argument {
        ast::TypeNameArgument::Type(data_type) => Some(data_type),
        _ => None,
    }
}

fn format_struct_type_argument(argument: &ast::TypeNameArgument) -> String {
    match argument {
        ast::TypeNameArgument::Field(field) => {
            format!(
                "{} {}",
                field.name.value,
                format_cast_type(&field.data_type)
            )
        }
        ast::TypeNameArgument::Type(data_type) => format_cast_type(data_type),
        ast::TypeNameArgument::Literal(literal) => format_literal_display_name(literal),
    }
}

fn format_json_struct_type_argument(argument: &ast::TypeNameArgument) -> String {
    match argument {
        ast::TypeNameArgument::Field(field) => format!(
            "{} {}",
            field.name.value,
            format_json_struct_field_cast_type(&field.data_type)
        ),
        ast::TypeNameArgument::Type(data_type) => format_json_struct_field_cast_type(data_type),
        ast::TypeNameArgument::Literal(literal) => format_literal_display_name(literal),
    }
}

fn format_json_struct_field_cast_type(data_type: &ast::TypeName) -> String {
    let name = data_type
        .name
        .parts
        .last()
        .map(|part| part.value.to_ascii_lowercase())
        .unwrap_or_default();
    match name.as_str() {
        "tinyint" => "tinyint(4)".to_string(),
        "smallint" => "smallint(6)".to_string(),
        "int" | "integer" => "int(11)".to_string(),
        "bigint" => "bigint(20)".to_string(),
        "string" => "varchar(65533)".to_string(),
        "binary" | "varbinary" => "varbinary".to_string(),
        "array" => format!(
            "array<{}>",
            data_type
                .arguments
                .first()
                .and_then(type_name_argument_as_type)
                .map(format_json_struct_field_cast_type)
                .unwrap_or_default()
        ),
        "map" => format!(
            "map<{}>",
            data_type
                .arguments
                .iter()
                .filter_map(type_name_argument_as_type)
                .map(format_json_struct_field_cast_type)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        "struct" => format!(
            "struct<{}>",
            data_type
                .arguments
                .iter()
                .map(format_json_struct_type_argument)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        _ => format_cast_type(data_type),
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

fn canonical_display_function_name(name: &str) -> String {
    match name.to_ascii_lowercase().as_str() {
        "boolor_agg" => "bool_or".to_string(),
        "booland_agg" | "every" => "bool_and".to_string(),
        "string_agg" => "group_concat".to_string(),
        "array_agg_distinct" => "array_agg".to_string(),
        "approx_count_distinct_hll_sketch" => "ds_hll_count_distinct".to_string(),
        "struct" => "row".to_string(),
        other => other.to_string(),
    }
}

fn format_function_display_name(function: &ast::FunctionCall) -> String {
    let original_name = function
        .name
        .parts
        .last()
        .map(|part| part.value.to_ascii_lowercase())
        .unwrap_or_default();
    let canonical_name = canonical_display_function_name(&original_name);
    if canonical_name == "map" {
        return format_map_display_name(function);
    }
    if canonical_name == "element_at" && function.arguments.len() == 2 {
        return format!(
            "{}[{}]",
            expr_display_name_preserve_path(&function.arguments[0]),
            expr_display_name(&function.arguments[1])
        );
    }

    let distinct = matches!(function.quantifier, ast::FunctionQuantifier::Distinct)
        || original_name == "array_agg_distinct";
    let (arguments, order_arguments, implicit_separator) = if canonical_name == "group_concat" {
        // The native AST stores an explicit `SEPARATOR` separately. Preserve
        // the legacy comma spelling only when its final positional argument
        // is a string literal; all other value lists use the default comma
        // separator.
        let (values, separator) = match function.separator.as_deref() {
            Some(separator) => (function.arguments.as_slice(), Some(separator)),
            None if function.arguments.len() > 1
                && function.arguments.last().is_some_and(|argument| {
                    matches!(
                        argument,
                        ast::Expr::Literal(ast::Literal {
                            kind: ast::LiteralKind::String(_),
                            ..
                        })
                    )
                }) =>
            {
                function
                    .arguments
                    .split_last()
                    .map(|(separator, values)| (values, Some(separator)))
                    .expect("more than one GROUP_CONCAT argument")
            }
            None => (function.arguments.as_slice(), None),
        };
        (
            values
                .iter()
                .map(expr_display_name_preserve_path)
                .collect::<Vec<_>>()
                .join(","),
            values,
            separator,
        )
    } else {
        (
            format_function_arguments(function, &canonical_name),
            function.arguments.as_slice(),
            None,
        )
    };
    let mut out = format!("{canonical_name}(");
    if distinct {
        out.push_str("DISTINCT ");
    }
    out.push_str(&arguments);
    if !function.order_by.is_empty() {
        let visible = function
            .order_by
            .iter()
            .filter(|item| !is_constant_function_order_by(item, order_arguments))
            .map(|item| format_function_order_by(item, order_arguments))
            .collect::<Vec<_>>();
        if !visible.is_empty() {
            if !arguments.is_empty() {
                out.push(' ');
            }
            out.push_str("ORDER BY ");
            out.push_str(&visible.join(", "));
        }
    }
    if canonical_name == "group_concat" {
        if !arguments.is_empty() {
            out.push(' ');
        }
        out.push_str("SEPARATOR ");
        out.push_str(
            function
                .separator
                .as_deref()
                .map(expr_display_name)
                .or_else(|| implicit_separator.map(expr_display_name))
                .as_deref()
                .unwrap_or("','"),
        );
    }
    out.push(')');

    if let Some(filter) = &function.filter {
        out.push_str(" FILTER (WHERE ");
        out.push_str(&expr_display_name(filter));
        out.push(')');
    }
    if let Some(null_treatment) = function.null_treatment
        && !function_null_treatment_is_argument(&canonical_name, function.arguments.len())
    {
        out.push(' ');
        out.push_str(match null_treatment {
            ast::NullTreatment::IgnoreNulls => "ignore nulls",
            ast::NullTreatment::RespectNulls => "respect nulls",
        });
    }
    if let Some(over) = &function.over {
        out.push_str(" OVER ");
        out.push_str(&format_window_display_name(over));
    }
    if canonical_name == "typeof" {
        restore_typeof_cast_spelling(out, function)
    } else {
        out
    }
}

fn restore_typeof_cast_spelling(display: String, function: &ast::FunctionCall) -> String {
    struct CastSpelling {
        canonical: String,
        display: String,
    }

    struct Collector {
        casts: Vec<CastSpelling>,
    }

    impl ast::Visit for Collector {
        fn visit_expr(&mut self, expression: &ast::Expr) {
            if let ast::Expr::Cast(cast) = expression {
                let source = cast
                    .data_type
                    .name
                    .parts
                    .last()
                    .map(|part| part.value.as_str())
                    .unwrap_or_default();
                let canonical = format_cast_type(&cast.data_type);
                let display = match source {
                    // `typeof` has legacy result-header spellings for these
                    // narrow integer casts. Keep that compatibility local to
                    // typeof; ordinary CAST expressions use canonical names.
                    "tinyint" | "smallint" | "bigint" => source.to_string(),
                    _ => canonical.clone(),
                };
                self.casts.push(CastSpelling { canonical, display });
            }
            ast::walk_expr(self, expression);
        }
    }

    let mut collector = Collector { casts: Vec::new() };
    for argument in &function.arguments {
        ast::Visit::visit_expr(&mut collector, argument);
    }
    if collector.casts.is_empty() {
        return display;
    }

    let mut restored = String::with_capacity(display.len());
    let mut remaining = display.as_str();
    for cast in collector.casts {
        let needle = format!(" AS {})", cast.canonical);
        let Some(index) = remaining.find(&needle) else {
            return display;
        };
        restored.push_str(&remaining[..index]);
        restored.push_str(" AS ");
        restored.push_str(&cast.display);
        restored.push(')');
        remaining = &remaining[index + needle.len()..];
    }
    restored.push_str(remaining);
    restored
}

fn format_function_arguments(function: &ast::FunctionCall, canonical_name: &str) -> String {
    let mut arguments = function
        .arguments
        .iter()
        .map(expr_display_name_preserve_path)
        .collect::<Vec<_>>();
    if function_null_treatment_is_argument(canonical_name, arguments.len())
        && let Some(null_treatment) = function.null_treatment
    {
        let modifier = match null_treatment {
            ast::NullTreatment::IgnoreNulls => "ignore nulls",
            ast::NullTreatment::RespectNulls => "respect nulls",
        };
        arguments[0].push(' ');
        arguments[0].push_str(modifier);
    }
    arguments.join(", ")
}

fn function_null_treatment_is_argument(canonical_name: &str, argument_count: usize) -> bool {
    (matches!(canonical_name, "first_value" | "last_value") && argument_count >= 1)
        || (matches!(canonical_name, "lead" | "lag" | "nth_value") && argument_count >= 2)
}

fn format_map_display_name(function: &ast::FunctionCall) -> String {
    let mut pairs = Vec::new();
    let mut arguments = function.arguments.iter();
    while let Some(key) = arguments.next() {
        let key = expr_display_name_preserve_path(key);
        if let Some(value) = arguments.next() {
            pairs.push(format!("{key}:{}", expr_display_name_preserve_path(value)));
        } else {
            pairs.push(key);
        }
    }
    format!("map{{{}}}", pairs.join(","))
}

fn is_constant_function_order_by(order_by: &ast::FunctionOrderBy, arguments: &[ast::Expr]) -> bool {
    ordinal_function_argument(&order_by.expr, arguments)
        .map(is_constant_expr)
        .unwrap_or_else(|| is_constant_expr(&order_by.expr))
}

fn ordinal_function_argument<'a>(
    expr: &ast::Expr,
    arguments: &'a [ast::Expr],
) -> Option<&'a ast::Expr> {
    let ast::Expr::Literal(ast::Literal {
        kind: ast::LiteralKind::Number(position),
        ..
    }) = expr
    else {
        return None;
    };
    position
        .parse::<usize>()
        .ok()
        .and_then(|position| arguments.get(position.saturating_sub(1)))
}

fn is_constant_expr(expr: &ast::Expr) -> bool {
    match expr {
        ast::Expr::Literal(_) => true,
        ast::Expr::Nested(nested) => is_constant_expr(&nested.expression),
        _ => false,
    }
}

fn format_function_order_by(order_by: &ast::FunctionOrderBy, arguments: &[ast::Expr]) -> String {
    let expression = ordinal_function_argument(&order_by.expr, arguments)
        .map(expr_display_name_preserve_path)
        .unwrap_or_else(|| expr_display_name(&order_by.expr));
    format_order_by_display_name(expression, order_by.asc, order_by.nulls_first)
}

fn format_order_by_display_name(
    expression: String,
    ascending: Option<bool>,
    nulls_first: Option<bool>,
) -> String {
    let ascending = ascending.unwrap_or(true);
    let mut out = format!("{expression} {}", if ascending { "ASC" } else { "DESC" });
    if let Some(nulls_first) = nulls_first
        && nulls_first != ascending
    {
        out.push_str(if nulls_first {
            " NULLS FIRST"
        } else {
            " NULLS LAST"
        });
    }
    out
}

fn format_window_display_name(over: &ast::WindowSpec) -> String {
    if let Some(name) = &over.existing_window_name {
        return name.value.clone();
    }

    let mut parts = Vec::new();
    if !over.partition_by.is_empty() {
        parts.push(format!(
            "PARTITION BY {}",
            over.partition_by
                .iter()
                .map(expr_display_name)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !over.order_by.is_empty() {
        parts.push(format!(
            "ORDER BY {}",
            over.order_by
                .iter()
                .map(|order_by| {
                    format_order_by_display_name(
                        expr_display_name(&order_by.expr),
                        order_by.asc,
                        order_by.nulls_first,
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if let Some(frame) = &over.window_frame {
        parts.push(format_window_frame_display_name(frame));
    }

    if parts.is_empty() {
        "()".to_string()
    } else if over.window_frame.is_some() {
        format!("({})", parts.join(" "))
    } else {
        format!("({} )", parts.join(" "))
    }
}

fn format_window_frame_display_name(frame: &ast::WindowFrame) -> String {
    let units = match frame.units {
        ast::WindowFrameUnits::Rows => "ROWS",
        ast::WindowFrameUnits::Range => "RANGE",
        ast::WindowFrameUnits::Groups => "GROUPS",
    };
    let start = format_window_bound_display_name(&frame.start_bound);
    match &frame.end_bound {
        Some(end) => format!(
            "{units} BETWEEN {start} AND {}",
            format_window_bound_display_name(end)
        ),
        None => format!("{units} {start}"),
    }
}

fn format_window_bound_display_name(bound: &ast::WindowFrameBound) -> String {
    match bound {
        ast::WindowFrameBound::CurrentRow(_) => "CURRENT ROW".to_string(),
        ast::WindowFrameBound::Preceding(None, _) => "UNBOUNDED PRECEDING".to_string(),
        ast::WindowFrameBound::Preceding(Some(expr), _) => {
            format!("{} PRECEDING", expr_display_name(expr))
        }
        ast::WindowFrameBound::Following(None, _) => "UNBOUNDED FOLLOWING".to_string(),
        ast::WindowFrameBound::Following(Some(expr), _) => {
            format!("{} FOLLOWING", expr_display_name(expr))
        }
    }
}

fn lowercase_leading_keyword(display: &str) -> String {
    let Some(paren) = display.find('(') else {
        return display.to_string();
    };
    let prefix = &display[..paren];
    if !prefix.is_empty()
        && prefix
            .chars()
            .all(|character| character.is_ascii_alphabetic())
    {
        format!("{}{}", prefix.to_ascii_lowercase(), &display[paren..])
    } else {
        display.to_string()
    }
}

// ---------------------------------------------------------------------------
// LIMIT / OFFSET extraction
// ---------------------------------------------------------------------------

pub(super) fn extract_limit(query: &ast::Query) -> Result<Option<i64>, AnalyzeError> {
    match &query.limit {
        Some(limit) => eval_limit_or_offset(limit, "LIMIT").map(Some),
        None => Ok(None),
    }
}

pub(super) fn extract_offset(query: &ast::Query) -> Result<Option<i64>, AnalyzeError> {
    match &query.offset {
        Some(offset) => eval_limit_or_offset(&offset.value, "OFFSET").map(Some),
        None => Ok(None),
    }
}

fn eval_limit_or_offset(expr: &ast::Expr, keyword: &str) -> Result<i64, AnalyzeError> {
    match expr {
        ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Number(value),
            ..
        }) => value.parse::<i64>().map_err(|error| {
            AnalyzeError::invalid_literal(format!("invalid {keyword} value: {error}"), expr.span())
        }),
        _ => Err(AnalyzeError::invalid_argument(
            format!("only constant {keyword} is supported"),
            expr.span(),
        )),
    }
}

/// Evaluate a constant integer expression (literals and simple arithmetic).
pub(super) fn eval_const_i64(expr: &ast::Expr) -> Result<i64, AnalyzeError> {
    match expr {
        ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Number(value),
            ..
        }) => value.parse::<i64>().map_err(|error| {
            AnalyzeError::invalid_literal(
                format!("cannot parse integer literal '{value}': {error}"),
                expr.span(),
            )
        }),
        ast::Expr::Unary(unary) if unary.operator == ast::UnaryOperator::Minus => {
            Ok(-eval_const_i64(&unary.expression)?)
        }
        ast::Expr::Binary(binary) => {
            let left = eval_const_i64(&binary.left)?;
            let right = eval_const_i64(&binary.right)?;
            match binary.operator {
                ast::BinaryOperator::Add => Ok(left + right),
                ast::BinaryOperator::Subtract => Ok(left - right),
                ast::BinaryOperator::Multiply => Ok(left * right),
                ast::BinaryOperator::Divide if right != 0 => Ok(left / right),
                ast::BinaryOperator::Modulo if right != 0 => Ok(left % right),
                _ => Err(AnalyzeError::invalid_argument(
                    format!(
                        "unsupported operator in constant expression: {}",
                        binary_operator_display(binary.operator)
                    ),
                    expr.span(),
                )),
            }
        }
        ast::Expr::Nested(nested) => eval_const_i64(&nested.expression),
        _ => Err(AnalyzeError::invalid_literal(
            format!(
                "expected constant integer expression, got: {}",
                printer::print_expr(expr)
            ),
            expr.span(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, TimeUnit};

    use super::{expr_display_name, sql_type_to_arrow};

    fn parse_select_expr(sql: &str) -> novarocks_parser::ast::Expr {
        let statements = novarocks_parser::parse(sql).expect("parse SQL");
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("expected query");
        };
        let novarocks_parser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select body");
        };
        let [novarocks_parser::ast::SelectItem::UnnamedExpr(expr)] = select.projection.as_slice()
        else {
            panic!("expected unnamed expr");
        };
        expr.clone()
    }

    fn parse_native_cast_type(sql: &str) -> novarocks_parser::ast::TypeName {
        let statements = novarocks_parser::parse(sql).expect("parse native SQL");
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("expected native query");
        };
        let novarocks_parser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected native select body");
        };
        let [
            novarocks_parser::ast::SelectItem::UnnamedExpr(novarocks_parser::ast::Expr::Cast(cast)),
        ] = select.projection.as_slice()
        else {
            panic!("expected native cast expression");
        };
        cast.data_type.clone()
    }

    #[test]
    fn expr_display_name_formats_distinct_function_args_recursively() {
        let expr = parse_select_expr("SELECT ARRAY_AGG(DISTINCT score > 0)");
        assert_eq!(expr_display_name(&expr), "array_agg(DISTINCT score > 0)");
    }

    #[test]
    fn expr_display_name_lowercases_nested_function_names() {
        let expr = parse_select_expr("SELECT array_min(ARRAY_UNIQUE_AGG(col_boolean))");
        assert_eq!(
            expr_display_name(&expr),
            "array_min(array_unique_agg(col_boolean))"
        );
    }

    #[test]
    fn expr_display_name_preserves_array_unique_agg_name() {
        let expr = parse_select_expr("SELECT ARRAY_UNIQUE_AGG(s_1)");
        assert_eq!(expr_display_name(&expr), "array_unique_agg(s_1)");
    }

    #[test]
    fn sql_type_to_arrow_accepts_datetime_ns_alias() {
        let data_type = parse_native_cast_type("SELECT CAST(NULL AS DATETIME_NS)");

        assert_eq!(
            sql_type_to_arrow(&data_type).expect("type"),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
    }

    #[test]
    fn sql_decimal_type_producer_preserves_width_precision_scale_and_alias_domains() {
        for (type_name, expected) in [
            ("DECIMAL", DataType::Decimal128(38, 0)),
            ("DECIMAL32", DataType::Decimal128(9, 0)),
            ("DECIMAL64", DataType::Decimal128(18, 0)),
            ("DECIMAL128", DataType::Decimal128(38, 0)),
            ("DECIMAL(38,38)", DataType::Decimal128(38, 38)),
            ("DECIMAL(39,0)", DataType::Decimal256(39, 0)),
            ("DEC(60,2)", DataType::Decimal256(60, 2)),
            ("NUMERIC(76,76)", DataType::Decimal256(76, 76)),
            ("DECIMAL32(9,9)", DataType::Decimal128(9, 9)),
            ("DECIMAL64(18,18)", DataType::Decimal128(18, 18)),
            ("DECIMAL128(38,38)", DataType::Decimal128(38, 38)),
        ] {
            let target = parse_native_cast_type(&format!("SELECT CAST(NULL AS {type_name})"));
            assert_eq!(sql_type_to_arrow(&target).unwrap(), expected, "{type_name}");
        }
    }

    #[test]
    fn sql_decimal_type_producer_rejects_invalid_and_wrapping_parameters_at_type_span() {
        for type_name in [
            "DECIMAL(0,0)",
            "DECIMAL(77,0)",
            "DECIMAL(10,11)",
            "DECIMAL(294,0)",
            "DECIMAL(38,256)",
            "DECIMAL32(10,0)",
            "DECIMAL64(19,0)",
            "DECIMAL128(39,0)",
            "DECIMAL(10,2,3)",
        ] {
            let target = parse_native_cast_type(&format!("SELECT CAST(NULL AS {type_name})"));
            let error = sql_type_to_arrow(&target)
                .expect_err("invalid decimal type must fail before binding");
            assert_eq!(error.span(), Some(target.span), "{type_name}: {error}");
        }
        let fractional = parse_native_cast_type("SELECT CAST(NULL AS DECIMAL(1.5,0))");
        assert!(sql_type_to_arrow(&fractional).is_err());
        // Signed parameters are already rejected by the native parser.
        for type_name in ["DECIMAL(-1,0)", "DECIMAL(10,-1)"] {
            assert!(
                novarocks_parser::parse(&format!("SELECT CAST(NULL AS {type_name})")).is_err(),
                "{type_name}"
            );
        }
    }

    #[test]
    fn sql_decimal_type_producer_preserves_nested_cast_carriers() {
        let target = parse_native_cast_type(
            "SELECT CAST(NULL AS STRUCT<a ARRAY<DECIMAL(60,2)>,m MAP<INT,DECIMAL(76,4)>>)",
        );
        let DataType::Struct(fields) = sql_type_to_arrow(&target).unwrap() else {
            panic!("expected STRUCT");
        };
        let DataType::List(item) = fields[0].data_type() else {
            panic!("expected ARRAY");
        };
        assert_eq!(item.data_type(), &DataType::Decimal256(60, 2));
        let DataType::Map(entries, _) = fields[1].data_type() else {
            panic!("expected MAP");
        };
        let DataType::Struct(entries) = entries.data_type() else {
            panic!("expected MAP key/value fields");
        };
        assert_eq!(entries[0].data_type(), &DataType::Int32);
        assert_eq!(entries[1].data_type(), &DataType::Decimal256(76, 4));
    }

    #[test]
    fn sql_type_to_arrow_accepts_variant_alias() {
        let data_type = parse_native_cast_type("SELECT CAST(NULL AS VARIANT)");

        assert_eq!(
            sql_type_to_arrow(&data_type).expect("type"),
            DataType::LargeBinary
        );
    }

    #[test]
    fn expr_display_name_formats_group_concat_like_starrocks() {
        let expr = parse_select_expr("SELECT group_concat(name, subject, ',' ORDER BY 1, 2)");
        assert_eq!(
            expr_display_name(&expr),
            "group_concat(name,subject ORDER BY name ASC, subject ASC SEPARATOR ',')"
        );
    }

    #[test]
    fn expr_display_name_normalizes_double_quoted_strings_to_single_quotes() {
        let expr = parse_select_expr("SELECT array_agg(\"中国\" ORDER BY 1, id)");
        assert_eq!(
            expr_display_name(&expr),
            "array_agg('中国' ORDER BY id ASC)"
        );
    }

    #[test]
    fn expr_display_name_normalizes_array_literal_string_quotes() {
        let expr = parse_select_expr("SELECT array_agg(DISTINCT [json_object(\"2:3\")])");
        assert_eq!(
            expr_display_name(&expr),
            "array_agg(DISTINCT [json_object('2:3')])"
        );
    }

    #[test]
    fn expr_display_name_formats_map_constructor_like_starrocks() {
        let expr = parse_select_expr("SELECT array_agg(map(2, 3))");
        assert_eq!(expr_display_name(&expr), "array_agg(map{2:3})");
    }

    #[test]
    fn expr_display_name_parenthesizes_is_not_null_inner_binary_expr() {
        let expr = parse_select_expr("SELECT count_if((v4 + v4) is not null)");
        assert_eq!(expr_display_name(&expr), "count_if((v4 + v4) IS NOT NULL)");
    }

    #[test]
    fn expr_display_name_formats_array_agg_distinct_like_starrocks() {
        let expr = parse_select_expr("SELECT array_agg_distinct(name ORDER BY 1 ASC)");
        assert_eq!(
            expr_display_name(&expr),
            "array_agg(DISTINCT name ORDER BY name ASC)"
        );
    }

    #[test]
    fn expr_display_name_formats_in_subquery_like_starrocks() {
        let expr = parse_select_expr("SELECT ai_1 IN (SELECT ai_1 FROM db.array_test t)");
        assert_eq!(
            expr_display_name(&expr),
            "ai_1 IN (((SELECT ai_1 FROM db.array_test AS t)))"
        );
    }

    #[test]
    fn expr_display_name_preserves_lambda_field_paths() {
        let expr = parse_select_expr("SELECT array_sortby((x) -> x.item, x)");
        assert_eq!(expr_display_name(&expr), "array_sortby(x -> x.item, x)");
    }

    #[test]
    fn expr_display_name_strips_table_qualifier_for_compound_identifier() {
        // MySQL / StarRocks convention: `SELECT t.col` displays as `col`.
        // The qualifier exists for name resolution, not for the result
        // header. `expr_display_name_preserve_path` is the variant that
        // keeps the dotted chain when the caller needs the full path.
        let expr = parse_select_expr("SELECT c13.a");
        assert_eq!(expr_display_name(&expr), "a");
    }

    #[test]
    fn expr_display_name_preserves_struct_field_paths_inside_function_args() {
        let expr =
            parse_select_expr("SELECT cast(percentile_approx_weighted(c13.a, c1, 0.5) as int)");
        assert_eq!(
            expr_display_name(&expr),
            "CAST((percentile_approx_weighted(c13.a, c1, 0.5)) AS INT)"
        );
    }

    #[test]
    fn expr_display_name_canonicalizes_top_level_integer_cast_types() {
        let int_expr = parse_select_expr("SELECT CAST(NULL AS INT)");
        let tinyint_expr = parse_select_expr("SELECT CAST(NULL AS TINYINT)");
        let bigint_expr = parse_select_expr("SELECT CAST(NULL AS bigint)");

        assert_eq!(expr_display_name(&int_expr), "CAST(NULL AS INT)");
        assert_eq!(expr_display_name(&tinyint_expr), "CAST(NULL AS TINYINT)");
        assert_eq!(expr_display_name(&bigint_expr), "CAST(NULL AS BIGINT)");
    }

    #[test]
    fn expr_display_name_preserves_legacy_typeof_cast_spelling() {
        let scalar_expr = parse_select_expr(
            "SELECT typeof(CAST(1 AS tinyint), CAST(1 AS smallint), CAST(1 AS bigint))",
        );
        let nested_expr =
            parse_select_expr("SELECT typeof(greatest(CAST(100 AS int), CAST(200 AS bigint)))");

        assert_eq!(
            expr_display_name(&scalar_expr),
            "typeof(CAST(1 AS tinyint), CAST(1 AS smallint), CAST(1 AS bigint))"
        );
        assert_eq!(
            expr_display_name(&nested_expr),
            "typeof(greatest(CAST(100 AS INT), CAST(200 AS bigint)))"
        );
    }

    #[test]
    fn expr_display_name_keeps_json_struct_field_widths_but_canonicalizes_other_structs() {
        let json_expr = parse_select_expr(
            "SELECT CAST(parse_json('[1,2,3]') AS \
             STRUCT<col1 INT, col2 ARRAY<INT>, col3 STRUCT<nested TINYINT>>)",
        );
        let row_expr = parse_select_expr(
            "SELECT CAST(row(1, 2, 3) AS \
             STRUCT<col1 INT, col2 ARRAY<INT>, col3 STRUCT<nested TINYINT>>)",
        );

        assert_eq!(
            expr_display_name(&json_expr),
            "CAST((parse_json('[1,2,3]')) AS struct<col1 int(11), col2 array<int(11)>, \
             col3 struct<nested tinyint(4)>>)"
        );
        assert_eq!(
            expr_display_name(&row_expr),
            "CAST((row(1, 2, 3)) AS struct<col1 INT, col2 ARRAY<INT>, \
             col3 struct<nested TINYINT>>)"
        );
    }
}
