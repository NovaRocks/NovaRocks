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

#[cfg(test)]
use arrow::datatypes::DataType;
use novarocks_parser::ast;

use crate::analyze_error::AnalyzeError;

/// Preserve the authored domain while changing a scope's null padding.
pub(super) fn with_nullability(
    ty: novarocks_type_contract::FunctionValueType,
    nullable: bool,
) -> novarocks_type_contract::FunctionValueType {
    ty.with_nullable(nullable)
        .expect("validated value root nullability transform")
}

/// A read-only SQL spelling projection of the sole value-domain identity.
pub(crate) fn sql_logical_projection(
    logical: novarocks_type_contract::ValueLogicalType,
) -> Option<novarocks_types::schema::SqlType> {
    use novarocks_type_contract::ValueLogicalType as V;
    use novarocks_types::schema::SqlType as S;
    match logical {
        V::Json => Some(S::Json),
        V::Hll => Some(S::Hll),
        V::Bitmap => Some(S::Bitmap),
        V::Variant => Some(S::Variant),
        V::LargeInt => Some(S::LargeInt),
        _ => None,
    }
}

enum ColumnProjectionError {
    Source(novarocks_types::ColumnValueTypeError),
    Control(novarocks_type_contract::CompileControlError),
}
impl From<novarocks_types::ColumnValueTypeError> for ColumnProjectionError {
    fn from(error: novarocks_types::ColumnValueTypeError) -> Self {
        Self::Source(error)
    }
}
impl From<novarocks_type_contract::ValueTypeError> for ColumnProjectionError {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Source(error.into())
    }
}

/// Catalog declaration validation borrows the original request's control.
pub(super) fn column_value_type(
    column: &novarocks_types::schema::ColumnDef,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<novarocks_type_contract::FunctionValueType, AnalyzeError> {
    let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
        control,
        novarocks_type_contract::CompilePhase::Validate,
    )
    .map_err(AnalyzeError::control)?;
    let result = column
        .declared_value_type_observed(|| work.step().map_err(ColumnProjectionError::Control))
        .map_err(|error| match error {
            ColumnProjectionError::Control(error) => AnalyzeError::control(error),
            ColumnProjectionError::Source(error) => {
                AnalyzeError::internal(format!("invalid catalog column `{}`: {error}", column.name))
            }
        });
    if result
        .as_ref()
        .err()
        .is_some_and(|error| error.control_error().is_some())
    {
        return result;
    }
    work.finish().map_err(AnalyzeError::control)?;
    result
}

pub(super) fn validate_value_type(
    ty: &novarocks_type_contract::FunctionValueType,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<(), AnalyzeError> {
    use novarocks_functions::KernelFailure;
    use novarocks_type_contract::{CompileCheckpoints, CompileControlError, CompilePhase};
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)
        .map_err(AnalyzeError::control)?;
    let result = novarocks_functions::validate_function_value_type_observed(ty, &mut work).map_err(
        |error| match error {
            KernelFailure::Cancelled => AnalyzeError::control(CompileControlError::Cancelled),
            KernelFailure::DeadlineExceeded => {
                AnalyzeError::control(CompileControlError::DeadlineExceeded)
            }
            KernelFailure::ResourceExhausted => {
                AnalyzeError::control(CompileControlError::ResourceExhausted)
            }
            error => AnalyzeError::internal(error.to_string()),
        },
    );
    if result
        .as_ref()
        .err()
        .is_some_and(|error| error.control_error().is_some())
    {
        return result;
    }
    work.finish().map_err(AnalyzeError::control)?;
    result
}

/// Keep the SQL value-type admission at the common assignment boundary.
pub(super) fn assignment_common_value_type(
    left: &novarocks_type_contract::FunctionValueType,
    right: &novarocks_type_contract::FunctionValueType,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<novarocks_type_contract::FunctionValueType, AnalyzeError> {
    use novarocks_type_contract::{AssignmentCommonTypeError, CompileCheckpoints, CompilePhase};

    validate_value_type(left, control)?;
    validate_value_type(right, control)?;
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)
        .map_err(AnalyzeError::control)?;
    let result = novarocks_type_contract::assignment_common_value_type(left, right, &mut work);
    work.finish_result(result, AssignmentCommonTypeError::Control)
        .map_err(|error| match error {
            AssignmentCommonTypeError::Control(cause) => AnalyzeError::control(cause),
            error @ AssignmentCommonTypeError::IncompatibleShape(_) => {
                AnalyzeError::type_mismatch(error.to_string(), novarocks_parser::SYNTHETIC)
            }
            error => AnalyzeError::internal(error.to_string()),
        })
}

/// Resolve a CAST or typed source declaration under the original analysis control.
pub(super) fn sql_type_to_value_type(
    sql_type: &ast::TypeName,
    nullable: bool,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<novarocks_type_contract::FunctionValueType, AnalyzeError> {
    novarocks_expression::typing::declaration::resolve_trino_type(sql_type, nullable, control)
        .map_err(type_declaration_error)
}

pub(super) fn type_declaration_error(
    error: novarocks_expression::typing::declaration::TypeDeclarationError,
) -> AnalyzeError {
    use novarocks_expression::typing::declaration::TypeDeclarationProblem;

    match error.problem {
        TypeDeclarationProblem::UnknownType | TypeDeclarationProblem::InvalidParameters => {
            AnalyzeError::type_mismatch(error.to_string(), error.span)
        }
        TypeDeclarationProblem::InvalidDecimalParameters => {
            AnalyzeError::invalid_type_argument(error.to_string(), error.span)
        }
        TypeDeclarationProblem::Unsupported => {
            AnalyzeError::unsupported_expression(error.to_string(), error.span)
        }
        TypeDeclarationProblem::Control => {
            AnalyzeError::control(error.control.expect("original type declaration control"))
        }
    }
}

#[cfg(test)]
pub(super) fn sql_type_to_arrow(sql_type: &ast::TypeName) -> Result<DataType, AnalyzeError> {
    sql_type_to_value_type(
        sql_type,
        true,
        &crate::compiler::SqlCompileControl::unbounded(),
    )
    .map(|value| value.data_type().clone())
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Expression display name
// ---------------------------------------------------------------------------

pub(super) fn expr_display_name(expr: &ast::Expr) -> String {
    match expr {
        ast::Expr::Literal(literal) => format_literal_display_name(literal),
        ast::Expr::Identifier(ident) => ident.value.clone(),
        ast::Expr::Dereference(node) => format_dereference_display_name(node, expr),
        ast::Expr::Subscript(node) => format!(
            "{}[{}]",
            expr_display_name_preserve_path(&node.base),
            expr_display_name(&node.index),
        ),
        ast::Expr::ArrayWildcardSubscript(_) => crate::parser::expression_for_diagnostic(expr),
        ast::Expr::Array(array) => format!(
            "[{}]",
            array
                .values
                .iter()
                .map(expr_display_name)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        ast::Expr::Lambda(lambda) => {
            let parameters = lambda
                .arguments
                .iter()
                .map(|argument| argument.name.value.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            let parameters = if lambda.arguments.len() == 1 {
                parameters
            } else {
                format!("({parameters})")
            };
            format!(
                "{parameters} -> {}",
                expr_display_name_preserve_path(&lambda.body)
            )
        }
        ast::Expr::GroupingOperation(node) => format_control_call_display(
            "grouping",
            &node.grouping_columns.iter().collect::<Vec<_>>(),
        ),
        ast::Expr::Trim(node) if node.trim_character.is_none() => {
            let name = match node.specification {
                ast::TrimSpecification::Both => "trim",
                ast::TrimSpecification::Leading => "ltrim",
                ast::TrimSpecification::Trailing => "rtrim",
            };
            format_control_call_display(name, &[&node.trim_source])
        }
        ast::Expr::Extract(_) => crate::parser::expression_for_diagnostic(expr),
        ast::Expr::If(node) => {
            let mut arguments = vec![&node.condition, &node.true_value];
            arguments.extend(node.false_value.iter());
            format_control_call_display("if", &arguments)
        }
        ast::Expr::NullIf(node) => {
            format_control_call_display("nullif", &[&node.first, &node.second])
        }
        ast::Expr::Coalesce(node) => {
            format_control_call_display("coalesce", &node.operands.iter().collect::<Vec<_>>())
        }
        ast::Expr::Try(node) => format_control_call_display("try", &[&node.inner]),
        ast::Expr::Format(node) => {
            format_control_call_display("format", &node.arguments.iter().collect::<Vec<_>>())
        }
        ast::Expr::FunctionCall(function)
            if function.processing_mode.is_some()
                || function
                    .arguments
                    .iter()
                    .any(|argument| argument.name.is_some()) =>
        {
            crate::parser::expression_for_diagnostic(expr)
        }
        ast::Expr::FunctionCall(function) => format_function_display_name(function),
        ast::Expr::Predicated(node) => format_predicated_display_name(expr, node),
        ast::Expr::Cast(cast) => {
            if type_name_is(&cast.data_type, "array")
                && matches!(&cast.expression, ast::Expr::Array(_))
            {
                return expr_display_name(&cast.expression);
            }
            format!(
                "CAST({} AS {})",
                expr_display_name_with_parens(&cast.expression),
                format_cast_type_for_expression(&cast.data_type, &cast.expression)
            )
        }
        ast::Expr::Binary(binary) => format!(
            "{} {} {}",
            expr_display_name_with_parens(&binary.left),
            binary_operator_display(binary.operator),
            expr_display_name_with_parens(&binary.right)
        ),
        ast::Expr::Logical(logical) => {
            let separator = match logical.operator {
                ast::LogicalOperator::And => " AND ",
                ast::LogicalOperator::Or => " OR ",
            };
            // Output names retain the existing adjacent-pair grouping. Only
            // rendering strings are grouped; syntax and source terms stay flat.
            let mut level = logical
                .terms
                .iter()
                .map(expr_display_name_with_parens)
                .collect::<Vec<_>>();
            while level.len() > 2 {
                let mut next = Vec::with_capacity(level.len().div_ceil(2));
                let mut terms = level.into_iter();
                while let Some(left) = terms.next() {
                    next.push(match terms.next() {
                        Some(right) => format!("({left}{separator}{right})"),
                        None => left,
                    });
                }
                level = next;
            }
            level.join(separator)
        }
        ast::Expr::InList(list) => format!(
            "({})",
            list.values
                .iter()
                .map(expr_display_name_with_parens)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        other => lowercase_leading_keyword(&crate::parser::expression_for_diagnostic(other)),
    }
}

fn format_subquery_display_name(query: &ast::Query) -> String {
    // AliasedRelation prints the canonical AS form directly.
    crate::parser::query_for_diagnostic(query)
}

fn pure_identifier_chain(expr: &ast::Expr) -> Option<ast::IdentifierChain<'_>> {
    ast::identifier_chain_observed(expr, |_| Ok::<(), std::convert::Infallible>(()))
        .unwrap_or_else(|never| match never {})
}

fn format_dereference_display_name(node: &ast::DereferenceExpr, whole: &ast::Expr) -> String {
    let Some(field) = &node.field else {
        return crate::parser::expression_for_diagnostic(whole);
    };
    if pure_identifier_chain(&node.base).is_some() {
        return field.value.clone();
    }
    format!(
        "{}.{}",
        expr_display_name_preserve_path(&node.base),
        field.value
    )
}

fn expr_display_name_preserve_path(expr: &ast::Expr) -> String {
    if let Some(chain) = pure_identifier_chain(expr) {
        let mut parts = chain
            .iter_rev()
            .map(|part| part.value.as_str())
            .collect::<Vec<_>>();
        parts.reverse();
        return parts.join(".");
    }
    match expr {
        ast::Expr::Dereference(node) => match &node.field {
            Some(field) => format!(
                "{}.{}",
                expr_display_name_preserve_path(&node.base),
                field.value
            ),
            None => crate::parser::expression_for_diagnostic(expr),
        },
        ast::Expr::Subscript(node) => format!(
            "{}[{}]",
            expr_display_name_preserve_path(&node.base),
            expr_display_name(&node.index)
        ),
        ast::Expr::ArrayWildcardSubscript(_) => crate::parser::expression_for_diagnostic(expr),
        _ => expr_display_name(expr),
    }
}
fn format_literal_display_name(literal: &ast::Literal) -> String {
    match &literal.kind {
        ast::LiteralKind::String(value) => format!("'{}'", value.replace('\'', "''")),
        ast::LiteralKind::Boolean(true) => "TRUE".to_string(),
        ast::LiteralKind::Boolean(false) => "FALSE".to_string(),
        ast::LiteralKind::Null => "NULL".to_string(),
        ast::LiteralKind::Long { spelling, .. } | ast::LiteralKind::Decimal(spelling) => {
            spelling.clone()
        }
        ast::LiteralKind::Double(value) => value.get().to_string(),
        ast::LiteralKind::Binary(bytes) => format!("X'{}'", hex::encode(bytes)),
        ast::LiteralKind::Generic(_) | ast::LiteralKind::Interval(_) => {
            crate::parser::expression_for_diagnostic(&ast::Expr::Literal(literal.clone()))
        }
    }
}

fn expr_display_name_with_parens(expr: &ast::Expr) -> String {
    if pure_identifier_chain(expr).is_some() {
        return expr_display_name(expr);
    }
    match expr {
        ast::Expr::Identifier(_) | ast::Expr::Literal(_) => expr_display_name(expr),
        ast::Expr::Unary(unary)
            if unary.operator == ast::UnaryOperator::Minus
                && matches!(unary.expression.as_ref(), ast::Expr::Literal(_)) =>
        {
            expr_display_name(expr)
        }
        _ => format!("({})", expr_display_name(expr)),
    }
}

fn format_predicated_display_name(expression: &ast::Expr, node: &ast::PredicatedExpr) -> String {
    let value = || expr_display_name_with_parens(&node.value);
    match &node.predicate {
        ast::Predicate::Comparison(p) => {
            let operator = match p.operator {
                ast::ComparisonOperator::Equal => "=",
                ast::ComparisonOperator::NotEqual => "!=",
                ast::ComparisonOperator::LessThan => "<",
                ast::ComparisonOperator::LessThanOrEqual => "<=",
                ast::ComparisonOperator::GreaterThan => ">",
                ast::ComparisonOperator::GreaterThanOrEqual => ">=",
            };
            format!(
                "{} {operator} {}",
                value(),
                expr_display_name_with_parens(&p.right)
            )
        }
        ast::Predicate::DistinctFrom(p) => {
            let equal = format!(
                "{} <=> {}",
                value(),
                expr_display_name_with_parens(&p.right)
            );
            if p.negated {
                equal
            } else {
                format!("NOT ({equal})")
            }
        }
        ast::Predicate::IsNull(p) => {
            format!("{} IS {}NULL", value(), if p.negated { "NOT " } else { "" })
        }
        ast::Predicate::BooleanTest(p) => {
            let truth = match p.truth_value {
                ast::TruthValue::True => "TRUE",
                ast::TruthValue::False => "FALSE",
                ast::TruthValue::Unknown => "UNKNOWN",
            };
            format!(
                "{} IS {}{truth}",
                value(),
                if p.negated { "NOT " } else { "" }
            )
        }
        ast::Predicate::In(p) => {
            let not = if p.negated { " NOT" } else { "" };
            match &p.value_list {
                ast::Expr::InList(list) => format!(
                    "{}{not} IN ({})",
                    value(),
                    list.values
                        .iter()
                        .map(expr_display_name_with_parens)
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                ast::Expr::Subquery(subquery) => format!(
                    "{}{not} IN ((({})))",
                    value(),
                    format_subquery_display_name(&subquery.query)
                ),
                _ => {
                    lowercase_leading_keyword(&crate::parser::expression_for_diagnostic(expression))
                }
            }
        }
        _ => lowercase_leading_keyword(&crate::parser::expression_for_diagnostic(expression)),
    }
}

fn binary_operator_display(operator: ast::BinaryOperator) -> &'static str {
    match operator {
        ast::BinaryOperator::Add => "+",
        ast::BinaryOperator::Subtract => "-",
        ast::BinaryOperator::Multiply => "*",
        ast::BinaryOperator::Divide => "/",
        ast::BinaryOperator::Modulo => "%",
    }
}

fn type_name_is(data_type: &ast::TypeName, expected: &str) -> bool {
    matches!(&data_type.kind, ast::TypeKind::Generic(value) if value.name.value.eq_ignore_ascii_case(expected))
}

fn format_cast_type(data_type: &ast::TypeName) -> String {
    format_type_display(data_type, 0)
}

fn format_cast_type_for_expression(data_type: &ast::TypeName, expression: &ast::Expr) -> String {
    if is_json_parse_call(expression) {
        format_json_cast_type(data_type)
    } else {
        format_cast_type(data_type)
    }
}

fn is_json_parse_call(expression: &ast::Expr) -> bool {
    let ast::Expr::FunctionCall(function) = expression else {
        return false;
    };
    function
        .name
        .parts
        .last()
        .is_some_and(|part| part.value.eq_ignore_ascii_case("json_parse"))
}

fn format_json_cast_type(data_type: &ast::TypeName) -> String {
    format_type_display(data_type, 1)
}

fn numeric_type_argument(data_type: &ast::TypeName, index: usize) -> Option<u64> {
    let ast::TypeKind::Generic(value) = &data_type.kind else {
        return None;
    };
    match value.arguments.get(index) {
        Some(ast::DataTypeParameter::Numeric(value)) => u64::try_from(value.parsed_value).ok(),
        _ => None,
    }
}

fn type_name_argument_as_type(argument: &ast::DataTypeParameter) -> Option<&ast::TypeName> {
    match argument {
        ast::DataTypeParameter::Type(value) => Some(&value.data_type),
        ast::DataTypeParameter::Numeric(_) => None,
    }
}

// These are the established output-column display spellings, not SQL syntax.
// The parser printer remains the owner of reparsable Trino type text.
fn format_type_display(data_type: &ast::TypeName, style: u8) -> String {
    match &data_type.kind {
        ast::TypeKind::Row(row) => {
            let fields = row
                .fields
                .iter()
                .map(|field| {
                    let child =
                        format_type_display(&field.data_type, if style == 0 { 0 } else { 2 });
                    field
                        .name
                        .as_ref()
                        .map(|name| format!("{} {child}", name.value))
                        .unwrap_or(child)
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("struct<{fields}>")
        }
        ast::TypeKind::Generic(generic) => {
            let name = generic.name.value.to_ascii_lowercase();
            let arguments = &generic.arguments;
            match name.as_str() {
                "array" => {
                    let child = arguments
                        .first()
                        .and_then(type_name_argument_as_type)
                        .map(|child| format_type_display(child, style))
                        .unwrap_or_default();
                    format!("{}<{child}>", if style == 2 { "array" } else { "ARRAY" })
                }
                "map" => {
                    let children = arguments
                        .iter()
                        .filter_map(type_name_argument_as_type)
                        .map(|child| format_type_display(child, style))
                        .collect::<Vec<_>>()
                        .join(if style == 2 { ", " } else { "," });
                    format!("{}<{children}>", if style == 2 { "map" } else { "MAP" })
                }
                "tinyint" if style == 2 => "tinyint(4)".into(),
                "smallint" if style == 2 => "smallint(6)".into(),
                "int" | "integer" if style == 2 => "int(11)".into(),
                "bigint" if style == 2 => "bigint(20)".into(),
                "string" if style == 2 => "varchar(65533)".into(),
                "binary" | "varbinary" if style == 2 => "varbinary".into(),
                "decimal" | "dec" | "numeric" | "decimal32" | "decimal64" | "decimal128" => {
                    let precision = numeric_type_argument(data_type, 0).unwrap_or(38);
                    let scale = numeric_type_argument(data_type, 1).unwrap_or(0);
                    format!("{}({precision},{scale})", decimal_kind(precision))
                }
                "tinyint" => "TINYINT".into(),
                "smallint" => "SMALLINT".into(),
                "bigint" => "BIGINT".into(),
                "largeint" => "LARGEINT".into(),
                "int" | "integer" => "INT".into(),
                "string" => "VARCHAR(65533)".into(),
                "binary" | "varbinary" => "VARBINARY".into(),
                _ => crate::parser::type_for_diagnostic(data_type),
            }
        }
        _ => crate::parser::type_for_diagnostic(data_type),
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
        "struct" => "row".to_string(),
        other => other.to_string(),
    }
}

fn format_control_call_display(name: &str, arguments: &[&ast::Expr]) -> String {
    format!(
        "{name}({})",
        arguments
            .iter()
            .map(|argument| expr_display_name_preserve_path(argument))
            .collect::<Vec<_>>()
            .join(", ")
    )
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
            expr_display_name_preserve_path(&function.arguments[0].value),
            expr_display_name(&function.arguments[1].value)
        );
    }

    let distinct = function.distinct || original_name == "array_agg_distinct";
    let argument_values = function
        .arguments
        .iter()
        .map(|argument| &argument.value)
        .collect::<Vec<_>>();
    // STRING_AGG's declared final argument is its separator. Raw ordinary
    // GROUP_CONCAT syntax has not yet acquired that canonical contract.
    let (arguments, order_arguments, concat_separator) =
        if original_name == "string_agg" && argument_values.len() >= 2 {
            let (separator, values) = argument_values
                .split_last()
                .expect("two canonical arguments");
            (
                values
                    .iter()
                    .map(|value| expr_display_name_preserve_path(value))
                    .collect::<Vec<_>>()
                    .join(","),
                values,
                Some(*separator),
            )
        } else {
            (
                format_function_arguments(function, &canonical_name),
                argument_values.as_slice(),
                None,
            )
        };
    let mut out = format!("{canonical_name}(");
    if distinct {
        out.push_str("DISTINCT ");
    }
    out.push_str(&arguments);
    if let Some(order_by) = &function.order_by {
        let visible = order_by
            .items
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
    if let Some(separator) = concat_separator {
        if !arguments.is_empty() {
            out.push(' ');
        }
        out.push_str("SEPARATOR ");
        out.push_str(&expr_display_name(separator));
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
    if let Some(over) = &function.window {
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
                let source = match &cast.data_type.kind {
                    ast::TypeKind::Generic(value) => value.name.value.as_str(),
                    _ => "",
                };
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
        ast::Visit::visit_expr(&mut collector, &argument.value);
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
        .map(|argument| expr_display_name_preserve_path(&argument.value))
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
    let mut arguments = function.arguments.iter().map(|argument| &argument.value);
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

fn is_constant_function_order_by(order_by: &ast::SortItem, arguments: &[&ast::Expr]) -> bool {
    ordinal_function_argument(&order_by.sort_key, arguments)
        .map(is_constant_expr)
        .unwrap_or_else(|| is_constant_expr(&order_by.sort_key))
}

fn ordinal_function_argument<'a>(
    expr: &ast::Expr,
    arguments: &'a [&'a ast::Expr],
) -> Option<&'a ast::Expr> {
    let ast::Expr::Literal(ast::Literal {
        kind: ast::LiteralKind::Long {
            parsed_value: position,
            ..
        },
        ..
    }) = expr
    else {
        return None;
    };
    usize::try_from(*position)
        .ok()
        .and_then(|position| arguments.get(position.saturating_sub(1)))
        .copied()
}

fn is_constant_expr(expr: &ast::Expr) -> bool {
    matches!(expr, ast::Expr::Literal(_))
}

fn format_function_order_by(order_by: &ast::SortItem, arguments: &[&ast::Expr]) -> String {
    let expression = ordinal_function_argument(&order_by.sort_key, arguments)
        .map(expr_display_name_preserve_path)
        .unwrap_or_else(|| expr_display_name(&order_by.sort_key));
    format_order_by_display_name(
        expression,
        Some(matches!(order_by.ordering, ast::SortOrdering::Ascending)),
        sort_item_explicit_nulls(order_by),
    )
}

/// Freeze execution flags using the original NULL placement rule. Undefined
/// remains an AST fact; this conversion does not rewrite it to a default enum.
pub(super) fn sort_item_flags(item: &ast::SortItem) -> (bool, bool) {
    let ascending = matches!(item.ordering, ast::SortOrdering::Ascending);
    let nulls_first = sort_item_explicit_nulls(item).unwrap_or(ascending);
    (ascending, nulls_first)
}

fn sort_item_explicit_nulls(item: &ast::SortItem) -> Option<bool> {
    match item.null_ordering {
        ast::NullOrdering::First => Some(true),
        ast::NullOrdering::Last => Some(false),
        ast::NullOrdering::Undefined => None,
    }
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

fn format_window_display_name(window: &ast::Window) -> String {
    match window {
        ast::Window::Reference(reference) => reference.name.value.clone(),
        ast::Window::Specification(specification) => {
            format_window_specification_display_name(specification)
        }
    }
}

fn format_window_specification_display_name(over: &ast::WindowSpecification) -> String {
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
    if let Some(order_by) = &over.order_by {
        parts.push(format!(
            "ORDER BY {}",
            order_by
                .items
                .iter()
                .map(|order_by| {
                    format_order_by_display_name(
                        expr_display_name(&order_by.sort_key),
                        Some(matches!(order_by.ordering, ast::SortOrdering::Ascending)),
                        sort_item_explicit_nulls(order_by),
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

pub(super) fn query_modifiers(query: &ast::Query) -> Result<&ast::QueryModifiers, AnalyzeError> {
    match query.body.as_ref() {
        ast::SetExpr::Select(select) => {
            if !query.modifiers.is_empty() {
                return Err(AnalyzeError::invalid_query_shape(
                    "direct SELECT modifiers must belong to the query specification",
                    query.span,
                ));
            }
            Ok(&select.modifiers)
        }
        _ => Ok(&query.modifiers),
    }
}

pub(super) fn extract_limit(
    modifiers: &ast::QueryModifiers,
    parameters: Option<&crate::compiler::SqlPreparedParameters>,
) -> Result<Option<i64>, AnalyzeError> {
    match &modifiers.limit {
        Some(ast::QueryLimit::Limit(limit)) => match &limit.row_count {
            ast::Expr::AllRows(_) => Ok(None),
            row_count if description_row_count(row_count, parameters) => Ok(None),
            row_count => eval_limit_or_offset(row_count, "LIMIT").map(Some),
        },
        Some(ast::QueryLimit::FetchFirst(fetch)) => Err(AnalyzeError::unsupported_expression(
            "FETCH FIRST is not supported",
            fetch.span,
        )),
        None => Ok(None),
    }
}

pub(super) fn extract_offset(
    modifiers: &ast::QueryModifiers,
    parameters: Option<&crate::compiler::SqlPreparedParameters>,
) -> Result<Option<i64>, AnalyzeError> {
    match &modifiers.offset {
        Some(offset) if description_row_count(&offset.row_count, parameters) => Ok(None),
        Some(offset) => eval_limit_or_offset(&offset.row_count, "OFFSET").map(Some),
        None => Ok(None),
    }
}

fn description_row_count(
    expression: &ast::Expr,
    parameters: Option<&crate::compiler::SqlPreparedParameters>,
) -> bool {
    matches!(expression, ast::Expr::Parameter(parameter) if parameters.is_some_and(
        |parameters| parameters.is_description() && parameters.ordinal(parameter.span).is_some()
    ))
}

fn eval_limit_or_offset(expr: &ast::Expr, keyword: &str) -> Result<i64, AnalyzeError> {
    match expr {
        ast::Expr::Parameter(_) => Err(AnalyzeError::unsupported_expression(
            format!("parameterized {keyword} is not supported"),
            expr.span(),
        )),
        ast::Expr::AllRows(_) => Err(AnalyzeError::unsupported_expression(
            format!("ALL is not supported for {keyword}"),
            expr.span(),
        )),
        ast::Expr::Literal(ast::Literal {
            kind:
                ast::LiteralKind::Long {
                    parsed_value: value,
                    ..
                },
            ..
        }) => Ok(*value),
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
            kind:
                ast::LiteralKind::Long {
                    parsed_value: value,
                    ..
                },
            ..
        }) => Ok(*value),
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
        _ => Err(AnalyzeError::invalid_literal(
            format!(
                "expected constant integer expression, got: {}",
                crate::parser::expression_for_diagnostic(expr)
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
        let query = &novarocks_parser::parser::parse_query(sql).expect("parse SQL");
        let novarocks_parser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select body");
        };
        let [novarocks_parser::ast::SelectItem::SingleColumn(projection_column)] =
            select.projection.as_slice()
        else {
            panic!("expected unnamed expr");
        };
        assert!(projection_column.alias.is_none(), "anonymous projection");
        let expr = &projection_column.expression;
        expr.clone()
    }

    fn parse_native_cast_type(sql: &str) -> novarocks_parser::ast::TypeName {
        let query = &novarocks_parser::parser::parse_query(sql).expect("parse native SQL");
        let novarocks_parser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected native select body");
        };
        let [novarocks_parser::ast::SelectItem::SingleColumn(projection_column)] =
            select.projection.as_slice()
        else {
            panic!("expected native cast expression");
        };
        assert!(projection_column.alias.is_none(), "anonymous projection");
        let novarocks_parser::ast::Expr::Cast(cast) = &projection_column.expression else {
            panic!("expected native cast expression");
        };
        cast.data_type.clone()
    }

    #[test]
    fn logical_display_preserves_original_adjacent_group_names_and_projection_contract() {
        // Independent strings from the existing adjacent-pair parser/display
        // contract cover even and odd tails, nested operators and operands.
        for (sql, expected) in [
            ("SELECT TRUE AND FALSE", "TRUE AND FALSE"),
            (
                "SELECT TRUE AND FALSE AND NULL",
                "(TRUE AND FALSE) AND NULL",
            ),
            (
                "SELECT TRUE AND FALSE AND NULL AND TRUE",
                "(TRUE AND FALSE) AND (NULL AND TRUE)",
            ),
            (
                "SELECT TRUE AND FALSE AND NULL AND TRUE AND FALSE",
                "((TRUE AND FALSE) AND (NULL AND TRUE)) AND FALSE",
            ),
            (
                "SELECT TRUE OR FALSE OR NULL OR TRUE OR FALSE",
                "((TRUE OR FALSE) OR (NULL OR TRUE)) OR FALSE",
            ),
            (
                "SELECT TRUE AND (FALSE OR NULL) AND TRUE",
                "(TRUE AND (FALSE OR NULL)) AND TRUE",
            ),
            (
                "SELECT 1 = 10 AND 2 = 20 AND 3 = 30",
                "((1 = 10) AND (2 = 20)) AND (3 = 30)",
            ),
        ] {
            let expression = parse_select_expr(sql);
            assert_eq!(expr_display_name(&expression), expected, "{sql}");
            let query = &novarocks_parser::parser::parse_query(sql).unwrap();
            let (resolved, _, _) = crate::analyzer::analyze(
                query,
                &crate::planning::catalog::PlannerMemoryCatalog::default(),
                "default",
            )
            .unwrap();
            let crate::analysis::QueryBody::Select(select) = resolved.body else {
                panic!("resolved select")
            };
            assert_eq!(select.projection[0].output_name, expected, "{sql}");
        }
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
    fn sql_type_to_arrow_requires_the_supported_timestamp_declaration() {
        let standard = parse_native_cast_type("SELECT CAST(NULL AS TIMESTAMP(6))");
        assert_eq!(
            sql_type_to_arrow(&standard).expect("type"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        let retired = parse_native_cast_type("SELECT CAST(NULL AS DATETIME_NS)");
        let error = sql_type_to_arrow(&retired).unwrap_err();
        assert_eq!(
            error.kind(),
            crate::analyze_error::AnalyzeErrorKind::TypeMismatch
        );
        assert_eq!(error.span(), Some(retired.span));
    }

    #[test]
    fn sql_decimal_type_producer_preserves_supported_precision_and_scale() {
        for (type_name, expected) in [
            ("DECIMAL", DataType::Decimal128(38, 0)),
            ("DECIMAL(9,0)", DataType::Decimal128(9, 0)),
            ("DECIMAL(18,0)", DataType::Decimal128(18, 0)),
            ("DECIMAL(38,38)", DataType::Decimal128(38, 38)),
            ("DECIMAL(9,9)", DataType::Decimal128(9, 9)),
            ("DECIMAL(18,18)", DataType::Decimal128(18, 18)),
        ] {
            let target = parse_native_cast_type(&format!("SELECT CAST(NULL AS {type_name})"));
            assert_eq!(sql_type_to_arrow(&target).unwrap(), expected, "{type_name}");
        }
    }

    #[test]
    fn sql_decimal_type_producer_rejects_invalid_and_wrapping_parameters_at_type_span() {
        for type_name in [
            "DECIMAL(0,0)",
            "DECIMAL(39,0)",
            "DECIMAL(77,0)",
            "DECIMAL(10,11)",
            "DECIMAL(294,0)",
            "DECIMAL(38,256)",
            "DECIMAL32(9,0)",
            "DECIMAL32(10,0)",
            "DECIMAL64(18,0)",
            "DECIMAL64(19,0)",
            "DECIMAL128(38,0)",
            "DECIMAL128(39,0)",
            "DEC(60,2)",
            "NUMERIC(76,76)",
            "DECIMAL(10,2,3)",
        ] {
            let target = parse_native_cast_type(&format!("SELECT CAST(NULL AS {type_name})"));
            let error = sql_type_to_arrow(&target)
                .expect_err("invalid decimal type must fail before binding");
            assert_eq!(error.span(), Some(target.span), "{type_name}: {error}");
        }
        // Numeric type parameters accept integer tokens. Fractions are syntax
        // errors whose rejected span must identify source within this CAST.
        const FRACTIONAL_SOURCE: &str = "SELECT CAST(NULL AS DECIMAL(1.5,0))";
        let error = novarocks_parser::parser::parse_query(FRACTIONAL_SOURCE)
            .expect_err("fractional type parameters have no final AST representation");
        let novarocks_parser::ParserError::Parse(novarocks_parser::ParseError::UnexpectedToken {
            span,
            ..
        }) = error
        else {
            panic!("fractional parameter must be a syntax rejection, got {error:?}");
        };
        let cast_start = FRACTIONAL_SOURCE.find("CAST").unwrap();
        assert!(!span.is_empty());
        assert!(span.start() >= cast_start && span.end() <= FRACTIONAL_SOURCE.len());
        // Signed parameters are already rejected by the native parser.
        for type_name in ["DECIMAL(-1,0)", "DECIMAL(10,-1)"] {
            assert!(
                novarocks_parser::parser::parse_query(&format!("SELECT CAST(NULL AS {type_name})"))
                    .is_err(),
                "{type_name}"
            );
        }
    }

    #[test]
    fn sql_decimal_type_producer_preserves_nested_cast_carriers() {
        let target = parse_native_cast_type(
            "SELECT CAST(NULL AS ROW(a ARRAY(DECIMAL(38,2)),m MAP(INTEGER,DECIMAL(38,4))))",
        );
        let DataType::Struct(fields) = sql_type_to_arrow(&target).unwrap() else {
            panic!("expected STRUCT");
        };
        let DataType::List(item) = fields[0].data_type() else {
            panic!("expected ARRAY");
        };
        assert_eq!(item.data_type(), &DataType::Decimal128(38, 2));
        let DataType::Map(entries, _) = fields[1].data_type() else {
            panic!("expected MAP");
        };
        let DataType::Struct(entries) = entries.data_type() else {
            panic!("expected MAP key/value fields");
        };
        assert_eq!(entries[0].data_type(), &DataType::Int32);
        assert_eq!(entries[1].data_type(), &DataType::Decimal128(38, 4));
    }

    #[test]
    fn sql_type_to_arrow_reports_unavailable_variant_capability() {
        let data_type = parse_native_cast_type("SELECT CAST(NULL AS VARIANT)");
        let error = sql_type_to_arrow(&data_type).unwrap_err();
        assert_eq!(
            error.kind(),
            crate::analyze_error::AnalyzeErrorKind::UnsupportedExpression
        );
        assert_eq!(error.span(), Some(data_type.span));
    }

    #[test]
    fn expr_display_name_formats_group_concat_like_starrocks() {
        let expr =
            parse_select_expr("SELECT string_agg(name, subject \n,\n ','\nORDER BY 1, 2 \n)");
        assert_eq!(
            expr_display_name(&expr),
            "group_concat(name,subject ORDER BY name ASC, subject ASC SEPARATOR ',')"
        );
    }

    #[test]
    fn expr_display_name_normalizes_double_quoted_strings_to_single_quotes() {
        let expr = parse_select_expr("SELECT array_agg('中国' ORDER BY 1, id)");
        assert_eq!(
            expr_display_name(&expr),
            "array_agg('中国' ORDER BY id ASC)"
        );
    }

    #[test]
    fn expr_display_name_normalizes_array_literal_string_quotes() {
        let expr = parse_select_expr("SELECT array_agg(DISTINCT ARRAY[JSON_OBJECT('k': 1)])");
        assert_eq!(
            expr_display_name(&expr),
            "array_agg(DISTINCT [JSON_OBJECT(KEY 'k' VALUE 1 NULL ON NULL WITHOUT UNIQUE KEYS)])"
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
            "SELECT CAST(json_parse('[1,2,3]') AS ROW(col1 INT, col2 ARRAY<INT>, col3 ROW(nested TINYINT)))",
        );
        let row_expr = parse_select_expr(
            "SELECT CAST(row(1, 2, 3) AS ROW(col1 INT, col2 ARRAY<INT>, col3 ROW(nested TINYINT)))",
        );

        assert_eq!(
            expr_display_name(&json_expr),
            "CAST((json_parse('[1,2,3]')) AS struct<col1 int(11), col2 array<int(11)>, \
             col3 struct<nested tinyint(4)>>)"
        );
        assert_eq!(
            expr_display_name(&row_expr),
            "CAST((row(1, 2, 3)) AS struct<col1 INT, col2 ARRAY<INT>, \
             col3 struct<nested TINYINT>>)"
        );
    }
    #[test]
    fn final_row_and_datetime_nodes_preserve_anonymity_and_exact_temporal_declarations() {
        use novarocks_parser::{Span, ast, build};
        use novarocks_type_contract::{
            TemporalPrecision, TemporalType, ValueKind, ValueLogicalType,
        };
        let control = crate::compiler::SqlCompileControl::unbounded();
        let row = build::row_type(vec![ast::RowDataTypeField {
            name: None,
            data_type: build::simple_type("JSON"),
            span: Span::new(4, 8),
        }]);
        let value = super::sql_type_to_value_type(&row, true, &control).unwrap();
        let ValueKind::Struct(fields) = value.value_type().kind() else {
            panic!("row")
        };
        assert_eq!(fields[0].row_name(), None);
        assert!(matches!(
            fields[0].value_type().kind(),
            ValueKind::Nominal(ValueLogicalType::Json)
        ));
        for (syntax, expected) in [
            ("TIME", TemporalType::Time(TemporalPrecision::DEFAULT)),
            (
                "TIME(6)",
                TemporalType::Time(TemporalPrecision::MICROSECONDS),
            ),
            (
                "TIMESTAMP",
                TemporalType::Timestamp(TemporalPrecision::DEFAULT),
            ),
            (
                "TIMESTAMP(6)",
                TemporalType::Timestamp(TemporalPrecision::MICROSECONDS),
            ),
            (
                "TIMESTAMP(9)",
                TemporalType::Timestamp(TemporalPrecision::NANOSECONDS),
            ),
            (
                "TIME(4) WITH TIME ZONE",
                TemporalType::TimeWithTimeZone(TemporalPrecision::new(4).unwrap()),
            ),
            (
                "TIMESTAMP(12) WITH TIME ZONE",
                TemporalType::TimestampWithTimeZone(TemporalPrecision::PICOSECONDS),
            ),
        ] {
            let ty = parse_native_cast_type(&format!("SELECT CAST(NULL AS {syntax})"));
            let value = super::sql_type_to_value_type(&ty, false, &control).unwrap();
            assert_eq!(value.value_type().temporal(), Some(expected), "{syntax}");
            assert_eq!(value.data_type(), &expected.carrier(), "{syntax}");
            assert!(!value.nullable());
        }
        for parameter in [
            build::numeric_parameter(13),
            build::type_parameter(build::simple_type("BIGINT")),
        ] {
            let ty = ast::TypeName {
                kind: ast::TypeKind::DateTime(ast::DateTimeDataType {
                    kind: ast::DateTimeType::Timestamp,
                    with_time_zone: false,
                    precision: Some(ast::AstBox::new(parameter)),
                }),
                span: Span::new(0, 12),
            };
            let error = super::sql_type_to_value_type(&ty, false, &control).unwrap_err();
            assert_eq!(
                error.kind(),
                crate::analyze_error::AnalyzeErrorKind::TypeMismatch
            );
            assert_eq!(error.span(), Some(ty.span));
        }
    }

    #[test]
    fn final_unsupported_type_and_literal_facts_reject_before_catalog_discovery() {
        use crate::analyze_error::AnalyzeErrorKind;
        use novarocks_parser::{Span, ast, build};
        struct NoLookup;
        impl crate::catalog::PlannerTableProvider for NoLookup {
            fn resolve_table_for_analysis(
                &self,
                _: Option<&str>,
                _: &str,
                _: &str,
            ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
                panic!("unsupported typed syntax must precede catalog lookup");
            }
        }
        let span = Span::new(20, 80);
        let explicit = ast::TypeName {
            kind: ast::TypeKind::ExplicitArray(ast::ExplicitArrayDataType {
                element: ast::AstBox::new(build::simple_type("INTEGER")),
                length: ast::ArrayLength {
                    spelling: "9999999999999999999999999999999999999999999999999".into(),
                    span,
                },
            }),
            span,
        };
        let unsupported_types = vec![
            explicit.clone(),
            build::type_name(
                build::ident("ARRAY", false),
                vec![build::type_parameter(explicit)],
            ),
        ];
        let mut expressions = unsupported_types
            .into_iter()
            .map(|ty| build::cast(build::null(), ty))
            .collect::<Vec<_>>();
        for type_name in ["LARGEINT", "\"DATE\""] {
            expressions.push(ast::Expr::Literal(ast::Literal {
                kind: ast::LiteralKind::Generic(ast::GenericLiteral {
                    type_name: type_name.into(),
                    value: "1".into(),
                }),
                span,
            }));
        }
        for expression in expressions {
            let query =
                &mut novarocks_parser::parser::parse_query("SELECT NULL FROM missing_type_input")
                    .unwrap();
            let ast::SetExpr::Select(select) = query.body.as_mut() else {
                panic!("select")
            };
            let expected = match &expression {
                ast::Expr::Literal(ast::Literal {
                    kind: ast::LiteralKind::Generic(_),
                    ..
                }) => AnalyzeErrorKind::TypeMismatch,
                _ => AnalyzeErrorKind::UnsupportedExpression,
            };
            select.projection[0] = novarocks_parser::build::item(expression, None);
            let error = crate::analyzer::analyze(query, &NoLookup, "default").unwrap_err();
            assert_eq!(error.kind(), expected);
            assert_eq!(error.span(), Some(span));
        }
    }
}
