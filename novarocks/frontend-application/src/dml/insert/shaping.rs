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
use novarocks_parser::{Span, ast, build, printer};
use novarocks_types::naming::normalize_identifier;
use novarocks_types::schema::{ColumnDef, ColumnDefault, validate_column_default};

use super::command::{literal_query, plain_function};
#[cfg(debug_assertions)]
use crate::query_execution::dml::iceberg_writer::value_type_to_sql_type_name;
use novarocks_type_contract::FunctionValueType;

/// Type direct row sources before their VALUES/UNION common-type analysis.
/// General query sources keep the original output projection boundary.
pub(crate) fn shape_insert_source(
    source: &ast::Query,
    insert_columns: &[String],
    source_columns: &[ColumnDef],
    write_columns: &[ColumnDef],
) -> Result<Option<ast::Query>, String> {
    shape_insert_source_in(source, insert_columns, source_columns, write_columns, None)
        .map(|source| source.map(|(query, _)| query))
}

/// Preserve constants from every leaf of the original eligible source.
/// Other expressions and the SELECT/UNION structure retain their old body.
pub(crate) fn shape_insert_source_with_constants(
    source: &ast::Query,
    insert_columns: &[String],
    source_columns: &[ColumnDef],
    write_columns: &[ColumnDef],
    policy: novarocks_functions::ConstantPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<
    Option<(
        ast::Query,
        Option<novarocks_sql::compiler::SqlExpressionConstants>,
    )>,
    String,
> {
    shape_insert_source_in(
        source,
        insert_columns,
        source_columns,
        write_columns,
        Some((policy, control)),
    )
}

struct ConstantSource<'a> {
    capture: Option<(
        novarocks_functions::ConstantPolicy,
        &'a dyn novarocks_type_contract::PureCompileControl,
    )>,
    path: Vec<novarocks_sql::compiler::SqlConstantQueryStep>,
    constants: Vec<novarocks_sql::compiler::SqlExpressionConstant>,
}
impl ConstantSource<'_> {
    fn push(&mut self, step: novarocks_sql::compiler::SqlConstantQueryStep) {
        if self.capture.is_some() {
            self.path.push(step);
        }
    }
    fn pop(&mut self) {
        if self.capture.is_some() {
            self.path.pop();
        }
    }
    fn publish(
        &mut self,
        authored: Option<Vec<Option<novarocks_functions::ConstantValue>>>,
        site: impl Fn(usize) -> novarocks_sql::compiler::SqlConstantExpressionSite,
    ) {
        if let Some(authored) = authored {
            for (column, value) in authored.into_iter().enumerate() {
                if let Some(value) = value {
                    self.constants
                        .push(novarocks_sql::compiler::SqlExpressionConstant::new(
                            self.path.clone(),
                            site(column),
                            value,
                        ));
                }
            }
        }
    }
}
fn shape_insert_source_in(
    source: &ast::Query,
    insert_columns: &[String],
    source_columns: &[ColumnDef],
    write_columns: &[ColumnDef],
    capture: Option<(
        novarocks_functions::ConstantPolicy,
        &dyn novarocks_type_contract::PureCompileControl,
    )>,
) -> Result<
    Option<(
        ast::Query,
        Option<novarocks_sql::compiler::SqlExpressionConstants>,
    )>,
    String,
> {
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
    let mut constants = ConstantSource {
        capture,
        path: Vec::new(),
        constants: Vec::new(),
    };
    shape_body(
        &mut query.body,
        &mapping,
        width,
        write_columns,
        &mut constants,
    )?;
    let constants = (!constants.constants.is_empty()).then(|| constants.constants.into());
    Ok(Some((query, constants)))
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
    constants: &mut ConstantSource<'_>,
) -> Result<(), String> {
    match body {
        ast::SetExpr::Values(values) => {
            for (row_index, row) in values.rows.iter_mut().enumerate() {
                let (syntax, authored) =
                    shape_row_in(row, mapping, width, columns, constants.capture)?;
                *row = syntax;
                constants.publish(authored, |column| {
                    novarocks_sql::compiler::SqlConstantExpressionSite::ValuesCell {
                        row: row_index,
                        column,
                    }
                });
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
            let (expressions, authored) =
                shape_row_in(&row, mapping, width, columns, constants.capture)?;
            // The eligible source projections are self-contained constants. Keep
            // the original SELECT as the row-producing relation so HAVING aliases,
            // filters and DISTINCT observe source types before sink conversion.
            // Each leaf's target projection still precedes UNION common typing.
            #[cfg(debug_assertions)]
            let legacy_sql = {
                let inner_sql =
                    printer::print_query(&build::query(ast::SetExpr::Select(select.clone())));
                format!(
                    "SELECT {} FROM ({inner_sql}) AS __nr_insert_literal_src",
                    expressions
                        .iter()
                        .map(printer::print_expr)
                        .collect::<Vec<_>>()
                        .join(", "),
                )
            };
            let expressions = expressions
                .into_iter()
                .map(canonical_insert_wrapper_expression)
                .collect::<Result<Vec<_>, _>>()?;
            let inner = build::query(ast::SetExpr::Select(select.clone()));
            let query = build::query(ast::SetExpr::Select(Box::new(
                build::select(
                    expressions
                        .into_iter()
                        .map(|expr| build::item(expr, None, false))
                        .collect(),
                    vec![build::relation(build::derived(
                        inner,
                        Some(build::alias(
                            build::ident("__nr_insert_literal_src", None),
                            Vec::new(),
                            true,
                        )),
                    ))],
                    None,
                    ast::GroupBy::None,
                )
                .map_err(|error| error.to_string())?,
            )));
            #[cfg(debug_assertions)]
            crate::query_execution::dml::equivalence::require("F7", &query, || Ok(legacy_sql))?;
            *body = ast::SetExpr::Query(Box::new(query));
            // Bind constants in the final target wrapper. Its original inner
            // SELECT and the published path remain attached to the same tree.
            constants.push(novarocks_sql::compiler::SqlConstantQueryStep::NestedQuery);
            if let Some(authored) = authored.as_ref() {
                for (column, value) in authored.iter().enumerate() {
                    let (Some(value), Some(source_column)) = (value, mapping[column]) else {
                        continue;
                    };
                    let Some(expression_path) = explicit_variant_source_path(&row[source_column])
                    else {
                        continue;
                    };
                    // This original source cast and the outer target are two
                    // real expressions. Both borrow the same encoded value;
                    // ordinary inner parse_json retains its Json declaration.
                    let mut path = constants.path.clone();
                    path.push(
                        novarocks_sql::compiler::SqlConstantQueryStep::SelectFromDerived {
                            from: 0,
                        },
                    );
                    constants.constants.push(
                        novarocks_sql::compiler::SqlExpressionConstant::new(
                            path,
                            novarocks_sql::compiler::SqlConstantExpressionSite::SelectProjection {
                                column: source_column,
                            },
                            value.clone(),
                        )
                        .with_expression_path(expression_path),
                    );
                }
            }
            constants.publish(authored, |column| {
                novarocks_sql::compiler::SqlConstantExpressionSite::SelectProjection { column }
            });
            constants.pop();
        }
        ast::SetExpr::Query(query) => {
            constants.push(novarocks_sql::compiler::SqlConstantQueryStep::NestedQuery);
            shape_body(&mut query.body, mapping, width, columns, constants)?;
            constants.pop();
        }
        ast::SetExpr::SetOperation(operation) => {
            if !matches!(operation.operator, ast::SetOperator::Union)
                || !matches!(operation.quantifier, ast::SetQuantifier::All)
            {
                return Err("INSERT literal source requires UNION ALL".to_string());
            }
            constants.push(novarocks_sql::compiler::SqlConstantQueryStep::UnionLeft);
            shape_body(&mut operation.left, mapping, width, columns, constants)?;
            constants.pop();
            constants.push(novarocks_sql::compiler::SqlConstantQueryStep::UnionRight);
            shape_body(&mut operation.right, mapping, width, columns, constants)?;
            constants.pop();
        }
    }
    Ok(())
}

type ShapedInsertRow = (
    Vec<ast::Expr>,
    Option<Vec<Option<novarocks_functions::ConstantValue>>>,
);

fn shape_row_in(
    row: &[ast::Expr],
    mapping: &[Option<usize>],
    width: usize,
    columns: &[ColumnDef],
    capture: Option<(
        novarocks_functions::ConstantPolicy,
        &dyn novarocks_type_contract::PureCompileControl,
    )>,
) -> Result<ShapedInsertRow, String> {
    if row.len() != width {
        return Err(format!(
            "insert column count mismatch: expected {width} values for column list, got {}",
            row.len()
        ));
    }
    let mut constants = capture.map(|_| Vec::with_capacity(columns.len()));
    let syntax = mapping
        .iter()
        .zip(columns)
        .map(|(index, column)| {
            let expr = match index {
                Some(index) => row[*index].clone(),
                None => omitted_insert_expr(column)?,
            };
            let target = column
                .declared_value_type()
                .map_err(|error| error.to_string())?;
            let capture_variant = capture.is_some()
                && target.logical_type() == novarocks_type_contract::ValueLogicalType::Variant;
            let mut encoded = None;
            let syntax = if capture_variant {
                constrain_literal_in(expr, &target, Some(&mut encoded))?
            } else {
                constrain_literal(expr, &target)?
            };
            let value = match (encoded, capture) {
                (Some(bytes), Some((policy, control))) => {
                    Some(variant_constant_value(&bytes, target, policy, control)?)
                }
                _ => None,
            };
            if let Some(constants) = constants.as_mut() {
                constants.push(value);
            }
            Ok(syntax)
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok((syntax, constants))
}

fn variant_constant_value(
    bytes: &[u8],
    target: novarocks_type_contract::FunctionValueType,
    policy: novarocks_functions::ConstantPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<novarocks_functions::ConstantValue, String> {
    use novarocks_type_contract::CompilePhase;
    // JSON null is an encoded non-NULL Variant value; SQL NULL stays on the
    // original syntax path and is never inferred from payload contents.
    let value_type = target
        .with_nullable(false)
        .map_err(|error| error.to_string())?;
    let field = std::sync::Arc::new(arrow::datatypes::Field::new(
        "",
        value_type.data_type().clone(),
        false,
    ));
    let value = novarocks_functions::ConstantValue::from_binary(
        field,
        value_type,
        bytes,
        policy,
        CompilePhase::Validate,
        control,
    )
    .map_err(|error| error.to_string())?;
    Ok(value)
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

fn is_variant_cast(cast: &ast::CastExpr) -> bool {
    printer::print_object_name(&cast.data_type.name).eq_ignore_ascii_case("variant")
}
fn explicit_variant_source_path(
    mut expr: &ast::Expr,
) -> Option<Vec<novarocks_sql::compiler::SqlConstantExpressionStep>> {
    let mut path = Vec::new();
    while let ast::Expr::Nested(nested) = expr {
        path.push(novarocks_sql::compiler::SqlConstantExpressionStep::Nested);
        expr = &nested.expression;
    }
    matches!(expr, ast::Expr::Cast(cast) if is_variant_cast(cast)).then_some(path)
}

fn constrain_literal(expr: ast::Expr, target: &FunctionValueType) -> Result<ast::Expr, String> {
    constrain_literal_in(expr, target, None)
}
fn constrain_literal_in(
    mut expr: ast::Expr,
    target: &FunctionValueType,
    mut encoded: Option<&mut Option<Vec<u8>>>,
) -> Result<ast::Expr, String> {
    match &mut expr {
        // Explicit CAST is a semantic boundary. Only its own VARIANT declaration
        // authorizes the existing constant JSON representation conversion.
        ast::Expr::Cast(cast) => {
            if is_variant_cast(cast)
                && let Some(syntax) = constant_variant_in(&cast.expr, encoded.as_deref_mut())?
            {
                *cast.expr = syntax;
            }
        }
        ast::Expr::Nested(nested) => {
            *nested.expression =
                constrain_literal_in((*nested.expression).clone(), target, encoded.as_deref_mut())?;
        }
        ast::Expr::Literal(value) => {
            if matches!(target.data_type(), DataType::Binary | DataType::LargeBinary)
                && let ast::LiteralKind::String(text) = &value.kind
            {
                value.kind = ast::LiteralKind::HexString(hex::encode_upper(
                    novarocks_sql::literal::latin1_string_to_bytes(text)?,
                ));
            }
        }
        ast::Expr::Array(array) if array.element_type.is_none() => {
            if matches!(target.data_type(), DataType::List(_)) {
                let item = target_child(target, 0)?;
                for element in &mut array.elements {
                    *element = constrain_literal(element.clone(), &item)?;
                }
            }
        }
        ast::Expr::Map(map) => {
            if map_types(target.data_type()).is_some() {
                let key = target_child(target, 0)?;
                let value = target_child(target, 1)?;
                for entry in &mut map.entries {
                    entry.key = constrain_literal(entry.key.clone(), &key)?;
                    entry.value = constrain_literal(entry.value.clone(), &value)?;
                }
            }
        }
        ast::Expr::Tuple(tuple) => {
            if let DataType::Struct(fields) = target.data_type()
                && fields.len() == tuple.expressions.len()
            {
                for (index, value) in tuple.expressions.iter_mut().enumerate() {
                    *value = constrain_literal(value.clone(), &target_child(target, index)?)?;
                }
            }
        }
        ast::Expr::Struct(structure) => {
            if let DataType::Struct(fields) = target.data_type()
                && fields.len() == structure.fields.len()
            {
                for (index, value) in structure.fields.iter_mut().enumerate() {
                    value.value =
                        constrain_literal(value.value.clone(), &target_child(target, index)?)?;
                }
            }
        }
        ast::Expr::FunctionCall(function) if plain_function(function) => {
            let name = printer::print_object_name(&function.name).to_ascii_lowercase();
            match (name.as_str(), target.data_type()) {
                ("array", DataType::List(_)) => {
                    let item = target_child(target, 0)?;
                    for value in &mut function.arguments {
                        *value = constrain_literal(value.clone(), &item)?;
                    }
                }
                ("row", DataType::Struct(fields)) if fields.len() == function.arguments.len() => {
                    for (index, value) in function.arguments.iter_mut().enumerate() {
                        *value = constrain_literal(value.clone(), &target_child(target, index)?)?;
                    }
                }
                ("named_struct", DataType::Struct(fields))
                    if fields.len() * 2 == function.arguments.len() =>
                {
                    for (index, pair) in function
                        .arguments
                        .as_chunks_mut::<2>()
                        .0
                        .iter_mut()
                        .enumerate()
                    {
                        pair[1] =
                            constrain_literal(pair[1].clone(), &target_child(target, index)?)?;
                    }
                }
                ("map", DataType::Map(..)) => {
                    if map_types(target.data_type()).is_some() {
                        let key = target_child(target, 0)?;
                        let value = target_child(target, 1)?;
                        for pair in function.arguments.as_chunks_mut::<2>().0 {
                            pair[0] = constrain_literal(pair[0].clone(), &key)?;
                            pair[1] = constrain_literal(pair[1].clone(), &value)?;
                        }
                    }
                }
                ("parse_json", DataType::LargeBinary)
                    if target.logical_type()
                        == novarocks_type_contract::ValueLogicalType::Variant =>
                {
                    if let Some(syntax) = constant_variant_in(&expr, encoded)? {
                        expr = syntax;
                    }
                }
                _ => {}
            }
        }
        _ => {}
    }
    cast_expr(expr, target)
}

fn target_child(target: &FunctionValueType, index: usize) -> Result<FunctionValueType, String> {
    target
        .child_at(index)
        .ok_or_else(|| "INSERT target lacks its declared semantic child".to_string())
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

fn constant_variant_in(
    expr: &ast::Expr,
    encoded: Option<&mut Option<Vec<u8>>>,
) -> Result<Option<ast::Expr>, String> {
    let Some(bytes) = constant_variant_bytes(expr)? else {
        return Ok(None);
    };
    let syntax = variant_bytes_expr(&bytes, expr.span());
    if let Some(encoded) = encoded {
        *encoded = Some(bytes);
    }
    Ok(Some(syntax))
}
fn variant_bytes_expr(bytes: &[u8], span: Span) -> ast::Expr {
    ast::Expr::Literal(ast::Literal {
        kind: ast::LiteralKind::HexString(hex::encode_upper(bytes)),
        span,
    })
}
fn constant_variant_bytes(expr: &ast::Expr) -> Result<Option<Vec<u8>>, String> {
    if let ast::Expr::Nested(nested) = expr {
        return constant_variant_bytes(&nested.expression);
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
    Ok(Some(bytes))
}

fn cast_expr(expr: ast::Expr, target: &FunctionValueType) -> Result<ast::Expr, String> {
    let data_type = novarocks_sql::sql_type_syntax(target).map_err(|error| error.to_string())?;
    #[cfg(debug_assertions)]
    {
        let template = build::query(ast::SetExpr::Select(Box::new(
            build::select(
                vec![build::item(
                    build::cast(build::null(), data_type.clone()),
                    None,
                    false,
                )],
                Vec::new(),
                None,
                ast::GroupBy::None,
            )
            .map_err(|error| error.to_string())?,
        )));
        crate::query_execution::dml::equivalence::require("F8", &template, || {
            Ok(format!(
                "SELECT CAST(NULL AS {})",
                value_type_to_sql_type_name(target)?
            ))
        })?;
    }
    Ok(build::cast(expr, data_type))
}

fn literal(kind: ast::LiteralKind) -> ast::Expr {
    build::literal(kind)
}
fn null_expr() -> ast::Expr {
    build::null()
}
fn number(value: String) -> ast::Expr {
    // Typed defaults retain their exact coefficient, including values that are
    // not source-language i64 literals. This is not a parser rejection fallback.
    build::literal(ast::LiteralKind::Number(value))
}
fn string(value: String) -> ast::Expr {
    build::string(value)
}
fn binary(value: &[u8]) -> ast::Expr {
    build::hex_string(hex::encode_upper(value))
}
fn array(elements: Vec<ast::Expr>) -> ast::Expr {
    build::array(elements)
}
fn tuple(expressions: Vec<ast::Expr>) -> Result<ast::Expr, String> {
    build::tuple_value(expressions).map_err(|error| error.to_string())
}
fn map(entries: Vec<(ast::Expr, ast::Expr)>) -> ast::Expr {
    build::map(entries)
}

/// Normalize only the expressions whose original INSERT wrapper crossed the
/// printer/parser boundary. VALUES retains its existing typed-value syntax.
/// Source positions and the shape of every already-admitted user subtree stay
/// attached to that subtree; no tree is returned after construction fails.
pub(crate) fn canonical_insert_wrapper_expression(expr: ast::Expr) -> Result<ast::Expr, String> {
    use ast::Fold;
    #[derive(Default)]
    struct WrapperSyntax {
        error: Option<String>,
    }
    impl ast::Fold for WrapperSyntax {
        fn fold_expr(&mut self, expression: ast::Expr) -> ast::Expr {
            if self.error.is_some() {
                return expression;
            }
            match expression {
                ast::Expr::Literal(original) => {
                    let ast::LiteralKind::Number(value) = &original.kind else {
                        return ast::Expr::Literal(original);
                    };
                    match build::number(value.clone()) {
                        Ok(mut canonical) => {
                            match &mut canonical {
                                ast::Expr::Literal(literal) => literal.span = original.span,
                                ast::Expr::Unary(unary) => {
                                    unary.span = original.span;
                                    if let ast::Expr::Literal(literal) = unary.expression.as_mut() {
                                        literal.span = original.span;
                                    }
                                }
                                _ => unreachable!("numeric constructor returns literal or unary"),
                            }
                            canonical
                        }
                        Err(error) => {
                            self.error = Some(error.to_string());
                            ast::Expr::Literal(original)
                        }
                    }
                }
                ast::Expr::Tuple(tuple) => {
                    let expressions = tuple
                        .expressions
                        .into_iter()
                        .map(|expr| self.fold_expr(expr))
                        .collect();
                    match build::tuple(expressions) {
                        Ok(mut canonical) => {
                            match &mut canonical {
                                ast::Expr::Tuple(value) => value.span = tuple.span,
                                ast::Expr::Nested(value) => value.span = tuple.span,
                                _ => unreachable!("tuple constructor returns tuple or parentheses"),
                            }
                            canonical
                        }
                        Err(error) => {
                            self.error = Some(error.to_string());
                            build::null()
                        }
                    }
                }
                expression => ast::fold_expr(self, expression),
            }
        }
    }
    let mut syntax = WrapperSyntax::default();
    let expression = syntax.fold_expr(expr);
    match syntax.error {
        Some(error) => Err(error),
        None => Ok(expression),
    }
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
            )?
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
