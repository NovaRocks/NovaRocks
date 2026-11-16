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
use novarocks_type_contract::FunctionValueType;

/// Type direct row sources before their VALUES/UNION common-type analysis.
/// General query sources keep the original output projection boundary.
#[cfg(test)]
pub(crate) fn shape_insert_source(
    source: &ast::Query,
    insert_columns: &[String],
    source_columns: &[ColumnDef],
    write_columns: &[ColumnDef],
) -> Result<Option<ast::Query>, String> {
    shape_insert_source_in(
        source,
        insert_columns,
        source_columns,
        write_columns,
        None,
        false,
    )
    .map(|source| source.map(|(query, _)| query))
}

/// Preserve constants from every leaf of the original eligible source.
/// Other expressions and the SELECT/UNION structure retain their old body.
#[cfg(test)]
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
        false,
    )
}

struct ConstantSource<'a> {
    typed_write: bool,
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
    typed_write: bool,
) -> Result<
    Option<(
        ast::Query,
        Option<novarocks_sql::compiler::SqlExpressionConstants>,
    )>,
    String,
> {
    let uncontrolled_values = matches!(source.body.as_ref(), ast::SetExpr::Values(_))
        && source.functions.is_empty()
        && source.with.is_none()
        && source.modifiers.order_by.is_none()
        && source.modifiers.limit.is_none()
        && source.modifiers.offset.is_none()
        && source.modifiers.fetch.is_none();
    if (typed_write && !uncontrolled_values) || (!uncontrolled_values && !literal_query(source)) {
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
        typed_write,
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
                || operation.corresponding.is_some()
                || operation.inputs.iter().any(body_has_distinct_set)
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
                let (syntax, authored) = shape_row_in(
                    row,
                    mapping,
                    width,
                    columns,
                    constants.capture,
                    constants.typed_write,
                )?;
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
            let (expressions, authored) = shape_row_in(
                &row,
                mapping,
                width,
                columns,
                constants.capture,
                constants.typed_write,
            )?;
            // The eligible source projections are self-contained constants. Keep
            // the original SELECT as the row-producing relation so HAVING aliases,
            // filters and DISTINCT observe source types before sink conversion.
            // Each leaf's target projection still precedes UNION common typing.
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
                            build::ident("__nr_insert_literal_src", false),
                            Vec::new(),
                            true,
                        )),
                    ))],
                    None,
                    ast::GroupBy::None,
                )
                .map_err(|error| error.to_string())?,
            )));
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
                || operation.corresponding.is_some()
            {
                return Err(
                    "INSERT literal source requires UNION ALL without CORRESPONDING".to_string(),
                );
            }
            for (index, input) in operation.inputs.iter_mut().enumerate() {
                constants.push(novarocks_sql::compiler::SqlConstantQueryStep::SetOperand { index });
                shape_body(input, mapping, width, columns, constants)?;
                constants.pop();
            }
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
    typed_write: bool,
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
            let syntax = if typed_write {
                typed_write_literal_source(expr, &target, capture_variant.then_some(&mut encoded))?
            } else if capture_variant {
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
    matches!(&cast.data_type.kind, ast::TypeKind::Generic(generic) if generic.name.value.eq_ignore_ascii_case("variant"))
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
                && let Some(syntax) = constant_variant_in(&cast.expression, encoded.as_deref_mut())?
            {
                cast.expression = syntax;
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
                value.kind =
                    ast::LiteralKind::Binary(novarocks_sql::literal::latin1_string_to_bytes(text)?);
            }
        }
        ast::Expr::Array(array) => {
            if matches!(target.data_type(), DataType::List(_)) {
                let item = target_child(target, 0)?;
                for element in &mut array.values {
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
        ast::Expr::Row(structure) => {
            if let DataType::Struct(fields) = target.data_type()
                && fields.len() == structure.fields.len()
            {
                for (index, value) in structure.fields.iter_mut().enumerate() {
                    value.expression =
                        constrain_literal(value.expression.clone(), &target_child(target, index)?)?;
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
        kind: ast::LiteralKind::Binary(bytes.to_vec()),
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
                    match &cast.data_type.kind {
                        ast::TypeKind::Generic(value) => value.name.value.to_ascii_lowercase(),
                        _ => String::new(),
                    }
                    .as_str(),
                    "varchar" | "string" | "text"
                ) =>
            {
                string_literal(&cast.expression)
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
    Ok(build::cast(expr, data_type))
}

fn literal(kind: ast::LiteralKind) -> ast::Expr {
    build::literal(kind)
}
fn null_expr() -> ast::Expr {
    build::null()
}
fn string(value: String) -> ast::Expr {
    build::string(value)
}
fn binary(value: &[u8]) -> ast::Expr {
    build::binary_literal(value.to_vec())
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
                // The SELECT wrapper preserves the original positional default
                // rule: one field becomes parentheses; multiple fields retain
                // their individual locations and positional value shape.
                ast::Expr::Row(mut row) if row.fields.iter().all(|field| field.name.is_none()) => {
                    row = ast::fold_row_expr(self, row);
                    match row.fields.len() {
                        0 => {
                            self.error = Some(build::BuildError::EmptyTuple.to_string());
                            build::null()
                        }
                        1 => {
                            let mut fields = row.fields.into_iter();
                            let field = fields.next().expect("one positional field");
                            let mut canonical = build::nested(field.expression);
                            let ast::Expr::Nested(nested) = &mut canonical else {
                                unreachable!("nested constructor returns parentheses");
                            };
                            nested.span = row.span;
                            canonical
                        }
                        _ => ast::Expr::Row(row),
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
        ) => build::integer(i64::from(*value)),
        (ColumnDefault::Int64(value), DataType::Int64) => build::integer(*value),
        (ColumnDefault::Float32 { bits }, DataType::Float32) => literal(ast::LiteralKind::Double(
            ast::DoubleValue::from_f64(f64::from(f32::from_bits(*bits))),
        )),
        (ColumnDefault::Float64 { bits }, DataType::Float64) => literal(ast::LiteralKind::Double(
            ast::DoubleValue::from_f64(f64::from_bits(*bits)),
        )),
        (
            ColumnDefault::Decimal {
                unscaled,
                precision,
                scale,
            },
            DataType::Decimal128(target_precision, target_scale),
        ) if precision == target_precision && scale == target_scale => literal(
            ast::LiteralKind::Decimal(format_decimal(*unscaled, *scale)?),
        ),
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

/// Direct uncontrolled VALUES can reorder cells/defaults without changing their
/// row/common-type semantics. General sources are wrapped after their complete
/// query by the existing writer projection author.
pub(crate) fn shape_typed_write_source(
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
    shape_insert_source_in(
        source,
        insert_columns,
        source_columns,
        write_columns,
        capture,
        true,
    )
}
fn typed_write_literal_source(
    mut expression: ast::Expr,
    target: &FunctionValueType,
    encoded: Option<&mut Option<Vec<u8>>>,
) -> Result<ast::Expr, String> {
    match &mut expression {
        ast::Expr::Nested(nested) => {
            *nested.expression =
                typed_write_literal_source((*nested.expression).clone(), target, encoded)?;
        }
        ast::Expr::Cast(cast)
            if is_variant_cast(cast)
                && target.logical_type() == novarocks_type_contract::ValueLogicalType::Variant =>
        {
            if let Some(syntax) = constant_variant_in(&cast.expression, encoded)? {
                cast.expression = syntax;
            }
        }
        _ => {}
    }
    Ok(expression)
}

/// Attach context only along the exact projection/derived-alias geometry
/// authored by the INSERT wrapper above the complete source query.
pub(crate) fn attach_write_literal_contexts(
    query: &ast::Query,
    assignments: std::sync::Arc<novarocks_sql::compiler::SqlWriteAssignments>,
    control: &novarocks_sql::compiler::SqlCompileControl,
) -> Result<
    std::sync::Arc<novarocks_sql::compiler::SqlWriteAssignments>,
    novarocks_sql::compiler::SqlCompileError,
> {
    use novarocks_sql::compiler::{SqlCompileError, SqlConstantQueryStep};
    use novarocks_type_contract::PureCompileControl;
    if source_assignment_before_common_type(query) {
        let mut contexts = Vec::new();
        for site in assignments.sites() {
            append_write_literal_sites(
                &query.body,
                site.output_ordinal(),
                site.output_ordinal(),
                &mut Vec::new(),
                &mut contexts,
                control,
                true,
            )?;
        }
        let assignments = std::sync::Arc::try_unwrap(assignments).map_err(|_| {
            SqlCompileError::InvalidRequest(
                "INSERT source assignments require their original declaration owner".into(),
            )
        })?;
        return Ok(std::sync::Arc::new(
            assignments.with_literal_contexts(contexts.into_boxed_slice()),
        ));
    }
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(assignments);
    };
    let assignments = if select.projection.iter().all(|item| {
        matches!(
            item,
            ast::SelectItem::UnnamedExpr(_) | ast::SelectItem::ExprWithAlias { .. }
        )
    }) {
        let mut sites = Vec::new();
        sites
            .try_reserve_exact(assignments.sites().len())
            .map_err(|cause| {
                SqlCompileError::CompilationSource(
                    novarocks_failure_contract::SharedFailureCause::new(
                        novarocks_functions::allocation_failure::AllocationRefusal::new(
                            "INSERT source assignment sites",
                            cause,
                        ),
                    ),
                )
            })?;
        for site in assignments.sites() {
            control.checkpoint(novarocks_type_contract::CompilePhase::Validate, 1)?;
            let site = match select.projection.get(site.output_ordinal()) {
                Some(
                    ast::SelectItem::UnnamedExpr(expr)
                    | ast::SelectItem::ExprWithAlias { expr, .. },
                ) => site.clone().with_source_span(expr.span()),
                _ => site.clone(),
            };
            sites.push(site);
        }
        std::sync::Arc::new(novarocks_sql::compiler::SqlWriteAssignments::try_new(
            sites.into_boxed_slice(),
            control,
        )?)
    } else {
        assignments
    };

    let [from] = select.from.as_slice() else {
        return Ok(assignments);
    };
    if !from.joins.is_empty() {
        return Ok(assignments);
    }
    let ast::TableFactor::Derived {
        subquery,
        alias: Some(alias),
        ..
    } = &from.relation
    else {
        return Ok(assignments);
    };
    if alias.name.value != "__nr_insert_src" {
        return Ok(assignments);
    }
    let mut contexts = Vec::new();
    for site in assignments.sites() {
        control.checkpoint(novarocks_type_contract::CompilePhase::Validate, 1)?;
        let Some(ast::SelectItem::ExprWithAlias { expr, .. } | ast::SelectItem::UnnamedExpr(expr)) =
            select.projection.get(site.output_ordinal())
        else {
            continue;
        };
        let Some(chain) = ast::identifier_chain_observed(expr, |_| {
            control.checkpoint(novarocks_type_contract::CompilePhase::Validate, 1)
        })?
        else {
            continue;
        };
        if chain.len() != 2 {
            continue;
        }
        let qualifier = chain.first();
        let name = chain.last();
        if qualifier.value != alias.name.value {
            continue;
        }
        let ordinal = alias
            .columns
            .iter()
            .position(|column| column.value == name.value)
            .ok_or_else(|| {
                SqlCompileError::InvalidRequest(
                    "INSERT source reference lost its original alias ordinal".into(),
                )
            })?;
        let mut path = vec![SqlConstantQueryStep::SelectFromDerived { from: 0 }];
        append_write_literal_sites(
            &subquery.body,
            ordinal,
            site.output_ordinal(),
            &mut path,
            &mut contexts,
            control,
            source_assignment_before_common_type(subquery),
        )?;
    }
    if contexts.is_empty() {
        return Ok(assignments);
    }
    let assignments = std::sync::Arc::try_unwrap(assignments).map_err(|_| {
        SqlCompileError::InvalidRequest(
            "INSERT literal contexts require their newly projected declaration owner".into(),
        )
    })?;
    Ok(std::sync::Arc::new(
        assignments.with_literal_contexts(contexts.into_boxed_slice()),
    ))
}
/// Keep early assignment within the original literal INSERT sources. Source
/// controls retain their independent inference above this boundary.
fn source_assignment_before_common_type(query: &ast::Query) -> bool {
    if !query.functions.is_empty()
        || query.with.is_some()
        || query.modifiers.order_by.is_some()
        || query.modifiers.limit.is_some()
        || query.modifiers.offset.is_some()
        || query.modifiers.fetch.is_some()
    {
        return false;
    }
    fn eligible(body: &ast::SetExpr) -> bool {
        match body {
            ast::SetExpr::Values(_) => true,
            ast::SetExpr::Select(select) => {
                select.modifiers.is_empty()
                    && select.from.is_empty()
                    && !matches!(select.quantifier, ast::SelectQuantifier::Distinct { .. })
                    && select.selection.is_none()
                    && matches!(select.group_by, ast::GroupBy::None)
                    && select.having.is_none()
                    && select.qualify.is_none()
                    && select.windows.is_empty()
                    && select.projection.iter().all(|item| match item {
                        ast::SelectItem::UnnamedExpr(expr)
                        | ast::SelectItem::ExprWithAlias { expr, .. } => {
                            super::command::is_literal_expr(expr)
                        }
                        _ => false,
                    })
            }
            ast::SetExpr::Query(query) => source_assignment_before_common_type(query),
            ast::SetExpr::SetOperation(operation) => {
                matches!(operation.operator, ast::SetOperator::Union)
                    && operation.quantifier == ast::SetQuantifier::All
                    && operation.corresponding.is_none()
                    && operation.inputs.iter().all(eligible)
            }
        }
    }
    eligible(&query.body)
}

fn append_write_literal_sites(
    body: &ast::SetExpr,
    source_ordinal: usize,
    output_ordinal: usize,
    path: &mut Vec<novarocks_sql::compiler::SqlConstantQueryStep>,
    contexts: &mut Vec<novarocks_sql::compiler::SqlWriteLiteralContext>,
    control: &novarocks_sql::compiler::SqlCompileControl,
    assignment_before_common_type: bool,
) -> Result<(), novarocks_sql::compiler::SqlCompileError> {
    use novarocks_sql::compiler::{
        SqlConstantExpressionSite as Site, SqlConstantQueryStep as Step, SqlWriteLiteralContext,
    };
    use novarocks_type_contract::PureCompileControl;
    control.checkpoint(novarocks_type_contract::CompilePhase::Validate, 1)?;
    match body {
        ast::SetExpr::Values(values) => {
            for (row, values) in values.rows.iter().enumerate() {
                if source_ordinal >= values.len() {
                    continue;
                }
                let context = SqlWriteLiteralContext::new(
                    path.clone(),
                    Site::ValuesCell {
                        row,
                        column: source_ordinal,
                    },
                    output_ordinal,
                );
                contexts.push(if assignment_before_common_type {
                    context.with_source_assignment()
                } else {
                    context
                });
            }
        }
        ast::SetExpr::Select(select) => {
            if select.projection.iter().all(|item| {
                matches!(
                    item,
                    ast::SelectItem::UnnamedExpr(_) | ast::SelectItem::ExprWithAlias { .. }
                )
            }) && source_ordinal < select.projection.len()
            {
                let context = SqlWriteLiteralContext::new(
                    path.clone(),
                    Site::SelectProjection {
                        column: source_ordinal,
                    },
                    output_ordinal,
                );
                contexts.push(if assignment_before_common_type {
                    context.with_source_assignment()
                } else {
                    context
                });
            }
        }
        ast::SetExpr::Query(query) => {
            path.push(Step::NestedQuery);
            append_write_literal_sites(
                &query.body,
                source_ordinal,
                output_ordinal,
                path,
                contexts,
                control,
                assignment_before_common_type,
            )?;
            path.pop();
        }
        ast::SetExpr::SetOperation(operation) => {
            for (index, child) in operation.inputs.iter().enumerate() {
                path.push(Step::SetOperand { index });
                append_write_literal_sites(
                    child,
                    source_ordinal,
                    output_ordinal,
                    path,
                    contexts,
                    control,
                    assignment_before_common_type,
                )?;
                path.pop();
            }
        }
    }
    Ok(())
}

/// Keep original explicit VARIANT constants in the existing holder even when
/// column selection wraps a complete non-VALUES query. This is no implicit
/// source-to-target conversion; only the source's own explicit CAST authorizes it.
pub(crate) fn capture_typed_write_constants(
    query: &mut ast::Query,
    insert_columns: &[String],
    source_columns: &[ColumnDef],
    write_columns: &[ColumnDef],
    policy: novarocks_functions::ConstantPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<novarocks_sql::compiler::SqlExpressionConstants>, String> {
    use novarocks_sql::compiler::SqlConstantQueryStep as Step;
    let mapping = column_mapping(insert_columns, source_columns, write_columns)?;
    let mut source = ConstantSource {
        typed_write: true,
        capture: Some((policy, control)),
        path: Vec::new(),
        constants: Vec::new(),
    };
    if let ast::SetExpr::Select(select) = query.body.as_mut()
        && select.from.len() == 1
        && let ast::TableFactor::Derived {
            subquery,
            alias: Some(alias),
            ..
        } = &mut select.from[0].relation
        && alias.name.value == "__nr_insert_src"
    {
        source.path.push(Step::SelectFromDerived { from: 0 });
        capture_typed_write_body(&mut subquery.body, &mapping, write_columns, &mut source)?;
    } else {
        capture_typed_write_body(&mut query.body, &mapping, write_columns, &mut source)?;
    }
    Ok((!source.constants.is_empty()).then(|| source.constants.into()))
}
fn capture_typed_write_body(
    body: &mut ast::SetExpr,
    mapping: &[Option<usize>],
    columns: &[ColumnDef],
    source: &mut ConstantSource<'_>,
) -> Result<(), String> {
    use novarocks_sql::compiler::{
        SqlConstantExpressionSite as Site, SqlConstantQueryStep as Step, SqlExpressionConstant,
    };
    let Some((policy, control)) = source.capture else {
        return Ok(());
    };
    control
        .checkpoint(novarocks_type_contract::CompilePhase::Validate, 1)
        .map_err(|error| error.to_string())?;
    let mut capture =
        |expression: &mut ast::Expr, site, target: &ColumnDef| -> Result<(), String> {
            let target = target
                .declared_value_type()
                .map_err(|error| error.to_string())?;
            if target.logical_type() != novarocks_type_contract::ValueLogicalType::Variant
                || explicit_variant_source_path(expression).is_none()
            {
                return Ok(());
            }
            let mut encoded = None;
            *expression =
                typed_write_literal_source(expression.clone(), &target, Some(&mut encoded))?;
            if let Some(encoded) = encoded {
                source.constants.push(SqlExpressionConstant::new(
                    source.path.clone(),
                    site,
                    variant_constant_value(&encoded, target, policy, control)?,
                ));
            }
            Ok(())
        };
    match body {
        ast::SetExpr::Values(values) => {
            for (row_index, row) in values.rows.iter_mut().enumerate() {
                for (target, source_index) in columns.iter().zip(mapping) {
                    if let Some(index) = source_index
                        && let Some(expression) = row.get_mut(*index)
                    {
                        capture(
                            expression,
                            Site::ValuesCell {
                                row: row_index,
                                column: *index,
                            },
                            target,
                        )?;
                    }
                }
            }
        }
        ast::SetExpr::Select(select) => {
            if select.projection.iter().any(|item| {
                !matches!(
                    item,
                    ast::SelectItem::UnnamedExpr(_) | ast::SelectItem::ExprWithAlias { .. }
                )
            }) {
                return Ok(());
            }
            for (target, source_index) in columns.iter().zip(mapping) {
                if let Some(index) = source_index
                    && let Some(
                        ast::SelectItem::UnnamedExpr(expression)
                        | ast::SelectItem::ExprWithAlias {
                            expr: expression, ..
                        },
                    ) = select.projection.get_mut(*index)
                {
                    capture(
                        expression,
                        Site::SelectProjection { column: *index },
                        target,
                    )?;
                }
            }
        }
        ast::SetExpr::Query(query) => {
            source.push(Step::NestedQuery);
            capture_typed_write_body(&mut query.body, mapping, columns, source)?;
            source.pop();
        }
        ast::SetExpr::SetOperation(operation)
            if matches!(operation.operator, ast::SetOperator::Union)
                && matches!(operation.quantifier, ast::SetQuantifier::All)
                && operation.corresponding.is_none() =>
        {
            for (index, input) in operation.inputs.iter_mut().enumerate() {
                source.push(Step::SetOperand { index });
                capture_typed_write_body(input, mapping, columns, source)?;
                source.pop();
            }
        }
        // The existing constant holder never admitted DISTINCT set paths.
        ast::SetExpr::SetOperation(_) => {}
    }
    Ok(())
}

#[cfg(test)]
mod typed_write_source_tests {
    use super::*;
    fn query(sql: &str) -> ast::Query {
        let statements = novarocks_parser::parse(sql).unwrap();
        let [ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("query");
        };
        query.clone()
    }
    fn column(name: &str, ty: DataType) -> ColumnDef {
        ColumnDef::from_value_type(name.into(), FunctionValueType::new(ty, true), None).unwrap()
    }
    #[test]
    fn write_source_values_keep_real_literal_bytes_and_default_coefficients_without_target_cast() {
        let mut columns = vec![
            column("payload", DataType::Binary),
            column("amount", DataType::Int64),
        ];
        columns[1].write_default = Some(ColumnDefault::Int64(9007199254740993));
        let (query, constants) = shape_typed_write_source(
            &query("VALUES ('abc')"),
            &["payload".into()],
            &columns,
            &columns,
            None,
        )
        .unwrap()
        .unwrap();
        assert!(constants.is_none());
        let ast::SetExpr::Values(values) = query.body.as_ref() else {
            panic!("VALUES");
        };
        assert!(
            matches!(&values.rows[0][0], ast::Expr::Literal(ast::Literal { kind: ast::LiteralKind::String(bytes), .. }) if bytes == "abc")
        );
        assert!(matches!(
            &values.rows[0][1],
            ast::Expr::Literal(ast::Literal {
                kind: ast::LiteralKind::Long {
                    parsed_value: 9007199254740993,
                    ..
                },
                ..
            })
        ));
    }
    #[test]
    fn write_source_general_distinct_union_limit_keeps_its_complete_semantics() {
        use ast::SyntaxEq;
        let columns = vec![column("p", DataType::Int64)];
        for sql in [
            "SELECT '1' AS p LIMIT 0",
            "SELECT '1' AS p OFFSET 1",
            "SELECT '1' AS p ORDER BY p",
            "SELECT '1' AS p FETCH FIRST 1 ROW ONLY",
            "SELECT DISTINCT '1' AS p ORDER BY p LIMIT 1",
            "SELECT '1' AS p UNION SELECT '01' AS p",
        ] {
            let source = query(sql);
            assert!(!source_assignment_before_common_type(&source), "{sql}");
            assert!(
                shape_typed_write_source(&source, &[], &columns, &columns, None)
                    .unwrap()
                    .is_none()
            );
            assert!(source.syntax_eq(&query(sql)));
        }
    }
    #[test]
    fn corresponding_union_does_not_enter_positional_literal_assignment() {
        let columns = vec![column("p", DataType::Int64)];
        let mut source = query("SELECT 1 AS p UNION ALL SELECT 2 AS p UNION ALL SELECT 3 AS p");
        let ast::SetExpr::SetOperation(operation) = source.body.as_mut() else {
            panic!("set operation");
        };
        operation.corresponding = Some(ast::Corresponding {
            columns: Vec::new(),
            span: operation.span,
        });
        assert!(!source_assignment_before_common_type(&source));
        assert!(body_has_distinct_set(&source.body));
        assert!(
            shape_typed_write_source(&source, &[], &columns, &columns, None)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn explicit_variant_capture_keeps_source_domain_and_constant_holder() {
        use novarocks_sql::compiler::SqlCompileControl;
        let target = FunctionValueType::try_with_logical_type(
            DataType::LargeBinary,
            true,
            novarocks_type_contract::ValueLogicalType::Variant,
        )
        .unwrap();
        let columns = vec![ColumnDef::from_value_type("p".into(), target, None).unwrap()];
        let mut source = query("SELECT CAST(parse_json('{\"a\":1}') AS VARIANT) AS p");
        let constants = capture_typed_write_constants(
            &mut source,
            &[],
            &columns,
            &columns,
            crate::application::test_constant_policy(),
            &SqlCompileControl::unbounded(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(constants.len(), 1);
        assert_eq!(
            constants[0].value().value_type().logical_type(),
            novarocks_type_contract::ValueLogicalType::Variant
        );
        assert!(!constants[0].value().value_type().nullable());
        let ast::SetExpr::Select(select) = source.body.as_ref() else {
            panic!("SELECT");
        };
        let ast::SelectItem::ExprWithAlias {
            expr: ast::Expr::Cast(cast),
            ..
        } = &select.projection[0]
        else {
            panic!("original explicit CAST");
        };
        assert!(is_variant_cast(cast));
        assert!(matches!(
            &cast.expression,
            ast::Expr::Literal(ast::Literal {
                kind: ast::LiteralKind::Binary(_),
                ..
            })
        ));
    }
}
