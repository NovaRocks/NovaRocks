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

use arrow::array::{ArrayRef, BooleanArray, StringArray};
use arrow::datatypes::DataType;
use novarocks_parser::{ast, printer};

use crate::mv::domain::readiness::MvReadinessPort;
use novarocks_query_application::api::{
    QueryResult, ResultField as QueryResultColumn, build_arrow_query_result,
};
use novarocks_query_application::protocol_delivery::QuerySessionOutput as StatementResult;

#[derive(Clone, Debug)]
struct MaterializedViewInfoRow {
    table_schema: String,
    table_name: String,
    is_active: bool,
    inactive_reason: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InfoColumn {
    TableSchema,
    TableName,
    IsActive,
    InactiveReason,
}

impl InfoColumn {
    fn parse(name: &str) -> Option<Self> {
        match normalize_column_name(name).as_str() {
            "table_schema" => Some(Self::TableSchema),
            "table_name" => Some(Self::TableName),
            "is_active" => Some(Self::IsActive),
            "inactive_reason" => Some(Self::InactiveReason),
            _ => None,
        }
    }

    fn sql_name(self) -> &'static str {
        match self {
            Self::TableSchema => "TABLE_SCHEMA",
            Self::TableName => "TABLE_NAME",
            Self::IsActive => "IS_ACTIVE",
            Self::InactiveReason => "INACTIVE_REASON",
        }
    }

    fn data_type(self) -> DataType {
        match self {
            Self::IsActive => DataType::Boolean,
            Self::TableSchema | Self::TableName | Self::InactiveReason => DataType::Utf8,
        }
    }

    fn nullable(self) -> bool {
        matches!(self, Self::InactiveReason)
    }
}

/// The materialized-views virtual table consumes only ready Accelerator
/// projections, never a retained row from a quarantined lake package.
pub fn try_query_materialized_views(
    readiness: &MvReadinessPort,
    query: &ast::Query,
) -> Result<Option<StatementResult>, String> {
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Ok(None);
    }
    if !is_information_schema_materialized_views(&select.from[0].relation) {
        return Ok(None);
    }

    let projection = projection_columns(select)?;
    let mut rows = materialized_view_rows(readiness)?;
    if let Some(selection) = select.selection.as_ref() {
        let mut filtered = Vec::with_capacity(rows.len());
        for row in rows {
            if selection_matches(&row, selection)? {
                filtered.push(row);
            }
        }
        rows = filtered;
    }
    apply_order_by(query, &mut rows)?;
    build_query_result(&projection, &rows)
        .map(StatementResult::Query)
        .map(Some)
}

fn materialized_view_rows(
    readiness: &MvReadinessPort,
) -> Result<Vec<MaterializedViewInfoRow>, String> {
    let projections = readiness
        .list_ready_projections()
        .map_err(|e| format!("load materialized view metadata failed: {e}"))?;
    let mut rows = Vec::new();
    for projection in &projections {
        let mv = &projection.definition;
        if mv.storage_engine.eq_ignore_ascii_case("iceberg") {
            let (Some(table_schema), Some(target_table)) =
                (mv.target_namespace.clone(), mv.target_table.clone())
            else {
                continue;
            };
            rows.push(MaterializedViewInfoRow {
                table_schema,
                table_name: target_table,
                is_active: true,
                inactive_reason: None,
            });
            continue;
        }
    }
    Ok(rows)
}

fn is_information_schema_materialized_views(factor: &ast::TableFactor) -> bool {
    let ast::TableFactor::Table { name, .. } = factor else {
        return false;
    };
    let parts = object_name_parts(name);
    matches!(
        parts.as_slice(),
        [schema, table]
            if schema.eq_ignore_ascii_case("information_schema")
                && table.eq_ignore_ascii_case("materialized_views")
    )
}

fn projection_columns(select: &ast::Select) -> Result<Vec<InfoColumn>, String> {
    let mut columns = Vec::new();
    for item in &select.projection {
        match item {
            ast::SelectItem::Wildcard { .. } => {
                columns.extend([
                    InfoColumn::TableSchema,
                    InfoColumn::TableName,
                    InfoColumn::IsActive,
                    InfoColumn::InactiveReason,
                ]);
            }
            ast::SelectItem::UnnamedExpr(expr) => {
                columns.push(expr_column(expr)?);
            }
            ast::SelectItem::ExprWithAlias { expr, .. } => {
                columns.push(expr_column(expr)?);
            }
            ast::SelectItem::QualifiedWildcard { .. } => {
                return Err(
                    "information_schema.materialized_views does not support qualified wildcard"
                        .to_string(),
                );
            }
        }
    }
    if columns.is_empty() {
        return Err("information_schema.materialized_views projection is empty".to_string());
    }
    Ok(columns)
}

fn expr_column(expr: &ast::Expr) -> Result<InfoColumn, String> {
    let name = expr_column_name(expr).ok_or_else(|| {
        format!(
            "unsupported information_schema.materialized_views projection: {}",
            printer::print_expr(expr)
        )
    })?;
    InfoColumn::parse(&name)
        .ok_or_else(|| format!("unknown information_schema.materialized_views column `{name}`"))
}

fn selection_matches(row: &MaterializedViewInfoRow, expr: &ast::Expr) -> Result<bool, String> {
    match expr {
        ast::Expr::Binary(binary) => match binary.operator {
            ast::BinaryOperator::And => {
                Ok(selection_matches(row, &binary.left)? && selection_matches(row, &binary.right)?)
            }
            ast::BinaryOperator::Or => {
                Ok(selection_matches(row, &binary.left)? || selection_matches(row, &binary.right)?)
            }
            ast::BinaryOperator::Equal => {
                let (column, value) = comparison_column_value(&binary.left, &binary.right)
                    .or_else(|| comparison_column_value(&binary.right, &binary.left))
                    .ok_or_else(|| {
                        format!(
                            "unsupported information_schema.materialized_views predicate: {}",
                            printer::print_expr(expr)
                        )
                    })?;
                Ok(row_string_value(row, column)
                    .map(|actual| actual.eq_ignore_ascii_case(&value))
                    .unwrap_or(false))
            }
            _ => Err(format!(
                "unsupported information_schema.materialized_views predicate operator: {:?}",
                binary.operator
            )),
        },
        ast::Expr::Nested(nested) => selection_matches(row, &nested.expression),
        _ => Err(format!(
            "unsupported information_schema.materialized_views predicate: {}",
            printer::print_expr(expr)
        )),
    }
}

fn comparison_column_value<'a>(
    column_expr: &'a ast::Expr,
    value_expr: &'a ast::Expr,
) -> Option<(InfoColumn, String)> {
    let column = expr_column_name(column_expr).and_then(|name| InfoColumn::parse(&name))?;
    let value = string_literal(value_expr)?;
    Some((column, value))
}

fn apply_order_by(query: &ast::Query, rows: &mut [MaterializedViewInfoRow]) -> Result<(), String> {
    if query.order_by.is_empty() {
        return Ok(());
    }
    let columns = query
        .order_by
        .iter()
        .map(|order| expr_column(&order.expr))
        .collect::<Result<Vec<_>, _>>()?;
    rows.sort_by(|left, right| {
        for column in &columns {
            let ord = row_sort_value(left, *column).cmp(&row_sort_value(right, *column));
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
    Ok(())
}

fn build_query_result(
    columns: &[InfoColumn],
    rows: &[MaterializedViewInfoRow],
) -> Result<QueryResult, String> {
    let query_columns = columns
        .iter()
        .map(|column| {
            QueryResultColumn::new(
                column.sql_name(),
                column.data_type(),
                column.nullable(),
                None,
            )
        })
        .collect::<Vec<_>>();
    let arrays = columns
        .iter()
        .map(|column| build_column_array(*column, rows))
        .collect::<Vec<_>>();
    build_arrow_query_result(query_columns, arrays)
        .map_err(|e| format!("build information_schema.materialized_views result failed: {e}"))
}

fn build_column_array(column: InfoColumn, rows: &[MaterializedViewInfoRow]) -> ArrayRef {
    match column {
        InfoColumn::TableSchema => Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.table_schema.clone()))
                .collect::<Vec<_>>(),
        )),
        InfoColumn::TableName => Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.table_name.clone()))
                .collect::<Vec<_>>(),
        )),
        InfoColumn::IsActive => Arc::new(BooleanArray::from(
            rows.iter()
                .map(|row| Some(row.is_active))
                .collect::<Vec<_>>(),
        )),
        InfoColumn::InactiveReason => Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.inactive_reason.clone())
                .collect::<Vec<_>>(),
        )),
    }
}

fn row_string_value(row: &MaterializedViewInfoRow, column: InfoColumn) -> Option<String> {
    match column {
        InfoColumn::TableSchema => Some(row.table_schema.clone()),
        InfoColumn::TableName => Some(row.table_name.clone()),
        InfoColumn::IsActive => Some(row.is_active.to_string()),
        InfoColumn::InactiveReason => row.inactive_reason.clone(),
    }
}

fn row_sort_value(row: &MaterializedViewInfoRow, column: InfoColumn) -> String {
    row_string_value(row, column).unwrap_or_default()
}

fn expr_column_name(expr: &ast::Expr) -> Option<String> {
    match expr {
        ast::Expr::Identifier(ident) => Some(ident.value.clone()),
        ast::Expr::CompoundIdentifier(parts) => parts.parts.last().map(|ident| ident.value.clone()),
        _ => None,
    }
}

fn string_literal(expr: &ast::Expr) -> Option<String> {
    match expr {
        ast::Expr::Literal(literal) => match &literal.kind {
            ast::LiteralKind::String(value) => Some(value.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn object_name_parts(name: &ast::ObjectName) -> Vec<String> {
    name.parts.iter().map(|part| part.value.clone()).collect()
}

fn normalize_column_name(name: &str) -> String {
    name.trim_matches('`').to_ascii_lowercase()
}
