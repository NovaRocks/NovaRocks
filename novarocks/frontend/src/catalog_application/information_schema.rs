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
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use sqlparser::ast as sqlast;

use crate::mv::domain::repository::MvRepository;
use novarocks::runtime::query_result::{QueryResult, QueryResultColumn, record_batch_to_chunk};
use novarocks::runtime::statement_result::StatementResult;

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

/// The materialized-views virtual table needs only the durable MV repository.
///
/// Taking that leaf port directly keeps this read-only projection independent
/// of whichever capability value composed the query.
pub fn try_query_materialized_views(
    mv_repository: &dyn MvRepository,
    query: &sqlast::Query,
) -> Result<Option<StatementResult>, String> {
    let sqlast::SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Ok(None);
    }
    if !is_information_schema_materialized_views(&select.from[0].relation) {
        return Ok(None);
    }

    let projection = projection_columns(select)?;
    let mut rows = materialized_view_rows(mv_repository)?;
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
    mv_repository: &dyn MvRepository,
) -> Result<Vec<MaterializedViewInfoRow>, String> {
    let definitions = mv_repository
        .list_definitions()
        .map_err(|e| format!("load materialized view metadata failed: {e}"))?;
    let mut rows = Vec::new();
    for mv in &definitions {
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

fn is_information_schema_materialized_views(factor: &sqlast::TableFactor) -> bool {
    let sqlast::TableFactor::Table { name, .. } = factor else {
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

fn projection_columns(select: &sqlast::Select) -> Result<Vec<InfoColumn>, String> {
    let mut columns = Vec::new();
    for item in &select.projection {
        match item {
            sqlast::SelectItem::Wildcard(_) => {
                columns.extend([
                    InfoColumn::TableSchema,
                    InfoColumn::TableName,
                    InfoColumn::IsActive,
                    InfoColumn::InactiveReason,
                ]);
            }
            sqlast::SelectItem::UnnamedExpr(expr) => {
                columns.push(expr_column(expr)?);
            }
            sqlast::SelectItem::ExprWithAlias { expr, .. } => {
                columns.push(expr_column(expr)?);
            }
            sqlast::SelectItem::QualifiedWildcard(_, _) => {
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

fn expr_column(expr: &sqlast::Expr) -> Result<InfoColumn, String> {
    let name = expr_column_name(expr).ok_or_else(|| {
        format!("unsupported information_schema.materialized_views projection: {expr}")
    })?;
    InfoColumn::parse(&name)
        .ok_or_else(|| format!("unknown information_schema.materialized_views column `{name}`"))
}

fn selection_matches(row: &MaterializedViewInfoRow, expr: &sqlast::Expr) -> Result<bool, String> {
    match expr {
        sqlast::Expr::BinaryOp { left, op, right } => match op {
            sqlast::BinaryOperator::And => {
                Ok(selection_matches(row, left)? && selection_matches(row, right)?)
            }
            sqlast::BinaryOperator::Or => {
                Ok(selection_matches(row, left)? || selection_matches(row, right)?)
            }
            sqlast::BinaryOperator::Eq => {
                let (column, value) = comparison_column_value(left, right)
                    .or_else(|| comparison_column_value(right, left))
                    .ok_or_else(|| {
                        format!(
                            "unsupported information_schema.materialized_views predicate: {expr}"
                        )
                    })?;
                Ok(row_string_value(row, column)
                    .map(|actual| actual.eq_ignore_ascii_case(&value))
                    .unwrap_or(false))
            }
            _ => Err(format!(
                "unsupported information_schema.materialized_views predicate operator: {op}"
            )),
        },
        sqlast::Expr::Nested(inner) => selection_matches(row, inner),
        _ => Err(format!(
            "unsupported information_schema.materialized_views predicate: {expr}"
        )),
    }
}

fn comparison_column_value<'a>(
    column_expr: &'a sqlast::Expr,
    value_expr: &'a sqlast::Expr,
) -> Option<(InfoColumn, String)> {
    let column = expr_column_name(column_expr).and_then(|name| InfoColumn::parse(&name))?;
    let value = string_literal(value_expr)?;
    Some((column, value))
}

fn apply_order_by(
    query: &sqlast::Query,
    rows: &mut [MaterializedViewInfoRow],
) -> Result<(), String> {
    let Some(sqlast::OrderBy {
        kind: sqlast::OrderByKind::Expressions(exprs),
        ..
    }) = &query.order_by
    else {
        return Ok(());
    };
    let columns = exprs
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
        .map(|column| QueryResultColumn {
            name: column.sql_name().to_string(),
            data_type: column.data_type(),
            nullable: column.nullable(),
            logical_type: None,
        })
        .collect::<Vec<_>>();
    let fields = columns
        .iter()
        .map(|column| Field::new(column.sql_name(), column.data_type(), column.nullable()))
        .collect::<Vec<_>>();
    let arrays = columns
        .iter()
        .map(|column| build_column_array(*column, rows))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| format!("build information_schema.materialized_views result failed: {e}"))?;
    Ok(QueryResult {
        columns: query_columns,
        chunks: vec![record_batch_to_chunk(batch)?],
    })
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

fn expr_column_name(expr: &sqlast::Expr) -> Option<String> {
    match expr {
        sqlast::Expr::Identifier(ident) => Some(ident.value.clone()),
        sqlast::Expr::CompoundIdentifier(parts) => parts.last().map(|ident| ident.value.clone()),
        _ => None,
    }
}

fn string_literal(expr: &sqlast::Expr) -> Option<String> {
    match expr {
        sqlast::Expr::Value(sqlast::ValueWithSpan {
            value:
                sqlast::Value::SingleQuotedString(value) | sqlast::Value::DoubleQuotedString(value),
            ..
        }) => Some(value.clone()),
        _ => None,
    }
}

fn object_name_parts(name: &sqlast::ObjectName) -> Vec<String> {
    name.0
        .iter()
        .filter_map(|part| match part {
            sqlast::ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
            _ => None,
        })
        .collect()
}

fn normalize_column_name(name: &str) -> String {
    name.trim_matches('`').to_ascii_lowercase()
}
