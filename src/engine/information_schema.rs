use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use sqlparser::ast as sqlast;

use crate::connector::starrocks::managed::store::{
    ManagedMvStorageEngine, ManagedTableKind, ManagedTableState,
};
use crate::engine::{QueryResult, QueryResultColumn, StandaloneState, StatementResult};

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

pub(crate) fn try_query_materialized_views(
    state: &Arc<StandaloneState>,
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
    let mut rows = materialized_view_rows(state)?;
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

pub(crate) fn try_update_be_configs(
    statement: &sqlast::Statement,
) -> Result<Option<StatementResult>, String> {
    let sqlast::Statement::Update(update) = statement else {
        return Ok(None);
    };
    if !is_information_schema_be_configs(&update.table.relation) {
        return Ok(None);
    }

    if !update.table.joins.is_empty() {
        return Err("information_schema.be_configs UPDATE does not support joins".to_string());
    }
    if update.optimizer_hint.is_some()
        || update.from.is_some()
        || update.returning.is_some()
        || update.or.is_some()
        || update.limit.is_some()
    {
        return Err(
            "information_schema.be_configs UPDATE only supports simple SET assignments".to_string(),
        );
    }
    if update.assignments.len() != 1 {
        return Err(
            "information_schema.be_configs UPDATE requires exactly one assignment".to_string(),
        );
    }

    let sqlast::AssignmentTarget::ColumnName(column_name) = &update.assignments[0].target else {
        return Err(
            "information_schema.be_configs UPDATE target must be a column name".to_string(),
        );
    };
    let column_parts = object_name_parts(column_name);
    if !matches!(column_parts.as_slice(), [column] if column.eq_ignore_ascii_case("value")) {
        return Err(
            "information_schema.be_configs UPDATE only supports assigning `value`".to_string(),
        );
    }

    Ok(Some(StatementResult::Ok))
}

fn materialized_view_rows(
    state: &Arc<StandaloneState>,
) -> Result<Vec<MaterializedViewInfoRow>, String> {
    let Some(metadata_store) = state.metadata_store.as_ref() else {
        return Ok(Vec::new());
    };
    let snapshot = metadata_store.load_snapshot()?.managed;
    let mut rows = Vec::new();
    for mv in &snapshot.materialized_views {
        if mv.storage_engine == ManagedMvStorageEngine::Iceberg {
            let (Some(table_schema), Some(table_name)) =
                (mv.target_namespace.clone(), mv.target_table.clone())
            else {
                continue;
            };
            rows.push(MaterializedViewInfoRow {
                table_schema,
                table_name,
                is_active: true,
                inactive_reason: None,
            });
            continue;
        }
        let Some(table) = snapshot.tables.iter().find(|table| {
            table.table_id == mv.mv_id && table.kind == ManagedTableKind::MaterializedView
        }) else {
            continue;
        };
        let Some(database) = snapshot
            .databases
            .iter()
            .find(|database| database.db_id == table.db_id)
        else {
            continue;
        };
        let is_active = table.state == ManagedTableState::Active;
        rows.push(MaterializedViewInfoRow {
            table_schema: database.name.clone(),
            table_name: table.name.clone(),
            is_active,
            inactive_reason: if is_active {
                None
            } else {
                Some(format!("{:?}", table.state))
            },
        });
    }
    Ok(rows)
}

fn is_information_schema_be_configs(factor: &sqlast::TableFactor) -> bool {
    let sqlast::TableFactor::Table { name, .. } = factor else {
        return false;
    };
    let parts = object_name_parts(name);
    matches!(
        parts.as_slice(),
        [schema, table]
            if schema.eq_ignore_ascii_case("information_schema")
                && table.eq_ignore_ascii_case("be_configs")
    )
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
        chunks: vec![crate::engine::record_batch_to_chunk(batch)?],
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

#[cfg(test)]
mod tests {
    use super::try_update_be_configs;
    use crate::engine::StatementResult;
    use crate::sql::parser::dialect::StarRocksDialect;
    use sqlparser::parser::Parser;

    fn parse_statement(sql: &str) -> sqlparser::ast::Statement {
        let dialect = StarRocksDialect;
        Parser::new(&dialect)
            .try_with_sql(sql)
            .expect("parse sql")
            .parse_statement()
            .expect("parse statement")
    }

    #[test]
    fn update_information_schema_be_configs_is_noop() {
        let stmt = parse_statement(
            r#"update information_schema.be_configs set value = "0" where name = "two_level_memory_threshold""#,
        );

        assert!(matches!(
            try_update_be_configs(&stmt).expect("be_configs update"),
            Some(StatementResult::Ok)
        ));
    }

    #[test]
    fn update_information_schema_be_configs_rejects_other_columns() {
        let stmt = parse_statement(
            r#"update information_schema.be_configs set name = "x" where name = "two_level_memory_threshold""#,
        );
        let err = try_update_be_configs(&stmt)
            .expect_err("unsupported be_configs assignment should fail");

        assert!(err.contains("only supports assigning `value`"), "err={err}");
    }
}
