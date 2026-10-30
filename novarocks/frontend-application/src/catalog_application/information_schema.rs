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

use arrow::array::ArrayRef;
#[cfg(test)]
use arrow::array::BooleanArray;
use arrow::datatypes::DataType;
use novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection;
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

/// The caller retains the original owner and result delivery capability.
/// Missing/expired original execution never selects the synchronous adapter.
pub(crate) async fn try_query_materialized_views_in(
    readiness: &MvReadinessPort,
    query: &ast::Query,
    execution: std::sync::Arc<dyn novarocks_state_store_api::StateStoreExecutionRequests>,
) -> Result<Option<StatementResult>, novarocks_query_application::session_error::QueryServiceError>
{
    let Some(projection) = materialized_view_projection(query).map_err(local_query_error)? else {
        return Ok(None);
    };
    let view = readiness.in_requests(execution);
    let mut inventory = view
        .local_projection_inventory()
        .await
        .map_err(repository_query_error)?;
    let mut rows = MaterializedViewRows::default();
    while let Some(loaded) = inventory
        .next_ready()
        .await
        .map_err(repository_query_error)?
    {
        rows.push(&loaded.projection).map_err(local_query_error)?;
    }
    render_materialized_view_rows(query, &projection, rows.rows)
        .map(Some)
        .map_err(local_query_error)
}

fn repository_query_error(
    error: novarocks_mv_application::repository::MvRepositoryError,
) -> novarocks_query_application::session_error::QueryServiceError {
    novarocks_query_application::session_error::QueryServiceError::from_failure_cause(
        novarocks_failure_contract::SharedFailureCause::from_static_detail(
            error,
            "load materialized view metadata failed",
        ),
    )
}
fn local_query_error(
    message: impl Into<String>,
) -> novarocks_query_application::session_error::QueryServiceError {
    novarocks_query_application::session_error::QueryServiceError::new(
        novarocks_query_application::session_error::QueryServiceErrorKind::Internal,
        message,
    )
}
fn materialized_view_projection(query: &ast::Query) -> Result<Option<Vec<InfoColumn>>, String> {
    if !is_local_materialized_views_query(query) {
        return Ok(None);
    }
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        unreachable!("local materialized-view admission requires SELECT");
    };
    projection_columns(select).map(Some)
}
fn render_materialized_view_rows(
    query: &ast::Query,
    projection: &[InfoColumn],
    mut rows: Vec<MaterializedViewInfoRow>,
) -> Result<StatementResult, String> {
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        unreachable!("local materialized-view admission requires SELECT");
    };
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
    build_query_result(projection, &rows).map(StatementResult::Query)
}
#[derive(Default)]
struct MaterializedViewRows {
    rows: Vec<MaterializedViewInfoRow>,
    bytes: usize,
    workspace_payload: usize,
}
impl MaterializedViewRows {
    // Shared row/byte/workspace author for legacy and original inventories.
    fn push(&mut self, projection: &StoredKeyedMvProjection) -> Result<(), String> {
        let bound = novarocks_query_application::api::LocalResultBound::V1;
        let target = projection.facts.target();
        let row_bytes = [target.namespace().len(), target.name().len(), 4, 0]
            .into_iter()
            .fold(0usize, |total, n| {
                total.saturating_add(
                    novarocks_query_application::api::LocalResultBound::cell_bytes(n),
                )
            });
        self.bytes = self.bytes.saturating_add(row_bytes);
        bound
            .admit(self.rows.len().saturating_add(1), self.bytes)
            .map_err(|e| format!("information_schema.materialized_views: {e}"))?;
        let capacity = if self.rows.len() == self.rows.capacity() {
            self.rows
                .capacity()
                .saturating_mul(2)
                .max(1)
                .min(bound.rows)
        } else {
            self.rows.capacity()
        };
        self.workspace_payload = self
            .workspace_payload
            .saturating_add(target.namespace().len())
            .saturating_add(target.name().len())
            .saturating_add(2 * 64);
        // Two vector backings cover growth, filtering and stable-sort scratch;
        // variable text is copied only after the complete workspace check.
        if capacity
            .saturating_mul(2)
            .saturating_mul(std::mem::size_of::<MaterializedViewInfoRow>())
            .saturating_add(self.workspace_payload)
            > bound.bytes
        {
            return Err(
                "information_schema.materialized_views exceeds its row workspace bound".into(),
            );
        }
        if capacity != self.rows.capacity() {
            self.rows
                .try_reserve_exact(capacity - self.rows.len())
                .map_err(|_| "information_schema.materialized_views workspace allocation failed")?;
        }
        self.rows.push(materialized_view_row(projection));
        Ok(())
    }
}

fn materialized_view_row(projection: &StoredKeyedMvProjection) -> MaterializedViewInfoRow {
    let target = projection.facts.target();
    MaterializedViewInfoRow {
        table_schema: target.namespace().to_string(),
        table_name: target.name().to_string(),
        is_active: true,
        inactive_reason: None,
    }
}

/// Borrow the exact shape used by the immediate producer before any result
/// window or catalog snapshot is acquired. Other system queries stay distributed.
pub(crate) fn is_local_materialized_views_query(query: &ast::Query) -> bool {
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    select.from.len() == 1
        && select.from[0].joins.is_empty()
        && is_information_schema_materialized_views(&select.from[0].relation)
}

fn is_information_schema_materialized_views(factor: &ast::TableFactor) -> bool {
    let ast::TableFactor::Table { name, .. } = factor else {
        return false;
    };
    matches!(
        name.parts.as_slice(),
        [schema, table]
            if schema.value.eq_ignore_ascii_case("information_schema")
                && table.value.eq_ignore_ascii_case("materialized_views")
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
            let ord = row_sort_value(left, *column).cmp(row_sort_value(right, *column));
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
    let bound = novarocks_query_application::api::LocalResultBound::V1;
    if columns.len() > bound.columns {
        return Err("information_schema.materialized_views exceeds its column bound".into());
    }
    let bytes = columns.iter().fold(0usize, |sum, column| {
        rows.iter().fold(sum, |sum, row| {
            sum.saturating_add(
                novarocks_query_application::api::LocalResultBound::cell_bytes(
                    row_string_value(row, *column).map_or(0, str::len),
                ),
            )
        })
    });
    bound.admit(rows.len(), bytes)?;
    let query_columns = columns
        .iter()
        .map(|column| {
            QueryResultColumn::new(
                column.sql_name(),
                novarocks_type_contract::FunctionValueType::new(
                    column.data_type(),
                    column.nullable(),
                ),
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
        InfoColumn::IsActive => {
            let mut builder = arrow::array::BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                builder.append_value(row.is_active);
            }
            Arc::new(builder.finish())
        }
        _ => {
            let bytes = rows.iter().fold(0usize, |sum, row| {
                sum.saturating_add(row_string_value(row, column).map_or(0, str::len))
            });
            let mut builder = arrow::array::StringBuilder::with_capacity(rows.len(), bytes);
            for row in rows {
                builder.append_option(row_string_value(row, column));
            }
            Arc::new(builder.finish())
        }
    }
}

fn row_string_value(row: &MaterializedViewInfoRow, column: InfoColumn) -> Option<&str> {
    match column {
        InfoColumn::TableSchema => Some(&row.table_schema),
        InfoColumn::TableName => Some(&row.table_name),
        InfoColumn::IsActive => Some(if row.is_active { "true" } else { "false" }),
        InfoColumn::InactiveReason => row.inactive_reason.as_deref(),
    }
}

fn row_sort_value(row: &MaterializedViewInfoRow, column: InfoColumn) -> &str {
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

fn normalize_column_name(name: &str) -> String {
    name.trim_matches('`').to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::test_admission::KeyedProjectionFixture as ProjectionFixture;
    use novarocks_mv_application::product::MvTarget;

    fn projection(snapshot_id: Option<i64>, paused: bool) -> StoredKeyedMvProjection {
        let mut fixture = ProjectionFixture::new(
            MvTarget::from_parts(Some("lake_alias"), "analytics", "orders_mv"),
            snapshot_id,
        );
        fixture.configuration.paused = paused;
        StoredKeyedMvProjection {
            mv_id: 42,
            facts: fixture.build().expect("valid document projection"),
        }
    }

    #[test]
    fn materialized_view_row_uses_projection_target_not_source_resolution() {
        let projection = projection(Some(201), false);
        let row = materialized_view_row(&projection);

        assert_eq!(
            projection
                .facts
                .definition()
                .query
                .resolution
                .default_namespace,
            "sales",
        );
        assert_eq!(row.table_schema, "analytics");
        assert_eq!(row.table_name, "orders_mv");
        assert!(row.is_active);
        assert_eq!(row.inactive_reason, None);
    }

    #[test]
    fn ready_unpublished_or_paused_projections_remain_active_inventory() {
        for snapshot_id in [None, Some(201)] {
            for paused in [false, true] {
                let row = materialized_view_row(&projection(snapshot_id, paused));
                assert!(row.is_active);
                assert_eq!(row.inactive_reason, None);
                let active = build_column_array(InfoColumn::IsActive, &[row]);
                assert!(
                    active
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .expect("active Boolean column")
                        .value(0),
                );
            }
        }
    }
}

#[cfg(test)]
mod bounded_source_tests {
    use super::*;

    #[test]
    fn row_comparison_lends_existing_names_without_a_per_comparison_clone() {
        let row = MaterializedViewInfoRow {
            table_schema: "sales".into(),
            table_name: "mv".into(),
            is_active: true,
            inactive_reason: None,
        };
        assert_eq!(
            row_string_value(&row, InfoColumn::TableName)
                .unwrap()
                .as_ptr(),
            row.table_name.as_ptr()
        );
        assert_eq!(
            row_sort_value(&row, InfoColumn::TableSchema).as_ptr(),
            row.table_schema.as_ptr()
        );
        assert_eq!(row_sort_value(&row, InfoColumn::IsActive), "true");
        assert_eq!(row_sort_value(&row, InfoColumn::InactiveReason), "");
    }

    #[test]
    fn repeated_virtual_columns_are_checked_as_one_whole_result() {
        let row = MaterializedViewInfoRow {
            table_schema: "sales".into(),
            table_name: "x".repeat(4 * 1024 * 1024),
            is_active: true,
            inactive_reason: None,
        };
        assert!(
            build_query_result(&[InfoColumn::TableName; 8], std::slice::from_ref(&row)).is_err()
        );
        let error = build_query_result(
            &vec![InfoColumn::IsActive; 4097],
            std::slice::from_ref(&row),
        )
        .unwrap_err();
        assert!(error.contains("column bound"));
    }
}

#[cfg(test)]
mod original_read_tests {
    use super::*;
    use novarocks_failure_contract::FailureProjection;
    use novarocks_state_store_api::{StateStoreExecutionContext, StateStoreExecutionPort};
    use std::time::{Duration, Instant};
    struct NoPhysicalPort;
    impl StateStoreExecutionPort for NoPhysicalPort {
        fn bind_original_in(
            &self,
            _: &Arc<dyn novarocks_state_store_api::StateStoreBlockingOriginal>,
            _: Instant,
            _: &mut Option<Box<dyn novarocks_state_store_api::StateStoreBlockingSlot>>,
        ) -> Result<(), novarocks_state_store_api::StateStoreExecutionFailure> {
            panic!("pure information_schema fixture must not dispatch physical work")
        }
        fn request_progress(&self) {}
    }
    fn execution() -> (Arc<dyn StateStoreExecutionPort>, StateStoreExecutionContext) {
        let owner: Arc<dyn StateStoreExecutionPort> = Arc::new(NoPhysicalPort);
        let context =
            StateStoreExecutionContext::new(&owner, Instant::now() + Duration::from_secs(5));
        (owner, context)
    }
    fn query(sql: &str) -> ast::Query {
        let mut statements = novarocks_parser::parse(sql).unwrap();
        match statements.remove(0) {
            ast::Statement::Query(query) => query,
            _ => panic!("expected a parsed query"),
        }
    }
    async fn readiness(store: Arc<dyn novarocks_state_store_api::StateStore>) -> MvReadinessPort {
        let repository =
            novarocks_mv_application::state_store_repository::StateStoreMvRepository::open(
                store,
                Default::default(),
            )
            .await
            .unwrap();
        MvReadinessPort::from_product(
            novarocks_mv_application::readiness::MvKeyedReadinessService::new(
                repository,
                Arc::default(),
            ),
            tokio::runtime::Handle::current(),
            crate::mv::domain::test_admission::management_entrance(),
        )
    }
    #[tokio::test(flavor = "current_thread")]
    async fn original_information_schema_keeps_repository_refusal_and_does_not_select_legacy() {
        let (_owner, execution) = execution();
        let port = readiness(Arc::new(
            novarocks_state_store_testkit::testing::InMemoryStateStore::new(
                "information-schema-no-original",
            ),
        ))
        .await;
        let query = query("SELECT TABLE_NAME FROM information_schema.materialized_views");
        let error =
            match try_query_materialized_views_in(&port, &query, Arc::new(execution.clone())).await
            {
                Err(error) => error,
                Ok(_) => panic!("unsupported Original must not become a legacy empty result"),
            };
        let repository = error
            .failure_cause()
            .unwrap()
            .downcast_ref::<novarocks_mv_application::repository::MvRepositoryError>()
            .unwrap();
        assert!(repository.original_failure().is_some());
        assert_eq!(
            repository.failure_fact(),
            novarocks_state_store_api::StateStoreOriginalUnsupported.failure_fact()
        );
        assert_eq!(
            error.execution_failure().unwrap().fact(),
            repository.failure_fact()
        );
    }
    #[tokio::test(flavor = "current_thread")]
    async fn original_information_schema_awaits_pure_inventory_and_keeps_recognition_and_column_errors()
     {
        let mut host = crate::state_store::testing::open_persistent(format!(
            "information-schema-original-{}",
            uuid::Uuid::now_v7()
        ))
        .await;
        let (_owner, execution) = execution();
        let port = readiness(host.state_store().unwrap()).await;
        let selected = query(
            "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'sales' ORDER BY TABLE_NAME",
        );
        match try_query_materialized_views_in(&port, &selected, Arc::new(execution.clone()))
            .await
            .unwrap()
            .unwrap()
        {
            StatementResult::Query(result) => assert_eq!(result.row_count(), 0),
            _ => panic!("expected the existing local Arrow result"),
        }
        let other = query("SELECT 1");
        let expired = execution.with_deadline(Instant::now() - Duration::from_secs(1));
        assert!(
            try_query_materialized_views_in(&port, &other, Arc::new(expired.clone()))
                .await
                .unwrap()
                .is_none()
        );
        let invalid = query("SELECT unknown_mv_column FROM information_schema.materialized_views");
        let local_column_error = materialized_view_projection(&invalid).unwrap_err();
        let new = try_query_materialized_views_in(&port, &invalid, Arc::new(expired.clone()))
            .await
            .unwrap_err();
        assert_eq!(new.to_string(), local_column_error);
        assert!(
            new.failure_cause().is_none(),
            "a local column error must not invent a provider fact"
        );
        drop(port);
        host.shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }
}
