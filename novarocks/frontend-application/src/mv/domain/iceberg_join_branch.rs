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

use novarocks_parser::{Span, ast};
use novarocks_sql::planning::mv::MV_JOIN_APPLY_KEY_COLUMN_NAME;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotWindow {
    pub from: i64,
    pub to: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BranchSide {
    Delta(SnapshotWindow),
    Snapshot(i64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) enum BranchDeltaSide {
    Left,
    Right,
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) const JOIN_LEFT_ROW_ID_COLUMN: &str = "__nova_left_row_id";
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) const JOIN_RIGHT_ROW_ID_COLUMN: &str = "__nova_right_row_id";
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) const JOIN_DELTA_TARGET_LOCATOR_TABLE: &str = "__nr_join_delta_target_locator";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinDeltaBranchPlan {
    pub(crate) left_base: novarocks_types::naming::TableIdentity,
    pub(crate) right_base: novarocks_types::naming::TableIdentity,
    pub(crate) left: BranchSide,
    pub(crate) right: BranchSide,
}

impl JoinDeltaBranchPlan {
    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
    pub(crate) fn delta_side(&self) -> Result<BranchDeltaSide, String> {
        match (self.left, self.right) {
            (BranchSide::Delta(_), BranchSide::Snapshot(_)) => Ok(BranchDeltaSide::Left),
            (BranchSide::Snapshot(_), BranchSide::Delta(_)) => Ok(BranchDeltaSide::Right),
            _ => Err("join branch plan must contain exactly one delta side".to_string()),
        }
    }
}

pub fn plan_join_delta_branches(
    left_base: &novarocks_types::naming::TableIdentity,
    right_base: &novarocks_types::naming::TableIdentity,
    left_window: SnapshotWindow,
    right_window: SnapshotWindow,
    left_has_changes: bool,
    right_has_changes: bool,
) -> Vec<JoinDeltaBranchPlan> {
    let mut plans = Vec::new();
    if left_has_changes {
        plans.push(JoinDeltaBranchPlan {
            left_base: left_base.clone(),
            right_base: right_base.clone(),
            left: BranchSide::Delta(left_window),
            right: BranchSide::Snapshot(right_window.from),
        });
    }
    if right_has_changes {
        plans.push(JoinDeltaBranchPlan {
            left_base: left_base.clone(),
            right_base: right_base.clone(),
            left: BranchSide::Snapshot(left_window.to),
            right: BranchSide::Delta(right_window),
        });
    }
    plans
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) fn rewrite_join_branch_query(
    query: &ast::Query,
    plan: &JoinDeltaBranchPlan,
    left_alias: &str,
    right_alias: &str,
) -> Result<ast::Query, String> {
    let mut query = query.clone();
    let ast::SetExpr::Select(select) = query.body.as_mut() else {
        return Err("join branch rewrite requires SELECT body".to_string());
    };
    let [from] = select.from.as_mut_slice() else {
        return Err("join branch rewrite requires one FROM item".to_string());
    };
    let [join] = from.joins.as_mut_slice() else {
        return Err("join branch rewrite requires one JOIN".to_string());
    };
    let left_branch =
        rewrite_branch_factor(&mut from.relation, &plan.left_base, plan.left, left_alias)?;
    let right_branch = rewrite_branch_factor(
        &mut join.relation,
        &plan.right_base,
        plan.right,
        right_alias,
    )?;
    append_join_hidden_projection(select, &left_branch, &right_branch)?;
    Ok(query)
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) fn rewrite_join_delta_coalesce_query(
    query: &ast::Query,
    branches: &[JoinDeltaBranchPlan],
    left_alias: &str,
    right_alias: &str,
    left_uuid: &str,
    right_uuid: &str,
) -> Result<ast::Query, String> {
    if branches.is_empty() {
        return Err("join delta coalesce rewrite requires at least one branch".to_string());
    }
    let mut branch_queries = Vec::with_capacity(branches.len());
    for branch in branches {
        branch_queries.push(rewrite_join_branch_query(
            query,
            branch,
            left_alias,
            right_alias,
        )?);
    }
    rewrite_join_delta_coalesce_query_with_branch_queries(
        query,
        branch_queries,
        left_uuid,
        right_uuid,
    )
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) fn rewrite_join_delta_coalesce_query_with_branch_queries(
    query: &ast::Query,
    branch_queries: Vec<ast::Query>,
    left_uuid: &str,
    right_uuid: &str,
) -> Result<ast::Query, String> {
    rewrite_join_delta_coalesce_query_with_branch_queries_and_locator(
        query,
        branch_queries,
        left_uuid,
        right_uuid,
        JOIN_DELTA_TARGET_LOCATOR_TABLE,
    )
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) fn rewrite_join_delta_coalesce_query_with_branch_queries_and_locator(
    query: &ast::Query,
    branch_queries: Vec<ast::Query>,
    left_uuid: &str,
    right_uuid: &str,
    target_locator_relation: &str,
) -> Result<ast::Query, String> {
    if branch_queries.is_empty() {
        return Err("join delta coalesce rewrite requires at least one branch".to_string());
    }
    let payload_columns = payload_projection_columns(query)?;
    let branch_ctes = (0..branch_queries.len())
        .map(|index| {
            format!(
                "{} AS (SELECT 1 AS __nr_join_delta_branch_placeholder)",
                join_delta_branch_cte_name(index),
            )
        })
        .collect::<Vec<_>>();
    let change_stream = (0..branch_queries.len())
        .map(|index| {
            format!(
                "SELECT {} FROM {}",
                change_stream_select_list(&payload_columns, left_uuid, right_uuid),
                join_delta_branch_cte_name(index)
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let payload_coalesced_select = payload_coalesced_select_list(&payload_columns);
    let payload_group_by = payload_group_by_list(&payload_columns);
    let key_shape_select = key_shape_select_list();
    let valid_payload_select = valid_payload_select_list(&payload_columns);
    let final_select = final_coalesced_select_list(&payload_columns);
    let change_stream_cte = format!("__nr_join_delta_change_stream AS ({change_stream})");
    let payload_coalesced_cte = format!(
        "__nr_join_delta_payload_coalesced AS (\
         SELECT {payload_coalesced_select} \
         FROM __nr_join_delta_change_stream \
         GROUP BY {payload_group_by} \
         HAVING SUM({}) <> 0 \
         AND assert_true(abs(SUM({})) <= 1, 'join delta per-payload net change exceeds 1'))",
        novarocks_execution::exec::change_op::CHANGE_OP_COLUMN,
        novarocks_execution::exec::change_op::CHANGE_OP_COLUMN,
    );
    let key_shape_cte = format!(
        "__nr_join_delta_key_shape AS (\
         SELECT {key_shape_select} \
         FROM __nr_join_delta_payload_coalesced \
         GROUP BY {} \
         HAVING assert_true(\
         SUM(CASE WHEN net > 0 THEN 1 ELSE 0 END) <= 1 \
         AND SUM(CASE WHEN net < 0 THEN 1 ELSE 0 END) <= 1, \
         'join delta multiple pending payloads for key'))",
        MV_JOIN_APPLY_KEY_COLUMN_NAME,
    );
    let coalesced_cte = format!(
        "__nr_join_delta_coalesced AS (\
         SELECT {valid_payload_select} \
         FROM __nr_join_delta_payload_coalesced pc \
         JOIN __nr_join_delta_key_shape ks \
         ON pc.{} = ks.{})",
        MV_JOIN_APPLY_KEY_COLUMN_NAME, MV_JOIN_APPLY_KEY_COLUMN_NAME,
    );
    let ctes = branch_ctes
        .into_iter()
        .chain([
            change_stream_cte,
            payload_coalesced_cte,
            key_shape_cte,
            coalesced_cte,
        ])
        .collect::<Vec<_>>()
        .join(", ");
    let key = MV_JOIN_APPLY_KEY_COLUMN_NAME;
    let sql = format!(
        "WITH {ctes} \
         SELECT {final_select} \
         FROM __nr_join_delta_coalesced coalesced \
         LEFT JOIN {target_locator_relation} tgt \
         ON coalesced.net < 0 AND coalesced.{key} = tgt.{key} \
         WHERE assert_true(\
         coalesced.net >= 0 OR (tgt._file IS NOT NULL AND tgt._pos IS NOT NULL), \
         'join delta DELETE row missing target locator')"
    );
    let mut parsed = parse_query_from_sql(&sql).map_err(|err| {
        format!("join delta coalesce rewrite generated invalid SQL: {err}; sql={sql}")
    })?;
    replace_branch_cte_queries(&mut parsed, branch_queries)?;
    Ok(parsed)
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) fn rewrite_join_delta_append_only_query(
    query: &ast::Query,
    branch_query: ast::Query,
    left_uuid: &str,
    right_uuid: &str,
) -> Result<ast::Query, String> {
    wrap_join_apply_key_query(
        query,
        branch_query,
        left_uuid,
        right_uuid,
        "__nr_join_delta_append_only_branch",
        "__nr_join_delta_append_only_placeholder",
    )
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn wrap_join_apply_key_query(
    query: &ast::Query,
    source_query: ast::Query,
    left_uuid: &str,
    right_uuid: &str,
    source_alias: &str,
    placeholder_name: &str,
) -> Result<ast::Query, String> {
    let payload_columns = payload_projection_columns(query)?;
    let mut items = payload_columns
        .iter()
        .map(|ident| {
            let rendered = render_ident(ident);
            format!("{rendered} AS {rendered}")
        })
        .collect::<Vec<_>>();
    items.push(format!(
        "join_row_key({}, {}, {}, {}) AS {}",
        sql_string_literal(left_uuid),
        JOIN_LEFT_ROW_ID_COLUMN,
        sql_string_literal(right_uuid),
        JOIN_RIGHT_ROW_ID_COLUMN,
        MV_JOIN_APPLY_KEY_COLUMN_NAME,
    ));
    items.push(format!(
        "CAST({} AS TINYINT) AS {}",
        novarocks_execution::exec::change_op::CHANGE_OP_COLUMN,
        novarocks_execution::exec::change_op::CHANGE_OP_COLUMN,
    ));
    let sql = format!(
        "SELECT {} FROM (SELECT 1 AS {placeholder_name}) AS {source_alias}",
        items.join(", "),
    );
    let mut parsed = parse_query_from_sql(&sql)
        .map_err(|err| format!("join apply-key rewrite generated invalid SQL: {err}; sql={sql}"))?;
    let ast::SetExpr::Select(select) = parsed.body.as_mut() else {
        return Err("join apply-key rewrite generated non-SELECT body".to_string());
    };
    let [from] = select.from.as_mut_slice() else {
        return Err("join apply-key rewrite generated invalid FROM".to_string());
    };
    let ast::TableFactor::Derived { subquery, .. } = &mut from.relation else {
        return Err("join apply-key rewrite generated non-derived FROM".to_string());
    };
    **subquery = source_query;
    Ok(parsed)
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) fn is_append_only_join_delta_eligible(query: &ast::Query) -> bool {
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    let [from] = select.from.as_slice() else {
        return false;
    };
    let [join] = from.joins.as_slice() else {
        return false;
    };
    matches!(
        join.operator,
        ast::JoinOperator::Inner | ast::JoinOperator::InnerExplicit | ast::JoinOperator::Cross
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
struct BranchRewrite {
    alias: ast::Ident,
    is_delta: bool,
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn payload_projection_columns(query: &ast::Query) -> Result<Vec<ast::Ident>, String> {
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err("join delta coalesce rewrite requires SELECT body".to_string());
    };
    let mut seen = std::collections::BTreeSet::new();
    let mut columns = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        let ident = payload_projection_column(item)?;
        validate_payload_projection_column(&ident, &mut seen)?;
        columns.push(ident);
    }
    Ok(columns)
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn payload_projection_column(item: &ast::SelectItem) -> Result<ast::Ident, String> {
    match item {
        ast::SelectItem::ExprWithAlias { alias, .. } => Ok(alias.clone()),
        ast::SelectItem::UnnamedExpr(expr) => projection_expr_default_name(expr).ok_or_else(|| {
            "join delta coalesce rewrite requires aliases for non-column payload expressions"
                .to_string()
        }),
        ast::SelectItem::Wildcard { .. } | ast::SelectItem::QualifiedWildcard { .. } => Err(
            "join delta coalesce rewrite requires explicit payload projection columns".to_string(),
        ),
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn projection_expr_default_name(expr: &ast::Expr) -> Option<ast::Ident> {
    match expr {
        ast::Expr::Identifier(ident) => Some(ident.clone()),
        ast::Expr::CompoundIdentifier(parts) => parts.parts.last().cloned(),
        ast::Expr::Nested(inner) => projection_expr_default_name(&inner.expression),
        _ => None,
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn validate_payload_projection_column(
    ident: &ast::Ident,
    seen: &mut std::collections::BTreeSet<String>,
) -> Result<(), String> {
    let normalized = ident.value.to_ascii_lowercase();
    if is_reserved_payload_projection_name(&normalized) {
        return Err(format!(
            "join delta coalesce rewrite reserved payload output column `{}`",
            ident.value
        ));
    }
    if !seen.insert(normalized) {
        return Err(format!(
            "join delta coalesce rewrite duplicate payload output column `{}`",
            ident.value
        ));
    }
    Ok(())
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn is_reserved_payload_projection_name(normalized: &str) -> bool {
    matches!(
        normalized,
        "net"
            | "pending_inserts"
            | "pending_deletes"
            | "__nr_join_delta_change_stream"
            | "__nr_join_delta_payload_coalesced"
            | "__nr_join_delta_key_shape"
            | "__nr_join_delta_coalesced"
            | "__nr_join_delta_target_locator"
            | "_file"
            | "_pos"
    ) || normalized == novarocks_execution::exec::change_op::CHANGE_OP_COLUMN
        || normalized == JOIN_LEFT_ROW_ID_COLUMN
        || normalized == JOIN_RIGHT_ROW_ID_COLUMN
        || normalized == MV_JOIN_APPLY_KEY_COLUMN_NAME
        || normalized.starts_with("__nr_join_delta_branch_")
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn join_delta_branch_cte_name(index: usize) -> String {
    format!("__nr_join_delta_branch_{index}")
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn change_stream_select_list(
    payload_columns: &[ast::Ident],
    left_uuid: &str,
    right_uuid: &str,
) -> String {
    let mut items = payload_columns.iter().map(render_ident).collect::<Vec<_>>();
    items.push(format!(
        "join_row_key({}, {}, {}, {}) AS {}",
        sql_string_literal(left_uuid),
        JOIN_LEFT_ROW_ID_COLUMN,
        sql_string_literal(right_uuid),
        JOIN_RIGHT_ROW_ID_COLUMN,
        MV_JOIN_APPLY_KEY_COLUMN_NAME,
    ));
    items.push(novarocks_execution::exec::change_op::CHANGE_OP_COLUMN.to_string());
    items.join(", ")
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn payload_coalesced_select_list(payload_columns: &[ast::Ident]) -> String {
    let mut items = vec![MV_JOIN_APPLY_KEY_COLUMN_NAME.to_string()];
    items.extend(payload_columns.iter().map(render_ident));
    items.push(format!(
        "SUM({}) AS net",
        novarocks_execution::exec::change_op::CHANGE_OP_COLUMN
    ));
    items.join(", ")
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn payload_group_by_list(payload_columns: &[ast::Ident]) -> String {
    let mut items = vec![MV_JOIN_APPLY_KEY_COLUMN_NAME.to_string()];
    items.extend(payload_columns.iter().map(render_ident));
    items.join(", ")
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn key_shape_select_list() -> String {
    [
        MV_JOIN_APPLY_KEY_COLUMN_NAME.to_string(),
        "SUM(CASE WHEN net > 0 THEN 1 ELSE 0 END) AS pending_inserts".to_string(),
        "SUM(CASE WHEN net < 0 THEN 1 ELSE 0 END) AS pending_deletes".to_string(),
    ]
    .join(", ")
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn valid_payload_select_list(payload_columns: &[ast::Ident]) -> String {
    let mut items = payload_columns
        .iter()
        .map(|ident| {
            let rendered = render_ident(ident);
            format!("pc.{rendered} AS {rendered}")
        })
        .collect::<Vec<_>>();
    let key = MV_JOIN_APPLY_KEY_COLUMN_NAME;
    items.push(format!("pc.{key} AS {key}"));
    items.push("pc.net AS net".to_string());
    items.join(", ")
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn final_coalesced_select_list(payload_columns: &[ast::Ident]) -> String {
    let mut items = payload_columns
        .iter()
        .map(|ident| {
            let rendered = render_ident(ident);
            format!("coalesced.{rendered} AS {rendered}")
        })
        .collect::<Vec<_>>();
    let key = MV_JOIN_APPLY_KEY_COLUMN_NAME;
    items.push(format!("coalesced.{key} AS {key}"));
    items.push(format!(
        "CAST(CASE WHEN coalesced.net > 0 THEN {} ELSE {} END AS TINYINT) AS {}",
        novarocks_execution::exec::change_op::CHANGE_OP_INSERT,
        novarocks_execution::exec::change_op::CHANGE_OP_DELETE,
        novarocks_execution::exec::change_op::CHANGE_OP_COLUMN,
    ));
    items.push("tgt._file AS _file".to_string());
    items.push("tgt._pos AS _pos".to_string());
    items.join(", ")
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) fn join_delta_target_locator_relation(namespace: &str) -> String {
    format!(
        "{}.{}",
        quote_sql_identifier(namespace),
        quote_sql_identifier(JOIN_DELTA_TARGET_LOCATOR_TABLE)
    )
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn quote_sql_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn parse_query_from_sql(sql: &str) -> Result<ast::Query, String> {
    let statements = novarocks_parser::parse(sql).map_err(|error| error.to_string())?;
    let [ast::Statement::Query(query)] = statements.as_slice() else {
        return Err("expected generated SQL to parse as query".to_string());
    };
    Ok(query.clone())
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn replace_branch_cte_queries(
    query: &mut ast::Query,
    branch_queries: Vec<ast::Query>,
) -> Result<(), String> {
    let branch_count = branch_queries.len();
    let with = query
        .with
        .as_mut()
        .ok_or_else(|| "join delta coalesce rewrite generated query without WITH".to_string())?;
    if with.ctes.len() < branch_count {
        return Err(format!(
            "join delta coalesce rewrite generated {} CTEs for {branch_count} branches",
            with.ctes.len()
        ));
    }
    for (index, (cte, branch_query)) in with.ctes.iter_mut().zip(branch_queries).enumerate() {
        let expected = join_delta_branch_cte_name(index);
        if !cte.name.value.eq_ignore_ascii_case(&expected) {
            return Err(format!(
                "join delta coalesce rewrite expected CTE `{expected}` at position {index}, found `{}`",
                cte.name.value
            ));
        }
        *cte.query = branch_query;
    }
    Ok(())
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn rewrite_branch_factor(
    factor: &mut ast::TableFactor,
    base: &novarocks_types::naming::TableIdentity,
    side: BranchSide,
    alias: &str,
) -> Result<BranchRewrite, String> {
    match side {
        BranchSide::Delta(window) => {
            let effective_alias = table_factor_alias(factor)
                .ok_or_else(|| "join branch delta side must be a table".to_string())?
                .unwrap_or_else(|| generated_ident(alias));
            *factor =
                build_nr_ivm_delta_table_factor_for_join(base, window, effective_alias.clone());
            Ok(BranchRewrite {
                alias: effective_alias,
                is_delta: true,
            })
        }
        BranchSide::Snapshot(snapshot_id) => {
            let ast::TableFactor::Table {
                name,
                version,
                alias: factor_alias,
                ..
            } = factor
            else {
                return Err("join branch snapshot side must be a table".to_string());
            };
            *name = base_table_object_name(base);
            *version = Some(ast::TableVersion {
                kind: ast::TableVersionKind::ForVersionAsOf,
                value: ast::Expr::Literal(ast::Literal {
                    kind: ast::LiteralKind::Number(snapshot_id.to_string()),
                    span: Span::new(0, 0),
                }),
                span: Span::new(0, 0),
            });
            let effective_alias = factor_alias
                .as_ref()
                .map(|alias| alias.name.clone())
                .unwrap_or_else(|| generated_ident(alias));
            if factor_alias.is_none() {
                *factor_alias = Some(ast::TableAlias {
                    name: effective_alias.clone(),
                    columns: Vec::new(),
                    explicit_as: true,
                    span: Span::new(0, 0),
                });
            }
            Ok(BranchRewrite {
                alias: effective_alias,
                is_delta: false,
            })
        }
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn table_factor_alias(factor: &ast::TableFactor) -> Option<Option<ast::Ident>> {
    let ast::TableFactor::Table { alias, .. } = factor else {
        return None;
    };
    Some(alias.as_ref().map(|alias| alias.name.clone()))
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn base_table_object_name(base: &novarocks_types::naming::TableIdentity) -> ast::ObjectName {
    ast::ObjectName {
        parts: vec![
            generated_ident(&base.catalog),
            generated_ident(&base.namespace),
            generated_ident(&base.table),
        ],
        span: Span::new(0, 0),
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn build_nr_ivm_delta_table_factor_for_join(
    base: &novarocks_types::naming::TableIdentity,
    window: SnapshotWindow,
    alias: ast::Ident,
) -> ast::TableFactor {
    let sql = format!(
        "SELECT * FROM __nr_ivm_delta({}, {}, {}) AS {}",
        sql_string_literal(&base.fqn()),
        window.from,
        window.to,
        render_ident(&alias),
    );
    let query = parse_query_from_sql(&sql)
        .expect("generated join delta table function must parse as a native query");
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        unreachable!("generated join delta query must have SELECT body");
    };
    select.from[0].relation.clone()
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn append_join_hidden_projection(
    select: &mut ast::Select,
    left_branch: &BranchRewrite,
    right_branch: &BranchRewrite,
) -> Result<(), String> {
    let delta_alias = match (left_branch.is_delta, right_branch.is_delta) {
        (true, false) => &left_branch.alias,
        (false, true) => &right_branch.alias,
        (false, false) => {
            return Err("join branch rewrite requires exactly one delta side".to_string());
        }
        (true, true) => {
            return Err("join branch rewrite requires exactly one delta side".to_string());
        }
    };
    select.projection.push(change_op_alias(delta_alias));
    select
        .projection
        .push(row_id_alias(&left_branch.alias, JOIN_LEFT_ROW_ID_COLUMN));
    select
        .projection
        .push(row_id_alias(&right_branch.alias, JOIN_RIGHT_ROW_ID_COLUMN));
    Ok(())
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn change_op_alias(alias: &ast::Ident) -> ast::SelectItem {
    qualified_alias(
        alias,
        novarocks_execution::exec::change_op::CHANGE_OP_COLUMN,
        novarocks_execution::exec::change_op::CHANGE_OP_COLUMN,
    )
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn row_id_alias(alias: &ast::Ident, output: &str) -> ast::SelectItem {
    qualified_alias(alias, "_row_id", output)
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn qualified_alias(qualifier: &ast::Ident, column: &str, output: &str) -> ast::SelectItem {
    let sql = format!(
        "SELECT {}.{} AS {}",
        render_ident(qualifier),
        quote_sql_identifier(column),
        quote_sql_identifier(output),
    );
    let query = parse_query_from_sql(&sql)
        .expect("generated join hidden projection must parse as a native query");
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        unreachable!("generated join hidden projection must have SELECT body");
    };
    select.projection[0].clone()
}

fn generated_ident(value: &str) -> ast::Ident {
    ast::Ident {
        value: value.to_string(),
        quoted: false,
        quote_style: None,
        span: Span::new(0, 0),
    }
}

fn render_ident(ident: &ast::Ident) -> String {
    if ident.quoted {
        quote_sql_identifier(&ident.value)
    } else {
        ident.value.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base(name: &str) -> novarocks_types::naming::TableIdentity {
        novarocks_types::naming::TableIdentity {
            catalog: "ice".to_string(),
            namespace: "ns".to_string(),
            table: name.to_string(),
        }
    }

    #[test]
    fn both_changed_uses_telescoping_order() {
        let left = base("left");
        let right = base("right");
        let plans = plan_join_delta_branches(
            &left,
            &right,
            SnapshotWindow { from: 10, to: 11 },
            SnapshotWindow { from: 20, to: 21 },
            true,
            true,
        );
        assert_eq!(plans.len(), 2);
        assert_eq!(
            plans[0].left,
            BranchSide::Delta(SnapshotWindow { from: 10, to: 11 })
        );
        assert_eq!(plans[0].right, BranchSide::Snapshot(20));
        assert_eq!(plans[1].left, BranchSide::Snapshot(11));
        assert_eq!(
            plans[1].right,
            BranchSide::Delta(SnapshotWindow { from: 20, to: 21 })
        );
    }

    #[test]
    fn only_left_changed_has_one_branch() {
        let left = base("left");
        let right = base("right");
        let plans = plan_join_delta_branches(
            &left,
            &right,
            SnapshotWindow { from: 10, to: 11 },
            SnapshotWindow { from: 20, to: 20 },
            true,
            false,
        );
        assert_eq!(plans.len(), 1);
        assert_eq!(
            plans[0].left,
            BranchSide::Delta(SnapshotWindow { from: 10, to: 11 })
        );
        assert_eq!(plans[0].right, BranchSide::Snapshot(20));
    }

    #[test]
    fn branch_rewrite_delta_left_snapshot_right() {
        let query = parse_query(
            "select l.id, r.label from ice.ns.left l join ice.ns.right r on l.id = r.id",
        );
        let left = base("left");
        let right = base("right");
        let plan = JoinDeltaBranchPlan {
            left_base: left,
            right_base: right,
            left: BranchSide::Delta(SnapshotWindow { from: 10, to: 11 }),
            right: BranchSide::Snapshot(20),
        };
        let rewritten = rewrite_join_branch_query(&query, &plan, "l", "r").expect("rewrite");
        let rendered = novarocks_parser::printer::print_query(&rewritten);
        assert!(rendered.contains("__nr_ivm_delta"), "sql={rendered}");
        assert!(rendered.contains("VERSION AS OF 20"), "sql={rendered}");
        assert!(
            rendered.contains("l.`__change_op` AS `__change_op`"),
            "sql={rendered}"
        );
        assert!(rendered.contains("__nova_left_row_id"), "sql={rendered}");
        assert!(rendered.contains("__nova_right_row_id"), "sql={rendered}");
    }

    #[test]
    fn branch_rewrite_snapshot_left_delta_right_qualifies_change_op_with_right_alias() {
        let query = parse_query(
            "select l.id, r.label from ice.ns.left l join ice.ns.right r on l.id = r.id",
        );
        let left = base("left");
        let right = base("right");
        let plan = JoinDeltaBranchPlan {
            left_base: left,
            right_base: right,
            left: BranchSide::Snapshot(11),
            right: BranchSide::Delta(SnapshotWindow { from: 20, to: 21 }),
        };
        let rewritten = rewrite_join_branch_query(&query, &plan, "l", "r").expect("rewrite");
        let rendered = novarocks_parser::printer::print_query(&rewritten);
        assert!(rendered.contains("VERSION AS OF 11"), "sql={rendered}");
        assert!(
            rendered.contains("r.`__change_op` AS `__change_op`"),
            "sql={rendered}"
        );
        assert!(rendered.contains("__nr_ivm_delta"), "sql={rendered}");
        assert!(rendered.contains("__nova_left_row_id"), "sql={rendered}");
        assert!(rendered.contains("__nova_right_row_id"), "sql={rendered}");
    }

    #[test]
    fn branch_rewrite_preserves_quoted_aliases_in_hidden_projection() {
        let query = parse_query(
            "select `Left Alias`.id, `Right Alias`.label \
             from ice.ns.left as `Left Alias` \
             join ice.ns.right as `Right Alias` on `Left Alias`.id = `Right Alias`.id",
        );
        let left = base("left");
        let right = base("right");
        let plan = JoinDeltaBranchPlan {
            left_base: left,
            right_base: right,
            left: BranchSide::Delta(SnapshotWindow { from: 10, to: 11 }),
            right: BranchSide::Snapshot(20),
        };
        let rewritten = rewrite_join_branch_query(&query, &plan, "fallback_left", "fallback_right")
            .expect("rewrite");
        let rendered = novarocks_parser::printer::print_query(&rewritten);
        assert!(
            rendered.contains("`Left Alias`.`__change_op` AS `__change_op`"),
            "sql={rendered}"
        );
        assert!(
            rendered.contains("`Left Alias`.`_row_id` AS `__nova_left_row_id"),
            "sql={rendered}"
        );
        assert!(
            rendered.contains("`Right Alias`.`_row_id` AS `__nova_right_row_id"),
            "sql={rendered}"
        );
        assert!(
            !rendered.contains("fallback_left") && !rendered.contains("fallback_right"),
            "sql={rendered}"
        );
    }

    #[test]
    fn join_delta_coalesce_two_branches_builds_grouped_change_stream() {
        let query = simple_join_query();
        let left = base("left");
        let right = base("right");
        let branches = plan_join_delta_branches(
            &left,
            &right,
            SnapshotWindow { from: 10, to: 11 },
            SnapshotWindow { from: 20, to: 21 },
            true,
            true,
        );

        let rewritten = rewrite_join_delta_coalesce_query(
            &query,
            &branches,
            "l",
            "r",
            "left-uuid",
            "right-uuid",
        )
        .expect("coalesce rewrite");
        let rendered = novarocks_parser::printer::print_query(&rewritten);

        assert_sql_contains(&rendered, "__nr_join_delta_branch_0");
        assert_sql_contains(&rendered, "__nr_join_delta_branch_1");
        assert_sql_contains(&rendered, "UNION ALL");
        assert_sql_contains(&rendered, "join_row_key");
        assert_sql_contains(&rendered, "'left-uuid'");
        assert_sql_contains(&rendered, JOIN_LEFT_ROW_ID_COLUMN);
        assert_sql_contains(&rendered, "'right-uuid'");
        assert_sql_contains(&rendered, JOIN_RIGHT_ROW_ID_COLUMN);
        assert_sql_contains(&rendered, MV_JOIN_APPLY_KEY_COLUMN_NAME);
        assert_sql_contains(&rendered, "GROUP BY __nova_join_row_key");
        assert_sql_contains(&rendered, "SUM(__change_op)");
        assert_sql_contains(&rendered, "__nr_join_delta_payload_coalesced");
        assert_sql_contains(&rendered, "GROUP BY __nova_join_row_key, id, label");
        assert_sql_contains(&rendered, "__nr_join_delta_key_shape");
        assert_sql_contains(&rendered, "pc.id AS id");
        assert_sql_contains(&rendered, "pc.label AS label");
        assert_sql_contains(&rendered, "HAVING SUM(__change_op) != 0");
        assert_sql_contains(
            &rendered,
            "assert_true(abs(SUM(__change_op)) <= 1, 'join delta per-payload net change exceeds 1')",
        );
        assert_sql_contains(
            &rendered,
            "assert_true(SUM(CASE WHEN net > 0 THEN 1 ELSE 0 END) <= 1 AND SUM(CASE WHEN net < 0 THEN 1 ELSE 0 END) <= 1, 'join delta multiple pending payloads for key')",
        );
        assert_sql_contains(
            &rendered,
            "CAST(CASE WHEN coalesced.net > 0 THEN 1 ELSE -1 END AS TINYINT) AS __change_op",
        );
        assert_sql_contains(&rendered, "__nova_join_row_key");
    }

    #[test]
    fn join_delta_coalesce_final_select_joins_target_locator_for_delete_positions() {
        let rendered = novarocks_parser::printer::print_query(
            &rewrite_simple_join_delta_coalesce(
                "select l.id, r.label from ice.ns.left l join ice.ns.right r on l.id = r.id",
            )
            .expect("coalesce rewrite"),
        );

        assert_sql_contains(&rendered, "LEFT JOIN");
        assert_sql_contains(&rendered, "__nr_join_delta_target_locator");
        assert_sql_contains(
            &rendered,
            "coalesced.__nova_join_row_key = tgt.__nova_join_row_key",
        );
        assert_sql_contains(&rendered, "tgt._file AS _file");
        assert_sql_contains(&rendered, "tgt._pos AS _pos");
        assert_sql_contains(
            &rendered,
            "assert_true(coalesced.net >= 0 OR (tgt._file IS NOT NULL AND tgt._pos IS NOT NULL), 'join delta DELETE row missing target locator')",
        );
    }

    #[test]
    fn join_delta_coalesce_groups_by_payload_before_key_shape_check() {
        let rendered = novarocks_parser::printer::print_query(
            &rewrite_simple_join_delta_coalesce(
                "select l.id, r.label from ice.ns.left l join ice.ns.right r on l.id = r.id",
            )
            .expect("coalesce rewrite"),
        );

        assert_sql_contains(&rendered, "__nr_join_delta_payload_coalesced");
        assert_sql_contains(&rendered, "GROUP BY __nova_join_row_key, id, label");
        assert_sql_contains(
            &rendered,
            "assert_true(abs(SUM(__change_op)) <= 1, 'join delta per-payload net change exceeds 1')",
        );
        assert_sql_contains(&rendered, "__nr_join_delta_key_shape");
        assert_sql_contains(
            &rendered,
            "SUM(CASE WHEN net > 0 THEN 1 ELSE 0 END) AS pending_inserts",
        );
        assert_sql_contains(
            &rendered,
            "SUM(CASE WHEN net < 0 THEN 1 ELSE 0 END) AS pending_deletes",
        );
        assert!(
            !rendered.contains("any_value(id) AS id"),
            "payload must not be chosen with any_value: sql={rendered}"
        );
    }

    #[test]
    fn join_delta_coalesce_one_branch_builds_grouped_change_stream() {
        let query = simple_join_query();
        let left = base("left");
        let right = base("right");
        let branches = plan_join_delta_branches(
            &left,
            &right,
            SnapshotWindow { from: 10, to: 11 },
            SnapshotWindow { from: 20, to: 20 },
            true,
            false,
        );

        let rewritten = rewrite_join_delta_coalesce_query(
            &query,
            &branches,
            "l",
            "r",
            "left-uuid",
            "right-uuid",
        )
        .expect("coalesce rewrite");
        let rendered = novarocks_parser::printer::print_query(&rewritten);

        assert_sql_contains(&rendered, "__nr_join_delta_branch_0");
        assert_sql_contains(&rendered, "__nr_join_delta_change_stream");
        assert_sql_contains(&rendered, "GROUP BY __nova_join_row_key");
        assert_sql_contains(&rendered, "SUM(__change_op)");
        assert_sql_contains(&rendered, "__nr_join_delta_payload_coalesced");
        assert_sql_contains(&rendered, "GROUP BY __nova_join_row_key, id, label");
        assert_sql_contains(&rendered, "__nr_join_delta_key_shape");
        assert_sql_contains(&rendered, "pc.id AS id");
        assert_sql_contains(&rendered, "pc.label AS label");
        assert_sql_contains(&rendered, "HAVING SUM(__change_op) != 0");
        assert_sql_contains(
            &rendered,
            "CAST(CASE WHEN coalesced.net > 0 THEN 1 ELSE -1 END AS TINYINT) AS __change_op",
        );
    }

    #[test]
    fn join_delta_coalesce_empty_branch_list_returns_error() {
        let query = simple_join_query();
        let err =
            rewrite_join_delta_coalesce_query(&query, &[], "l", "r", "left-uuid", "right-uuid")
                .expect_err("empty branch list must be rejected");

        assert!(
            err.contains("requires at least one branch"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn join_delta_coalesce_rejects_duplicate_payload_output_names() {
        let err = rewrite_simple_join_delta_coalesce(
            "select l.id, r.id from ice.ns.left l join ice.ns.right r on l.id = r.id",
        )
        .expect_err("duplicate payload names must be rejected");

        assert!(
            err.contains("duplicate payload output column `id`"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn join_delta_coalesce_rejects_reserved_payload_output_names() {
        for (sql, expected) in [
            (
                "select l.id as net, r.label from ice.ns.left l join ice.ns.right r on l.id = r.id",
                "reserved payload output column `net`",
            ),
            (
                "select l.id as __change_op, r.label from ice.ns.left l join ice.ns.right r on l.id = r.id",
                "reserved payload output column `__change_op`",
            ),
        ] {
            let err = rewrite_simple_join_delta_coalesce(sql)
                .expect_err("reserved name must be rejected");
            assert!(err.contains(expected), "unexpected error: {err}");
        }
    }

    #[test]
    fn join_delta_coalesce_rejects_wildcard_payload_projection() {
        let err = rewrite_simple_join_delta_coalesce(
            "select * from ice.ns.left l join ice.ns.right r on l.id = r.id",
        )
        .expect_err("wildcard payload must be rejected");

        assert!(
            err.contains("requires explicit payload projection columns"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn join_delta_coalesce_rejects_unaliased_non_column_payload_expression() {
        let err = rewrite_simple_join_delta_coalesce(
            "select l.id + 1, r.label from ice.ns.left l join ice.ns.right r on l.id = r.id",
        )
        .expect_err("unaliased expression payload must be rejected");

        assert!(
            err.contains("requires aliases for non-column payload expressions"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn join_delta_coalesce_preserves_quoted_payload_alias() {
        let rendered = novarocks_parser::printer::print_query(
            &rewrite_simple_join_delta_coalesce(
                "select l.id as `payload id`, r.label from ice.ns.left l join ice.ns.right r on l.id = r.id",
            )
            .expect("coalesce rewrite"),
        );

        assert_sql_contains(
            &rendered,
            "GROUP BY __nova_join_row_key, `payload id`, label",
        );
        assert_sql_contains(&rendered, "pc.`payload id` AS `payload id`");
        assert_sql_contains(&rendered, "`payload id` AS `payload id`");
    }

    #[test]
    fn join_delta_coalesce_rejects_unexpected_branch_cte_name() {
        let mut query = parse_query("WITH wrong AS (SELECT 1) SELECT * FROM wrong");
        let err = replace_branch_cte_queries(&mut query, vec![simple_join_query()])
            .expect_err("branch CTE name mismatch must be rejected");

        assert!(
            err.contains("expected CTE `__nr_join_delta_branch_0`"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn join_delta_append_only_wraps_branch_without_coalesce_grouping() {
        let branch_query = rewrite_left_delta_branch_query();
        let wrapped = rewrite_join_delta_append_only_query(
            &simple_join_query(),
            branch_query,
            "left-uuid",
            "right-uuid",
        )
        .expect("append-only rewrite");
        let rendered = novarocks_parser::printer::print_query(&wrapped);

        assert_sql_contains(&rendered, "join_row_key");
        assert_sql_contains(&rendered, "'left-uuid'");
        assert_sql_contains(&rendered, JOIN_LEFT_ROW_ID_COLUMN);
        assert_sql_contains(&rendered, "'right-uuid'");
        assert_sql_contains(&rendered, JOIN_RIGHT_ROW_ID_COLUMN);
        assert_sql_contains(&rendered, "AS __nova_join_row_key");
        assert_sql_contains(&rendered, "CAST(__change_op AS TINYINT) AS __change_op");
        assert!(
            !rendered.contains("GROUP BY"),
            "append-only fast path must not coalesce with GROUP BY: sql={rendered}"
        );
        assert!(
            !rendered.contains("__nr_join_delta_coalesced"),
            "append-only fast path must not use coalesce CTEs: sql={rendered}"
        );
        assert_final_select_excludes_row_id_columns(&wrapped);
    }

    #[test]
    fn join_delta_append_only_join_type_eligibility() {
        assert!(is_append_only_join_delta_eligible(&parse_query(
            "select l.id from ice.ns.left l join ice.ns.right r on l.id = r.id"
        )));
        assert!(is_append_only_join_delta_eligible(&parse_query(
            "select l.id from ice.ns.left l inner join ice.ns.right r on l.id = r.id"
        )));
        assert!(is_append_only_join_delta_eligible(&parse_query(
            "select l.id from ice.ns.left l cross join ice.ns.right r"
        )));

        assert!(!is_append_only_join_delta_eligible(&parse_query(
            "select l.id from ice.ns.left l left join ice.ns.right r on l.id = r.id"
        )));
        assert!(!is_append_only_join_delta_eligible(&parse_query(
            "select l.id from ice.ns.left l right join ice.ns.right r on l.id = r.id"
        )));
        assert!(!is_append_only_join_delta_eligible(&parse_query(
            "select l.id from ice.ns.left l full outer join ice.ns.right r on l.id = r.id"
        )));
    }

    fn simple_join_query() -> ast::Query {
        parse_query("select l.id, r.label from ice.ns.left l join ice.ns.right r on l.id = r.id")
    }

    fn rewrite_left_delta_branch_query() -> ast::Query {
        let query = simple_join_query();
        let left = base("left");
        let right = base("right");
        let plan = JoinDeltaBranchPlan {
            left_base: left,
            right_base: right,
            left: BranchSide::Delta(SnapshotWindow { from: 10, to: 11 }),
            right: BranchSide::Snapshot(20),
        };
        rewrite_join_branch_query(&query, &plan, "l", "r").expect("branch rewrite")
    }

    fn assert_final_select_excludes_row_id_columns(query: &ast::Query) {
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected SELECT body");
        };
        for item in &select.projection {
            let alias = match item {
                ast::SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.as_str()),
                ast::SelectItem::UnnamedExpr(ast::Expr::Identifier(ident)) => {
                    Some(ident.value.as_str())
                }
                _ => None,
            };
            assert!(
                alias != Some(JOIN_LEFT_ROW_ID_COLUMN) && alias != Some(JOIN_RIGHT_ROW_ID_COLUMN),
                "final select must not project row-id column alias: {item:?}"
            );
        }
    }

    fn rewrite_simple_join_delta_coalesce(sql: &str) -> Result<ast::Query, String> {
        let query = parse_query(sql);
        let left = base("left");
        let right = base("right");
        let branches = plan_join_delta_branches(
            &left,
            &right,
            SnapshotWindow { from: 10, to: 11 },
            SnapshotWindow { from: 20, to: 21 },
            true,
            true,
        );
        rewrite_join_delta_coalesce_query(&query, &branches, "l", "r", "left-uuid", "right-uuid")
    }

    fn assert_sql_contains(sql: &str, expected: &str) {
        assert!(sql.contains(expected), "expected `{expected}` in sql={sql}");
    }

    fn parse_query(sql: &str) -> ast::Query {
        let statements = novarocks_parser::parse(sql).expect("parse");
        let [ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("expected query");
        };
        query.clone()
    }
}
