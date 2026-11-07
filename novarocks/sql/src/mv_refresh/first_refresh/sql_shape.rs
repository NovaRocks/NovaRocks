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

//! Pure SQL shaping for a materialized-view first refresh.
//!
//! The application captures connector generations and turns them into this
//! immutable value before it enters the compiler.  The helpers below only
//! transform SQL ASTs; they have no catalog, execution, or connector
//! dependency.

use std::collections::HashSet;

use novarocks_parser::Span;
use novarocks_parser::printer;
use novarocks_parser::{ast, build};
use novarocks_spi::connector::ConnectorTableObjectId;
use novarocks_types::naming::TableIdentity;

use crate::compiler::SqlMvRelationOccurrenceId;
use crate::planner::vocabulary::{BRANCH_ID_COLUMN_NAME, HIDDEN_APPLY_KEY_COLUMN_NAME};

/// One definition occurrence and its exact first-refresh read facts.
///
/// The locator validates the matching SQL occurrence; identity is never
/// inferred from the locator and repeated locators remain distinct entries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvSnapshotPinOccurrence {
    occurrence_id: SqlMvRelationOccurrenceId,
    table: TableIdentity,
    snapshot_id: i64,
    table_object_id: ConnectorTableObjectId,
}

impl SqlMvSnapshotPinOccurrence {
    pub fn try_new(
        occurrence_id: SqlMvRelationOccurrenceId,
        table: TableIdentity,
        snapshot_id: i64,
        table_object_id: ConnectorTableObjectId,
    ) -> Result<Self, String> {
        if table.catalog.trim().is_empty()
            || table.namespace.trim().is_empty()
            || table.table.trim().is_empty()
            || snapshot_id < 0
            || table_object_id.as_bytes().is_empty()
        {
            return Err("MV first-refresh snapshot occurrence has invalid facts".to_string());
        }
        Ok(Self {
            occurrence_id,
            table,
            snapshot_id,
            table_object_id,
        })
    }

    pub const fn occurrence_id(&self) -> SqlMvRelationOccurrenceId {
        self.occurrence_id
    }

    pub const fn table(&self) -> &TableIdentity {
        &self.table
    }

    pub const fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    pub const fn table_object_id(&self) -> &ConnectorTableObjectId {
        &self.table_object_id
    }
}

/// Ordered, copied occurrence facts consumed by first-refresh SQL shaping.
/// It owns values only; it carries no catalog, table, or planning graph handle.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SqlMvSnapshotPin {
    occurrences: Vec<SqlMvSnapshotPinOccurrence>,
}

impl SqlMvSnapshotPin {
    pub fn try_from_occurrences(
        occurrences: Vec<SqlMvSnapshotPinOccurrence>,
    ) -> Result<Self, String> {
        if occurrences.is_empty() {
            return Err("MV first-refresh snapshot pin has incomplete identity facts".to_string());
        }
        let mut occurrence_ids = HashSet::with_capacity(occurrences.len());
        if occurrences
            .iter()
            .any(|occurrence| !occurrence_ids.insert(occurrence.occurrence_id))
        {
            return Err("MV first-refresh snapshot pin repeats an occurrence".to_string());
        }
        Ok(Self { occurrences })
    }

    #[cfg(test)]
    pub(super) fn from_entries_for_tests(entries: &[(u32, &str, i64, &str)]) -> Self {
        Self::try_from_occurrences(
            entries
                .iter()
                .map(|(occurrence_id, fqn, snapshot_id, object_id)| {
                    let parts = fqn.split('.').collect::<Vec<_>>();
                    let [catalog, namespace, table] = parts.as_slice() else {
                        panic!("test table identity must have three parts")
                    };
                    SqlMvSnapshotPinOccurrence::try_new(
                        SqlMvRelationOccurrenceId::new(*occurrence_id),
                        TableIdentity {
                            catalog: catalog.to_string(),
                            namespace: namespace.to_string(),
                            table: table.to_string(),
                        },
                        *snapshot_id,
                        ConnectorTableObjectId::try_new(bytes::Bytes::copy_from_slice(
                            object_id.as_bytes(),
                        ))
                        .expect("test object ID"),
                    )
                })
                .collect::<Result<Vec<_>, _>>()
                .expect("test MV snapshot occurrences must be valid"),
        )
        .expect("test MV snapshot pin must be valid")
    }

    /// Return exact facts for one frozen definition occurrence.
    pub fn get(
        &self,
        occurrence_id: SqlMvRelationOccurrenceId,
    ) -> Option<&SqlMvSnapshotPinOccurrence> {
        self.occurrences
            .iter()
            .find(|occurrence| occurrence.occurrence_id == occurrence_id)
    }

    pub fn len(&self) -> usize {
        self.occurrences.len()
    }

    pub fn is_empty(&self) -> bool {
        self.occurrences.is_empty()
    }

    pub fn occurrences(&self) -> &[SqlMvSnapshotPinOccurrence] {
        &self.occurrences
    }
}

pub(super) fn prepare_projection_full_read_sql(
    select_query: &ast::Query,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<ast::Query, String> {
    let mut query = select_query.clone();
    inject_pin_as_version_as_of(&mut query, pin, current_catalog, current_database)?;
    append_physical_apply_key(query)
}

pub(super) fn prepare_union_projection_full_read_sql(
    select_query: &ast::Query,
    branch_count: usize,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<ast::Query, String> {
    if branch_count < 2 {
        return Err("iceberg UNION ALL MV full refresh requires at least 2 branches".to_string());
    }
    let branch_count_i32 = i32::try_from(branch_count).map_err(|_| {
        format!("iceberg UNION ALL MV full refresh branch count {branch_count} does not fit in i32")
    })?;
    let mut query = select_query.clone();
    inject_pin_as_version_as_of(&mut query, pin, current_catalog, current_database)?;

    let mut validated_branch_count = 0;
    let mut saw_union_all = false;
    validate_union_projection_set_expr(
        query.body.as_ref(),
        branch_count,
        &mut validated_branch_count,
        &mut saw_union_all,
    )?;
    if !saw_union_all {
        return Err("iceberg UNION ALL MV full refresh requires an actual UNION ALL".to_string());
    }
    if validated_branch_count != branch_count {
        return Err(format!(
            "iceberg UNION ALL MV full refresh expected {branch_count} branches, rewrote {validated_branch_count}"
        ));
    }

    let mut next_branch_id = 0;
    append_union_projection_hidden_columns(query.body.as_mut(), &mut next_branch_id)?;
    debug_assert_eq!(next_branch_id, branch_count_i32);
    Ok(query)
}

pub(super) fn pin_state_sql(
    state_query: &ast::Query,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<ast::Query, String> {
    let mut query = state_query.clone();
    inject_pin_as_version_as_of(&mut query, pin, current_catalog, current_database)?;
    Ok(query)
}

pub(super) fn branch_union_queries(
    select_query: &ast::Query,
    branch_count: usize,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<Vec<ast::Query>, String> {
    let mut query = select_query.clone();
    inject_pin_as_version_as_of(&mut query, pin, current_catalog, current_database)?;
    let mut branch_bodies = Vec::new();
    flatten_branch_union_all_set_expr(query.body.as_ref(), &mut branch_bodies)?;
    if branch_bodies.len() != branch_count {
        return Err(format!(
            "iceberg branch UNION ALL aggregate first refresh expected {branch_count} branches, found {}",
            branch_bodies.len()
        ));
    }
    branch_bodies
        .into_iter()
        .map(|body| {
            let mut branch_query = query.clone();
            branch_query.body = Box::new(body);
            Ok(branch_query)
        })
        .collect()
}

fn append_physical_apply_key(mut query: ast::Query) -> Result<ast::Query, String> {
    let ast::SetExpr::Select(select) = query.body.as_mut() else {
        return Err("iceberg MV physical SELECT expects a SELECT body".to_string());
    };
    validate_reserved_projection_output_names(
        select,
        &[(HIDDEN_APPLY_KEY_COLUMN_NAME, "apply key")],
    )?;
    for item in &select.projection {
        if matches!(
            item,
            ast::SelectItem::Wildcard { .. } | ast::SelectItem::QualifiedWildcard { .. }
        ) {
            return Err(
                "iceberg MV physical SELECT requires explicit projection columns".to_string(),
            );
        }
    }
    select.projection.push(build::item(
        build::identifier(build::ident("_row_id", None)),
        Some(build::ident(HIDDEN_APPLY_KEY_COLUMN_NAME, None)),
        true,
    ));
    Ok(query)
}

fn validate_reserved_projection_output_names(
    select: &ast::Select,
    reserved: &[(&str, &str)],
) -> Result<(), String> {
    for item in &select.projection {
        let output_name = match item {
            ast::SelectItem::UnnamedExpr(expr) => Some(printer::print_expr(expr)),
            ast::SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
            ast::SelectItem::Wildcard { .. } | ast::SelectItem::QualifiedWildcard { .. } => None,
        };
        let Some(output_name) = output_name else {
            continue;
        };
        for (reserved_name, purpose) in reserved {
            if output_name.eq_ignore_ascii_case(reserved_name) {
                return Err(format!(
                    "Iceberg MV output column name {reserved_name} is reserved for internal {purpose}"
                ));
            }
        }
    }
    Ok(())
}

fn validate_union_projection_set_expr(
    set_expr: &ast::SetExpr,
    branch_count: usize,
    validated_branch_count: &mut usize,
    saw_union_all: &mut bool,
) -> Result<(), String> {
    match set_expr {
        ast::SetExpr::SetOperation(ast::SetOperation {
            operator,
            quantifier,
            left,
            right,
            ..
        }) => {
            if *operator != ast::SetOperator::Union || *quantifier != ast::SetQuantifier::All {
                return Err("iceberg UNION ALL MV full refresh supports UNION ALL only".to_string());
            }
            *saw_union_all = true;
            validate_union_projection_set_expr(
                left,
                branch_count,
                validated_branch_count,
                saw_union_all,
            )?;
            validate_union_projection_set_expr(
                right,
                branch_count,
                validated_branch_count,
                saw_union_all,
            )
        }
        ast::SetExpr::Query(query) => validate_union_projection_set_expr(
            query.body.as_ref(),
            branch_count,
            validated_branch_count,
            saw_union_all,
        ),
        ast::SetExpr::Select(select) => {
            if *validated_branch_count >= branch_count {
                return Err(format!(
                    "iceberg UNION ALL MV full refresh found more than {branch_count} branches"
                ));
            }
            validate_reserved_projection_output_names(
                select,
                &[
                    (HIDDEN_APPLY_KEY_COLUMN_NAME, "apply key"),
                    (BRANCH_ID_COLUMN_NAME, "branch id"),
                ],
            )?;
            *validated_branch_count += 1;
            Ok(())
        }
        _ => Err("iceberg UNION ALL MV full refresh expects SELECT branches".to_string()),
    }
}

fn append_union_projection_hidden_columns(
    set_expr: &mut ast::SetExpr,
    next_branch_id: &mut i32,
) -> Result<(), String> {
    match set_expr {
        ast::SetExpr::SetOperation(ast::SetOperation { left, right, .. }) => {
            append_union_projection_hidden_columns(left.as_mut(), next_branch_id)?;
            append_union_projection_hidden_columns(right.as_mut(), next_branch_id)
        }
        ast::SetExpr::Query(query) => {
            append_union_projection_hidden_columns(query.body.as_mut(), next_branch_id)
        }
        ast::SetExpr::Select(select) => {
            let branch_id = *next_branch_id;
            *next_branch_id = next_branch_id
                .checked_add(1)
                .ok_or_else(|| "iceberg UNION ALL MV branch id overflow".to_string())?;
            select.projection.push(build::item(
                build::identifier(build::ident("_row_id", None)),
                Some(build::ident(HIDDEN_APPLY_KEY_COLUMN_NAME, None)),
                true,
            ));
            select.projection.push(build::item(
                build::cast(
                    build::integer(i64::from(branch_id)),
                    build::simple_type("INT"),
                ),
                Some(build::ident(BRANCH_ID_COLUMN_NAME, None)),
                true,
            ));
            Ok(())
        }
        _ => Err("iceberg UNION ALL MV full refresh expects SELECT branches".to_string()),
    }
}

fn inject_pin_as_version_as_of(
    query: &mut ast::Query,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<usize, String> {
    let mut state = InjectState {
        pin,
        current_catalog,
        current_database,
        next_occurrence: 0,
        count: 0,
        first_error: None,
    };
    walk_query(query, &HashSet::new(), &mut state);
    if let Some(error) = state.first_error {
        return Err(error);
    }
    if state.next_occurrence != pin.len() {
        let next = &pin.occurrences()[state.next_occurrence];
        return Err(format!(
            "refresh SELECT did not contain definition occurrence {} ({})",
            next.occurrence_id().get(),
            next.table().fqn()
        ));
    }
    Ok(state.count)
}

struct InjectState<'a> {
    pin: &'a SqlMvSnapshotPin,
    current_catalog: Option<&'a str>,
    current_database: &'a str,
    next_occurrence: usize,
    count: usize,
    first_error: Option<String>,
}

fn walk_query(query: &mut ast::Query, outer_ctes: &HashSet<String>, state: &mut InjectState<'_>) {
    let mut body_ctes = outer_ctes.clone();
    if let Some(with) = &mut query.with {
        let local_names = with
            .ctes
            .iter()
            .map(|cte| cte.name.value.to_ascii_lowercase())
            .collect::<Vec<_>>();
        body_ctes.extend(local_names.iter().cloned());
        for cte in &mut with.ctes {
            walk_query(cte.query.as_mut(), &body_ctes, state);
        }
    }
    walk_set_expr(query.body.as_mut(), &body_ctes, state);
}

fn walk_set_expr(
    expr: &mut ast::SetExpr,
    visible_ctes: &HashSet<String>,
    state: &mut InjectState<'_>,
) {
    if state.first_error.is_some() {
        return;
    }
    match expr {
        ast::SetExpr::Select(select) => {
            for table_with_joins in &mut select.from {
                walk_table_with_joins(table_with_joins, visible_ctes, state);
            }
        }
        ast::SetExpr::SetOperation(ast::SetOperation { left, right, .. }) => {
            walk_set_expr(left.as_mut(), visible_ctes, state);
            walk_set_expr(right.as_mut(), visible_ctes, state);
        }
        ast::SetExpr::Query(query) => walk_query(query.as_mut(), visible_ctes, state),
        _ => {}
    }
}

fn walk_table_with_joins(
    table_with_joins: &mut ast::TableWithJoins,
    visible_ctes: &HashSet<String>,
    state: &mut InjectState<'_>,
) {
    walk_factor(&mut table_with_joins.relation, visible_ctes, state);
    for join in &mut table_with_joins.joins {
        walk_factor(&mut join.relation, visible_ctes, state);
    }
}

fn walk_factor(
    factor: &mut ast::TableFactor,
    visible_ctes: &HashSet<String>,
    state: &mut InjectState<'_>,
) {
    if state.first_error.is_some() {
        return;
    }
    match factor {
        ast::TableFactor::Table { name, version, .. } => {
            let parts = name
                .parts
                .iter()
                .map(|part| part.value.to_ascii_lowercase())
                .collect::<Vec<_>>();
            if parts.len() == 1 && visible_ctes.contains(&parts[0]) {
                return;
            }
            let Some(base_ref) =
                resolve_table_factor(&parts, state.current_catalog, state.current_database)
            else {
                state.first_error = Some(
                    "refresh SELECT contains an unsupported base relation identity".to_string(),
                );
                return;
            };
            let Some(pinned) = state.pin.occurrences().get(state.next_occurrence) else {
                state.first_error = Some(format!(
                    "refresh SELECT contains an unpinned relation occurrence {}",
                    base_ref.fqn()
                ));
                return;
            };
            if !same_table_identity(pinned.table(), &base_ref) {
                state.first_error = Some(format!(
                    "refresh SELECT occurrence {} resolved to {}, expected {}",
                    pinned.occurrence_id().get(),
                    base_ref.fqn(),
                    pinned.table().fqn()
                ));
                return;
            }
            if version.is_some() {
                state.first_error = Some(format!(
                    "refresh SELECT must not write explicit FOR VERSION AS OF for occurrence {} ({}); refresh pin would conflict",
                    pinned.occurrence_id().get(),
                    base_ref.fqn(),
                ));
                return;
            }
            *version = Some(ast::TableVersion {
                kind: ast::TableVersionKind::ForVersionAsOf,
                value: build::integer(pinned.snapshot_id()),
                span: Span::new(0, 0),
            });
            state.next_occurrence += 1;
            state.count += 1;
        }
        ast::TableFactor::Derived { subquery, .. } => {
            walk_query(subquery.as_mut(), visible_ctes, state)
        }
        ast::TableFactor::NestedJoin {
            table_with_joins, ..
        } => walk_table_with_joins(table_with_joins.as_mut(), visible_ctes, state),
        _ => {}
    }
}

fn same_table_identity(left: &TableIdentity, right: &TableIdentity) -> bool {
    left.catalog.eq_ignore_ascii_case(&right.catalog)
        && left.namespace.eq_ignore_ascii_case(&right.namespace)
        && left.table.eq_ignore_ascii_case(&right.table)
}

fn resolve_table_factor(
    parts: &[String],
    current_catalog: Option<&str>,
    current_database: &str,
) -> Option<TableIdentity> {
    let current_database = current_database.to_ascii_lowercase();
    let current_catalog = current_catalog.map(str::to_ascii_lowercase);
    match parts {
        [table] => current_catalog.map(|catalog| TableIdentity {
            catalog,
            namespace: current_database,
            table: table.clone(),
        }),
        [database, table] => current_catalog.map(|catalog| TableIdentity {
            catalog,
            namespace: database.clone(),
            table: table.clone(),
        }),
        [catalog, database, table] => Some(TableIdentity {
            catalog: catalog.clone(),
            namespace: database.clone(),
            table: table.clone(),
        }),
        _ => None,
    }
}

fn flatten_branch_union_all_set_expr(
    body: &ast::SetExpr,
    out: &mut Vec<ast::SetExpr>,
) -> Result<(), String> {
    match body {
        ast::SetExpr::SetOperation(ast::SetOperation {
            operator,
            quantifier,
            left,
            right,
            ..
        }) => {
            if !matches!(operator, ast::SetOperator::Union)
                || !matches!(quantifier, ast::SetQuantifier::All)
            {
                return Err(
                    "iceberg branch UNION ALL aggregate first refresh supports UNION ALL only"
                        .to_string(),
                );
            }
            flatten_branch_union_all_set_expr(left, out)?;
            flatten_branch_union_all_set_expr(right, out)
        }
        ast::SetExpr::Query(query) => flatten_branch_union_all_set_expr(query.body.as_ref(), out),
        ast::SetExpr::Select(_) => {
            out.push(body.clone());
            Ok(())
        }
        _ => Err(
            "iceberg branch UNION ALL aggregate first refresh expects SELECT branches".to_string(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_query(sql: &str) -> ast::Query {
        let statements = novarocks_parser::parse(sql).expect("parse query");
        let [ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("fixture must be a query");
        };
        query.clone()
    }

    #[test]
    fn sqlx2_mv_sql_shape_pins_projection_without_application_refresh_state() {
        let pin =
            SqlMvSnapshotPin::from_entries_for_tests(&[(7, "ice.db.fact", 42, "fact-incarnation")]);

        let query = parse_query("SELECT id FROM ice.db.fact");
        let sql = prepare_projection_full_read_sql(&query, &pin, Some("ice"), "db")
            .expect("SQL-only projection shape");

        use ast::SyntaxEq;
        assert!(sql.syntax_eq(&parse_query(
            "SELECT id, _row_id AS __nova_base_row_id FROM ice.db.fact FOR VERSION AS OF 42",
        )));
        assert_eq!(
            pin.get(SqlMvRelationOccurrenceId::new(7))
                .map(SqlMvSnapshotPinOccurrence::snapshot_id),
            Some(42)
        );
        assert_eq!(
            pin.get(SqlMvRelationOccurrenceId::new(7))
                .map(SqlMvSnapshotPinOccurrence::table_object_id)
                .map(|object_id| object_id.as_bytes().as_ref()),
            Some(b"fact-incarnation".as_ref())
        );
    }

    #[test]
    fn sqlx2_mv_sql_shape_rejects_incomplete_snapshot_identity() {
        let error = SqlMvSnapshotPin::try_from_occurrences(Vec::new())
            .expect_err("missing incarnation must fail before SQL shaping");

        assert!(error.contains("incomplete identity facts"), "{error}");
    }

    #[test]
    fn sqlx2_mv_sql_shape_preserves_sparse_repeated_occurrences() {
        let pin = SqlMvSnapshotPin::from_entries_for_tests(&[
            (7, "ice.db.fact", 11, "fact-incarnation"),
            (42, "ice.db.fact", 22, "fact-incarnation"),
        ]);
        let query =
            parse_query("SELECT l.id FROM ice.db.fact AS l JOIN ice.db.fact AS r ON l.id = r.id");
        let sql = prepare_projection_full_read_sql(&query, &pin, Some("ice"), "db")
            .expect("repeated definition occurrences remain distinct");

        use ast::SyntaxEq;
        assert!(sql.syntax_eq(&parse_query(
            "SELECT l.id, _row_id AS __nova_base_row_id FROM ice.db.fact FOR VERSION AS OF 11 AS l JOIN ice.db.fact FOR VERSION AS OF 22 AS r ON l.id = r.id",
        )));
        assert_eq!(pin.occurrences()[0].occurrence_id().get(), 7);
        assert_eq!(pin.occurrences()[1].occurrence_id().get(), 42);
    }

    #[test]
    fn sqlx2_mv_snapshot_pin_rejects_duplicate_occurrence_ids() {
        let entries = [
            (7, "ice.db.a", 11, "a-incarnation"),
            (7, "ice.db.b", 22, "b-incarnation"),
        ];
        let occurrences = entries
            .iter()
            .map(|(id, fqn, snapshot, object)| {
                let parts = fqn.split('.').collect::<Vec<_>>();
                SqlMvSnapshotPinOccurrence::try_new(
                    SqlMvRelationOccurrenceId::new(*id),
                    TableIdentity {
                        catalog: parts[0].to_string(),
                        namespace: parts[1].to_string(),
                        table: parts[2].to_string(),
                    },
                    *snapshot,
                    ConnectorTableObjectId::try_new(bytes::Bytes::copy_from_slice(
                        object.as_bytes(),
                    ))
                    .unwrap(),
                )
                .unwrap()
            })
            .collect();

        assert!(
            SqlMvSnapshotPin::try_from_occurrences(occurrences)
                .unwrap_err()
                .contains("repeats an occurrence")
        );
    }
}
