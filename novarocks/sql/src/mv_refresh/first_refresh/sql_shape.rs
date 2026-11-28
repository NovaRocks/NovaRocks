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
use novarocks_parser::{ast, build};
use novarocks_spi::connector::ConnectorTableObjectId;
use novarocks_types::naming::TableIdentity;

use crate::compiler::SqlMvRelationOccurrenceId;

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
    captured_sources: bool,
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
        Ok(Self {
            occurrences,
            captured_sources: false,
        })
    }

    /// The original expanded relation bindings already address these pins.
    /// In this mode a syntax-level table can be a view rather than a scan.
    pub fn with_captured_sources(mut self) -> Self {
        self.captured_sources = true;
        self
    }

    pub(super) const fn uses_captured_sources(&self) -> bool {
        self.captured_sources
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
    Ok(query)
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
            branch_query.body = ast::AstBox::new(body);
            Ok(branch_query)
        })
        .collect()
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
            distinct,
            inputs,
            corresponding,
            ..
        }) => {
            if corresponding.is_some() {
                return Err(
                    "iceberg UNION ALL MV full refresh does not support CORRESPONDING".to_string(),
                );
            }
            if *operator != ast::SetOperator::Union || *distinct {
                return Err("iceberg UNION ALL MV full refresh supports UNION ALL only".to_string());
            }
            if inputs.len() < 2 {
                return Err(
                    "iceberg UNION ALL MV full refresh requires at least 2 inputs".to_string(),
                );
            }
            *saw_union_all = true;
            for input in inputs {
                validate_union_projection_set_expr(
                    input,
                    branch_count,
                    validated_branch_count,
                    saw_union_all,
                )?;
            }
            Ok(())
        }
        ast::SetExpr::Query(query) => validate_union_projection_set_expr(
            query.query.body.as_ref(),
            branch_count,
            validated_branch_count,
            saw_union_all,
        ),
        ast::SetExpr::Select(_) => {
            if *validated_branch_count >= branch_count {
                return Err(format!(
                    "iceberg UNION ALL MV full refresh found more than {branch_count} branches"
                ));
            }
            *validated_branch_count += 1;
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
    if pin.uses_captured_sources() {
        return Ok(0);
    }
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
            if let Some(relation) = &mut select.from {
                walk_factor(relation, visible_ctes, state);
            }
        }
        ast::SetExpr::SetOperation(operation) => {
            if operation.corresponding.is_some() {
                state.first_error =
                    Some("iceberg MV first refresh does not support CORRESPONDING".to_string());
                return;
            }
            for input in &mut operation.inputs {
                walk_set_expr(input, visible_ctes, state);
            }
        }
        ast::SetExpr::Query(query) => walk_query(query.query.as_mut(), visible_ctes, state),
        _ => {}
    }
}

fn walk_factor(
    factor: &mut ast::Relation,
    visible_ctes: &HashSet<String>,
    state: &mut InjectState<'_>,
) {
    if state.first_error.is_some() {
        return;
    }
    match factor {
        ast::Relation::Table(table) => {
            let name = &table.name;
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
            if table.query_period.is_some() {
                state.first_error = Some(format!(
                    "refresh SELECT must not write explicit FOR VERSION AS OF for occurrence {} ({}); refresh pin would conflict",
                    pinned.occurrence_id().get(),
                    base_ref.fqn(),
                ));
                return;
            }
            table.query_period = Some(ast::QueryPeriod {
                range_type: ast::QueryPeriodRangeType::Version,
                start: None,
                end: Some(ast::AstBox::new(build::integer(pinned.snapshot_id()))),
                span: Span::new(0, 0),
            });
            state.next_occurrence += 1;
            state.count += 1;
        }
        ast::Relation::TableSubquery(node) => walk_query(&mut node.query, visible_ctes, state),
        ast::Relation::Lateral(node) => walk_query(&mut node.query, visible_ctes, state),
        ast::Relation::Aliased(node) => walk_factor(&mut node.relation, visible_ctes, state),
        ast::Relation::Join(node) => {
            walk_factor(&mut node.left, visible_ctes, state);
            walk_factor(&mut node.right, visible_ctes, state);
        }
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
            distinct,
            inputs,
            corresponding,
            ..
        }) => {
            if corresponding.is_some() {
                return Err("iceberg branch UNION ALL aggregate first refresh does not support CORRESPONDING".to_string());
            }
            if !matches!(operator, ast::SetOperator::Union) || *distinct {
                return Err(
                    "iceberg branch UNION ALL aggregate first refresh supports UNION ALL only"
                        .to_string(),
                );
            }
            if inputs.len() < 2 {
                return Err(
                    "iceberg branch UNION ALL aggregate first refresh requires at least 2 inputs"
                        .to_string(),
                );
            }
            for input in inputs {
                flatten_branch_union_all_set_expr(input, out)?;
            }
            Ok(())
        }
        ast::SetExpr::Query(query) => {
            flatten_branch_union_all_set_expr(query.query.body.as_ref(), out)
        }
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
        let query = &novarocks_parser::parser::parse_query(sql).expect("parse query");
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
            "SELECT id FROM ice.db.fact FOR VERSION AS OF 42",
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
            "SELECT l.id FROM ice.db.fact FOR VERSION AS OF 11 AS l JOIN ice.db.fact FOR VERSION AS OF 22 AS r ON l.id = r.id",
        )));
        assert_eq!(pin.occurrences()[0].occurrence_id().get(), 7);
        assert_eq!(pin.occurrences()[1].occurrence_id().get(), 42);

        let union = parse_query(
            "(SELECT l.id FROM ice.db.fact AS l) UNION ALL SELECT r.id FROM ice.db.fact AS r",
        );
        let original_union = union.clone();
        let original_pin = pin.clone();
        let prepared = prepare_union_projection_full_read_sql(&union, 2, &pin, Some("ice"), "db")
            .expect("positional UNION ALL preserves repeated pinned occurrences");
        let ast::SetExpr::SetOperation(original) = union.body.as_ref() else {
            panic!("source UNION ALL");
        };
        let ast::SetExpr::SetOperation(operation) = prepared.body.as_ref() else {
            panic!("prepared UNION ALL");
        };
        assert_eq!(operation.inputs.len(), 2);
        assert!(matches!(&operation.inputs[0], ast::SetExpr::Query(_)));
        for (index, (input, source)) in operation.inputs.iter().zip(&original.inputs).enumerate() {
            let (select, source_select) = match (input, source) {
                (ast::SetExpr::Query(query), ast::SetExpr::Query(source_query)) => {
                    assert_eq!(query.span, source_query.span);
                    let ast::SetExpr::Select(select) = query.query.body.as_ref() else {
                        panic!("wrapped SELECT");
                    };
                    let ast::SetExpr::Select(source) = source_query.query.body.as_ref() else {
                        panic!("source wrapped SELECT");
                    };
                    (select, source)
                }
                (ast::SetExpr::Select(select), ast::SetExpr::Select(source)) => (select, source),
                _ => panic!("source Query boundary changed"),
            };
            assert_eq!(select.projection[0], source_select.projection[0]);
            type RelationAlias<'a> = (&'a ast::Ident, &'a Option<Vec<ast::Ident>>, Span);

            fn base(relation: &ast::Relation) -> (&ast::Table, Option<RelationAlias<'_>>) {
                match relation {
                    ast::Relation::Table(table) => (table, None),
                    ast::Relation::Aliased(alias) => {
                        let ast::Relation::Table(table) = alias.relation.as_ref() else {
                            panic!("base table")
                        };
                        (table, Some((&alias.alias, &alias.column_names, alias.span)))
                    }
                    _ => panic!("base table"),
                }
            }
            let (table, alias) = base(select.from.as_ref().unwrap());
            let (original_table, original_alias) = base(source_select.from.as_ref().unwrap());
            assert_eq!(table.name, original_table.name);
            assert_eq!(alias, original_alias);
            let version = table.query_period.as_ref().expect("pinned query period");
            assert_eq!(version.range_type, ast::QueryPeriodRangeType::Version);
            assert!(
                crate::analyzer::iceberg_ref::query_period_end(version)
                    .unwrap()
                    .syntax_eq(&build::integer(if index == 0 { 11 } else { 22 }))
            );
            assert_eq!(select.projection, source_select.projection);
        }
        assert_eq!(union, original_union);
        assert_eq!(pin, original_pin);
        for columns in [Vec::new(), vec![build::ident("id", false)]] {
            let mut corresponding = union.clone();
            let ast::SetExpr::SetOperation(operation) = corresponding.body.as_mut() else {
                panic!("UNION ALL");
            };
            operation.corresponding = Some(ast::Corresponding {
                columns,
                span: Span::new(0, 0),
            });
            assert!(
                prepare_union_projection_full_read_sql(&corresponding, 2, &pin, Some("ice"), "db")
                    .unwrap_err()
                    .contains("CORRESPONDING")
            );
            assert!(
                branch_union_queries(&corresponding, 2, &pin, Some("ice"), "db")
                    .unwrap_err()
                    .contains("CORRESPONDING")
            );
        }
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
