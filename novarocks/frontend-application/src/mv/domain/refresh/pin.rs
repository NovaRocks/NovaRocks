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

//! Refresh-scoped snapshot pin for iceberg-backed materialized views.
//!
//! `RefreshSnapshotPin` stores, for one refresh, the ordered D relation
//! occurrence, its `current_snapshot_id`, and its opaque provider object ID.
//! Repeated references to one physical table remain distinct. The pin is the
//! single source of truth for snapshot ids during the refresh:
//!
//! * provider change-window planning uses pin[occurrence] as its `to_snapshot_id`
//! * `begin_mv_refresh_intent` records pin as the refresh target
//! * publication binds each D relation occurrence to its exact captured input
//!
//! For single-base MVs (the only shape currently supported by the DDL gate),
//! this guarantees delta computation and bookkeeping agree on the same
//! snapshot, even if the base table commits concurrently during the refresh.
//!
//! For multi-base MVs (future), the pin additionally guarantees cross-table
//! consistency: every base table is read at the snapshot it had at refresh
//! start, regardless of intervening external commits.

use std::collections::HashSet;

#[cfg(test)]
use novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection;
use novarocks_spi::connector::{ConnectorExactSemanticRevision, ConnectorTableObjectId};
use novarocks_sql::compiler::SqlMvRelationOccurrenceId;
use novarocks_types::naming::TableIdentity;

/// One persisted D relation occurrence and the exact provider facts captured
/// for it at refresh entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefreshSnapshotPinOccurrence {
    occurrence_id: SqlMvRelationOccurrenceId,
    table: TableIdentity,
    snapshot_id: i64,
    table_object_id: ConnectorTableObjectId,
    semantic_revision: ConnectorExactSemanticRevision,
}

impl RefreshSnapshotPinOccurrence {
    pub fn try_new(
        occurrence_id: SqlMvRelationOccurrenceId,
        table: TableIdentity,
        snapshot_id: i64,
        table_object_id: ConnectorTableObjectId,
        semantic_revision: ConnectorExactSemanticRevision,
    ) -> Result<Self, String> {
        if table.catalog.trim().is_empty()
            || table.namespace.trim().is_empty()
            || table.table.trim().is_empty()
            || snapshot_id < 0
            || table_object_id.as_bytes().is_empty()
        {
            return Err("MV refresh snapshot occurrence has invalid facts".to_string());
        }
        if semantic_revision.object_identity().value().as_ref()
            != table_object_id.as_bytes().as_ref()
        {
            return Err(
                "MV refresh snapshot occurrence exact revision names a different object"
                    .to_string(),
            );
        }
        Ok(Self {
            occurrence_id,
            table,
            snapshot_id,
            table_object_id,
            semantic_revision,
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

    /// The provider's own exact revision for this occurrence, frozen by the
    /// same observation that produced the object ID. It stays opaque: the
    /// numeric selector beside it is the provider's fact, never a decode.
    pub const fn semantic_revision(&self) -> &ConnectorExactSemanticRevision {
        &self.semantic_revision
    }
}

/// Per-refresh snapshot pin. Vector order is persisted D occurrence order and
/// is also the order in which relation occurrences are consumed from the SQL
/// AST. The occurrence ID is an identity, not a vector index.
#[allow(dead_code)]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RefreshSnapshotPin {
    occurrences: Vec<RefreshSnapshotPinOccurrence>,
}

#[allow(dead_code)]
impl RefreshSnapshotPin {
    pub fn try_from_occurrences(
        occurrences: Vec<RefreshSnapshotPinOccurrence>,
    ) -> Result<Self, String> {
        if occurrences.is_empty() {
            return Err("MV refresh snapshot pin has no D relation occurrences".to_string());
        }
        let mut occurrence_ids = HashSet::with_capacity(occurrences.len());
        if occurrences
            .iter()
            .any(|occurrence| !occurrence_ids.insert(occurrence.occurrence_id))
        {
            return Err("MV refresh snapshot pin repeats a D relation occurrence".to_string());
        }
        Ok(Self { occurrences })
    }

    /// The exact revision every D occurrence was pinned at, keyed by D's own
    /// occurrence id. This is what P records as its input watermark, so the
    /// repeated occurrences of one self-joined object stay separate.
    pub fn exact_revisions_by_occurrence(
        &self,
    ) -> std::collections::BTreeMap<u32, ConnectorExactSemanticRevision> {
        self.occurrences
            .iter()
            .map(|occurrence| {
                (
                    occurrence.occurrence_id.get(),
                    occurrence.semantic_revision.clone(),
                )
            })
            .collect()
    }

    pub fn get(
        &self,
        occurrence_id: SqlMvRelationOccurrenceId,
    ) -> Option<&RefreshSnapshotPinOccurrence> {
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

    pub fn occurrences(&self) -> &[RefreshSnapshotPinOccurrence] {
        &self.occurrences
    }
}

#[cfg(test)]
/// Rejects a refresh when a persisted base-table identity no longer matches
/// the identity frozen in this attempt's pin.
pub fn validate_refresh_pin_table_object_ids(
    mv_definition: &StoredKeyedMvProjection,
    pin: &RefreshSnapshotPin,
    base_refs: &[TableIdentity],
) -> Result<(), String> {
    validate_refresh_pin_table_object_ids_for_operation(
        mv_definition,
        pin,
        base_refs,
        "incremental refresh is unsafe, rebuild or recreate the MV",
    )
}

#[cfg(test)]
fn validate_refresh_pin_table_object_ids_for_operation(
    mv_definition: &StoredKeyedMvProjection,
    pin: &RefreshSnapshotPin,
    base_refs: &[TableIdentity],
    unsafe_message: &str,
) -> Result<(), String> {
    let occurrences = &mv_definition.facts.definition().relation_occurrences;
    if occurrences.len() != base_refs.len() {
        return Err(
            "refresh base references do not retain every D relation occurrence".to_string(),
        );
    }
    if pin.len() != occurrences.len() {
        return Err("refresh pin does not retain every D relation occurrence".to_string());
    }
    for (occurrence, base_ref) in occurrences.iter().zip(base_refs) {
        if occurrence.catalog_at_binding != base_ref.catalog
            || occurrence.namespace_at_binding != base_ref.namespace
            || occurrence.relation_at_binding != base_ref.table
        {
            return Err(format!(
                "refresh base reference does not match D relation occurrence {}",
                occurrence.occurrence_id,
            ));
        }
        let occurrence_id = SqlMvRelationOccurrenceId::new(occurrence.occurrence_id);
        let pinned = pin.get(occurrence_id).ok_or_else(|| {
            format!(
                "refresh pin missing D relation occurrence {}",
                occurrence.occurrence_id
            )
        })?;
        if !same_table_identity(pinned.table(), base_ref) {
            return Err(format!(
                "iceberg MV base locator changed for {} (occurrence {}); {unsafe_message}",
                base_ref.fqn(),
                occurrence.occurrence_id,
            ));
        }
    }
    Ok(())
}

/// Walk `query` in place. For each `TableFactor::Table` whose 3-part name
/// resolves to the next persisted D occurrence, inject that occurrence's
/// exact snapshot unless its occurrence ID is in `delta_bearing`. Returns the
/// number of mutations performed.
///
/// Rules:
/// - `TableFactor::Table` with `version = Some(_)` already -> Err. The
///   refresh SELECT is not allowed to combine user-written FOR VERSION AS OF
///   with refresh pinning.
/// - A visible CTE reference does not consume a D occurrence.
/// - A relation that differs from the next D occurrence is rejected.
/// - An occurrence in `delta_bearing` is unchanged (handled by
///   the rewrite-path incremental refresh in iceberg_refresh.rs).
/// - Any other occurrence receives its own exact pinned version.
///
/// In scope-B single-base MVs, the unique base is delta-bearing, so this
/// function is a no-op in production. It exists for the multi-base future.
#[allow(dead_code)]
pub(crate) fn inject_pin_as_for_version_as_of(
    query: &mut novarocks_parser::ast::Query,
    pin: &RefreshSnapshotPin,
    delta_bearing: &HashSet<SqlMvRelationOccurrenceId>,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<usize, String> {
    let (count, consumed) = inject_pin_prefix_as_for_version_as_of(
        query,
        pin,
        delta_bearing,
        current_catalog,
        current_database,
        0,
    )?;
    require_pin_fully_consumed(pin, consumed)?;
    Ok(count)
}

/// Inject into one query that covers only part of the pin, starting at
/// `start_occurrence`.
///
/// A branch UNION is compiled one branch at a time, and each branch names only
/// the D occurrences it owns. Branches are laid out left to right in the same
/// order D records its occurrences, so a branch consumes a contiguous run of
/// the pin. Returns the mutation count and the next unconsumed occurrence
/// index; the caller is responsible for proving the whole pin was consumed
/// exactly once with `require_pin_fully_consumed`.
#[allow(dead_code)]
pub(crate) fn inject_pin_prefix_as_for_version_as_of(
    query: &mut novarocks_parser::ast::Query,
    pin: &RefreshSnapshotPin,
    delta_bearing: &HashSet<SqlMvRelationOccurrenceId>,
    current_catalog: Option<&str>,
    current_database: &str,
    start_occurrence: usize,
) -> Result<(usize, usize), String> {
    if start_occurrence > pin.len() {
        return Err("refresh pin cursor is past its last D relation occurrence".to_string());
    }
    let mut state = InjectState {
        pin,
        delta_bearing,
        current_catalog,
        current_database,
        next_occurrence: start_occurrence,
        count: 0,
        first_error: None,
    };
    walk_query(query, &HashSet::new(), &mut state);
    if let Some(err) = state.first_error {
        return Err(err);
    }
    Ok((state.count, state.next_occurrence))
}

/// Every D relation occurrence must have been named exactly once.
#[allow(dead_code)]
pub(crate) fn require_pin_fully_consumed(
    pin: &RefreshSnapshotPin,
    consumed: usize,
) -> Result<(), String> {
    if consumed == pin.len() {
        return Ok(());
    }
    let next = &pin.occurrences()[consumed];
    Err(format!(
        "refresh SELECT did not contain D relation occurrence {} ({})",
        next.occurrence_id().get(),
        next.table().fqn(),
    ))
}

struct InjectState<'a> {
    pin: &'a RefreshSnapshotPin,
    delta_bearing: &'a HashSet<SqlMvRelationOccurrenceId>,
    current_catalog: Option<&'a str>,
    current_database: &'a str,
    next_occurrence: usize,
    count: usize,
    first_error: Option<String>,
}

fn walk_query(
    query: &mut novarocks_parser::ast::Query,
    outer_ctes: &HashSet<String>,
    state: &mut InjectState<'_>,
) {
    let mut visible_ctes = outer_ctes.clone();
    if let Some(with) = &mut query.with {
        visible_ctes.extend(
            with.ctes
                .iter()
                .map(|cte| cte.name.value.to_ascii_lowercase()),
        );
        for cte in &mut with.ctes {
            walk_query(cte.query.as_mut(), &visible_ctes, state);
        }
    }
    walk_set_expr(query.body.as_mut(), &visible_ctes, state);
}

fn walk_set_expr(
    expr: &mut novarocks_parser::ast::SetExpr,
    visible_ctes: &HashSet<String>,
    state: &mut InjectState<'_>,
) {
    use novarocks_parser::ast::SetExpr;
    if state.first_error.is_some() {
        return;
    }
    match expr {
        SetExpr::Select(select) => {
            for tw in &mut select.from {
                walk_table_with_joins(tw, visible_ctes, state);
            }
        }
        novarocks_parser::ast::SetExpr::SetOperation(operation) => {
            walk_set_expr(operation.left.as_mut(), visible_ctes, state);
            walk_set_expr(operation.right.as_mut(), visible_ctes, state);
        }
        SetExpr::Query(query) => walk_query(query.as_mut(), visible_ctes, state),
        _ => {}
    }
}

fn walk_table_with_joins(
    table_with_joins: &mut novarocks_parser::ast::TableWithJoins,
    visible_ctes: &HashSet<String>,
    state: &mut InjectState<'_>,
) {
    walk_factor(&mut table_with_joins.relation, visible_ctes, state);
    for join in &mut table_with_joins.joins {
        walk_factor(&mut join.relation, visible_ctes, state);
    }
}

fn walk_factor(
    factor: &mut novarocks_parser::ast::TableFactor,
    visible_ctes: &HashSet<String>,
    state: &mut InjectState<'_>,
) {
    use novarocks_parser::ast::{
        Expr, Literal, LiteralKind, TableFactor, TableVersion, TableVersionKind,
    };
    if state.first_error.is_some() {
        return;
    }
    match factor {
        TableFactor::Table { name, version, .. } => {
            let parts: Vec<String> = name
                .parts
                .iter()
                .map(|ident| ident.value.to_ascii_lowercase())
                .collect();
            if parts.len() == 1 && visible_ctes.contains(&parts[0]) {
                return;
            }
            let Some(base_ref) =
                resolve_table_factor(&parts, state.current_catalog, state.current_database)
            else {
                state.first_error =
                    Some("refresh SELECT contains an unsupported base relation identity".into());
                return;
            };
            let Some(pinned) = state.pin.occurrences().get(state.next_occurrence) else {
                state.first_error = Some(format!(
                    "refresh SELECT contains an unpinned relation occurrence {}",
                    base_ref.fqn(),
                ));
                return;
            };
            if !same_table_identity(pinned.table(), &base_ref) {
                state.first_error = Some(format!(
                    "refresh SELECT occurrence {} resolved to {}, expected {}",
                    pinned.occurrence_id().get(),
                    base_ref.fqn(),
                    pinned.table().fqn(),
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
            state.next_occurrence += 1;
            if state.delta_bearing.contains(&pinned.occurrence_id()) {
                return;
            }
            *version = Some(TableVersion {
                kind: TableVersionKind::ForVersionAsOf,
                value: Expr::Literal(Literal {
                    kind: LiteralKind::Number(pinned.snapshot_id().to_string()),
                    span: name.span,
                }),
                span: name.span,
            });
            state.count += 1;
        }
        TableFactor::Derived { subquery, .. } => {
            walk_query(subquery.as_mut(), visible_ctes, state);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            walk_table_with_joins(table_with_joins.as_mut(), visible_ctes, state);
        }
        _ => {}
    }
}

fn same_table_identity(left: &TableIdentity, right: &TableIdentity) -> bool {
    left.catalog.eq_ignore_ascii_case(&right.catalog)
        && left.namespace.eq_ignore_ascii_case(&right.namespace)
        && left.table.eq_ignore_ascii_case(&right.table)
}

#[cfg(test)]
fn test_occurrence(
    occurrence_id: u32,
    fqn: &str,
    snapshot_id: i64,
    object_id: &[u8],
) -> RefreshSnapshotPinOccurrence {
    let parts = fqn.split('.').collect::<Vec<_>>();
    let [catalog, namespace, table] = parts.as_slice() else {
        panic!("test table identity must have three parts")
    };
    RefreshSnapshotPinOccurrence::try_new(
        SqlMvRelationOccurrenceId::new(occurrence_id),
        TableIdentity {
            catalog: catalog.to_string(),
            namespace: namespace.to_string(),
            table: table.to_string(),
        },
        snapshot_id,
        ConnectorTableObjectId::try_new(bytes::Bytes::copy_from_slice(object_id))
            .expect("test object ID is bounded"),
        ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                .expect("test provider ID"),
            &ConnectorTableObjectId::try_new(bytes::Bytes::copy_from_slice(object_id))
                .expect("test object ID is bounded"),
            Some(snapshot_id),
        )
        .expect("test exact revision"),
    )
    .expect("test refresh occurrence is valid")
}

fn resolve_table_factor(
    parts: &[String],
    current_catalog: Option<&str>,
    current_database: &str,
) -> Option<TableIdentity> {
    let current_database = current_database.to_ascii_lowercase();
    let current_catalog = current_catalog.map(|s| s.to_ascii_lowercase());
    match parts {
        [tbl] => current_catalog.map(|cat| TableIdentity {
            catalog: cat,
            namespace: current_database,
            table: tbl.clone(),
        }),
        [db, tbl] => current_catalog.map(|cat| TableIdentity {
            catalog: cat,
            namespace: db.clone(),
            table: tbl.clone(),
        }),
        [cat, db, tbl] => Some(TableIdentity {
            catalog: cat.clone(),
            namespace: db.clone(),
            table: tbl.clone(),
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::test_admission::KeyedProjectionFixture as ProjectionFixture;
    use novarocks_mv_application::product::MvTarget;
    use novarocks_parser::printer::print_query;

    fn parse_select_for_test(sql: &str) -> novarocks_parser::ast::Query {
        let statements = novarocks_parser::parse(sql).expect("test SQL must parse");
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("test SQL must be a query");
        };
        query.clone()
    }

    fn make_pin(entries: &[(u32, &str, i64, &[u8])]) -> RefreshSnapshotPin {
        RefreshSnapshotPin::try_from_occurrences(
            entries
                .iter()
                .map(|(id, fqn, snapshot, object)| test_occurrence(*id, fqn, *snapshot, object))
                .collect(),
        )
        .expect("test refresh pin")
    }

    fn make_ref(c: &str, n: &str, t: &str) -> TableIdentity {
        TableIdentity {
            catalog: c.to_string(),
            namespace: n.to_string(),
            table: t.to_string(),
        }
    }

    #[test]
    fn pin_identity_validation_retains_repeated_definition_occurrences() {
        let projection = StoredKeyedMvProjection {
            mv_id: 1,
            facts: ProjectionFixture::new(MvTarget::from_parts(Some("ice"), "sales", "mv"), None)
                .build()
                .expect("projection"),
        };
        let base = make_ref("ice", "sales", "orders");
        let pin = make_pin(&[
            (7, "ice.sales.orders", 42, &[11]),
            (8, "ice.sales.orders", 43, &[11]),
        ]);
        validate_refresh_pin_table_object_ids(&projection, &pin, &[base.clone(), base.clone()])
            .expect("both exact occurrences remain present");
        assert!(
            validate_refresh_pin_table_object_ids(&projection, &pin, std::slice::from_ref(&base))
                .unwrap_err()
                .contains("every D relation occurrence")
        );
        let wrong_locator = make_pin(&[
            (7, "ice.sales.other", 42, &[11]),
            (8, "ice.sales.orders", 43, &[11]),
        ]);
        assert!(
            validate_refresh_pin_table_object_ids(
                &projection,
                &wrong_locator,
                &[base.clone(), base]
            )
            .unwrap_err()
            .contains("occurrence 7")
        );
    }

    #[test]
    fn pin_preserves_sparse_repeated_occurrences_without_fqn_deduplication() {
        let pin = make_pin(&[
            (7, "ice.db.orders", 10, b"object-old"),
            (42, "ice.db.orders", 30, b"object-new"),
        ]);

        assert_eq!(pin.len(), 2);
        assert!(!pin.is_empty());
        assert_eq!(
            pin.occurrences()
                .iter()
                .map(|entry| (
                    entry.occurrence_id().get(),
                    entry.table().fqn(),
                    entry.snapshot_id(),
                    entry.table_object_id().as_bytes().to_vec(),
                ))
                .collect::<Vec<_>>(),
            vec![
                (7, "ice.db.orders".to_string(), 10, b"object-old".to_vec()),
                (42, "ice.db.orders".to_string(), 30, b"object-new".to_vec()),
            ]
        );
        assert_eq!(
            pin.get(SqlMvRelationOccurrenceId::new(42))
                .map(RefreshSnapshotPinOccurrence::snapshot_id),
            Some(30)
        );
    }

    #[test]
    fn pin_rejects_duplicate_occurrence_ids() {
        let error = RefreshSnapshotPin::try_from_occurrences(vec![
            test_occurrence(7, "ice.db.orders", 10, b"object-old"),
            test_occurrence(7, "ice.db.customers", 30, b"object-new"),
        ])
        .unwrap_err();

        assert!(error.contains("repeats a D relation occurrence"), "{error}");
    }

    #[test]
    fn inject_pin_skips_delta_bearing_base() {
        let mut query = parse_select_for_test("SELECT * FROM ice.db.orders");
        let pin = make_pin(&[(7, "ice.db.orders", 42, b"object-orders")]);
        let delta_bearing = HashSet::from([SqlMvRelationOccurrenceId::new(7)]);

        let count =
            inject_pin_as_for_version_as_of(&mut query, &pin, &delta_bearing, Some("ice"), "db")
                .expect("inject must succeed");

        assert_eq!(count, 0);
        assert_eq!(print_query(&query), "SELECT * FROM ice.db.orders");
    }

    #[test]
    fn inject_pin_injects_non_delta_bearing_base() {
        let mut query =
            parse_select_for_test("SELECT * FROM db.orders JOIN ice.db.customers ON true");
        let pin = make_pin(&[
            (7, "ice.db.orders", 42, b"object-orders"),
            (42, "ice.db.customers", 99, b"object-customers"),
        ]);
        let delta_bearing = HashSet::from([SqlMvRelationOccurrenceId::new(7)]);

        let count =
            inject_pin_as_for_version_as_of(&mut query, &pin, &delta_bearing, Some("ice"), "db")
                .expect("inject must succeed");

        assert_eq!(count, 1);
        assert_eq!(
            print_query(&query),
            "SELECT * FROM db.orders JOIN ice.db.customers FOR VERSION AS OF 99 ON TRUE"
        );
    }

    #[test]
    fn inject_pin_consumes_cte_definition_and_union_but_skips_cte_reference() {
        let mut query = parse_select_for_test(
            "WITH recent AS (SELECT * FROM ice.db.orders) SELECT * FROM recent UNION ALL SELECT * FROM ice.db.orders",
        );
        let pin = make_pin(&[
            (7, "ice.db.orders", 11, b"object-orders"),
            (42, "ice.db.orders", 22, b"object-orders"),
        ]);
        let delta_bearing = HashSet::new();

        let count =
            inject_pin_as_for_version_as_of(&mut query, &pin, &delta_bearing, Some("ice"), "db")
                .expect("inject must succeed");

        assert_eq!(count, 2);
        assert_eq!(
            print_query(&query),
            "WITH recent AS (SELECT * FROM ice.db.orders FOR VERSION AS OF 11) SELECT * FROM recent UNION ALL SELECT * FROM ice.db.orders FOR VERSION AS OF 22"
        );
    }

    #[test]
    fn inject_pin_preserves_repeated_locator_occurrences() {
        let mut query = parse_select_for_test(
            "SELECT l.id FROM ice.db.orders AS l JOIN ice.db.orders AS r ON l.id = r.id",
        );
        let pin = make_pin(&[
            (7, "ice.db.orders", 11, b"object-orders"),
            (42, "ice.db.orders", 22, b"object-orders"),
        ]);

        let count =
            inject_pin_as_for_version_as_of(&mut query, &pin, &HashSet::new(), Some("ice"), "db")
                .expect("inject must preserve both occurrences");

        let sql = print_query(&query);
        assert_eq!(count, 2);
        assert_eq!(sql.matches("VERSION AS OF 11").count(), 1, "{sql}");
        assert_eq!(sql.matches("VERSION AS OF 22").count(), 1, "{sql}");
    }

    #[test]
    fn inject_pin_rejects_existing_for_version_as_of() {
        let mut query = parse_select_for_test("SELECT * FROM ice.db.orders FOR VERSION AS OF 7");
        let pin = make_pin(&[(7, "ice.db.orders", 42, b"object-orders")]);
        let delta_bearing = HashSet::new();

        let err =
            inject_pin_as_for_version_as_of(&mut query, &pin, &delta_bearing, Some("ice"), "db")
                .expect_err("explicit version must be rejected");

        assert_eq!(
            err,
            "refresh SELECT must not write explicit FOR VERSION AS OF for occurrence 7 (ice.db.orders); refresh pin would conflict"
        );
    }

    #[test]
    fn inject_pin_rejects_delta_bearing_base_with_existing_for_version_as_of() {
        let mut query = parse_select_for_test("SELECT * FROM ice.db.orders FOR VERSION AS OF 7");
        let pin = make_pin(&[(7, "ice.db.orders", 42, b"object-orders")]);
        let delta_bearing = HashSet::from([SqlMvRelationOccurrenceId::new(7)]);

        let err =
            inject_pin_as_for_version_as_of(&mut query, &pin, &delta_bearing, Some("ice"), "db")
                .expect_err("explicit version on delta-bearing base must be rejected");

        assert_eq!(
            err,
            "refresh SELECT must not write explicit FOR VERSION AS OF for occurrence 7 (ice.db.orders); refresh pin would conflict"
        );
    }

    #[test]
    fn inject_pin_walks_nested_join() {
        let mut query = parse_select_for_test(
            "SELECT * FROM (SELECT * FROM ice.db.orders JOIN ice.db.customers ON TRUE) AS joined",
        );
        let pin = make_pin(&[
            (7, "ice.db.orders", 42, b"object-orders"),
            (42, "ice.db.customers", 99, b"object-customers"),
        ]);
        let delta_bearing = HashSet::from([SqlMvRelationOccurrenceId::new(7)]);

        let count =
            inject_pin_as_for_version_as_of(&mut query, &pin, &delta_bearing, Some("ice"), "db")
                .expect("inject must succeed");

        assert_eq!(count, 1);
        assert_eq!(
            print_query(&query),
            "SELECT * FROM (SELECT * FROM ice.db.orders JOIN ice.db.customers FOR VERSION AS OF 99 ON TRUE) AS joined"
        );
    }
}
