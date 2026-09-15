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
//! `RefreshSnapshotPin` stores, for one refresh, the
//! `current_snapshot_id` and opaque object ID of every base table. The pin is the
//! single source of truth for snapshot ids during the refresh:
//!
//! * provider change-window planning uses pin[base] as its `to_snapshot_id`
//! * `begin_mv_refresh_intent` records pin as the refresh target
//! * `update_starrocks_mv_refresh_summary` writes `last_refresh_snapshots = pin`
//!
//! For single-base MVs (the only shape currently supported by the DDL gate),
//! this guarantees delta computation and bookkeeping agree on the same
//! snapshot, even if the base table commits concurrently during the refresh.
//!
//! For multi-base MVs (future), the pin additionally guarantees cross-table
//! consistency: every base table is read at the snapshot it had at refresh
//! start, regardless of intervening external commits.

use std::collections::{BTreeMap, HashSet};

use novarocks_mv_application::persistence::definition::StoredMvDefinition;
use novarocks_spi::connector::ConnectorTableObjectId;
use novarocks_types::naming::TableIdentity;

/// Per-refresh snapshot pin: each base table is pinned to the
/// `current_snapshot_id` it had at refresh entry time.
#[allow(dead_code)]
#[derive(Clone, Debug, Default)]
pub struct RefreshSnapshotPin {
    snapshots: BTreeMap<String, i64>,
    table_object_ids: BTreeMap<String, ConnectorTableObjectId>,
}

#[allow(dead_code)]
impl RefreshSnapshotPin {
    pub fn from_captured_entries(
        entries: impl IntoIterator<Item = (TableIdentity, i64, ConnectorTableObjectId)>,
    ) -> Self {
        let mut pin = RefreshSnapshotPin::default();
        for (table, snapshot_id, object_id) in entries {
            let fqn = table.fqn();
            pin.snapshots.insert(fqn.clone(), snapshot_id);
            pin.table_object_ids.insert(fqn, object_id);
        }
        pin
    }

    pub fn get(&self, base: &TableIdentity) -> Option<i64> {
        self.snapshots.get(&base.fqn()).copied()
    }

    pub fn object_id(&self, base: &TableIdentity) -> Option<&ConnectorTableObjectId> {
        self.table_object_ids.get(&base.fqn())
    }

    pub fn len(&self) -> usize {
        self.snapshots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.snapshots.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, i64)> {
        self.snapshots.iter().map(|(k, v)| (k.as_str(), *v))
    }

    pub fn to_snapshot_map(&self) -> BTreeMap<String, i64> {
        self.snapshots.clone()
    }

    pub fn to_table_object_id_map(&self) -> BTreeMap<String, ConnectorTableObjectId> {
        self.table_object_ids.clone()
    }
}

/// Rejects a refresh when a persisted base-table identity no longer matches
/// the identity frozen in this attempt's pin.
pub fn validate_refresh_pin_table_object_ids(
    mv_definition: &StoredMvDefinition,
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

fn validate_refresh_pin_table_object_ids_for_operation(
    mv_definition: &StoredMvDefinition,
    pin: &RefreshSnapshotPin,
    base_refs: &[TableIdentity],
    unsafe_message: &str,
) -> Result<(), String> {
    for base_ref in base_refs {
        let Some(previous_object_id) = mv_definition
            .last_refresh_table_object_ids
            .get(&base_ref.fqn())
        else {
            continue;
        };
        let current_object_id = pin.object_id(base_ref).ok_or_else(|| {
            format!(
                "refresh pin missing object ID for base {} (this should not happen)",
                base_ref.fqn()
            )
        })?;
        if previous_object_id != current_object_id {
            return Err(format!(
                "iceberg MV base table identity changed for {}; {unsafe_message}",
                base_ref.fqn(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
impl RefreshSnapshotPin {
    /// Build a pin with explicit entries; for use from other modules' unit
    /// tests that need to construct a `RefreshSnapshotPin` without going
    /// through `capture`. Each tuple is `(fqn, snapshot_id, object_id_bytes)`.
    pub(crate) fn from_entries_for_tests(entries: &[(&str, i64, &[u8])]) -> Self {
        let mut pin = RefreshSnapshotPin::default();
        for (fqn, snapshot_id, object_id) in entries {
            pin.snapshots.insert((*fqn).to_string(), *snapshot_id);
            pin.table_object_ids.insert(
                (*fqn).to_string(),
                ConnectorTableObjectId::try_new(bytes::Bytes::copy_from_slice(object_id))
                    .expect("test object ID is bounded"),
            );
        }
        pin
    }
}

/// Walk `query` in place. For each `TableFactor::Table` whose 3-part name
/// resolves into the pin and is not in `delta_bearing`, set
/// `version = Some(VersionAsOf(Number(pin[base])))`. Returns the number
/// of mutations performed.
///
/// Rules:
/// - `TableFactor::Table` with `version = Some(_)` already -> Err. The
///   refresh SELECT is not allowed to combine user-written FOR VERSION AS OF
///   with refresh pinning.
/// - Table not in pin -> unchanged (likely a CTE, a different catalog, or
///   an alias not addressed by base_refs).
/// - Table in pin and in delta_bearing -> unchanged (handled by
///   the rewrite-path incremental refresh in iceberg_refresh.rs).
/// - Table in pin and not in delta_bearing -> inject version.
///
/// In scope-B single-base MVs, the unique base is delta-bearing, so this
/// function is a no-op in production. It exists for the multi-base future.
#[allow(dead_code)]
pub(crate) fn inject_pin_as_for_version_as_of(
    query: &mut novarocks_parser::ast::Query,
    pin: &RefreshSnapshotPin,
    delta_bearing: &HashSet<TableIdentity>,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<usize, String> {
    let mut state = InjectState {
        pin,
        delta_bearing,
        current_catalog,
        current_database,
        count: 0,
        first_error: None,
    };
    if let Some(with) = &mut query.with {
        for cte in &mut with.ctes {
            walk_set_expr(cte.query.body.as_mut(), &mut state);
        }
    }
    walk_set_expr(query.body.as_mut(), &mut state);
    if let Some(err) = state.first_error {
        return Err(err);
    }
    Ok(state.count)
}

struct InjectState<'a> {
    pin: &'a RefreshSnapshotPin,
    delta_bearing: &'a HashSet<TableIdentity>,
    current_catalog: Option<&'a str>,
    current_database: &'a str,
    count: usize,
    first_error: Option<String>,
}

fn walk_set_expr(expr: &mut novarocks_parser::ast::SetExpr, state: &mut InjectState<'_>) {
    use novarocks_parser::ast::SetExpr;
    if state.first_error.is_some() {
        return;
    }
    match expr {
        SetExpr::Select(select) => {
            for tw in &mut select.from {
                walk_table_with_joins(tw, state);
            }
        }
        novarocks_parser::ast::SetExpr::SetOperation(operation) => {
            walk_set_expr(operation.left.as_mut(), state);
            walk_set_expr(operation.right.as_mut(), state);
        }
        SetExpr::Query(q) => walk_set_expr(q.body.as_mut(), state),
        _ => {}
    }
}

fn walk_table_with_joins(
    table_with_joins: &mut novarocks_parser::ast::TableWithJoins,
    state: &mut InjectState<'_>,
) {
    walk_factor(&mut table_with_joins.relation, state);
    for join in &mut table_with_joins.joins {
        walk_factor(&mut join.relation, state);
    }
}

fn walk_factor(factor: &mut novarocks_parser::ast::TableFactor, state: &mut InjectState<'_>) {
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
            let Some(base_ref) =
                resolve_table_factor(&parts, state.current_catalog, state.current_database)
            else {
                return;
            };
            let Some(pinned) = state.pin.get(&base_ref) else {
                return;
            };
            if version.is_some() {
                state.first_error = Some(format!(
                    "refresh SELECT must not write explicit FOR VERSION AS OF for base table {}; refresh pin would conflict",
                    base_ref.fqn()
                ));
                return;
            }
            if state.delta_bearing.contains(&base_ref) {
                return;
            }
            *version = Some(TableVersion {
                kind: TableVersionKind::ForVersionAsOf,
                value: Expr::Literal(Literal {
                    kind: LiteralKind::Number(pinned.to_string()),
                    span: name.span,
                }),
                span: name.span,
            });
            state.count += 1;
        }
        TableFactor::Derived { subquery, .. } => {
            walk_set_expr(subquery.body.as_mut(), state);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            walk_table_with_joins(table_with_joins.as_mut(), state);
        }
        _ => {}
    }
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
    use novarocks_parser::printer::print_query;

    fn parse_select_for_test(sql: &str) -> novarocks_parser::ast::Query {
        let statements = novarocks_parser::parse(sql).expect("test SQL must parse");
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("test SQL must be a query");
        };
        query.clone()
    }

    fn make_pin(entries: &[(&str, i64, &[u8])]) -> RefreshSnapshotPin {
        let mut pin = RefreshSnapshotPin::default();
        for (fqn, snapshot_id, object_id) in entries {
            pin.snapshots.insert((*fqn).to_string(), *snapshot_id);
            pin.table_object_ids.insert(
                (*fqn).to_string(),
                ConnectorTableObjectId::try_new(bytes::Bytes::copy_from_slice(object_id))
                    .expect("test object ID"),
            );
        }
        pin
    }

    fn make_ref(c: &str, n: &str, t: &str) -> TableIdentity {
        TableIdentity {
            catalog: c.to_string(),
            namespace: n.to_string(),
            table: t.to_string(),
        }
    }

    #[test]
    fn pin_get_and_iter_use_fqn_keys() {
        let mixed_case = make_ref("IceCase", "DbName", "Orders");
        let lowercase = make_ref("alpha", "db", "customers");
        let pin = RefreshSnapshotPin::from_captured_entries([
            (
                lowercase.clone(),
                20,
                ConnectorTableObjectId::try_new(bytes::Bytes::from_static(b"object-alpha"))
                    .expect("test object ID"),
            ),
            (
                mixed_case.clone(),
                10,
                ConnectorTableObjectId::try_new(bytes::Bytes::from_static(b"object-old"))
                    .expect("test object ID"),
            ),
            (
                mixed_case.clone(),
                30,
                ConnectorTableObjectId::try_new(bytes::Bytes::from_static(b"object-new"))
                    .expect("test object ID"),
            ),
        ]);

        assert_eq!(pin.get(&mixed_case), Some(30));
        assert_eq!(
            pin.object_id(&mixed_case).map(|id| id.as_bytes().as_ref()),
            Some(b"object-new".as_ref())
        );
        assert_eq!(pin.get(&lowercase), Some(20));
        assert_eq!(pin.len(), 2);
        assert!(!pin.is_empty());

        assert_eq!(
            pin.iter().collect::<Vec<_>>(),
            vec![("IceCase.DbName.Orders", 30), ("alpha.db.customers", 20)]
        );
        assert_eq!(
            pin.to_snapshot_map().into_iter().collect::<Vec<_>>(),
            vec![
                ("IceCase.DbName.Orders".to_string(), 30),
                ("alpha.db.customers".to_string(), 20),
            ]
        );
        assert_eq!(
            pin.to_table_object_id_map()
                .into_iter()
                .map(|(fqn, object_id)| (fqn, object_id.as_bytes().to_vec()))
                .collect::<Vec<_>>(),
            vec![
                ("IceCase.DbName.Orders".to_string(), b"object-new".to_vec(),),
                ("alpha.db.customers".to_string(), b"object-alpha".to_vec()),
            ]
        );
    }

    #[test]
    fn inject_pin_skips_delta_bearing_base() {
        let mut query = parse_select_for_test("SELECT * FROM ice.db.orders");
        let pin = make_pin(&[("ice.db.orders", 42, b"object-orders")]);
        let delta_bearing = std::collections::HashSet::from([make_ref("ice", "db", "orders")]);

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
            ("ice.db.orders", 42, b"object-orders"),
            ("ice.db.customers", 99, b"object-customers"),
        ]);
        let delta_bearing = std::collections::HashSet::from([make_ref("ice", "db", "orders")]);

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
    fn inject_pin_skips_tables_not_in_pin() {
        let mut query = parse_select_for_test(
            "WITH recent AS (SELECT * FROM local_db.orders) SELECT * FROM recent JOIN other.db.dim ON true",
        );
        let pin = make_pin(&[("ice.db.orders", 42, b"object-orders")]);
        let delta_bearing = std::collections::HashSet::new();

        let count =
            inject_pin_as_for_version_as_of(&mut query, &pin, &delta_bearing, Some("ice"), "db")
                .expect("inject must succeed");

        assert_eq!(count, 0);
        assert_eq!(
            print_query(&query),
            "WITH recent AS (SELECT * FROM local_db.orders) SELECT * FROM recent JOIN other.db.dim ON TRUE"
        );
    }

    #[test]
    fn inject_pin_rejects_existing_for_version_as_of() {
        let mut query = parse_select_for_test("SELECT * FROM ice.db.orders FOR VERSION AS OF 7");
        let pin = make_pin(&[("ice.db.orders", 42, b"object-orders")]);
        let delta_bearing = std::collections::HashSet::new();

        let err =
            inject_pin_as_for_version_as_of(&mut query, &pin, &delta_bearing, Some("ice"), "db")
                .expect_err("explicit version must be rejected");

        assert_eq!(
            err,
            "refresh SELECT must not write explicit FOR VERSION AS OF for base table ice.db.orders; refresh pin would conflict"
        );
    }

    #[test]
    fn inject_pin_rejects_delta_bearing_base_with_existing_for_version_as_of() {
        let mut query = parse_select_for_test("SELECT * FROM ice.db.orders FOR VERSION AS OF 7");
        let pin = make_pin(&[("ice.db.orders", 42, b"object-orders")]);
        let delta_bearing = std::collections::HashSet::from([make_ref("ice", "db", "orders")]);

        let err =
            inject_pin_as_for_version_as_of(&mut query, &pin, &delta_bearing, Some("ice"), "db")
                .expect_err("explicit version on delta-bearing base must be rejected");

        assert_eq!(
            err,
            "refresh SELECT must not write explicit FOR VERSION AS OF for base table ice.db.orders; refresh pin would conflict"
        );
    }

    #[test]
    fn inject_pin_walks_nested_join() {
        let mut query = parse_select_for_test(
            "SELECT * FROM (SELECT * FROM ice.db.orders JOIN ice.db.customers ON TRUE) AS joined",
        );
        let pin = make_pin(&[
            ("ice.db.orders", 42, b"object-orders"),
            ("ice.db.customers", 99, b"object-customers"),
        ]);
        let delta_bearing = std::collections::HashSet::from([make_ref("ice", "db", "orders")]);

        let count =
            inject_pin_as_for_version_as_of(&mut query, &pin, &delta_bearing, Some("ice"), "db")
                .expect("inject must succeed");

        assert_eq!(count, 1);
        assert_eq!(
            print_query(&query),
            "SELECT * FROM (SELECT * FROM ice.db.orders JOIN ice.db.customers FOR VERSION AS OF 99 ON TRUE) AS joined"
        );
    }

    #[test]
    fn inject_pin_skips_table_valued_functions() {
        let mut query = parse_select_for_test("SELECT * FROM __nr_ivm_delta('ice.db.orders')");
        let pin = make_pin(&[("ice.db.orders", 42, b"object-orders")]);
        let delta_bearing = std::collections::HashSet::new();

        let count =
            inject_pin_as_for_version_as_of(&mut query, &pin, &delta_bearing, Some("ice"), "db")
                .expect("inject must succeed");

        assert_eq!(count, 0);
        assert_eq!(
            print_query(&query),
            "SELECT * FROM __nr_ivm_delta('ice.db.orders')"
        );
    }
}
