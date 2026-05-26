//! Refresh-scoped snapshot pin for iceberg-backed materialized views.
//!
//! `RefreshSnapshotPin` captures, at the start of a refresh, the
//! `current_snapshot_id` and `uuid` of every base table. The pin is the
//! single source of truth for snapshot ids during the refresh:
//!
//! * `plan_changes` uses pin[base] as its `to_snapshot_id`
//! * `begin_mv_refresh_intent` records pin as the refresh target
//! * `update_managed_mv_refresh_summary` writes `last_refresh_snapshots = pin`
//!
//! For single-base MVs (the only shape currently supported by the DDL gate),
//! this guarantees delta computation and bookkeeping agree on the same
//! snapshot, even if the base table commits concurrently during the refresh.
//!
//! For multi-base MVs (future), the pin additionally guarantees cross-table
//! consistency: every base table is read at the snapshot it had at refresh
//! start, regardless of intervening external commits.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use crate::connector::starrocks::managed::model::IcebergTableRef;
use crate::engine::StandaloneState;

/// Per-refresh snapshot pin: each base table is pinned to the
/// `current_snapshot_id` it had at refresh entry time.
#[allow(dead_code)]
#[derive(Clone, Debug, Default)]
pub(crate) struct RefreshSnapshotPin {
    snapshots: BTreeMap<String, i64>,
    table_uuids: BTreeMap<String, String>,
}

#[allow(dead_code)]
impl RefreshSnapshotPin {
    /// Capture the current snapshot id and uuid for each base table.
    ///
    /// Fails fast if any base table has no current snapshot - refresh
    /// against an empty iceberg table is not a supported flow at this
    /// layer; the caller is expected to handle that earlier.
    pub(crate) fn capture(
        state: &Arc<StandaloneState>,
        base_refs: &[IcebergTableRef],
    ) -> Result<Self, String> {
        let mut pin = RefreshSnapshotPin::default();
        for base_ref in base_refs {
            let loaded =
                crate::connector::starrocks::managed::mv_refresh::load_current_iceberg_base_table(
                    state, base_ref,
                )?;
            let snapshot_id = loaded
                .table
                .metadata()
                .current_snapshot()
                .map(|s| s.snapshot_id())
                .ok_or_else(|| {
                    format!(
                        "iceberg base table {} has no current snapshot; cannot freeze refresh pin",
                        base_ref.fqn()
                    )
                })?;
            pin.snapshots.insert(base_ref.fqn(), snapshot_id);
            pin.table_uuids
                .insert(base_ref.fqn(), loaded.table.metadata().uuid().to_string());
        }
        #[cfg(test)]
        invoke_after_capture_hook();
        Ok(pin)
    }

    pub(crate) fn get(&self, base: &IcebergTableRef) -> Option<i64> {
        self.snapshots.get(&base.fqn()).copied()
    }

    pub(crate) fn uuid(&self, base: &IcebergTableRef) -> Option<&str> {
        self.table_uuids.get(&base.fqn()).map(String::as_str)
    }

    pub(crate) fn len(&self) -> usize {
        self.snapshots.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.snapshots.is_empty()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&str, i64)> {
        self.snapshots.iter().map(|(k, v)| (k.as_str(), *v))
    }

    pub(crate) fn to_snapshot_map(&self) -> BTreeMap<String, i64> {
        self.snapshots.clone()
    }

    pub(crate) fn to_table_uuid_map(&self) -> BTreeMap<String, String> {
        self.table_uuids.clone()
    }
}

#[cfg(test)]
impl RefreshSnapshotPin {
    /// Build a pin with explicit entries; for use from other modules' unit
    /// tests that need to construct a `RefreshSnapshotPin` without going
    /// through `capture`. Each tuple is `(fqn, snapshot_id, table_uuid)`.
    pub(crate) fn from_entries_for_tests(entries: &[(&str, i64, &str)]) -> Self {
        let mut pin = RefreshSnapshotPin::default();
        for (fqn, snapshot_id, uuid) in entries {
            pin.snapshots.insert((*fqn).to_string(), *snapshot_id);
            pin.table_uuids
                .insert((*fqn).to_string(), (*uuid).to_string());
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
///   mutate_query_for_ivm_delta_scan in iceberg_refresh.rs).
/// - Table in pin and not in delta_bearing -> inject version.
///
/// In scope-B single-base MVs, the unique base is delta-bearing, so this
/// function is a no-op in production. It exists for the multi-base future.
#[allow(dead_code)]
pub(crate) fn inject_pin_as_for_version_as_of(
    query: &mut sqlparser::ast::Query,
    pin: &RefreshSnapshotPin,
    delta_bearing: &HashSet<IcebergTableRef>,
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
        for cte in &mut with.cte_tables {
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
    delta_bearing: &'a HashSet<IcebergTableRef>,
    current_catalog: Option<&'a str>,
    current_database: &'a str,
    count: usize,
    first_error: Option<String>,
}

fn walk_set_expr(expr: &mut sqlparser::ast::SetExpr, state: &mut InjectState<'_>) {
    use sqlparser::ast::SetExpr;
    if state.first_error.is_some() {
        return;
    }
    match expr {
        SetExpr::Select(select) => {
            for tw in &mut select.from {
                walk_table_with_joins(tw, state);
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            walk_set_expr(left.as_mut(), state);
            walk_set_expr(right.as_mut(), state);
        }
        SetExpr::Query(q) => walk_set_expr(q.body.as_mut(), state),
        _ => {}
    }
}

fn walk_table_with_joins(
    table_with_joins: &mut sqlparser::ast::TableWithJoins,
    state: &mut InjectState<'_>,
) {
    walk_factor(&mut table_with_joins.relation, state);
    for join in &mut table_with_joins.joins {
        walk_factor(&mut join.relation, state);
    }
}

fn walk_factor(factor: &mut sqlparser::ast::TableFactor, state: &mut InjectState<'_>) {
    use sqlparser::ast::{Expr, ObjectNamePart, TableFactor, TableVersion, Value};
    if state.first_error.is_some() {
        return;
    }
    match factor {
        TableFactor::Table {
            name,
            version,
            args,
            ..
        } => {
            // Skip table-valued functions (e.g. __nr_ivm_delta).
            if args.is_some() {
                return;
            }
            let parts: Vec<String> = name
                .0
                .iter()
                .filter_map(|p| match p {
                    ObjectNamePart::Identifier(i) => Some(i.value.to_ascii_lowercase()),
                    _ => None,
                })
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
            *version = Some(TableVersion::VersionAsOf(Expr::Value(
                Value::Number(pinned.to_string(), false).into(),
            )));
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
) -> Option<IcebergTableRef> {
    let current_database = current_database.to_ascii_lowercase();
    let current_catalog = current_catalog.map(|s| s.to_ascii_lowercase());
    match parts {
        [tbl] => current_catalog.map(|cat| IcebergTableRef {
            catalog: cat,
            namespace: current_database,
            table: tbl.clone(),
        }),
        [db, tbl] => current_catalog.map(|cat| IcebergTableRef {
            catalog: cat,
            namespace: db.clone(),
            table: tbl.clone(),
        }),
        [cat, db, tbl] => Some(IcebergTableRef {
            catalog: cat.clone(),
            namespace: db.clone(),
            table: tbl.clone(),
        }),
        _ => None,
    }
}

#[cfg(test)]
pub(crate) type AfterCaptureHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
struct AfterCaptureHookRegistration {
    owner: std::thread::ThreadId,
    hook: AfterCaptureHook,
}

#[cfg(test)]
pub(crate) fn lock_after_capture_hook_for_test() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .expect("after_capture_hook test lock")
}

#[cfg(test)]
fn after_capture_hook_slot() -> &'static std::sync::Mutex<Option<AfterCaptureHookRegistration>> {
    static HOOK: std::sync::OnceLock<std::sync::Mutex<Option<AfterCaptureHookRegistration>>> =
        std::sync::OnceLock::new();
    HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn invoke_after_capture_hook() {
    let current_thread = std::thread::current().id();
    let hook = after_capture_hook_slot()
        .lock()
        .expect("after_capture_hook lock")
        .as_ref()
        .and_then(|registration| {
            (registration.owner == current_thread).then(|| Arc::clone(&registration.hook))
        });
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
pub(crate) fn set_after_capture_hook(f: AfterCaptureHook) {
    *after_capture_hook_slot()
        .lock()
        .expect("after_capture_hook lock") = Some(AfterCaptureHookRegistration {
        owner: std::thread::current().id(),
        hook: f,
    });
}

#[cfg(test)]
pub(crate) fn clear_after_capture_hook() {
    *after_capture_hook_slot()
        .lock()
        .expect("after_capture_hook lock") = None;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_select_for_test(sql: &str) -> sqlparser::ast::Query {
        let statement = crate::sql::parser::parse_sql_raw(sql).expect("test SQL must parse");
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("test SQL must be a query");
        };
        *query
    }

    fn make_pin(entries: &[(&str, i64, &str)]) -> RefreshSnapshotPin {
        let mut pin = RefreshSnapshotPin::default();
        for (fqn, snapshot_id, uuid) in entries {
            pin.snapshots.insert((*fqn).to_string(), *snapshot_id);
            pin.table_uuids
                .insert((*fqn).to_string(), (*uuid).to_string());
        }
        pin
    }

    fn make_ref(c: &str, n: &str, t: &str) -> IcebergTableRef {
        IcebergTableRef {
            catalog: c.to_string(),
            namespace: n.to_string(),
            table: t.to_string(),
        }
    }

    #[test]
    fn pin_get_and_iter_use_fqn_keys() {
        let mut pin = RefreshSnapshotPin::default();
        pin.snapshots.insert("ice.db.a".to_string(), 10);
        pin.snapshots.insert("ice.db.b".to_string(), 20);
        pin.table_uuids
            .insert("ice.db.a".to_string(), "uuid-a".to_string());
        pin.table_uuids
            .insert("ice.db.b".to_string(), "uuid-b".to_string());
        let a = IcebergTableRef {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "a".to_string(),
        };
        assert_eq!(pin.get(&a), Some(10));
        assert_eq!(pin.uuid(&a), Some("uuid-a"));
        assert_eq!(pin.len(), 2);
        assert!(!pin.is_empty());

        let snapshot_map = pin.to_snapshot_map();
        assert_eq!(snapshot_map.get("ice.db.a"), Some(&10));
        assert_eq!(snapshot_map.get("ice.db.b"), Some(&20));
    }

    #[test]
    fn after_capture_hook_round_trip() {
        let _hook_lock = lock_after_capture_hook_for_test();
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag_for_hook = Arc::clone(&flag);
        set_after_capture_hook(Arc::new(move || {
            flag_for_hook.store(true, std::sync::atomic::Ordering::SeqCst);
        }));
        std::thread::spawn(invoke_after_capture_hook)
            .join()
            .expect("invoke hook from non-owner thread");
        assert!(!flag.load(std::sync::atomic::Ordering::SeqCst));
        invoke_after_capture_hook();
        assert!(flag.load(std::sync::atomic::Ordering::SeqCst));
        clear_after_capture_hook();
        flag.store(false, std::sync::atomic::Ordering::SeqCst);
        invoke_after_capture_hook();
        assert!(!flag.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn inject_pin_skips_delta_bearing_base() {
        let mut query = parse_select_for_test("SELECT * FROM ice.db.orders");
        let pin = make_pin(&[("ice.db.orders", 42, "uuid-orders")]);
        let delta_bearing = std::collections::HashSet::from([make_ref("ice", "db", "orders")]);

        let count =
            inject_pin_as_for_version_as_of(&mut query, &pin, &delta_bearing, Some("ice"), "db")
                .expect("inject must succeed");

        assert_eq!(count, 0);
        assert_eq!(query.to_string(), "SELECT * FROM ice.db.orders");
    }

    #[test]
    fn inject_pin_injects_non_delta_bearing_base() {
        let mut query =
            parse_select_for_test("SELECT * FROM db.orders JOIN ice.db.customers ON true");
        let pin = make_pin(&[
            ("ice.db.orders", 42, "uuid-orders"),
            ("ice.db.customers", 99, "uuid-customers"),
        ]);
        let delta_bearing = std::collections::HashSet::from([make_ref("ice", "db", "orders")]);

        let count =
            inject_pin_as_for_version_as_of(&mut query, &pin, &delta_bearing, Some("ice"), "db")
                .expect("inject must succeed");

        assert_eq!(count, 1);
        assert_eq!(
            query.to_string(),
            "SELECT * FROM db.orders JOIN ice.db.customers VERSION AS OF 99 ON true"
        );
    }

    #[test]
    fn inject_pin_skips_tables_not_in_pin() {
        let mut query = parse_select_for_test(
            "WITH recent AS (SELECT * FROM local_db.orders) SELECT * FROM recent JOIN other.db.dim ON true",
        );
        let pin = make_pin(&[("ice.db.orders", 42, "uuid-orders")]);
        let delta_bearing = std::collections::HashSet::new();

        let count =
            inject_pin_as_for_version_as_of(&mut query, &pin, &delta_bearing, Some("ice"), "db")
                .expect("inject must succeed");

        assert_eq!(count, 0);
        assert_eq!(
            query.to_string(),
            "WITH recent AS (SELECT * FROM local_db.orders) SELECT * FROM recent JOIN other.db.dim ON true"
        );
    }

    #[test]
    fn inject_pin_rejects_existing_for_version_as_of() {
        let mut query = parse_select_for_test("SELECT * FROM ice.db.orders VERSION AS OF 7");
        let pin = make_pin(&[("ice.db.orders", 42, "uuid-orders")]);
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
        let mut query = parse_select_for_test("SELECT * FROM ice.db.orders VERSION AS OF 7");
        let pin = make_pin(&[("ice.db.orders", 42, "uuid-orders")]);
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
        let mut query =
            parse_select_for_test("SELECT * FROM (ice.db.orders JOIN ice.db.customers ON true)");
        let pin = make_pin(&[
            ("ice.db.orders", 42, "uuid-orders"),
            ("ice.db.customers", 99, "uuid-customers"),
        ]);
        let delta_bearing = std::collections::HashSet::from([make_ref("ice", "db", "orders")]);

        let count =
            inject_pin_as_for_version_as_of(&mut query, &pin, &delta_bearing, Some("ice"), "db")
                .expect("inject must succeed");

        assert_eq!(count, 1);
        assert_eq!(
            query.to_string(),
            "SELECT * FROM (ice.db.orders JOIN ice.db.customers VERSION AS OF 99 ON true)"
        );
    }

    #[test]
    fn inject_pin_skips_table_valued_functions() {
        let mut query = parse_select_for_test("SELECT * FROM __nr_ivm_delta('ice.db.orders')");
        let pin = make_pin(&[("ice.db.orders", 42, "uuid-orders")]);
        let delta_bearing = std::collections::HashSet::new();

        let count =
            inject_pin_as_for_version_as_of(&mut query, &pin, &delta_bearing, Some("ice"), "db")
                .expect("inject must succeed");

        assert_eq!(count, 0);
        assert_eq!(
            query.to_string(),
            "SELECT * FROM __nr_ivm_delta('ice.db.orders')"
        );
    }
}
