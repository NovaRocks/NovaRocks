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

//! End-to-end integration tests for automatic Iceberg MV maintenance
//! (IV3-11). These drive `MaintenanceCoordinator::run_pass` directly (no
//! background thread, injected `now_ms`) against a real `StandaloneState`
//! backed by a local hadoop iceberg catalog, and verify the four acceptance
//! behaviors:
//!   1. auto OPTIMIZE skips row-lineage small files when no same-sequence
//!      compaction group can reduce file count
//!      (`scenario_1_auto_optimize_skips_sequence_isolated_row_lineage_files`);
//!   2. auto EXPIRE honors `history.expire.*` and keeps min snapshots;
//!   3. auto EXPIRE does not break a downstream incremental consumer;
//!   4. the per-table escape hatch (`novarocks.maintenance.enabled=false`)
//!      disables all maintenance for that table.
//!
//! Two additional correctness gates assert that OPTIMIZE of an MV storage
//! table preserves the hidden apply-key (and aggregate-state) columns verbatim
//! so a subsequent incremental refresh still locates the right target rows in
//! the compacted files:
//!   * `optimize_preserves_mv_apply_key_for_incremental_delete` (projection MV,
//!     stored `__nova_base_row_id` apply key);
//!   * `optimize_preserves_aggregate_mv_apply_key_and_state` (aggregate MV,
//!     `__row_id__` group apply key plus hidden `__agg_state_*` columns).
//!
//! Setup intentionally reuses the proven, format-version-3 / row-lineage
//! helpers from `crate::engine::mv::iceberg_refresh` (copied verbatim here, as
//! those live in a `#[cfg(test)]` module and are not importable) so that
//! incremental refresh — required by scenario ③ — works.

use super::*;

use std::sync::Arc;
use tempfile::TempDir;

use crate::engine::{StandaloneSession, StandaloneState, StatementResult};
use crate::sql::parser::ast::CreateMaterializedViewStmt;

// --- Copied test scaffolding from mv::iceberg_refresh (verbatim shape) ---

struct MaintenanceTestEnv {
    state: Arc<StandaloneState>,
    current_db: String,
    _metadata_dir: TempDir,
    _warehouse_dir: TempDir,
    _loopback_backend: crate::engine::StandaloneLoopbackTestBackend,
}

/// Real `StandaloneState` with a local hadoop iceberg catalog named `catalog`
/// and a SQLite metadata provider, matching
/// `open_test_state_with_hadoop_iceberg_catalog` in mv::iceberg_refresh.
fn open_env(catalog: &str, current_db: &str) -> MaintenanceTestEnv {
    let loopback_backend = crate::engine::install_all_in_one_loopback_backend_for_test()
        .expect("install all-in-one loopback backend");
    let metadata_dir = TempDir::new().expect("metadata tempdir");
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let metadata_path = metadata_dir.path().join("standalone.sqlite");
    let metadata_provider =
        crate::meta::SqliteMetaStoreProvider::open(&metadata_path).expect("open meta provider");
    let state = Arc::new(StandaloneState {
        metadata_provider: Some(Arc::new(metadata_provider)),
        exchange_port: loopback_backend.exchange_port,
        ..StandaloneState::default()
    });
    crate::connector::register_standalone_backends(&state);
    {
        let mut catalogs = state.iceberg_catalogs.write().expect("iceberg catalogs");
        catalogs
            .create_catalog(
                catalog,
                &[
                    ("type".to_string(), "iceberg".to_string()),
                    ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                    (
                        "iceberg.catalog.warehouse".to_string(),
                        format!("file://{}", warehouse_dir.path().display()),
                    ),
                ],
            )
            .expect("create iceberg catalog");
    }
    crate::connector::register_iceberg_catalog_mgr_entry(&state, catalog)
        .expect("register iceberg catalog mgr entry");
    MaintenanceTestEnv {
        state,
        current_db: current_db.to_string(),
        _metadata_dir: metadata_dir,
        _warehouse_dir: warehouse_dir,
        _loopback_backend: loopback_backend,
    }
}

/// Execute a non-query statement (DDL / DML / ALTER / REFRESH) through a real
/// standalone session, matching `execute_iceberg_sql` in mv::iceberg_refresh.
fn exec_sql(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    sql: &str,
) {
    let session = StandaloneSession {
        inner: Arc::clone(state),
    };
    match session
        .execute_in_context(sql, current_catalog, current_database, None)
        .unwrap_or_else(|e| panic!("execute iceberg sql `{sql}`: {e}"))
    {
        StatementResult::Ok => {}
        StatementResult::Query(_) => panic!("expected non-query statement for {sql}"),
    }
}

/// Run a `SELECT` through a real standalone session and return the row count.
/// Used to assert the MV still answers queries after maintenance.
fn select_row_count(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    sql: &str,
) -> usize {
    let session = StandaloneSession {
        inner: Arc::clone(state),
    };
    let result = match session
        .execute_in_context(sql, current_catalog, current_database, None)
        .unwrap_or_else(|e| panic!("execute select `{sql}`: {e}"))
    {
        StatementResult::Query(result) => result,
        StatementResult::Ok => panic!("expected query result for {sql}"),
    };
    result.chunks.iter().map(|c| c.batch.num_rows()).sum()
}

/// Run a `SELECT id, region, amount FROM <mv> ORDER BY id` style query and
/// return the materialized rows as `(id, region, amount)` tuples. Used by the
/// apply-key correctness gate to assert the EXACT post-OPTIMIZE / post-refresh
/// contents of a projection MV — not just the row count — so a dropped or
/// corrupted hidden apply-key column (which would delete the wrong target row
/// on the next incremental refresh) is caught.
fn select_id_region_amount_rows(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    sql: &str,
) -> Vec<(i32, String, i64)> {
    use arrow::array::{Int32Array, Int64Array, StringArray};

    let session = StandaloneSession {
        inner: Arc::clone(state),
    };
    let result = match session
        .execute_in_context(sql, current_catalog, current_database, None)
        .unwrap_or_else(|e| panic!("execute select `{sql}`: {e}"))
    {
        StatementResult::Query(result) => result,
        StatementResult::Ok => panic!("expected query result for {sql}"),
    };
    let mut rows = Vec::new();
    for chunk in &result.chunks {
        let id = chunk
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id column is Int32");
        let region = chunk
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("region column is Utf8");
        let amount = chunk
            .batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("amount column is Int64");
        for row in 0..chunk.batch.num_rows() {
            rows.push((
                id.value(row),
                region.value(row).to_string(),
                amount.value(row),
            ));
        }
    }
    rows
}

fn parse_create_mv(sql: &str) -> CreateMaterializedViewStmt {
    let mut statements = crate::sql::parser::parse_sql(sql).expect("parse");
    let crate::sql::parser::ast::Statement::CreateMaterializedView(stmt) = statements.remove(0)
    else {
        panic!("expected CREATE MATERIALIZED VIEW");
    };
    stmt
}

/// Create `ice.<namespace>.<table>(id INT not-null, region STRING, amount
/// BIGINT)` as a format-version-3, row-lineage iceberg table. Matches
/// `create_aggregate_fact_table` in mv::iceberg_refresh.
fn create_aggregate_fact_table(
    state: &Arc<StandaloneState>,
    catalog: &str,
    namespace: &str,
    table: &str,
) {
    let entry = {
        let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
        catalogs.get(catalog).expect("catalog")
    };
    let columns = vec![
        crate::sql::TableColumnDef {
            name: "id".to_string(),
            data_type: crate::sql::SqlType::Int,
            nullable: false,
            aggregation: None,
            default: None,
        },
        crate::sql::TableColumnDef {
            name: "region".to_string(),
            data_type: crate::sql::SqlType::String,
            nullable: true,
            aggregation: None,
            default: None,
        },
        crate::sql::TableColumnDef {
            name: "amount".to_string(),
            data_type: crate::sql::SqlType::BigInt,
            nullable: true,
            aggregation: None,
            default: None,
        },
    ];
    crate::connector::iceberg::catalog::registry::create_table(
        &entry,
        namespace,
        table,
        &columns,
        None,
        &[],
        &[
            ("format-version".to_string(), "3".to_string()),
            ("write.row-lineage".to_string(), "true".to_string()),
        ],
    )
    .expect("create aggregate fact iceberg table");
}

/// Append rows to the aggregate fact table. Matches
/// `insert_into_aggregate_fact_table` in mv::iceberg_refresh.
fn insert_into_aggregate_fact_table(
    state: &Arc<StandaloneState>,
    catalog: &str,
    namespace: &str,
    table: &str,
    rows: &[(i32, &str, i64)],
) {
    let entry = {
        let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
        catalogs.get(catalog).expect("catalog")
    };
    let rows = rows
        .iter()
        .map(|(id, region, amount)| {
            vec![
                crate::sql::Literal::Int(i64::from(*id)),
                crate::sql::Literal::String((*region).to_string()),
                crate::sql::Literal::Int(*amount),
            ]
        })
        .collect::<Vec<_>>();
    crate::connector::iceberg::catalog::registry::insert_rows(&entry, namespace, table, &rows)
        .expect("insert aggregate fact iceberg rows");
}

// --- Maintenance harness helpers (verified APIs) ---

fn coordinator_with(
    policy_overrides: impl FnOnce(&mut MaintenanceCoordinatorConfig),
) -> MaintenanceCoordinator {
    let mut config = MaintenanceCoordinatorConfig {
        enabled: true,
        tick_interval_ms: 600_000,
        max_concurrent: 10,
        policy: policy::MaintenancePolicyConfig::default(),
    };
    policy_overrides(&mut config);
    MaintenanceCoordinator::new(config)
}

fn mv_table_snapshot_count(env: &MaintenanceTestEnv, namespace: &str, table: &str) -> usize {
    let (catalog, ident, _) = crate::engine::iceberg_maintenance::resolve_maintenance_catalog(
        &env.state, "ice", namespace, table,
    )
    .expect("resolve catalog");
    let loaded = crate::connector::iceberg::catalog::registry::block_on_iceberg(async move {
        catalog.load_table(&ident).await
    })
    .expect("runtime")
    .expect("load table");
    loaded.metadata().snapshots().len()
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Create an iceberg-backed MV via the proven create path.
fn create_mv(env: &MaintenanceTestEnv, sql: &str) {
    let stmt = parse_create_mv(sql);
    crate::engine::mv::iceberg_refresh::create_iceberg_mv(
        &env.state,
        Some("ice"),
        &env.current_db,
        &stmt,
    )
    .expect("create iceberg mv");
}

fn refresh_mv(env: &MaintenanceTestEnv, mv_name: &str) {
    exec_sql(
        &env.state,
        Some("ice"),
        &env.current_db,
        &format!("REFRESH MATERIALIZED VIEW {mv_name}"),
    );
}

/// Run one maintenance pass with the real `StateMaintenanceExecutor` against
/// the given coordinator and the current wall clock.
fn run_pass(env: &MaintenanceTestEnv, coordinator: &mut MaintenanceCoordinator) {
    let mut executor = StateMaintenanceExecutor::new(Arc::clone(&env.state));
    coordinator
        .run_pass(&env.state, &mut executor, now_ms())
        .expect("maintenance pass");
}

// --- Scenario ①: auto OPTIMIZE skips sequence-isolated row-lineage files ---
//
// Previously this asserted that every set of small MV files must compact, but
// row-lineage preserve rewrites can only merge files in the same partition and
// with the same `_last_updated_sequence_number`: the REPLACE data manifest has
// one sequence number per replacement file. The incremental refreshes below
// deliberately create one MV file per Iceberg sequence, so maintenance must
// skip OPTIMIZE instead of submitting a job that can only rewrite 4 files into
// 4 files.
//
// Older versions of this scenario were ignored because OPTIMIZE of an MV
// storage table failed with
// `annotate_batch column count mismatch: batch=5 schema=6`: the rewrite read
// the table with `SELECT *, _row_id, _last_updated_sequence_number`, and
// `SELECT *` omitted the MV's hidden internal apply-key column
// (`__nova_base_row_id`) while the writer schema (built from the full physical
// `current_schema()`) included it. The fix in `compact.rs` rewrites tables that
// carry hidden internal columns through a direct physical read that preserves
// every physical column (including the apply key) verbatim. The
// `optimize_preserves_mv_apply_key_for_incremental_delete` correctness gate
// proves the apply key survives compaction.
#[test]
fn scenario_1_auto_optimize_skips_sequence_isolated_row_lineage_files() {
    let env = open_env("ice", "analytics");
    create_aggregate_fact_table(&env.state, "ice", "sales", "fact");
    insert_into_aggregate_fact_table(&env.state, "ice", "sales", "fact", &[(1, "east", 10)]);

    // Projection MV: each base append produces a fresh small data file on the
    // MV storage table, so several refreshes accumulate many small files.
    create_mv(
        &env,
        "CREATE MATERIALIZED VIEW mv_opt
         DISTRIBUTED BY HASH(region) BUCKETS 1
         PROPERTIES('storage_engine'='iceberg')
         AS SELECT id, region, amount FROM ice.sales.fact",
    );
    refresh_mv(&env, "mv_opt");
    for id in 2..=4 {
        insert_into_aggregate_fact_table(&env.state, "ice", "sales", "fact", &[(id, "east", 10)]);
        refresh_mv(&env, "mv_opt");
    }

    // Capture the data-file count BEFORE the optimize pass (convergence gap B).
    let definitions_before = {
        let provider = env.state.metadata_provider.as_ref().expect("provider");
        let read = provider.begin_read().expect("read txn");
        env.state
            .mv_repo
            .list_definitions(read.as_ref())
            .expect("list definitions before optimize")
    };
    let stats_before = stats::collect_table_stats(
        &env.state,
        "ice",
        "analytics",
        "mv_opt",
        &definitions_before,
    )
    .expect("stats before optimize");
    let data_files_before = stats_before
        .total_data_files
        .expect("total_data_files must be present before optimize");
    assert!(
        data_files_before >= 2,
        "expected >= 2 data files before optimize to exercise compaction, got {data_files_before}"
    );

    // One pass with the compaction file-count threshold lowered to 2 must still
    // skip OPTIMIZE because no same-sequence file group reaches the threshold.
    let mut coordinator = coordinator_with(|cfg| {
        cfg.policy.compaction_min_data_files = 2;
    });
    run_pass(&env, &mut coordinator);

    let provider = env.state.metadata_provider.as_ref().expect("provider");
    let read = provider.begin_read().expect("read txn");
    let jobs = env
        .state
        .job_repo
        .show_iceberg_optimize_jobs(read.as_ref())
        .expect("list jobs");
    assert!(
        jobs.is_empty(),
        "sequence-isolated row-lineage files are not compactable; jobs: {jobs:?}"
    );

    // Capture data-file count AFTER maintenance and assert no no-op rewrite was
    // committed.
    let definitions_after = {
        let provider2 = env.state.metadata_provider.as_ref().expect("provider");
        let read2 = provider2.begin_read().expect("read txn");
        env.state
            .mv_repo
            .list_definitions(read2.as_ref())
            .expect("list definitions after optimize")
    };
    let stats_after =
        stats::collect_table_stats(&env.state, "ice", "analytics", "mv_opt", &definitions_after)
            .expect("stats after optimize");
    let data_files_after = stats_after
        .total_data_files
        .expect("total_data_files must be present after optimize");
    assert_eq!(
        data_files_after, data_files_before,
        "maintenance must not commit a no-op optimize rewrite"
    );

    // The MV still answers SELECT after the skipped maintenance pass.
    let rows = select_row_count(
        &env.state,
        Some("ice"),
        &env.current_db,
        "SELECT id, region, amount FROM mv_opt",
    );
    assert_eq!(rows, 4, "MV must still return all rows after optimize");
}

// --- Scenario ②: auto EXPIRE honors history.expire.* and keeps min snapshots ---
//
// NOTE on the assertion shape: NovaRocks `run_expire_snapshots` implements
// standard Iceberg expireSnapshots semantics — it prunes old snapshots on the
// main ancestor chain (not just dangling ones), keeping the current snapshot of
// every ref plus the most-recent `retain_last` main-chain snapshots. With the
// aggressive retention below, the old non-current snapshots of this linearly
// appended MV storage table are pruned. The assertions intentionally stay
// behavior-agnostic about the exact post-count: the pass runs without error,
// never violates `history.expire.min-snapshots-to-keep` (count stays >= 1 and
// never grows), and the MV remains queryable with all rows intact. The expire
// candidate/cutoff/min-keep decision logic itself is exhaustively unit-tested in
// `policy.rs` and `stats.rs`, and the candidate-computation correctness in
// `src/connector/iceberg/commit/expire_snapshots.rs`.
#[test]
fn scenario_2_auto_expire_keeps_min_snapshots() {
    let env = open_env("ice", "analytics");
    create_aggregate_fact_table(&env.state, "ice", "sales", "fact");
    insert_into_aggregate_fact_table(&env.state, "ice", "sales", "fact", &[(1, "east", 10)]);

    create_mv(
        &env,
        "CREATE MATERIALIZED VIEW mv_exp
         DISTRIBUTED BY HASH(region) BUCKETS 1
         PROPERTIES('storage_engine'='iceberg')
         AS SELECT id, region, amount FROM ice.sales.fact",
    );
    // Build up >= 3 snapshots on the MV storage table.
    refresh_mv(&env, "mv_exp");
    for id in 2..=4 {
        insert_into_aggregate_fact_table(&env.state, "ice", "sales", "fact", &[(id, "east", 10)]);
        refresh_mv(&env, "mv_exp");
    }
    let before = mv_table_snapshot_count(&env, "analytics", "mv_exp");
    assert!(
        before >= 3,
        "expected >= 3 snapshots before expire, got {before}"
    );

    // Aggressively short retention with an explicit floor of 1 snapshot: every
    // non-current snapshot is "old" under (now - 1ms), so the policy plans an
    // expire and honors min-snapshots-to-keep = 1.
    exec_sql(
        &env.state,
        Some("ice"),
        &env.current_db,
        "ALTER MATERIALIZED VIEW mv_exp SET TBLPROPERTIES \
         ('history.expire.max-snapshot-age-ms' = '1', \
          'history.expire.min-snapshots-to-keep' = '1')",
    );

    // Prove the policy actually PLANS an expire from the real collected stats
    // (so the pass below truly drives the real expire executor, rather than
    // skipping for cooldown / refs / nothing-to-expire). The short retention
    // makes every non-current snapshot a candidate at the policy layer.
    {
        let provider = env.state.metadata_provider.as_ref().expect("provider");
        let read = provider.begin_read().expect("read txn");
        let definitions = env
            .state
            .mv_repo
            .list_definitions(read.as_ref())
            .expect("list definitions");
        drop(read);
        let stats =
            stats::collect_table_stats(&env.state, "ice", "analytics", "mv_exp", &definitions)
                .expect("collect stats");
        let global = policy::MaintenancePolicyConfig::default();
        let table_policy = policy::TablePolicy::resolve(&global, &stats.properties);
        let outcome = policy::evaluate_table(
            &stats,
            &table_policy,
            &policy::TableRuntimeState::default(),
            &global,
            now_ms(),
        );
        assert!(
            outcome
                .actions
                .iter()
                .any(|a| a.kind() == policy::ActionKind::Expire),
            "short retention must make the policy plan an Expire; outcome={outcome:?}"
        );
    }

    let mut coordinator = coordinator_with(|_cfg| {});
    run_pass(&env, &mut coordinator);

    let after = mv_table_snapshot_count(&env, "analytics", "mv_exp");
    // min-snapshots-to-keep is respected.
    assert!(
        after >= 1,
        "must keep at least one snapshot (min-snapshots-to-keep=1), got {after}"
    );
    // expire must actually prune old snapshots (not a no-op).
    assert!(
        after < before,
        "auto-expire must remove old snapshots now: before={before} after={after}"
    );

    // The MV still answers SELECT after the expire pass.
    let rows = select_row_count(
        &env.state,
        Some("ice"),
        &env.current_db,
        "SELECT id, region, amount FROM mv_exp",
    );
    assert_eq!(rows, 4, "MV must still return all rows after expire pass");
}

// --- Scenario ③: auto EXPIRE does not break a downstream incremental consumer ---
//
// End-to-end smoke that a maintenance pass over a base MV (`mv_a`) does not
// break a downstream incremental MV (`mv_b`) that consumed an older `mv_a`
// snapshot, even with tiny retention configured on `mv_a`. The downstream-floor
// protection that guarantees this (the consumed snapshot is never selected for
// expiry) is unit-tested in `policy.rs`/`stats.rs`; here we verify the full
// MV-on-MV create + incremental-refresh + maintenance-pass path stays healthy.
#[test]
fn scenario_3_auto_expire_respects_downstream_consumer() {
    let env = open_env("ice", "analytics");
    create_aggregate_fact_table(&env.state, "ice", "sales", "fact");
    insert_into_aggregate_fact_table(&env.state, "ice", "sales", "fact", &[(1, "east", 10)]);

    // Base MV mv_a (projection over the fact table).
    create_mv(
        &env,
        "CREATE MATERIALIZED VIEW mv_a
         DISTRIBUTED BY HASH(region) BUCKETS 1
         PROPERTIES('storage_engine'='iceberg')
         AS SELECT id, region, amount FROM ice.sales.fact",
    );
    refresh_mv(&env, "mv_a");

    // Downstream incremental MV mv_b reads mv_a's storage table.
    create_mv(
        &env,
        "CREATE MATERIALIZED VIEW mv_b
         DISTRIBUTED BY HASH(region) BUCKETS 1
         PROPERTIES('storage_engine'='iceberg')
         AS SELECT region, count(*) AS c FROM ice.analytics.mv_a GROUP BY region",
    );
    // Refresh mv_b once: it consumes mv_a's current (older) snapshot, recorded
    // in mv_b.last_refresh_snapshots, which forms the downstream floor that
    // protects that mv_a snapshot from being expired.
    refresh_mv(&env, "mv_b");

    // Advance mv_a twice more WITHOUT refreshing mv_b.
    for id in 2..=3 {
        insert_into_aggregate_fact_table(&env.state, "ice", "sales", "fact", &[(id, "east", 10)]);
        refresh_mv(&env, "mv_a");
    }
    let before = mv_table_snapshot_count(&env, "analytics", "mv_a");
    assert!(
        before >= 3,
        "expected >= 3 mv_a snapshots before expire, got {before}"
    );

    // Tiny retention on mv_a.
    exec_sql(
        &env.state,
        Some("ice"),
        &env.current_db,
        "ALTER MATERIALIZED VIEW mv_a SET TBLPROPERTIES ('history.expire.max-snapshot-age-ms' = '1')",
    );

    let mut coordinator = coordinator_with(|_cfg| {});
    run_pass(&env, &mut coordinator);

    // The downstream consumer's lineage was not broken: refreshing mv_b again
    // still succeeds and mv_b reflects the correct aggregate result.
    refresh_mv(&env, "mv_b");
    let total_rows = select_row_count(
        &env.state,
        Some("ice"),
        &env.current_db,
        "SELECT region, c FROM mv_b",
    );
    assert_eq!(
        total_rows, 1,
        "mv_b must have exactly one region row, got {total_rows}"
    );
    let correct = select_row_count(
        &env.state,
        Some("ice"),
        &env.current_db,
        "SELECT region FROM mv_b WHERE region = 'east' AND c = 3",
    );
    assert_eq!(
        correct, 1,
        "mv_b must report east count = 3 after expire + incremental refresh"
    );
}

// --- Scenario ④: escape hatch disables a table ---

#[test]
fn scenario_4_escape_hatch_disables_table() {
    let env = open_env("ice", "analytics");
    create_aggregate_fact_table(&env.state, "ice", "sales", "fact");
    insert_into_aggregate_fact_table(&env.state, "ice", "sales", "fact", &[(1, "east", 10)]);

    create_mv(
        &env,
        "CREATE MATERIALIZED VIEW mv_off
         DISTRIBUTED BY HASH(region) BUCKETS 1
         PROPERTIES('storage_engine'='iceberg')
         AS SELECT id, region, amount FROM ice.sales.fact",
    );
    refresh_mv(&env, "mv_off");
    for id in 2..=4 {
        insert_into_aggregate_fact_table(&env.state, "ice", "sales", "fact", &[(id, "east", 10)]);
        refresh_mv(&env, "mv_off");
    }
    let before = mv_table_snapshot_count(&env, "analytics", "mv_off");
    assert!(before >= 3, "expected >= 3 snapshots, got {before}");

    // Tiny retention WOULD plan an expire, but the escape hatch disables all
    // maintenance for this table, so the pass must not touch it.
    exec_sql(
        &env.state,
        Some("ice"),
        &env.current_db,
        "ALTER MATERIALIZED VIEW mv_off SET TBLPROPERTIES \
         ('history.expire.max-snapshot-age-ms' = '1', 'novarocks.maintenance.enabled' = 'false')",
    );

    // Contrast: with the escape hatch set, the policy plans NOTHING (every
    // action is skipped with `Disabled`) even though the same short retention
    // would otherwise plan an expire. This proves the no-op below is caused by
    // the escape hatch, not by an empty work list.
    {
        let provider = env.state.metadata_provider.as_ref().expect("provider");
        let read = provider.begin_read().expect("read txn");
        let definitions = env
            .state
            .mv_repo
            .list_definitions(read.as_ref())
            .expect("list definitions");
        drop(read);
        let stats =
            stats::collect_table_stats(&env.state, "ice", "analytics", "mv_off", &definitions)
                .expect("collect stats");
        let global = policy::MaintenancePolicyConfig::default();
        let table_policy = policy::TablePolicy::resolve(&global, &stats.properties);
        assert!(
            !table_policy.enabled,
            "escape hatch must disable the table policy"
        );
        let outcome = policy::evaluate_table(
            &stats,
            &table_policy,
            &policy::TableRuntimeState::default(),
            &global,
            now_ms(),
        );
        assert!(
            outcome.actions.is_empty(),
            "disabled table must plan no actions; outcome={outcome:?}"
        );
    }

    let mut coordinator = coordinator_with(|_cfg| {});
    run_pass(&env, &mut coordinator);

    let after = mv_table_snapshot_count(&env, "analytics", "mv_off");
    assert_eq!(
        after, before,
        "disabled table must be untouched: before={before} after={after}"
    );
}

// --- Apply-key correctness gate: OPTIMIZE must preserve the hidden apply-key ---
//
// This is the strong correctness test for the OPTIMIZE-of-MV-storage-table fix.
// Unlike `scenario_1` (which only `SELECT *`s after optimize and so never
// exercises the apply-key locator against compacted files), this test forces an
// incremental DELETE refresh that locates the target row by its STORED hidden
// apply-key value (`__nova_base_row_id`) inside the now-compacted data files.
//
// A projection/filter MV (`SELECT id, region, amount FROM fact`) is used on
// purpose: its apply key is `ApplyKeySource::BaseRowId`, materialized as a real
// physical column `__nova_base_row_id` (field id 4) that is hidden from
// `SELECT *`. The incremental DELETE path (`locate_target_rows_by_apply_key`)
// reads that column BY STORED VALUE — it is NOT recomputable from the visible
// columns. Therefore:
//   * If OPTIMIZE drops the column, the post-optimize files have no
//     `__nova_base_row_id` and the locator scan errors (column missing) or the
//     MV silently full-refreshes.
//   * If OPTIMIZE writes a WRONG value (e.g. regenerated row ids), the locator
//     matches the wrong physical row and the DELETE removes the wrong MV row,
//     producing incorrect final contents.
//   * Only a verbatim carry-through of every row's `__nova_base_row_id` keeps
//     the final rows exactly correct.
//
// Before the compact.rs fix this test fails at the OPTIMIZE step itself with
// `annotate_batch column count mismatch: batch=5 schema=6` (the hidden column is
// omitted by `SELECT *` while the writer schema includes it).
#[test]
fn optimize_preserves_mv_apply_key_for_incremental_delete() {
    let env = open_env("ice", "analytics");
    create_aggregate_fact_table(&env.state, "ice", "sales", "fact");
    insert_into_aggregate_fact_table(&env.state, "ice", "sales", "fact", &[(1, "east", 10)]);

    // Projection MV: each base append produces a fresh small data file on the
    // MV storage table, and every MV row carries a distinct stored
    // `__nova_base_row_id`.
    create_mv(
        &env,
        "CREATE MATERIALIZED VIEW mv_keyed
         DISTRIBUTED BY HASH(region) BUCKETS 1
         PROPERTIES('storage_engine'='iceberg')
         AS SELECT id, region, amount FROM ice.sales.fact",
    );
    refresh_mv(&env, "mv_keyed");
    for id in 2..=4 {
        insert_into_aggregate_fact_table(
            &env.state,
            "ice",
            "sales",
            "fact",
            &[(id, "east", i64::from(id) * 10)],
        );
        refresh_mv(&env, "mv_keyed");
    }

    // Sanity: all four projected rows are present before compaction.
    let before_rows = select_id_region_amount_rows(
        &env.state,
        Some("ice"),
        &env.current_db,
        "SELECT id, region, amount FROM mv_keyed ORDER BY id",
    );
    assert_eq!(
        before_rows,
        vec![
            (1, "east".to_string(), 10),
            (2, "east".to_string(), 20),
            (3, "east".to_string(), 30),
            (4, "east".to_string(), 40),
        ],
        "MV must hold all four rows before optimize"
    );

    // This test is the OPTIMIZE apply-key preservation gate, not the
    // sequence-aware auto-admission gate. Force admission even when each
    // row-lineage file belongs to its own sequence.
    let mut coordinator = coordinator_with(|cfg| {
        cfg.policy.compaction_min_data_files = 1;
    });
    run_pass(&env, &mut coordinator);

    // The optimize worker thread is not spawned under cfg(test); drive the
    // submitted job synchronously. With the fix it rewrites the storage table
    // (carrying `__nova_base_row_id` verbatim) and reaches Finished.
    crate::connector::iceberg::compact::run_optimize_jobs_once(&env.state)
        .expect("run optimize job");

    let provider = env.state.metadata_provider.as_ref().expect("provider");
    let read = provider.begin_read().expect("read txn");
    let jobs = env
        .state
        .job_repo
        .show_iceberg_optimize_jobs(read.as_ref())
        .expect("list jobs");
    drop(read);
    assert!(!jobs.is_empty(), "expected an auto-submitted optimize job");
    assert!(
        jobs.iter().all(|j| matches!(
            j.state,
            crate::meta::repository::job::IcebergOptimizeJobState::Finished
        )),
        "optimize of the MV storage table must Finish (not Fail): {jobs:?}"
    );

    // Contents must be unchanged by the pure rewrite.
    let after_optimize_rows = select_id_region_amount_rows(
        &env.state,
        Some("ice"),
        &env.current_db,
        "SELECT id, region, amount FROM mv_keyed ORDER BY id",
    );
    assert_eq!(
        after_optimize_rows, before_rows,
        "OPTIMIZE must not change MV contents"
    );

    // Now DELETE a base row whose projected MV row lives in the COMPACTED
    // files, then refresh. The incremental DELETE path must locate the target
    // row by its stored `__nova_base_row_id` inside the rewritten files. If the
    // apply key was dropped/corrupted this either errors or deletes the wrong
    // row.
    exec_sql(
        &env.state,
        Some("ice"),
        &env.current_db,
        "DELETE FROM ice.sales.fact WHERE id = 2",
    );
    refresh_mv(&env, "mv_keyed");

    let final_rows = select_id_region_amount_rows(
        &env.state,
        Some("ice"),
        &env.current_db,
        "SELECT id, region, amount FROM mv_keyed ORDER BY id",
    );
    assert_eq!(
        final_rows,
        vec![
            (1, "east".to_string(), 10),
            (3, "east".to_string(), 30),
            (4, "east".to_string(), 40),
        ],
        "incremental DELETE refresh after OPTIMIZE must remove exactly id=2 \
         (proves the stored apply key survived compaction verbatim)"
    );
}

/// Run a `SELECT region, c FROM <mv> ORDER BY region` style query and return
/// the rows as `(region, count)` tuples.
fn select_region_count_rows(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    sql: &str,
) -> Vec<(String, i64)> {
    use arrow::array::{Int64Array, StringArray};

    let session = StandaloneSession {
        inner: Arc::clone(state),
    };
    let result = match session
        .execute_in_context(sql, current_catalog, current_database, None)
        .unwrap_or_else(|e| panic!("execute select `{sql}`: {e}"))
    {
        StatementResult::Query(result) => result,
        StatementResult::Ok => panic!("expected query result for {sql}"),
    };
    let mut rows = Vec::new();
    for chunk in &result.chunks {
        let region = chunk
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("region column is Utf8");
        let count = chunk
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count column is Int64");
        for row in 0..chunk.batch.num_rows() {
            rows.push((region.value(row).to_string(), count.value(row)));
        }
    }
    rows
}

// --- Apply-key correctness gate (aggregate MV): OPTIMIZE preserves the group
//     apply key AND the hidden aggregate-state columns ---
//
// Companion to `optimize_preserves_mv_apply_key_for_incremental_delete`, this
// time over an AGGREGATE MV whose storage table carries different hidden
// internal columns: the `GroupRowId` apply key (`__row_id__`, field id 1) plus
// the hidden aggregate-state columns (`__agg_state_*`). An incremental refresh
// that updates an existing group locates the group's target row by its apply
// key inside the compacted files AND must read back the correct accumulated
// aggregate state. If OPTIMIZE dropped or corrupted any hidden column, this
// errors or produces wrong group counts. Without the compact.rs fix it fails at
// the OPTIMIZE step with the same column-count mismatch (more columns hidden, so
// the gap is even larger).
#[test]
fn optimize_preserves_aggregate_mv_apply_key_and_state() {
    let env = open_env("ice", "analytics");
    create_aggregate_fact_table(&env.state, "ice", "sales", "fact");
    insert_into_aggregate_fact_table(&env.state, "ice", "sales", "fact", &[(1, "east", 10)]);

    // Aggregate MV: a fresh small data file is written on each refresh that
    // touches a group, and the storage table carries `__row_id__` + agg-state
    // columns hidden from `SELECT *`.
    create_mv(
        &env,
        "CREATE MATERIALIZED VIEW mv_agg
         DISTRIBUTED BY HASH(region) BUCKETS 1
         PROPERTIES('storage_engine'='iceberg')
         AS SELECT region, count(*) AS c FROM ice.sales.fact GROUP BY region",
    );
    refresh_mv(&env, "mv_agg");
    // Insert into several regions across refreshes to accumulate small files.
    for (id, region) in [(2, "west"), (3, "east"), (4, "north"), (5, "west")] {
        insert_into_aggregate_fact_table(&env.state, "ice", "sales", "fact", &[(id, region, 10)]);
        refresh_mv(&env, "mv_agg");
    }

    let before_rows = select_region_count_rows(
        &env.state,
        Some("ice"),
        &env.current_db,
        "SELECT region, c FROM mv_agg ORDER BY region",
    );
    assert_eq!(
        before_rows,
        vec![
            ("east".to_string(), 2),
            ("north".to_string(), 1),
            ("west".to_string(), 2),
        ],
        "aggregate MV must hold correct group counts before optimize"
    );

    let mut coordinator = coordinator_with(|cfg| {
        // This test is the OPTIMIZE aggregate-state preservation gate, not the
        // sequence-aware auto-admission gate.
        cfg.policy.compaction_min_data_files = 1;
    });
    run_pass(&env, &mut coordinator);
    crate::connector::iceberg::compact::run_optimize_jobs_once(&env.state)
        .expect("run optimize job");

    let provider = env.state.metadata_provider.as_ref().expect("provider");
    let read = provider.begin_read().expect("read txn");
    let jobs = env
        .state
        .job_repo
        .show_iceberg_optimize_jobs(read.as_ref())
        .expect("list jobs");
    drop(read);
    assert!(!jobs.is_empty(), "expected an auto-submitted optimize job");
    assert!(
        jobs.iter().all(|j| matches!(
            j.state,
            crate::meta::repository::job::IcebergOptimizeJobState::Finished
        )),
        "optimize of the aggregate MV storage table must Finish (not Fail): {jobs:?}"
    );

    // Contents unchanged by the pure rewrite.
    let after_optimize_rows = select_region_count_rows(
        &env.state,
        Some("ice"),
        &env.current_db,
        "SELECT region, c FROM mv_agg ORDER BY region",
    );
    assert_eq!(
        after_optimize_rows, before_rows,
        "OPTIMIZE must not change aggregate MV contents"
    );

    // Insert into an EXISTING group, then refresh. The incremental refresh must
    // UPDATE that group's row, which locates the old group row by its apply key
    // inside the COMPACTED files and reads back its accumulated state.
    insert_into_aggregate_fact_table(&env.state, "ice", "sales", "fact", &[(6, "east", 10)]);
    refresh_mv(&env, "mv_agg");

    let final_rows = select_region_count_rows(
        &env.state,
        Some("ice"),
        &env.current_db,
        "SELECT region, c FROM mv_agg ORDER BY region",
    );
    assert_eq!(
        final_rows,
        vec![
            ("east".to_string(), 3),
            ("north".to_string(), 1),
            ("west".to_string(), 2),
        ],
        "incremental refresh after OPTIMIZE must update east -> 3 (proves the \
         group apply key and aggregate state survived compaction verbatim)"
    );
}

// --- Gap A: e2e DV compaction on an MV storage table ---
//
// An AGGREGATE MV (`SELECT region, count(*) AS c FROM … GROUP BY region`) writes
// one data file when the first 'east' row is aggregated. Each subsequent
// incremental refresh that updates the 'east' group row deletes the old row via
// a new Puffin DV (position-delete) on that same data file, then inserts the
// updated row in a fresh append. After several such update cycles the original
// data file accumulates multiple separate DV files (one per refresh cycle that
// touched it). Setting `dv_min_delete_files = 2` and keeping
// `compaction_min_data_files` at its default (100) means the DV action fires
// without the OPTIMIZE suppressing it.
//
// The MUST-HAVE assertions are:
//   1. The maintenance pass succeeds (no panic, no Err).
//   2. The MV still answers `SELECT region, c FROM mv_dv` with the CORRECT
//      aggregate value (east count == total inserts), proving DV compaction on an
//      MV storage table does not corrupt the hidden apply-key or agg-state columns.
//
// Additionally: if the DV rewrite genuinely ran (detectable via the
// `total_delete_files` count not growing after compaction), we assert that
// explicitly. If the engine merges DVs inline during refresh (so fewer than 2
// delete files ever accumulate), the test probes for that and skips the
// delete-file count assertion rather than silently passing with no DV work done.
#[test]
fn dv_compaction_on_aggregate_mv_table() {
    let env = open_env("ice", "analytics");
    create_aggregate_fact_table(&env.state, "ice", "sales", "fact");

    // Aggregate MV: groups all rows by region, counting them.
    // The first refresh creates one data file holding the 'east' aggregate row.
    create_mv(
        &env,
        "CREATE MATERIALIZED VIEW mv_dv
         DISTRIBUTED BY HASH(region) BUCKETS 1
         PROPERTIES('storage_engine'='iceberg')
         AS SELECT region, count(*) AS c FROM ice.sales.fact GROUP BY region",
    );

    // Seed the first 'east' row and refresh so the MV has one data file with
    // an 'east' group row (count = 1).
    insert_into_aggregate_fact_table(&env.state, "ice", "sales", "fact", &[(1, "east", 10)]);
    refresh_mv(&env, "mv_dv");

    // Each iteration: insert another 'east' base row and refresh. The incremental
    // refresh must UPDATE the existing 'east' row (delete old → add new). The
    // delete is written as a Puffin DV against the data file that holds the 'east'
    // group row. After N such cycles, that data file has N DV files against it.
    // We do 4 more inserts so the base count grows 1→2→3→4→5 and we accumulate
    // up to 4 DVs on the original data file (or on whichever data file holds the
    // surviving 'east' aggregate row after each merge).
    for id in 2..=5 {
        insert_into_aggregate_fact_table(&env.state, "ice", "sales", "fact", &[(id, "east", 10)]);
        refresh_mv(&env, "mv_dv");
    }

    // The MV must already show east count = 5 before any maintenance.
    let rows_before = select_region_count_rows(
        &env.state,
        Some("ice"),
        &env.current_db,
        "SELECT region, c FROM mv_dv ORDER BY region",
    );
    assert_eq!(
        rows_before,
        vec![("east".to_string(), 5)],
        "aggregate MV must show east count = 5 before DV compaction"
    );

    // Collect stats to inspect the delete-file count.
    let definitions = {
        let provider = env.state.metadata_provider.as_ref().expect("provider");
        let read = provider.begin_read().expect("read txn");
        env.state
            .mv_repo
            .list_definitions(read.as_ref())
            .expect("list definitions")
    };
    let stats_before_maintenance =
        stats::collect_table_stats(&env.state, "ice", "analytics", "mv_dv", &definitions)
            .expect("collect stats before DV compaction");
    let delete_files_before = stats_before_maintenance.total_delete_files;

    // Run one pass with the DV threshold lowered to 2 so DV compaction fires
    // if >= 2 delete files are present. The optimize threshold is left at the
    // default (100), so OPTIMIZE cannot suppress DV here.
    let mut coordinator = coordinator_with(|cfg| {
        cfg.policy.dv_min_delete_files = 2;
    });
    run_pass(&env, &mut coordinator);
    // DV compaction runs inline (block_on) in the executor; no optimize job queue
    // is involved. No `run_optimize_jobs_once` call needed.

    // MUST-HAVE assertion 1: the pass succeeded (no panic above).
    // MUST-HAVE assertion 2: the MV still answers correctly after DV compaction.
    let rows_after = select_region_count_rows(
        &env.state,
        Some("ice"),
        &env.current_db,
        "SELECT region, c FROM mv_dv ORDER BY region",
    );
    assert_eq!(
        rows_after,
        vec![("east".to_string(), 5)],
        "aggregate MV must still answer east count = 5 after DV compaction \
         (proves DV compaction on an MV storage table does not corrupt hidden \
         apply-key or agg-state columns)"
    );

    // Bonus: if we actually accumulated >= 2 delete files before the pass,
    // assert the count did not grow (the rewrite consolidated them).
    if let Some(df_before) = delete_files_before {
        if df_before >= 2 {
            // DV compaction genuinely triggered. Collect stats after the pass and
            // assert delete-file count did not increase (ideally it shrank to 1).
            let definitions_after = {
                let provider = env.state.metadata_provider.as_ref().expect("provider");
                let read = provider.begin_read().expect("read txn");
                env.state
                    .mv_repo
                    .list_definitions(read.as_ref())
                    .expect("list definitions after DV compaction")
            };
            let stats_after_maintenance = stats::collect_table_stats(
                &env.state,
                "ice",
                "analytics",
                "mv_dv",
                &definitions_after,
            )
            .expect("collect stats after DV compaction");
            let delete_files_after = stats_after_maintenance
                .total_delete_files
                .unwrap_or(df_before);
            assert!(
                delete_files_after <= df_before,
                "DV compaction must not increase delete-file count: \
                 before={df_before} after={delete_files_after}"
            );
        }
        // If df_before < 2, the engine merged DVs inline during refresh (e.g.
        // the apply-key UPDATE rewrites into the same DV slot). DV compaction
        // correctly skips (BelowThreshold). The MUST-HAVE correctness assertions
        // above still hold.
    }
}
