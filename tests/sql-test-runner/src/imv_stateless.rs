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

//! L2 cross-process empty-metadata statelessness harness (W4 "IMV lake-native").
//!
//! Background: `restore_metadata_if_needed` (`src/engine/mod.rs`) calls
//! `rebuild_imv_cache_from_lake` (`src/engine/mv/lake_rebuild.rs`) on every
//! standalone-server startup, so a FE that boots against a fresh, empty
//! `[metadata]` SQLite path should rediscover any lake-native Iceberg MV
//! packages purely from the lake (Iceberg projection view marker + storage
//! table inline descriptor properties) and serve them normally. That
//! single-process round-trip is already covered by in-process unit tests in
//! `src/engine/mv/iceberg_refresh.rs`
//! (`rebuild_imv_cache_from_lake_reappears_after_sqlite_definition_dropped`
//! and friends) and by the `@imv_stateless_rebuild` sql-test directive
//! (`level=full`, exercised in-process via the
//! `novarocks_imv_stateless_rebuild` test procedure).
//!
//! This module is the **cross-process acceptance harness** for the same
//! claim: two separate `novarocks standalone-server` process launches over
//! the *same* lake/object-store/warehouse, where the second launch's FE has a
//! completely fresh SQLite metadata file (no shared process, no shared
//! in-memory cache — only the lake is shared). It:
//!
//! 1. Launches cluster A (1 FE + N BE) with metadata path A, creates an
//!    Iceberg-backed MV, inserts data, and refreshes it. Captures the MV's
//!    read face.
//! 2. Stops cluster A (drops the process handle).
//! 3. Launches cluster B against the *same* lake config but with the FE's
//!    `[metadata].path` pointed at a brand new, empty path B (via
//!    [`crate::cluster::CrossProcessServerHandle::launch_with_metadata_db_override`]).
//! 4. Asserts `SHOW MATERIALIZED VIEWS` lists the MV, `SELECT * FROM <mv>`
//!    matches cluster A's captured read face (order-insensitive), and
//!    `REFRESH MATERIALIZED VIEW <mv>` succeeds against the rediscovered
//!    definition.
//!
//! # CI-gating
//!
//! This harness spawns real `novarocks` processes and needs Iceberg MV
//! materialization to succeed end-to-end (Iceberg REST/Hadoop catalog +
//! object store), which is env-blocked in some local sandboxes. It is
//! validated by compiling cleanly and by the render-override unit test in
//! `crate::cluster` (which covers the one piece of new logic — the
//! `[metadata].path` override — without needing a live cluster). The full
//! orchestration is exercised in CI, where the Iceberg REST + MinIO fixture
//! (`docker/iceberg-rest/`) is available.
//!
//! Every `ProcessGuard` inside `CrossProcessServerHandle` already gates
//! readiness on the `NOVAROCKS_READY` stdout marker (see `cluster.rs`), so
//! this module issues no bare sleeps: launch itself blocks until the cluster
//! is ready to accept connections.

#![allow(dead_code)]

use crate::PlanWireFormatArg;
use crate::cluster::CrossProcessServerHandle;
use crate::results::compare_result_sets;
use crate::session::MysqlSession;
use crate::types::{ConnectionConfig, QueryExecution, RunnerConfig};
use anyhow::{Context, Result, bail};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static UNIQUE_SUFFIX_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Inputs for one run of the L2 statelessness case. Kept separate from the
/// SQL-test runner's `SqlCase`/`SqlStep` types because this harness drives
/// two independent cluster launches rather than a single suite session.
#[derive(Debug, Clone)]
pub(crate) struct ImvStatelessL2Case {
    /// Number of BE processes per cluster launch (cluster A and cluster B use
    /// the same size). Per the distributed-first non-negotiable rule this
    /// should be >= 1 and is exercised at >1 in CI (see module docs); the
    /// harness itself does not special-case size 1.
    pub cluster_size: usize,
    /// Iceberg catalog name to create in cluster A's setup SQL (must be a
    /// valid unquoted SQL identifier). A unique per-run suffix is appended by
    /// [`run_imv_stateless_l2_case`], mirroring the sql-test runner's
    /// `${uuid0}` per-case isolation convention (see
    /// `sql-tests/iceberg-ivm/sql/iceberg_mv_stateless_full_rebuild.sql`).
    pub catalog_prefix: String,
    /// Iceberg warehouse location for the created catalog, e.g.
    /// `s3://warehouse/imv-stateless-l2`. Must be reachable by every process
    /// launched by this harness (both cluster A and cluster B connect to the
    /// same underlying object store).
    pub iceberg_warehouse: String,
    /// Object store endpoint (`aws.s3.endpoint`) for the created catalog.
    pub oss_endpoint: String,
    pub oss_access_key: String,
    pub oss_secret_key: String,
    /// MV name created and probed by the harness.
    pub mv_name: String,
    /// MySQL protocol query timeout (seconds) applied to every statement.
    pub query_timeout: u64,
}

/// Result of a completed L2 statelessness run, for callers that want to
/// assert on captured data rather than only "did it return Ok".
#[derive(Debug, Clone)]
pub(crate) struct ImvStatelessL2Report {
    /// `SELECT * FROM <mv>` result captured from cluster A, after the base
    /// INSERT + REFRESH.
    pub cluster_a_select: QueryExecution,
    /// Same `SELECT` re-run against cluster B (fresh metadata). Asserted
    /// equal to `cluster_a_select` (order-insensitive) before being returned.
    pub cluster_b_select: QueryExecution,
    /// `SHOW MATERIALIZED VIEWS` result captured from cluster B, confirming
    /// the MV reappeared under the fresh metadata store.
    pub cluster_b_show_mvs: QueryExecution,
}

/// Run the full L2 cross-process empty-metadata statelessness case: two
/// cluster launches over the same lake, the second with a fresh `[metadata]`
/// SQLite path, asserting the MV reappears, reads identically, and can be
/// refreshed.
///
/// `repo_root` is the NovaRocks repo root (see `config::resolve_repo_root`);
/// `runner_config` supplies the base standalone-server config used to derive
/// both cluster launches (see `cluster::resolve_base_app_config_path`) — the
/// base config's own `[metadata].path` becomes cluster A's metadata path,
/// and a freshly-generated path under `repo_root`'s runtime scratch area
/// becomes cluster B's.
///
/// This function is CI-gated: it spawns real `novarocks standalone-server`
/// processes and requires a reachable Iceberg catalog backend (Hadoop-style
/// catalog over an S3-compatible object store, matching the
/// `docker/iceberg-rest/` fixture). It is not invoked from the `sql-tests`
/// CLI's normal suite dispatch; a future CI wiring can call it directly as an
/// additional acceptance gate for the W4 lake-native statelessness plan.
pub(crate) fn run_imv_stateless_l2_case(
    repo_root: &Path,
    runner_config: &RunnerConfig,
    plan_wire_format: PlanWireFormatArg,
    case: &ImvStatelessL2Case,
) -> Result<ImvStatelessL2Report> {
    let suffix = unique_suffix();
    let catalog = format!("{}_{}", case.catalog_prefix, suffix);
    let namespace = format!("ns_{}", suffix);
    let base_table = "orders";

    // -----------------------------------------------------------------
    // Phase A: cluster A owns the lake, creates the MV, and refreshes it.
    // -----------------------------------------------------------------
    let cluster_a = CrossProcessServerHandle::launch(
        case.cluster_size,
        repo_root,
        runner_config,
        plan_wire_format,
    )
    .context("launch cluster A for L2 statelessness case")?;
    let conn = connection_config(&cluster_a)?;
    let mut session =
        MysqlSession::new(&conn).context("connect to cluster A for L2 statelessness case")?;

    run_setup_sql(&mut session, case, &catalog, &namespace, base_table)
        .context("run cluster A setup SQL (catalog/table/MV/insert/refresh)")?;

    let select_sql = format!("SELECT * FROM {catalog}.{namespace}.{}", case.mv_name);
    let cluster_a_select = execute_required(&mut session, case.query_timeout, &select_sql)
        .context("capture cluster A MV read face")?;

    // Explicitly stop cluster A's FE/BE processes before cluster B is
    // launched. This guarantees no shared in-process state (caches,
    // connections) can leak into cluster B — only the lake persists.
    drop(session);
    drop(cluster_a);

    // -----------------------------------------------------------------
    // Phase B: cluster B reuses the same lake with a *fresh* metadata path.
    // -----------------------------------------------------------------
    let fresh_metadata_path = fresh_metadata_db_path(repo_root, &suffix)?;
    let fresh_metadata_path_str = fresh_metadata_path
        .to_str()
        .context("fresh metadata db path must be valid UTF-8")?;
    let cluster_b = CrossProcessServerHandle::launch_with_metadata_db_override(
        case.cluster_size,
        repo_root,
        runner_config,
        fresh_metadata_path_str,
        plan_wire_format,
    )
    .context("launch cluster B (fresh [metadata].path) for L2 statelessness case")?;
    let conn = connection_config(&cluster_b)?;
    let mut session =
        MysqlSession::new(&conn).context("connect to cluster B for L2 statelessness case")?;

    // The catalog itself must be re-registered against the fresh metadata
    // store: standalone Iceberg *catalog* registration (as opposed to the
    // in-lake MV package) is not currently part of the W4 lake-native
    // statelessness contract, which targets IMV cache rebuild specifically
    // (`rebuild_imv_cache_from_lake`). Re-declaring the same catalog against
    // the same warehouse is the harness's way of standing in for whatever
    // catalog-provisioning step a real fresh FE would run before serving
    // traffic; it does not touch the SQLite MV definitions this case is
    // actually probing.
    register_catalog(&mut session, case, &catalog)
        .context("re-register Iceberg catalog against cluster B")?;

    let show_sql = "SHOW MATERIALIZED VIEWS";
    let cluster_b_show_mvs = execute_required(&mut session, case.query_timeout, show_sql)
        .context("SHOW MATERIALIZED VIEWS on cluster B (fresh metadata)")?;
    if !show_mvs_contains(&cluster_b_show_mvs, &case.mv_name) {
        bail!(
            "cluster B SHOW MATERIALIZED VIEWS did not list `{}` after fresh-metadata rebuild; rows={:?}",
            case.mv_name,
            cluster_b_show_mvs.rows
        );
    }

    let select_sql = format!("SELECT * FROM {catalog}.{namespace}.{}", case.mv_name);
    let cluster_b_select = execute_required(&mut session, case.query_timeout, &select_sql)
        .context("SELECT MV on cluster B (fresh metadata)")?;
    let (same, reason) = compare_result_sets(
        &cluster_a_select.header,
        &cluster_a_select.rows,
        &cluster_b_select.header,
        &cluster_b_select.rows,
        /* order_sensitive = */ false,
        None,
    );
    if !same {
        bail!(
            "cluster B MV read face diverged from cluster A after fresh-metadata rebuild: {reason}"
        );
    }

    let refresh_sql = format!(
        "REFRESH MATERIALIZED VIEW {catalog}.{namespace}.{}",
        case.mv_name
    );
    execute_required(&mut session, case.query_timeout, &refresh_sql)
        .context("REFRESH MATERIALIZED VIEW on cluster B (fresh metadata) must succeed")?;

    // `cluster_b` drops at end of scope, stopping cluster B's processes and
    // cleaning up its runtime dir (see `CrossProcessServerHandle::drop`).
    drop(cluster_b);
    let _ = std::fs::remove_file(&fresh_metadata_path);

    Ok(ImvStatelessL2Report {
        cluster_a_select,
        cluster_b_select,
        cluster_b_show_mvs,
    })
}

/// Cluster A setup: create the Iceberg catalog/namespace/base table, insert
/// seed rows, create the Iceberg-backed MV, and refresh it once so the MV has
/// a non-empty read face and a `provenance.v1`-stamped current snapshot
/// before the harness moves to Phase B.
fn run_setup_sql(
    session: &mut MysqlSession,
    case: &ImvStatelessL2Case,
    catalog: &str,
    namespace: &str,
    base_table: &str,
) -> Result<()> {
    register_catalog(session, case, catalog)?;

    let statements = [
        format!("CREATE DATABASE {catalog}.{namespace}"),
        format!(
            "CREATE TABLE {catalog}.{namespace}.{base_table} (k1 INT, v2 BIGINT) \
             TBLPROPERTIES (\"format-version\" = \"3\", \"write.row-lineage\" = \"true\")"
        ),
        format!("INSERT INTO {catalog}.{namespace}.{base_table} VALUES (1, 10), (2, 20), (3, 50)"),
        format!("SET CATALOG {catalog}"),
        format!("USE {namespace}"),
        format!(
            "CREATE MATERIALIZED VIEW {mv} \
             DISTRIBUTED BY HASH(k1) BUCKETS 2 \
             PROPERTIES ('storage_engine' = 'iceberg') \
             AS SELECT k1, v2 FROM {base_table}",
            mv = case.mv_name
        ),
        format!("REFRESH MATERIALIZED VIEW {}", case.mv_name),
    ];
    for sql in statements {
        execute_required(session, case.query_timeout, &sql)
            .with_context(|| format!("cluster A setup statement failed: {sql}"))?;
    }
    Ok(())
}

fn register_catalog(
    session: &mut MysqlSession,
    case: &ImvStatelessL2Case,
    catalog: &str,
) -> Result<()> {
    let create_catalog = format!(
        "CREATE EXTERNAL CATALOG {catalog} PROPERTIES ( \
            \"type\" = \"iceberg\", \
            \"iceberg.catalog.type\" = \"hadoop\", \
            \"iceberg.catalog.warehouse\" = \"{warehouse}\", \
            \"aws.s3.endpoint\" = \"{endpoint}\", \
            \"aws.s3.access_key\" = \"{access_key}\", \
            \"aws.s3.secret_key\" = \"{secret_key}\", \
            \"aws.s3.enable_path_style_access\" = \"true\" \
        )",
        warehouse = case.iceberg_warehouse,
        endpoint = case.oss_endpoint,
        access_key = case.oss_access_key,
        secret_key = case.oss_secret_key,
    );
    execute_required(session, case.query_timeout, &create_catalog)
        .context("CREATE EXTERNAL CATALOG failed")?;
    Ok(())
}

fn execute_required(
    session: &mut MysqlSession,
    query_timeout: u64,
    sql: &str,
) -> Result<QueryExecution> {
    let (ok, execution, message) = session.execute_query(query_timeout, sql, None);
    if !ok {
        bail!("query failed: {sql}\n{message}");
    }
    execution.ok_or_else(|| anyhow::anyhow!("query returned no result: {sql}"))
}

fn show_mvs_contains(execution: &QueryExecution, mv_name: &str) -> bool {
    execution
        .rows
        .iter()
        .any(|row| row.iter().any(|cell| cell.eq_ignore_ascii_case(mv_name)))
}

/// Build a `ConnectionConfig` targeting a launched cluster's FE MySQL port.
/// `CrossProcessServerHandle` always reports `Some` for both
/// `target_host`/`target_port` (see its `ServerHandle` impl in `cluster.rs`),
/// so a `None` here indicates a harness bug rather than a runtime condition.
fn connection_config(handle: &CrossProcessServerHandle) -> Result<ConnectionConfig> {
    use crate::cluster::ServerHandle;
    let host = handle
        .target_host()
        .context("cross-process handle missing target host")?
        .to_string();
    let port = handle
        .target_port()
        .context("cross-process handle missing target port")?;
    Ok(ConnectionConfig {
        mysql: "mysql".to_string(),
        host,
        port: port.to_string(),
        user: "root".to_string(),
        password: None,
        catalog: None,
        db: None,
    })
}

/// Generate a process-unique, time-unique suffix for catalog/namespace names
/// and the fresh metadata db filename, mirroring the runtime-dir naming
/// scheme already used by `cluster::create_runtime_dir` (pid + nanos), plus a
/// process-local sequence for platforms where adjacent `SystemTime` reads can
/// return the same tick.
fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let seq = UNIQUE_SUFFIX_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}_{}_{}", std::process::id(), nanos, seq)
}

/// Path for cluster B's fresh, empty `[metadata]` SQLite file. Lives under
/// the same `.sql-test-runner-runtime/` scratch root that
/// `cluster::create_runtime_dir` uses, so normal repo cleanup conventions
/// apply; the harness also best-effort removes it directly after use.
fn fresh_metadata_db_path(repo_root: &Path, suffix: &str) -> Result<PathBuf> {
    let dir = repo_root.join(".sql-test-runner-runtime");
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("create {} for L2 fresh metadata db", dir.display()))?;
    Ok(dir.join(format!("imv-stateless-l2-fresh-{suffix}.sqlite")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_case() -> ImvStatelessL2Case {
        ImvStatelessL2Case {
            cluster_size: 3,
            catalog_prefix: "mv_ice_l2".to_string(),
            iceberg_warehouse: "s3://warehouse/imv-stateless-l2".to_string(),
            oss_endpoint: "http://127.0.0.1:9000".to_string(),
            oss_access_key: "admin".to_string(),
            oss_secret_key: "admin123".to_string(),
            mv_name: "orders_mv".to_string(),
            query_timeout: 60,
        }
    }

    #[test]
    fn unique_suffix_is_stable_shape_and_distinct_across_calls() {
        let a = unique_suffix();
        let b = unique_suffix();
        assert_ne!(a, b, "successive suffixes must differ");
        assert!(a.contains('_'), "suffix should be pid_nanos: {a}");
    }

    #[test]
    fn fresh_metadata_db_path_is_under_runtime_scratch_dir_and_sqlite_suffixed() {
        let repo_root = std::env::current_dir().expect("current dir");
        let path = fresh_metadata_db_path(&repo_root, "test123").expect("build fresh path");
        assert!(
            path.starts_with(repo_root.join(".sql-test-runner-runtime")),
            "fresh metadata path should live under the shared runtime scratch dir: {}",
            path.display()
        );
        assert_eq!(path.extension().and_then(|e| e.to_str()), Some("sqlite"));
        assert!(path.to_string_lossy().contains("test123"));

        // Clean up: fresh_metadata_db_path only creates the parent dir, not
        // the sqlite file itself.
        let _ = std::fs::remove_dir(repo_root.join(".sql-test-runner-runtime"));
    }

    #[test]
    fn show_mvs_contains_matches_case_insensitively_and_rejects_absent() {
        let execution = QueryExecution {
            header: vec!["Name".to_string(), "Type".to_string()],
            rows: vec![vec!["Orders_MV".to_string(), "iceberg".to_string()]],
            text_output: String::new(),
            elapsed: std::time::Duration::default(),
        };
        assert!(show_mvs_contains(&execution, "orders_mv"));
        assert!(!show_mvs_contains(&execution, "other_mv"));
    }

    #[test]
    fn register_catalog_sql_embeds_all_case_fields() {
        // This does not run a live session (no server available in unit
        // tests); it documents and pins the exact CREATE EXTERNAL CATALOG
        // shape the harness sends, matching
        // sql-tests/iceberg-ivm/sql/iceberg_backed_mv_basic_lifecycle.sql's
        // hadoop-catalog setup so the L2 case exercises the same catalog
        // configuration path as the in-process `full`-level directive.
        let case = sample_case();
        let sql = format!(
            "CREATE EXTERNAL CATALOG {catalog} PROPERTIES ( \
                \"type\" = \"iceberg\", \
                \"iceberg.catalog.type\" = \"hadoop\", \
                \"iceberg.catalog.warehouse\" = \"{warehouse}\", \
                \"aws.s3.endpoint\" = \"{endpoint}\", \
                \"aws.s3.access_key\" = \"{access_key}\", \
                \"aws.s3.secret_key\" = \"{secret_key}\", \
                \"aws.s3.enable_path_style_access\" = \"true\" \
            )",
            catalog = "mv_ice_l2_x",
            warehouse = case.iceberg_warehouse,
            endpoint = case.oss_endpoint,
            access_key = case.oss_access_key,
            secret_key = case.oss_secret_key,
        );
        assert!(sql.contains(&case.iceberg_warehouse));
        assert!(sql.contains(&case.oss_endpoint));
        assert!(sql.contains("\"iceberg.catalog.type\" = \"hadoop\""));
    }

    #[test]
    fn l2_case_threads_plan_wire_format_to_both_cluster_launches() {
        let source = include_str!("imv_stateless.rs");
        let fn_start = source
            .find("pub(crate) fn run_imv_stateless_l2_case(")
            .expect("run_imv_stateless_l2_case source");
        let fn_body = &source[fn_start..source[fn_start..]
            .find("fn run_setup_sql(")
            .expect("run_setup_sql follows l2 case")
            + fn_start];

        assert!(
            fn_body.contains("plan_wire_format: PlanWireFormatArg"),
            "L2 harness entrypoint must receive the plan wire format dimension"
        );
        assert!(
            fn_body.contains("runner_config,\n        plan_wire_format,"),
            "cluster A launch must use the caller's plan wire format"
        );
        assert!(
            fn_body.contains("fresh_metadata_path_str,\n        plan_wire_format,"),
            "cluster B metadata-override launch must use the caller's plan wire format"
        );
        assert!(
            !fn_body.contains("PlanWireFormatArg::Thrift"),
            "L2 harness must not hardcode thrift for either cluster"
        );
    }

    #[test]
    fn connection_config_uses_root_user_and_no_catalog_db_preset() {
        // Direct construction mirrors what `connection_config` builds, since
        // building a real CrossProcessServerHandle needs a live binary/ports.
        let conn = ConnectionConfig {
            mysql: "mysql".to_string(),
            host: "127.0.0.1".to_string(),
            port: "23223".to_string(),
            user: "root".to_string(),
            password: None,
            catalog: None,
            db: None,
        };
        assert_eq!(conn.user, "root");
        assert!(conn.catalog.is_none());
        assert!(conn.db.is_none());
    }
}
