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

mod benchmark_bootstrap;
mod cluster;
mod compat_artifact;
mod compat_directive;
mod config;
mod fault_injection;
mod imv_stateless;
mod managed_process;
mod parser;
mod results;
mod runner;
mod session;
mod shell;
mod starrocks_compat_cluster;
mod suite_manifest;
mod types;

use crate::benchmark_bootstrap::{
    BenchmarkBootstrapOptions, ensure_benchmark_data, parse_scale_overrides,
};
use crate::cluster::{ClusterMode, ServerHandle, launch_server, validate_cluster_args};
use crate::compat_artifact::CompatArtifact;
use crate::config::{
    build_suite_configs, case_auto_db_name, env_optional, env_or_default, list_sql_files,
    load_runner_config, placeholder_variables, resolve_config_path, resolve_path,
    resolve_reference_port, resolve_repo_root, resolve_target_port, suite_default_query_timeout,
};
use crate::parser::load_suite_hook;
use crate::results::{
    case_result_path, compare_result_sets, find_legacy_result_paths, load_expected_results,
    normalize_explain_timing_rows, step_allows_missing_expected_result,
    step_has_implicit_skip_result, step_requires_recorded_result, step_retry_count,
    step_retry_interval, verify_text_assertions, write_mismatch_artifacts, write_result_file,
};
use crate::runner::{
    error_message_matches, extract_engine_error_code, parse_selector_list, summarize_connection,
};
use crate::session::{MysqlSession, drop_case_database, execute_suite_hook, reset_case_database};
use crate::suite_manifest::{SuiteServerMode, select_suite_names};
use crate::types::*;
use anyhow::{Context, Result, bail};
use clap::{ArgAction, Parser, ValueEnum};
use rayon::prelude::*;
use regex::Regex;
use std::collections::{BTreeMap, HashSet};
use std::fmt::Write as FmtWrite;
use std::fs;
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::{Duration, Instant};

fn resolve_effective_target_port(
    server_port: Option<u16>,
    cli_port: Option<&str>,
    runner_config: &RunnerConfig,
) -> Result<String> {
    match server_port {
        Some(port) => Ok(port.to_string()),
        None => resolve_target_port(cli_port, runner_config),
    }
}

fn resolve_selected_cluster(
    server_mode: SuiteServerMode,
    manifest_cluster_size: usize,
    cli_mode: ClusterMode,
    cli_cluster_size: Option<usize>,
) -> Result<(ClusterMode, usize)> {
    match server_mode {
        SuiteServerMode::Native => Ok((cli_mode, cli_cluster_size.unwrap_or(1))),
        SuiteServerMode::StarRocksCompat => {
            if let Some(cluster_size) = cli_cluster_size
                && cluster_size != manifest_cluster_size
            {
                bail!(
                    "starrocks-compat mode requires --cluster-size {} (got {})",
                    manifest_cluster_size,
                    cluster_size
                );
            }
            Ok((ClusterMode::StarRocksCompat, manifest_cluster_size))
        }
    }
}

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Mode {
    Verify,
    Record,
    Diff,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum RecordFrom {
    Target,
    Reference,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CaseStatus {
    Pass,
    Fail,
    Skipped,
}

fn expected_engine_error_code_result(err_msg: &str, expected_code: &str) -> Result<(), String> {
    let actual_code = extract_engine_error_code(err_msg);
    if actual_code.as_deref() == Some(expected_code) {
        Ok(())
    } else {
        Err(format!(
            "expected engine error code {:?}, got {:?}: {}",
            expected_code, actual_code, err_msg
        ))
    }
}

fn evaluate_expected_error_branch(
    meta: &QueryMeta,
    ok: bool,
    err_msg: &str,
) -> Option<Result<(), String>> {
    if let Some(expected_code) = meta.expect_error_code.as_deref() {
        return Some(if ok {
            Err(format!(
                "expected engine error code {:?}, but query succeeded",
                expected_code
            ))
        } else {
            expected_engine_error_code_result(err_msg, expected_code)
        });
    }

    let expected_error = meta.expect_error.as_deref()?;
    Some(if ok {
        Err(format!(
            "expected error containing {:?}, but query succeeded",
            expected_error
        ))
    } else if error_message_matches(err_msg, expected_error) {
        Ok(())
    } else {
        Err(format!(
            "expected error containing {:?}, got: {}",
            expected_error, err_msg
        ))
    })
}

fn annotate_failure_with_engine_error_code(message: &str, source: &str) -> String {
    if message.contains("engine_error_code=") {
        return message.to_string();
    }
    extract_engine_error_code(source)
        .or_else(|| extract_engine_error_code(message))
        .or_else(|| {
            message
                .strip_prefix("target execute failed: ")
                .and_then(extract_engine_error_code)
        })
        .map(|code| format!("engine_error_code={} {}", code, message))
        .unwrap_or_else(|| message.to_string())
}

fn expected_engine_error_code_diff_result(
    expected_code: &str,
    target_ok: bool,
    target_err: &str,
    reference_ok: bool,
    reference_err: &str,
) -> Result<(), String> {
    let target_code = extract_engine_error_code(target_err);
    let reference_code = extract_engine_error_code(reference_err);
    let target_matched = !target_ok && target_code.as_deref() == Some(expected_code);
    let reference_matched = !reference_ok && reference_code.as_deref() == Some(expected_code);
    if target_matched && reference_matched {
        Ok(())
    } else {
        Err(format!(
            "expected engine error code {:?} (target_ok={}, target_code={:?}, target_err={}, reference_ok={}, reference_code={:?}, reference_err={})",
            expected_code,
            target_ok,
            target_code,
            target_err,
            reference_ok,
            reference_code,
            reference_err
        ))
    }
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Debug, Parser)]
#[command(
    name = "sql-tests",
    about = "Run SQL correctness tests for suite directories under sql-tests/"
)]
struct Cli {
    /// Suite name(s), comma-separated.  Use "all" to run every discovered suite.
    #[arg(long)]
    suite: String,

    #[arg(long)]
    config: Option<String>,

    #[arg(long, value_enum, default_value_t = Mode::Verify)]
    mode: Mode,

    #[arg(long, value_enum, default_value_t = RecordFrom::Reference)]
    record_from: RecordFrom,

    #[arg(long)]
    sql_dir: Option<String>,

    #[arg(long)]
    result_dir: Option<String>,

    #[arg(long)]
    sql_glob: Option<String>,

    #[arg(long)]
    mysql: Option<String>,

    #[arg(long)]
    host: Option<String>,

    #[arg(long)]
    port: Option<String>,

    #[arg(long)]
    user: Option<String>,

    #[arg(long)]
    password: Option<String>,

    #[arg(long)]
    ref_mysql: Option<String>,

    #[arg(long)]
    ref_host: Option<String>,

    #[arg(long)]
    ref_port: Option<String>,

    #[arg(long)]
    ref_user: Option<String>,

    #[arg(long)]
    ref_password: Option<String>,

    #[arg(long)]
    query_timeout: Option<u64>,

    #[arg(long, action = ArgAction::SetTrue, conflicts_with = "no_verify")]
    verify: bool,

    #[arg(long = "no-verify", action = ArgAction::SetTrue, conflicts_with = "verify")]
    no_verify: bool,

    #[arg(long, action = ArgAction::SetTrue)]
    update_expected: bool,

    #[arg(long)]
    write_actual_dir: Option<String>,

    #[arg(long)]
    only: Option<String>,

    #[arg(long)]
    skip: Option<String>,

    #[arg(long)]
    limit: Option<usize>,

    #[arg(long, action = ArgAction::SetTrue)]
    order_sensitive_default: bool,

    #[arg(long)]
    float_epsilon: Option<f64>,

    #[arg(long, default_value_t = 3)]
    preview_lines: usize,

    #[arg(long, value_enum, default_value_t = ClusterMode::AllInOne)]
    cluster_mode: ClusterMode,

    /// Number of BE processes to launch in cross-process cluster mode (>= 1).
    /// All-in-one mode requires cluster_size = 1.
    #[arg(long)]
    cluster_size: Option<usize>,

    /// SQL statements applied, in order, to each target case session before its first step.
    #[arg(long = "target-session-sql", value_name = "SQL", action = ArgAction::Append)]
    target_session_sql: Vec<String>,

    #[arg(long, action = ArgAction::SetTrue)]
    dry_run: bool,

    #[arg(long, action = ArgAction::SetTrue)]
    fail_fast: bool,

    #[arg(long, action = ArgAction::SetTrue)]
    no_auto_bootstrap_benchmark_data: bool,

    #[arg(long = "benchmark-scale", value_name = "BENCHMARK_SCALE", action = ArgAction::Append)]
    benchmark_scale: Vec<String>,

    #[arg(long, action = ArgAction::SetTrue)]
    benchmark_bootstrap_rebuild: bool,

    /// Number of parallel test workers.  0 = auto-detect (number of logical CPUs).
    /// 1 = serial execution (legacy behaviour).
    #[arg(short = 'j', long, default_value_t = 0)]
    jobs: usize,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn verify_override(cli: &Cli) -> Option<bool> {
    if cli.verify {
        Some(true)
    } else if cli.no_verify {
        Some(false)
    } else {
        None
    }
}

fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::Verify => "verify",
        Mode::Record => "record",
        Mode::Diff => "diff",
    }
}

fn query_order_sensitive(step: &SqlStep, default: bool) -> bool {
    step.meta.order_sensitive.unwrap_or(default)
}

fn query_float_epsilon(step: &SqlStep, default: Option<f64>) -> Option<f64> {
    step.meta.float_epsilon.or(default)
}

fn execute_target_session_sql_with<S>(
    session: &mut S,
    statements: &[String],
    mut execute: impl FnMut(&mut S, &str) -> Result<(), String>,
) -> Result<(), String> {
    for statement in statements {
        execute(session, statement)
            .map_err(|error| format!("target session SQL failed for {statement:?}: {error}"))?;
    }
    Ok(())
}

/// Best-effort TCP probe for a MinIO-style endpoint like `http://127.0.0.1:9000`.
fn endpoint_reachable(endpoint: &str) -> bool {
    let stripped = endpoint
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(endpoint);
    let authority = stripped.split('/').next().unwrap_or(stripped);
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) => match port.parse::<u16>() {
            Ok(port) => (host, port),
            Err(_) => return false,
        },
        None => {
            let default_port = if endpoint.starts_with("https://") {
                443
            } else {
                80
            };
            (authority, default_port)
        }
    };
    let Ok(addr) = format!("{host}:{port}").parse() else {
        return false;
    };
    TcpStream::connect_timeout(&addr, Duration::from_secs(1)).is_ok()
}

/// When the runner config declares an Iceberg warehouse, fail fast if the
/// object-store endpoint is not reachable. Without this probe, the first
/// `CREATE TABLE` in a suite would timeout deep inside the standalone server.
fn ensure_iceberg_object_store_prereqs(runner_config: &RunnerConfig) -> Result<()> {
    if !runner_config.values.contains_key("iceberg_catalog_warehouse") {
        return Ok(());
    }
    let endpoint = runner_config
        .values
        .get("oss_endpoint")
        .cloned()
        .unwrap_or_else(|| env_or_default("AWS_S3_ENDPOINT", "http://127.0.0.1:9000"));
    if endpoint_reachable(&endpoint) {
        return Ok(());
    }
    bail!(
        "MinIO at {} is unreachable.\n\
         hint: start it with:\n  \
         mkdir -p ~/minio-data && minio server ~/minio-data --console-address :9001 &",
        endpoint
    );
}

// ---------------------------------------------------------------------------
// Parallel execution types
// ---------------------------------------------------------------------------

/// Shared, read-only context for running cases within a suite.
struct SuiteRunContext {
    suite_name: String,
    mode: Mode,
    record_from: RecordFrom,
    target_conn_base: ConnectionConfig,
    reference_conn_base: ConnectionConfig,
    target_admin_conn: ConnectionConfig,
    reference_admin_conn: ConnectionConfig,
    result_dir: Option<PathBuf>,
    actual_artifact_dir: Option<PathBuf>,
    verify_enabled: bool,
    query_timeout: u64,
    reference_required: bool,
    auto_case_db: bool,
    order_sensitive_default: bool,
    float_epsilon: Option<f64>,
    preview_lines: usize,
    update_expected: bool,
    target_session_sql: Vec<String>,
    marker_re: Regex,
    fail_fast: bool,
    server_handle: Arc<Mutex<Box<dyn ServerHandle>>>,
}

struct CaseOutcome {
    case_id: String,
    status: CaseStatus,
    elapsed: Duration,
    log: String,
}

struct SuiteOutcome {
    suite_name: String,
    total: usize,
    outcomes: Vec<CaseOutcome>,
    cleanup_errors: Vec<String>,
    wall_time: Duration,
}

#[derive(Clone)]
struct CaseTiming {
    suite_name: String,
    case_id: String,
    status: CaseStatus,
    elapsed: Duration,
}

/// Everything needed to run a suite: context + prepared cases + hooks.
struct PreparedSuite {
    ctx: SuiteRunContext,
    cases: Vec<SqlCase>,
    init_hook: Option<SuiteHook>,
    cleanup_hook: Option<SuiteHook>,
}

// ---------------------------------------------------------------------------
// wait_alter helper – polls SHOW ALTER TABLE until the latest job is FINISHED
// ---------------------------------------------------------------------------

fn execute_wait_alter(
    session: &mut MysqlSession,
    query_timeout: u64,
    db_name: &str,
    table_name: &str,
    kind: &str, // "COLUMN" or "ROLLUP"
    max_retries: usize,
    interval: Duration,
    log: &mut String,
) -> (bool, Duration) {
    // Test cases may scope @db as `catalog.db` to switch catalog before running
    // the wrapped statement. SHOW ALTER TABLE OPTIMIZE FROM <name> expects the
    // engine to receive each identifier part quoted independently — wrapping
    // `catalog.db` in a single backtick group flattens it into one db name and
    // makes the lookup miss.
    let qualified_from = match db_name.split_once('.') {
        Some((catalog, db)) => format!("`{}`.`{}`", catalog, db),
        None => format!("`{}`", db_name),
    };
    let show_sql = format!(
        "SHOW ALTER TABLE {} FROM {} WHERE TableName = '{}' ORDER BY CreateTime DESC LIMIT 1",
        kind, qualified_from, table_name,
    );
    let mut total_elapsed = Duration::ZERO;
    for attempt in 0..max_retries {
        let (ok, execution, _err) = session.execute_query(query_timeout, &show_sql, None);
        if let Some(ref exec) = execution {
            total_elapsed += exec.elapsed;
        }
        if ok {
            if let Some(exec) = &execution {
                if exec.text_output.contains("FINISHED") {
                    let _ = writeln!(
                        log,
                        "    ✅ wait_alter_{} on `{}` finished (attempt {}/{})",
                        kind.to_lowercase(),
                        table_name,
                        attempt + 1,
                        max_retries,
                    );
                    return (true, total_elapsed);
                }
            }
        }
        if attempt + 1 < max_retries {
            let _ = writeln!(
                log,
                "    ⏳ wait_alter_{} on `{}`: attempt {}/{}, retrying after {}ms",
                kind.to_lowercase(),
                table_name,
                attempt + 1,
                max_retries,
                interval.as_millis(),
            );
            sleep(interval);
        }
    }
    let _ = writeln!(
        log,
        "    ❌ wait_alter_{} on `{}` timed out after {} retries",
        kind.to_lowercase(),
        table_name,
        max_retries,
    );
    (false, total_elapsed)
}

/// Run all wait_alter annotations on a step.  Returns (ok, extra_elapsed).
fn run_step_wait_alters(
    step: &SqlStep,
    session: &mut MysqlSession,
    query_timeout: u64,
    primary_case_db: Option<&str>,
    log: &mut String,
) -> (bool, Duration) {
    let mut total = Duration::ZERO;
    let db = step.meta.db.as_deref().or(primary_case_db).unwrap_or("");
    for (table, kind, default_retries) in [
        (&step.meta.wait_alter_column, "COLUMN", 60usize),
        (&step.meta.wait_alter_rollup, "ROLLUP", 120usize),
        (&step.meta.wait_alter_optimize, "OPTIMIZE", 300usize),
    ] {
        if let Some(table_name) = table {
            let (ok, elapsed) = execute_wait_alter(
                session,
                query_timeout,
                db,
                table_name,
                kind,
                default_retries,
                Duration::from_secs(1),
                log,
            );
            total += elapsed;
            if !ok {
                return (false, total);
            }
        }
    }
    (true, total)
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// @imv_equivalence_check helpers
// ---------------------------------------------------------------------------

/// Pure multiset comparison of an MV's incremental contents vs a full recompute.
/// Returns `None` when equal, or `Some(reason)` describing the mismatch.
fn imv_equivalence_failure(
    mv: &str,
    inc: &crate::types::QueryExecution,
    full: &crate::types::QueryExecution,
    epsilon: Option<f64>,
) -> Option<String> {
    let (same, reason) = compare_result_sets(
        &inc.header,
        &inc.rows,
        &full.header,
        &full.rows,
        false,
        epsilon,
    );
    if same {
        None
    } else {
        Some(format!(
            "@imv_equivalence_check: incremental result != full recompute for `{mv}`\n{reason}"
        ))
    }
}

/// Capture the MV's current (incremental) contents, derive a full recompute by
/// running the MV's `SelectText` query directly against the base tables (obtained
/// from `SHOW MATERIALIZED VIEWS`), and assert multiset equality of results.
/// MV is qualified by `db` like wait_alter_*. No side effects on the MV.
fn run_imv_equivalence_check(
    mv: &str,
    session: &mut crate::session::MysqlSession,
    query_timeout: u64,
    db: Option<&str>,
    epsilon: Option<f64>,
    log: &mut String,
) -> Result<(), String> {
    let fqn = match db {
        Some(d) if !d.is_empty() => format!("{d}.{mv}"),
        _ => mv.to_string(),
    };
    let select = format!("SELECT * FROM {fqn}");
    let _ = writeln!(
        log,
        "    @imv_equivalence_check: capturing incremental contents of {fqn}"
    );
    let (ok, inc, msg) = session.execute_query(query_timeout, &select, None);
    if !ok {
        return Err(format!(
            "@imv_equivalence_check: incremental SELECT failed: {msg}"
        ));
    }
    let inc = inc.ok_or_else(|| {
        "@imv_equivalence_check: incremental SELECT returned no result".to_string()
    })?;

    // Derive the full-recompute result by running the MV's defining SELECT
    // directly against the base tables. We obtain the SELECT SQL from
    // `SHOW MATERIALIZED VIEWS`, which exposes `SelectText` for each MV.
    // This avoids `REFRESH MATERIALIZED VIEW ... FULL`, which is intentionally
    // disabled pending redesign.
    let _ = writeln!(
        log,
        "    @imv_equivalence_check: deriving full recompute via SelectText"
    );
    let (ok, show_result, msg) =
        session.execute_query(query_timeout, "SHOW MATERIALIZED VIEWS", None);
    if !ok {
        return Err(format!(
            "@imv_equivalence_check: SHOW MATERIALIZED VIEWS failed: {msg}"
        ));
    }
    let show_result = show_result.ok_or_else(|| {
        "@imv_equivalence_check: SHOW MATERIALIZED VIEWS returned no result".to_string()
    })?;

    // Find the column indices for "Name", "SelectText", and optionally "Database".
    let name_col = show_result
        .header
        .iter()
        .position(|h| h.eq_ignore_ascii_case("Name"))
        .ok_or_else(|| {
            "@imv_equivalence_check: SHOW MATERIALIZED VIEWS result has no 'Name' column"
                .to_string()
        })?;
    let select_text_col = show_result
        .header
        .iter()
        .position(|h| h.eq_ignore_ascii_case("SelectText"))
        .ok_or_else(|| {
            "@imv_equivalence_check: SHOW MATERIALIZED VIEWS result has no 'SelectText' column"
                .to_string()
        })?;
    // Database column is optional — if absent we fall back to bare-name match
    // but still apply the ambiguity check below.
    let db_col = show_result
        .header
        .iter()
        .position(|h| h.eq_ignore_ascii_case("Database"));

    // Find all rows whose name matches `mv` AND, when `db` is known and the
    // Database column is present, whose Database column also matches `db`.
    // This prevents silently binding the wrong SelectText when two MVs in
    // different databases of the same catalog share a bare name.
    let matched: Vec<&Vec<String>> = show_result
        .rows
        .iter()
        .filter(|row| {
            let name_matches = row
                .get(name_col)
                .map(|n| n.eq_ignore_ascii_case(mv))
                .unwrap_or(false);
            if !name_matches {
                return false;
            }
            // Apply db-qualification when both the caller supplied a non-empty
            // db and the SHOW output has a Database column.
            if let (Some(d), Some(dc)) = (db.filter(|d| !d.is_empty()), db_col) {
                row.get(dc)
                    .map(|dbname| dbname.eq_ignore_ascii_case(d))
                    .unwrap_or(false)
            } else {
                true
            }
        })
        .collect();

    let select_sql = match matched.len() {
        0 => {
            return Err(format!(
                "@imv_equivalence_check: MV `{mv}` not found in SHOW MATERIALIZED VIEWS output"
            ));
        }
        1 => matched[0]
            .get(select_text_col)
            .ok_or_else(|| {
                format!("@imv_equivalence_check: MV `{mv}` row has no SelectText value")
            })?
            .clone(),
        n => {
            return Err(format!(
                "@imv_equivalence_check: MV `{mv}` is ambiguous in SHOW MATERIALIZED VIEWS \
                 (qualify with a unique db); matched {n} rows"
            ));
        }
    };

    // SelectText is the canonical re-parseable CREATE-MV query body (sqlparser
    // Display round-trip, re-parsed on every refresh), so it is safe to re-run.
    let (ok, full, msg) = session.execute_query(query_timeout, &select_sql, None);
    if !ok {
        return Err(format!(
            "@imv_equivalence_check: full-recompute SELECT failed: {msg}"
        ));
    }
    let full = full.ok_or_else(|| {
        "@imv_equivalence_check: full-recompute SELECT returned no result".to_string()
    })?;
    match imv_equivalence_failure(&fqn, &inc, &full, epsilon) {
        Some(reason) => Err(reason),
        None => {
            let _ = writeln!(log, "    @imv_equivalence_check: incremental == full ✅");
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// @imv_stateless_rebuild helpers
// ---------------------------------------------------------------------------

/// A server that can rebuild at `available` can also serve any weaker
/// (lower-fidelity) requirement, since `ImvStatelessLevel`'s derived `Ord`
/// is defined in increasing fidelity order (Baseline < Package < Provenance < Full).
fn stateless_level_satisfies(available: ImvStatelessLevel, required: ImvStatelessLevel) -> bool {
    available >= required
}

/// Run `sql` and unwrap the `(bool, Option<QueryExecution>, String)` triple
/// returned by `MysqlSession::execute_query` into a single `Result`, so
/// `@imv_stateless_rebuild` call sites can use `?`.
fn execute_required_query(
    session: &mut crate::session::MysqlSession,
    query_timeout: u64,
    sql: &str,
) -> Result<crate::types::QueryExecution, String> {
    let (ok, execution, msg) = session.execute_query(query_timeout, sql, None);
    if !ok {
        return Err(format!("query failed: {sql}\n{msg}"));
    }
    execution.ok_or_else(|| format!("query returned no result: {sql}"))
}

/// Parse the `AvailableLevel` value out of the first row/column of the
/// `CALL <catalog>.system.novarocks_imv_stateless_rebuild(...)` result.
/// Matching is case-insensitive since the value crosses a wire boundary from
/// the server.
fn parse_available_stateless_level(
    exec: &crate::types::QueryExecution,
) -> Result<ImvStatelessLevel, String> {
    let raw = exec
        .rows
        .first()
        .and_then(|row| row.first())
        .ok_or_else(|| {
            "novarocks_imv_stateless_rebuild returned no AvailableLevel row".to_string()
        })?;
    match raw.to_ascii_lowercase().as_str() {
        "baseline" => Ok(ImvStatelessLevel::Baseline),
        "package" => Ok(ImvStatelessLevel::Package),
        "provenance" => Ok(ImvStatelessLevel::Provenance),
        "full" => Ok(ImvStatelessLevel::Full),
        other => Err(format!(
            "novarocks_imv_stateless_rebuild returned unknown AvailableLevel `{other}`"
        )),
    }
}

/// Trigger a stateless rebuild of `directive.mv` via the server-side
/// `novarocks_imv_stateless_rebuild` procedure and assert that (a) the
/// server's reported available fidelity level covers the requested level,
/// and (b) the MV's read face (`SELECT * FROM <fqn>`) is unchanged by the
/// rebuild — the trigger must not alter what the MV returns, only how it is
/// physically backed. MV is qualified by `db` like wait_alter_*.
fn run_imv_stateless_rebuild_check(
    directive: &ImvStatelessDirective,
    session: &mut crate::session::MysqlSession,
    query_timeout: u64,
    db: Option<&str>,
    epsilon: Option<f64>,
    log: &mut String,
) -> Result<(), String> {
    // When no case db is active, `fqn` is the bare MV name and both the SELECT
    // and the `table => '<mv>'` procedure argument resolve their namespace from
    // the session's current database. A case mounting this directive must have
    // established the catalog/db (e.g. via SET CATALOG / USE) on the shared
    // session before this step, as the iceberg-ivm cases do.
    let fqn = match db {
        Some(d) if !d.is_empty() => format!("{d}.{}", directive.mv),
        _ => directive.mv.clone(),
    };
    let select = format!("SELECT * FROM {fqn}");
    let _ = writeln!(
        log,
        "    @imv_stateless_rebuild: capturing read face of {fqn} before rebuild"
    );
    let before = execute_required_query(session, query_timeout, &select)?;

    let catalog = directive.catalog.as_deref().unwrap_or("ice");
    let call = format!(
        "CALL {catalog}.system.novarocks_imv_stateless_rebuild(table => '{fqn}', level => '{}')",
        directive.level.as_sql()
    );
    let _ = writeln!(log, "    @imv_stateless_rebuild: {call}");
    let procedure_rows = execute_required_query(session, query_timeout, &call)?;
    let available = parse_available_stateless_level(&procedure_rows)?;
    if !stateless_level_satisfies(available, directive.level) {
        return Err(format!(
            "@imv_stateless_rebuild: server only supports level {:?}, required {:?}",
            available, directive.level
        ));
    }

    let after = execute_required_query(session, query_timeout, &select)?;
    let (same, reason) = compare_result_sets(
        &before.header,
        &before.rows,
        &after.header,
        &after.rows,
        false,
        epsilon,
    );
    if !same {
        return Err(format!(
            "@imv_stateless_rebuild: SELECT result changed after lake rebuild for `{fqn}`\n{reason}"
        ));
    }
    let _ = writeln!(
        log,
        "    @imv_stateless_rebuild: {fqn} level {:?} passed ✅",
        directive.level
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// @explain_* helpers
// ---------------------------------------------------------------------------

/// If `sql` begins with an EXPLAIN prefix, strip it so the runner can
/// wrap `EXPLAIN VERBOSE` around the underlying query body. Handles
/// `EXPLAIN [VERBOSE|COSTS|ANALYZE]` case-insensitively.
fn explain_contains_target_body(sql: &str) -> String {
    let trimmed = sql.trim_start();
    let lower = trimmed.to_ascii_lowercase();
    for prefix in [
        "explain verbose ",
        "explain costs ",
        "explain analyze ",
        "explain ",
    ] {
        if lower.starts_with(prefix) {
            return trimmed[prefix.len()..].trim_start().to_string();
        }
    }
    trimmed.to_string()
}

/// Cheap leading-keyword sniff: SELECT/WITH and REFRESH MATERIALIZED VIEW
/// bodies are valid EXPLAIN targets. Other DDL and DML are explicitly
/// rejected for @explain_* directives.
fn is_select_or_with(body: &str) -> bool {
    let head = body
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    if head == "select" || head == "with" {
        return true;
    }
    let lower = body.trim_start().to_ascii_lowercase();
    lower.starts_with("refresh materialized view ")
}

fn explain_matching_lines(explain_text: &str, needle: &str) -> Vec<String> {
    explain_text
        .lines()
        .filter(|line| line.contains(needle))
        .map(ToString::to_string)
        .collect()
}

/// Run the `@explain_contains` and `@explain_not_contains` assertions for a
/// step: issue `EXPLAIN VERBOSE` on the step's SQL body and assert each
/// required/forbidden substring.
/// Returns `Ok(())` if all assertions pass, or `Err(message)` on the first failure.
fn run_explain_directive_checks(
    step: &SqlStep,
    session: &mut crate::session::MysqlSession,
    query_timeout: u64,
    log: &mut String,
) -> Result<(), String> {
    let body = explain_contains_target_body(&step.sql);
    if !is_select_or_with(&body) {
        return Err(format!(
            "@explain_contains/@explain_not_contains is only valid on SELECT / WITH statements (got: {})",
            body.split_whitespace().next().unwrap_or("(empty)")
        ));
    }
    let explain_sql = format!("EXPLAIN VERBOSE {}", body);
    let _ = writeln!(log, "    @explain directives: running EXPLAIN VERBOSE");
    let (ok, exec, msg) = session.execute_query(query_timeout, &explain_sql, None);
    if !ok {
        return Err(format!(
            "@explain directives: EXPLAIN VERBOSE failed.\n  error: {}",
            msg
        ));
    }
    let explain_text = exec.map(|e| e.text_output).unwrap_or_default();
    for needle in &step.meta.explain_contains {
        if !explain_text.contains(needle.as_str()) {
            return Err(format!(
                "@explain_contains assertion failed.\n  expected substring: {}\n  EXPLAIN VERBOSE output:\n{}",
                needle, explain_text
            ));
        }
    }
    for needle in &step.meta.explain_not_contains {
        if explain_text.contains(needle.as_str()) {
            let matching_lines = explain_matching_lines(&explain_text, needle);
            if matching_lines.is_empty() {
                return Err(format!(
                    "@explain_not_contains assertion failed.\n  forbidden substring: {}\n  EXPLAIN VERBOSE output:\n{}",
                    needle, explain_text
                ));
            } else {
                return Err(format!(
                    "@explain_not_contains assertion failed.\n  forbidden substring: {}\n  matching line(s):\n{}",
                    needle,
                    matching_lines.join("\n")
                ));
            }
        }
    }
    Ok(())
}

fn run_compat_directives_for_successful_step(
    step: &SqlStep,
    server_handle: &Arc<Mutex<Box<dyn ServerHandle>>>,
    snapshot: &compat_directive::BeLogSnapshot,
    log: &mut String,
) -> Result<(), String> {
    match server_handle.lock() {
        Ok(server_handle) => compat_directive::run(step, server_handle.as_ref(), snapshot, log)
            .map_err(|error| format!("compatibility directive failed: {error:#}")),
        Err(_) => Err("server handle mutex is poisoned".to_string()),
    }
}

fn execute_target_query_with_fault(
    meta: &QueryMeta,
    server_handle: &Arc<Mutex<Box<dyn ServerHandle>>>,
    session: &mut MysqlSession,
    query_timeout: u64,
    sql: &str,
    db: Option<&str>,
    fault_deadline: Option<Instant>,
) -> (bool, Option<QueryExecution>, String) {
    let query_timeout = bounded_fault_query_timeout(query_timeout, fault_deadline);
    let result = fault_injection::execute_with_post_fragment_start_fault(
        meta,
        server_handle,
        Some(session.connection_id()),
        fault_deadline,
        || session.execute_query(query_timeout, sql, db),
    )
    .unwrap_or_else(|error| {
        (
            false,
            None,
            format!("FAIL (runner fault injection): {error:#}"),
        )
    });
    if meta.kill_fe_after_control_ready_count.is_some() || meta.kill_fe_at_lifecycle_phase.is_some()
    {
        if fault_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return (
                false,
                None,
                fault_timeout_diagnostics(
                    server_handle,
                    "runner FE restart reconnect: shared lifecycle deadline expired",
                ),
            );
        }
        if let Err(error) = session.reconnect() {
            return (
                false,
                None,
                fault_timeout_diagnostics(
                    server_handle,
                    &format!("runner FE restart reconnect failed: {error:#}"),
                ),
            );
        }
        if fault_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return (
                false,
                None,
                fault_timeout_diagnostics(
                    server_handle,
                    "runner FE restart reconnect exceeded shared lifecycle deadline",
                ),
            );
        }
    }
    result
}

fn fault_timeout_diagnostics(
    server_handle: &Arc<Mutex<Box<dyn ServerHandle>>>,
    message: &str,
) -> String {
    let Ok(server) = server_handle.lock() else {
        return format!("FAIL ({message}); server handle mutex is poisoned");
    };
    let tail = |contents: String| {
        contents
            .lines()
            .rev()
            .take(20)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>()
            .join("\n")
    };
    let fe_tail = server
        .fe_log_contents()
        .map(tail)
        .unwrap_or_else(|error| format!("<read failed: {error:#}>"));
    let be_tails = (0..server.be_count())
        .map(|index| {
            server
                .be_log_contents(index)
                .map(tail)
                .unwrap_or_else(|error| format!("<read failed: {error:#}>"))
        })
        .collect::<Vec<_>>();
    format!("FAIL ({message}); fe_tail={fe_tail:?}; be_tails={be_tails:?}")
}

fn bounded_fault_query_timeout(query_timeout: u64, fault_deadline: Option<Instant>) -> u64 {
    const TERMINAL_EVIDENCE_RESERVE: Duration = Duration::from_secs(2);
    let Some(deadline) = fault_deadline else {
        return query_timeout;
    };
    let remaining = deadline.saturating_duration_since(Instant::now());
    let bounded = remaining
        .saturating_sub(TERMINAL_EVIDENCE_RESERVE)
        .as_secs()
        .max(1);
    query_timeout.min(bounded)
}

fn finish_expected_error_step(
    step: &SqlStep,
    matched_expected_error: bool,
    last_failure: &str,
    server_handle: &Arc<Mutex<Box<dyn ServerHandle>>>,
    snapshot: &compat_directive::BeLogSnapshot,
    log: &mut String,
) -> bool {
    if !matched_expected_error {
        let _ = writeln!(
            log,
            "    ❌ {}",
            annotate_failure_with_engine_error_code(last_failure, last_failure)
        );
        return false;
    }
    if let Err(reason) =
        run_compat_directives_for_successful_step(step, server_handle, snapshot, log)
    {
        let _ = writeln!(log, "    ❌ FAIL: {reason}");
        return false;
    }
    if let Some(expected_code) = step.meta.expect_error_code.as_deref() {
        let _ = writeln!(
            log,
            "    ✅ PASS (expected error matched): engine_error_code={} {}",
            expected_code, last_failure
        );
    } else {
        let _ = writeln!(
            log,
            "    ✅ PASS (expected error matched): {}",
            last_failure
        );
    }
    true
}

// Per-case execution
// ---------------------------------------------------------------------------

fn run_case(ctx: &SuiteRunContext, case: &SqlCase, abort: &AtomicBool) -> CaseOutcome {
    let mut log = String::with_capacity(2048);

    // Check global abort (fail-fast from another case)
    if abort.load(Ordering::Relaxed) {
        return CaseOutcome {
            case_id: case.case_id.clone(),
            status: CaseStatus::Skipped,
            elapsed: Duration::ZERO,
            log,
        };
    }

    // multi_step here controls the recorded-result format, not step execution.
    // Count only steps that need a recorded result set — DDL/DML and REFRESH
    // MV steps are implicit-skip and do not contribute to the expected-result
    // file structure.
    let result_bearing_steps: Vec<&SqlStep> = case
        .steps
        .iter()
        .filter(|step| step_requires_recorded_result(step))
        .collect();
    let multi_step = result_bearing_steps.len() > 1;
    let single_step_query_number = if !multi_step {
        result_bearing_steps.first().map(|s| s.query_number)
    } else {
        None
    };
    let case_path = ctx
        .result_dir
        .as_ref()
        .map(|dir| case_result_path(dir, &case.case_id));
    let case_requires_result_file = case.steps.iter().any(step_requires_recorded_result);
    let mut case_elapsed = Duration::from_secs(0);
    let mut case_failed = false;

    let _ = writeln!(
        log,
        "\n[{}] {} (steps={})",
        ctx.suite_name,
        case.case_id,
        case.steps.len()
    );

    // --- result_dir checks (verify / record) ---
    if matches!(ctx.mode, Mode::Verify | Mode::Record) {
        let Some(dir) = &ctx.result_dir else {
            let _ = writeln!(log, "    ❌ missing result_dir in {:?} mode", ctx.mode);
            if ctx.fail_fast {
                abort.store(true, Ordering::Relaxed);
            }
            return CaseOutcome {
                case_id: case.case_id.clone(),
                status: CaseStatus::Fail,
                elapsed: case_elapsed,
                log,
            };
        };

        match find_legacy_result_paths(dir, &case.case_id) {
            Ok(paths) if !paths.is_empty() => {
                let _ = writeln!(
                    log,
                    "    ❌ legacy split result files are no longer supported: {}",
                    paths
                        .iter()
                        .map(|p| p.display().to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                if ctx.fail_fast {
                    abort.store(true, Ordering::Relaxed);
                }
                return CaseOutcome {
                    case_id: case.case_id.clone(),
                    status: CaseStatus::Fail,
                    elapsed: case_elapsed,
                    log,
                };
            }
            Ok(_) => {}
            Err(exc) => {
                let _ = writeln!(log, "    ❌ failed to inspect result_dir: {}", exc);
                if ctx.fail_fast {
                    abort.store(true, Ordering::Relaxed);
                }
                return CaseOutcome {
                    case_id: case.case_id.clone(),
                    status: CaseStatus::Fail,
                    elapsed: case_elapsed,
                    log,
                };
            }
        }
    }

    // --- load expected results ---
    let expected_results = if ctx.mode == Mode::Verify && ctx.verify_enabled {
        if let Some(path) = case_path.as_ref().filter(|p| p.exists()) {
            match load_expected_results(path, multi_step, &ctx.marker_re, single_step_query_number)
            {
                Some(results) => Some(results),
                None => {
                    let _ = writeln!(
                        log,
                        "    ❌ failed to load expected result: {}",
                        path.display()
                    );
                    if ctx.fail_fast {
                        abort.store(true, Ordering::Relaxed);
                    }
                    return CaseOutcome {
                        case_id: case.case_id.clone(),
                        status: CaseStatus::Fail,
                        elapsed: case_elapsed,
                        log,
                    };
                }
            }
        } else {
            None
        }
    } else {
        None
    };

    // --- record mode pre-check ---
    if ctx.mode == Mode::Record
        && case_requires_result_file
        && case_path.as_ref().is_some_and(|p| p.exists())
        && !ctx.update_expected
    {
        let _ = writeln!(
            log,
            "    ❌ expected file exists ({}); rerun with --update-expected",
            case_path
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_default()
        );
        if ctx.fail_fast {
            abort.store(true, Ordering::Relaxed);
        }
        return CaseOutcome {
            case_id: case.case_id.clone(),
            status: CaseStatus::Fail,
            elapsed: case_elapsed,
            log,
        };
    }

    // --- per-case database isolation ---
    // Databases are determined by either:
    //   (a) ${case_db} placeholder detection in SQL (new mechanism), or
    //   (b) suite-level auto_case_db flag (legacy materialized-view mechanism).
    let case_dbs: Vec<String> = if !case.case_dbs.is_empty() {
        case.case_dbs.clone()
    } else if ctx.auto_case_db {
        vec![case_auto_db_name(&case.case_id)]
    } else {
        vec![]
    };
    let primary_case_db: Option<&str> = case_dbs.first().map(|s| s.as_str());
    let target_case_db_admin_conn = ConnectionConfig {
        db: None,
        ..ctx.target_conn_base.clone()
    };
    let reference_case_db_admin_conn = ConnectionConfig {
        db: None,
        ..ctx.reference_conn_base.clone()
    };

    // Helper closure: drop all case databases (best-effort).
    let drop_all_case_dbs = |ctx: &SuiteRunContext, dbs: &[String]| {
        for db in dbs {
            let _ = drop_case_database(&target_case_db_admin_conn, ctx.query_timeout, db, "target");
            if ctx.reference_required {
                let _ = drop_case_database(
                    &reference_case_db_admin_conn,
                    ctx.query_timeout,
                    db,
                    "reference",
                );
            }
        }
    };

    for db_name in &case_dbs {
        if let Err(exc) = reset_case_database(
            &target_case_db_admin_conn,
            ctx.query_timeout,
            db_name,
            "target",
        ) {
            drop_all_case_dbs(ctx, &case_dbs);
            let _ = writeln!(
                log,
                "    ❌ failed to prepare target case database {}: {:#}",
                db_name, exc
            );
            if ctx.fail_fast {
                abort.store(true, Ordering::Relaxed);
            }
            return CaseOutcome {
                case_id: case.case_id.clone(),
                status: CaseStatus::Fail,
                elapsed: case_elapsed,
                log,
            };
        }
        if ctx.reference_required {
            if let Err(exc) = reset_case_database(
                &reference_case_db_admin_conn,
                ctx.query_timeout,
                db_name,
                "reference",
            ) {
                drop_all_case_dbs(ctx, &case_dbs);
                let _ = writeln!(
                    log,
                    "    ❌ failed to prepare reference case database {}: {:#}",
                    db_name, exc
                );
                if ctx.fail_fast {
                    abort.store(true, Ordering::Relaxed);
                }
                return CaseOutcome {
                    case_id: case.case_id.clone(),
                    status: CaseStatus::Fail,
                    elapsed: case_elapsed,
                    log,
                };
            }
        }
    }

    // --- MySQL sessions ---
    let case_target_conn = ConnectionConfig {
        db: primary_case_db
            .map(|db| Some(db.to_string()))
            .unwrap_or_else(|| ctx.target_conn_base.db.clone()),
        ..ctx.target_conn_base.clone()
    };
    let case_reference_conn = ConnectionConfig {
        db: primary_case_db
            .map(|db| Some(db.to_string()))
            .unwrap_or_else(|| ctx.reference_conn_base.db.clone()),
        ..ctx.reference_conn_base.clone()
    };

    let mut target_session = match MysqlSession::new(&case_target_conn) {
        Ok(s) => s,
        Err(exc) => {
            drop_all_case_dbs(ctx, &case_dbs);
            let _ = writeln!(
                log,
                "    ❌ failed to create target mysql session: {:#}",
                exc
            );
            if ctx.fail_fast {
                abort.store(true, Ordering::Relaxed);
            }
            return CaseOutcome {
                case_id: case.case_id.clone(),
                status: CaseStatus::Fail,
                elapsed: case_elapsed,
                log,
            };
        }
    };

    let mut reference_session = if ctx.reference_required {
        match MysqlSession::new(&case_reference_conn) {
            Ok(s) => Some(s),
            Err(exc) => {
                drop_all_case_dbs(ctx, &case_dbs);
                let _ = writeln!(
                    log,
                    "    ❌ failed to create reference mysql session: {:#}",
                    exc
                );
                if ctx.fail_fast {
                    abort.store(true, Ordering::Relaxed);
                }
                return CaseOutcome {
                    case_id: case.case_id.clone(),
                    status: CaseStatus::Fail,
                    elapsed: case_elapsed,
                    log,
                };
            }
        }
    } else {
        None
    };

    if let Err(error) = execute_target_session_sql_with(
        &mut target_session,
        &ctx.target_session_sql,
        |session, statement| {
            let (ok, _, message) = session.execute_query(ctx.query_timeout, statement, None);
            if ok { Ok(()) } else { Err(message) }
        },
    ) {
        drop_all_case_dbs(ctx, &case_dbs);
        let _ = writeln!(log, "    ❌ {error}");
        if ctx.fail_fast {
            abort.store(true, Ordering::Relaxed);
        }
        return CaseOutcome {
            case_id: case.case_id.clone(),
            status: CaseStatus::Fail,
            elapsed: case_elapsed,
            log,
        };
    }

    // --- step execution loop ---
    let mut recorded_results: BTreeMap<usize, ResultSet> = BTreeMap::new();

    for step in &case.steps {
        let order_sensitive = query_order_sensitive(step, ctx.order_sensitive_default);
        let epsilon = query_float_epsilon(step, ctx.float_epsilon);

        let _ = writeln!(
            log,
            "  step {} (order_sensitive={}, epsilon={:?})",
            step.query_number, order_sensitive, epsilon
        );

        let compat_deadline = compat_directive::step_evidence_deadline(&step.meta);
        let resource_baseline = if fault_injection::has_fault(&step.meta) {
            match ctx.server_handle.lock() {
                Ok(mut server_handle)
                    if server_handle.supports_query_execution_resource_oracle() =>
                {
                    match server_handle.query_execution_resource_snapshot() {
                        Ok(Some(snapshot)) => Some(snapshot),
                        Ok(None) => {
                            case_failed = true;
                            let _ = writeln!(
                                log,
                                "    ❌ query-execution resource oracle was advertised but returned no baseline"
                            );
                            break;
                        }
                        Err(error) => {
                            case_failed = true;
                            let _ = writeln!(
                                log,
                                "    ❌ failed to capture pre-fault query-execution resource baseline: {error:#}"
                            );
                            break;
                        }
                    }
                }
                Ok(_) => None,
                Err(_) => {
                    case_failed = true;
                    let _ = writeln!(log, "    ❌ server handle mutex is poisoned");
                    break;
                }
            }
        } else {
            None
        };
        if fault_injection::has_fault(&step.meta) {
            let fault_result = ctx
                .server_handle
                .lock()
                .map_err(|_| anyhow::anyhow!("server handle mutex is poisoned"))
                .and_then(|mut server_handle| {
                    fault_injection::apply_pre_query(&step.meta, server_handle.as_mut())
                });
            if let Err(exc) = fault_result {
                case_failed = true;
                let _ = writeln!(
                    log,
                    "    ❌ failed to apply fault injection before step {}: {:#}",
                    step.query_number, exc
                );
                break;
            }
        }
        let _fragment_failure_guard = fault_injection::fragment_failure_step_guard(
            &step.meta,
            Arc::clone(&ctx.server_handle),
        );
        let _query_lifecycle_fault_guard = fault_injection::query_lifecycle_fault_step_guard(
            &step.meta,
            Arc::clone(&ctx.server_handle),
        );

        let compat_snapshot = match ctx.server_handle.lock() {
            Ok(server_handle) => compat_directive::snapshot_with_deadline(
                &step.meta,
                server_handle.as_ref(),
                compat_deadline,
            ),
            Err(_) => Err(anyhow::anyhow!("server handle mutex is poisoned")),
        };
        let compat_snapshot = match compat_snapshot {
            Ok(snapshot) => snapshot,
            Err(error) => {
                case_failed = true;
                let _ = writeln!(
                    log,
                    "    ❌ failed to snapshot compatibility evidence before step {}: {error:#}",
                    step.query_number
                );
                break;
            }
        };

        match ctx.mode {
            Mode::Verify => {
                let retry_count = step_retry_count(step);
                let retry_interval = step_retry_interval(step);
                let mut matched_expected_error = false;
                let mut passed_execution: Option<QueryExecution> = None;
                let mut last_execution: Option<QueryExecution> = None;
                let mut last_failure = String::new();

                for attempt in 0..retry_count {
                    let (ok, execution, err_msg) = if shell::is_shell_step(&step.sql) {
                        let cmd = step
                            .sql
                            .trim_start()
                            .strip_prefix("shell:")
                            .unwrap_or("")
                            .trim();
                        let exec = shell::execute_shell_command(cmd);
                        (true, Some(exec), String::new())
                    } else {
                        execute_target_query_with_fault(
                            &step.meta,
                            &ctx.server_handle,
                            &mut target_session,
                            ctx.query_timeout,
                            &step.sql,
                            step.meta.db.as_deref(),
                            compat_snapshot.evidence_deadline(),
                        )
                    };
                    let elapsed = execution
                        .as_ref()
                        .map(|result| result.elapsed)
                        .unwrap_or_default();
                    case_elapsed += elapsed;
                    last_execution = execution.clone();

                    if let Some(expected_result) =
                        evaluate_expected_error_branch(&step.meta, ok, &err_msg)
                    {
                        if let Err(reason) = expected_result {
                            last_failure =
                                annotate_failure_with_engine_error_code(&reason, &err_msg);
                        } else {
                            matched_expected_error = true;
                            last_failure = err_msg.clone();
                            break;
                        }
                    } else if !ok || execution.is_none() {
                        last_failure = annotate_failure_with_engine_error_code(
                            &format!("target execute failed: {}", err_msg),
                            &err_msg,
                        );
                    } else {
                        let execution = execution.expect("checked above");
                        let (assertions_ok, assertions_reason) =
                            verify_text_assertions(step, &execution);
                        if !assertions_ok {
                            last_failure = format!("VERIFY FAILED: {}", assertions_reason);
                        } else if !ctx.verify_enabled {
                            passed_execution = Some(execution);
                            break;
                        } else if step.meta.skip_result_check || step_has_implicit_skip_result(step)
                        {
                            passed_execution = Some(execution);
                            break;
                        } else if let Some(expected) = expected_results
                            .as_ref()
                            .and_then(|results| results.get(&step.query_number))
                        {
                            let (cmp_expected_rows, cmp_actual_rows) =
                                if step.meta.normalize_explain_timing {
                                    (
                                        normalize_explain_timing_rows(&expected.rows),
                                        normalize_explain_timing_rows(&execution.rows),
                                    )
                                } else {
                                    (expected.rows.clone(), execution.rows.clone())
                                };
                            let (same, reason) = compare_result_sets(
                                &expected.header,
                                &cmp_expected_rows,
                                &execution.header,
                                &cmp_actual_rows,
                                order_sensitive,
                                epsilon,
                            );
                            if same {
                                passed_execution = Some(execution);
                                break;
                            }
                            last_failure = format!("VERIFY FAILED: {}", reason);
                        } else if step_allows_missing_expected_result(step) {
                            passed_execution = Some(execution);
                            break;
                        } else if let Some(path) = &case_path {
                            if path.exists() {
                                last_failure = format!(
                                    "missing expected result section for step {} in {}",
                                    step.query_number,
                                    path.display()
                                );
                            } else {
                                last_failure =
                                    format!("missing expected result file: {}", path.display());
                            }
                        } else {
                            last_failure = "missing result_dir in verify mode".to_string();
                        }
                    }

                    if attempt + 1 < retry_count {
                        let _ = writeln!(
                            log,
                            "    ⏳ retrying attempt {}/{} after {}ms: {}",
                            attempt + 2,
                            retry_count,
                            retry_interval.as_millis(),
                            last_failure
                        );
                        sleep(retry_interval);
                    }
                }

                if step.meta.expect_error_code.is_some() || step.meta.expect_error.is_some() {
                    if !finish_expected_error_step(
                        step,
                        matched_expected_error,
                        &last_failure,
                        &ctx.server_handle,
                        &compat_snapshot,
                        &mut log,
                    ) {
                        case_failed = true;
                    }
                } else if let Some(execution) = passed_execution {
                    // Run wait_alter post-execution polling if annotated.
                    let (wait_ok, wait_elapsed) = run_step_wait_alters(
                        step,
                        &mut target_session,
                        ctx.query_timeout,
                        primary_case_db,
                        &mut log,
                    );
                    case_elapsed += wait_elapsed;
                    if !wait_ok {
                        case_failed = true;
                    } else {
                        let compat_ok = run_compat_directives_for_successful_step(
                            step,
                            &ctx.server_handle,
                            &compat_snapshot,
                            &mut log,
                        );
                        let compat_ok = match compat_ok {
                            Ok(()) => true,
                            Err(reason) => {
                                let _ = writeln!(log, "    ❌ FAIL: {reason}");
                                case_failed = true;
                                false
                            }
                        };
                        // @imv_stateless_rebuild: trigger the lake-native stateless
                        // rebuild procedure for the named MV and assert its read
                        // face is unchanged. Runs before @imv_equivalence_check so
                        // that, when both directives are present on a step, the
                        // equivalence oracle below validates the rebuilt MV.
                        if let Some(directive) = step.meta.imv_stateless_rebuild.as_ref() {
                            if let Err(reason) = run_imv_stateless_rebuild_check(
                                directive,
                                &mut target_session,
                                ctx.query_timeout,
                                step.meta.db.as_deref().or(primary_case_db),
                                epsilon,
                                &mut log,
                            ) {
                                let _ = writeln!(log, "    ❌ FAIL: {reason}");
                                case_failed = true;
                            }
                        }
                        // @imv_equivalence_check: assert MV incremental contents
                        // == a full recompute derived by running the MV's SelectText
                        // directly against the base tables (no MV side effects).
                        // Verify-mode only by design: diff mode compares against a
                        // reference engine (no full-recompute oracle here), and the
                        // check needs the MV's own SelectText which only verify drives.
                        if let Some(mv) = step.meta.imv_equivalence_check.as_deref() {
                            if let Err(reason) = run_imv_equivalence_check(
                                mv,
                                &mut target_session,
                                ctx.query_timeout,
                                step.meta.db.as_deref().or(primary_case_db),
                                epsilon,
                                &mut log,
                            ) {
                                let _ = writeln!(log, "    ❌ FAIL: {reason}");
                                case_failed = true;
                            }
                        }
                        // @explain_*: issue EXPLAIN VERBOSE and assert substrings.
                        let explain_ok = if !step.meta.explain_contains.is_empty()
                            || !step.meta.explain_not_contains.is_empty()
                        {
                            match run_explain_directive_checks(
                                step,
                                &mut target_session,
                                ctx.query_timeout,
                                &mut log,
                            ) {
                                Ok(()) => true,
                                Err(msg) => {
                                    case_failed = true;
                                    let _ = writeln!(log, "    ❌ {}", msg);
                                    false
                                }
                            }
                        } else {
                            true
                        };
                        if explain_ok && compat_ok {
                            if !ctx.verify_enabled {
                                let _ = writeln!(
                                    log,
                                    "    ✅ PASS (verify disabled) ({:.2}s)",
                                    execution.elapsed.as_secs_f64()
                                );
                            } else if step.meta.skip_result_check
                                || step_has_implicit_skip_result(step)
                            {
                                let _ = writeln!(
                                    log,
                                    "    ✅ PASS ({:.2}s, skip_result_check)",
                                    execution.elapsed.as_secs_f64()
                                );
                            } else if expected_results
                                .as_ref()
                                .and_then(|results| results.get(&step.query_number))
                                .is_some()
                            {
                                let _ = writeln!(
                                    log,
                                    "    ✅ PASS ({:.2}s, rows={})",
                                    execution.elapsed.as_secs_f64(),
                                    execution.rows.len()
                                );
                                for row in execution.rows.iter().take(ctx.preview_lines) {
                                    let _ = writeln!(log, "    {:?}", row);
                                }
                            } else {
                                let _ = writeln!(
                                    log,
                                    "    ✅ PASS ({:.2}s, text assertions only)",
                                    execution.elapsed.as_secs_f64()
                                );
                            }
                        }
                    }
                } else {
                    case_failed = true;
                    let _ = writeln!(
                        log,
                        "    ❌ {}",
                        annotate_failure_with_engine_error_code(&last_failure, &last_failure)
                    );
                    if let (Some(root), Some(expected), Some(execution)) = (
                        ctx.actual_artifact_dir.as_ref(),
                        expected_results
                            .as_ref()
                            .and_then(|results| results.get(&step.query_number)),
                        last_execution.as_ref(),
                    ) {
                        if last_failure.starts_with("VERIFY FAILED: ") {
                            let artifact_id =
                                format!("{}-query{}", case.case_id, step.query_number);
                            let reason = last_failure
                                .trim_start_matches("VERIFY FAILED: ")
                                .to_string();
                            if let Err(exc) = write_mismatch_artifacts(
                                root,
                                &ctx.suite_name,
                                &artifact_id,
                                &expected.header,
                                &expected.rows,
                                &execution.header,
                                &execution.rows,
                                &reason,
                            ) {
                                let _ = writeln!(
                                    log,
                                    "    ⚠️ failed to write mismatch artifacts: {}",
                                    exc
                                );
                            }
                        }
                    }
                }
            }
            Mode::Record => {
                let retry_count = step_retry_count(step);
                let retry_interval = step_retry_interval(step);
                let mut matched_expected_error = false;
                let mut recorded_execution: Option<QueryExecution> = None;
                let mut last_failure = String::new();

                for attempt in 0..retry_count {
                    let (ok, execution, err_msg) = if ctx.record_from == RecordFrom::Target {
                        if shell::is_shell_step(&step.sql) {
                            let cmd = step
                                .sql
                                .trim_start()
                                .strip_prefix("shell:")
                                .unwrap_or("")
                                .trim();
                            let exec = shell::execute_shell_command(cmd);
                            (true, Some(exec), String::new())
                        } else {
                            execute_target_query_with_fault(
                                &step.meta,
                                &ctx.server_handle,
                                &mut target_session,
                                ctx.query_timeout,
                                &step.sql,
                                step.meta.db.as_deref(),
                                compat_snapshot.evidence_deadline(),
                            )
                        }
                    } else if shell::is_shell_step(&step.sql) {
                        let cmd = step
                            .sql
                            .trim_start()
                            .strip_prefix("shell:")
                            .unwrap_or("")
                            .trim();
                        let exec = shell::execute_shell_command(cmd);
                        (true, Some(exec), String::new())
                    } else {
                        reference_session
                            .as_mut()
                            .expect("reference session required in record-from=reference")
                            .execute_query(ctx.query_timeout, &step.sql, step.meta.db.as_deref())
                    };
                    let elapsed = execution
                        .as_ref()
                        .map(|result| result.elapsed)
                        .unwrap_or_default();
                    case_elapsed += elapsed;

                    if let Some(expected_result) =
                        evaluate_expected_error_branch(&step.meta, ok, &err_msg)
                    {
                        if let Err(reason) = expected_result {
                            last_failure = reason;
                        } else {
                            matched_expected_error = true;
                            last_failure = err_msg.clone();
                            break;
                        }
                    } else if !ok || execution.is_none() {
                        last_failure = format!("record source execute failed: {}", err_msg);
                    } else {
                        let execution = execution.expect("checked above");
                        let (assertions_ok, assertions_reason) =
                            verify_text_assertions(step, &execution);
                        if !assertions_ok {
                            last_failure = format!("VERIFY FAILED: {}", assertions_reason);
                        } else {
                            recorded_execution = Some(execution);
                            break;
                        }
                    }

                    if attempt + 1 < retry_count {
                        let _ = writeln!(
                            log,
                            "    ⏳ retrying attempt {}/{} after {}ms: {}",
                            attempt + 2,
                            retry_count,
                            retry_interval.as_millis(),
                            last_failure
                        );
                        sleep(retry_interval);
                    }
                }

                let expected_error_compat_ok =
                    if matched_expected_error && ctx.record_from == RecordFrom::Target {
                        match run_compat_directives_for_successful_step(
                            step,
                            &ctx.server_handle,
                            &compat_snapshot,
                            &mut log,
                        ) {
                            Ok(()) => true,
                            Err(reason) => {
                                case_failed = true;
                                last_failure = reason;
                                false
                            }
                        }
                    } else {
                        true
                    };

                if let Some(expected_code) = step.meta.expect_error_code.as_deref() {
                    if matched_expected_error && expected_error_compat_ok {
                        let _ = writeln!(
                            log,
                            "    ✅ RECORDED EXPECTED ERROR: engine_error_code={} {}",
                            expected_code, last_failure
                        );
                    } else {
                        case_failed = true;
                        let _ = writeln!(log, "    ❌ {}", last_failure);
                    }
                } else if step.meta.expect_error.is_some() {
                    if matched_expected_error && expected_error_compat_ok {
                        let _ = writeln!(log, "    ✅ RECORDED EXPECTED ERROR: {}", last_failure);
                    } else {
                        case_failed = true;
                        let _ = writeln!(log, "    ❌ {}", last_failure);
                    }
                } else if let Some(execution) = recorded_execution {
                    // Run wait_alter post-execution polling if annotated.
                    let (wait_ok, wait_elapsed) = run_step_wait_alters(
                        step,
                        &mut target_session,
                        ctx.query_timeout,
                        primary_case_db,
                        &mut log,
                    );
                    case_elapsed += wait_elapsed;
                    if !wait_ok {
                        case_failed = true;
                    } else {
                        let compat_ok = if ctx.record_from == RecordFrom::Target {
                            match run_compat_directives_for_successful_step(
                                step,
                                &ctx.server_handle,
                                &compat_snapshot,
                                &mut log,
                            ) {
                                Ok(()) => true,
                                Err(reason) => {
                                    case_failed = true;
                                    let _ = writeln!(log, "    ❌ FAIL: {reason}");
                                    false
                                }
                            }
                        } else {
                            true
                        };
                        // @explain_*: validate during record too.
                        let explain_ok = compat_ok
                            && if !step.meta.explain_contains.is_empty()
                                || !step.meta.explain_not_contains.is_empty()
                            {
                                match run_explain_directive_checks(
                                    step,
                                    &mut target_session,
                                    ctx.query_timeout,
                                    &mut log,
                                ) {
                                    Ok(()) => true,
                                    Err(msg) => {
                                        case_failed = true;
                                        let _ = writeln!(log, "    ❌ {}", msg);
                                        false
                                    }
                                }
                            } else {
                                true
                            };
                        if explain_ok {
                            if step_requires_recorded_result(step) {
                                let record_rows = if step.meta.normalize_explain_timing {
                                    normalize_explain_timing_rows(&execution.rows)
                                } else {
                                    execution.rows.clone()
                                };
                                recorded_results.insert(
                                    step.query_number,
                                    ResultSet {
                                        header: execution.header.clone(),
                                        rows: record_rows,
                                    },
                                );
                            }
                            if step.meta.skip_result_check || step_has_implicit_skip_result(step) {
                                let _ = writeln!(
                                    log,
                                    "    ✅ STEP RECORDED ({:.2}s, skip_result_check)",
                                    execution.elapsed.as_secs_f64()
                                );
                            } else {
                                let _ = writeln!(
                                    log,
                                    "    ✅ STEP RECORDED ({:.2}s, rows={})",
                                    execution.elapsed.as_secs_f64(),
                                    execution.rows.len()
                                );
                            }
                        }
                    }
                } else {
                    case_failed = true;
                    let _ = writeln!(log, "    ❌ {}", last_failure);
                }
            }
            Mode::Diff => {
                if let Some(expected_code) = step.meta.expect_error_code.as_deref() {
                    let (ok_t, execution_t, err_t) = if shell::is_shell_step(&step.sql) {
                        let cmd = step
                            .sql
                            .trim_start()
                            .strip_prefix("shell:")
                            .unwrap_or("")
                            .trim();
                        let exec = shell::execute_shell_command(cmd);
                        (true, Some(exec), String::new())
                    } else {
                        execute_target_query_with_fault(
                            &step.meta,
                            &ctx.server_handle,
                            &mut target_session,
                            ctx.query_timeout,
                            &step.sql,
                            step.meta.db.as_deref(),
                            compat_snapshot.evidence_deadline(),
                        )
                    };
                    let (ok_r, execution_r, err_r) = if shell::is_shell_step(&step.sql) {
                        let cmd = step
                            .sql
                            .trim_start()
                            .strip_prefix("shell:")
                            .unwrap_or("")
                            .trim();
                        let exec = shell::execute_shell_command(cmd);
                        (true, Some(exec), String::new())
                    } else {
                        reference_session
                            .as_mut()
                            .expect("reference session required in diff mode")
                            .execute_query(ctx.query_timeout, &step.sql, step.meta.db.as_deref())
                    };
                    let elapsed = execution_t.as_ref().map(|r| r.elapsed).unwrap_or_default()
                        + execution_r.as_ref().map(|r| r.elapsed).unwrap_or_default();
                    case_elapsed += elapsed;

                    let expected_code_result = expected_engine_error_code_diff_result(
                        expected_code,
                        ok_t,
                        &err_t,
                        ok_r,
                        &err_r,
                    );
                    if expected_code_result.is_ok() {
                        let _ = writeln!(
                            log,
                            "    ✅ DIFF PASS (both sides matched expected error: engine_error_code={})",
                            expected_code
                        );
                    } else {
                        case_failed = true;
                        let _ = writeln!(
                            log,
                            "    ❌ DIFF FAILED {}",
                            expected_code_result.expect_err("checked mismatch above")
                        );
                    }
                } else if let Some(expected_error) = step.meta.expect_error.as_deref() {
                    let (ok_t, execution_t, err_t) = if shell::is_shell_step(&step.sql) {
                        let cmd = step
                            .sql
                            .trim_start()
                            .strip_prefix("shell:")
                            .unwrap_or("")
                            .trim();
                        let exec = shell::execute_shell_command(cmd);
                        (true, Some(exec), String::new())
                    } else {
                        execute_target_query_with_fault(
                            &step.meta,
                            &ctx.server_handle,
                            &mut target_session,
                            ctx.query_timeout,
                            &step.sql,
                            step.meta.db.as_deref(),
                            compat_snapshot.evidence_deadline(),
                        )
                    };
                    let (ok_r, execution_r, err_r) = if shell::is_shell_step(&step.sql) {
                        let cmd = step
                            .sql
                            .trim_start()
                            .strip_prefix("shell:")
                            .unwrap_or("")
                            .trim();
                        let exec = shell::execute_shell_command(cmd);
                        (true, Some(exec), String::new())
                    } else {
                        reference_session
                            .as_mut()
                            .expect("reference session required in diff mode")
                            .execute_query(ctx.query_timeout, &step.sql, step.meta.db.as_deref())
                    };
                    let elapsed = execution_t.as_ref().map(|r| r.elapsed).unwrap_or_default()
                        + execution_r.as_ref().map(|r| r.elapsed).unwrap_or_default();
                    case_elapsed += elapsed;

                    let target_matched = !ok_t && error_message_matches(&err_t, expected_error);
                    let reference_matched = !ok_r && error_message_matches(&err_r, expected_error);
                    if target_matched && reference_matched {
                        let _ = writeln!(
                            log,
                            "    ✅ DIFF PASS (both sides matched expected error: {:?})",
                            expected_error
                        );
                    } else {
                        case_failed = true;
                        let _ = writeln!(
                            log,
                            "    ❌ DIFF FAILED expected error {:?} (target_ok={}, target_err={}, reference_ok={}, reference_err={})",
                            expected_error, ok_t, err_t, ok_r, err_r
                        );
                    }
                } else {
                    let (ok_t, execution_t, err_t) = if shell::is_shell_step(&step.sql) {
                        let cmd = step
                            .sql
                            .trim_start()
                            .strip_prefix("shell:")
                            .unwrap_or("")
                            .trim();
                        let exec = shell::execute_shell_command(cmd);
                        (true, Some(exec), String::new())
                    } else {
                        execute_target_query_with_fault(
                            &step.meta,
                            &ctx.server_handle,
                            &mut target_session,
                            ctx.query_timeout,
                            &step.sql,
                            step.meta.db.as_deref(),
                            compat_snapshot.evidence_deadline(),
                        )
                    };
                    if !ok_t || execution_t.is_none() {
                        case_failed = true;
                        let failure = annotate_failure_with_engine_error_code(
                            &format!("target execute failed: {}", err_t),
                            &err_t,
                        );
                        let _ = writeln!(log, "    ❌ {}", failure);
                    } else {
                        let execution_t = execution_t.expect("checked above");
                        let (ok_r, execution_r, err_r) = if shell::is_shell_step(&step.sql) {
                            let cmd = step
                                .sql
                                .trim_start()
                                .strip_prefix("shell:")
                                .unwrap_or("")
                                .trim();
                            let exec = shell::execute_shell_command(cmd);
                            (true, Some(exec), String::new())
                        } else {
                            reference_session
                                .as_mut()
                                .expect("reference session required in diff mode")
                                .execute_query(
                                    ctx.query_timeout,
                                    &step.sql,
                                    step.meta.db.as_deref(),
                                )
                        };
                        if !ok_r || execution_r.is_none() {
                            case_failed = true;
                            case_elapsed += execution_t.elapsed;
                            let _ = writeln!(log, "    ❌ reference execute failed: {}", err_r);
                        } else {
                            let execution_r = execution_r.expect("checked above");
                            let elapsed = execution_t.elapsed + execution_r.elapsed;
                            case_elapsed += elapsed;

                            let (same, reason) = compare_result_sets(
                                &execution_r.header,
                                &execution_r.rows,
                                &execution_t.header,
                                &execution_t.rows,
                                order_sensitive,
                                epsilon,
                            );
                            if same {
                                let _ = writeln!(
                                    log,
                                    "    ✅ DIFF PASS (target={:.2}s, reference={:.2}s)",
                                    execution_t.elapsed.as_secs_f64(),
                                    execution_r.elapsed.as_secs_f64()
                                );
                            } else {
                                case_failed = true;
                                let _ = writeln!(log, "    ❌ DIFF FAILED: {}", reason);
                                if let Some(root) = &ctx.actual_artifact_dir {
                                    let artifact_id =
                                        format!("{}-query{}", case.case_id, step.query_number);
                                    if let Err(exc) = write_mismatch_artifacts(
                                        root,
                                        &ctx.suite_name,
                                        &artifact_id,
                                        &execution_r.header,
                                        &execution_r.rows,
                                        &execution_t.header,
                                        &execution_t.rows,
                                        &reason,
                                    ) {
                                        let _ = writeln!(
                                            log,
                                            "    ⚠️ failed to write mismatch artifacts: {}",
                                            exc
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        if let Some(baseline) = resource_baseline {
            let convergence_deadline = Instant::now() + Duration::from_secs(30);
            let resource_result = ctx
                .server_handle
                .lock()
                .map_err(|_| anyhow::anyhow!("server handle mutex is poisoned"))
                .and_then(|mut server_handle| {
                    server_handle
                        .await_query_execution_resource_convergence(
                            &baseline,
                            fault_injection::permits_terminal_retention(&step.meta),
                            convergence_deadline,
                        )
                });
            match resource_result {
                Ok(()) => {
                    let _ = writeln!(
                        log,
                        "    ✅ query-execution resources converged to the pre-fault baseline"
                    );
                }
                Err(error) => {
                    case_failed = true;
                    let _ = writeln!(
                        log,
                        "    ❌ query-execution resource convergence failed: {error:#}"
                    );
                }
            }
        }

        if case_failed {
            let _ = writeln!(log, "    ⏭️ skipping remaining steps in {}", case.case_id);
            break;
        }
    }

    // --- cleanup ---
    drop(target_session);
    drop(reference_session);

    for db_name in &case_dbs {
        if let Err(exc) = drop_case_database(
            &target_case_db_admin_conn,
            ctx.query_timeout,
            db_name,
            "target",
        ) {
            case_failed = true;
            let _ = writeln!(
                log,
                "    ❌ failed to cleanup target case database {}: {:#}",
                db_name, exc
            );
        }
        if ctx.reference_required {
            if let Err(exc) = drop_case_database(
                &reference_case_db_admin_conn,
                ctx.query_timeout,
                db_name,
                "reference",
            ) {
                case_failed = true;
                let _ = writeln!(
                    log,
                    "    ❌ failed to cleanup reference case database {}: {:#}",
                    db_name, exc
                );
            }
        }
    }

    if !case_failed && ctx.mode == Mode::Record && case_requires_result_file {
        if let Some(path) = case_path.as_ref() {
            if let Err(exc) = write_result_file(path, &recorded_results, multi_step) {
                case_failed = true;
                let _ = writeln!(log, "    ❌ failed to write expected result: {}", exc);
            } else {
                let _ = writeln!(log, "    ✅ RECORDED CASE -> {}", path.display());
            }
        }
    }

    let status = if case_failed {
        CaseStatus::Fail
    } else {
        CaseStatus::Pass
    };

    if case_failed && ctx.fail_fast {
        abort.store(true, Ordering::Relaxed);
    }

    CaseOutcome {
        case_id: case.case_id.clone(),
        status,
        elapsed: case_elapsed,
        log,
    }
}

// ---------------------------------------------------------------------------
// Per-suite execution (init -> parallel cases -> cleanup)
// ---------------------------------------------------------------------------

fn run_suite(ps: &PreparedSuite, abort: &AtomicBool, stdout_lock: &Mutex<()>) -> SuiteOutcome {
    let wall_start = Instant::now();
    let ctx = &ps.ctx;
    let total = ps.cases.len();
    let pass_count = AtomicUsize::new(0);
    let fail_count = AtomicUsize::new(0);

    // --- suite init hook ---
    if let Some(hook) = ps.init_hook.as_ref() {
        {
            let _guard = stdout_lock.lock().unwrap();
            println!(
                "[{}] running suite init on target: {}",
                ctx.suite_name,
                hook.path.display()
            );
        }
        if let Err(exc) =
            execute_suite_hook(&ctx.target_admin_conn, ctx.query_timeout, hook, "target")
        {
            if let Some(cleanup) = ps.cleanup_hook.as_ref() {
                let _ = execute_suite_hook(
                    &ctx.target_admin_conn,
                    ctx.query_timeout,
                    cleanup,
                    "target cleanup after init failure",
                );
            }
            let _guard = stdout_lock.lock().unwrap();
            println!("[{}] ❌ suite init failed: {}", ctx.suite_name, exc);
            let outcomes: Vec<CaseOutcome> = ps
                .cases
                .iter()
                .map(|case| CaseOutcome {
                    case_id: case.case_id.clone(),
                    status: CaseStatus::Fail,
                    elapsed: Duration::ZERO,
                    log: format!(
                        "\n[{}] {} (steps={})\n    ❌ suite init failed\n",
                        ctx.suite_name,
                        case.case_id,
                        case.steps.len(),
                    ),
                })
                .collect();
            return SuiteOutcome {
                suite_name: ctx.suite_name.clone(),
                total,
                outcomes,
                cleanup_errors: vec![],
                wall_time: wall_start.elapsed(),
            };
        }
        if ctx.reference_required {
            {
                let _guard = stdout_lock.lock().unwrap();
                println!(
                    "[{}] running suite init on reference: {}",
                    ctx.suite_name,
                    hook.path.display()
                );
            }
            if let Err(exc) = execute_suite_hook(
                &ctx.reference_admin_conn,
                ctx.query_timeout,
                hook,
                "reference",
            ) {
                if let Some(cleanup) = ps.cleanup_hook.as_ref() {
                    let _ = execute_suite_hook(
                        &ctx.reference_admin_conn,
                        ctx.query_timeout,
                        cleanup,
                        "reference cleanup after init failure",
                    );
                    let _ = execute_suite_hook(
                        &ctx.target_admin_conn,
                        ctx.query_timeout,
                        cleanup,
                        "target cleanup after init failure",
                    );
                }
                let _guard = stdout_lock.lock().unwrap();
                println!(
                    "[{}] ❌ suite reference init failed: {}",
                    ctx.suite_name, exc
                );
                let outcomes: Vec<CaseOutcome> = ps
                    .cases
                    .iter()
                    .map(|case| CaseOutcome {
                        case_id: case.case_id.clone(),
                        status: CaseStatus::Fail,
                        elapsed: Duration::ZERO,
                        log: format!(
                            "\n[{}] {} (steps={})\n    ❌ suite reference init failed\n",
                            ctx.suite_name,
                            case.case_id,
                            case.steps.len(),
                        ),
                    })
                    .collect();
                return SuiteOutcome {
                    suite_name: ctx.suite_name.clone(),
                    total,
                    outcomes,
                    cleanup_errors: vec![],
                    wall_time: wall_start.elapsed(),
                };
            }
        }
    }

    // --- split cases into parallel and sequential groups ---
    let (sequential_cases, parallel_cases): (Vec<&SqlCase>, Vec<&SqlCase>) =
        ps.cases.iter().partition(|c| c.sequential);

    let report_outcome = |outcome: &CaseOutcome| {
        match outcome.status {
            CaseStatus::Pass => {
                pass_count.fetch_add(1, Ordering::Relaxed);
            }
            CaseStatus::Fail => {
                fail_count.fetch_add(1, Ordering::Relaxed);
            }
            CaseStatus::Skipped => {}
        }
        let p = pass_count.load(Ordering::Relaxed);
        let f = fail_count.load(Ordering::Relaxed);
        {
            let _guard = stdout_lock.lock().unwrap();
            print!("{}", outcome.log);
            if outcome.status != CaseStatus::Skipped {
                println!(
                    "    [{}] progress: pass={}, fail={}, total={}",
                    ctx.suite_name, p, f, total
                );
            }
        }
    };

    // Run parallel cases first
    let mut outcomes: Vec<CaseOutcome> = parallel_cases
        .par_iter()
        .map(|case| {
            let outcome = run_case(ctx, case, abort);
            report_outcome(&outcome);
            outcome
        })
        .collect();

    // Then run sequential cases one by one
    for case in &sequential_cases {
        let outcome = run_case(ctx, case, abort);
        report_outcome(&outcome);
        outcomes.push(outcome);
    }

    // --- suite cleanup hook ---
    let mut cleanup_errors = Vec::new();
    if let Some(hook) = ps.cleanup_hook.as_ref() {
        {
            let _guard = stdout_lock.lock().unwrap();
            println!(
                "\n[{}] running suite cleanup on target: {}",
                ctx.suite_name,
                hook.path.display()
            );
        }
        if let Err(exc) =
            execute_suite_hook(&ctx.target_admin_conn, ctx.query_timeout, hook, "target")
        {
            cleanup_errors.push(format!("[{}] {}", ctx.suite_name, exc));
        }
        if ctx.reference_required {
            {
                let _guard = stdout_lock.lock().unwrap();
                println!(
                    "[{}] running suite cleanup on reference: {}",
                    ctx.suite_name,
                    hook.path.display()
                );
            }
            if let Err(exc) = execute_suite_hook(
                &ctx.reference_admin_conn,
                ctx.query_timeout,
                hook,
                "reference",
            ) {
                cleanup_errors.push(format!("[{}] {}", ctx.suite_name, exc));
            }
        }
    }

    SuiteOutcome {
        suite_name: ctx.suite_name.clone(),
        total,
        outcomes,
        cleanup_errors,
        wall_time: wall_start.elapsed(),
    }
}

fn case_status_label(status: CaseStatus) -> &'static str {
    match status {
        CaseStatus::Pass => "PASS",
        CaseStatus::Fail => "FAIL",
        CaseStatus::Skipped => "SKIPPED",
    }
}

fn format_case_timings(timings: &[CaseTiming]) -> String {
    let mut out = String::from("\ncase timings (all):\n");
    for timing in timings {
        let _ = writeln!(
            out,
            "  [{}] {} {} {:.2}s",
            timing.suite_name,
            timing.case_id,
            case_status_label(timing.status),
            timing.elapsed.as_secs_f64()
        );
    }
    out
}

fn cases_have_fault_directives(cases: &[SqlCase]) -> bool {
    cases
        .iter()
        .flat_map(|case| case.steps.iter())
        .any(|step| fault_injection::has_fault(&step.meta))
}

fn validate_fault_injection_jobs(cases: &[SqlCase], jobs: usize) -> Result<()> {
    if jobs != 1 && cases_have_fault_directives(cases) {
        bail!(
            "fault injection directives require -j 1 because they mutate the shared cross-process cluster"
        );
    }
    Ok(())
}

fn sql_text_has_query_lifecycle_fault_directive(sql: &str) -> bool {
    const DIRECTIVES: &[&str] = &[
        "drop_next_init_ack_be_index",
        "stop_query_control_heartbeat_be_index",
        "kill_fe_after_control_ready_count",
        "restart_be_after_init_ack_index",
        "kill_query_after_control_ready_count",
        "fail_stage_prepare_ordinal",
        "drop_next_stage_ack_be_index",
        "drop_next_start_ack_be_index",
        "suppress_start_ack_be_index",
        "drop_next_terminal_ack_be_index",
        "drop_terminal_snapshot_stream_be_index",
        "terminal_snapshot_conflict_be_index",
        "kill_query_at_lifecycle_phase",
        "kill_fe_at_lifecycle_phase",
        "stop_query_control_heartbeat_after_stage_be_index",
        "hold_start_until_early_ingress",
        "query_control_fragment_backend_limit",
    ];
    sql.lines().any(|line| {
        let line = line.trim_start();
        DIRECTIVES
            .iter()
            .any(|directive| line.starts_with(&format!("-- @{directive}=")))
    })
}

fn validate_selected_suite_cluster(
    suite_names: &[String],
    mode: ClusterMode,
    cluster_size: usize,
) -> Result<()> {
    if suite_names
        .iter()
        .any(|suite| suite == "distributed-resilience")
        && (mode != ClusterMode::CrossProcess || cluster_size != 3)
    {
        bail!("distributed-resilience requires --cluster-mode cross-process --cluster-size 3");
    }
    Ok(())
}

fn selected_cases_require_query_lifecycle_faults(
    cli: &Cli,
    suite_names: &[String],
    suite_configs: &BTreeMap<String, SuiteConfig>,
    base_dir: &std::path::Path,
) -> Result<bool> {
    for suite_name in suite_names {
        let suite = suite_configs
            .get(suite_name)
            .with_context(|| format!("selected suite {suite_name} is missing"))?;
        let sql_dir =
            resolve_path(cli.sql_dir.as_deref(), base_dir).unwrap_or_else(|| suite.sql_dir.clone());
        let sql_glob = cli
            .sql_glob
            .clone()
            .unwrap_or_else(|| suite.sql_glob.clone());
        let mut files = list_sql_files(&sql_dir, &sql_glob)?;
        let available = files
            .iter()
            .filter_map(|path| path.file_stem().and_then(|stem| stem.to_str()))
            .map(ToOwned::to_owned)
            .collect::<HashSet<_>>();
        let only = parse_selector_list(cli.only.as_deref(), &available, "--only")?;
        let skip = parse_selector_list(cli.skip.as_deref(), &available, "--skip")?;
        files.retain(|path| {
            let Some(case_id) = path.file_stem().and_then(|stem| stem.to_str()) else {
                return false;
            };
            (only.is_empty() || only.contains(case_id)) && !skip.contains(case_id)
        });
        if let Some(limit) = cli.limit {
            files.truncate(limit.min(files.len()));
        }
        for path in files {
            let sql = fs::read_to_string(&path)
                .with_context(|| format!("read lifecycle fault preflight {}", path.display()))?;
            if sql_text_has_query_lifecycle_fault_directive(&sql) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() -> Result<()> {
    let exit_code = run()?;
    if exit_code != 0 {
        std::process::exit(exit_code);
    }
    Ok(())
}

fn shutdown_server_handle(server_handle: &Arc<Mutex<Box<dyn ServerHandle>>>) -> Result<()> {
    let mut errors = Vec::new();
    match server_handle.lock() {
        Ok(mut handle) => {
            if let Err(error) = handle.shutdown() {
                errors.push(format!("shutdown failed: {error:#}"));
            }
            let residual = handle.residual_process_ids();
            if !residual.is_empty() {
                errors.push(format!(
                    "residual server process IDs after shutdown: {residual:?}"
                ));
            }
        }
        Err(_) => errors.push("server handle lock poisoned during shutdown".to_string()),
    }
    if errors.is_empty() {
        Ok(())
    } else {
        bail!(errors.join("; "))
    }
}

fn finish_run_with_server_cleanup(
    server_handle: Arc<Mutex<Box<dyn ServerHandle>>>,
    primary_result: Result<i32>,
) -> Result<i32> {
    let cleanup_result = shutdown_server_handle(&server_handle);
    match (primary_result, cleanup_result) {
        (Ok(exit_code), Ok(())) => Ok(exit_code),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(0), Err(cleanup)) => Err(anyhow::anyhow!(
            "server lifecycle cleanup failed: {cleanup:#}"
        )),
        (Ok(exit_code), Err(cleanup)) => Err(anyhow::anyhow!(
            "run exited with code {exit_code}; server lifecycle cleanup failed: {cleanup:#}"
        )),
        (Err(primary), Err(cleanup)) => Err(anyhow::anyhow!(
            "run failed: {primary:#}; server lifecycle cleanup failed: {cleanup:#}"
        )),
    }
}

fn run() -> Result<i32> {
    let cli = Cli::parse();
    let base_dir = resolve_repo_root()?;
    let config_path = resolve_config_path(cli.config.as_deref(), &base_dir);
    let runner_config = load_runner_config(config_path.as_deref())?;

    ensure_iceberg_object_store_prereqs(&runner_config)?;

    let suite_configs = build_suite_configs(&base_dir)?;
    if suite_configs.is_empty() {
        println!("❌ ERROR: no suite directories found under sql-tests");
        return Ok(1);
    }

    let suite_names = match select_suite_names(&cli.suite, &suite_configs) {
        Ok(suite_names) => suite_names,
        Err(error) => {
            println!("❌ ERROR: {error}");
            return Ok(1);
        }
    };
    let selected_manifest = &suite_configs
        .get(&suite_names[0])
        .expect("selected suite exists")
        .manifest;
    let (selected_cluster_mode, selected_cluster_size) = match resolve_selected_cluster(
        selected_manifest.server_mode,
        selected_manifest.cluster_size,
        cli.cluster_mode,
        cli.cluster_size,
    ) {
        Ok(selection) => selection,
        Err(error) => {
            println!("❌ ERROR: {error}");
            return Ok(1);
        }
    };
    if let Err(error) = validate_cluster_args(selected_cluster_mode, selected_cluster_size) {
        println!("❌ ERROR: {error}");
        return Ok(1);
    }
    if let Err(error) =
        validate_selected_suite_cluster(&suite_names, selected_cluster_mode, selected_cluster_size)
    {
        println!("❌ ERROR: {error}");
        return Ok(1);
    }

    // Validate: per-suite path overrides conflict with multi-suite
    let multi_suite = suite_names.len() > 1;
    if multi_suite && (cli.sql_dir.is_some() || cli.result_dir.is_some() || cli.sql_glob.is_some())
    {
        println!(
            "❌ ERROR: --sql-dir, --result-dir, --sql-glob cannot be used with multiple suites"
        );
        return Ok(1);
    }

    if let Some(eps) = cli.float_epsilon {
        if eps <= 0.0 {
            println!("❌ ERROR: --float-epsilon must be > 0");
            return Ok(1);
        }
    }

    let benchmark_bootstrap_options = BenchmarkBootstrapOptions {
        enabled: !cli.no_auto_bootstrap_benchmark_data,
        rebuild: cli.benchmark_bootstrap_rebuild,
        scales: parse_scale_overrides(&cli.benchmark_scale)?,
    };
    let query_lifecycle_faults_enabled = !cli.dry_run
        && selected_cases_require_query_lifecycle_faults(
            &cli,
            &suite_names,
            &suite_configs,
            &base_dir,
        )?;

    let launch_cluster_mode = if cli.dry_run {
        ClusterMode::AllInOne
    } else {
        selected_cluster_mode
    };
    let launch_cluster_size = if cli.dry_run {
        1
    } else {
        selected_cluster_size
    };
    let compat_artifact = if launch_cluster_mode == ClusterMode::StarRocksCompat {
        Some(CompatArtifact::resolve_expected(&base_dir)?)
    } else {
        None
    };
    let server_handle = launch_server(
        launch_cluster_mode,
        launch_cluster_size,
        &base_dir,
        &runner_config,
        compat_artifact,
        query_lifecycle_faults_enabled,
    )?;
    let launched_target_port = server_handle.target_port();
    let launched_target_host = server_handle.target_host().map(ToOwned::to_owned);
    let server_handle = Arc::new(Mutex::new(server_handle));
    let primary_result = (|| -> Result<i32> {
        // Resolve global connection params
        let reference_required = cli.mode == Mode::Diff
            || (cli.mode == Mode::Record && cli.record_from == RecordFrom::Reference);
        let target_port = resolve_effective_target_port(
            launched_target_port,
            cli.port.as_deref(),
            &runner_config,
        )?;
        let reference_port =
            resolve_reference_port(cli.ref_port.as_deref(), &target_port, reference_required)?;

        let target_host = launched_target_host
            .or_else(|| cli.host.clone())
            .or_else(|| env_optional("STARUST_TEST_HOST"))
            .or_else(|| runner_config.cluster.get("host").cloned())
            .unwrap_or_else(|| "127.0.0.1".to_string());
        let target_user = cli
            .user
            .clone()
            .or_else(|| env_optional("STARUST_TEST_USER"))
            .or_else(|| runner_config.cluster.get("user").cloned())
            .unwrap_or_else(|| "root".to_string());
        let target_password = cli
            .password
            .clone()
            .or_else(|| env_optional("STARUST_TEST_PASSWORD"))
            .or_else(|| runner_config.cluster.get("password").cloned());
        let target_mysql_bin = cli
            .mysql
            .clone()
            .unwrap_or_else(|| env_or_default("STARUST_TEST_MYSQL", "mysql"));

        let ref_host = cli
            .ref_host
            .clone()
            .or_else(|| env_optional("STARUST_REF_HOST"))
            .unwrap_or_else(|| "127.0.0.1".to_string());
        let ref_user = cli
            .ref_user
            .clone()
            .or_else(|| env_optional("STARUST_REF_USER"))
            .unwrap_or_else(|| "root".to_string());
        let ref_password = cli
            .ref_password
            .clone()
            .or_else(|| env_optional("STARUST_REF_PASSWORD"));
        let ref_mysql_bin = cli
            .ref_mysql
            .clone()
            .unwrap_or_else(|| env_or_default("STARUST_REF_MYSQL", "mysql"));

        let verify_enabled_override = verify_override(&cli);
        let actual_artifact_dir = resolve_path(cli.write_actual_dir.as_deref(), &base_dir);
        let meta_re = Regex::new(r"^--\s*@([a-zA-Z0-9_]+)\s*=\s*(.+?)\s*$")?;
        let marker_re = Regex::new(r"(?i)^--\s*query\s+(\d+)(?:\s+.*)?$")?;

        // Configure thread pool
        let jobs = if cli.jobs == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        } else {
            cli.jobs
        };

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(jobs)
            .build()
            .context("failed to build rayon thread pool")?;

        // Prepare all suites
        let mut prepared_suites: Vec<PreparedSuite> = Vec::new();

        for suite_name in &suite_names {
            let suite = suite_configs
                .get(suite_name)
                .expect("suite already validated");

            let sql_dir = if !multi_suite {
                resolve_path(cli.sql_dir.as_deref(), &base_dir)
                    .unwrap_or_else(|| suite.sql_dir.clone())
            } else {
                suite.sql_dir.clone()
            };
            let result_dir = if !multi_suite {
                resolve_path(cli.result_dir.as_deref(), &base_dir)
                    .or_else(|| suite.result_dir.clone())
            } else {
                suite.result_dir.clone()
            };
            let sql_glob = if !multi_suite {
                cli.sql_glob
                    .clone()
                    .unwrap_or_else(|| suite.sql_glob.clone())
            } else {
                suite.sql_glob.clone()
            };

            let placeholder_vars = placeholder_variables(&runner_config, &suite.name);
            let suite_init_hook =
                load_suite_hook(suite.init_sql.as_deref(), &meta_re, &placeholder_vars)
                    .with_context(|| {
                        format!("failed to load suite init hook for {}", suite.name)
                    })?;
            let suite_cleanup_hook =
                load_suite_hook(suite.cleanup_sql.as_deref(), &meta_re, &placeholder_vars)
                    .with_context(|| {
                        format!("failed to load suite cleanup hook for {}", suite.name)
                    })?;

            let suite_catalog_override = suite_init_hook
                .as_ref()
                .and_then(|hook| hook.catalog.clone());
            let suite_db_override = suite_init_hook.as_ref().and_then(|hook| hook.db.clone());

            let target_db_default = suite_db_override
                .clone()
                .unwrap_or_else(|| suite.default_db.clone());
            let ref_db_default = suite_db_override
                .clone()
                .unwrap_or_else(|| suite.default_db.clone());

            let verify_enabled = verify_enabled_override.unwrap_or(suite.verify_default);
            let query_timeout = cli.query_timeout.unwrap_or_else(|| {
                env_optional("STARUST_TEST_TIMEOUT")
                    .and_then(|raw| raw.parse().ok())
                    .unwrap_or_else(|| suite_default_query_timeout(&suite.name))
            });

            let target_catalog_name = suite_catalog_override
                .clone()
                .unwrap_or_else(|| suite.default_catalog.clone());
            let reference_catalog_name = suite_catalog_override
                .clone()
                .unwrap_or_else(|| suite.default_catalog.clone());

            let target_conn_base = ConnectionConfig {
                mysql: target_mysql_bin.clone(),
                host: target_host.clone(),
                port: target_port.clone(),
                user: target_user.clone(),
                password: target_password.clone(),
                catalog: Some(target_catalog_name.clone()),
                db: if target_db_default.is_empty() {
                    None
                } else {
                    Some(target_db_default)
                },
            };

            let reference_conn_base = ConnectionConfig {
                mysql: ref_mysql_bin.clone(),
                host: ref_host.clone(),
                port: reference_port.clone(),
                user: ref_user.clone(),
                password: ref_password.clone(),
                catalog: Some(reference_catalog_name),
                db: if ref_db_default.is_empty() {
                    None
                } else {
                    Some(ref_db_default)
                },
            };

            let target_admin_conn = ConnectionConfig {
                catalog: None,
                db: None,
                ..target_conn_base.clone()
            };
            let reference_admin_conn = ConnectionConfig {
                catalog: None,
                db: None,
                ..reference_conn_base.clone()
            };

            // Load and filter cases
            if !sql_dir.exists() {
                println!(
                    "❌ ERROR: SQL directory not found for suite {}: {}",
                    suite.name,
                    sql_dir.display()
                );
                return Ok(1);
            }

            let sql_files = list_sql_files(&sql_dir, &sql_glob)?;
            if sql_files.is_empty() {
                println!(
                    "❌ ERROR: no SQL files found in {} with pattern {} (suite {})",
                    sql_dir.display(),
                    sql_glob,
                    suite.name,
                );
                return Ok(1);
            }

            let mut cases: Vec<SqlCase> = Vec::new();
            for sql_file in sql_files {
                match parser::load_sql_case_from_file(
                    &sql_file,
                    &meta_re,
                    &marker_re,
                    &placeholder_vars,
                ) {
                    Ok(Some(case)) => cases.push(case),
                    Ok(None) => {
                        println!(
                            "Warning: skipping SQL file without executable steps: {}",
                            sql_file.display()
                        );
                    }
                    Err(exc) => {
                        println!("❌ ERROR: {}", exc);
                        return Ok(1);
                    }
                }
            }

            let available_case_ids: HashSet<String> =
                cases.iter().map(|c| c.case_id.clone()).collect();
            let only_set = parse_selector_list(cli.only.as_deref(), &available_case_ids, "--only")?;
            let skip_set = parse_selector_list(cli.skip.as_deref(), &available_case_ids, "--skip")?;

            cases.retain(|case| {
                if !only_set.is_empty() && !only_set.contains(&case.case_id) {
                    return false;
                }
                !skip_set.contains(&case.case_id)
            });

            if let Some(limit) = cli.limit {
                if cases.len() > limit {
                    cases.truncate(limit);
                }
            }

            if cases.is_empty() {
                println!("⚠️ WARNING: no queries selected for suite {}", suite.name);
                continue;
            }

            for case in &cases {
                for step in &case.steps {
                    if let Err(error) = compat_directive::validate_record_source(
                        &step.meta,
                        cli.mode,
                        cli.record_from,
                    ) {
                        println!(
                            "❌ ERROR: suite {} case {} step {}: {error}",
                            suite.name, case.case_id, step.query_number
                        );
                        return Ok(1);
                    }
                    if let Err(error) =
                        compat_directive::validate_execution_mode(&step.meta, cli.mode)
                    {
                        println!(
                            "❌ ERROR: suite {} case {} step {}: {error}",
                            suite.name, case.case_id, step.query_number
                        );
                        return Ok(1);
                    }
                    if let Err(error) =
                        compat_directive::validate_mode(&step.meta, selected_cluster_mode)
                    {
                        println!(
                            "❌ ERROR: suite {} case {} step {}: {error}",
                            suite.name, case.case_id, step.query_number
                        );
                        return Ok(1);
                    }
                }
            }

            if let Err(exc) = validate_fault_injection_jobs(&cases, jobs) {
                println!("❌ ERROR: {}", exc);
                return Ok(1);
            }

            if matches!(cli.mode, Mode::Verify | Mode::Record) && result_dir.is_none() {
                println!(
                    "❌ ERROR: result_dir is required for verify/record mode (suite {})",
                    suite.name
                );
                return Ok(1);
            }

            if cli.mode == Mode::Verify
                && verify_enabled
                && result_dir.is_some()
                && !result_dir.as_ref().is_some_and(|p| p.exists())
            {
                println!(
                    "❌ ERROR: result_dir not found for suite {}: {}",
                    suite.name,
                    result_dir
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_default()
                );
                return Ok(1);
            }

            if cli.mode == Mode::Record {
                if let Some(dir) = &result_dir {
                    fs::create_dir_all(dir)
                        .with_context(|| format!("create result_dir failed: {}", dir.display()))?;
                }
            }

            if !cli.dry_run {
                ensure_benchmark_data(
                    &benchmark_bootstrap_options,
                    &runner_config,
                    &base_dir,
                    &suite.name,
                    &target_catalog_name,
                    &target_host,
                    &target_port,
                    &target_user,
                    target_password.as_deref(),
                )
                .with_context(|| format!("failed to prepare benchmark data for {}", suite.name))?;
            }

            // Print suite header
            println!("{}", "=".repeat(72));
            println!(
                "📋 {} correctness runner (jobs={})",
                suite.name.to_uppercase(),
                jobs
            );
            println!("{}", "=".repeat(72));
            println!("mode={}", mode_name(cli.mode));
            println!(
                "cluster_mode={}",
                match selected_cluster_mode {
                    ClusterMode::AllInOne => "all-in-one",
                    ClusterMode::CrossProcess => "cross-process",
                    ClusterMode::StarRocksCompat => "starrocks-compat",
                }
            );
            println!("sql_dir={}", sql_dir.display());
            println!("sql_glob={}", sql_glob);
            if let Some(path) = runner_config.path.as_deref() {
                println!("config={}", path.display());
            }
            if let Some(dir) = &result_dir {
                println!("result_dir={}", dir.display());
            }
            println!("query_timeout={}s", query_timeout);
            println!("{}", summarize_connection("target", &target_conn_base));
            if cli.mode == Mode::Diff
                || (cli.mode == Mode::Record && cli.record_from == RecordFrom::Reference)
            {
                println!(
                    "{}",
                    summarize_connection("reference", &reference_conn_base)
                );
            }
            if cli.mode == Mode::Verify {
                println!("verify_enabled={}", verify_enabled);
            }
            if let Some(hook) = suite_init_hook.as_ref() {
                println!("suite_init={}", hook.path.display());
                if let Some(catalog) = hook.catalog.as_deref() {
                    println!("suite_env.catalog={}", catalog);
                }
                if let Some(db) = hook.db.as_deref() {
                    println!("suite_env.db={}", db);
                }
            }
            if let Some(hook) = suite_cleanup_hook.as_ref() {
                println!("suite_cleanup={}", hook.path.display());
            }
            let seq_count = cases.iter().filter(|c| c.sequential).count();
            if seq_count > 0 {
                println!(
                    "cases={} (parallel={}, sequential={})",
                    cases.len(),
                    cases.len() - seq_count,
                    seq_count
                );
            } else {
                println!("cases={}", cases.len());
            }
            println!("{}", "=".repeat(72));

            if cli.dry_run {
                println!("selected cases for suite {}:", suite.name);
                for case in &cases {
                    let file_name = case
                        .source_file
                        .file_name()
                        .and_then(|s| s.to_str())
                        .unwrap_or_default();
                    let seq_tag = if case.sequential { " [sequential]" } else { "" };
                    println!(
                        "  {} ({}, steps={}{})",
                        case.case_id,
                        file_name,
                        case.steps.len(),
                        seq_tag,
                    );
                }
                continue;
            }

            let ctx = SuiteRunContext {
                suite_name: suite.name.clone(),
                mode: cli.mode,
                record_from: cli.record_from,
                target_conn_base,
                reference_conn_base,
                target_admin_conn,
                reference_admin_conn,
                result_dir,
                actual_artifact_dir: actual_artifact_dir.clone(),
                verify_enabled,
                query_timeout,
                reference_required,
                auto_case_db: suite.auto_case_db,
                order_sensitive_default: cli.order_sensitive_default,
                float_epsilon: cli.float_epsilon,
                preview_lines: cli.preview_lines,
                update_expected: cli.update_expected,
                target_session_sql: cli.target_session_sql.clone(),
                marker_re: marker_re.clone(),
                fail_fast: cli.fail_fast,
                server_handle: Arc::clone(&server_handle),
            };

            prepared_suites.push(PreparedSuite {
                ctx,
                cases,
                init_hook: suite_init_hook,
                cleanup_hook: suite_cleanup_hook,
            });
        }

        if cli.dry_run {
            return Ok(0);
        }

        if prepared_suites.is_empty() {
            println!("❌ ERROR: no suites to run");
            return Ok(1);
        }

        // Global abort flag for fail-fast
        let abort = AtomicBool::new(false);
        let stdout_lock = Mutex::new(());

        // Run suites (parallel via rayon thread pool)
        let suite_outcomes: Vec<SuiteOutcome> = pool.install(|| {
            prepared_suites
                .par_iter()
                .map(|ps| run_suite(ps, &abort, &stdout_lock))
                .collect()
        });

        // Aggregate results
        let mut grand_total = 0usize;
        let mut grand_passed = 0usize;
        let mut grand_failed = 0usize;
        let mut grand_skipped = 0usize;
        let mut all_case_timings: Vec<CaseTiming> = Vec::new();
        let mut all_failed_cases: Vec<(String, String)> = Vec::new();
        let mut all_cleanup_errors: Vec<String> = Vec::new();

        for so in &suite_outcomes {
            grand_total += so.total;
            for co in &so.outcomes {
                match co.status {
                    CaseStatus::Pass => grand_passed += 1,
                    CaseStatus::Fail => {
                        grand_failed += 1;
                        all_failed_cases.push((so.suite_name.clone(), co.case_id.clone()));
                    }
                    CaseStatus::Skipped => grand_skipped += 1,
                }
                all_case_timings.push(CaseTiming {
                    suite_name: so.suite_name.clone(),
                    case_id: co.case_id.clone(),
                    status: co.status,
                    elapsed: co.elapsed,
                });
            }
            all_cleanup_errors.extend(so.cleanup_errors.iter().cloned());
        }

        let total_cpu_time: Duration = all_case_timings.iter().map(|timing| timing.elapsed).sum();

        // Print summary
        println!("\n{}", "=".repeat(72));
        if suite_outcomes.len() == 1 {
            println!(
                "summary ({}, mode={})",
                suite_outcomes[0].suite_name,
                mode_name(cli.mode)
            );
        } else {
            let names: Vec<&str> = suite_outcomes
                .iter()
                .map(|s| s.suite_name.as_str())
                .collect();
            println!(
                "summary ({} suites: {}, mode={})",
                names.len(),
                names.join(", "),
                mode_name(cli.mode)
            );
        }
        println!("{}", "=".repeat(72));
        println!("total={}", grand_total);
        println!("pass={}", grand_passed);
        println!("fail={}", grand_failed);
        if grand_skipped > 0 {
            println!("skipped={}", grand_skipped);
        }
        println!("cpu_time={:.2}s", total_cpu_time.as_secs_f64());
        for so in &suite_outcomes {
            println!(
                "  suite {} wall_time={:.2}s",
                so.suite_name,
                so.wall_time.as_secs_f64()
            );
        }

        let mut slowest_case_timings = all_case_timings.clone();
        slowest_case_timings.sort_by(|a, b| b.elapsed.cmp(&a.elapsed));
        println!("\nslowest cases (top 5):");
        for timing in slowest_case_timings.iter().take(5) {
            println!(
                "  [{}] {}: {:.2}s",
                timing.suite_name,
                timing.case_id,
                timing.elapsed.as_secs_f64()
            );
        }

        all_case_timings.sort_by(|a, b| {
            a.suite_name
                .cmp(&b.suite_name)
                .then_with(|| b.elapsed.cmp(&a.elapsed))
                .then_with(|| a.case_id.cmp(&b.case_id))
        });
        print!("{}", format_case_timings(&all_case_timings));

        if !all_failed_cases.is_empty() {
            println!("\nfailed cases:");
            for (suite, case_id) in &all_failed_cases {
                println!("  [{}] {}", suite, case_id);
            }
        }
        if !all_cleanup_errors.is_empty() {
            println!("\ncleanup errors:");
            for err in &all_cleanup_errors {
                println!("  {}", err);
            }
        }
        println!("{}", "=".repeat(72));

        if grand_failed > 0 || !all_cleanup_errors.is_empty() {
            return Ok(1);
        }

        Ok(0)
    })();
    finish_run_with_server_cleanup(server_handle, primary_result)
}

#[cfg(test)]
mod tests {
    use crate::cluster::{
        BePorts, ClusterMode, ClusterProcessRole, CrossProcessRuntime, build_novarocks_command,
        discover_novarocks_binary_with_override, render_cross_process_config,
        startup_timeout_from_env, validate_cluster_args,
    };
    use crate::config::substitute_placeholders;
    use crate::parser::{extract_suite_hook, load_sql_case_from_file};
    use crate::results::{load_expected_results, parse_output, write_result_file};
    use crate::runner::{is_transient_iceberg_commit_error, parse_selector_list};
    use crate::types::{QueryMeta, ResultSet, SqlCase, SqlStep};
    use crate::{
        Cli, annotate_failure_with_engine_error_code, bounded_fault_query_timeout,
        evaluate_expected_error_branch, execute_target_session_sql_with,
        expected_engine_error_code_diff_result, expected_engine_error_code_result,
        finish_expected_error_step, sql_text_has_query_lifecycle_fault_directive,
        validate_fault_injection_jobs, validate_selected_suite_cluster,
    };
    use clap::Parser;
    use regex::Regex;
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    fn test_runtime_dir() -> PathBuf {
        let dir = crate::resolve_repo_root()
            .expect("repo root")
            .join("tests/sql-test-runner/.test-runtime");
        fs::create_dir_all(&dir).expect("create test runtime dir");
        dir
    }

    fn temp_result_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before unix epoch")
            .as_nanos();
        test_runtime_dir().join(format!(
            "novarocks_sql_tests_{}_{}_{}.result",
            name,
            std::process::id(),
            nanos
        ))
    }

    fn temp_sql_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before unix epoch")
            .as_nanos();
        test_runtime_dir().join(format!(
            "novarocks_sql_tests_{}_{}_{}.sql",
            name,
            std::process::id(),
            nanos
        ))
    }

    fn test_case_with_meta(meta: QueryMeta) -> SqlCase {
        SqlCase {
            source_file: PathBuf::from("fault.sql"),
            case_id: "fault_case".to_string(),
            steps: vec![SqlStep {
                query_number: 1,
                sql: "select 1".to_string(),
                meta,
            }],
            case_dbs: vec![],
            sequential: false,
        }
    }

    #[test]
    fn fault_directives_require_serial_jobs() {
        let cases = vec![test_case_with_meta(QueryMeta {
            kill_be_index: Some(0),
            ..QueryMeta::default()
        })];

        let err = validate_fault_injection_jobs(&cases, 2)
            .expect_err("fault directives should reject parallel jobs");
        assert!(
            err.to_string().contains(
                "fault injection directives require -j 1 because they mutate the shared cross-process cluster"
            ),
            "unexpected error: {err}"
        );

        validate_fault_injection_jobs(&cases, 1).expect("serial jobs should be accepted");
        validate_fault_injection_jobs(&[test_case_with_meta(QueryMeta::default())], 8)
            .expect("cases without fault directives should allow parallel jobs");
    }

    #[test]
    fn lifecycle_fault_preflight_only_matches_explicit_directives() {
        assert!(sql_text_has_query_lifecycle_fault_directive(
            "-- @drop_next_init_ack_be_index=1\nSELECT 1;"
        ));
        assert!(sql_text_has_query_lifecycle_fault_directive(
            "-- @kill_query_after_control_ready_count=3\nSELECT 1;"
        ));
        assert!(sql_text_has_query_lifecycle_fault_directive(
            "-- @drop_next_stage_ack_be_index=1\nSELECT 1;"
        ));
        assert!(sql_text_has_query_lifecycle_fault_directive(
            "-- @hold_start_until_early_ingress=true\nSELECT 1;"
        ));
        assert!(!sql_text_has_query_lifecycle_fault_directive(
            "-- query-control-heartbeat-loss is only a case name\nSELECT 1;"
        ));
    }

    #[test]
    fn distributed_resilience_requires_exact_native_three_be_topology() {
        let suites = vec!["distributed-resilience".to_string()];
        validate_selected_suite_cluster(&suites, ClusterMode::CrossProcess, 3)
            .expect("canonical distributed topology");
        for (mode, size) in [
            (ClusterMode::CrossProcess, 2),
            (ClusterMode::CrossProcess, 4),
            (ClusterMode::AllInOne, 1),
        ] {
            let error = validate_selected_suite_cluster(&suites, mode, size)
                .expect_err("distributed-resilience must reject noncanonical topology");
            assert!(
                error.to_string().contains(
                    "distributed-resilience requires --cluster-mode cross-process --cluster-size 3"
                ),
                "unexpected error: {error}"
            );
        }
        validate_selected_suite_cluster(&["join".to_string()], ClusterMode::AllInOne, 1)
            .expect("other suites retain their own topology");
    }

    #[test]
    fn expect_error_code_result_accepts_matching_code() {
        expected_engine_error_code_result(
            "ERROR (0.01s): ERROR 1105 (HY000): [CommitUnknown] commit outcome unavailable",
            "CommitUnknown",
        )
        .expect("matching code should pass");
    }

    #[test]
    fn expect_error_code_result_reports_missing_code() {
        let err = expected_engine_error_code_result(
            "ERROR (0.01s): ERROR 1105 (HY000): plain error",
            "CommitUnknown",
        )
        .expect_err("missing code should fail");

        assert!(
            err.contains("expected engine error code \"CommitUnknown\", got None"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn expect_error_code_takes_precedence_over_expect_error() {
        let meta = QueryMeta {
            expect_error: Some("different substring".to_string()),
            expect_error_code: Some("CommitUnknown".to_string()),
            ..QueryMeta::default()
        };

        let result = evaluate_expected_error_branch(
            &meta,
            false,
            "ERROR (0.01s): ERROR 1105 (HY000): [CommitUnknown] commit outcome unavailable",
        )
        .expect("expected branch should be active");

        assert_eq!(result, Ok(()));
    }

    #[test]
    fn expect_error_code_branch_fails_when_query_succeeds() {
        let meta = QueryMeta {
            expect_error_code: Some("CommitUnknown".to_string()),
            ..QueryMeta::default()
        };

        let err = evaluate_expected_error_branch(&meta, true, "")
            .expect("expected branch should be active")
            .expect_err("success should fail an expected code branch");

        assert!(
            err.contains("expected engine error code \"CommitUnknown\", but query succeeded"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn expect_error_code_branch_reports_mismatched_code() {
        let meta = QueryMeta {
            expect_error_code: Some("CommitUnknown".to_string()),
            ..QueryMeta::default()
        };

        let err = evaluate_expected_error_branch(
            &meta,
            false,
            "ERROR (0.01s): ERROR 1105 (HY000): [ProtocolDecodeError] bad payload",
        )
        .expect("expected branch should be active")
        .expect_err("wrong code should fail");

        assert!(
            err.contains(
                "expected engine error code \"CommitUnknown\", got Some(\"ProtocolDecodeError\")"
            ),
            "unexpected error: {err}"
        );
        assert!(err.contains("[ProtocolDecodeError] bad payload"));
    }

    #[test]
    fn expect_error_code_branch_reports_missing_code() {
        let meta = QueryMeta {
            expect_error_code: Some("CommitUnknown".to_string()),
            ..QueryMeta::default()
        };

        let err = evaluate_expected_error_branch(
            &meta,
            false,
            "ERROR (0.01s): ERROR 1105 (HY000): plain error",
        )
        .expect("expected branch should be active")
        .expect_err("missing code should fail");

        assert!(
            err.contains("expected engine error code \"CommitUnknown\", got None"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn expect_error_code_diff_requires_both_sides_to_match() {
        expected_engine_error_code_diff_result(
            "CommitUnknown",
            false,
            "ERROR (0.01s): ERROR 1105 (HY000): [CommitUnknown] target",
            false,
            "ERROR (0.01s): ERROR 1105 (HY000): [CommitUnknown] reference",
        )
        .expect("both sides should match");

        let one_side_success = expected_engine_error_code_diff_result(
            "CommitUnknown",
            true,
            "",
            false,
            "ERROR (0.01s): ERROR 1105 (HY000): [CommitUnknown] reference",
        )
        .expect_err("target success should fail");
        assert!(
            one_side_success.contains("target_ok=true"),
            "unexpected error: {one_side_success}"
        );

        let wrong_code = expected_engine_error_code_diff_result(
            "CommitUnknown",
            false,
            "ERROR (0.01s): ERROR 1105 (HY000): [ProtocolDecodeError] target",
            false,
            "ERROR (0.01s): ERROR 1105 (HY000): [CommitUnknown] reference",
        )
        .expect_err("wrong target code should fail");
        assert!(
            wrong_code.contains("target_code=Some(\"ProtocolDecodeError\")"),
            "unexpected error: {wrong_code}"
        );
    }

    #[test]
    fn failure_log_includes_engine_error_code_when_available() {
        let msg =
            "target execute failed: FAIL (0.00s): ERROR 1105 (HY000): [QueryTimeout] slow query";

        assert_eq!(
            annotate_failure_with_engine_error_code(msg, msg),
            "engine_error_code=QueryTimeout target execute failed: FAIL (0.00s): ERROR 1105 (HY000): [QueryTimeout] slow query"
        );
    }

    #[test]
    fn failure_log_leaves_plain_errors_unclassified() {
        let msg = "target execute failed: plain execution error";

        assert_eq!(annotate_failure_with_engine_error_code(msg, msg), msg);
    }

    struct MissingCompatEvidenceServer {
        endpoints: Vec<crate::types::CompatBeEndpoint>,
    }

    impl crate::cluster::ServerHandle for MissingCompatEvidenceServer {
        fn target_host(&self) -> Option<&str> {
            Some("127.0.0.1")
        }

        fn target_port(&self) -> Option<u16> {
            Some(9030)
        }

        fn be_endpoints(&self) -> &[crate::types::CompatBeEndpoint] {
            &self.endpoints
        }

        fn be_log_count(&self, _index: usize, _needle: &str) -> anyhow::Result<usize> {
            Ok(0)
        }
    }

    #[test]
    fn expected_error_compat_directive_failure_prevents_pass_marker() {
        let step = SqlStep {
            query_number: 1,
            sql: "SELECT rejected".to_string(),
            meta: QueryMeta {
                expect_error: Some("planned rejection".to_string()),
                be_log_contains: vec!["compat_ingress rejected".to_string()],
                ..QueryMeta::default()
            },
        };
        let server_handle: Arc<Mutex<Box<dyn crate::cluster::ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(MissingCompatEvidenceServer {
                endpoints: vec![crate::types::CompatBeEndpoint {
                    host: "127.0.0.1".to_string(),
                    heartbeat_port: 19050,
                    be_port: 19060,
                    brpc_port: 18060,
                    http_port: 18040,
                    grpc_port: 18070,
                    starlet_port: 19070,
                }],
            })));
        let mut log = String::new();

        let passed = finish_expected_error_step(
            &step,
            true,
            "planned rejection",
            &server_handle,
            &crate::compat_directive::BeLogSnapshot::default(),
            &mut log,
        );

        assert!(!passed, "directive failure must fail the step");
        assert!(log.contains("compatibility directive failed"), "{log}");
        assert!(!log.contains("PASS (expected error matched)"), "{log}");
    }

    struct PresentCompatEvidenceServer(MissingCompatEvidenceServer);

    impl crate::cluster::ServerHandle for PresentCompatEvidenceServer {
        fn target_host(&self) -> Option<&str> {
            self.0.target_host()
        }

        fn target_port(&self) -> Option<u16> {
            self.0.target_port()
        }

        fn be_endpoints(&self) -> &[crate::types::CompatBeEndpoint] {
            self.0.be_endpoints()
        }

        fn be_log_count(&self, _index: usize, _needle: &str) -> anyhow::Result<usize> {
            Ok(1)
        }
    }

    #[test]
    fn expected_error_runs_compat_directives_before_pass_marker() {
        let step = SqlStep {
            query_number: 1,
            sql: "SELECT rejected".to_string(),
            meta: QueryMeta {
                expect_error_code: Some("ProtocolDecodeError".to_string()),
                be_log_contains: vec!["compat_ingress rejected".to_string()],
                ..QueryMeta::default()
            },
        };
        let server_handle: Arc<Mutex<Box<dyn crate::cluster::ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(PresentCompatEvidenceServer(
                MissingCompatEvidenceServer {
                    endpoints: vec![crate::types::CompatBeEndpoint {
                        host: "127.0.0.1".to_string(),
                        heartbeat_port: 19050,
                        be_port: 19060,
                        brpc_port: 18060,
                        http_port: 18040,
                        grpc_port: 18070,
                        starlet_port: 19070,
                    }],
                },
            ))));
        let mut log = String::new();

        assert!(finish_expected_error_step(
            &step,
            true,
            "[ProtocolDecodeError] planned rejection",
            &server_handle,
            &crate::compat_directive::BeLogSnapshot::default(),
            &mut log,
        ));

        let directive = log.find("@be_log_contains PASS").expect("directive PASS");
        let expected_error = log
            .find("PASS (expected error matched)")
            .expect("expected-error PASS");
        assert!(directive < expected_error, "{log}");
    }

    #[test]
    fn help_includes_benchmark_bootstrap_options() {
        let help = <crate::Cli as clap::CommandFactory>::command()
            .render_long_help()
            .to_string();

        assert!(help.contains("--no-auto-bootstrap-benchmark-data"));
        assert!(help.contains("--benchmark-scale <BENCHMARK_SCALE>"));
        assert!(help.contains("--benchmark-bootstrap-rebuild"));
    }

    #[test]
    fn help_includes_cluster_mode_option() {
        let help = <crate::Cli as clap::CommandFactory>::command()
            .render_long_help()
            .to_string();

        assert!(help.contains("--cluster-mode <CLUSTER_MODE>"));
        assert!(help.contains("cross-process"));
    }

    #[test]
    fn help_excludes_retired_wire_selector_option() {
        let help = <crate::Cli as clap::CommandFactory>::command()
            .render_long_help()
            .to_string();
        let retired_option = ["--plan", "wire", "format"].join("-");

        assert!(!help.contains(&retired_option));
    }

    #[test]
    fn cli_cluster_mode_defaults_to_all_in_one() {
        let cli = crate::Cli::parse_from(["sql-tests", "--suite", "ssb"]);
        assert_eq!(cli.cluster_mode, ClusterMode::AllInOne);
    }

    #[test]
    fn target_session_sql_defaults_to_empty() {
        let cli = crate::Cli::parse_from(["sql-tests", "--suite", "ssb"]);

        assert!(cli.target_session_sql.is_empty());
    }

    #[test]
    fn target_session_sql_repeated_arguments_preserve_order() {
        let cli = crate::Cli::parse_from([
            "sql-tests",
            "--suite",
            "ssb",
            "--target-session-sql",
            "SET first = true",
            "--target-session-sql",
            "SET second = false",
        ]);

        assert_eq!(
            cli.target_session_sql,
            ["SET first = true", "SET second = false"]
        );
    }

    #[test]
    fn target_session_sql_failure_prevents_case_step() {
        let statements = vec![
            "SET first = true".to_string(),
            "SET rejected = true".to_string(),
        ];
        let mut events = Vec::new();

        let setup =
            execute_target_session_sql_with(&mut events, &statements, |events, statement| {
                events.push(statement.to_string());
                if statement.contains("rejected") {
                    Err("session override rejected".to_string())
                } else {
                    Ok(())
                }
            });
        if setup.is_ok() {
            events.push("CASE STEP".to_string());
        }

        assert_eq!(
            setup,
            Err(
                "target session SQL failed for \"SET rejected = true\": session override rejected"
                    .to_string()
            )
        );
        assert_eq!(events, ["SET first = true", "SET rejected = true"]);
        assert!(!events.iter().any(|event| event == "CASE STEP"));
    }

    #[test]
    fn target_session_sql_does_not_touch_reference_session() {
        let statements = vec!["SET target_only = true".to_string()];
        let mut target_events = Vec::new();
        let reference_events: Vec<String> = Vec::new();

        execute_target_session_sql_with(&mut target_events, &statements, |events, statement| {
            events.push(statement.to_string());
            Ok(())
        })
        .expect("target session SQL should pass");

        assert_eq!(target_events, ["SET target_only = true"]);
        assert!(reference_events.is_empty());
    }

    #[test]
    fn cli_rejects_retired_wire_selector_option() {
        let retired_option = ["--plan", "wire", "format"].join("-");
        let err = crate::Cli::try_parse_from([
            "sql-tests",
            "--suite",
            "ssb",
            retired_option.as_str(),
            "proto",
        ])
        .expect_err("runner must reject the retired wire selector option");

        assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
    }

    #[test]
    fn cli_cluster_mode_accepts_cross_process() {
        let cli = crate::Cli::parse_from([
            "sql-tests",
            "--suite",
            "ssb",
            "--cluster-mode",
            "cross-process",
        ]);
        assert_eq!(cli.cluster_mode, ClusterMode::CrossProcess);
    }

    #[test]
    fn cli_cannot_select_internal_starrocks_compat_cluster_mode() {
        let error = Cli::try_parse_from([
            "sql-tests",
            "--suite",
            "ssb",
            "--cluster-mode",
            "star-rocks-compat",
        ])
        .expect_err("internal compatibility mode must not be a CLI escape hatch");
        let message = error.to_string();
        assert!(
            message.contains("[possible values: all-in-one, cross-process]"),
            "hidden mode leaked into CLI choices: {message}"
        );
    }

    #[test]
    fn selected_cluster_is_manifest_driven_for_starrocks_compat() {
        let native = super::resolve_selected_cluster(
            crate::suite_manifest::SuiteServerMode::Native,
            1,
            crate::cluster::ClusterMode::CrossProcess,
            Some(2),
        )
        .expect("native CLI selection");
        assert_eq!(native, (crate::cluster::ClusterMode::CrossProcess, 2));

        for override_size in [None, Some(3)] {
            let compat = super::resolve_selected_cluster(
                crate::suite_manifest::SuiteServerMode::StarRocksCompat,
                3,
                crate::cluster::ClusterMode::AllInOne,
                override_size,
            )
            .expect("manifest-selected compatibility cluster");
            assert_eq!(compat, (crate::cluster::ClusterMode::StarRocksCompat, 3));
        }
        let error = super::resolve_selected_cluster(
            crate::suite_manifest::SuiteServerMode::StarRocksCompat,
            3,
            crate::cluster::ClusterMode::CrossProcess,
            Some(2),
        )
        .expect_err("compatibility cluster size override must be exactly three");
        assert!(
            error
                .to_string()
                .contains("starrocks-compat mode requires --cluster-size 3"),
            "{error:#}"
        );
    }

    struct CleanupFailureServer;

    impl crate::cluster::ServerHandle for CleanupFailureServer {
        fn target_host(&self) -> Option<&str> {
            Some("127.0.0.1")
        }

        fn target_port(&self) -> Option<u16> {
            Some(9030)
        }

        fn residual_process_ids(&self) -> Vec<u32> {
            vec![4242]
        }

        fn shutdown(&mut self) -> anyhow::Result<()> {
            anyhow::bail!("injected shutdown failure")
        }
    }

    #[test]
    fn post_launch_cleanup_reports_primary_shutdown_and_residual_failures() {
        let server: std::sync::Arc<std::sync::Mutex<Box<dyn crate::cluster::ServerHandle>>> =
            std::sync::Arc::new(std::sync::Mutex::new(Box::new(CleanupFailureServer)));
        let error = super::finish_run_with_server_cleanup(
            server,
            Err(anyhow::anyhow!("injected execution failure")),
        )
        .expect_err("primary and cleanup failures must be returned together");
        let message = format!("{error:#}");
        assert!(message.contains("injected execution failure"), "{message}");
        assert!(message.contains("injected shutdown failure"), "{message}");
        assert!(message.contains("4242"), "{message}");
    }

    #[test]
    fn cross_process_configs_preserve_base_sections_and_patch_cluster_ports() {
        let runtime = CrossProcessRuntime {
            be: vec![BePorts {
                http: 18080,
                grpc: 19070,
            }],
            fe_http_port: 28080,
            fe_grpc_port: 29070,
            fe_mysql_port: 29030,
        };
        let base = r#"
[metadata]
provider = "sqlite"
path = "tmp/sql-tests.sqlite"

[standalone_server]
mysql_port = 9030

[connector.object_store]
endpoint = "http://127.0.0.1:9000"
access_key_id = "admin"
enable_path_style_access = true
"#;

        let fe = render_cross_process_config(base, ClusterProcessRole::Fe, 0, &runtime)
            .expect("render fe config");
        let be = render_cross_process_config(base, ClusterProcessRole::Be, 0, &runtime)
            .expect("render be config");

        let fe_value: toml::Value = fe.parse().expect("parse fe toml");
        let be_value: toml::Value = be.parse().expect("parse be toml");

        assert_eq!(
            fe_value["metadata"]["path"].as_str(),
            Some("tmp/sql-tests.sqlite")
        );
        assert_eq!(
            fe_value["connector"]["object_store"]["endpoint"].as_str(),
            Some("http://127.0.0.1:9000")
        );
        assert_eq!(
            fe_value["standalone_server"]["mysql_port"].as_integer(),
            Some(29030)
        );
        assert_eq!(fe_value["cluster"]["role"].as_str(), Some("fe"));
        assert_eq!(
            fe_value["cluster"]["backends"]
                .as_array()
                .and_then(|items| items.first())
                .and_then(|value| value.as_str()),
            Some("127.0.0.1:19070")
        );

        assert_eq!(be_value["cluster"]["role"].as_str(), Some("be"));
        assert_eq!(be_value["server"]["grpc_port"].as_integer(), Some(19070));
        assert!(
            be_value
                .get("connector")
                .and_then(|value| value.get("object_store"))
                .is_some()
        );
    }

    #[test]
    fn discover_novarocks_binary_prefers_env_override() {
        let repo_root = crate::resolve_repo_root().expect("repo root");
        let test_root = repo_root.join("tests/sql-test-runner/.test-runtime/discover-bin");
        fs::create_dir_all(&test_root).expect("create test runtime dir");
        let fake_bin = test_root.join("novarocks-env");
        fs::write(&fake_bin, "#!/bin/sh\nexit 0\n").expect("write fake bin");

        let resolved = discover_novarocks_binary_with_override(&repo_root, Some(fake_bin.clone()))
            .expect("discover binary");
        assert_eq!(resolved, fake_bin);
        let _ = fs::remove_file(&fake_bin);
        let _ = fs::remove_dir_all(&test_root);
    }

    #[test]
    fn cross_process_command_sets_required_test_env() {
        let command = build_novarocks_command(
            PathBuf::from("ignored-binary").as_path(),
            "fe",
            PathBuf::from("runner.toml").as_path(),
        );
        let no_proxy = command
            .get_envs()
            .find(|(key, _)| key.to_str() == Some("NO_PROXY"))
            .and_then(|(_, value)| value)
            .and_then(|value| value.to_str());
        assert_eq!(no_proxy, Some("127.0.0.1,localhost"));

        let imv_stateless_rebuild = command
            .get_envs()
            .find(|(key, _)| key.to_str() == Some("NOVAROCKS_ENABLE_TEST_IMV_STATELESS_REBUILD"))
            .and_then(|(_, value)| value)
            .and_then(|value| value.to_str());
        assert_eq!(imv_stateless_rebuild, Some("1"));
    }

    #[test]
    fn startup_timeout_defaults_to_120_seconds() {
        assert_eq!(
            startup_timeout_from_env(None),
            std::time::Duration::from_secs(120)
        );
        assert_eq!(
            startup_timeout_from_env(Some("180")),
            std::time::Duration::from_secs(120)
        );
        assert_eq!(
            startup_timeout_from_env(Some(&u64::MAX.to_string())),
            std::time::Duration::from_secs(120)
        );
        assert_eq!(
            startup_timeout_from_env(Some("bogus")),
            std::time::Duration::from_secs(120)
        );
    }

    #[test]
    fn cross_process_target_port_prefers_server_handle_port() {
        let runner_config = crate::types::RunnerConfig::default();
        let port = crate::resolve_effective_target_port(Some(12345), None, &runner_config)
            .expect("server-provided port should bypass external port resolution");
        assert_eq!(port, "12345");
    }

    #[test]
    fn load_expected_result_accepts_empty_file() {
        let path = temp_result_path("empty_load");
        fs::write(&path, "\n").expect("write empty file");
        let marker_re = Regex::new(r"(?i)^--\s*query\s+(\d+)(?:\s+.*)?$").expect("marker regex");
        let loaded = load_expected_results(&path, false, &marker_re, None)
            .expect("must parse empty result file");
        let result_set = loaded.get(&1).expect("single-step result");
        assert!(result_set.header.is_empty());
        assert!(result_set.rows.is_empty());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn write_result_file_persists_empty_result_set() {
        let path = temp_result_path("empty_write");
        write_result_file(&path, &BTreeMap::new(), false).expect("write empty result file");
        let content = fs::read_to_string(&path).expect("read empty result file");
        assert_eq!(content, "");
        let marker_re = Regex::new(r"(?i)^--\s*query\s+(\d+)(?:\s+.*)?$").expect("marker regex");
        let loaded = load_expected_results(&path, false, &marker_re, None)
            .expect("must parse empty result file");
        let result_set = loaded.get(&1).expect("single-step result");
        assert!(result_set.header.is_empty());
        assert!(result_set.rows.is_empty());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn load_expected_result_accepts_single_step_marker() {
        let path = temp_result_path("single_marker");
        fs::write(&path, "-- query 2\ncount(*)\n3\n").expect("write result file");
        let marker_re = Regex::new(r"(?i)^--\s*query\s+(\d+)(?:\s+.*)?$").expect("marker regex");
        let loaded =
            load_expected_results(&path, false, &marker_re, None).expect("must parse result file");
        assert_eq!(
            loaded.get(&2).expect("query 2"),
            &ResultSet {
                header: vec!["count(*)".to_string()],
                rows: vec![vec!["3".to_string()]],
            }
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn multi_step_result_round_trip() {
        let path = temp_result_path("multi_step");
        let marker_re = Regex::new(r"(?i)^--\s*query\s+(\d+)(?:\s+.*)?$").expect("marker regex");
        let result_sets = BTreeMap::from([
            (
                1usize,
                ResultSet {
                    header: vec!["count(*)".to_string()],
                    rows: vec![vec!["1".to_string()]],
                },
            ),
            (
                3usize,
                ResultSet {
                    header: vec!["k1".to_string(), "c1".to_string()],
                    rows: vec![vec!["1".to_string(), "2".to_string()]],
                },
            ),
        ]);
        write_result_file(&path, &result_sets, true).expect("write multi-step result file");
        let loaded = load_expected_results(&path, true, &marker_re, None)
            .expect("must parse multi-step result file");
        assert_eq!(loaded, result_sets);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn load_expected_results_rejects_multi_step_without_markers() {
        let path = temp_result_path("bad_multi_step");
        fs::write(&path, "count(*)\n1\n").expect("write bad multi-step file");
        let marker_re = Regex::new(r"(?i)^--\s*query\s+(\d+)(?:\s+.*)?$").expect("marker regex");
        let loaded = load_expected_results(&path, true, &marker_re, None);
        assert!(loaded.is_none());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn load_expected_results_preserves_trailing_empty_columns() {
        let path = temp_result_path("trailing_empty_columns");
        fs::write(
            &path,
            "-- query 1\nField\tType\tNull\tKey\tDefault\tExtra\nevent_day\tdate\tYES\ttrue\tNULL\t\n",
        )
        .expect("write result file");
        let marker_re = Regex::new(r"(?i)^--\s*query\s+(\d+)(?:\s+.*)?$").expect("marker regex");
        let loaded = load_expected_results(&path, true, &marker_re, None).expect("load result");
        assert_eq!(
            loaded.get(&1).expect("query 1"),
            &ResultSet {
                header: vec![
                    "Field".to_string(),
                    "Type".to_string(),
                    "Null".to_string(),
                    "Key".to_string(),
                    "Default".to_string(),
                    "Extra".to_string(),
                ],
                rows: vec![vec![
                    "event_day".to_string(),
                    "date".to_string(),
                    "YES".to_string(),
                    "true".to_string(),
                    "NULL".to_string(),
                    String::new(),
                ]],
            }
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn parse_output_preserves_trailing_empty_columns() {
        let (header, rows) = parse_output(
            "Field\tType\tNull\tKey\tDefault\tExtra\nevent_day\tdate\tYES\ttrue\tNULL\t\n",
        );
        assert_eq!(
            header,
            vec![
                "Field".to_string(),
                "Type".to_string(),
                "Null".to_string(),
                "Key".to_string(),
                "Default".to_string(),
                "Extra".to_string(),
            ]
        );
        assert_eq!(
            rows,
            vec![vec![
                "event_day".to_string(),
                "date".to_string(),
                "YES".to_string(),
                "true".to_string(),
                "NULL".to_string(),
                String::new(),
            ]]
        );
    }

    #[test]
    fn selector_list_rejects_legacy_step_ids() {
        let available_cases = std::collections::HashSet::from(["foo".to_string()]);
        let err = parse_selector_list(Some("foo-2"), &available_cases, "--only")
            .expect_err("legacy step id must fail");
        assert!(err.to_string().contains("sub-query selectors"));
    }

    #[test]
    fn transient_iceberg_commit_error_matches_missing_metadata() {
        let message = "ERROR 1064 (HY000) at line 11: Metadata file for version 2 is missing under file:/tmp/table/metadata";
        assert!(is_transient_iceberg_commit_error(message));
    }

    #[test]
    fn fault_query_timeout_reserves_time_for_terminal_evidence() {
        let deadline = Instant::now() + Duration::from_secs(30);

        assert!(bounded_fault_query_timeout(120, Some(deadline)) <= 28);
        assert_eq!(bounded_fault_query_timeout(5, Some(deadline)), 5);
        assert_eq!(bounded_fault_query_timeout(120, None), 120);
    }

    #[test]
    fn transient_iceberg_commit_error_ignores_regular_failures() {
        let message =
            "ERROR 5904 (42000) at line 10: Warehouse default_warehouse is not available.";
        assert!(!is_transient_iceberg_commit_error(message));
    }

    #[test]
    fn format_case_timings_lists_every_case_with_status_and_elapsed() {
        let timings = vec![
            crate::CaseTiming {
                suite_name: "aggregate".to_string(),
                case_id: "agg_fast".to_string(),
                status: crate::CaseStatus::Pass,
                elapsed: std::time::Duration::from_millis(120),
            },
            crate::CaseTiming {
                suite_name: "analytic".to_string(),
                case_id: "window_slow".to_string(),
                status: crate::CaseStatus::Fail,
                elapsed: std::time::Duration::from_millis(1234),
            },
        ];

        let rendered = crate::format_case_timings(&timings);

        assert!(rendered.contains("case timings (all):"));
        assert!(rendered.contains("  [aggregate] agg_fast PASS 0.12s"));
        assert!(rendered.contains("  [analytic] window_slow FAIL 1.23s"));
    }

    #[test]
    fn legacy_name_sequential_tag_marks_case_sequential() {
        let meta_re = Regex::new(r"^--\s*@([a-zA-Z0-9_]+)\s*=\s*(.+?)\s*$").expect("meta regex");
        let marker_re = Regex::new(r"(?i)^--\s*query\s+(\d+)(?:\s+.*)?$").expect("marker regex");
        let path = temp_sql_path("legacy_sequential");
        fs::write(
            &path,
            "-- query 1\nSELECT 1;\n\n-- name: legacy_agg @sequential\n-- query 2\nSELECT 2;\n",
        )
        .expect("write sql file");

        let case = load_sql_case_from_file(
            &path,
            &meta_re,
            &marker_re,
            &std::collections::HashMap::new(),
        )
        .expect("load sql case")
        .expect("case should be loaded");

        assert!(case.sequential);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn suite_hook_extracts_catalog_override_and_sql() {
        let meta_re = Regex::new(r"^--\s*@([a-zA-Z0-9_]+)\s*=\s*(.+?)\s*$").expect("meta regex");
        let raw = "-- @catalog=iceberg_cat_${uuid0}\n-- @db=tpch\nCREATE EXTERNAL CATALOG `iceberg_cat_${uuid0}`;";
        let variables =
            std::collections::HashMap::from([("uuid0".to_string(), "abc123".to_string())]);
        let substituted =
            substitute_placeholders(raw, &variables, "test suite hook").expect("substitute");
        let lines: Vec<String> = substituted.lines().map(ToString::to_string).collect();
        let (catalog, db, sql) = extract_suite_hook(&lines, &meta_re).expect("extract hook");

        assert_eq!(catalog.as_deref(), Some("iceberg_cat_abc123"));
        assert_eq!(db.as_deref(), Some("tpch"));
        assert_eq!(sql, "CREATE EXTERNAL CATALOG `iceberg_cat_abc123`;");
    }

    #[test]
    fn explain_contains_target_body_strips_explain_prefix() {
        assert_eq!(crate::explain_contains_target_body("SELECT 1"), "SELECT 1");
        assert_eq!(
            crate::explain_contains_target_body("EXPLAIN VERBOSE SELECT 1"),
            "SELECT 1"
        );
        assert_eq!(
            crate::explain_contains_target_body("explain analyze select 2"),
            "select 2"
        );
        assert_eq!(
            crate::explain_contains_target_body("EXPLAIN COSTS SELECT 3"),
            "SELECT 3"
        );
        assert_eq!(
            crate::explain_contains_target_body("EXPLAIN SELECT 4"),
            "SELECT 4"
        );
        assert_eq!(
            crate::explain_contains_target_body("EXPLAIN REFRESH MATERIALIZED VIEW mv1"),
            "REFRESH MATERIALIZED VIEW mv1"
        );
    }

    #[test]
    fn is_select_or_with_accepts_select_with_and_refresh() {
        assert!(crate::is_select_or_with("SELECT 1"));
        assert!(crate::is_select_or_with(
            "WITH cte AS (SELECT 1) SELECT * FROM cte"
        ));
        assert!(crate::is_select_or_with("REFRESH MATERIALIZED VIEW mv1"));
        assert!(!crate::is_select_or_with("CREATE TABLE foo (a INT)"));
        assert!(!crate::is_select_or_with("INSERT INTO foo VALUES (1)"));
    }

    #[test]
    fn explain_matching_lines_reports_lines_containing_forbidden_substring() {
        let explain = "0:PhysicalScan\n1:PhysicalHashJoin\n2:PhysicalProject";
        assert_eq!(
            crate::explain_matching_lines(explain, "HashJoin"),
            vec!["1:PhysicalHashJoin".to_string()]
        );
    }

    // ---------------------------------------------------------------------------
    // CLI cluster-size parsing and validation tests
    // ---------------------------------------------------------------------------

    #[test]
    fn cli_cluster_size_defaults_to_one() {
        let cli = Cli::try_parse_from(["sql-tests", "--suite", "ssb"]).expect("parse cli");
        assert_eq!(cli.cluster_size, None);
    }

    #[test]
    fn cli_cluster_size_2_cross_process() {
        let cli = Cli::try_parse_from([
            "sql-tests",
            "--suite",
            "ssb",
            "--cluster-mode",
            "cross-process",
            "--cluster-size",
            "2",
        ])
        .expect("parse cli");
        assert_eq!(cli.cluster_size, Some(2));
        assert_eq!(cli.cluster_mode, crate::cluster::ClusterMode::CrossProcess);
    }

    #[test]
    fn cli_cluster_size_zero_rejected() {
        let cli = Cli::try_parse_from([
            "sql-tests",
            "--suite",
            "ssb",
            "--cluster-mode",
            "cross-process",
            "--cluster-size",
            "0",
        ])
        .expect("parse cli");
        // Parsing succeeds; validation rejects it.
        let err = validate_cluster_args(
            cli.cluster_mode,
            cli.cluster_size.expect("explicit cluster size"),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("--cluster-size must be >= 1"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn cli_all_in_one_with_cluster_size_2_rejected() {
        let cli = Cli::try_parse_from(["sql-tests", "--suite", "ssb", "--cluster-size", "2"])
            .expect("parse cli");
        assert_eq!(cli.cluster_mode, crate::cluster::ClusterMode::AllInOne);
        let err = validate_cluster_args(
            cli.cluster_mode,
            cli.cluster_size.expect("explicit cluster size"),
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("all-in-one mode requires --cluster-size 1"),
            "unexpected: {err}"
        );
    }

    fn exec(header: &[&str], rows: &[&[&str]]) -> crate::types::QueryExecution {
        crate::types::QueryExecution {
            header: header.iter().map(|s| s.to_string()).collect(),
            rows: rows
                .iter()
                .map(|row| row.iter().map(|s| s.to_string()).collect())
                .collect(),
            text_output: String::new(),
            elapsed: std::time::Duration::ZERO,
        }
    }

    #[test]
    fn imv_equivalence_failure_none_when_multiset_equal() {
        let inc = exec(&["k", "c"], &[&["a", "2"], &["b", "1"]]);
        let full = exec(&["k", "c"], &[&["b", "1"], &["a", "2"]]);
        assert!(super::imv_equivalence_failure("m", &inc, &full, None).is_none());
    }

    #[test]
    fn imv_equivalence_failure_some_when_rows_differ() {
        let inc = exec(&["k", "c"], &[&["a", "2"]]);
        let full = exec(&["k", "c"], &[&["a", "3"]]);
        let msg = super::imv_equivalence_failure("m", &inc, &full, None).expect("diff");
        assert!(
            msg.contains("incremental result != full recompute"),
            "{msg}"
        );
    }

    #[test]
    fn imv_stateless_available_level_must_cover_required_level() {
        use crate::types::ImvStatelessLevel;

        assert!(super::stateless_level_satisfies(
            ImvStatelessLevel::Package,
            ImvStatelessLevel::Baseline
        ));
        assert!(super::stateless_level_satisfies(
            ImvStatelessLevel::Package,
            ImvStatelessLevel::Package
        ));
        assert!(!super::stateless_level_satisfies(
            ImvStatelessLevel::Baseline,
            ImvStatelessLevel::Package
        ));
    }
}
