mod config;
mod parser;
mod results;
mod runner;
mod session;
mod shell;
mod types;

use crate::config::{
    build_suite_configs, case_auto_db_name, env_optional, env_or_default, list_sql_files,
    load_runner_config, placeholder_variables, resolve_config_path, resolve_path,
    resolve_reference_port, resolve_repo_root, resolve_target_port, suite_default_query_timeout,
};
use crate::parser::load_suite_hook;
use crate::results::{
    case_result_path, compare_result_sets, find_legacy_result_paths, load_expected_results,
    step_allows_missing_expected_result, step_has_implicit_skip_result,
    step_requires_recorded_result, step_retry_count, step_retry_interval, verify_text_assertions,
    write_mismatch_artifacts, write_result_file,
};
use crate::runner::{error_message_matches, parse_selector_list, summarize_connection};
use crate::session::{
    MysqlSession, drop_case_database, execute_suite_hook, reset_case_database,
};
use crate::types::*;
use anyhow::{Context, Result};
use clap::{ArgAction, Parser, ValueEnum};
use rayon::prelude::*;
use regex::Regex;
use std::collections::{BTreeMap, HashSet};
use std::fmt::Write as FmtWrite;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread::sleep;
use std::time::{Duration, Instant};

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

    #[arg(long, action = ArgAction::SetTrue)]
    dry_run: bool,

    #[arg(long, action = ArgAction::SetTrue)]
    fail_fast: bool,

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
    marker_re: Regex,
    fail_fast: bool,
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

/// Everything needed to run a suite: context + prepared cases + hooks.
struct PreparedSuite {
    ctx: SuiteRunContext,
    cases: Vec<SqlCase>,
    init_hook: Option<SuiteHook>,
    cleanup_hook: Option<SuiteHook>,
}

// ---------------------------------------------------------------------------
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

    let multi_step = case.steps.len() > 1;
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
            match load_expected_results(path, multi_step, &ctx.marker_re) {
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

    // Helper closure: drop all case databases (best-effort).
    let drop_all_case_dbs = |ctx: &SuiteRunContext, dbs: &[String]| {
        for db in dbs {
            let _ = drop_case_database(&ctx.target_admin_conn, ctx.query_timeout, db, "target");
            if ctx.reference_required {
                let _ = drop_case_database(
                    &ctx.reference_admin_conn,
                    ctx.query_timeout,
                    db,
                    "reference",
                );
            }
        }
    };

    for db_name in &case_dbs {
        if let Err(exc) =
            reset_case_database(&ctx.target_admin_conn, ctx.query_timeout, db_name, "target")
        {
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
                &ctx.reference_admin_conn,
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
                        let cmd =
                            step.sql.trim_start().strip_prefix("shell:").unwrap_or("").trim();
                        let exec = shell::execute_shell_command(cmd);
                        (true, Some(exec), String::new())
                    } else {
                        target_session.execute_query(
                            ctx.query_timeout,
                            &step.sql,
                            step.meta.db.as_deref(),
                        )
                    };
                    let elapsed = execution
                        .as_ref()
                        .map(|result| result.elapsed)
                        .unwrap_or_default();
                    case_elapsed += elapsed;
                    last_execution = execution.clone();

                    if let Some(expected_error) = step.meta.expect_error.as_deref() {
                        if ok {
                            last_failure = format!(
                                "expected error containing {:?}, but query succeeded",
                                expected_error
                            );
                        } else if error_message_matches(&err_msg, expected_error) {
                            matched_expected_error = true;
                            last_failure = err_msg.clone();
                            break;
                        } else {
                            last_failure = format!(
                                "expected error containing {:?}, got: {}",
                                expected_error, err_msg
                            );
                        }
                    } else if !ok || execution.is_none() {
                        last_failure = format!("target execute failed: {}", err_msg);
                    } else {
                        let execution = execution.expect("checked above");
                        let (assertions_ok, assertions_reason) =
                            verify_text_assertions(step, &execution);
                        if !assertions_ok {
                            last_failure = format!("VERIFY FAILED: {}", assertions_reason);
                        } else if !ctx.verify_enabled {
                            passed_execution = Some(execution);
                            break;
                        } else if step.meta.skip_result_check
                            || step_has_implicit_skip_result(step)
                        {
                            passed_execution = Some(execution);
                            break;
                        } else if let Some(expected) = expected_results
                            .as_ref()
                            .and_then(|results| results.get(&step.query_number))
                        {
                            let (same, reason) = compare_result_sets(
                                &expected.header,
                                &expected.rows,
                                &execution.header,
                                &execution.rows,
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

                if let Some(expected_error) = step.meta.expect_error.as_deref() {
                    if matched_expected_error {
                        let _ = writeln!(
                            log,
                            "    ✅ PASS (expected error matched): {}",
                            last_failure
                        );
                    } else {
                        case_failed = true;
                        let _ = writeln!(log, "    ❌ {}", last_failure);
                        let _ = expected_error;
                    }
                } else if let Some(execution) = passed_execution {
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
                } else {
                    case_failed = true;
                    let _ = writeln!(log, "    ❌ {}", last_failure);
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
                            let reason =
                                last_failure.trim_start_matches("VERIFY FAILED: ").to_string();
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
                            target_session.execute_query(
                                ctx.query_timeout,
                                &step.sql,
                                step.meta.db.as_deref(),
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
                            .execute_query(
                                ctx.query_timeout,
                                &step.sql,
                                step.meta.db.as_deref(),
                            )
                    };
                    let elapsed = execution
                        .as_ref()
                        .map(|result| result.elapsed)
                        .unwrap_or_default();
                    case_elapsed += elapsed;

                    if let Some(expected_error) = step.meta.expect_error.as_deref() {
                        if ok {
                            last_failure = format!(
                                "expected error containing {:?}, but query succeeded",
                                expected_error
                            );
                        } else if error_message_matches(&err_msg, expected_error) {
                            matched_expected_error = true;
                            last_failure = err_msg.clone();
                            break;
                        } else {
                            last_failure = format!(
                                "expected error containing {:?}, got: {}",
                                expected_error, err_msg
                            );
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

                if step.meta.expect_error.is_some() {
                    if matched_expected_error {
                        let _ =
                            writeln!(log, "    ✅ RECORDED EXPECTED ERROR: {}", last_failure);
                    } else {
                        case_failed = true;
                        let _ = writeln!(log, "    ❌ {}", last_failure);
                    }
                } else if let Some(execution) = recorded_execution {
                    if step_requires_recorded_result(step) {
                        recorded_results.insert(
                            step.query_number,
                            ResultSet {
                                header: execution.header.clone(),
                                rows: execution.rows.clone(),
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
                } else {
                    case_failed = true;
                    let _ = writeln!(log, "    ❌ {}", last_failure);
                }
            }
            Mode::Diff => {
                if let Some(expected_error) = step.meta.expect_error.as_deref() {
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
                        target_session.execute_query(
                            ctx.query_timeout,
                            &step.sql,
                            step.meta.db.as_deref(),
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
                            .execute_query(
                                ctx.query_timeout,
                                &step.sql,
                                step.meta.db.as_deref(),
                            )
                    };
                    let elapsed = execution_t
                        .as_ref()
                        .map(|r| r.elapsed)
                        .unwrap_or_default()
                        + execution_r
                            .as_ref()
                            .map(|r| r.elapsed)
                            .unwrap_or_default();
                    case_elapsed += elapsed;

                    let target_matched =
                        !ok_t && error_message_matches(&err_t, expected_error);
                    let reference_matched =
                        !ok_r && error_message_matches(&err_r, expected_error);
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
                        target_session.execute_query(
                            ctx.query_timeout,
                            &step.sql,
                            step.meta.db.as_deref(),
                        )
                    };
                    if !ok_t || execution_t.is_none() {
                        case_failed = true;
                        let _ = writeln!(log, "    ❌ target execute failed: {}", err_t);
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
                            let _ =
                                writeln!(log, "    ❌ reference execute failed: {}", err_r);
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
                                    let artifact_id = format!(
                                        "{}-query{}",
                                        case.case_id, step.query_number
                                    );
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

        if case_failed {
            let _ = writeln!(
                log,
                "    ⏭️ skipping remaining steps in {}",
                case.case_id
            );
            break;
        }
    }

    // --- cleanup ---
    drop(target_session);
    drop(reference_session);

    for db_name in &case_dbs {
        if let Err(exc) =
            drop_case_database(&ctx.target_admin_conn, ctx.query_timeout, db_name, "target")
        {
            case_failed = true;
            let _ = writeln!(
                log,
                "    ❌ failed to cleanup target case database {}: {:#}",
                db_name, exc
            );
        }
        if ctx.reference_required {
            if let Err(exc) = drop_case_database(
                &ctx.reference_admin_conn,
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

fn run_suite(
    ps: &PreparedSuite,
    abort: &AtomicBool,
    stdout_lock: &Mutex<()>,
) -> SuiteOutcome {
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

    // --- run cases in parallel ---
    let outcomes: Vec<CaseOutcome> = ps
        .cases
        .par_iter()
        .map(|case| {
            let outcome = run_case(ctx, case, abort);
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
            // Flush case log atomically
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
            outcome
        })
        .collect();

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

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() -> Result<()> {
    let cli = Cli::parse();
    let base_dir = resolve_repo_root()?;
    let config_path = resolve_config_path(cli.config.as_deref(), &base_dir);
    let runner_config = load_runner_config(config_path.as_deref())?;

    let suite_configs = build_suite_configs(&base_dir)?;
    if suite_configs.is_empty() {
        println!("❌ ERROR: no suite directories found under sql-tests");
        std::process::exit(1);
    }

    // Resolve selected suites
    let suite_names: Vec<String> = if cli.suite.eq_ignore_ascii_case("all") {
        suite_configs.keys().cloned().collect()
    } else {
        cli.suite
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string)
            .collect()
    };

    let all_available: Vec<String> = suite_configs.keys().cloned().collect();
    for name in &suite_names {
        if !suite_configs.contains_key(name) {
            println!(
                "❌ ERROR: unknown suite '{}'; available suites: {}",
                name,
                all_available.join(", ")
            );
            std::process::exit(1);
        }
    }

    if suite_names.is_empty() {
        println!("❌ ERROR: no suites selected");
        std::process::exit(1);
    }

    // Validate: per-suite path overrides conflict with multi-suite
    let multi_suite = suite_names.len() > 1;
    if multi_suite
        && (cli.sql_dir.is_some() || cli.result_dir.is_some() || cli.sql_glob.is_some())
    {
        println!(
            "❌ ERROR: --sql-dir, --result-dir, --sql-glob cannot be used with multiple suites"
        );
        std::process::exit(1);
    }

    if let Some(eps) = cli.float_epsilon {
        if eps <= 0.0 {
            println!("❌ ERROR: --float-epsilon must be > 0");
            std::process::exit(1);
        }
    }

    // Resolve global connection params
    let reference_required = cli.mode == Mode::Diff
        || (cli.mode == Mode::Record && cli.record_from == RecordFrom::Reference);
    let target_port = resolve_target_port(cli.port.as_deref(), &runner_config)?;
    let reference_port =
        resolve_reference_port(cli.ref_port.as_deref(), &target_port, reference_required)?;

    let target_host = cli
        .host
        .clone()
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
                .with_context(|| format!("failed to load suite init hook for {}", suite.name))?;
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
            catalog: Some(target_catalog_name),
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
            std::process::exit(1);
        }

        let sql_files = list_sql_files(&sql_dir, &sql_glob)?;
        if sql_files.is_empty() {
            println!(
                "❌ ERROR: no SQL files found in {} with pattern {} (suite {})",
                sql_dir.display(),
                sql_glob,
                suite.name,
            );
            std::process::exit(1);
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
                    std::process::exit(1);
                }
            }
        }

        let available_case_ids: HashSet<String> =
            cases.iter().map(|c| c.case_id.clone()).collect();
        let only_set =
            parse_selector_list(cli.only.as_deref(), &available_case_ids, "--only")?;
        let skip_set =
            parse_selector_list(cli.skip.as_deref(), &available_case_ids, "--skip")?;

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
            println!(
                "⚠️ WARNING: no queries selected for suite {}",
                suite.name
            );
            continue;
        }

        if matches!(cli.mode, Mode::Verify | Mode::Record) && result_dir.is_none() {
            println!(
                "❌ ERROR: result_dir is required for verify/record mode (suite {})",
                suite.name
            );
            std::process::exit(1);
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
            std::process::exit(1);
        }

        if cli.mode == Mode::Record {
            if let Some(dir) = &result_dir {
                fs::create_dir_all(dir)
                    .with_context(|| format!("create result_dir failed: {}", dir.display()))?;
            }
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
        println!("sql_dir={}", sql_dir.display());
        println!("sql_glob={}", sql_glob);
        if let Some(path) = runner_config.path.as_deref() {
            println!("config={}", path.display());
        }
        if let Some(dir) = &result_dir {
            println!("result_dir={}", dir.display());
        }
        println!("query_timeout={}s", query_timeout);
        println!(
            "{}",
            summarize_connection("target", &target_conn_base)
        );
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
        println!("cases={}", cases.len());
        println!("{}", "=".repeat(72));

        if cli.dry_run {
            println!("selected cases for suite {}:", suite.name);
            for case in &cases {
                let file_name = case
                    .source_file
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or_default();
                println!(
                    "  {} ({}, steps={})",
                    case.case_id, file_name,
                    case.steps.len()
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
            marker_re: marker_re.clone(),
            fail_fast: cli.fail_fast,
        };

        prepared_suites.push(PreparedSuite {
            ctx,
            cases,
            init_hook: suite_init_hook,
            cleanup_hook: suite_cleanup_hook,
        });
    }

    if cli.dry_run {
        return Ok(());
    }

    if prepared_suites.is_empty() {
        println!("❌ ERROR: no suites to run");
        std::process::exit(1);
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
    let mut all_case_times: Vec<(String, String, Duration)> = Vec::new();
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
            all_case_times.push((so.suite_name.clone(), co.case_id.clone(), co.elapsed));
        }
        all_cleanup_errors.extend(so.cleanup_errors.iter().cloned());
    }

    let total_cpu_time: Duration = all_case_times.iter().map(|(_, _, d)| *d).sum();

    // Print summary
    println!("\n{}", "=".repeat(72));
    if suite_outcomes.len() == 1 {
        println!(
            "summary ({}, mode={})",
            suite_outcomes[0].suite_name,
            mode_name(cli.mode)
        );
    } else {
        let names: Vec<&str> = suite_outcomes.iter().map(|s| s.suite_name.as_str()).collect();
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

    all_case_times.sort_by(|a, b| b.2.cmp(&a.2));
    println!("\nslowest cases (top 5):");
    for (suite, case_id, elapsed) in all_case_times.iter().take(5) {
        println!("  [{}] {}: {:.2}s", suite, case_id, elapsed.as_secs_f64());
    }

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
        std::process::exit(1);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::parser::extract_suite_hook;
    use crate::results::{load_expected_results, parse_output, write_result_file};
    use crate::runner::{is_transient_iceberg_commit_error, parse_selector_list};
    use crate::config::substitute_placeholders;
    use crate::types::ResultSet;
    use regex::Regex;
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_result_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "novarocks_sql_tests_{}_{}_{}.result",
            name,
            std::process::id(),
            nanos
        ))
    }

    #[test]
    fn load_expected_result_accepts_empty_file() {
        let path = temp_result_path("empty_load");
        fs::write(&path, "\n").expect("write empty file");
        let marker_re = Regex::new(r"(?i)^--\s*query\s+(\d+)(?:\s+.*)?$").expect("marker regex");
        let loaded =
            load_expected_results(&path, false, &marker_re).expect("must parse empty result file");
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
        let loaded =
            load_expected_results(&path, false, &marker_re).expect("must parse empty result file");
        let result_set = loaded.get(&1).expect("single-step result");
        assert!(result_set.header.is_empty());
        assert!(result_set.rows.is_empty());
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
        let loaded = load_expected_results(&path, true, &marker_re)
            .expect("must parse multi-step result file");
        assert_eq!(loaded, result_sets);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn load_expected_results_rejects_multi_step_without_markers() {
        let path = temp_result_path("bad_multi_step");
        fs::write(&path, "count(*)\n1\n").expect("write bad multi-step file");
        let marker_re = Regex::new(r"(?i)^--\s*query\s+(\d+)(?:\s+.*)?$").expect("marker regex");
        let loaded = load_expected_results(&path, true, &marker_re);
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
        let loaded = load_expected_results(&path, true, &marker_re).expect("load result");
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
    fn transient_iceberg_commit_error_ignores_regular_failures() {
        let message =
            "ERROR 5904 (42000) at line 10: Warehouse default_warehouse is not available.";
        assert!(!is_transient_iceberg_commit_error(message));
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
}
