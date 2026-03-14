mod config;
mod parser;
mod results;
mod runner;
mod session;
mod shell;
mod types;

use crate::config::{
    build_suite_configs, env_optional, env_or_default, list_sql_files, load_runner_config,
    placeholder_variables, resolve_config_path, resolve_path, resolve_reference_port,
    resolve_repo_root, resolve_target_port, suite_default_query_timeout,
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
    MysqlSession, case_auto_db_name, drop_case_database, execute_suite_hook, reset_case_database,
};
use crate::types::*;
use anyhow::{Context, Result};
use clap::{ArgAction, Parser, ValueEnum};
use regex::Regex;
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::thread::sleep;
use std::time::Duration;

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

#[derive(Debug, Parser)]
#[command(
    name = "sql-tests",
    about = "Run SQL correctness tests for suite directories under sql-tests/"
)]
struct Cli {
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
}

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

fn query_order_sensitive(step: &SqlStep, cli: &Cli) -> bool {
    step.meta
        .order_sensitive
        .unwrap_or(cli.order_sensitive_default)
}

fn query_float_epsilon(step: &SqlStep, cli: &Cli) -> Option<f64> {
    step.meta.float_epsilon.or(cli.float_epsilon)
}

fn query_db(step: &SqlStep, fallback: Option<&str>) -> Option<String> {
    step.meta
        .db
        .clone()
        .or_else(|| fallback.map(ToString::to_string))
}

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

    let suite_names: Vec<String> = suite_configs.keys().cloned().collect();
    if !suite_configs.contains_key(&cli.suite) {
        println!(
            "❌ ERROR: unknown suite '{}'; available suites: {}",
            cli.suite,
            suite_names.join(", ")
        );
        std::process::exit(1);
    }

    if let Some(eps) = cli.float_epsilon {
        if eps <= 0.0 {
            println!("❌ ERROR: --float-epsilon must be > 0");
            std::process::exit(1);
        }
    }

    let suite = suite_configs
        .get(&cli.suite)
        .context("suite lookup failed unexpectedly")?;

    let sql_dir =
        resolve_path(cli.sql_dir.as_deref(), &base_dir).unwrap_or_else(|| suite.sql_dir.clone());
    let result_dir =
        resolve_path(cli.result_dir.as_deref(), &base_dir).or_else(|| suite.result_dir.clone());
    let sql_glob = cli
        .sql_glob
        .clone()
        .unwrap_or_else(|| suite.sql_glob.clone());

    let meta_re = Regex::new(r"^--\s*@([a-zA-Z0-9_]+)\s*=\s*(.+?)\s*$")?;
    let marker_re = Regex::new(r"(?i)^--\s*query\s+(\d+)(?:\s+.*)?$")?;
    let placeholder_vars = placeholder_variables(&runner_config, &suite.name);
    let suite_init_hook =
        load_suite_hook(suite.init_sql.as_deref(), &meta_re, &placeholder_vars)
            .with_context(|| format!("failed to load suite init hook for {}", suite.name))?;
    let suite_cleanup_hook =
        load_suite_hook(suite.cleanup_sql.as_deref(), &meta_re, &placeholder_vars)
            .with_context(|| format!("failed to load suite cleanup hook for {}", suite.name))?;

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

    let verify_enabled = verify_override(&cli).unwrap_or(suite.verify_default);
    let query_timeout = cli.query_timeout.unwrap_or_else(|| {
        env_optional("STARUST_TEST_TIMEOUT")
            .and_then(|raw| raw.parse().ok())
            .unwrap_or_else(|| suite_default_query_timeout(&suite.name))
    });
    let reference_required = cli.mode == Mode::Diff
        || (cli.mode == Mode::Record && cli.record_from == RecordFrom::Reference);
    let target_port = resolve_target_port(cli.port.as_deref(), &runner_config)?;
    let reference_port =
        resolve_reference_port(cli.ref_port.as_deref(), &target_port, reference_required)?;
    let target_catalog_name = suite_catalog_override
        .clone()
        .unwrap_or_else(|| suite.default_catalog.clone());
    let reference_catalog_name = suite_catalog_override
        .clone()
        .unwrap_or_else(|| suite.default_catalog.clone());

    let target_conn_base = ConnectionConfig {
        mysql: cli
            .mysql
            .clone()
            .unwrap_or_else(|| env_or_default("STARUST_TEST_MYSQL", "mysql")),
        host: cli
            .host
            .clone()
            .or_else(|| env_optional("STARUST_TEST_HOST"))
            .or_else(|| runner_config.cluster.get("host").cloned())
            .unwrap_or_else(|| "127.0.0.1".to_string()),
        port: target_port,
        user: cli
            .user
            .clone()
            .or_else(|| env_optional("STARUST_TEST_USER"))
            .or_else(|| runner_config.cluster.get("user").cloned())
            .unwrap_or_else(|| "root".to_string()),
        password: cli
            .password
            .clone()
            .or_else(|| env_optional("STARUST_TEST_PASSWORD"))
            .or_else(|| runner_config.cluster.get("password").cloned()),
        catalog: Some(target_catalog_name.clone()),
        db: if target_db_default.is_empty() {
            None
        } else {
            Some(target_db_default)
        },
    };

    let reference_conn_base = ConnectionConfig {
        mysql: cli
            .ref_mysql
            .clone()
            .unwrap_or_else(|| env_or_default("STARUST_REF_MYSQL", "mysql")),
        host: cli
            .ref_host
            .clone()
            .or_else(|| env_optional("STARUST_REF_HOST"))
            .unwrap_or_else(|| "127.0.0.1".to_string()),
        port: reference_port,
        user: cli
            .ref_user
            .clone()
            .or_else(|| env_optional("STARUST_REF_USER"))
            .unwrap_or_else(|| "root".to_string()),
        password: cli
            .ref_password
            .clone()
            .or_else(|| env_optional("STARUST_REF_PASSWORD")),
        catalog: Some(reference_catalog_name.clone()),
        db: if ref_db_default.is_empty() {
            None
        } else {
            Some(ref_db_default)
        },
    };

    println!("{}", "=".repeat(72));
    println!("📋 {} correctness runner", suite.name.to_uppercase());
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
    println!("{}", "=".repeat(72));

    if !sql_dir.exists() {
        println!("❌ ERROR: SQL directory not found: {}", sql_dir.display());
        std::process::exit(1);
    }

    let sql_files = list_sql_files(&sql_dir, &sql_glob)?;
    if sql_files.is_empty() {
        println!(
            "❌ ERROR: no SQL files found in {} with pattern {}",
            sql_dir.display(),
            sql_glob
        );
        std::process::exit(1);
    }

    let mut cases: Vec<SqlCase> = Vec::new();

    for sql_file in sql_files {
        match parser::load_sql_case_from_file(&sql_file, &meta_re, &marker_re, &placeholder_vars) {
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
        cases.iter().map(|case| case.case_id.clone()).collect();
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
        println!("❌ ERROR: no queries selected");
        std::process::exit(1);
    }

    if cli.dry_run {
        println!("selected cases:");
        for case in &cases {
            let file_name = case
                .source_file
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or_default();
            println!(
                "  {} ({}, steps={})",
                case.case_id,
                file_name,
                case.steps.len()
            );
        }
        return Ok(());
    }

    if matches!(cli.mode, Mode::Verify | Mode::Record) && result_dir.is_none() {
        println!("❌ ERROR: result_dir is required for verify/record mode");
        std::process::exit(1);
    }

    if cli.mode == Mode::Verify
        && verify_enabled
        && result_dir.is_some()
        && !result_dir.as_ref().is_some_and(|p| p.exists())
    {
        println!(
            "❌ ERROR: result_dir not found: {}",
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

    if let Some(hook) = suite_init_hook.as_ref() {
        println!("running suite init on target: {}", hook.path.display());
        if let Err(exc) = execute_suite_hook(&target_admin_conn, query_timeout, hook, "target") {
            if let Some(cleanup) = suite_cleanup_hook.as_ref() {
                let _ = execute_suite_hook(
                    &target_admin_conn,
                    query_timeout,
                    cleanup,
                    "target cleanup after init failure",
                );
            }
            return Err(exc);
        }
        if reference_required {
            println!("running suite init on reference: {}", hook.path.display());
            if let Err(exc) =
                execute_suite_hook(&reference_admin_conn, query_timeout, hook, "reference")
            {
                if let Some(cleanup) = suite_cleanup_hook.as_ref() {
                    let _ = execute_suite_hook(
                        &reference_admin_conn,
                        query_timeout,
                        cleanup,
                        "reference cleanup after init failure",
                    );
                    let _ = execute_suite_hook(
                        &target_admin_conn,
                        query_timeout,
                        cleanup,
                        "target cleanup after init failure",
                    );
                }
                return Err(exc);
            }
        }
    }

    let actual_artifact_dir = resolve_path(cli.write_actual_dir.as_deref(), &base_dir);

    let total = cases.len();
    let mut passed = 0usize;
    let mut failed = 0usize;
    let mut total_time = Duration::from_secs(0);
    let mut per_case_times: Vec<(String, Duration)> = Vec::new();
    let mut failed_case_ids: Vec<String> = Vec::new();
    let mut abort_run = false;

    'case_loop: for (idx, case) in cases.iter().enumerate() {
        let multi_step = case.steps.len() > 1;
        let case_path = result_dir
            .as_ref()
            .map(|dir| case_result_path(dir, &case.case_id));
        let case_requires_result_file = case.steps.iter().any(step_requires_recorded_result);
        let mut case_elapsed = Duration::from_secs(0);
        let mut case_failed = false;

        println!(
            "\n[{}/{}] {} (steps={})",
            idx + 1,
            total,
            case.case_id,
            case.steps.len()
        );

        if matches!(cli.mode, Mode::Verify | Mode::Record) {
            let Some(dir) = &result_dir else {
                println!("    ❌ missing result_dir in {:?} mode", cli.mode);
                failed += 1;
                failed_case_ids.push(case.case_id.clone());
                if cli.fail_fast {
                    break 'case_loop;
                }
                continue;
            };

            match find_legacy_result_paths(dir, &case.case_id) {
                Ok(paths) if !paths.is_empty() => {
                    failed += 1;
                    failed_case_ids.push(case.case_id.clone());
                    println!(
                        "    ❌ legacy split result files are no longer supported: {}",
                        paths
                            .iter()
                            .map(|path| path.display().to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                    if cli.fail_fast {
                        break 'case_loop;
                    }
                    continue;
                }
                Ok(_) => {}
                Err(exc) => {
                    failed += 1;
                    failed_case_ids.push(case.case_id.clone());
                    println!("    ❌ failed to inspect result_dir: {}", exc);
                    if cli.fail_fast {
                        break 'case_loop;
                    }
                    continue;
                }
            }
        }

        let expected_results = if cli.mode == Mode::Verify && verify_enabled {
            if let Some(path) = case_path.as_ref().filter(|path| path.exists()) {
                match load_expected_results(path, multi_step, &marker_re) {
                    Some(results) => Some(results),
                    None => {
                        failed += 1;
                        failed_case_ids.push(case.case_id.clone());
                        println!("    ❌ failed to load expected result: {}", path.display());
                        if cli.fail_fast {
                            break 'case_loop;
                        }
                        continue;
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        if cli.mode == Mode::Record
            && case_requires_result_file
            && case_path.as_ref().is_some_and(|path| path.exists())
            && !cli.update_expected
        {
            failed += 1;
            failed_case_ids.push(case.case_id.clone());
            println!(
                "    ❌ expected file exists ({}); rerun with --update-expected",
                case_path
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_default()
            );
            if cli.fail_fast {
                break 'case_loop;
            }
            continue;
        }

        let case_auto_db = suite
            .auto_case_db
            .then(|| case_auto_db_name(&case.case_id));

        if let Some(db_name) = case_auto_db.as_deref() {
            if let Err(exc) =
                reset_case_database(&target_admin_conn, query_timeout, db_name, "target")
            {
                failed += 1;
                failed_case_ids.push(case.case_id.clone());
                println!("    ❌ failed to prepare target case database: {:#}", exc);
                if cli.fail_fast {
                    break 'case_loop;
                }
                continue;
            }
            if reference_required {
                if let Err(exc) =
                    reset_case_database(&reference_admin_conn, query_timeout, db_name, "reference")
                {
                    let _ = drop_case_database(&target_admin_conn, query_timeout, db_name, "target");
                    failed += 1;
                    failed_case_ids.push(case.case_id.clone());
                    println!("    ❌ failed to prepare reference case database: {:#}", exc);
                    if cli.fail_fast {
                        break 'case_loop;
                    }
                    continue;
                }
            }
        }

        let case_target_conn = ConnectionConfig {
            db: case_auto_db
                .clone()
                .map(Some)
                .unwrap_or_else(|| target_conn_base.db.clone()),
            ..target_conn_base.clone()
        };
        let case_reference_conn = ConnectionConfig {
            db: case_auto_db
                .clone()
                .map(Some)
                .unwrap_or_else(|| reference_conn_base.db.clone()),
            ..reference_conn_base.clone()
        };

        let mut target_session = match MysqlSession::new(&case_target_conn) {
            Ok(session) => session,
            Err(exc) => {
                if let Some(db_name) = case_auto_db.as_deref() {
                    let _ = drop_case_database(&target_admin_conn, query_timeout, db_name, "target");
                    if reference_required {
                        let _ = drop_case_database(
                            &reference_admin_conn,
                            query_timeout,
                            db_name,
                            "reference",
                        );
                    }
                }
                failed += 1;
                failed_case_ids.push(case.case_id.clone());
                println!("    ❌ failed to create target mysql session: {:#}", exc);
                if cli.fail_fast {
                    break 'case_loop;
                }
                continue;
            }
        };
        let mut reference_session = if reference_required {
            match MysqlSession::new(&case_reference_conn) {
                Ok(session) => Some(session),
                Err(exc) => {
                    if let Some(db_name) = case_auto_db.as_deref() {
                        let _ =
                            drop_case_database(&target_admin_conn, query_timeout, db_name, "target");
                        let _ = drop_case_database(
                            &reference_admin_conn,
                            query_timeout,
                            db_name,
                            "reference",
                        );
                    }
                    failed += 1;
                    failed_case_ids.push(case.case_id.clone());
                    println!("    ❌ failed to create reference mysql session: {:#}", exc);
                    if cli.fail_fast {
                        break 'case_loop;
                    }
                    continue;
                }
            }
        } else {
            None
        };

        let mut recorded_results: BTreeMap<usize, ResultSet> = BTreeMap::new();

        for step in &case.steps {
            let order_sensitive = query_order_sensitive(step, &cli);
            let epsilon = query_float_epsilon(step, &cli);

            println!(
                "  step {} (order_sensitive={}, epsilon={:?})",
                step.query_number, order_sensitive, epsilon
            );

            match cli.mode {
                Mode::Verify => {
                    let retry_count = step_retry_count(step);
                    let retry_interval = step_retry_interval(step);
                    let mut matched_expected_error = false;
                    let mut passed_execution: Option<QueryExecution> = None;
                    let mut last_execution: Option<QueryExecution> = None;
                    let mut last_failure = String::new();

                    for attempt in 0..retry_count {
                        let (ok, execution, err_msg) = if shell::is_shell_step(&step.sql) {
                            let cmd = step.sql.trim_start().strip_prefix("shell:").unwrap_or("").trim();
                            let exec = shell::execute_shell_command(cmd);
                            (true, Some(exec), String::new())
                        } else {
                            target_session.execute_query(
                                query_timeout,
                                &step.sql,
                                step.meta.db.as_deref(),
                            )
                        };
                        let elapsed = execution
                            .as_ref()
                            .map(|result| result.elapsed)
                            .unwrap_or_default();
                        case_elapsed += elapsed;
                        total_time += elapsed;
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
                            } else if !verify_enabled {
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
                            println!(
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
                            println!("    ✅ PASS (expected error matched): {}", last_failure);
                        } else {
                            case_failed = true;
                            println!("    ❌ {}", last_failure);
                            let _ = expected_error;
                        }
                    } else if let Some(execution) = passed_execution {
                        if !verify_enabled {
                            println!(
                                "    ✅ PASS (verify disabled) ({:.2}s)",
                                execution.elapsed.as_secs_f64()
                            );
                        } else if step.meta.skip_result_check
                            || step_has_implicit_skip_result(step)
                        {
                            println!(
                                "    ✅ PASS ({:.2}s, skip_result_check)",
                                execution.elapsed.as_secs_f64()
                            );
                        } else if expected_results
                            .as_ref()
                            .and_then(|results| results.get(&step.query_number))
                            .is_some()
                        {
                            println!(
                                "    ✅ PASS ({:.2}s, rows={})",
                                execution.elapsed.as_secs_f64(),
                                execution.rows.len()
                            );
                            for row in execution.rows.iter().take(cli.preview_lines) {
                                println!("    {:?}", row);
                            }
                        } else {
                            println!(
                                "    ✅ PASS ({:.2}s, text assertions only)",
                                execution.elapsed.as_secs_f64()
                            );
                        }
                    } else {
                        case_failed = true;
                        println!("    ❌ {}", last_failure);
                        if let (Some(root), Some(expected), Some(execution)) = (
                            actual_artifact_dir.as_ref(),
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
                                    &suite.name,
                                    &artifact_id,
                                    &expected.header,
                                    &expected.rows,
                                    &execution.header,
                                    &execution.rows,
                                    &reason,
                                ) {
                                    println!(
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
                        let (ok, execution, err_msg) = if cli.record_from == RecordFrom::Target {
                            if shell::is_shell_step(&step.sql) {
                                let cmd = step.sql.trim_start().strip_prefix("shell:").unwrap_or("").trim();
                                let exec = shell::execute_shell_command(cmd);
                                (true, Some(exec), String::new())
                            } else {
                                target_session.execute_query(
                                    query_timeout,
                                    &step.sql,
                                    step.meta.db.as_deref(),
                                )
                            }
                        } else if shell::is_shell_step(&step.sql) {
                            let cmd = step.sql.trim_start().strip_prefix("shell:").unwrap_or("").trim();
                            let exec = shell::execute_shell_command(cmd);
                            (true, Some(exec), String::new())
                        } else {
                            reference_session
                                .as_mut()
                                .expect("reference session required in record-from=reference")
                                .execute_query(
                                    query_timeout,
                                    &step.sql,
                                    step.meta.db.as_deref(),
                                )
                        };
                        let elapsed = execution
                            .as_ref()
                            .map(|result| result.elapsed)
                            .unwrap_or_default();
                        case_elapsed += elapsed;
                        total_time += elapsed;

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
                            println!(
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
                            println!("    ✅ RECORDED EXPECTED ERROR: {}", last_failure);
                        } else {
                            case_failed = true;
                            println!("    ❌ {}", last_failure);
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
                            println!(
                                "    ✅ STEP RECORDED ({:.2}s, skip_result_check)",
                                execution.elapsed.as_secs_f64()
                            );
                        } else {
                            println!(
                                "    ✅ STEP RECORDED ({:.2}s, rows={})",
                                execution.elapsed.as_secs_f64(),
                                execution.rows.len()
                            );
                        }
                    } else {
                        case_failed = true;
                        println!("    ❌ {}", last_failure);
                    }
                }
                Mode::Diff => {
                    if let Some(expected_error) = step.meta.expect_error.as_deref() {
                        let (ok_t, execution_t, err_t) = if shell::is_shell_step(&step.sql) {
                            let cmd = step.sql.trim_start().strip_prefix("shell:").unwrap_or("").trim();
                            let exec = shell::execute_shell_command(cmd);
                            (true, Some(exec), String::new())
                        } else {
                            target_session.execute_query(
                                query_timeout,
                                &step.sql,
                                step.meta.db.as_deref(),
                            )
                        };
                        let (ok_r, execution_r, err_r) = if shell::is_shell_step(&step.sql) {
                            let cmd = step.sql.trim_start().strip_prefix("shell:").unwrap_or("").trim();
                            let exec = shell::execute_shell_command(cmd);
                            (true, Some(exec), String::new())
                        } else {
                            reference_session
                                .as_mut()
                                .expect("reference session required in diff mode")
                                .execute_query(
                                    query_timeout,
                                    &step.sql,
                                    step.meta.db.as_deref(),
                                )
                        };
                        let elapsed = execution_t
                            .as_ref()
                            .map(|result| result.elapsed)
                            .unwrap_or_default()
                            + execution_r
                                .as_ref()
                                .map(|result| result.elapsed)
                                .unwrap_or_default();
                        case_elapsed += elapsed;
                        total_time += elapsed;

                        let target_matched = !ok_t && error_message_matches(&err_t, expected_error);
                        let reference_matched =
                            !ok_r && error_message_matches(&err_r, expected_error);
                        if target_matched && reference_matched {
                            println!(
                                "    ✅ DIFF PASS (both sides matched expected error: {:?})",
                                expected_error
                            );
                        } else {
                            case_failed = true;
                            println!(
                                "    ❌ DIFF FAILED expected error {:?} (target_ok={}, target_err={}, reference_ok={}, reference_err={})",
                                expected_error, ok_t, err_t, ok_r, err_r
                            );
                        }
                    } else {
                        let (ok_t, execution_t, err_t) = if shell::is_shell_step(&step.sql) {
                            let cmd = step.sql.trim_start().strip_prefix("shell:").unwrap_or("").trim();
                            let exec = shell::execute_shell_command(cmd);
                            (true, Some(exec), String::new())
                        } else {
                            target_session.execute_query(
                                query_timeout,
                                &step.sql,
                                step.meta.db.as_deref(),
                            )
                        };
                        if !ok_t || execution_t.is_none() {
                            case_failed = true;
                            println!("    ❌ target execute failed: {}", err_t);
                        } else {
                            let execution_t = execution_t.expect("checked above");
                            let (ok_r, execution_r, err_r) = if shell::is_shell_step(&step.sql) {
                                let cmd = step.sql.trim_start().strip_prefix("shell:").unwrap_or("").trim();
                                let exec = shell::execute_shell_command(cmd);
                                (true, Some(exec), String::new())
                            } else {
                                reference_session
                                    .as_mut()
                                    .expect("reference session required in diff mode")
                                    .execute_query(
                                        query_timeout,
                                        &step.sql,
                                        step.meta.db.as_deref(),
                                    )
                            };
                            if !ok_r || execution_r.is_none() {
                                case_failed = true;
                                case_elapsed += execution_t.elapsed;
                                total_time += execution_t.elapsed;
                                println!("    ❌ reference execute failed: {}", err_r);
                            } else {
                                let execution_r = execution_r.expect("checked above");
                                let elapsed = execution_t.elapsed + execution_r.elapsed;
                                case_elapsed += elapsed;
                                total_time += elapsed;

                                let (same, reason) = compare_result_sets(
                                    &execution_r.header,
                                    &execution_r.rows,
                                    &execution_t.header,
                                    &execution_t.rows,
                                    order_sensitive,
                                    epsilon,
                                );
                                if same {
                                    println!(
                                        "    ✅ DIFF PASS (target={:.2}s, reference={:.2}s)",
                                        execution_t.elapsed.as_secs_f64(),
                                        execution_r.elapsed.as_secs_f64()
                                    );
                                } else {
                                    case_failed = true;
                                    println!("    ❌ DIFF FAILED: {}", reason);
                                    if let Some(root) = &actual_artifact_dir {
                                        let artifact_id =
                                            format!("{}-query{}", case.case_id, step.query_number);
                                        if let Err(exc) = write_mismatch_artifacts(
                                            root,
                                            &suite.name,
                                            &artifact_id,
                                            &execution_r.header,
                                            &execution_r.rows,
                                            &execution_t.header,
                                            &execution_t.rows,
                                            &reason,
                                        ) {
                                            println!(
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
                println!("    ⏭️ skipping remaining steps in {}", case.case_id);
                break;
            }
        }

        drop(target_session);
        drop(reference_session);

        if let Some(db_name) = case_auto_db.as_deref() {
            if let Err(exc) = drop_case_database(&target_admin_conn, query_timeout, db_name, "target")
            {
                case_failed = true;
                println!("    ❌ failed to cleanup target case database: {:#}", exc);
            }
            if reference_required {
                if let Err(exc) = drop_case_database(
                    &reference_admin_conn,
                    query_timeout,
                    db_name,
                    "reference",
                ) {
                    case_failed = true;
                    println!("    ❌ failed to cleanup reference case database: {:#}", exc);
                }
            }
        }

        if !case_failed && cli.mode == Mode::Record && case_requires_result_file {
            if let Some(path) = case_path.as_ref() {
                if let Err(exc) = write_result_file(path, &recorded_results, multi_step) {
                    case_failed = true;
                    println!("    ❌ failed to write expected result: {}", exc);
                } else {
                    println!("    ✅ RECORDED CASE -> {}", path.display());
                }
            }
        }

        per_case_times.push((case.case_id.clone(), case_elapsed));
        if case_failed {
            failed += 1;
            failed_case_ids.push(case.case_id.clone());
            if cli.fail_fast {
                abort_run = true;
            }
        } else {
            passed += 1;
        }

        println!(
            "    progress: pass={}, fail={}, elapsed={:.2}s",
            passed,
            failed,
            total_time.as_secs_f64()
        );

        if abort_run {
            break;
        }
    }

    let mut cleanup_errors = Vec::new();
    if let Some(hook) = suite_cleanup_hook.as_ref() {
        println!("\nrunning suite cleanup on target: {}", hook.path.display());
        if let Err(exc) = execute_suite_hook(&target_admin_conn, query_timeout, hook, "target") {
            cleanup_errors.push(exc.to_string());
        }
        if reference_required {
            println!(
                "running suite cleanup on reference: {}",
                hook.path.display()
            );
            if let Err(exc) =
                execute_suite_hook(&reference_admin_conn, query_timeout, hook, "reference")
            {
                cleanup_errors.push(exc.to_string());
            }
        }
    }

    println!("\n{}", "=".repeat(72));
    println!("summary ({}, mode={})", suite.name, mode_name(cli.mode));
    println!("{}", "=".repeat(72));
    println!("total={}", total);
    println!("pass={}", passed);
    println!("fail={}", failed);
    println!("elapsed={:.2}s", total_time.as_secs_f64());

    per_case_times.sort_by(|a, b| b.1.cmp(&a.1));
    println!("\nslowest cases (top 5):");
    for (case_id, elapsed) in per_case_times.iter().take(5) {
        println!("  {}: {:.2}s", case_id, elapsed.as_secs_f64());
    }

    if !failed_case_ids.is_empty() {
        println!("\nfailed cases:");
        for id in &failed_case_ids {
            println!("  {}", id);
        }
    }
    if !cleanup_errors.is_empty() {
        println!("\ncleanup errors:");
        for err in &cleanup_errors {
            println!("  {}", err);
        }
    }
    println!("{}", "=".repeat(72));

    if failed > 0 || !cleanup_errors.is_empty() {
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
