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

mod be_log_directive;
pub mod build_information;
mod catalog_symbol;
mod cluster;
mod config;
mod control_plan;
mod dataset_fixture;
mod extension_manifest;
mod failure_artifacts;
mod fault_injection;
mod fixture_context;
mod fixture_object_store;
mod iceberg_orphan_fixture;
mod mv_rest_document_graph;
mod namespace_guard;
mod parser;
mod publication_catalog;
mod publication_service;
mod query_stats_contract;
mod results;
mod results_record;
mod runner;
mod session;
mod shell;
#[cfg(unix)]
mod socket_deadline;
mod sql_error_codes;
mod statement_controls;
mod suite_manifest;
mod types;

#[path = "../../../../../novarocks/types/src/engine_error_codes.rs"]
mod engine_error_codes;

use crate::cluster::{ClusterMode, ServerHandle, launch_server, validate_cluster_args};
use crate::config::{
    FixtureEndpoints, build_suite_configs, env_optional, env_or_default, list_sql_files,
    load_runner_config, placeholder_variables, project_fixture_value, resolve_config_path,
    resolve_path, resolve_reference_port, resolve_repo_root, resolve_target_port,
    suite_default_query_timeout,
};
use crate::failure_artifacts::{FailureArtifactContext, FailureArtifactRecorder};
use crate::parser::load_suite_hook;
use crate::results::{
    MismatchArtifacts, case_result_path, compare_result_sets, find_legacy_result_paths,
    load_expected_results, normalize_explain_timing_rows, step_allows_missing_expected_result,
    step_has_implicit_skip_result, step_requires_recorded_result, step_retry_count,
    step_retry_interval, verify_text_assertions, write_mismatch_artifacts, write_result_file,
};
use crate::runner::{
    error_message_matches, extract_engine_error_code, extract_sql_error_code,
    extract_sql_error_location, parse_selector_list, summarize_connection,
};
use crate::session::MysqlSession;
use crate::sql_error_codes::{
    SqlErrorDescriptor, SqlErrorPhase, is_sql_error_identity, lookup_sql_error_descriptor,
};
use crate::suite_manifest::select_suite_names;
use crate::types::*;
use anyhow::{Context, Result, bail, ensure};
use clap::{ArgAction, Parser, ValueEnum};
use rayon::prelude::*;
use regex::Regex;
use std::collections::{BTreeMap, HashSet};
use std::fmt::Write as FmtWrite;
use std::fs;
use std::path::PathBuf;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::{Duration, Instant};

const SQL_ERROR_MANIFEST: &str =
    include_str!("../../../../../tools/error-manifest/error-manifest-v1.tsv");

static PRODUCTION_SQL_ERROR_DESCRIPTORS: LazyLock<Vec<SqlErrorDescriptor>> = LazyLock::new(|| {
    parse_sql_error_manifest(SQL_ERROR_MANIFEST)
        .expect("checked-in SQL error manifest must be valid")
});

pub(crate) fn production_sql_error_descriptors() -> &'static [SqlErrorDescriptor] {
    PRODUCTION_SQL_ERROR_DESCRIPTORS.as_slice()
}

fn parse_sql_error_manifest(source: &'static str) -> Result<Vec<SqlErrorDescriptor>, String> {
    let mut lines = source.lines().filter(|line| !line.starts_with('#'));
    if lines.next() != Some("schema_version=1") {
        return Err("SQL error manifest has unsupported schema".to_owned());
    }
    if lines.next() != Some("code\tphase") {
        return Err("SQL error manifest has invalid header".to_owned());
    }

    let mut descriptors = Vec::new();
    let mut codes = HashSet::new();
    for line in lines.filter(|line| !line.is_empty()) {
        let mut fields = line.split('\t');
        let code = fields.next().unwrap_or_default();
        let phase = fields.next().unwrap_or_default();
        if fields.next().is_some() {
            return Err(format!("invalid SQL error manifest entry: {line}"));
        }
        let phase = SqlErrorPhase::parse(phase)
            .ok_or_else(|| format!("invalid SQL error manifest phase: {phase}"))?;
        let descriptor = SqlErrorDescriptor { code, phase };
        if !is_sql_error_identity(code, std::slice::from_ref(&descriptor)) {
            return Err(format!("invalid SQL error manifest entry: {line}"));
        }
        if !codes.insert(code) {
            return Err(format!("duplicate SQL error manifest code: {code}"));
        }
        descriptors.push(descriptor);
    }
    Ok(descriptors)
}

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

fn expected_sql_error_code_result(err_msg: &str, expected_code: &str) -> Result<(), String> {
    let actual_code = extract_sql_error_code(err_msg);
    if actual_code.as_deref() == Some(expected_code) {
        Ok(())
    } else {
        Err(format!(
            "expected SQL error code {:?}, got {:?}: {}",
            expected_code, actual_code, err_msg
        ))
    }
}

fn expected_sql_error_phase_result(
    err_msg: &str,
    expected_phase: SqlErrorPhase,
    sql_error_descriptors: &[SqlErrorDescriptor],
) -> Result<(), String> {
    let actual_code = extract_sql_error_code(err_msg);
    let actual_descriptor = actual_code
        .as_deref()
        .and_then(|code| lookup_sql_error_descriptor(sql_error_descriptors, code));
    if actual_descriptor.is_some_and(|descriptor| descriptor.phase == expected_phase) {
        Ok(())
    } else {
        Err(format!(
            "expected SQL error phase {}, got code {:?} with phase {:?}: {}",
            expected_phase.as_str(),
            actual_code,
            actual_descriptor.map(|descriptor| descriptor.phase),
            err_msg
        ))
    }
}

fn expected_error_location_result(
    err_msg: &str,
    expected_location: SqlErrorLocation,
) -> Result<(), String> {
    let actual_location = extract_sql_error_location(err_msg);
    if actual_location == Some(expected_location) {
        Ok(())
    } else {
        Err(format!(
            "expected error at {}:{}, got {:?}: {}",
            expected_location.line, expected_location.column, actual_location, err_msg
        ))
    }
}

fn expected_error_expectation_label(meta: &QueryMeta) -> String {
    let mut labels = Vec::new();
    if let Some(code) = meta.expect_error_code.as_deref() {
        labels.push(format!("engine_error_code={code}"));
    }
    if let Some(code) = meta.expect_sql_code.as_deref() {
        labels.push(format!("sql_error_code={code}"));
    }
    if let Some(phase) = meta.expect_sql_phase {
        labels.push(format!("sql_error_phase={}", phase.as_str()));
    }
    if let Some(location) = meta.expect_error_at {
        labels.push(format!("error_at={}:{}", location.line, location.column));
    }
    if let Some(expected) = meta.expect_error.as_deref()
        && meta.expect_error_code.is_none()
    {
        labels.push(format!("error_contains={expected:?}"));
    }
    labels.join(", ")
}

fn evaluate_expected_error_branch(
    meta: &QueryMeta,
    ok: bool,
    err_msg: &str,
) -> Option<Result<(), String>> {
    evaluate_expected_error_branch_with_sql_error_descriptors(
        meta,
        ok,
        err_msg,
        production_sql_error_descriptors(),
    )
}

fn evaluate_expected_error_branch_with_sql_error_descriptors(
    meta: &QueryMeta,
    ok: bool,
    err_msg: &str,
    sql_error_descriptors: &[SqlErrorDescriptor],
) -> Option<Result<(), String>> {
    if !meta.has_error_expectation() {
        return None;
    }

    if ok {
        if let Some(expected_code) = meta.expect_error_code.as_deref() {
            return Some(Err(format!(
                "expected engine error code {:?}, but query succeeded",
                expected_code
            )));
        }
        if let Some(expected_code) = meta.expect_sql_code.as_deref() {
            return Some(Err(format!(
                "expected SQL error code {:?}, but query succeeded",
                expected_code
            )));
        }
        if let Some(expected_phase) = meta.expect_sql_phase {
            return Some(Err(format!(
                "expected SQL error phase {}, but query succeeded",
                expected_phase.as_str()
            )));
        }
        if let Some(expected_location) = meta.expect_error_at {
            return Some(Err(format!(
                "expected error at {}:{}, but query succeeded",
                expected_location.line, expected_location.column
            )));
        }
        let expected_error = meta
            .expect_error
            .as_deref()
            .expect("error expectation checked");
        return Some(Err(format!(
            "expected error containing {:?}, but query succeeded",
            expected_error
        )));
    }

    if let Some(expected_code) = meta.expect_error_code.as_deref()
        && let Err(error) = expected_engine_error_code_result(err_msg, expected_code)
    {
        return Some(Err(error));
    }
    if let Some(expected_code) = meta.expect_sql_code.as_deref()
        && let Err(error) = expected_sql_error_code_result(err_msg, expected_code)
    {
        return Some(Err(error));
    }
    if let Some(expected_phase) = meta.expect_sql_phase
        && let Err(error) =
            expected_sql_error_phase_result(err_msg, expected_phase, sql_error_descriptors)
    {
        return Some(Err(error));
    }
    if let Some(expected_location) = meta.expect_error_at
        && let Err(error) = expected_error_location_result(err_msg, expected_location)
    {
        return Some(Err(error));
    }
    if meta.expect_error_code.is_some() {
        // Preserve the old precedence rule when both legacy directives exist.
        return Some(Ok(()));
    }
    let Some(expected_error) = meta.expect_error.as_deref() else {
        return Some(Ok(()));
    };
    Some(if error_message_matches(err_msg, expected_error) {
        Ok(())
    } else {
        Err(format!(
            "expected error containing {:?}, got: {}",
            expected_error, err_msg
        ))
    })
}

fn annotate_failure_with_engine_error_code(message: &str, source: &str) -> String {
    let mut labels = Vec::new();
    if !message.contains("engine_error_code=")
        && let Some(code) = extract_engine_error_code(source)
            .or_else(|| extract_engine_error_code(message))
            .or_else(|| {
                message
                    .strip_prefix("target execute failed: ")
                    .and_then(extract_engine_error_code)
            })
    {
        labels.push(format!("engine_error_code={code}"));
    }
    if !message.contains("sql_error_code=")
        && let Some(code) = extract_sql_error_code(source)
            .or_else(|| extract_sql_error_code(message))
            .or_else(|| {
                message
                    .strip_prefix("target execute failed: ")
                    .and_then(extract_sql_error_code)
            })
    {
        labels.push(format!("sql_error_code={code}"));
    }
    if labels.is_empty() {
        message.to_string()
    } else {
        format!("{} {}", labels.join(" "), message)
    }
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
    name = "novarocks-sql-test",
    about = "Run SQL correctness tests for suite directories under tests/sql/correctness/"
)]
pub(crate) struct Cli {
    /// Suite name(s), comma-separated. Use "all" for the default non-explicit suites.
    #[arg(long, required_unless_present_any = ["list_extensions", "list_suites", "list_declarations"])]
    suite: Option<String>,

    /// Print the deterministic manifest derived from @nova_extension annotations and exit.
    #[arg(long, action = ArgAction::SetTrue)]
    list_extensions: bool,

    /// Print the deterministic correctness suite names and exit.
    #[arg(long, action = ArgAction::SetTrue, conflicts_with = "list_extensions")]
    list_suites: bool,

    /// Print native impact and lifecycle declarations as JSON without starting resources.
    #[arg(long, conflicts_with_all = ["list_extensions", "list_suites"])]
    list_declarations: bool,

    /// Append structured native execution facts to a new JSON Lines file.
    #[arg(long)]
    results_file: Option<String>,

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

    /// Complete invocation budget, including fixture startup and cleanup reserve.
    #[arg(long, default_value_t = 3600)]
    fixture_budget_secs: u64,

    #[arg(long, action = ArgAction::SetTrue, conflicts_with = "no_verify")]
    verify: bool,

    #[arg(long = "no-verify", action = ArgAction::SetTrue, conflicts_with = "verify")]
    no_verify: bool,

    #[arg(long, action = ArgAction::SetTrue)]
    update_expected: bool,

    #[arg(long)]
    write_actual_dir: Option<String>,

    /// Directory for failure-only cross-process FE/BE log snapshots.
    ///
    /// Defaults to `logs/sql-test-failures`; the environment fallback is
    /// `NOVAROCKS_SQL_TEST_FAILURE_ARTIFACT_DIR`.
    #[arg(long)]
    failure_artifact_dir: Option<String>,

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

    /// Cross-process launch profile: fault-scenario or performance.
    /// Performance supports release binaries and refuses fault directives.
    #[arg(long)]
    cluster_launch_profile: Option<novarocks_cluster_harness::LaunchProfile>,

    /// SQL statements applied, in order, to each target case session before its first step.
    #[arg(long = "target-session-sql", value_name = "SQL", action = ArgAction::Append)]
    target_session_sql: Vec<String>,

    /// Treat matching @explain_contains values as @explain_not_contains values.
    #[arg(
        long = "rewrite-explain-contains-as-not-contains",
        value_name = "SUBSTRING",
        action = ArgAction::Append
    )]
    rewrite_explain_contains_as_not_contains: Vec<String>,

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

// ---------------------------------------------------------------------------
// Parallel execution types
// ---------------------------------------------------------------------------

/// Shared, read-only context for running cases within a suite.
struct SuiteRunContext {
    fixture_budget: novarocks_cluster_harness::fixture_run::RunBudget,
    fixture_storage: Option<novarocks_cluster_harness::fixture_run::RunStorageScope>,
    fixture_writers_settled: Arc<AtomicBool>,
    namespace_catalogs: namespace_guard::CatalogFacts,
    suite_name: String,
    controls: Option<Arc<control_plan::SqlControlExecution>>,
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
    rewrite_explain_contains_as_not_contains: Vec<String>,
    marker_re: Regex,
    fail_fast: bool,
    server_handle: Arc<Mutex<Box<dyn ServerHandle>>>,
    failure_artifacts: Arc<FailureArtifactRecorder>,
    publication_catalog_control: Option<publication_catalog::FixtureControl>,
    publication_service_control: Option<publication_service::Control>,
    results: Option<Arc<results_record::SuiteRecorder>>,
}

impl SuiteRunContext {
    fn work_deadline(&self) -> Instant {
        self.controls
            .as_ref()
            .map_or(self.fixture_budget.work_deadline(), |c| {
                c.deadline.min(self.fixture_budget.work_deadline())
            })
    }
    fn cleanup_deadline(&self) -> Instant {
        self.fixture_budget.deadline()
    }
    fn namespace_guard(
        &self,
        conn: &ConnectionConfig,
        cleanup: bool,
    ) -> namespace_guard::NamespaceGuard {
        namespace_guard::NamespaceGuard::new(
            self.fixture_budget.identity(),
            self.fixture_storage.as_ref(),
            self.namespace_catalogs.clone(),
            conn,
            if cleanup {
                self.cleanup_deadline()
            } else {
                self.work_deadline()
            },
        )
    }
    fn execute_shell(
        &self,
        sql: &str,
        deadline: Option<Instant>,
        cleanup: bool,
    ) -> (bool, Option<QueryExecution>, String) {
        let limit = if cleanup {
            self.cleanup_deadline()
        } else {
            self.work_deadline()
        };
        match (
            self.fixture_budget.to_json(),
            self.fixture_storage
                .as_ref()
                .map(|scope| scope.to_json())
                .transpose(),
        ) {
            (Ok(context), Ok(storage)) => shell::execute_shell_step_observed(
                sql,
                Some(deadline.map_or(limit, |d| d.min(limit))),
                Some(&context),
                storage.as_deref(),
                cleanup,
                Some(&self.fixture_writers_settled),
            ),
            (Err(error), _) | (_, Err(error)) => (
                false,
                None,
                format!("SQL run context serialization failed: {error:#}"),
            ),
        }
    }
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

/// A case file the runner could not turn into executable steps.  It is carried
/// alongside the loadable cases so one unparseable file fails as itself instead
/// of aborting the whole suite and hiding every other case.
struct CaseLoadFailure {
    case_id: String,
    source_file: PathBuf,
    error: String,
}

#[derive(Clone)]
struct CaseTiming {
    suite_name: String,
    case_id: String,
    status: CaseStatus,
    elapsed: Duration,
}

/// Once-resolved inputs shared by prelaunch authorization and actual execution.
struct PreparedSqlInputs {
    suite_name: String,
    sql_dir: PathBuf,
    result_dir: Option<PathBuf>,
    sql_glob: String,
    suite_catalog_override: Option<String>,
    target_db_default: String,
    ref_db_default: String,
    verify_enabled: bool,
    query_timeout: u64,
    cases: Vec<SqlCase>,
    load_failures: Vec<CaseLoadFailure>,
    suite_init_hook: Option<SuiteHook>,
    suite_cleanup_hook: Option<SuiteHook>,
}

/// Everything needed to run a suite: context + prepared cases + hooks.
struct PreparedSuite {
    ctx: SuiteRunContext,
    cases: Vec<SqlCase>,
    load_failures: Vec<CaseLoadFailure>,
    init_hook: Option<SuiteHook>,
    cleanup_hook: Option<SuiteHook>,
}

// ---------------------------------------------------------------------------
// wait_alter helper – polls SHOW ALTER TABLE until the latest job is terminal
// ---------------------------------------------------------------------------

#[derive(Debug, Eq, PartialEq)]
enum AlterJobPollState {
    Pending,
    Finished,
    Failed(String),
}

fn classify_alter_job_poll(execution: &crate::types::QueryExecution) -> AlterJobPollState {
    let Some(state_index) = execution
        .header
        .iter()
        .position(|column| column.eq_ignore_ascii_case("State"))
    else {
        return AlterJobPollState::Pending;
    };
    let message_index = execution
        .header
        .iter()
        .position(|column| column.eq_ignore_ascii_case("Msg"));
    let Some(row) = execution.rows.first() else {
        return AlterJobPollState::Pending;
    };
    match row.get(state_index).map(String::as_str) {
        Some("FINISHED") => AlterJobPollState::Finished,
        Some("FAILED") => AlterJobPollState::Failed(
            message_index
                .and_then(|index| row.get(index))
                .filter(|message| !message.is_empty())
                .cloned()
                .unwrap_or_else(|| "ALTER job failed without an error message".to_string()),
        ),
        _ => AlterJobPollState::Pending,
    }
}

struct WaitAlterRequest<'a> {
    db_name: &'a str,
    table_name: &'a str,
    kind: &'a str,
    max_retries: usize,
    interval: Duration,
}

fn execute_wait_alter(
    session: &mut MysqlSession,
    query_timeout: u64,
    request: WaitAlterRequest<'_>,
    log: &mut String,
) -> (bool, Duration) {
    let WaitAlterRequest {
        db_name,
        table_name,
        kind,
        max_retries,
        interval,
    } = request;
    // Test cases may scope @db as `catalog.db` to switch catalog before running
    // the wrapped statement. SHOW ALTER TABLE OPTIMIZE FROM <name> expects the
    // engine to receive each identifier part quoted independently — wrapping
    // `catalog.db` in a single backtick group flattens it into one db name and
    // makes the lookup miss.
    let show_sql = if db_name.is_empty() {
        format!(
            "SHOW ALTER TABLE {} WHERE TableName = '{}' ORDER BY CreateTime DESC LIMIT 1",
            kind, table_name,
        )
    } else {
        let qualified_from = match db_name.split_once('.') {
            Some((catalog, db)) => format!("`{}`.`{}`", catalog, db),
            None => format!("`{}`", db_name),
        };
        format!(
            "SHOW ALTER TABLE {} FROM {} WHERE TableName = '{}' ORDER BY CreateTime DESC LIMIT 1",
            kind, qualified_from, table_name,
        )
    };
    let mut total_elapsed = Duration::ZERO;
    for attempt in 0..max_retries {
        let (ok, execution, _err) = session.execute_query(query_timeout, &show_sql, None);
        if let Some(ref exec) = execution {
            total_elapsed += exec.elapsed;
        }
        if ok && let Some(exec) = &execution {
            match classify_alter_job_poll(exec) {
                AlterJobPollState::Finished => {
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
                AlterJobPollState::Failed(message) => {
                    let _ = writeln!(
                        log,
                        "    ❌ wait_alter_{} on `{}` failed (attempt {}/{}): {}",
                        kind.to_lowercase(),
                        table_name,
                        attempt + 1,
                        max_retries,
                        message,
                    );
                    return (false, total_elapsed);
                }
                AlterJobPollState::Pending => {}
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
            sleep(session.original_deadline().map_or(interval, |deadline| {
                interval.min(deadline.saturating_duration_since(Instant::now()))
            }));
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
    log: &mut String,
) -> (bool, Duration) {
    let mut total = Duration::ZERO;
    let db = step.meta.db.as_deref().unwrap_or("");
    for (table, kind, default_retries) in [
        (&step.meta.wait_alter_column, "COLUMN", 60usize),
        (&step.meta.wait_alter_rollup, "ROLLUP", 120usize),
        (&step.meta.wait_alter_optimize, "OPTIMIZE", 300usize),
    ] {
        if let Some(table_name) = table {
            let (ok, elapsed) = execute_wait_alter(
                session,
                query_timeout,
                WaitAlterRequest {
                    db_name: db,
                    table_name,
                    kind,
                    max_retries: default_retries,
                    interval: Duration::from_secs(1),
                },
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

    // SelectText is the canonical persisted, re-parseable CREATE-MV query body.
    // The native parser re-admits it on every refresh, so it is safe to re-run.
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
/// is defined in increasing fidelity order (Baseline < Package < Provenance).
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
    rewrite_contains_as_not_contains: &[String],
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
        if rewrite_contains_as_not_contains
            .iter()
            .any(|value| value == needle)
        {
            if explain_text.contains(needle.as_str()) {
                return Err(format!(
                    "@explain_contains rewrite assertion failed.\\n  expected substring to be absent: {}\\n  EXPLAIN VERBOSE output:\\n{}",
                    needle, explain_text
                ));
            }
            continue;
        }
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

fn run_be_log_directives_for_successful_step(
    step: &SqlStep,
    server_handle: &Arc<Mutex<Box<dyn ServerHandle>>>,
    snapshot: &be_log_directive::BeLogSnapshot,
    log: &mut String,
) -> Result<(), String> {
    match server_handle.lock() {
        Ok(server_handle) => be_log_directive::run(step, server_handle.as_ref(), snapshot, log)
            .map_err(|error| format!("BE log directive failed: {error:#}")),
        Err(_) => Err("server handle mutex is poisoned".to_string()),
    }
}

struct LifecycleStatementObservationGuard {
    server_handle: Arc<Mutex<Box<dyn ServerHandle>>>,
    observation: novarocks_cluster_harness::QueryLifecycleStatementObservation,
    drained: bool,
}

impl LifecycleStatementObservationGuard {
    fn drain(&mut self) -> Result<()> {
        self.server_handle
            .lock()
            .map_err(|_| anyhow::anyhow!("server handle mutex is poisoned"))?
            .drain_query_lifecycle_statement_observation(&self.observation)?;
        self.drained = true;
        Ok(())
    }
}

impl Drop for LifecycleStatementObservationGuard {
    fn drop(&mut self) {
        if self.drained {
            return;
        }
        match self.server_handle.lock() {
            Ok(mut server) => {
                if let Err(error) =
                    server.drain_query_lifecycle_statement_observation(&self.observation)
                {
                    eprintln!("failed to drain statement lifecycle observation: {error:#}");
                }
            }
            Err(_) => eprintln!(
                "server handle mutex is poisoned while draining statement lifecycle observation"
            ),
        }
    }
}

fn await_lifecycle_statement_observation(
    guard: &LifecycleStatementObservationGuard,
    deadline: Instant,
) -> Result<novarocks_cluster_harness::QueryLifecycleStructuredSnapshot> {
    guard
        .server_handle
        .lock()
        .map_err(|_| anyhow::anyhow!("server handle mutex is poisoned"))?
        .await_query_lifecycle_statement_observation(&guard.observation, true, deadline)
}

fn runtime_filter_details(
    rollup: &novarocks_cluster_harness::RuntimeFilterTerminalRollup,
) -> Result<Vec<&novarocks_cluster_harness::RuntimeFilterParticipantTerminalDetails>> {
    let novarocks_cluster_harness::RuntimeFilterTerminalRollup::Available { participants, .. } =
        rollup
    else {
        bail!("runtime-filter terminal facts are unavailable: {rollup:?}");
    };
    participants
        .iter()
        .map(|participant| {
            match &participant.telemetry {
            novarocks_cluster_harness::RuntimeFilterParticipantTerminalTelemetryValue::Available(
                details,
            ) => Ok(details),
            novarocks_cluster_harness::RuntimeFilterParticipantTerminalTelemetryValue::Unavailable(
                unavailable,
            ) => bail!(
                "runtime-filter participant telemetry is unavailable for {:?}: {unavailable:?}",
                participant.participant
            ),
        }
        })
        .collect()
}

fn runtime_filter_total_value(
    totals: &novarocks_cluster_harness::RuntimeFilterTerminalTotals,
    metric: RuntimeFilterTotalMetric,
) -> u64 {
    match metric {
        RuntimeFilterTotalMetric::ChannelCount => totals.channels.count,
        RuntimeFilterTotalMetric::ChannelCompletedCount => totals.channels.completed_count,
        RuntimeFilterTotalMetric::ProducerStreamCount => totals.producer_streams.count,
        RuntimeFilterTotalMetric::ProducerAcceptedCount => totals.producer_streams.accepted_count,
        RuntimeFilterTotalMetric::TransportRouteCount => totals.transport_routes.count,
        RuntimeFilterTotalMetric::TransportSentCount => totals.transport_routes.sent_count,
        RuntimeFilterTotalMetric::TransportAckedCount => totals.transport_routes.acked_count,
        RuntimeFilterTotalMetric::ConsumerCount => totals.consumers.count,
        RuntimeFilterTotalMetric::ConsumerRowEvaluations => totals.consumers.row_evaluations,
        RuntimeFilterTotalMetric::ConsumerInputRows => totals.consumers.input_rows,
        RuntimeFilterTotalMetric::ConsumerOutputRows => totals.consumers.output_rows,
    }
}

fn verify_runtime_filter_structured_assertion(
    assertion: &QueryLifecycleStructuredAssertion,
    after: &novarocks_cluster_harness::QueryLifecycleStructuredSnapshot,
) -> Result<()> {
    let has_runtime_filter_assertion = assertion.runtime_filter_availability.is_some()
        || !assertion.runtime_filter_details.is_empty()
        || !assertion.runtime_filter_totals_at_least.is_empty();
    if !has_runtime_filter_assertion {
        return Ok(());
    }

    if let Some(expected_availability) = assertion.runtime_filter_availability.as_ref()
        && !matches!(
            after.runtime_filter,
            novarocks_cluster_harness::RuntimeFilterTerminalRollup::Available { .. }
        )
    {
        bail!(
            "runtime-filter availability mismatch: expected {}, actual {:?}, execution_id={:?}",
            expected_availability.as_str(),
            after.runtime_filter,
            after.execution_id
        );
    }

    let details = runtime_filter_details(&after.runtime_filter)?;
    for expected in &assertion.runtime_filter_details {
        let present = match expected {
            RuntimeFilterDetailExpectation::CompletedChannel => details.iter().any(|details| {
                details.channels.iter().any(|channel| {
                    channel.terminal_state
                        == novarocks_cluster_harness::RuntimeFilterChannelTerminalState::Completed
                })
            }),
            RuntimeFilterDetailExpectation::AcceptedProducer => details.iter().any(|details| {
                details
                    .producer_streams
                    .iter()
                    .any(|producer| producer.accepted_count > 0)
            }),
            RuntimeFilterDetailExpectation::SentAckedTransport => details.iter().any(|details| {
                details
                    .transport_routes
                    .iter()
                    .any(|transport| transport.sent_count > 0 && transport.acked_count > 0)
            }),
            RuntimeFilterDetailExpectation::DeliveredConsumer => details.iter().any(|details| {
                details
                    .consumers
                    .iter()
                    .any(|consumer| consumer.latest_delivered_logical_version.is_some())
            }),
            RuntimeFilterDetailExpectation::DeliveredAppliedConsumer => {
                details.iter().any(|details| {
                    details.consumers.iter().any(|consumer| {
                        consumer.latest_delivered_logical_version.is_some()
                            && consumer.latest_applied_logical_version.is_some()
                    })
                })
            }
        };
        if !present {
            bail!(
                "runtime-filter detail assertion failed: expected {} in execution_id={:?}, actual {:?}",
                expected.as_str(),
                after.execution_id,
                after.runtime_filter
            );
        }
    }

    if !assertion.runtime_filter_totals_at_least.is_empty() {
        let novarocks_cluster_harness::RuntimeFilterTerminalRollup::Available { totals, .. } =
            &after.runtime_filter
        else {
            bail!(
                "runtime-filter totals are unavailable: {:?}",
                after.runtime_filter
            );
        };
        let novarocks_cluster_harness::RuntimeFilterTerminalTotalsTelemetry::Available(totals) =
            totals
        else {
            bail!("runtime-filter totals are unavailable: {totals:?}");
        };
        for expected in &assertion.runtime_filter_totals_at_least {
            let actual = runtime_filter_total_value(totals, expected.metric);
            if actual < expected.value {
                bail!(
                    "runtime-filter total assertion failed: expected {} >= {}, actual {} (execution_id={:?})",
                    expected.metric.as_str(),
                    expected.value,
                    actual,
                    after.execution_id
                );
            }
        }
    }
    Ok(())
}

fn verify_lifecycle_structured_assertion(
    meta: &QueryMeta,
    after: &novarocks_cluster_harness::QueryLifecycleStructuredSnapshot,
) -> Result<()> {
    let Some(assertion) = meta.query_lifecycle_structured_assertion.as_ref() else {
        return Ok(());
    };
    verify_runtime_filter_structured_assertion(assertion, after)?;
    Ok(())
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
    execute_target_query_with_selected_control(
        meta,
        server_handle,
        session,
        query_timeout,
        sql,
        db,
        fault_deadline,
        None,
        &mut None,
    )
}

fn execute_target_query_with_selected_control(
    meta: &QueryMeta,
    server_handle: &Arc<Mutex<Box<dyn ServerHandle>>>,
    session: &mut MysqlSession,
    query_timeout: u64,
    sql: &str,
    db: Option<&str>,
    fault_deadline: Option<Instant>,
    selected: Option<(&control_plan::StepControl, usize, Instant)>,
    guard: &mut Option<LifecycleStatementObservationGuard>,
) -> (bool, Option<QueryExecution>, String) {
    #[cfg(unix)]
    let mut operation_owner = match fault_deadline
        .map(|deadline| {
            crate::socket_deadline::SocketDeadlineOwner::duplicate(
                &session.conn,
                session
                    .original_deadline()
                    .map_or(deadline, |original| deadline.min(original)),
            )
        })
        .transpose()
    {
        Ok(owner) => owner,
        Err(error) => {
            return (
                false,
                None,
                format!("FAIL (runner query socket deadline): {error:#}"),
            );
        }
    };
    let query_timeout = bounded_fault_query_timeout(query_timeout, fault_deadline);
    let mut result = fault_injection::execute_with_post_fragment_start_fault(
        meta,
        server_handle,
        Some(session.connection_id()),
        fault_deadline,
        || {
            let result = session.execute_query_with_before_submit(
                query_timeout,
                sql,
                db,
                |transient, statement_index, connection_id, submitted| {
                    let Some((control, outer, deadline)) = selected else {
                        return Ok(());
                    };
                    let target = control
                        .submissions
                        .iter()
                        .find(|target| {
                            target.outer_attempt == outer
                                && target.transient_attempt == transient
                                && target.statement_index == statement_index
                        })
                        .context("actual SQL submission has no frozen control target")?;
                    target.validate_sql(submitted)?;
                    if let Some(mut previous) = guard.take() {
                        previous.drain()?;
                    }
                    if control.needs_nomination {
                        let observation = server_handle
                            .lock()
                            .map_err(|_| anyhow::anyhow!("server handle mutex is poisoned"))?
                            .arm_selected_statement_observation(
                                &target.run_token,
                                connection_id,
                                submitted,
                                deadline,
                            )?;
                        *guard = Some(LifecycleStatementObservationGuard {
                            server_handle: Arc::clone(server_handle),
                            observation,
                            drained: false,
                        });
                    }
                    Ok(())
                },
            );
            if fault_injection::has_fault(meta) && !result.0 {
                eprintln!(
                    "target query before fault verification failed: {}",
                    result.2
                );
            }
            result
        },
    )
    .unwrap_or_else(|error| {
        (
            false,
            None,
            format!("FAIL (runner fault injection): {error:#}"),
        )
    });
    #[cfg(unix)]
    if let Some(owner) = &mut operation_owner
        && let Err(error) = owner.close_and_join()
    {
        result.0 = false;
        result.2 = format!("{}; query socket owner cleanup: {error:#}", result.2);
    }
    if meta.kill_fe_after_mv_known_committed_before_projector_cas
        || meta.kill_fe_after_be_log_contains.is_some()
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

struct InflightPublicationFrontendKill<'a> {
    namespace_guard: namespace_guard::NamespaceGuard,
    meta: &'a QueryMeta,
    server_handle: &'a Arc<Mutex<Box<dyn ServerHandle>>>,
    session_config: ConnectionConfig,
    original_deadline: Option<Instant>,
    session_to_reconnect: &'a mut MysqlSession,
    query_timeout: u64,
    sql: String,
    db: Option<String>,
    fault_guard: &'a mut publication_catalog::FixtureFaultGuard,
    fault_deadline: Option<Instant>,
    log: &'a mut String,
}

struct InflightPublicationConcurrentShell<'a> {
    namespace_guard: namespace_guard::NamespaceGuard,
    run_context: String,
    writer_exit: Arc<AtomicBool>,
    meta: &'a QueryMeta,
    server_handle: &'a Arc<Mutex<Box<dyn ServerHandle>>>,
    session_config: ConnectionConfig,
    original_deadline: Option<Instant>,
    query_timeout: u64,
    sql: String,
    db: Option<String>,
    fault_guard: &'a mut publication_catalog::FixtureFaultGuard,
    fault_deadline: Option<Instant>,
    shell: &'a str,
    log: &'a mut String,
}

struct InflightPublicationServiceShell<'a> {
    namespace_guard: namespace_guard::NamespaceGuard,
    run_context: String,
    writer_exit: Arc<AtomicBool>,
    meta: &'a QueryMeta,
    server_handle: &'a Arc<Mutex<Box<dyn ServerHandle>>>,
    session_config: ConnectionConfig,
    original_deadline: Option<Instant>,
    complete_deadline: Instant,
    query_timeout: u64,
    sql: String,
    db: Option<String>,
    hold: &'a mut publication_service::Hold,
    shell: &'a str,
    log: &'a mut String,
}

fn execute_target_query_with_inflight_publication_service_shell(
    request: InflightPublicationServiceShell<'_>,
) -> (bool, Option<QueryExecution>, String) {
    let InflightPublicationServiceShell {
        namespace_guard,
        run_context,
        writer_exit,
        meta,
        server_handle,
        session_config,
        original_deadline,
        complete_deadline: _,
        query_timeout,
        sql,
        db,
        hold,
        shell,
        log,
    } = request;
    let action_deadline = Instant::now() + Duration::from_secs(query_timeout.saturating_add(10));
    let deadline =
        original_deadline.map_or(action_deadline, |original| original.min(action_deadline));
    let thread_meta = meta.clone();
    let thread_server = Arc::clone(server_handle);
    let query_thread = std::thread::spawn(move || {
        match MysqlSession::new_with_deadline(&session_config, Some(deadline)) {
            Ok(mut session) => {
                session.set_namespace_guard(namespace_guard);
                execute_target_query_with_fault(
                    &thread_meta,
                    &thread_server,
                    &mut session,
                    query_timeout,
                    &sql,
                    db.as_deref(),
                    Some(deadline),
                )
            }
            Err(error) => (
                false,
                None,
                format!("FAIL (runner service hold): open query session: {error:#}"),
            ),
        }
    });
    if let Err(error) = hold.wait_until_held(deadline) {
        let _ = hold.release();
        let primary = match query_thread.join() {
            Ok((_, _, message)) => message,
            Err(_) => "query thread panicked".to_string(),
        };
        return (
            false,
            None,
            format!("FAIL (runner service hold): {error:#}; primary query: {primary}"),
        );
    }
    let _ = writeln!(
        log,
        "    @publication_service_hold post-requirements boundary reached"
    );
    let storage = std::env::var(novarocks_cluster_harness::fixture_run::RUN_STORAGE_ENV).ok();
    let (companion_ok, _, companion_error) = shell::execute_shell_step_observed(
        &format!("shell: {shell}"),
        Some(deadline),
        Some(&run_context),
        storage.as_deref(),
        false,
        Some(&writer_exit),
    );
    if !companion_ok {
        let _ = hold.release();
        let _ = query_thread.join();
        return (
            false,
            None,
            format!("FAIL (runner service hold companion): {companion_error}"),
        );
    }
    let _ = writeln!(
        log,
        "    @publication_catalog_concurrent_shell completed before release"
    );
    if let Err(error) = hold.release() {
        let retry = hold.release();
        let actual_exit = query_thread.join().is_ok();
        return (
            false,
            None,
            format!(
                "FAIL (runner service hold release): {error:#}; release_retry={retry:?}; query_actor_joined={actual_exit}"
            ),
        );
    }
    let result = match query_thread.join() {
        Ok(result) => result,
        Err(_) => {
            return (
                false,
                None,
                "FAIL (runner service hold): query thread panicked".into(),
            );
        }
    };
    match hold.verify_original_commit_conflicted() {
        Ok(evidence) => {
            let _ = writeln!(
                log,
                "    @publication_service_hold original commit conflict PASS {evidence}"
            );
            result
        }
        Err(error) => (
            false,
            result.1,
            format!("FAIL (runner service hold trace): {error:#}"),
        ),
    }
}

fn execute_target_query_while_publication_service_holds_shell(
    request: InflightPublicationServiceShell<'_>,
) -> (bool, Option<QueryExecution>, String) {
    let InflightPublicationServiceShell {
        namespace_guard,
        run_context,
        writer_exit,
        meta,
        server_handle,
        session_config,
        original_deadline,
        complete_deadline,
        query_timeout,
        sql,
        db,
        hold,
        shell,
        log,
    } = request;
    let action_deadline = Instant::now() + Duration::from_secs(query_timeout.saturating_add(10));
    let deadline =
        original_deadline.map_or(action_deadline, |original| original.min(action_deadline));
    let shell = format!("shell: {shell}");
    let shell_thread = std::thread::spawn(move || {
        let storage = std::env::var(novarocks_cluster_harness::fixture_run::RUN_STORAGE_ENV).ok();
        shell::execute_shell_step_with_group_observation(
            &shell,
            deadline,
            complete_deadline,
            Some(&run_context),
            storage.as_deref(),
        )
    });
    if let Err(error) = hold.wait_until_held(deadline) {
        writer_exit.store(false, Ordering::Release);
        let _ = hold.release();
        let _ = shell_thread.join();
        return (
            false,
            None,
            format!("FAIL (runner service hold): external request did not reach hold: {error:#}"),
        );
    }
    let _ = writeln!(
        log,
        "    @publication_service_hold external request reached post-requirements boundary"
    );
    let query_result = match MysqlSession::new_with_deadline(&session_config, Some(deadline)) {
        Ok(mut session) => {
            session.set_namespace_guard(namespace_guard);
            execute_target_query_with_fault(
                meta,
                server_handle,
                &mut session,
                query_timeout,
                &sql,
                db.as_deref(),
                Some(deadline),
            )
        }
        Err(error) => (
            false,
            None,
            format!("FAIL (runner service hold): open query session: {error:#}"),
        ),
    };
    if let Err(error) = hold.release() {
        writer_exit.store(false, Ordering::Release);
        let retry = hold.release();
        let actual_exit = shell_thread.join().is_ok();
        return (
            false,
            None,
            format!(
                "FAIL (runner service hold release): {error:#}; release_retry={retry:?}; shell_actor_joined={actual_exit}"
            ),
        );
    }
    let shell_result = shell_thread.join();
    // This exact Hold owns the companion request. Always observe its delegate
    // outcome and actual method exit, even if the shell's 409 assertion failed.
    let remote_exit = hold.wait_for_original_exit(deadline);
    let ((shell_ok, _, shell_error), local_group_exit) = match shell_result {
        Ok(result) => result,
        Err(_) => {
            writer_exit.store(false, Ordering::Release);
            return (
                false,
                None,
                "FAIL (runner service hold): external request thread panicked".into(),
            );
        }
    };
    if !local_group_exit || remote_exit.is_err() {
        writer_exit.store(false, Ordering::Release);
    }
    let _ = writeln!(
        log,
        "    @publication_service_hold original local_group_exit={local_group_exit} remote_exit={remote_exit:?}"
    );
    if !shell_ok {
        return (
            false,
            None,
            format!(
                "FAIL (runner service hold companion): {shell_error}; primary query: {}",
                query_result.2
            ),
        );
    }
    if !local_group_exit || remote_exit.is_err() {
        return (
            false,
            None,
            format!(
                "FAIL (runner service hold actual exit): local_group_exit={local_group_exit}; remote_exit={remote_exit:?}"
            ),
        );
    }
    match hold.verify_original_commit_conflicted() {
        Ok(evidence) => {
            let _ = writeln!(
                log,
                "    @publication_service_hold frozen external conflict PASS {evidence}"
            );
            query_result
        }
        Err(error) => (
            false,
            None,
            format!("FAIL (runner service hold trace): {error:#}"),
        ),
    }
}

fn execute_target_query_with_inflight_publication_concurrent_shell(
    request: InflightPublicationConcurrentShell<'_>,
) -> (bool, Option<QueryExecution>, String) {
    let InflightPublicationConcurrentShell {
        namespace_guard,
        run_context,
        writer_exit,
        meta,
        server_handle,
        session_config,
        original_deadline,
        query_timeout,
        sql,
        db,
        fault_guard,
        fault_deadline,
        shell,
        log,
    } = request;
    let action_deadline = fault_deadline
        .unwrap_or_else(|| Instant::now() + Duration::from_secs(query_timeout.saturating_add(10)));
    let deadline =
        original_deadline.map_or(action_deadline, |original| original.min(action_deadline));
    let thread_meta = meta.clone();
    let thread_server = Arc::clone(server_handle);
    let query_timeout = bounded_fault_query_timeout(query_timeout, Some(deadline));
    let query_thread = std::thread::spawn(move || {
        match MysqlSession::new_with_deadline(&session_config, Some(deadline)) {
            Ok(mut session) => {
                session.set_namespace_guard(namespace_guard);
                execute_target_query_with_fault(
                    &thread_meta,
                    &thread_server,
                    &mut session,
                    query_timeout,
                    &sql,
                    db.as_deref(),
                    Some(deadline),
                )
            }
            Err(error) => (
                false,
                None,
                format!("FAIL (runner publication hold): open query session: {error:#}"),
            ),
        }
    });

    if let Err(error) = fault_guard.wait_until_entered(deadline) {
        let _ = fault_guard.release();
        let primary = match query_thread.join() {
            Ok((_, _, message)) => format!("; primary query: {message}"),
            Err(_) => "; primary query thread panicked".to_string(),
        };
        return (
            false,
            None,
            fault_timeout_diagnostics(
                server_handle,
                &format!("publication before-dispatch hold was not observed: {error:#}{primary}"),
            ),
        );
    }
    let _ = writeln!(
        log,
        "    @publication_catalog_fault before-dispatch hold reached"
    );

    let storage = std::env::var(novarocks_cluster_harness::fixture_run::RUN_STORAGE_ENV).ok();
    let (companion_ok, _, companion_error) = shell::execute_shell_step_observed(
        &format!("shell: {shell}"),
        Some(deadline),
        Some(&run_context),
        storage.as_deref(),
        false,
        Some(&writer_exit),
    );
    if !companion_ok {
        let _ = fault_guard.release();
        let _ = query_thread.join();
        return (
            false,
            None,
            format!("FAIL (runner concurrent publication shell): {companion_error}"),
        );
    }
    let _ = writeln!(
        log,
        "    @publication_catalog_concurrent_shell completed before release"
    );
    if let Err(error) = fault_guard.release() {
        // The query socket retains its original deadline. Keep ownership and
        // join its actual exit even if the control acknowledgement was lost.
        let retry = fault_guard.release();
        let actual_exit = query_thread.join().is_ok();
        return (
            false,
            None,
            format!(
                "FAIL (runner publication hold): release before-dispatch hold: {error:#}; release_retry={retry:?}; query_actor_joined={actual_exit}"
            ),
        );
    }
    match query_thread.join() {
        Ok(result) => result,
        Err(_) => (
            false,
            None,
            "FAIL (runner publication hold): query thread panicked".to_string(),
        ),
    }
}

fn execute_target_query_with_inflight_publication_frontend_kill(
    request: InflightPublicationFrontendKill<'_>,
) -> (bool, Option<QueryExecution>, String) {
    let InflightPublicationFrontendKill {
        namespace_guard,
        meta,
        server_handle,
        session_config,
        original_deadline,
        session_to_reconnect,
        query_timeout,
        sql,
        db,
        fault_guard,
        fault_deadline,
        log,
    } = request;
    let action_deadline = fault_deadline
        .unwrap_or_else(|| Instant::now() + Duration::from_secs(query_timeout.saturating_add(10)));
    let deadline =
        original_deadline.map_or(action_deadline, |original| original.min(action_deadline));
    let be_count = match server_handle.lock() {
        Ok(server) => server.be_count(),
        Err(_) => {
            return (
                false,
                None,
                "FAIL (runner publication hold): server handle mutex is poisoned".to_string(),
            );
        }
    };
    if be_count == 0 {
        return (
            false,
            None,
            "FAIL (runner publication hold): requires a runner-owned cross-process frontend"
                .to_string(),
        );
    }

    let thread_meta = meta.clone();
    let thread_server = Arc::clone(server_handle);
    let query_timeout = bounded_fault_query_timeout(query_timeout, Some(deadline));
    let query_thread = std::thread::spawn(move || {
        match MysqlSession::new_with_deadline(&session_config, Some(deadline)) {
            Ok(mut session) => {
                session.set_namespace_guard(namespace_guard);
                execute_target_query_with_fault(
                    &thread_meta,
                    &thread_server,
                    &mut session,
                    query_timeout,
                    &sql,
                    db.as_deref(),
                    Some(deadline),
                )
            }
            Err(error) => (
                false,
                None,
                format!("FAIL (runner publication hold): open query session: {error:#}"),
            ),
        }
    });

    if let Err(error) = fault_guard.wait_until_downstream_successful_hold(deadline) {
        let _ = fault_guard.release();
        let _ = query_thread.join();
        return (
            false,
            None,
            fault_timeout_diagnostics(
                server_handle,
                &format!("publication downstream-successful hold was not observed: {error:#}"),
            ),
        );
    }
    let _ = writeln!(
        log,
        "    @publication_catalog_fault downstream-successful hold reached"
    );

    let restart_result = server_handle
        .lock()
        .map_err(|_| anyhow::anyhow!("server handle mutex is poisoned"))
        .and_then(|mut server| {
            server
                .kill_fe()
                .context("kill frontend during publication response hold")?;
            server
                .restart_fe_until(deadline)
                .context("restart frontend after in-flight publication kill")
        });
    if let Err(error) = restart_result {
        let _ = fault_guard.release();
        let _ = query_thread.join();
        return (
            false,
            None,
            fault_timeout_diagnostics(
                server_handle,
                &format!("publication in-flight frontend kill failed: {error:#}"),
            ),
        );
    }
    let _ = writeln!(
        log,
        "    @publication_catalog_fault in-flight FE kill/restart PASS"
    );

    let (query_ok, query_execution, query_error) = match query_thread.join() {
        Ok(result) => result,
        Err(_) => {
            return (
                false,
                None,
                "FAIL (runner publication hold): query thread panicked".to_string(),
            );
        }
    };
    if query_ok || query_execution.is_some() {
        return (
            false,
            None,
            "FAIL (runner publication hold): statement unexpectedly survived frontend kill"
                .to_string(),
        );
    }
    if let Err(error) = session_to_reconnect.reconnect() {
        return (
            false,
            None,
            fault_timeout_diagnostics(
                server_handle,
                &format!("reconnect runner session after in-flight frontend kill: {error:#}"),
            ),
        );
    }
    let _ = writeln!(
        log,
        "    @publication_catalog_fault client disconnect observed and runner session reconnected"
    );
    (
        true,
        Some(QueryExecution {
            text_output: String::new(),
            header: Vec::new(),
            rows: Vec::new(),
            elapsed: Duration::ZERO,
        }),
        query_error,
    )
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
    snapshot: &be_log_directive::BeLogSnapshot,
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
        run_be_log_directives_for_successful_step(step, server_handle, snapshot, log)
    {
        let _ = writeln!(log, "    ❌ FAIL: {reason}");
        return false;
    }
    let _ = writeln!(
        log,
        "    ✅ PASS (expected error matched): {} {}",
        expected_error_expectation_label(&step.meta),
        last_failure
    );
    true
}

// Per-case execution
// ---------------------------------------------------------------------------

fn mismatch_artifact_root(ctx: &SuiteRunContext, log: &mut String) -> Option<PathBuf> {
    match ctx
        .actual_artifact_dir
        .clone()
        .map(Ok)
        .unwrap_or_else(|| ctx.failure_artifacts.case_artifact_root())
    {
        Ok(root) => Some(root),
        Err(error) => {
            let _ = writeln!(log, "    failed to allocate mismatch artifacts: {error:#}");
            None
        }
    }
}

fn preserve_case_failure_snapshot(
    ctx: &SuiteRunContext,
    case_id: &str,
    step_number: Option<usize>,
    log: &mut String,
) {
    let result = ctx.failure_artifacts.persist_case_failure(
        ctx.server_handle.as_ref(),
        &ctx.suite_name,
        case_id,
        step_number,
    );
    match result {
        Ok(Some(path)) => {
            let _ = writeln!(log, "    failure artifacts captured at {}", path.display());
        }
        Ok(None) => {}
        Err(error) => {
            let _ = writeln!(
                log,
                "    ⚠️ failed to preserve case failure artifacts: {error:#}"
            );
        }
    }
}

fn preserve_suite_failure_snapshot(ctx: &SuiteRunContext, phase: &str, stdout_lock: &Mutex<()>) {
    let result = ctx.failure_artifacts.persist_suite_failure(
        ctx.server_handle.as_ref(),
        &ctx.suite_name,
        phase,
    );
    let message = match result {
        Ok(Some(path)) => Some(format!(
            "[{}] failure artifacts captured at {}",
            ctx.suite_name,
            path.display()
        )),
        Ok(None) => None,
        Err(error) => Some(format!(
            "[{}] ⚠️ failed to preserve suite failure artifacts: {error:#}",
            ctx.suite_name
        )),
    };
    if let Some(message) = message {
        let _guard = stdout_lock.lock().unwrap();
        println!("{message}");
    }
}

fn arm_publication_catalog_fault(
    step: &SqlStep,
    ctx: &SuiteRunContext,
) -> Result<publication_catalog::FixtureFaultGuard> {
    let directive = step
        .meta
        .publication_catalog_fault
        .context("publication fault arm requires its original directive")?;
    let control = ctx
        .publication_catalog_control
        .as_ref()
        .context("publication fault arm requires its original fixture")?;
    match &step.meta.publication_catalog_fault_target {
        Some(target) => control.arm_next_for_table(
            directive.action.as_str(),
            directive.fault.as_str(),
            &target.namespace,
            &target.table,
        ),
        None => control.arm_next(directive.action.as_str(), directive.fault.as_str()),
    }
}

fn finish_publication_catalog_fault(
    guard: &mut Option<publication_catalog::FixtureFaultGuard>,
    query: usize,
    evidence: &mut results_record::CaseEvidence,
    log: &mut String,
) -> bool {
    let Some(guard) = guard.take() else {
        return true;
    };
    let trace_before_finish = guard.trace();
    match guard.finish() {
        Ok(evidence) => {
            let _ = writeln!(
                log,
                "    @publication_catalog_fault consumed and cleared trace={} details={}",
                evidence.summary(),
                evidence.trace()
            );
            true
        }
        Err(error) => {
            evidence.record_case_cleanup_failure(
                Some(query),
                format!("publication catalog fault cleanup: {error:#}"),
            );
            let _ = writeln!(
                log,
                "    ❌ publication catalog fault cleanup: {error:#}; trace_before_finish={trace_before_finish}"
            );
            false
        }
    }
}

/// Retire only this step's original frozen launch slots that this case did not issue.
/// This neither stops a role nor transfers an unused grant to another launch.
fn retire_unissued_case_role_launches(
    ctx: &SuiteRunContext,
    case: &SqlCase,
    query: Option<usize>,
    log: &mut String,
) -> Result<()> {
    let Some(controls) = &ctx.controls else {
        return Ok(());
    };
    for ((suite, case_id, step), launches) in &controls.step_role_launches {
        if suite != &ctx.suite_name
            || case_id != &case.case_id
            || query.is_some_and(|query| query != *step)
        {
            continue;
        }
        for (role, reference) in launches {
            let mut server = ctx
                .server_handle
                .lock()
                .map_err(|_| anyhow::anyhow!("server handle mutex is poisoned"))?;
            if server.retire_unissued_role_launch_if_reserved(*role, reference)? {
                let _ = writeln!(
                    log,
                    "    retired unissued step {role:?} launch for step {step}: {}",
                    reference.session_id
                );
            }
        }
    }
    Ok(())
}

/// Complete one step's unused exact launches before another step can restart a role.
fn retire_unissued_step_role_launches(
    ctx: &SuiteRunContext,
    case: &SqlCase,
    query: usize,
    abort: &AtomicBool,
    evidence: &mut results_record::CaseEvidence,
    log: &mut String,
) -> bool {
    if let Err(error) = retire_unissued_case_role_launches(ctx, case, Some(query), log) {
        abort.store(true, Ordering::Relaxed);
        evidence.record_independent_failure(
            Some(query),
            format!("retire unissued role launch: {error:#}"),
        );
        let _ = writeln!(log, "    ❌ retire unissued role launch: {error:#}");
        return false;
    }
    true
}

fn run_case(ctx: &SuiteRunContext, case: &SqlCase, abort: &AtomicBool) -> CaseOutcome {
    let mut evidence = results_record::CaseEvidence::default();
    let mut outcome = run_case_observed(ctx, case, abort, &mut evidence);
    // Covers setup failures and early breaks, as well as the ordinary cleanup
    // path. Issued and already retired originals are observed, never retired again.
    if let Err(error) = retire_unissued_case_role_launches(ctx, case, None, &mut outcome.log) {
        outcome.status = CaseStatus::Fail;
        abort.store(true, Ordering::Relaxed);
        evidence
            .record_independent_failure(None, format!("retire unissued role launch: {error:#}"));
        let _ = writeln!(outcome.log, "    ❌ retire unissued role launch: {error:#}");
        preserve_case_failure_snapshot(ctx, &case.case_id, None, &mut outcome.log);
    }
    evidence.finish_case(outcome.status == CaseStatus::Fail);
    if let Some(recorder) = &ctx.results {
        recorder.case(case, &outcome, &evidence);
    }
    outcome
}

fn run_case_observed(
    ctx: &SuiteRunContext,
    case: &SqlCase,
    abort: &AtomicBool,
    evidence: &mut results_record::CaseEvidence,
) -> CaseOutcome {
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
            preserve_case_failure_snapshot(ctx, &case.case_id, None, &mut log);
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
                preserve_case_failure_snapshot(ctx, &case.case_id, None, &mut log);
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
                preserve_case_failure_snapshot(ctx, &case.case_id, None, &mut log);
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
                Some(mut results) => {
                    for (query, result) in &mut results {
                        if let Some(step) =
                            case.steps.iter().find(|step| step.query_number == *query)
                            && let Err(error) = fixture_context::render_expected_result(
                                &step.sql,
                                result,
                                &case.expected_variables,
                                &case.namespace_rewrites,
                            )
                        {
                            let _ = writeln!(
                                log,
                                "    ❌ expected catalog metadata binding failed: {error:#}"
                            );
                            return CaseOutcome {
                                case_id: case.case_id.clone(),
                                status: CaseStatus::Fail,
                                elapsed: case_elapsed,
                                log,
                            };
                        }
                    }
                    Some(results)
                }
                None => {
                    let _ = writeln!(
                        log,
                        "    ❌ failed to load expected result: {}",
                        path.display()
                    );
                    if ctx.fail_fast {
                        abort.store(true, Ordering::Relaxed);
                    }
                    preserve_case_failure_snapshot(ctx, &case.case_id, None, &mut log);
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
        preserve_case_failure_snapshot(ctx, &case.case_id, None, &mut log);
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
        vec![case.auto_db.clone()]
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
    let drop_all_case_dbs = |ctx: &SuiteRunContext, dbs: &[String], log: &mut String| {
        for db in dbs {
            for (connection, role) in [
                (&target_case_db_admin_conn, "target"),
                (&reference_case_db_admin_conn, "reference"),
            ] {
                if role == "reference" && !ctx.reference_required {
                    continue;
                }
                if let Err(error) = session::drop_case_database_until(
                    connection,
                    ctx.query_timeout,
                    db,
                    role,
                    Some(ctx.cleanup_deadline()),
                ) {
                    let _ = writeln!(
                        log,
                        "    case cleanup failure {role} database={db}: {error:#}"
                    );
                }
            }
        }
    };

    for db_name in &case_dbs {
        if let Err(exc) = session::reset_case_database_scoped_until(
            &target_case_db_admin_conn,
            ctx.query_timeout,
            db_name,
            "target",
            Some(ctx.work_deadline()),
            Some(ctx.namespace_guard(&target_case_db_admin_conn, false)),
        ) {
            preserve_case_failure_snapshot(ctx, &case.case_id, None, &mut log);
            drop_all_case_dbs(ctx, &case_dbs, &mut log);
            let _ = writeln!(
                log,
                "    ❌ failed to prepare target case database {}: {:#}",
                db_name, exc
            );
            if ctx.fail_fast {
                abort.store(true, Ordering::Relaxed);
            }
            preserve_case_failure_snapshot(ctx, &case.case_id, None, &mut log);
            return CaseOutcome {
                case_id: case.case_id.clone(),
                status: CaseStatus::Fail,
                elapsed: case_elapsed,
                log,
            };
        }
        if ctx.reference_required
            && let Err(exc) = session::reset_case_database_scoped_until(
                &reference_case_db_admin_conn,
                ctx.query_timeout,
                db_name,
                "reference",
                Some(ctx.work_deadline()),
                Some(ctx.namespace_guard(&reference_case_db_admin_conn, false)),
            )
        {
            preserve_case_failure_snapshot(ctx, &case.case_id, None, &mut log);
            drop_all_case_dbs(ctx, &case_dbs, &mut log);
            let _ = writeln!(
                log,
                "    ❌ failed to prepare reference case database {}: {:#}",
                db_name, exc
            );
            if ctx.fail_fast {
                abort.store(true, Ordering::Relaxed);
            }
            preserve_case_failure_snapshot(ctx, &case.case_id, None, &mut log);
            return CaseOutcome {
                case_id: case.case_id.clone(),
                status: CaseStatus::Fail,
                elapsed: case_elapsed,
                log,
            };
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

    let mut target_session =
        match MysqlSession::new_with_deadline(&case_target_conn, Some(ctx.work_deadline())) {
            Ok(s) => s,
            Err(exc) => {
                preserve_case_failure_snapshot(ctx, &case.case_id, None, &mut log);
                drop_all_case_dbs(ctx, &case_dbs, &mut log);
                let _ = writeln!(
                    log,
                    "    ❌ failed to create target mysql session: {:#}",
                    exc
                );
                if ctx.fail_fast {
                    abort.store(true, Ordering::Relaxed);
                }
                preserve_case_failure_snapshot(ctx, &case.case_id, None, &mut log);
                return CaseOutcome {
                    case_id: case.case_id.clone(),
                    status: CaseStatus::Fail,
                    elapsed: case_elapsed,
                    log,
                };
            }
        };

    target_session.set_namespace_guard(ctx.namespace_guard(&case_target_conn, false));
    let mut reference_session = if ctx.reference_required {
        match MysqlSession::new_with_deadline(&case_reference_conn, Some(ctx.work_deadline())) {
            Ok(s) => Some(s),
            Err(exc) => {
                preserve_case_failure_snapshot(ctx, &case.case_id, None, &mut log);
                drop_all_case_dbs(ctx, &case_dbs, &mut log);
                let _ = writeln!(
                    log,
                    "    ❌ failed to create reference mysql session: {:#}",
                    exc
                );
                if ctx.fail_fast {
                    abort.store(true, Ordering::Relaxed);
                }
                preserve_case_failure_snapshot(ctx, &case.case_id, None, &mut log);
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

    if let Some(reference) = &mut reference_session {
        reference.set_namespace_guard(ctx.namespace_guard(&case_reference_conn, false));
    }
    if let Err(error) = execute_target_session_sql_with(
        &mut target_session,
        &ctx.target_session_sql,
        |session, statement| {
            let (ok, _, message) = session.execute_query(ctx.query_timeout, statement, None);
            if ok { Ok(()) } else { Err(message) }
        },
    ) {
        preserve_case_failure_snapshot(ctx, &case.case_id, None, &mut log);
        drop_all_case_dbs(ctx, &case_dbs, &mut log);
        let _ = writeln!(log, "    ❌ {error}");
        if ctx.fail_fast {
            abort.store(true, Ordering::Relaxed);
        }
        preserve_case_failure_snapshot(ctx, &case.case_id, None, &mut log);
        return CaseOutcome {
            case_id: case.case_id.clone(),
            status: CaseStatus::Fail,
            elapsed: case_elapsed,
            log,
        };
    }

    // --- step execution loop ---
    let mut recorded_results: BTreeMap<usize, ResultSet> = BTreeMap::new();
    // Set once a step fails. The loop then runs only the steps the case marked
    // as its teardown: what a half-finished case leaves behind is adopted by
    // the next case that attaches a catalog onto the same warehouse, so a case
    // that skipped its own cleanup fails the ones after it.
    let mut cleanup_only = false;
    let fault_cleanup_errors = fault_injection::FaultStepCleanupErrors::default();

    let mut cleanup_transport = false;
    for step in &case.steps {
        if cleanup_only && !step.meta.cleanup {
            if let Err(error) =
                retire_unissued_case_role_launches(ctx, case, Some(step.query_number), &mut log)
            {
                case_failed = true;
                abort.store(true, Ordering::Relaxed);
                evidence.record_independent_failure(
                    Some(step.query_number),
                    format!("retire skipped role launch: {error:#}"),
                );
                let _ = writeln!(log, "    ❌ retire skipped role launch: {error:#}");
                break;
            }
            continue;
        }
        let step_deadline = if step.meta.cleanup {
            ctx.cleanup_deadline()
        } else {
            ctx.work_deadline()
        };
        if step.meta.cleanup && !cleanup_transport {
            cleanup_transport = true;
            for (role, result) in std::iter::once((
                "target",
                target_session.begin_cleanup(ctx.cleanup_deadline()),
            ))
            .chain(
                reference_session
                    .as_mut()
                    .map(|session| ("reference", session.begin_cleanup(ctx.cleanup_deadline()))),
            ) {
                if let Err(error) = result {
                    case_failed = true;
                    let _ = writeln!(
                        log,
                        "    prior {role} socket settlement before cleanup: {error:#}"
                    );
                }
            }
        }
        if !step.meta.cleanup && Instant::now() >= step_deadline {
            case_failed = true;
            cleanup_only = true;
            let _ = writeln!(
                log,
                "    original fixture work deadline expired before step {}",
                step.query_number
            );
            if let Err(error) =
                retire_unissued_case_role_launches(ctx, case, Some(step.query_number), &mut log)
            {
                abort.store(true, Ordering::Relaxed);
                evidence.record_independent_failure(
                    Some(step.query_number),
                    format!("retire skipped role launch: {error:#}"),
                );
                let _ = writeln!(log, "    ❌ retire skipped role launch: {error:#}");
                break;
            }
            continue;
        }
        evidence.begin_step(step.query_number, case_failed);
        if step.meta.cleanup {
            evidence.mark_cleanup_step();
        }
        let order_sensitive = query_order_sensitive(step, ctx.order_sensitive_default);
        let epsilon = query_float_epsilon(step, ctx.float_epsilon);

        let _ = writeln!(
            log,
            "  step {} (order_sensitive={}, epsilon={:?})",
            step.query_number, order_sensitive, epsilon
        );

        let be_log_deadline =
            be_log_directive::step_evidence_deadline(&step.meta).map(|end| end.min(step_deadline));
        let resource_baseline = if fault_injection::has_fault(&step.meta) {
            match ctx.server_handle.lock() {
                Ok(mut server_handle)
                    if server_handle.supports_query_execution_resource_oracle() =>
                {
                    match server_handle.query_execution_resource_snapshot() {
                        Ok(Some(snapshot)) => Some(snapshot),
                        Ok(None) => {
                            case_failed = true;
                            evidence.record_independent_failure(
                                Some(step.query_number),
                                "query-execution resource oracle was advertised but returned no baseline".into(),
                            );
                            let _ = writeln!(
                                log,
                                "    ❌ query-execution resource oracle was advertised but returned no baseline"
                            );
                            break;
                        }
                        Err(error) => {
                            case_failed = true;
                            evidence.record_independent_failure(
                                Some(step.query_number),
                                format!("failed to capture pre-fault query-execution resource baseline: {error:#}"),
                            );
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
                    evidence.record_independent_failure(
                        Some(step.query_number),
                        "server handle mutex is poisoned before resource baseline capture".into(),
                    );
                    let _ = writeln!(log, "    ❌ server handle mutex is poisoned");
                    break;
                }
            }
        } else {
            None
        };
        let selected_control = ctx
            .controls
            .as_ref()
            .and_then(|controls| controls.step(&ctx.suite_name, &case.case_id, step.query_number));
        let mut structured_lifecycle_observation: Option<LifecycleStatementObservationGuard> = None;
        let query_lifecycle_fault_guard = fault_injection::query_lifecycle_fault_step_guard(
            &step.meta,
            Arc::clone(&ctx.server_handle),
            step.query_number,
            fault_cleanup_errors.clone(),
        );
        let cleanup_fault_guard = fault_injection::cleanup_fault_step_guard(
            &step.meta,
            Arc::clone(&ctx.server_handle),
            step.query_number,
            fault_cleanup_errors.clone(),
        );
        if fault_injection::has_fault(&step.meta) {
            let fault_result = ctx
                .server_handle
                .lock()
                .map_err(|_| anyhow::anyhow!("server handle mutex is poisoned"))
                .and_then(|mut server_handle| {
                    fault_injection::apply_pre_query_with_token(
                        &step.meta,
                        server_handle.as_mut(),
                        selected_control
                            .and_then(|control| control.submissions.first())
                            .map(|target| target.run_token.as_str()),
                        Some(step_deadline),
                    )
                });
            if let Err(exc) = fault_result {
                case_failed = true;
                evidence.record_independent_failure(
                    Some(step.query_number),
                    format!(
                        "failed to apply fault injection before step {}: {exc:#}",
                        step.query_number
                    ),
                );
                let _ = writeln!(
                    log,
                    "    ❌ failed to apply fault injection before step {}: {:#}",
                    step.query_number, exc
                );
                break;
            }
        }
        let orphan_fixture = match step.meta.iceberg_orphan_fixture.as_deref() {
            Some(table) => match iceberg_orphan_fixture::install(table) {
                Ok(fixture) => {
                    let _ = writeln!(
                        log,
                        "    @iceberg_orphan_fixture installed {}",
                        fixture.location()
                    );
                    Some(fixture)
                }
                Err(error) => {
                    case_failed = true;
                    evidence.record_independent_failure(
                        Some(step.query_number),
                        format!("install orphan fixture: {error:#}"),
                    );
                    let _ = writeln!(log, "    ❌ install orphan fixture: {error:#}");
                    break;
                }
            },
            None if step.meta.iceberg_orphan_fixture_absent => {
                case_failed = true;
                evidence.record_independent_failure(
                    Some(step.query_number),
                    "@iceberg_orphan_fixture_absent requires @iceberg_orphan_fixture".into(),
                );
                let _ = writeln!(
                    log,
                    "    ❌ @iceberg_orphan_fixture_absent requires @iceberg_orphan_fixture"
                );
                break;
            }
            None => None,
        };
        let mut publication_catalog_fault_guard = None;
        if let Some(directive) = step.meta.publication_catalog_fault
            && step.meta.publication_catalog_fault_timing
                != Some(PublicationCatalogFaultTiming::AfterFrontendExit)
        {
            match arm_publication_catalog_fault(step, ctx) {
                Ok(guard) => {
                    let _ = writeln!(
                        log,
                        "    @publication_catalog_fault armed action={} fault={}",
                        directive.action.as_str(),
                        directive.fault.as_str()
                    );
                    publication_catalog_fault_guard = Some(guard);
                }
                Err(error) => {
                    case_failed = true;
                    evidence.record_independent_failure(
                        Some(step.query_number),
                        format!("arm publication catalog fault: {error:#}"),
                    );
                    let _ = writeln!(log, "    ❌ arm publication catalog fault: {error:#}");
                    break;
                }
            }
        }
        let mut publication_service_hold = match step.meta.publication_service_hold.as_ref() {
            Some(directive) => match ctx.publication_service_control.as_ref() {
                Some(control) => match control.arm(&directive.table) {
                    Ok(hold) => Some(hold),
                    Err(error) => {
                        case_failed = true;
                        evidence.record_independent_failure(
                            Some(step.query_number),
                            format!("arm REST service hold: {error:#}"),
                        );
                        let _ = writeln!(log, "    ❌ arm REST service hold: {error:#}");
                        finish_publication_catalog_fault(
                            &mut publication_catalog_fault_guard,
                            step.query_number,
                            evidence,
                            &mut log,
                        );
                        break;
                    }
                },
                None => {
                    case_failed = true;
                    evidence.record_independent_failure(
                        Some(step.query_number),
                        "REST service hold requires the V11 fixture".into(),
                    );
                    let _ = writeln!(log, "    ❌ REST service hold requires the V11 fixture");
                    finish_publication_catalog_fault(
                        &mut publication_catalog_fault_guard,
                        step.query_number,
                        evidence,
                        &mut log,
                    );
                    break;
                }
            },
            None => None,
        };

        let be_log_snapshot = match ctx.server_handle.lock() {
            Ok(server_handle) => be_log_directive::snapshot_with_deadline(
                &step.meta,
                server_handle.as_ref(),
                be_log_deadline,
                Some(step_deadline),
            ),
            Err(_) => Err(anyhow::anyhow!("server handle mutex is poisoned")),
        };
        let be_log_snapshot = match be_log_snapshot {
            Ok(snapshot) => snapshot,
            Err(error) => {
                case_failed = true;
                evidence.record_independent_failure(
                    Some(step.query_number),
                    format!(
                        "failed to snapshot BE log evidence before step {}: {error:#}",
                        step.query_number
                    ),
                );
                let _ = writeln!(
                    log,
                    "    ❌ failed to snapshot BE log evidence before step {}: {error:#}",
                    step.query_number
                );
                finish_publication_catalog_fault(
                    &mut publication_catalog_fault_guard,
                    step.query_number,
                    evidence,
                    &mut log,
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
                    evidence.begin_attempt();
                    let (ok, execution, err_msg) = if shell::is_shell_step(&step.sql) {
                        ctx.execute_shell(&step.sql, Some(step_deadline), step.meta.cleanup)
                    } else if let Some(hold) = publication_service_hold.as_mut() {
                        let Some(concurrent_shell) =
                            step.meta.publication_catalog_concurrent_shell.as_deref()
                        else {
                            unreachable!("validated service hold requires a companion shell");
                        };
                        let request = InflightPublicationServiceShell {
                            writer_exit: Arc::clone(&ctx.fixture_writers_settled),
                            namespace_guard: ctx
                                .namespace_guard(&case_target_conn, step.meta.cleanup),
                            run_context: ctx
                                .fixture_budget
                                .to_json()
                                .expect("admitted context serialization"),
                            meta: &step.meta,
                            server_handle: &ctx.server_handle,
                            session_config: case_target_conn.clone(),
                            original_deadline: Some(step_deadline),
                            complete_deadline: ctx.cleanup_deadline(),
                            query_timeout: ctx.query_timeout,
                            sql: step.sql.clone(),
                            db: step.meta.db.clone(),
                            hold,
                            shell: concurrent_shell,
                            log: &mut log,
                        };
                        match step.meta.publication_service_hold.as_ref().unwrap().actor {
                            PublicationServiceActor::Sql => {
                                execute_target_query_with_inflight_publication_service_shell(
                                    request,
                                )
                            }
                            PublicationServiceActor::Shell => {
                                execute_target_query_while_publication_service_holds_shell(request)
                            }
                        }
                    } else if step
                        .meta
                        .publication_catalog_fault
                        .is_some_and(|directive| directive.fault.requires_concurrent_shell())
                    {
                        let Some(fault_guard) = publication_catalog_fault_guard.as_mut() else {
                            unreachable!("concurrent shell requires an armed fixture guard");
                        };
                        let Some(concurrent_shell) =
                            step.meta.publication_catalog_concurrent_shell.as_deref()
                        else {
                            unreachable!("validated concurrent shell directive is missing");
                        };
                        execute_target_query_with_inflight_publication_concurrent_shell(
                            InflightPublicationConcurrentShell {
                                writer_exit: Arc::clone(&ctx.fixture_writers_settled),
                                namespace_guard: ctx
                                    .namespace_guard(&case_target_conn, step.meta.cleanup),
                                run_context: ctx
                                    .fixture_budget
                                    .to_json()
                                    .expect("admitted context serialization"),
                                meta: &step.meta,
                                server_handle: &ctx.server_handle,
                                session_config: case_target_conn.clone(),
                                original_deadline: Some(step_deadline),
                                query_timeout: ctx.query_timeout,
                                sql: step.sql.clone(),
                                db: step.meta.db.clone(),
                                fault_guard,
                                fault_deadline: be_log_snapshot.evidence_deadline(),
                                shell: concurrent_shell,
                                log: &mut log,
                            },
                        )
                    } else if step
                        .meta
                        .publication_catalog_fault
                        .is_some_and(|directive| directive.fault.requires_inflight_frontend_kill())
                    {
                        let Some(fault_guard) = publication_catalog_fault_guard.as_mut() else {
                            unreachable!(
                                "in-flight publication kill requires an armed fixture guard"
                            );
                        };
                        execute_target_query_with_inflight_publication_frontend_kill(
                            InflightPublicationFrontendKill {
                                namespace_guard: ctx
                                    .namespace_guard(&case_target_conn, step.meta.cleanup),
                                meta: &step.meta,
                                server_handle: &ctx.server_handle,
                                session_config: case_target_conn.clone(),
                                original_deadline: Some(step_deadline),
                                session_to_reconnect: &mut target_session,
                                query_timeout: ctx.query_timeout,
                                sql: step.sql.clone(),
                                db: step.meta.db.clone(),
                                fault_guard,
                                fault_deadline: be_log_snapshot.evidence_deadline(),
                                log: &mut log,
                            },
                        )
                    } else {
                        execute_target_query_with_selected_control(
                            &step.meta,
                            &ctx.server_handle,
                            &mut target_session,
                            ctx.query_timeout,
                            &step.sql,
                            step.meta.db.as_deref(),
                            be_log_snapshot.evidence_deadline(),
                            selected_control.map(|control| {
                                (control, attempt, ctx.controls.as_ref().unwrap().deadline)
                            }),
                            &mut structured_lifecycle_observation,
                        )
                    };
                    let elapsed = execution
                        .as_ref()
                        .map(|result| result.elapsed)
                        .unwrap_or_default();
                    case_elapsed += elapsed;
                    last_execution = execution.clone();

                    if step.meta.publication_service_hold.is_some()
                        && err_msg.starts_with("FAIL (runner service hold")
                    {
                        evidence.record_independent_failure(
                            Some(step.query_number),
                            format!("runner service hold failed: {err_msg}"),
                        );
                        last_failure = err_msg;
                        break;
                    }

                    if let Some(expected_result) =
                        evaluate_expected_error_branch(&step.meta, ok, &err_msg)
                    {
                        evidence.record(
                            step,
                            "expected_error",
                            expected_result.is_ok(),
                            serde_json::json!({}),
                        );
                        if expected_result.is_err() {
                            evidence.record_failure(
                                serde_json::json!({"class":"error", "error_code":extract_engine_error_code(&err_msg).or_else(|| extract_sql_error_code(&err_msg)), "statement_index":step.query_number, "actual_digest":null}),
                            );
                        }
                        if let Err(reason) = expected_result {
                            last_failure =
                                annotate_failure_with_engine_error_code(&reason, &err_msg);
                        } else {
                            matched_expected_error = true;
                            last_failure = err_msg.clone();
                            break;
                        }
                    } else if !ok || execution.is_none() {
                        evidence.record_failure(
                            serde_json::json!({"class":"error_code", "error_code":extract_engine_error_code(&err_msg).or_else(|| extract_sql_error_code(&err_msg)), "statement_index":step.query_number, "actual_digest":null}),
                        );
                        last_failure = annotate_failure_with_engine_error_code(
                            &format!("target execute failed: {}", err_msg),
                            &err_msg,
                        );
                    } else {
                        let Some(execution) = execution else {
                            unreachable!("checked above");
                        };
                        let (assertions_ok, assertions_reason) =
                            verify_text_assertions(step, &execution);
                        if results_record::required(step).contains(&"text_assertions") {
                            evidence.record(
                                step,
                                "text_assertions",
                                assertions_ok,
                                serde_json::json!({}),
                            );
                        }
                        if !assertions_ok {
                            last_failure = format!("VERIFY FAILED: {}", assertions_reason);
                        } else if !ctx.verify_enabled
                            || step.meta.skip_result_check
                            || step_has_implicit_skip_result(step)
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
                            evidence.record(step, "result_set", same, serde_json::json!({"float_epsilon":epsilon,"order_sensitive":order_sensitive,"explain_rewritten":false}));
                            if !same {
                                let mut normalized = cmp_actual_rows.clone();
                                if !order_sensitive {
                                    normalized.sort();
                                }
                                evidence.record_failure(
                                    serde_json::json!({"class":"mismatch","error_code":null,"statement_index":step.query_number,"actual_digest":results_record::digest(&serde_json::to_vec(&normalized).expect("rows serialize"))}),
                                );
                            }
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
                            evidence.record_failure(serde_json::json!({
                                "class":"error", "error_code":null,
                                "statement_index":step.query_number, "actual_digest":null,
                                "error":last_failure
                            }));
                        } else {
                            last_failure = "missing result_dir in verify mode".to_string();
                            evidence.record_failure(serde_json::json!({
                                "class":"error", "error_code":null,
                                "statement_index":step.query_number, "actual_digest":null,
                                "error":last_failure
                            }));
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
                        sleep(
                            retry_interval.min(
                                ctx.work_deadline()
                                    .saturating_duration_since(Instant::now()),
                            ),
                        );
                    }
                }

                if step.meta.has_error_expectation() {
                    let expected_ok = finish_expected_error_step(
                        step,
                        matched_expected_error,
                        &last_failure,
                        &ctx.server_handle,
                        &be_log_snapshot,
                        &mut log,
                    );
                    evidence.record_expected_error_log(step, matched_expected_error, expected_ok);
                    if !expected_ok {
                        case_failed = true;
                    } else {
                        // A durable maintenance error can intentionally be
                        // followed by an FE restart.  In particular cleanup
                        // response-loss cases must prove startup reconciles
                        // the frozen prepared batch rather than merely
                        // returning the expected client error.
                        if let Err(error) = run_post_step_server_actions(
                            step,
                            ctx,
                            &mut target_session,
                            &mut publication_catalog_fault_guard,
                            &mut log,
                        ) {
                            case_failed = true;
                            evidence.record_independent_failure(
                                Some(step.query_number),
                                format!("post-step server action failed: {error:#}"),
                            );
                            let _ = writeln!(log, "    ❌ {error:#}");
                            finish_publication_catalog_fault(
                                &mut publication_catalog_fault_guard,
                                step.query_number,
                                evidence,
                                &mut log,
                            );
                            if !retire_unissued_step_role_launches(
                                ctx,
                                case,
                                step.query_number,
                                abort,
                                evidence,
                                &mut log,
                            ) {
                                break;
                            }
                            cleanup_only = true;
                            continue;
                        }
                        if step.meta.iceberg_orphan_fixture_absent {
                            let Some(fixture) = orphan_fixture.as_ref() else {
                                case_failed = true;
                                evidence.record_independent_failure(
                                    Some(step.query_number),
                                    "orphan fixture was not installed before the absence check"
                                        .into(),
                                );
                                let _ = writeln!(log, "    ❌ orphan fixture was not installed");
                                finish_publication_catalog_fault(
                                    &mut publication_catalog_fault_guard,
                                    step.query_number,
                                    evidence,
                                    &mut log,
                                );
                                if !retire_unissued_step_role_launches(
                                    ctx,
                                    case,
                                    step.query_number,
                                    abort,
                                    evidence,
                                    &mut log,
                                ) {
                                    break;
                                }
                                cleanup_only = true;
                                continue;
                            };
                            let absent_result = fixture.assert_absent();
                            evidence.record(
                                step,
                                "iceberg_orphan_absent",
                                absent_result.is_ok(),
                                serde_json::json!({}),
                            );
                            if let Err(error) = absent_result {
                                case_failed = true;
                                let _ = writeln!(log, "    ❌ {error:#}");
                                finish_publication_catalog_fault(
                                    &mut publication_catalog_fault_guard,
                                    step.query_number,
                                    evidence,
                                    &mut log,
                                );
                                if !retire_unissued_step_role_launches(
                                    ctx,
                                    case,
                                    step.query_number,
                                    abort,
                                    evidence,
                                    &mut log,
                                ) {
                                    break;
                                }
                                cleanup_only = true;
                                continue;
                            }
                            let _ = writeln!(log, "    @iceberg_orphan_fixture_absent PASS");
                        }
                        let be_log_result = run_be_log_directives_for_successful_step(
                            step,
                            &ctx.server_handle,
                            &be_log_snapshot,
                            &mut log,
                        );
                        evidence.record_expected_error_log(step, true, be_log_result.is_ok());
                        if let Err(reason) = be_log_result {
                            case_failed = true;
                            let _ = writeln!(log, "    ❌ FAIL: {reason}");
                        }
                    }
                } else if let Some(execution) = passed_execution {
                    if let Err(error) = run_post_step_server_actions(
                        step,
                        ctx,
                        &mut target_session,
                        &mut publication_catalog_fault_guard,
                        &mut log,
                    ) {
                        case_failed = true;
                        evidence.record_independent_failure(
                            Some(step.query_number),
                            format!("post-step server action failed: {error:#}"),
                        );
                        let _ = writeln!(log, "    ❌ {error:#}");
                        finish_publication_catalog_fault(
                            &mut publication_catalog_fault_guard,
                            step.query_number,
                            evidence,
                            &mut log,
                        );
                        if !retire_unissued_step_role_launches(
                            ctx,
                            case,
                            step.query_number,
                            abort,
                            evidence,
                            &mut log,
                        ) {
                            break;
                        }
                        cleanup_only = true;
                        continue;
                    }
                    if step.meta.iceberg_orphan_fixture_absent {
                        let Some(fixture) = orphan_fixture.as_ref() else {
                            case_failed = true;
                            evidence.record_independent_failure(
                                Some(step.query_number),
                                "orphan fixture was not installed before the absence check".into(),
                            );
                            let _ = writeln!(log, "    ❌ orphan fixture was not installed");
                            finish_publication_catalog_fault(
                                &mut publication_catalog_fault_guard,
                                step.query_number,
                                evidence,
                                &mut log,
                            );
                            if !retire_unissued_step_role_launches(
                                ctx,
                                case,
                                step.query_number,
                                abort,
                                evidence,
                                &mut log,
                            ) {
                                break;
                            }
                            cleanup_only = true;
                            continue;
                        };
                        let absent_result = fixture.assert_absent();
                        evidence.record(
                            step,
                            "iceberg_orphan_absent",
                            absent_result.is_ok(),
                            serde_json::json!({}),
                        );
                        if let Err(error) = absent_result {
                            case_failed = true;
                            let _ = writeln!(log, "    ❌ {error:#}");
                            finish_publication_catalog_fault(
                                &mut publication_catalog_fault_guard,
                                step.query_number,
                                evidence,
                                &mut log,
                            );
                            if !retire_unissued_step_role_launches(
                                ctx,
                                case,
                                step.query_number,
                                abort,
                                evidence,
                                &mut log,
                            ) {
                                break;
                            }
                            cleanup_only = true;
                            continue;
                        }
                        let _ = writeln!(log, "    @iceberg_orphan_fixture_absent PASS");
                    }
                    // Run wait_alter post-execution polling if annotated.
                    let (wait_ok, wait_elapsed) = run_step_wait_alters(
                        step,
                        &mut target_session,
                        ctx.query_timeout,
                        &mut log,
                    );
                    case_elapsed += wait_elapsed;
                    if results_record::required(step).contains(&"wait_alter") {
                        evidence.record(step, "wait_alter", wait_ok, serde_json::json!({}));
                    }
                    if !wait_ok {
                        case_failed = true;
                    } else {
                        let be_log_ok = run_be_log_directives_for_successful_step(
                            step,
                            &ctx.server_handle,
                            &be_log_snapshot,
                            &mut log,
                        );
                        if step.meta.has_be_log_directives() {
                            evidence.record(
                                step,
                                "be_log",
                                be_log_ok.is_ok(),
                                serde_json::json!({}),
                            );
                        }
                        let be_log_ok = match be_log_ok {
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
                            let oracle_result = run_imv_stateless_rebuild_check(
                                directive,
                                &mut target_session,
                                ctx.query_timeout,
                                step.meta.db.as_deref().or(primary_case_db),
                                epsilon,
                                &mut log,
                            );
                            evidence.record(
                                step,
                                "imv_stateless_rebuild",
                                oracle_result.is_ok(),
                                serde_json::json!({}),
                            );
                            if let Err(reason) = oracle_result {
                                let _ = writeln!(log, "    ❌ FAIL: {reason}");
                                case_failed = true;
                            }
                        }
                        if let Some(directive) = step.meta.mv_rest_document_graph.as_deref() {
                            let graph_result =
                                mv_rest_document_graph::assert_graph(&ctx.suite_name, directive);
                            if graph_result.is_err() {
                                evidence.record(
                                    step,
                                    "mv_rest_document_graph",
                                    false,
                                    serde_json::json!({}),
                                );
                            }
                            match graph_result {
                                Ok(summary) => {
                                    let _ =
                                        writeln!(log, "    @mv_rest_document_graph PASS {summary}");
                                    let mutations = (|| -> anyhow::Result<String> {
                                        let expectation =
                                            mv_rest_document_graph::parse_expectation(directive)?;
                                        let (namespace, table, publications) = (
                                            expectation.namespace,
                                            expectation.table,
                                            expectation.publications,
                                        );
                                        let control = ctx.publication_catalog_control.as_ref()
                                            .ok_or_else(|| anyhow::anyhow!(
                                                "MV REST mutation oracle requires the runner-owned publication catalog proxy"
                                            ))?;
                                        let counts = control.mutation_counts(namespace, table);
                                        let expected_commits = expectation
                                            .table_commits
                                            .unwrap_or(publications + expectation.slot_changes + 1);
                                        anyhow::ensure!(
                                            counts.stage_create_forwarded == 1
                                                && counts.stage_create_succeeded == 1
                                                && counts.table_commit_forwarded
                                                    == expected_commits
                                                        + expectation.failed_table_commits
                                                && counts.table_commit_succeeded
                                                    == expected_commits
                                                && counts.other_forwarded == 0
                                                && counts.other_succeeded == 0,
                                            "MV REST mutation count for {namespace}.{table}: observed {counts:?}, expected one successful stage-create, {expected_commits} successful table commits, and {} failed table commits",
                                            expectation.failed_table_commits
                                        );
                                        Ok(format!(
                                            "stage-create=1/1 table-commit={}/{} other=0/0",
                                            expected_commits + expectation.failed_table_commits,
                                            expected_commits
                                        ))
                                    })();
                                    evidence.record(
                                        step,
                                        "mv_rest_document_graph",
                                        mutations.is_ok(),
                                        serde_json::json!({}),
                                    );
                                    match mutations {
                                        Ok(summary) => {
                                            let _ = writeln!(
                                                log,
                                                "    @mv_rest_mutations PASS {summary}"
                                            );
                                        }
                                        Err(reason) => {
                                            let _ = writeln!(log, "    ❌ FAIL: {reason:#}");
                                            case_failed = true;
                                        }
                                    }
                                }
                                Err(reason) => {
                                    let _ = writeln!(log, "    ❌ FAIL: {reason:#}");
                                    case_failed = true;
                                }
                            }
                        }
                        // @imv_equivalence_check: assert MV incremental contents
                        // == a full recompute derived by running the MV's SelectText
                        // directly against the base tables (no MV side effects).
                        // Verify-mode only by design: diff mode compares against a
                        // reference engine (no full-recompute oracle here), and the
                        // check needs the MV's own SelectText which only verify drives.
                        if let Some(mv) = step.meta.imv_equivalence_check.as_deref() {
                            let oracle_result = run_imv_equivalence_check(
                                mv,
                                &mut target_session,
                                ctx.query_timeout,
                                step.meta.db.as_deref().or(primary_case_db),
                                epsilon,
                                &mut log,
                            );
                            evidence.record(
                                step,
                                "imv_equivalence",
                                oracle_result.is_ok(),
                                serde_json::json!({}),
                            );
                            if let Err(reason) = oracle_result {
                                let _ = writeln!(log, "    ❌ FAIL: {reason}");
                                case_failed = true;
                            }
                        }
                        // @explain_*: issue EXPLAIN VERBOSE and assert substrings.
                        let explain_requested = !step.meta.explain_contains.is_empty()
                            || !step.meta.explain_not_contains.is_empty();
                        let explain_ok = if explain_requested {
                            match run_explain_directive_checks(
                                step,
                                &mut target_session,
                                ctx.query_timeout,
                                &ctx.rewrite_explain_contains_as_not_contains,
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
                        for kind in ["explain_contains", "explain_not_contains"] {
                            if results_record::required(step).contains(&kind) {
                                evidence.record(step, kind, explain_ok, serde_json::json!({"explain_rewritten":!ctx.rewrite_explain_contains_as_not_contains.is_empty()}));
                            }
                        }
                        if explain_ok && be_log_ok {
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
                    if last_failure.contains("result missing")
                        && let Some(execution) = last_execution.as_ref()
                    {
                        let preview = execution.text_output.chars().take(500).collect::<String>();
                        let _ = writeln!(log, "    last result preview: {preview:?}");
                    }
                    if last_failure.starts_with("VERIFY FAILED: ")
                        && expected_results
                            .as_ref()
                            .and_then(|results| results.get(&step.query_number))
                            .is_none()
                        && let Some(execution) = last_execution.as_ref()
                    {
                        match ctx.failure_artifacts.persist_case_actual_result(
                            &ctx.suite_name,
                            &case.case_id,
                            step.query_number,
                            execution,
                        ) {
                            Ok(path) => {
                                let _ = writeln!(
                                    log,
                                    "    complete actual result retained at {}",
                                    path.display()
                                );
                                evidence.artifacts.push(path);
                            }
                            Err(error) => {
                                let _ = writeln!(
                                    log,
                                    "    ⚠️ failed to retain complete actual result: {error:#}"
                                );
                            }
                        }
                    }
                    if let (Some(expected), Some(execution)) = (
                        expected_results
                            .as_ref()
                            .and_then(|results| results.get(&step.query_number)),
                        last_execution.as_ref(),
                    ) && last_failure.starts_with("VERIFY FAILED: ")
                    {
                        let root = mismatch_artifact_root(ctx, &mut log);
                        let artifact_id = format!("{}-query{}", case.case_id, step.query_number);
                        let reason = last_failure
                            .trim_start_matches("VERIFY FAILED: ")
                            .to_string();
                        if let Some(root) = root.as_ref()
                            && let Err(exc) = write_mismatch_artifacts(MismatchArtifacts {
                                root_dir: root,
                                suite_name: &ctx.suite_name,
                                artifact_id: &artifact_id,
                                expected_header: &expected.header,
                                expected_rows: &expected.rows,
                                actual_header: &execution.header,
                                actual_rows: &execution.rows,
                                reason: &reason,
                            })
                        {
                            let _ =
                                writeln!(log, "    ⚠️ failed to write mismatch artifacts: {}", exc);
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
                    evidence.begin_attempt();
                    let (ok, execution, err_msg) = if ctx.record_from == RecordFrom::Target {
                        if shell::is_shell_step(&step.sql) {
                            ctx.execute_shell(&step.sql, Some(step_deadline), step.meta.cleanup)
                        } else {
                            execute_target_query_with_selected_control(
                                &step.meta,
                                &ctx.server_handle,
                                &mut target_session,
                                ctx.query_timeout,
                                &step.sql,
                                step.meta.db.as_deref(),
                                be_log_snapshot.evidence_deadline(),
                                selected_control.map(|control| {
                                    (control, attempt, ctx.controls.as_ref().unwrap().deadline)
                                }),
                                &mut structured_lifecycle_observation,
                            )
                        }
                    } else if shell::is_shell_step(&step.sql) {
                        ctx.execute_shell(&step.sql, Some(step_deadline), step.meta.cleanup)
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
                        evidence.record(
                            step,
                            "expected_error",
                            expected_result.is_ok(),
                            serde_json::json!({}),
                        );
                        if expected_result.is_err() {
                            evidence.record_failure(
                                serde_json::json!({"class":"error", "error_code":extract_engine_error_code(&err_msg).or_else(|| extract_sql_error_code(&err_msg)), "statement_index":step.query_number, "actual_digest":null}),
                            );
                        }
                        if let Err(reason) = expected_result {
                            last_failure = reason;
                        } else {
                            matched_expected_error = true;
                            last_failure = err_msg.clone();
                            break;
                        }
                    } else if !ok || execution.is_none() {
                        last_failure = format!("record source execute failed: {}", err_msg);
                        evidence.record_failure(serde_json::json!({
                            "class":"error", "error_code":extract_engine_error_code(&err_msg).or_else(|| extract_sql_error_code(&err_msg)),
                            "statement_index":step.query_number, "actual_digest":null,
                            "error":last_failure
                        }));
                    } else {
                        let Some(execution) = execution else {
                            unreachable!("checked above");
                        };
                        let (assertions_ok, assertions_reason) =
                            verify_text_assertions(step, &execution);
                        if results_record::required(step).contains(&"text_assertions") {
                            evidence.record(
                                step,
                                "text_assertions",
                                assertions_ok,
                                serde_json::json!({}),
                            );
                        }
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
                        sleep(
                            retry_interval.min(
                                ctx.work_deadline()
                                    .saturating_duration_since(Instant::now()),
                            ),
                        );
                    }
                }

                let expected_error_be_log_ok =
                    if matched_expected_error && ctx.record_from == RecordFrom::Target {
                        match run_be_log_directives_for_successful_step(
                            step,
                            &ctx.server_handle,
                            &be_log_snapshot,
                            &mut log,
                        ) {
                            Ok(()) => true,
                            Err(reason) => {
                                case_failed = true;
                                evidence.record_expected_error_log(step, true, false);
                                last_failure = reason;
                                false
                            }
                        }
                    } else {
                        true
                    };

                if step.meta.has_error_expectation() {
                    if matched_expected_error && expected_error_be_log_ok {
                        let _ = writeln!(
                            log,
                            "    ✅ RECORDED EXPECTED ERROR: {} {}",
                            expected_error_expectation_label(&step.meta),
                            last_failure
                        );
                    } else {
                        case_failed = true;
                        let _ = writeln!(log, "    ❌ {}", last_failure);
                    }
                } else if let Some(execution) = recorded_execution {
                    if ctx.record_from == RecordFrom::Target
                        && let Err(error) = run_post_step_server_actions(
                            step,
                            ctx,
                            &mut target_session,
                            &mut publication_catalog_fault_guard,
                            &mut log,
                        )
                    {
                        case_failed = true;
                        evidence.record_independent_failure(
                            Some(step.query_number),
                            format!("post-step server action failed: {error:#}"),
                        );
                        let _ = writeln!(log, "    ❌ {error:#}");
                        finish_publication_catalog_fault(
                            &mut publication_catalog_fault_guard,
                            step.query_number,
                            evidence,
                            &mut log,
                        );
                        if !retire_unissued_step_role_launches(
                            ctx,
                            case,
                            step.query_number,
                            abort,
                            evidence,
                            &mut log,
                        ) {
                            break;
                        }
                        cleanup_only = true;
                        continue;
                    }
                    if step.meta.iceberg_orphan_fixture_absent {
                        let Some(fixture) = orphan_fixture.as_ref() else {
                            case_failed = true;
                            evidence.record_independent_failure(
                                Some(step.query_number),
                                "orphan fixture was not installed before the absence check".into(),
                            );
                            let _ = writeln!(log, "    ❌ orphan fixture was not installed");
                            finish_publication_catalog_fault(
                                &mut publication_catalog_fault_guard,
                                step.query_number,
                                evidence,
                                &mut log,
                            );
                            if !retire_unissued_step_role_launches(
                                ctx,
                                case,
                                step.query_number,
                                abort,
                                evidence,
                                &mut log,
                            ) {
                                break;
                            }
                            cleanup_only = true;
                            continue;
                        };
                        let absent_result = fixture.assert_absent();
                        evidence.record(
                            step,
                            "iceberg_orphan_absent",
                            absent_result.is_ok(),
                            serde_json::json!({}),
                        );
                        if let Err(error) = absent_result {
                            case_failed = true;
                            let _ = writeln!(log, "    ❌ {error:#}");
                            finish_publication_catalog_fault(
                                &mut publication_catalog_fault_guard,
                                step.query_number,
                                evidence,
                                &mut log,
                            );
                            if !retire_unissued_step_role_launches(
                                ctx,
                                case,
                                step.query_number,
                                abort,
                                evidence,
                                &mut log,
                            ) {
                                break;
                            }
                            cleanup_only = true;
                            continue;
                        }
                        let _ = writeln!(log, "    @iceberg_orphan_fixture_absent PASS");
                    }
                    // Run wait_alter post-execution polling if annotated.
                    let (wait_ok, wait_elapsed) = run_step_wait_alters(
                        step,
                        &mut target_session,
                        ctx.query_timeout,
                        &mut log,
                    );
                    case_elapsed += wait_elapsed;
                    if !wait_ok {
                        case_failed = true;
                        evidence.record(step, "wait_alter", false, serde_json::json!({}));
                    } else {
                        let be_log_ok = if ctx.record_from == RecordFrom::Target {
                            match run_be_log_directives_for_successful_step(
                                step,
                                &ctx.server_handle,
                                &be_log_snapshot,
                                &mut log,
                            ) {
                                Ok(()) => true,
                                Err(reason) => {
                                    case_failed = true;
                                    evidence.record(step, "be_log", false, serde_json::json!({}));
                                    let _ = writeln!(log, "    ❌ FAIL: {reason}");
                                    false
                                }
                            }
                        } else {
                            true
                        };
                        // @explain_*: validate during record too.
                        let explain_ok = be_log_ok
                            && if !step.meta.explain_contains.is_empty()
                                || !step.meta.explain_not_contains.is_empty()
                            {
                                match run_explain_directive_checks(
                                    step,
                                    &mut target_session,
                                    ctx.query_timeout,
                                    &ctx.rewrite_explain_contains_as_not_contains,
                                    &mut log,
                                ) {
                                    Ok(()) => true,
                                    Err(msg) => {
                                        case_failed = true;
                                        for kind in ["explain_contains", "explain_not_contains"] {
                                            if results_record::required(step).contains(&kind) {
                                                evidence.record(step, kind, false, serde_json::json!({"explain_rewritten":!ctx.rewrite_explain_contains_as_not_contains.is_empty()}));
                                            }
                                        }
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
                        ctx.execute_shell(&step.sql, Some(step_deadline), step.meta.cleanup)
                    } else {
                        execute_target_query_with_selected_control(
                            &step.meta,
                            &ctx.server_handle,
                            &mut target_session,
                            ctx.query_timeout,
                            &step.sql,
                            step.meta.db.as_deref(),
                            be_log_snapshot.evidence_deadline(),
                            selected_control.map(|control| {
                                (control, 0, ctx.controls.as_ref().unwrap().deadline)
                            }),
                            &mut structured_lifecycle_observation,
                        )
                    };
                    let (ok_r, execution_r, err_r) = if shell::is_shell_step(&step.sql) {
                        ctx.execute_shell(&step.sql, Some(step_deadline), step.meta.cleanup)
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
                        evidence.record_failure(serde_json::json!({
                            "class":"error", "error_code":null,
                            "statement_index":step.query_number, "actual_digest":null,
                            "error":format!("diff expected engine error mismatch: {expected_code_result:?}")
                        }));
                        let _ = writeln!(
                            log,
                            "    ❌ DIFF FAILED {}",
                            expected_code_result.expect_err("checked mismatch above")
                        );
                    }
                } else if step.meta.has_sql_error_assertion() {
                    // SQL codes, phases, and source locations are NovaRocks
                    // conformance contracts. A reference engine cannot be
                    // required to emit NovaRocks-owned descriptors, so Diff
                    // validates this branch against the target only.
                    let (ok_t, execution_t, err_t) = if shell::is_shell_step(&step.sql) {
                        ctx.execute_shell(&step.sql, Some(step_deadline), step.meta.cleanup)
                    } else {
                        execute_target_query_with_selected_control(
                            &step.meta,
                            &ctx.server_handle,
                            &mut target_session,
                            ctx.query_timeout,
                            &step.sql,
                            step.meta.db.as_deref(),
                            be_log_snapshot.evidence_deadline(),
                            selected_control.map(|control| {
                                (control, 0, ctx.controls.as_ref().unwrap().deadline)
                            }),
                            &mut structured_lifecycle_observation,
                        )
                    };
                    case_elapsed += execution_t
                        .as_ref()
                        .map(|result| result.elapsed)
                        .unwrap_or_default();

                    match evaluate_expected_error_branch(&step.meta, ok_t, &err_t) {
                        Some(Ok(())) => {
                            let _ = writeln!(
                                log,
                                "    ✅ DIFF PASS (target SQL error contract matched: {})",
                                expected_error_expectation_label(&step.meta)
                            );
                        }
                        Some(Err(reason)) => {
                            case_failed = true;
                            evidence.record_failure(serde_json::json!({
                                "class":"error", "error_code":null,
                                "statement_index":step.query_number, "actual_digest":null,
                                "error":format!("diff target SQL error contract failed: {reason}")
                            }));
                            let _ = writeln!(log, "    ❌ DIFF FAILED {reason}");
                        }
                        None => unreachable!("checked has_sql_error_assertion above"),
                    }
                } else if let Some(expected_error) = step.meta.expect_error.as_deref() {
                    let (ok_t, execution_t, err_t) = if shell::is_shell_step(&step.sql) {
                        ctx.execute_shell(&step.sql, Some(step_deadline), step.meta.cleanup)
                    } else {
                        execute_target_query_with_selected_control(
                            &step.meta,
                            &ctx.server_handle,
                            &mut target_session,
                            ctx.query_timeout,
                            &step.sql,
                            step.meta.db.as_deref(),
                            be_log_snapshot.evidence_deadline(),
                            selected_control.map(|control| {
                                (control, 0, ctx.controls.as_ref().unwrap().deadline)
                            }),
                            &mut structured_lifecycle_observation,
                        )
                    };
                    let (ok_r, execution_r, err_r) = if shell::is_shell_step(&step.sql) {
                        ctx.execute_shell(&step.sql, Some(step_deadline), step.meta.cleanup)
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
                        evidence.record_failure(serde_json::json!({
                            "class":"error", "error_code":null,
                            "statement_index":step.query_number, "actual_digest":null,
                            "error":format!("diff expected error mismatch: target_ok={ok_t}, target_err={err_t}, reference_ok={ok_r}, reference_err={err_r}")
                        }));
                        let _ = writeln!(
                            log,
                            "    ❌ DIFF FAILED expected error {:?} (target_ok={}, target_err={}, reference_ok={}, reference_err={})",
                            expected_error, ok_t, err_t, ok_r, err_r
                        );
                    }
                } else {
                    let (ok_t, execution_t, err_t) = if shell::is_shell_step(&step.sql) {
                        ctx.execute_shell(&step.sql, Some(step_deadline), step.meta.cleanup)
                    } else {
                        execute_target_query_with_selected_control(
                            &step.meta,
                            &ctx.server_handle,
                            &mut target_session,
                            ctx.query_timeout,
                            &step.sql,
                            step.meta.db.as_deref(),
                            be_log_snapshot.evidence_deadline(),
                            selected_control.map(|control| {
                                (control, 0, ctx.controls.as_ref().unwrap().deadline)
                            }),
                            &mut structured_lifecycle_observation,
                        )
                    };
                    if !ok_t || execution_t.is_none() {
                        case_failed = true;
                        let failure = annotate_failure_with_engine_error_code(
                            &format!("target execute failed: {}", err_t),
                            &err_t,
                        );
                        evidence.record_failure(serde_json::json!({
                            "class":"error", "error_code":extract_engine_error_code(&err_t).or_else(|| extract_sql_error_code(&err_t)),
                            "statement_index":step.query_number, "actual_digest":null,
                            "error":failure
                        }));
                        let _ = writeln!(log, "    ❌ {}", failure);
                    } else {
                        let Some(execution_t) = execution_t else {
                            unreachable!("checked above");
                        };
                        let (ok_r, execution_r, err_r) = if shell::is_shell_step(&step.sql) {
                            ctx.execute_shell(&step.sql, Some(step_deadline), step.meta.cleanup)
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
                            evidence.record_failure(serde_json::json!({
                                "class":"error", "error_code":extract_engine_error_code(&err_r).or_else(|| extract_sql_error_code(&err_r)),
                                "statement_index":step.query_number, "actual_digest":null,
                                "error":format!("diff reference execute failed: {err_r}")
                            }));
                            case_elapsed += execution_t.elapsed;
                            let _ = writeln!(log, "    ❌ reference execute failed: {}", err_r);
                        } else {
                            let Some(execution_r) = execution_r else {
                                unreachable!("checked above");
                            };
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
                                evidence.record_failure(serde_json::json!({
                                    "class":"error", "error_code":null,
                                    "statement_index":step.query_number, "actual_digest":null,
                                    "error":format!("diff result comparison failed: {reason}")
                                }));
                                let _ = writeln!(log, "    ❌ DIFF FAILED: {}", reason);
                                if let Some(root) = mismatch_artifact_root(ctx, &mut log).as_ref() {
                                    let artifact_id =
                                        format!("{}-query{}", case.case_id, step.query_number);
                                    if let Err(exc) = write_mismatch_artifacts(MismatchArtifacts {
                                        root_dir: root,
                                        suite_name: &ctx.suite_name,
                                        artifact_id: &artifact_id,
                                        expected_header: &execution_r.header,
                                        expected_rows: &execution_r.rows,
                                        actual_header: &execution_t.header,
                                        actual_rows: &execution_t.rows,
                                        reason: &reason,
                                    }) {
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
            // Freeze the first failing statement's server logs before fault
            // guards, resource convergence, case cleanup, or later cases can
            // add unrelated lifecycle traffic.
            preserve_case_failure_snapshot(ctx, &case.case_id, Some(step.query_number), &mut log);
        }

        if !finish_publication_catalog_fault(
            &mut publication_catalog_fault_guard,
            step.query_number,
            evidence,
            &mut log,
        ) {
            case_failed = true;
        }

        for (action, result) in [
            ("query lifecycle", query_lifecycle_fault_guard.finish()),
            ("connector cleanup", cleanup_fault_guard.finish()),
        ] {
            if let Err(error) = result {
                case_failed = true;
                evidence.record_case_cleanup_failure(
                    Some(step.query_number),
                    format!("{action} fault cleanup: {error:#}"),
                );
                let _ = writeln!(log, "    ❌ {action} fault cleanup: {error:#}");
                preserve_case_failure_snapshot(
                    ctx,
                    &case.case_id,
                    Some(step.query_number),
                    &mut log,
                );
            }
        }

        if let Some(baseline) = resource_baseline {
            let convergence_deadline = ctx.controls.as_ref().map_or(
                Instant::now() + Duration::from_secs(30),
                |controls| {
                    controls
                        .deadline
                        .min(Instant::now() + Duration::from_secs(30))
                },
            );
            let resource_result = ctx
                .server_handle
                .lock()
                .map_err(|_| anyhow::anyhow!("server handle mutex is poisoned"))
                .and_then(|mut server_handle| {
                    server_handle
                        .await_query_execution_resource_convergence(&baseline, convergence_deadline)
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
                    evidence.record_independent_failure(
                        Some(step.query_number),
                        format!("query-execution resource convergence failed: {error:#}"),
                    );
                    let _ = writeln!(
                        log,
                        "    ❌ query-execution resource convergence failed: {error:#}"
                    );
                }
            }
        }

        if step.meta.query_lifecycle_structured_assertion.is_some() {
            let assertion_deadline =
                step_deadline.min(Instant::now() + Duration::from_secs(ctx.query_timeout));
            let assertion_result = structured_lifecycle_observation
                .as_ref()
                .context("exact statement lifecycle observation disappeared after the step")
                .and_then(|guard| await_lifecycle_statement_observation(guard, assertion_deadline))
                .and_then(|after| {
                    verify_lifecycle_structured_assertion(&step.meta, &after)?;
                    Ok(after.execution_id)
                });
            evidence.record(
                step,
                "runtime_lifecycle",
                assertion_result.is_ok(),
                serde_json::json!({}),
            );
            match assertion_result {
                Ok(execution_id) => {
                    let _ = writeln!(
                        log,
                        "    ✅ structured lifecycle assertion passed execution_id={execution_id:?}"
                    );
                }
                Err(error) => {
                    case_failed = true;
                    let _ = writeln!(
                        log,
                        "    ❌ structured lifecycle assertion failed: {error:#}"
                    );
                }
            }
            if let Some(guard) = structured_lifecycle_observation.as_mut()
                && let Err(error) = guard.drain()
            {
                case_failed = true;
                evidence.record_case_cleanup_failure(
                    Some(step.query_number),
                    format!("exact statement lifecycle observation cleanup failed: {error:#}"),
                );
                let _ = writeln!(
                    log,
                    "    ❌ exact statement lifecycle observation cleanup failed: {error:#}"
                );
            }
        }

        if !step.meta.cleanup && Instant::now() >= step_deadline {
            case_failed = true;
            evidence.record_failure(serde_json::json!({"class":"error", "error":"original SQL invocation work deadline expired", "statement_index":step.query_number}));
            let _ = writeln!(log, "    ❌ original SQL invocation work deadline expired");
        }
        // This step is over even if its selected mode never ran a restart.
        // Any remaining original slot belongs to this step, never its successor.
        if !retire_unissued_step_role_launches(
            ctx,
            case,
            step.query_number,
            abort,
            evidence,
            &mut log,
        ) {
            case_failed = true;
            break;
        }
        if case_failed && !cleanup_only {
            cleanup_only = true;
            let remaining_cleanup = case
                .steps
                .iter()
                .filter(|candidate| {
                    candidate.query_number > step.query_number && candidate.meta.cleanup
                })
                .count();
            if remaining_cleanup == 0 {
                let _ = writeln!(log, "    ⏭️ skipping remaining steps in {}", case.case_id);
                break;
            }
            let _ = writeln!(
                log,
                "    ⏭️ skipping remaining steps in {}, running {remaining_cleanup} cleanup step(s)",
                case.case_id
            );
        }
    }

    evidence.finish_execution(case_failed);

    if case_failed {
        // Pre-execution failures can break out before the mode dispatch above.
        // The recorder deduplicates this against an earlier statement capture.
        preserve_case_failure_snapshot(ctx, &case.case_id, None, &mut log);
    }

    for (query_number, error) in fault_cleanup_errors.take() {
        case_failed = true;
        evidence.record_case_cleanup_failure(
            Some(query_number),
            format!("step {query_number} early-exit fault cleanup: {error}"),
        );
        let _ = writeln!(
            log,
            "    ❌ step {query_number} early-exit fault cleanup: {error}"
        );
        preserve_case_failure_snapshot(ctx, &case.case_id, Some(query_number), &mut log);
    }

    // --- cleanup ---
    for (role, closed) in std::iter::once(("target", target_session.finish())).chain(
        reference_session
            .take()
            .map(|session| ("reference", session.finish())),
    ) {
        if let Err(error) = closed {
            case_failed = true;
            evidence.record_case_cleanup_failure(
                None,
                format!("{role} MySQL socket owner cleanup: {error:#}"),
            );
            let _ = writeln!(log, "    ❌ {role} MySQL socket owner cleanup: {error:#}");
        }
    }

    for db_name in &case_dbs {
        if let Err(exc) = session::drop_case_database_until(
            &target_case_db_admin_conn,
            ctx.query_timeout,
            db_name,
            "target",
            Some(ctx.cleanup_deadline()),
        ) {
            case_failed = true;
            evidence.record_case_cleanup_failure(
                None,
                format!("failed to cleanup target case database {db_name}: {exc:#}"),
            );
            let _ = writeln!(
                log,
                "    ❌ failed to cleanup target case database {}: {:#}",
                db_name, exc
            );
        }
        if ctx.reference_required
            && let Err(exc) = session::drop_case_database_until(
                &reference_case_db_admin_conn,
                ctx.query_timeout,
                db_name,
                "reference",
                Some(ctx.cleanup_deadline()),
            )
        {
            case_failed = true;
            evidence.record_case_cleanup_failure(
                None,
                format!("failed to cleanup reference case database {db_name}: {exc:#}"),
            );
            let _ = writeln!(
                log,
                "    ❌ failed to cleanup reference case database {}: {:#}",
                db_name, exc
            );
        }
    }

    if !case_failed
        && ctx.mode == Mode::Record
        && case_requires_result_file
        && let Some(path) = case_path.as_ref()
    {
        if let Err(exc) = write_result_file(path, &recorded_results, multi_step) {
            case_failed = true;
            let _ = writeln!(log, "    ❌ failed to write expected result: {}", exc);
        } else {
            let _ = writeln!(log, "    ✅ RECORDED CASE -> {}", path.display());
        }
    }

    if case_failed {
        // Cleanup and record-output failures originate after the earlier
        // capture points. Preserve them here only when this is the case's
        // first failure.
        preserve_case_failure_snapshot(ctx, &case.case_id, None, &mut log);
    }

    let status = if case_failed {
        CaseStatus::Fail
    } else {
        CaseStatus::Pass
    };

    if case_failed {
        match ctx.failure_artifacts.persist_case_diagnostic(
            &ctx.suite_name,
            &case.case_id,
            &case.source_file,
            case_path.as_deref(),
            &log,
        ) {
            Ok(path) => {
                evidence.artifacts.push(path.clone());
                let _ = writeln!(log, "    case diagnostics retained at {}", path.display());
            }
            Err(error) => {
                let _ = writeln!(log, "    failed to retain case diagnostics: {error:#}");
            }
        }
    }

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

/// Render every unparseable case file as a failing case of the suite, so a
/// metadata error is attributed to its own case instead of hiding the rest.
fn load_failure_outcomes(suite_name: &str, failures: &[CaseLoadFailure]) -> Vec<CaseOutcome> {
    failures
        .iter()
        .map(|failure| CaseOutcome {
            case_id: failure.case_id.clone(),
            status: CaseStatus::Fail,
            elapsed: Duration::ZERO,
            log: format!(
                "\n[{}] {} (unloadable)\n    ❌ failed to load {}: {}\n",
                suite_name,
                failure.case_id,
                failure.source_file.display(),
                failure.error,
            ),
        })
        .collect()
}

fn run_suite(ps: &PreparedSuite, abort: &AtomicBool, stdout_lock: &Mutex<()>) -> SuiteOutcome {
    run_suite_with_hooks(ps, abort, stdout_lock, |conn, timeout, hook, label| {
        let cleanup = ps
            .cleanup_hook
            .as_ref()
            .is_some_and(|cleanup| cleanup.path == hook.path);
        session::run_mysql_sql_scoped_until(
            conn,
            timeout,
            &hook.sql,
            Some(if cleanup {
                ps.ctx.cleanup_deadline()
            } else {
                ps.ctx.work_deadline()
            }),
            Some(ps.ctx.namespace_guard(conn, cleanup)),
        )
        .with_context(|| format!("{label} suite hook failed: {}", hook.path.display()))
        .map(|_| ())
    })
}

fn run_suite_with_hooks(
    ps: &PreparedSuite,
    abort: &AtomicBool,
    stdout_lock: &Mutex<()>,
    mut execute_hook: impl FnMut(&ConnectionConfig, u64, &SuiteHook, &str) -> Result<()>,
) -> SuiteOutcome {
    let mut init_errors = Vec::new();
    if let Some(recorder) = &ps.ctx.results {
        for failure in &ps.load_failures {
            recorder.load_failure(failure);
        }
    }
    let mut result =
        run_suite_with_hooks_observed(ps, abort, stdout_lock, |conn, timeout, hook, role| {
            let result = execute_hook(conn, timeout, hook, role);
            if ps
                .init_hook
                .as_ref()
                .is_some_and(|init| init.path == hook.path)
                && let Err(error) = &result
            {
                init_errors.push(format!("{error:#}"));
                if let Some(recorder) = &ps.ctx.results {
                    recorder.init_failed(&ps.cases);
                }
            }
            result
        });
    if !init_errors.is_empty() {
        // Init failure bypasses run_case entirely; none of these step
        // launches ran. Retire their exact originals before a later suite.
        for case in &ps.cases {
            let mut log = String::new();
            if let Err(error) = retire_unissued_case_role_launches(&ps.ctx, case, None, &mut log) {
                abort.store(true, Ordering::Relaxed);
                result.cleanup_errors.push(format!(
                    "retire unissued role launch after suite init failure: {error:#}"
                ));
                let _ = writeln!(
                    log,
                    "    ❌ retire unissued role launch after suite init failure: {error:#}"
                );
            }
            if let Some(outcome) = result
                .outcomes
                .iter_mut()
                .find(|outcome| outcome.case_id == case.case_id)
            {
                outcome.log.push_str(&log);
            }
        }
    }
    if let Some(recorder) = &ps.ctx.results {
        recorder.finish(ps, &result, abort.load(Ordering::Relaxed), &init_errors);
    }
    result
}

fn run_suite_with_hooks_observed(
    ps: &PreparedSuite,
    abort: &AtomicBool,
    stdout_lock: &Mutex<()>,
    mut execute_hook: impl FnMut(&ConnectionConfig, u64, &SuiteHook, &str) -> Result<()>,
) -> SuiteOutcome {
    let wall_start = Instant::now();
    let ctx = &ps.ctx;
    let total = ps.cases.len() + ps.load_failures.len();
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
        if let Err(exc) = execute_hook(&ctx.target_admin_conn, ctx.query_timeout, hook, "target") {
            ctx.failure_artifacts.record_provenance(
                format!("suite-init/{}", ctx.suite_name),
                serde_json::json!({"role": "target", "error": format!("{exc:#}")}),
            );
            preserve_suite_failure_snapshot(ctx, "init-target", stdout_lock);
            let mut cleanup_errors = Vec::new();
            if let Some(cleanup) = ps.cleanup_hook.as_ref()
                && let Err(error) = execute_hook(
                    &ctx.target_admin_conn,
                    ctx.query_timeout,
                    cleanup,
                    "target cleanup after init failure",
                )
            {
                cleanup_errors.push(format!(
                    "[{}] target cleanup after init failure: {error:#}",
                    ctx.suite_name
                ));
            }
            let _guard = stdout_lock.lock().unwrap();
            println!("[{}] ❌ suite init failed: {exc:#}", ctx.suite_name);
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
                .chain(load_failure_outcomes(&ctx.suite_name, &ps.load_failures))
                .collect();
            return SuiteOutcome {
                suite_name: ctx.suite_name.clone(),
                total,
                outcomes,
                cleanup_errors,
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
            if let Err(exc) = execute_hook(
                &ctx.reference_admin_conn,
                ctx.query_timeout,
                hook,
                "reference",
            ) {
                ctx.failure_artifacts.record_provenance(
                    format!("suite-init/{}", ctx.suite_name),
                    serde_json::json!({"role": "reference", "error": format!("{exc:#}")}),
                );
                preserve_suite_failure_snapshot(ctx, "init-reference", stdout_lock);
                let mut cleanup_errors = Vec::new();
                if let Some(cleanup) = ps.cleanup_hook.as_ref() {
                    for (connection, role) in [
                        (&ctx.reference_admin_conn, "reference"),
                        (&ctx.target_admin_conn, "target"),
                    ] {
                        if let Err(error) =
                            execute_hook(connection, ctx.query_timeout, cleanup, role)
                        {
                            cleanup_errors.push(format!(
                                "[{}] {role} cleanup after init failure: {error:#}",
                                ctx.suite_name
                            ));
                        }
                    }
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
                    .chain(load_failure_outcomes(&ctx.suite_name, &ps.load_failures))
                    .collect();
                return SuiteOutcome {
                    suite_name: ctx.suite_name.clone(),
                    total,
                    outcomes,
                    cleanup_errors,
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

    let outcomes: Vec<CaseOutcome> = {
        // Unloadable case files count as failures of themselves.
        let mut outcomes: Vec<CaseOutcome> =
            load_failure_outcomes(&ctx.suite_name, &ps.load_failures);
        for outcome in &outcomes {
            report_outcome(outcome);
        }

        // Run parallel cases first.
        if ctx
            .controls
            .as_ref()
            .is_some_and(|controls| controls.has_restarts)
        {
            for case in &parallel_cases {
                let outcome = run_case(ctx, case, abort);
                report_outcome(&outcome);
                outcomes.push(outcome);
            }
        } else {
            outcomes.extend(
                parallel_cases
                    .par_iter()
                    .map(|case| {
                        let outcome = run_case(ctx, case, abort);
                        report_outcome(&outcome);
                        outcome
                    })
                    .collect::<Vec<CaseOutcome>>(),
            );
        }

        // Then run sequential cases one by one.
        for case in &sequential_cases {
            let outcome = run_case(ctx, case, abort);
            report_outcome(&outcome);
            outcomes.push(outcome);
        }
        outcomes
    };

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
        if let Err(exc) = execute_hook(&ctx.target_admin_conn, ctx.query_timeout, hook, "target") {
            preserve_suite_failure_snapshot(ctx, "cleanup-target", stdout_lock);
            cleanup_errors.push(format!("[{}] {exc:#}", ctx.suite_name));
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
            if let Err(exc) = execute_hook(
                &ctx.reference_admin_conn,
                ctx.query_timeout,
                hook,
                "reference",
            ) {
                preserve_suite_failure_snapshot(ctx, "cleanup-reference", stdout_lock);
                cleanup_errors.push(format!("[{}] {exc:#}", ctx.suite_name));
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
    cases.iter().flat_map(|case| case.steps.iter()).any(|step| {
        fault_injection::has_fault(&step.meta)
            || step.meta.restart_fe_after_step
            || step.meta.imv_accelerator_wipe_restart.is_some()
    })
}

fn validate_fault_injection_jobs(cases: &[SqlCase], jobs: usize) -> Result<()> {
    if jobs != 1 && cases_have_fault_directives(cases) {
        bail!(
            "fault injection directives require -j 1 because they mutate the shared cross-process cluster"
        );
    }
    Ok(())
}

/// SQL that drives a row-level DML operation through the frontend's coordination
/// journal.
///
/// Detection is deliberately textual and generous: this gates parallelism, so a
/// false positive costs wall-clock while a false negative costs a flaky suite.
fn statement_starts_dml_operation(sql: &str) -> bool {
    let mut head = sql.trim_start();
    // Skip line comments, which every case in these suites opens with.
    while let Some(rest) = head.strip_prefix("--") {
        head = match rest.find('\n') {
            Some(idx) => rest[idx + 1..].trim_start(),
            None => "",
        };
    }
    let head = head.to_ascii_uppercase();
    ["DELETE ", "UPDATE ", "MERGE ", "INSERT ", "ALTER TABLE "]
        .iter()
        .any(|prefix| head.starts_with(prefix))
}

fn cases_start_dml_operations(cases: &[SqlCase]) -> bool {
    cases
        .iter()
        .flat_map(|case| case.steps.iter())
        .any(|step| statement_starts_dml_operation(&step.sql))
}

/// Reject parallel jobs for DML suites on a shared cross-process cluster.
///
/// One frontend owns one coordination journal. Two DML operations admitted
/// concurrently contend for the same coordination lease, and the loser surfaces
/// as `CoordinationUnresolved: OperationNotCommitted` on whichever case happened
/// to lose. That is indistinguishable from a real failure in the report, and it
/// moves between cases from run to run, so a suite that is actually stable looks
/// intermittently broken. Measured on `iceberg-dml` at 1FE+3BE: 4 to 12 failures
/// per run with the default job count and none of them reproducible, against 6
/// stable failures with `-j 1`.
///
/// This is the same reasoning as the fault-injection guard above: when a suite
/// mutates shared cluster state, serial execution is a correctness requirement
/// for the report, not a performance preference.
fn validate_dml_cluster_jobs(cases: &[SqlCase], jobs: usize, mode: ClusterMode) -> Result<()> {
    if jobs != 1 && mode == ClusterMode::CrossProcess && cases_start_dml_operations(cases) {
        bail!(
            "row-DML suites require -j 1 on a cross-process cluster because concurrent operations \
             contend for one frontend's coordination lease and report the loser as \
             CoordinationUnresolved"
        );
    }
    Ok(())
}

/// Every action a successful step asks the runner to perform on the server
/// itself: an Accelerator wipe, a frontend restart, and the operator
/// declaration that retires a restart barrier.
fn run_post_step_server_actions(
    step: &SqlStep,
    ctx: &SuiteRunContext,
    session: &mut MysqlSession,
    publication_fault: &mut Option<publication_catalog::FixtureFaultGuard>,
    log: &mut String,
) -> Result<()> {
    if step.meta.publication_catalog_fault_timing
        == Some(PublicationCatalogFaultTiming::AfterFrontendExit)
    {
        ensure!(
            step.meta.restart_fe_after_step && step.meta.imv_accelerator_wipe_restart.is_none(),
            "after-frontend-exit timing requires the original explicit frontend restart"
        );
        let mut called = false;
        let result = {
            let mut after_exit = || {
                ensure!(
                    !called,
                    "after-frontend-exit hook was invoked more than once"
                );
                called = true;
                ensure!(
                    publication_fault.is_none(),
                    "publication fault is already armed"
                );
                *publication_fault = Some(arm_publication_catalog_fault(step, ctx)?);
                Ok(())
            };
            restart_frontend_after_step(
                step,
                &ctx.server_handle,
                session,
                Some(&mut after_exit),
                log,
            )
        };
        if publication_fault.is_some() {
            let _ = writeln!(
                log,
                "    @publication_catalog_fault armed after original frontend exit"
            );
        }
        result?;
        ensure!(
            called,
            "frontend restart did not invoke its required after-exit hook"
        );
    } else {
        restart_frontend_after_step(step, &ctx.server_handle, session, None, log)?;
    }
    resume_mv_management_after_step(step, session, log)
}

/// Retire one target's management barrier the way an operator does.
///
/// It reads the status this process issues and declares the incarnation that
/// status names isolated. Nothing is invented: a target with no barrier and no
/// challenge fails the directive, because a case that resumed nothing proves
/// nothing.
fn resume_mv_management_after_step(
    step: &SqlStep,
    session: &mut MysqlSession,
    log: &mut String,
) -> Result<()> {
    let Some(resume) = step.meta.mv_resume_management.as_ref() else {
        return Ok(());
    };
    let status_sql = format!(
        "CALL novarocks_mv_management_status('{}', '{}', '{}')",
        resume.catalog, resume.database, resume.mv
    );
    let _ = writeln!(log, "    @mv_resume_management: {status_sql}");
    let status = execute_required_query(session, 60, &status_sql)
        .map_err(|reason| anyhow::anyhow!("@mv_resume_management status failed: {reason}"))?;
    let property = |name: &str| -> Option<String> {
        status
            .rows
            .iter()
            .find(|row| row.first().map(String::as_str) == Some(name))
            .and_then(|row| row.get(1))
            .filter(|value| !value.is_empty() && value.as_str() != "NULL")
            .cloned()
    };
    let challenge = property("Challenge").ok_or_else(|| {
        anyhow::anyhow!(
            "@mv_resume_management found no challenge for {}.{}.{}; management was not closed",
            resume.catalog,
            resume.database,
            resume.mv
        )
    })?;
    let old_incarnation = property("UnsettledEffect1Incarnation").ok_or_else(|| {
        anyhow::anyhow!(
            "@mv_resume_management found no unresolved effect for {}.{}.{}; there is no barrier \
             to retire",
            resume.catalog,
            resume.database,
            resume.mv
        )
    })?;
    let resume_sql = format!(
        "CALL novarocks_mv_resume_management('{}', '{}', '{}', '{challenge}', \
         '{old_incarnation}', 'sql-test-runner', \
         'the runner replaced the declared frontend process before this statement')",
        resume.catalog, resume.database, resume.mv
    );
    let result = execute_required_query(session, 60, &resume_sql)
        .map_err(|reason| anyhow::anyhow!("@mv_resume_management declaration failed: {reason}"))?;
    let settled = result
        .rows
        .iter()
        .find(|row| row.first().map(String::as_str) == Some("SettledEffects"))
        .and_then(|row| row.get(1))
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or_default();
    if settled == 0 {
        bail!("@mv_resume_management settled no effect");
    }
    let _ = writeln!(
        log,
        "    @mv_resume_management PASS (settled {settled}, old incarnation {old_incarnation})"
    );
    Ok(())
}

fn restart_frontend_after_step(
    step: &SqlStep,
    server_handle: &Arc<Mutex<Box<dyn ServerHandle>>>,
    session: &mut MysqlSession,
    after_exit: Option<&mut dyn FnMut() -> Result<()>>,
    log: &mut String,
) -> Result<()> {
    let Some(wipe) = step.meta.imv_accelerator_wipe_restart.as_ref() else {
        if !step.meta.restart_fe_after_step {
            return Ok(());
        }
        let mut server = server_handle
            .lock()
            .map_err(|_| anyhow::anyhow!("server handle mutex is poisoned"))?;
        if server.be_count() == 0 {
            bail!("@restart_fe_after_step requires a runner-owned cross-process frontend");
        }
        match after_exit {
            Some(after_exit) => server.restart_fe_with_after_exit_hook(after_exit),
            None => server.restart_fe(),
        }
        .context("restart frontend after successful SQL step")?;
        drop(server);
        session
            .reconnect()
            .context("reconnect target SQL session after frontend restart")?;
        let _ = writeln!(log, "    @restart_fe_after_step PASS");
        return Ok(());
    };

    let catalog = wipe.catalog.as_deref().unwrap_or("ice");
    let fqn = wipe.mv.as_str();
    let call = format!(
        "CALL {catalog}.system.novarocks_imv_stateless_rebuild(table => '{fqn}', level => 'wipe')"
    );
    let _ = writeln!(log, "    @imv_accelerator_wipe_restart: {call}");
    let result = execute_required_query(session, 60, &call)
        .map_err(|reason| anyhow::anyhow!("@imv_accelerator_wipe_restart failed: {reason}"))?;
    let level = result
        .rows
        .first()
        .and_then(|row| row.first())
        .map(String::as_str);
    if !level.is_some_and(|value| value.eq_ignore_ascii_case("wipe")) {
        bail!("@imv_accelerator_wipe_restart did not confirm Accelerator wipe");
    }
    let mut server = server_handle
        .lock()
        .map_err(|_| anyhow::anyhow!("server handle mutex is poisoned"))?;
    if server.be_count() == 0 {
        bail!("@imv_accelerator_wipe_restart requires a runner-owned cross-process frontend");
    }
    match after_exit {
        Some(after_exit) => server.restart_fe_with_after_exit_hook(after_exit),
        None => server.restart_fe(),
    }
    .context("restart frontend after Accelerator wipe")?;
    drop(server);
    session
        .reconnect()
        .context("reconnect target SQL session after frontend restart")?;
    let _ = writeln!(log, "    @imv_accelerator_wipe_restart PASS");
    Ok(())
}

#[cfg(test)]
fn sql_text_has_query_lifecycle_fault_directive(sql: &str) -> bool {
    const DIRECTIVES: &[&str] = &[
        "kill_fe_after_mv_known_committed_before_projector_cas",
        "restart_be_after_establish_context_index",
        "kill_query_after_be_log_contains",
        "kill_fe_after_be_log_contains",
        "kill_be_after_be_log_contains",
        "terminal_snapshot_conflict_be_index",
        "query_lifecycle_fault",
        // Structured runtime-filter assertions read the same debug projection
        // as the lifecycle fault assertions, even when the query itself has no
        // injected fault.
        "expect_runtime_filter_available",
        "expect_runtime_filter_detail",
        "expect_runtime_filter_total_at_least",
        "kill_query_at_lifecycle_phase",
        "kill_be_at_lifecycle_phase",
        "query_control_fragment_backend_limit",
    ];
    const LIFECYCLE_EVIDENCE_MARKERS: &[&str] = &[
        "NOVAROCKS_QUERY_INIT_",
        "NOVAROCKS_QUERY_CONTROL_",
        "NOVAROCKS_QUERY_FRAGMENT_",
        "NOVAROCKS_QUERY_LIFECYCLE_",
        "NOVAROCKS_QUERY_TERMINAL_",
    ];
    sql.lines().any(|line| {
        let line = line.trim_start();
        DIRECTIVES
            .iter()
            .any(|directive| line.starts_with(&format!("-- @{directive}=")))
            || (line.starts_with("-- @be_log")
                && LIFECYCLE_EVIDENCE_MARKERS
                    .iter()
                    .any(|marker| line.contains(marker)))
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
    if suite_names
        .iter()
        .any(|suite| suite == "mv-publication-v11")
        && (mode != ClusterMode::CrossProcess || cluster_size != 3)
    {
        bail!("mv-publication-v11 requires --cluster-mode cross-process --cluster-size 3");
    }
    Ok(())
}

fn validate_lake_publication_preflight(
    suite_names: &[String],
    mode: ClusterMode,
    cluster_size: usize,
    jobs: usize,
) -> Result<()> {
    let requires_native_lake_gate = suite_names.iter().any(|suite| {
        suite == "lake-publication" || suite == "lnp-3a-mv-rebuild" || suite == "mv-publication-v11"
    });
    if !requires_native_lake_gate {
        return Ok(());
    }
    if mode != ClusterMode::CrossProcess || cluster_size != 3 {
        bail!("lake publication suites require --cluster-mode cross-process --cluster-size 3");
    }
    if jobs != 1 {
        bail!(
            "lake publication suites require -j 1 because their fixtures own one destructive token"
        );
    }
    Ok(())
}

fn validate_lnp_3c_runtime_cut_preflight(
    suite_names: &[String],
    mode: ClusterMode,
    cluster_size: usize,
    jobs: usize,
) -> Result<()> {
    if !suite_names
        .iter()
        .any(|suite| suite == "lnp-3c-runtime-cut")
    {
        return Ok(());
    }
    if mode != ClusterMode::CrossProcess || cluster_size != 3 {
        bail!("lnp-3c-runtime-cut requires --cluster-mode cross-process --cluster-size 3");
    }
    if jobs != 1 {
        bail!("lnp-3c-runtime-cut requires -j 1 because it restarts the shared frontend");
    }
    Ok(())
}

fn validate_lnp_3d_mv_accelerator_preflight(
    suite_names: &[String],
    mode: ClusterMode,
    cluster_size: usize,
    jobs: usize,
) -> Result<()> {
    if !suite_names
        .iter()
        .any(|suite| suite == "lnp-3d-mv-accelerator")
    {
        return Ok(());
    }
    if mode != ClusterMode::CrossProcess || cluster_size != 3 {
        bail!("lnp-3d-mv-accelerator requires --cluster-mode cross-process --cluster-size 3");
    }
    if jobs != 1 {
        bail!(
            "lnp-3d-mv-accelerator requires -j 1 because it wipes and restarts the shared frontend"
        );
    }
    Ok(())
}

fn validate_publication_catalog_directives(
    suite_name: &str,
    cases: &[SqlCase],
    fixture_available: bool,
    cluster_mode: ClusterMode,
    mode: Mode,
) -> Result<()> {
    for case in cases {
        for step in &case.steps {
            if step.meta.publication_catalog_fault.is_none()
                && step.meta.publication_catalog_fault_timing.is_none()
                && step.meta.publication_catalog_fault_target.is_none()
                && step.meta.publication_service_hold.is_none()
                && step.meta.publication_catalog_concurrent_shell.is_none()
            {
                continue;
            }
            if step.meta.publication_catalog_fault_timing.is_some()
                && step.meta.publication_catalog_fault.is_none()
            {
                bail!("publication_catalog_fault_timing requires its original fault directive");
            }
            if step.meta.publication_catalog_fault_timing
                == Some(PublicationCatalogFaultTiming::AfterFrontendExit)
                && (!matches!(
                    step.meta.publication_catalog_fault,
                    Some(PublicationCatalogFaultDirective {
                        action: PublicationCatalogAction::TableLoad,
                        fault: PublicationCatalogFault::CorruptPackage,
                    })
                ) || step.meta.publication_catalog_fault_target.is_none()
                    || !step.meta.restart_fe_after_step
                    || step.meta.imv_accelerator_wipe_restart.is_some()
                    || mode != Mode::Verify
                    || cluster_mode != ClusterMode::CrossProcess)
            {
                bail!(
                    "after-frontend-exit timing requires an exact table-load,corrupt-package fault and explicit cross-process frontend restart in verify mode"
                );
            }
            if step.meta.publication_catalog_fault_target.is_some()
                && !matches!(
                    step.meta.publication_catalog_fault,
                    Some(PublicationCatalogFaultDirective {
                        action: PublicationCatalogAction::TableLoad,
                        fault: PublicationCatalogFault::CorruptPackage,
                    })
                )
            {
                bail!("publication_catalog_fault_target requires table-load,corrupt-package");
            }
            if !publication_catalog_fixture_suite(suite_name) {
                bail!(
                    "@publication_catalog_fault is acceptance-only and is only valid in {} (found in {suite_name}/{})",
                    PUBLICATION_CATALOG_FIXTURE_SUITES.join(", "),
                    case.case_id
                );
            }
            if step.meta.publication_service_hold.is_some() && suite_name != "mv-publication-v11" {
                bail!("@publication_service_hold is only valid in mv-publication-v11");
            }
            if step.meta.publication_service_hold.is_some()
                && step.meta.retry_count.is_some_and(|count| count != 1)
            {
                bail!("@publication_service_hold permits exactly one primary query attempt");
            }
            if step.meta.publication_service_hold.is_some()
                && step.meta.publication_catalog_fault.is_some()
            {
                bail!("a REST service hold cannot share a step with a proxy fault");
            }
            if !case.sequential {
                bail!(
                    "@publication_catalog_fault requires file-level @sequential=true ({suite_name}/{})",
                    case.case_id
                );
            }
            if !fixture_available {
                bail!(
                    "@publication_catalog_fault requires the runner-owned publication catalog fixture"
                );
            }
            let requires_concurrent_shell = step
                .meta
                .publication_catalog_fault
                .is_some_and(|directive| directive.fault.requires_concurrent_shell())
                || step.meta.publication_service_hold.is_some();
            if requires_concurrent_shell != step.meta.publication_catalog_concurrent_shell.is_some()
            {
                bail!(
                    "@publication_catalog_concurrent_shell must appear exactly with a proxy or REST service publication hold"
                );
            }
            if requires_concurrent_shell && mode != Mode::Verify {
                bail!(
                    "@publication_catalog_concurrent_shell is acceptance-only and requires --mode verify"
                );
            }
            if step
                .meta
                .publication_catalog_fault
                .is_some_and(|directive| directive.fault.requires_inflight_frontend_kill())
                && cluster_mode != ClusterMode::CrossProcess
            {
                bail!(
                    "@publication_catalog_fault=table-commit,after-commit-hold-for-frontend-kill requires --cluster-mode cross-process"
                );
            }
        }
    }
    Ok(())
}

fn selected_meta_requires_lifecycle(meta: &QueryMeta) -> bool {
    let lifecycle_marker = |marker: &str| {
        [
            "NOVAROCKS_QUERY_INIT_",
            "NOVAROCKS_QUERY_CONTROL_",
            "NOVAROCKS_QUERY_FRAGMENT_",
            "NOVAROCKS_QUERY_LIFECYCLE_",
            "NOVAROCKS_QUERY_TERMINAL_",
        ]
        .iter()
        .any(|prefix| marker.contains(prefix))
    };
    meta.kill_fe_after_mv_known_committed_before_projector_cas
        || meta.restart_be_after_establish_context_index.is_some()
        || meta.kill_query_after_be_log_contains.is_some()
        || meta.kill_fe_after_be_log_contains.is_some()
        || meta.kill_be_after_be_log_contains.is_some()
        || meta.terminal_snapshot_conflict_be_index.is_some()
        || meta.query_lifecycle_fault.is_some()
        || !meta.query_lifecycle_faults.is_empty()
        || meta.query_lifecycle_structured_assertion.is_some()
        || meta.kill_query_at_lifecycle_phase.is_some()
        || meta.kill_be_at_lifecycle_phase.is_some()
        || meta.query_control_fragment_backend_limit.is_some()
        || meta
            .be_log_contains
            .iter()
            .chain(&meta.be_log_not_contains)
            .any(|marker| lifecycle_marker(marker))
        || meta
            .be_log_count_at_least
            .iter()
            .chain(&meta.be_log_be_count_at_least)
            .any(|(marker, _)| lifecycle_marker(marker))
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

pub fn run_correctness(build: build_information::BuildInformation) -> Result<()> {
    if std::env::args().skip(1).eq(["--build-info"]) {
        println!("{}", build_information::value(&build));
        return Ok(());
    }
    let exit_code = match run_cli(Cli::parse()) {
        Err(error)
            if error.is::<novarocks_cluster_harness::fixture_run::FixtureBudgetRejected>() =>
        {
            eprintln!("{error:#}");
            75
        }
        result => result?,
    };
    if exit_code != 0 {
        std::process::exit(exit_code);
    }
    Ok(())
}

fn shutdown_server_handle(
    server_handle: &Arc<Mutex<Box<dyn ServerHandle>>>,
    retain_runtime_artifacts: bool,
) -> (Result<()>, bool) {
    let mut errors = Vec::new();
    let mut writer_exit_observed = false;
    match server_handle.lock() {
        Ok(mut handle) => {
            if retain_runtime_artifacts {
                handle.retain_runtime_artifacts();
            }
            if let Err(error) = handle.shutdown() {
                errors.push(format!("shutdown failed: {error:#}"));
            }
            writer_exit_observed = handle.writer_exit_observed();
            let residual = handle.residual_process_ids();
            if !residual.is_empty() {
                writer_exit_observed = false;
                errors.push(format!(
                    "residual server process IDs after shutdown: {residual:?}"
                ));
            }
        }
        Err(_) => errors.push("server handle lock poisoned during shutdown".to_string()),
    }
    let result = if errors.is_empty() {
        Ok(())
    } else {
        Err(anyhow::anyhow!(errors.join("; ")))
    };
    (result, writer_exit_observed)
}

#[cfg(test)]
fn finish_run_with_server_cleanup(
    server_handle: Arc<Mutex<Box<dyn ServerHandle>>>,
    primary_result: Result<i32>,
    failure_artifacts: &FailureArtifactRecorder,
) -> Result<i32> {
    finish_run_with_server_cleanup_recorded(
        server_handle,
        primary_result,
        failure_artifacts,
        None,
        false,
        None,
    )
}

fn finish_run_with_server_cleanup_recorded(
    server_handle: Arc<Mutex<Box<dyn ServerHandle>>>,
    primary_result: Result<i32>,
    failure_artifacts: &FailureArtifactRecorder,
    results: Option<&Arc<results_record::ResultsWriter>>,
    external: bool,
    writer_exit: Option<&AtomicBool>,
) -> Result<i32> {
    let primary_failed = !matches!(&primary_result, Ok(0));
    // A real cross-process shutdown removes its runtime directory even when
    // process cleanup reports an error. Stage one bounded terminal candidate
    // while the logs still exist, but persist it only if shutdown is the
    // failure; successful runs retain the existing no-artifact cleanup.
    let shutdown_failure_candidate = if primary_failed {
        Ok(None)
    } else {
        failure_artifacts.stage_run_failure(server_handle.as_ref())
    };
    let mut preserve_result = if primary_failed {
        failure_artifacts.persist_run_failure(server_handle.as_ref())
    } else {
        Ok(None)
    };
    report_preserved_failure_artifacts(&preserve_result);
    // A successful stop proves process cleanup, not that the SQL run passed.
    // Keep the originals if execution or its already-attempted preservation
    // failed; bounded failure snapshots do not replace those original files.
    let retain_runtime_artifacts =
        primary_failed || preserve_result.is_err() || shutdown_failure_candidate.is_err();
    let (cleanup_result, writer_exit_observed) =
        shutdown_server_handle(&server_handle, retain_runtime_artifacts);
    let writer_exit_observed = writer_exit_observed && !external;
    if let Some(observed) = writer_exit {
        observed.store(writer_exit_observed, Ordering::Release);
    }
    if let Some(results) = results {
        results.observe(serde_json::json!({"kind":"server","cleanup":{"status":if cleanup_result.is_err(){"failed"}else if external{"external"}else{"ok"},"error":cleanup_result.as_ref().err().map(|e|format!("{e:#}")),"residual_pids":[],"writer_exit_observed":writer_exit_observed}}));
    }
    if let Err(error) = &cleanup_result {
        failure_artifacts.record_provenance(
            "server-cleanup".to_string(),
            serde_json::json!(format!("{error:#}")),
        );
    }
    if !primary_failed && cleanup_result.is_err() {
        preserve_result = shutdown_failure_candidate
            .and_then(|candidate| failure_artifacts.persist_staged_run_failure(candidate));
        report_preserved_failure_artifacts(&preserve_result);
    }
    if let Ok(Some(path)) = &preserve_result {
        let outcome = serde_json::json!({
            "primary": match &primary_result {Ok(code) => format!("exit_code={code}"), Err(error) => format!("{error:#}")},
            "server_cleanup": match &cleanup_result {Ok(()) => "complete; no residual server processes".to_string(), Err(error) => format!("{error:#}")}
        });
        if let Err(error) = failure_artifacts.persist_terminal_outcome(path, &outcome) {
            preserve_result = Err(error);
        }
    }
    let run_result = match (primary_result, cleanup_result) {
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
    };
    match (run_result, preserve_result) {
        (result, Ok(_)) => result,
        (Ok(exit_code), Err(artifact)) => Err(anyhow::anyhow!(
            "run exited with code {exit_code}; failure artifact preservation failed: {artifact:#}"
        )),
        (Err(run), Err(artifact)) => Err(anyhow::anyhow!(
            "{run:#}; failure artifact preservation failed: {artifact:#}"
        )),
    }
}

fn report_preserved_failure_artifacts(result: &Result<Option<PathBuf>>) {
    match result {
        Ok(Some(path)) => eprintln!("cross-process failure artifacts: {}", path.display()),
        Ok(None) => {}
        Err(error) => eprintln!("failed to preserve cross-process failure artifacts: {error:#}"),
    }
}

fn select_cluster_launch_profile(
    requested: Option<novarocks_cluster_harness::LaunchProfile>,
    query_lifecycle_faults: bool,
    cleanup_faults: bool,
) -> Result<novarocks_cluster_harness::LaunchProfile> {
    use novarocks_cluster_harness::LaunchProfile;
    let has_faults = query_lifecycle_faults || cleanup_faults;
    let profile = requested.unwrap_or(LaunchProfile::FaultScenario);
    if profile == LaunchProfile::Performance && has_faults {
        bail!("performance launch profile refuses query-lifecycle or cleanup fault directives");
    }
    Ok(profile)
}

fn validate_dataset_launch_profile(
    selected: &[String],
    suites: &BTreeMap<String, SuiteConfig>,
    profile: novarocks_cluster_harness::LaunchProfile,
) -> Result<()> {
    if profile != novarocks_cluster_harness::LaunchProfile::Performance {
        for suite in selected {
            anyhow::ensure!(
                suites[suite].manifest.prepared_dataset.is_none(),
                "suite {suite} requires the clean --cluster-launch-profile performance; selected profile is {profile:?}"
            );
        }
    }
    Ok(())
}

pub(crate) fn run_cli(cli: Cli) -> Result<i32> {
    let base_dir = resolve_repo_root()?;
    let suite_configs = build_suite_configs(&base_dir)?;
    if suite_configs.is_empty() {
        println!(
            "❌ ERROR: no suite directories found under {}",
            base_dir.join("tests/sql/correctness").display()
        );
        return Ok(1);
    }

    if cli.list_declarations {
        println!(
            "{}",
            serde_json::to_string_pretty(&results_record::declarations(&suite_configs)?)?
        );
        return Ok(0);
    }

    if cli.list_suites {
        let listed = if let Some(requested) = cli.suite.as_deref() {
            select_suite_names(requested, &suite_configs)?
        } else {
            suite_configs
                .iter()
                .filter(|(_, suite)| suite.manifest.disabled_reason.is_none())
                .map(|(name, _)| name.clone())
                .collect()
        };
        for suite_name in listed {
            println!("{suite_name}");
        }
        return Ok(0);
    }

    if cli.list_extensions {
        let meta_re = Regex::new(r"^--\s*@([a-zA-Z0-9_]+)\s*=\s*(.+?)\s*$")?;
        let marker_re = Regex::new(r"(?i)^--\s*query\s+(\d+)(?:\s+.*)?$")?;
        let entries =
            extension_manifest::derive_extension_manifest(&suite_configs, &meta_re, &marker_re)?;
        print!(
            "{}",
            extension_manifest::render_extension_manifest(&entries)
        );
        return Ok(0);
    }

    let config_path = resolve_config_path(cli.config.as_deref(), &base_dir);
    let mut runner_config = load_runner_config(config_path.as_deref())?;
    let suite_names = match select_suite_names(
        cli.suite
            .as_deref()
            .expect("clap requires --suite unless --list-extensions is set"),
        &suite_configs,
    ) {
        Ok(suite_names) => suite_names,
        Err(error) => {
            println!("❌ ERROR: {error}");
            return Ok(1);
        }
    };
    let failure_artifact_override = cli
        .failure_artifact_dir
        .clone()
        .or_else(|| env_optional("NOVAROCKS_SQL_TEST_FAILURE_ARTIFACT_DIR"));
    let failure_artifact_root = resolve_path(failure_artifact_override.as_deref(), &base_dir)
        .unwrap_or_else(|| base_dir.join("logs/sql-test-failures"));
    let failure_artifacts = Arc::new(FailureArtifactRecorder::new(FailureArtifactContext {
        root: failure_artifact_root,
        lane: "correctness".to_string(),
        suites: suite_names.clone(),
    }));
    let selected_cluster_mode = cli.cluster_mode;
    let selected_cluster_size = cli.cluster_size.unwrap_or(1);
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
    if let Err(error) = validate_lake_publication_preflight(
        &suite_names,
        selected_cluster_mode,
        selected_cluster_size,
        cli.jobs,
    ) {
        println!("❌ ERROR: {error}");
        return Ok(1);
    }
    if let Err(error) = validate_lnp_3c_runtime_cut_preflight(
        &suite_names,
        selected_cluster_mode,
        selected_cluster_size,
        cli.jobs,
    ) {
        println!("❌ ERROR: {error}");
        return Ok(1);
    }
    if let Err(error) = validate_lnp_3d_mv_accelerator_preflight(
        &suite_names,
        selected_cluster_mode,
        selected_cluster_size,
        cli.jobs,
    ) {
        println!("❌ ERROR: {error}");
        return Ok(1);
    }
    FixtureEndpoints::preflight_cloud(&runner_config, &suite_names)?;
    let owns_fixture_run =
        std::env::var_os(novarocks_cluster_harness::fixture_run::RUN_CONTEXT_ENV).is_none();
    let original_fixture_env = runner_config
        .values
        .get("fixture_env_file")
        .map(PathBuf::from);
    let server_writers_exited = AtomicBool::new(false);
    let fixture_writers_settled = Arc::new(AtomicBool::new(true));
    let fixture_budget =
        fixture_context::admit(&runner_config, &base_dir, cli.fixture_budget_secs)?;
    let fixture_storage = fixture_context::storage_scope(&runner_config, &fixture_budget)?;
    let mut original_fixture_material = match (&original_fixture_env, &fixture_storage) {
        (Some(env_file), Some(storage)) => {
            let material = failure_artifacts.persist_original_fixture_run(
                &fixture_budget.to_json()?,
                &storage.to_json()?,
                env_file,
                owns_fixture_run,
            )?;
            eprintln!(
                "Original fixture run recovery material: {}",
                material.path().display()
            );
            Some(material)
        }
        _ => None,
    };
    let naming_sources =
        fixture_context::selected_sources(&cli, &base_dir, &suite_names, &suite_configs)?;
    FixtureEndpoints::reject_unsupported_sources(&runner_config, &naming_sources)?;
    let mut fixture_names = fixture_context::RunNames::prepare(&fixture_budget, &naming_sources)?;
    fixture_names.seal_catalogs(&runner_config, &naming_sources, &suite_configs)?;
    runner_config.fixture_names = Some(Arc::new(fixture_names));
    fixture_context::startup_sweep(&runner_config, &base_dir, &fixture_budget)?;
    // Before the proxy: it forwards to whatever REST Catalog this run uses, and
    // an isolated one has to exist first.
    let results_writer =
        results_record::ResultsWriter::create(&base_dir, cli.results_file.as_deref())?;
    let fixture_root = suite_names
        .iter()
        .any(|s| isolated_rest_catalog_suite(s))
        .then(|| base_dir.join("tests/sql/.runtime/isolated-rest"));
    let resources_before = match &fixture_root {
        Some(root) => novarocks_cluster_harness::resource_registration::list(root)?,
        None => Default::default(),
    };
    let effective_config = results_writer.capture_configuration(&cli, &base_dir, &runner_config)?;
    let run_record = results_record::run_record(
        &cli,
        &base_dir,
        Some(&effective_config),
        &suite_names,
        fixture_root.as_deref(),
    )?;
    for key in ["runner", "server", "config"] {
        if let Some(path) = run_record[key]["path"].as_str() {
            ensure!(
                results_writer.track_file(std::path::Path::new(path))?
                    == run_record[key]["sha256"]
                        .as_str()
                        .context("recorded input hash absent")?,
                "execution input {key} changed before launch"
            );
        }
    }
    results_writer.write(run_record)?;
    let (mut isolated_rest_catalog, publication_control_uri) = results_record::fixture_start(
        &results_writer,
        fixture_root.as_deref(),
        &resources_before,
        || start_isolated_rest_catalog(&suite_names, &fixture_budget),
    )?;
    let selection_written = AtomicBool::new(false);
    let server_recorded = AtomicBool::new(false);
    let totals = Mutex::new((0usize, 0usize, 0usize));
    let mut suite_facts = BTreeMap::new();
    let namespace_catalogs = namespace_guard::CatalogFacts::default();
    let mut publication_catalog_fixture: Option<publication_catalog::FixtureHandle> = None;
    let fixture_primary = (|| -> Result<i32> {
        if let Some(fixture) = isolated_rest_catalog.as_ref() {
            namespace_catalogs.bind_fixture_storage(&fixture.rest_storage_binding())?;
        }
        let fixture_endpoints = if let Some(fixture) = isolated_rest_catalog.as_ref() {
            FixtureEndpoints::from_isolated(
                fixture.endpoints(),
                &fixture.static_s3_identity(),
                fixture.runtime_env_file()?,
            )?
        } else {
            FixtureEndpoints::from_runner_config(&runner_config)?
        };
        let mut subprocess_environment = fixture_endpoints.project(&mut runner_config);
        if let Some(storage) = &fixture_storage {
            subprocess_environment.insert(
                novarocks_cluster_harness::fixture_run::RUN_STORAGE_ENV.into(),
                storage.to_json()?,
            );
            if isolated_rest_catalog.is_none() {
                runner_config.values.insert(
                    "__shared_rest_run_root".into(),
                    fixture_budget.identity().scope_root.clone(),
                );
            }
        }
        // SAFETY: no runner-owned server, proxy or case worker exists yet.
        // Publish the physical fixture projection once before starting those threads.
        unsafe {
            if fixture_endpoints.is_node_local() {
                for key in [
                    "AWS_S3_ACCESS_KEY_ID",
                    "AWS_S3_SECRET_ACCESS_KEY",
                    "AWS_ACCESS_KEY_ID",
                    "AWS_SECRET_ACCESS_KEY",
                    "AWS_SESSION_TOKEN",
                    "MINIO_ROOT_USER",
                    "MINIO_ROOT_PASSWORD",
                ] {
                    std::env::remove_var(key);
                }
            }
            for (key, value) in &subprocess_environment {
                std::env::set_var(key, value);
            }
        }
        let mut dataset_bindings = BTreeMap::new();
        for suite_name in &suite_names {
            if let Some(requirement) = &suite_configs[suite_name].manifest.prepared_dataset {
                let binding = dataset_fixture::bind_prepared_dataset_with_budget(
                    requirement,
                    &runner_config,
                    &base_dir,
                    &fixture_budget,
                )?;
                failure_artifacts.record_provenance(
                    format!("dataset/{suite_name}"),
                    serde_json::to_value(&binding)?,
                );
                dataset_bindings.insert(suite_name.clone(), binding);
            }
        }
        let publication_service_control = if let Some(control_uri) = publication_control_uri {
            let fixture = isolated_rest_catalog
                .as_ref()
                .context("MV V11 requires a private REST Catalog")?;
            let identity = fixture.runtime_identity()?;
            let image = identity
                .images
                .get("rest")
                .context("publication hook image identity is absent")?;
            println!(
                "  V11 post-requirements REST hook image={} id={}",
                image.image_reference, image.image_id
            );
            Some(publication_service::Control::new(control_uri)?)
        } else {
            None
        };
        let publication_downstream = runner_config.values.get("iceberg_rest_uri").cloned();
        publication_catalog_fixture =
            start_publication_catalog_fixture(&mut runner_config, &suite_names)?;
        if isolated_rest_catalog.is_some()
            && let Some(proxy) = publication_catalog_fixture.as_ref()
        {
            namespace_catalogs.bind_owned_proxy_alias(
                proxy.uri(),
                publication_downstream
                    .as_deref()
                    .context("owned REST proxy omitted its physical downstream")?,
            )?;
        }
        let publication_catalog_control = publication_catalog_fixture
            .as_ref()
            .map(publication_catalog::FixtureHandle::control)
            .transpose()?;

        // Validate: per-suite path overrides conflict with multi-suite
        let multi_suite = suite_names.len() > 1;
        if multi_suite
            && (cli.sql_dir.is_some() || cli.result_dir.is_some() || cli.sql_glob.is_some())
        {
            println!(
                "❌ ERROR: --sql-dir, --result-dir, --sql-glob cannot be used with multiple suites"
            );
            return Ok(1);
        }

        if let Some(eps) = cli.float_epsilon
            && eps <= 0.0
        {
            println!("❌ ERROR: --float-epsilon must be > 0");
            return Ok(1);
        }

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

        let mut prepared_inputs: Vec<PreparedSqlInputs> = Vec::new();
        let mut all_available_case_ids = HashSet::new();
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

            let mut snapshot_config = suite.clone();
            snapshot_config.sql_dir = sql_dir.clone();
            snapshot_config.result_dir = result_dir.clone();
            snapshot_config.sql_glob = sql_glob.clone();
            let parse_start = results_record::suite_inputs(&snapshot_config)?;
            let mut placeholder_vars = placeholder_variables(&runner_config, &suite.name)?;
            if let Some(binding) = dataset_bindings.get(suite_name) {
                placeholder_vars.insert(
                    "dataset_iceberg_catalog_warehouse".to_string(),
                    binding.exact_warehouse.clone(),
                );
                println!(
                    "dataset_binding suite={} ready={} publication={} warehouse={}",
                    suite_name,
                    binding.ready_uri,
                    binding.publication_identity,
                    binding.exact_warehouse
                );
            }
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

            let catalog = suite_init_hook
                .as_ref()
                .and_then(|hook| hook.catalog.clone())
                .unwrap_or_else(|| suite.default_catalog.clone());
            let suite_catalog_override = Some(
                runner_config
                    .fixture_names
                    .as_ref()
                    .and_then(|names| names.catalogs.lookup(&catalog))
                    .unwrap_or(&catalog)
                    .to_string(),
            );
            let suite_db_override = suite_init_hook.as_ref().and_then(|hook| hook.db.clone());

            let target_db_default = suite_db_override
                .clone()
                .unwrap_or_else(|| suite.default_db.clone());
            let ref_db_default = suite_db_override
                .clone()
                .unwrap_or_else(|| suite.default_db.clone());

            let verify_enabled = verify_enabled_override.unwrap_or(suite.verify_default);
            let query_timeout = cli.query_timeout.unwrap_or_else(|| {
                env_optional("NOVAROCKS_SQL_TEST_TIMEOUT")
                    .and_then(|raw| raw.parse().ok())
                    .unwrap_or_else(|| suite_default_query_timeout(&suite.name))
            });

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
            let mut load_failures: Vec<CaseLoadFailure> = Vec::new();
            for sql_file in sql_files {
                match parser::load_sql_case_from_file(
                    &sql_file,
                    &meta_re,
                    &marker_re,
                    &placeholder_vars,
                ) {
                    Ok(Some(case)) => cases.push(case),
                    Ok(None) => {
                        load_failures.push(CaseLoadFailure {
                            case_id: sql_file
                                .file_stem()
                                .and_then(|s| s.to_str())
                                .unwrap_or("<unnamed>")
                                .to_owned(),
                            source_file: sql_file.clone(),
                            error: "SQL case contains no executable steps".to_owned(),
                        });
                    }
                    // An unparseable case file is that case failing, not a
                    // suite-level abort: keep loading so the remaining cases
                    // still run and still report.
                    Err(exc) => {
                        let case_id = sql_file
                            .file_stem()
                            .and_then(|stem| stem.to_str())
                            .unwrap_or("<unnamed>")
                            .to_string();
                        println!("❌ ERROR: {}", exc);
                        load_failures.push(CaseLoadFailure {
                            case_id,
                            source_file: sql_file.clone(),
                            error: format!("{exc:#}"),
                        });
                    }
                }
            }

            let available_case_ids: HashSet<String> = cases
                .iter()
                .map(|c| c.case_id.clone())
                .chain(load_failures.iter().map(|f| f.case_id.clone()))
                .collect();
            all_available_case_ids.extend(available_case_ids.iter().cloned());
            let only_set = parse_selector_list(cli.only.as_deref(), &available_case_ids, "--only")?;
            let skip_set = parse_selector_list(cli.skip.as_deref(), &available_case_ids, "--skip")?;

            cases.retain(|case| {
                if !only_set.is_empty() && !only_set.contains(&case.case_id) {
                    return false;
                }
                !skip_set.contains(&case.case_id)
            });
            load_failures.retain(|failure| {
                if !only_set.is_empty() && !only_set.contains(&failure.case_id) {
                    return false;
                }
                !skip_set.contains(&failure.case_id)
            });

            if let Some(limit) = cli.limit
                && cases.len() > limit
            {
                cases.truncate(limit);
            }

            if cases.is_empty() && load_failures.is_empty() {
                println!("⚠️ WARNING: no queries selected for suite {}", suite.name);
                continue;
            }

            for case in &cases {
                for step in &case.steps {
                    parser::validate_test_capability_tags(&step.meta.tags)?;
                    if step.meta.cleanup_fault.is_some()
                        || step.meta.kill_query_at_lifecycle_phase.is_some()
                        || step.meta.kill_be_at_lifecycle_phase.is_some()
                    {
                        bail!(
                            "suite {} case {} step {} requests an internal cleanup/FE-phase control with no current product owner",
                            suite.name,
                            case.case_id,
                            step.query_number
                        );
                    }
                    if let Err(error) = be_log_directive::validate_record_source(
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
                        be_log_directive::validate_execution_mode(&step.meta, cli.mode)
                    {
                        println!(
                            "❌ ERROR: suite {} case {} step {}: {error}",
                            suite.name, case.case_id, step.query_number
                        );
                        return Ok(1);
                    }
                    if let Err(error) =
                        be_log_directive::validate_mode(&step.meta, selected_cluster_mode)
                    {
                        println!(
                            "❌ ERROR: suite {} case {} step {}: {error}",
                            suite.name, case.case_id, step.query_number
                        );
                        return Ok(1);
                    }
                    if step.meta.restart_fe_after_step
                        && selected_cluster_mode != ClusterMode::CrossProcess
                    {
                        println!(
                            "❌ ERROR: suite {} case {} step {}: @restart_fe_after_step requires --cluster-mode cross-process",
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

            if let Err(exc) = validate_dml_cluster_jobs(&cases, jobs, selected_cluster_mode) {
                println!("❌ ERROR: {}", exc);
                return Ok(1);
            }

            if let Err(error) = validate_publication_catalog_directives(
                &suite.name,
                &cases,
                publication_catalog_control.is_some(),
                selected_cluster_mode,
                cli.mode,
            ) {
                println!("❌ ERROR: suite {}: {error}", suite.name);
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

            if cli.mode == Mode::Record
                && let Some(dir) = &result_dir
            {
                fs::create_dir_all(dir)
                    .with_context(|| format!("create result_dir failed: {}", dir.display()))?;
            }

            let parse_end = results_record::suite_inputs(&snapshot_config)?;
            ensure!(
                parse_start["inputs_sha256"] == parse_end["inputs_sha256"],
                "suite {} inputs changed during loading",
                suite.name
            );
            suite_facts.insert(suite.name.clone(), (snapshot_config, parse_start));
            prepared_inputs.push(PreparedSqlInputs {
                suite_name: suite.name.clone(),
                sql_dir,
                result_dir,
                sql_glob,
                suite_catalog_override,
                target_db_default,
                ref_db_default,
                verify_enabled,
                query_timeout,
                cases,
                load_failures,
                suite_init_hook,
                suite_cleanup_hook,
            });
        }

        let mut selected_suites = Vec::new();
        for input in &prepared_inputs {
            let mut effective = suite_configs[&input.suite_name].clone();
            effective.sql_dir = input.sql_dir.clone();
            effective.result_dir = input.result_dir.clone();
            effective.sql_glob = input.sql_glob.clone();
            let mut fact = suite_facts[&input.suite_name].1.clone();
            ensure!(
                fact["inputs_sha256"] == results_record::suite_inputs(&effective)?["inputs_sha256"],
                "suite {} inputs changed before selection",
                input.suite_name
            );
            let selected_ids = input
                .cases
                .iter()
                .map(|c| c.case_id.as_str())
                .chain(input.load_failures.iter().map(|c| c.case_id.as_str()))
                .collect::<HashSet<_>>();
            for case in fact["cases"].as_array_mut().expect("suite case array") {
                let id = case["case"].as_str().unwrap();
                let failure = input.load_failures.iter().find(|f| f.case_id == id);
                case["loadable"] = serde_json::json!(failure.is_none());
                case["load_error"] = serde_json::json!(failure.map(|f| f.error.clone()));
            }
            fact["cases"]
                .as_array_mut()
                .unwrap()
                .retain(|c| selected_ids.contains(c["case"].as_str().unwrap()));
            fact["verify_enabled"] = serde_json::json!(input.verify_enabled);
            fact["query_timeout_secs"] = serde_json::json!(input.query_timeout);
            selected_suites.push(fact.clone());
            suite_facts.insert(input.suite_name.clone(), (effective, fact));
        }
        let fixture_env = runner_config
            .values
            .get("fixture_env_file")
            .map(PathBuf::from);
        if let Some(path) = &fixture_env {
            results_writer.track_file(path)?;
        }
        results_writer.write(serde_json::json!({"kind":"selection",
            "fixture":{"env_file":fixture_env,"env_file_sha256":results_record::optional_hash(fixture_env.as_deref())?,
                "isolated_runtime":isolated_rest_catalog.as_ref().map(|f| f.runtime_identity()).transpose()?, "datasets":dataset_bindings},
            "suites":selected_suites,
            "unresolved_only":cli.only.as_deref().unwrap_or("").split(',').map(str::trim).filter(|id|!id.is_empty() && !all_available_case_ids.contains(*id)).collect::<Vec<_>>(),
            "unresolved_skip":cli.skip.as_deref().unwrap_or("").split(',').map(str::trim).filter(|id|!id.is_empty() && !all_available_case_ids.contains(*id)).collect::<Vec<_>>()}))?;
        selection_written.store(true, Ordering::Relaxed);

        let query_lifecycle_faults_enabled = !cli.dry_run
            && prepared_inputs
                .iter()
                .flat_map(|input| &input.cases)
                .flat_map(|case| &case.steps)
                .any(|step| selected_meta_requires_lifecycle(&step.meta));
        let cleanup_faults_enabled = !cli.dry_run
            && prepared_inputs
                .iter()
                .flat_map(|input| &input.cases)
                .flat_map(|case| &case.steps)
                .any(|step| step.meta.cleanup_fault.is_some());
        let selected_load_failure_count: usize = prepared_inputs
            .iter()
            .map(|input| input.load_failures.len())
            .sum();
        let launch_profile = select_cluster_launch_profile(
            cli.cluster_launch_profile,
            query_lifecycle_faults_enabled,
            cleanup_faults_enabled,
        )?;

        validate_dataset_launch_profile(&suite_names, &suite_configs, launch_profile)?;
        failure_artifacts.record_build_provenance(
            &base_dir,
            launch_profile,
            selected_cluster_mode,
            selected_cluster_size,
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
        let (test_sessions, controls) = if !cli.dry_run && control_plan::required(&prepared_inputs)
        {
            ensure!(
                launch_cluster_mode == ClusterMode::CrossProcess,
                "internal SQL controls require a runner-owned native cross-process launch"
            );
            let binary = cluster::discover_novarocks_binary(&base_dir)?;
            let auto_case_db = suite_configs
                .iter()
                .map(|(name, suite)| (name.clone(), suite.auto_case_db))
                .collect();
            match control_plan::build(
                &prepared_inputs,
                launch_cluster_size,
                cluster::startup_timeout(),
                &binary,
                &auto_case_db,
                &cli.target_session_sql,
                Some(fixture_budget.work_deadline()),
            )? {
                Some((plan, execution)) => {
                    failure_artifacts.record_provenance("selected-control-intents".into(),serde_json::json!({
                        "role_launches":plan.planned_launch_summaries(),
                        "remaining_original_budget_millis":execution.deadline.saturating_duration_since(Instant::now()).as_millis(),
                        "serial_role_order":execution.has_restarts,
                        "status":"frozen-intents-not-action-evidence",
                        "mysql_cumulative_slow_progress_handshake":"excluded-by-explicit-user-decision"}));
                    (Some(plan), Some(Arc::new(execution)))
                }
                None => (None, None),
            }
        } else {
            (None, None)
        };
        ensure!(
            controls
                .as_ref()
                .is_none_or(|controls| !controls.has_restarts || jobs == 1),
            "selected role restart/wipe controls require -j 1"
        );
        let fixture_budget = controls.as_ref().map_or_else(
            || fixture_budget.clone(),
            |controls| {
                fixture_budget
                    .clone()
                    .with_work_deadline_limit(controls.deadline)
            },
        );
        let server_handle = launch_server(
            launch_cluster_mode,
            launch_cluster_size,
            &base_dir,
            &runner_config,
            launch_profile,
            test_sessions,
            &fixture_budget,
        )?;
        let launched_target_port = server_handle.target_port();
        let launched_target_host = server_handle.target_host().map(ToOwned::to_owned);
        let server_handle = Arc::new(Mutex::new(server_handle));
        let primary_result = (|| -> Result<i32> {
            if let Some(evidence) = server_handle
                .lock()
                .map_err(|_| anyhow::anyhow!("server handle mutex poisoned"))?
                .actual_build_information_evidence()?
            {
                failure_artifacts
                    .record_provenance("actual-role-build-information".into(), evidence);
            }
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
                .or_else(|| env_optional("NOVAROCKS_SQL_TEST_HOST"))
                .or_else(|| runner_config.cluster.get("host").cloned())
                .unwrap_or_else(|| "127.0.0.1".to_string());
            let target_user = cli
                .user
                .clone()
                .or_else(|| env_optional("NOVAROCKS_SQL_TEST_USER"))
                .or_else(|| runner_config.cluster.get("user").cloned())
                .unwrap_or_else(|| "root".to_string());
            let target_password = cli
                .password
                .clone()
                .or_else(|| env_optional("NOVAROCKS_SQL_TEST_PASSWORD"))
                .or_else(|| runner_config.cluster.get("password").cloned());
            let target_mysql_bin = cli
                .mysql
                .clone()
                .unwrap_or_else(|| env_or_default("NOVAROCKS_SQL_TEST_MYSQL", "mysql"));

            let ref_host = cli
                .ref_host
                .clone()
                .or_else(|| env_optional("NOVAROCKS_SQL_TEST_REF_HOST"))
                .unwrap_or_else(|| "127.0.0.1".to_string());
            let ref_user = cli
                .ref_user
                .clone()
                .or_else(|| env_optional("NOVAROCKS_SQL_TEST_REF_USER"))
                .unwrap_or_else(|| "root".to_string());
            let ref_password = cli
                .ref_password
                .clone()
                .or_else(|| env_optional("NOVAROCKS_SQL_TEST_REF_PASSWORD"));
            let ref_mysql_bin = cli
                .ref_mysql
                .clone()
                .unwrap_or_else(|| env_or_default("NOVAROCKS_SQL_TEST_REF_MYSQL", "mysql"));

            // The execution pool consumes the frozen selection.
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(jobs)
                .build()
                .context("failed to build rayon thread pool")?;

            // Prepare all suites
            let mut prepared_suites: Vec<PreparedSuite> = Vec::new();

            for input in prepared_inputs {
                let PreparedSqlInputs {
                    suite_name,
                    sql_dir,
                    result_dir,
                    sql_glob,
                    suite_catalog_override,
                    target_db_default,
                    ref_db_default,
                    verify_enabled,
                    query_timeout,
                    cases,
                    load_failures,
                    suite_init_hook,
                    suite_cleanup_hook,
                } = input;
                let suite = suite_configs
                    .get(&suite_name)
                    .expect("selected suite already validated");
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
                    fixture_budget: fixture_budget.clone(),
                    fixture_storage: fixture_storage.clone(),
                    fixture_writers_settled: Arc::clone(&fixture_writers_settled),
                    namespace_catalogs: namespace_catalogs.clone(),
                    controls: controls.clone(),
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
                    rewrite_explain_contains_as_not_contains: cli
                        .rewrite_explain_contains_as_not_contains
                        .clone(),
                    marker_re: marker_re.clone(),
                    fail_fast: cli.fail_fast,
                    server_handle: Arc::clone(&server_handle),
                    failure_artifacts: Arc::clone(&failure_artifacts),
                    publication_catalog_control: publication_catalog_control.clone(),
                    publication_service_control: publication_service_control.clone(),
                    results: Some(Arc::new({
                        let (config, fact) = &suite_facts[&suite.name];
                        results_record::SuiteRecorder::new(
                            Arc::clone(&results_writer),
                            config.clone(),
                            fact.clone(),
                        )
                    })),
                };

                prepared_suites.push(PreparedSuite {
                    ctx,
                    cases,
                    load_failures,
                    init_hook: suite_init_hook,
                    cleanup_hook: suite_cleanup_hook,
                });
            }

            if cli.dry_run {
                // --dry-run is the case-validation path: unloadable case files must
                // fail it rather than being reported and then exiting zero.
                let unloadable = selected_load_failure_count;
                if unloadable > 0 {
                    println!("❌ ERROR: {unloadable} case file(s) failed to load");
                    return Ok(1);
                }
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
                if controls.is_some() && jobs == 1 {
                    // Frozen role generations follow the selected serial suite order.
                    prepared_suites
                        .iter()
                        .map(|ps| run_suite(ps, &abort, &stdout_lock))
                        .collect()
                } else {
                    prepared_suites
                        .par_iter()
                        .map(|ps| run_suite(ps, &abort, &stdout_lock))
                        .collect()
                }
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
                if !so.cleanup_errors.is_empty() {
                    failure_artifacts.record_provenance(
                        format!("suite-cleanup/{}", so.suite_name),
                        serde_json::json!(so.cleanup_errors),
                    );
                }
                all_cleanup_errors.extend(so.cleanup_errors.iter().cloned());
            }

            let total_cpu_time: Duration =
                all_case_timings.iter().map(|timing| timing.elapsed).sum();

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
            slowest_case_timings.sort_by_key(|timing| std::cmp::Reverse(timing.elapsed));
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

            *totals.lock().unwrap() = (grand_passed, grand_failed, grand_skipped);
            results_writer.check()?;
            if grand_failed > 0
                || !all_cleanup_errors.is_empty()
                || Instant::now() >= fixture_budget.deadline()
            {
                return Ok(1);
            }

            Ok(0)
        })();
        let result = finish_run_with_server_cleanup_recorded(
            server_handle,
            primary_result,
            failure_artifacts.as_ref(),
            Some(&results_writer),
            selected_cluster_mode == ClusterMode::AllInOne,
            Some(&server_writers_exited),
        );
        server_recorded.store(true, Ordering::Relaxed);
        result
    })();
    let mut fixture_failures = Vec::new();
    if let Some(fixture) = publication_catalog_fixture.as_mut()
        && let Err(error) = fixture.shutdown()
    {
        fixture_failures.push(format!("join publication fixture: {error:#}"));
    }
    // Consumers and proxy actors have actually exited before the physical fixture is reclaimed.
    if let Some(fixture) = isolated_rest_catalog.as_mut() {
        let workspace = fixture.workspace_root().to_path_buf();
        match fixture.shutdown() {
            Ok(()) => {
                if let Err(error) = fs::remove_dir_all(&workspace)
                    && error.kind() != std::io::ErrorKind::NotFound
                {
                    fixture_failures.push(format!(
                        "remove isolated fixture workspace {}: {error}",
                        workspace.display()
                    ));
                }
            }
            Err(error) => fixture_failures.push(format!("stop isolated fixture: {error:#}")),
        }
    }
    if let (Some(storage), Some(env_file)) = (&fixture_storage, &original_fixture_env) {
        if owns_fixture_run && !cli.dry_run && selected_cluster_mode != ClusterMode::AllInOne {
            if server_writers_exited.load(Ordering::Acquire)
                && fixture_writers_settled.load(Ordering::Acquire)
                && fixture_failures.is_empty()
            {
                match fixture_context::cleanup_run(&base_dir, env_file, &fixture_budget, storage) {
                    Ok(report) => {
                        failure_artifacts
                            .record_provenance("fixture-scratch-cleanup".into(), report);
                        if let Some(material) = original_fixture_material.take() {
                            let path = material.path().to_path_buf();
                            let removed = material.release_after_cleanup();
                            failure_artifacts.record_provenance(
                                "original-fixture-run-material-retirement".into(),
                                serde_json::json!({"directory": path, "removed": removed}),
                            );
                            if !removed {
                                eprintln!(
                                    "Original run cleanup succeeded; private recovery material remains at {}",
                                    path.display()
                                );
                            }
                        }
                    }
                    Err(error) => fixture_failures.push(format!("run scratch cleanup: {error:#}")),
                }
            } else if server_recorded.load(Ordering::Acquire) {
                fixture_failures.push("run scratch files retained: writer exit or private/proxy cleanup was not confirmed".into());
            }
        } else {
            failure_artifacts.record_provenance("fixture-scratch-cleanup".into(), serde_json::json!({
                "status":"deferred", "owner":if cli.dry_run {"dry-run"} else if !owns_fixture_run {"original-outer-run"} else {"external-server"}, "objects_reclaimed":false
            }));
        }
    }
    if selection_written.load(Ordering::Relaxed) && !server_recorded.load(Ordering::Relaxed) {
        results_writer.write(serde_json::json!({"kind":"server","cleanup":{"status":"not_launched","error":null,"residual_pids":[]}}))?;
    }
    let resources = match &fixture_root {
        Some(root) => novarocks_cluster_harness::resource_registration::collect(
            root,
            &resources_before,
            std::process::id(),
        )?,
        None => Vec::new(),
    };
    results_writer.write(serde_json::json!({"kind":"fixture","start":if isolated_rest_catalog.is_some(){"ok"}else{"not_needed"},"error":null,
        "shutdown":{"status":if fixture_failures.is_empty(){"ok"}else{"failed"},"errors":fixture_failures},"resources":resources}))?;
    results_writer.check()?;
    if fixture_failures.is_empty() {
        let code = match &fixture_primary {
            Ok(code) => *code,
            Err(_) => 1,
        };
        let (pass, fail, not_run) = *totals.lock().unwrap();
        results_writer.write(serde_json::json!({"kind":"end","completed":true,"exit_code":code,"totals":{"pass":pass,"fail":fail,"not_run":not_run},"ended_at":results_record::now()}))?;
        return fixture_primary;
    }
    failure_artifacts.record_provenance(
        "fixture-cleanup".into(),
        serde_json::json!(&fixture_failures),
    );
    let cleanup = fixture_failures.join("; ");
    let (pass, fail, not_run) = *totals.lock().unwrap();
    results_writer.write(serde_json::json!({"kind":"end","completed":true,"exit_code":1,"totals":{"pass":pass,"fail":fail,"not_run":not_run},"ended_at":results_record::now()}))?;
    match fixture_primary {
        Ok(code) => Err(anyhow::anyhow!(
            "run exited with code {code}; fixture cleanup failed: {cleanup}"
        )),
        Err(primary) => Err(anyhow::anyhow!(
            "{primary:#}; fixture cleanup failed: {cleanup}"
        )),
    }
}

/// The suites that must not share the ordinary `docker/iceberg-rest`
/// environment.
///
/// Worktrees bound to the same runtime contract share one REST Catalog
/// container and therefore one catalog database.
/// Isolation there reaches the object-store prefix and the generated names,
/// but not the namespace listing: every attachment enumerates every worktree's
/// tables. A suite that restarts the frontend and lets it rediscover its own
/// materialized views from the lake cannot tolerate that -- it adopts the other
/// worktrees' views as well, which is correct behaviour against a catalog that
/// really does hold them. Even without a restart, the catalog-drop guard sees
/// those foreign MV references and refuses cleanup. Such a suite gets its own
/// REST Catalog so its outcome does not depend on other worktrees.
const ISOLATED_REST_CATALOG_SUITES: &[&str] = &[
    "iceberg-ivm",
    "iceberg-mv-apply",
    "iceberg-mv-scheduler",
    "lnp-3a-mv-rebuild",
    "lnp-3d-mv-accelerator",
    "mv-storage-contract",
    "mv-storage-physical-occ",
    "mv-publication-v11",
    "mv-rewrite",
];

fn isolated_rest_catalog_suite(suite: &str) -> bool {
    ISOLATED_REST_CATALOG_SUITES.contains(&suite)
}

/// Starts a private REST Catalog and MinIO for the selected suites. Its caller
/// projects the resulting endpoints before launching any consumer.
///
/// Returning no fixture or control URI is the ordinary shared-runtime case.
fn start_isolated_rest_catalog(
    selected_suites: &[String],
    budget: &novarocks_cluster_harness::fixture_run::RunBudget,
) -> Result<(
    Option<novarocks_cluster_harness::isolated_iceberg_rest::IsolatedIcebergRestFixture>,
    Option<String>,
)> {
    use novarocks_cluster_harness::isolated_iceberg_rest::IsolatedIcebergRestFixture;
    if !selected_suites
        .iter()
        .any(|suite| isolated_rest_catalog_suite(suite.as_str()))
    {
        return Ok((None, None));
    }
    if let Some(disallowed) = selected_suites
        .iter()
        .find(|suite| !isolated_rest_catalog_suite(suite.as_str()))
    {
        bail!(
            "an isolated REST catalog replaces the shared one for the whole run; selected suite {disallowed} expects the shared docker/iceberg-rest environment and cannot share this run"
        );
    }
    let scenario_root = resolve_repo_root()?.join("tests/sql/.runtime/isolated-rest");
    std::fs::create_dir_all(&scenario_root).with_context(|| {
        format!(
            "create isolated REST catalog root {}",
            scenario_root.display()
        )
    })?;
    println!("→ starting an isolated Iceberg REST catalog for this run");
    let (fixture, control_uri) = IsolatedIcebergRestFixture::start_private_with_budget(
        &scenario_root,
        budget,
        selected_suites
            .iter()
            .any(|suite| suite == "mv-publication-v11"),
    )?;
    println!(
        "  isolated REST catalog {} (compose project {})",
        fixture.endpoints().rest_uri,
        fixture.endpoints().compose_project
    );
    Ok((Some(fixture), control_uri))
}

/// The suites the transparent publication-catalog fixture serves.
///
/// It can consume one bounded fault token at a REST stage-create or
/// table-commit boundary, which is an acceptance-only capability: a suite that
/// uses it owns a runner-owned cluster and cannot share a run with an ordinary
/// suite.
const PUBLICATION_CATALOG_FIXTURE_SUITES: &[&str] = &[
    "lake-publication",
    "lnp-3d-mv-accelerator",
    "mv-storage-contract",
    "mv-storage-physical-occ",
    "mv-publication-v11",
];

fn publication_catalog_fixture_suite(suite: &str) -> bool {
    PUBLICATION_CATALOG_FIXTURE_SUITES.contains(&suite)
}

fn start_publication_catalog_fixture(
    runner_config: &mut RunnerConfig,
    selected_suites: &[String],
) -> Result<Option<publication_catalog::FixtureHandle>> {
    if !selected_suites
        .iter()
        .any(|suite| publication_catalog_fixture_suite(suite.as_str()))
    {
        return Ok(None);
    }
    if let Some(disallowed) = selected_suites
        .iter()
        .find(|suite| !publication_catalog_fixture_suite(suite.as_str()))
    {
        bail!(
            "publication catalog fixture is acceptance-only; selected suite {disallowed} cannot run with a lake publication acceptance suite"
        );
    }
    let downstream = runner_config
        .values
        .get("iceberg_rest_uri")
        .cloned()
        .filter(|uri| !uri.trim().is_empty())
        .context("lake publication acceptance requires iceberg_rest_uri")?;
    let fixture = publication_catalog::FixtureHandle::start(downstream)?;
    // This fault proxy overlays SQL/engine transport only. Provider subprocesses
    // keep the physical publication's environment and container-side Spark URI.
    project_fixture_value(runner_config, "iceberg_rest_uri", fixture.uri());
    Ok(Some(fixture))
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
    use crate::sql_error_codes::SqlErrorPhase;
    use crate::types::{
        ConnectionConfig, PublicationCatalogAction, PublicationCatalogFault,
        PublicationCatalogFaultDirective, PublicationCatalogFaultTarget, QueryExecution,
        QueryLifecycleStructuredAssertion, QueryMeta, ResultSet, RuntimeFilterDetailExpectation,
        SqlCase, SqlErrorLocation, SqlErrorTier, SqlStep, SuiteHook,
    };
    use crate::{
        AlterJobPollState, CaseStatus, Cli, Mode, RecordFrom,
        annotate_failure_with_engine_error_code, bounded_fault_query_timeout,
        classify_alter_job_poll, evaluate_expected_error_branch,
        evaluate_expected_error_branch_with_sql_error_descriptors, execute_target_session_sql_with,
        expected_engine_error_code_diff_result, expected_engine_error_code_result,
        finish_expected_error_step, sql_text_has_query_lifecycle_fault_directive,
        statement_starts_dml_operation, validate_dml_cluster_jobs, validate_fault_injection_jobs,
        validate_lake_publication_preflight, validate_lnp_3d_mv_accelerator_preflight,
        validate_publication_catalog_directives, validate_selected_suite_cluster,
        verify_runtime_filter_structured_assertion,
    };
    use clap::Parser;
    use regex::Regex;
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    #[test]
    fn publication_fault_after_exit_timing_rejects_orphan_and_wrong_restart_shapes() {
        use crate::types::PublicationCatalogFaultTiming;
        let mut case = test_case_with_meta(QueryMeta {
            publication_catalog_fault_timing: Some(
                PublicationCatalogFaultTiming::AfterFrontendExit,
            ),
            ..Default::default()
        });
        case.sequential = true;
        let valid = |case: &SqlCase, cluster_mode, mode| {
            validate_publication_catalog_directives(
                "lnp-3d-mv-accelerator",
                std::slice::from_ref(case),
                true,
                cluster_mode,
                mode,
            )
        };
        assert!(valid(&case, ClusterMode::CrossProcess, Mode::Verify).is_err());
        case.steps[0].meta.publication_catalog_fault = Some(PublicationCatalogFaultDirective {
            action: PublicationCatalogAction::TableLoad,
            fault: PublicationCatalogFault::CorruptPackage,
        });
        case.steps[0].meta.publication_catalog_fault_target = Some(PublicationCatalogFaultTarget {
            namespace: "owned_ns".into(),
            table: "a_mv".into(),
        });
        assert!(valid(&case, ClusterMode::CrossProcess, Mode::Verify).is_err());
        case.steps[0].meta.restart_fe_after_step = true;
        assert!(valid(&case, ClusterMode::CrossProcess, Mode::Verify).is_ok());
        assert!(valid(&case, ClusterMode::AllInOne, Mode::Verify).is_err());
        assert!(valid(&case, ClusterMode::CrossProcess, Mode::Record).is_err());
        let mut no_target = case.clone();
        no_target.steps[0].meta.publication_catalog_fault_target = None;
        assert!(valid(&no_target, ClusterMode::CrossProcess, Mode::Verify).is_err());
        let mut wrong_fault = case.clone();
        wrong_fault.steps[0].meta.publication_catalog_fault =
            Some(PublicationCatalogFaultDirective {
                action: PublicationCatalogAction::TableCommit,
                fault: PublicationCatalogFault::AfterCommitBeforeResponse,
            });
        assert!(valid(&wrong_fault, ClusterMode::CrossProcess, Mode::Verify).is_err());
        case.steps[0].meta.publication_catalog_fault_timing = None;
        case.steps[0].meta.restart_fe_after_step = false;
        assert!(valid(&case, ClusterMode::CrossProcess, Mode::Verify).is_ok());
    }

    #[test]
    fn unsupported_restart_owner_never_arms_an_after_exit_fault() {
        let mut server = ArtifactCapableServer;
        let mut called = false;
        let mut after_exit = || {
            called = true;
            Ok(())
        };
        assert!(
            crate::cluster::ServerHandle::restart_fe_with_after_exit_hook(
                &mut server,
                &mut after_exit,
            )
            .is_err()
        );
        assert!(!called);
    }

    #[test]
    fn actual_unconsumed_fault_finish_retains_restart_failure_and_releases_the_arm() {
        let mut fixture =
            super::publication_catalog::FixtureHandle::start("http://127.0.0.1:9".into()).unwrap();
        let control = fixture.control().unwrap();
        let mut guard = Some(
            control
                .arm_next_for_table("table-load", "corrupt-package", "owned_ns", "a_mv")
                .unwrap(),
        );
        let mut evidence = crate::results_record::CaseEvidence::default();
        evidence.record_independent_failure(Some(2), "original restart failure".into());
        let mut log = String::new();
        assert!(!super::finish_publication_catalog_fault(
            &mut guard,
            2,
            &mut evidence,
            &mut log,
        ));
        assert!(guard.is_none());
        let failure = evidence.failure.as_ref().unwrap();
        assert_eq!(
            failure["primary_failure"]["error"],
            "original restart failure"
        );
        assert_eq!(failure["additional_failures"][0]["cleanup"], true);
        assert!(log.contains("publication catalog fault cleanup"));
        // A failed match oracle still completed the original control release.
        let unused = control.arm_next("table-load", "corrupt-package").unwrap();
        assert!(unused.finish().is_err());
        fixture.shutdown().unwrap();
    }

    #[test]
    fn compiled_execution_identity_manifest_is_exact_and_rejects_bad_entries() {
        let descriptors = super::parse_sql_error_manifest(
            "schema_version=1\ncode\tphase\nUnsupportedCompiledShape\tExecution\n",
        )
        .unwrap();
        assert_eq!(
            descriptors,
            [crate::sql_error_codes::SqlErrorDescriptor {
                code: "UnsupportedCompiledShape",
                phase: SqlErrorPhase::Execution,
            }]
        );
        let production = super::production_sql_error_descriptors();
        assert_eq!(
            production
                .iter()
                .filter(|entry| entry.code == "UnsupportedCompiledShape")
                .count(),
            1,
            "the generated production manifest must register the exact identity once",
        );
        assert_eq!(
            crate::sql_error_codes::lookup_sql_error_descriptor(
                production,
                "UnsupportedCompiledShape"
            )
            .unwrap()
            .phase,
            SqlErrorPhase::Execution,
        );
        assert_eq!(
            crate::runner::extract_sql_error_code(
                "ERROR 1105 (HY000): [UnsupportedCompiledShape] FULL OUTER hash join",
            )
            .as_deref(),
            Some("UnsupportedCompiledShape"),
        );
        for invalid in [
            "schema_version=1\ncode\tphase\nUnsupportedCompiledShape\tAnalyze\n",
            "schema_version=1\ncode\tphase\nUnsupportedCompiledShape\tParse\n",
            "schema_version=1\ncode\tphase\nCommitUnknown\tExecution\n",
            "schema_version=1\ncode\tphase\nUnsupportedcompiledShape\tExecution\n",
            "schema_version=1\ncode\tphase\nUnsupportedCompiledShapeX\tExecution\n",
            "schema_version=1\ncode\tphase\nUnsupportedCompiledShape\tExecution\nUnsupportedCompiledShape\tExecution\n",
            "schema_version=1\ncode\tphase\nUnsupportedCompiledShape\tExecution\textra\n",
        ] {
            assert!(
                super::parse_sql_error_manifest(invalid).is_err(),
                "{invalid}"
            );
        }
    }

    fn test_runtime_dir() -> PathBuf {
        let dir = crate::resolve_repo_root()
            .expect("repo root")
            .join("tests/sql/.runtime/unit");
        fs::create_dir_all(&dir).expect("create test runtime dir");
        dir
    }

    fn temp_result_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before unix epoch")
            .as_nanos();
        test_runtime_dir().join(format!(
            "novarocks_sql_test_{}_{}_{}.result",
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
            expected_variables: Default::default(),
            namespace_rewrites: Default::default(),
            auto_db: String::new(),
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

    fn test_case_with_sql(sql: &str) -> SqlCase {
        SqlCase {
            expected_variables: Default::default(),
            namespace_rewrites: Default::default(),
            auto_db: String::new(),
            source_file: PathBuf::from("dml.sql"),
            case_id: "dml_case".to_string(),
            steps: vec![SqlStep {
                query_number: 1,
                sql: sql.to_string(),
                meta: QueryMeta::default(),
            }],
            case_dbs: vec![],
            sequential: false,
        }
    }

    #[test]
    fn exact_publication_target_requires_the_original_table_load_fault() {
        let mut case = test_case_with_meta(QueryMeta {
            publication_catalog_fault_target: Some(PublicationCatalogFaultTarget {
                namespace: "owned_ns".into(),
                table: "a_mv".into(),
            }),
            ..QueryMeta::default()
        });
        case.sequential = true;
        assert!(
            validate_publication_catalog_directives(
                "lnp-3d-mv-accelerator",
                std::slice::from_ref(&case),
                true,
                ClusterMode::CrossProcess,
                Mode::Verify
            )
            .is_err()
        );
        case.steps[0].meta.publication_catalog_fault = Some(PublicationCatalogFaultDirective {
            action: PublicationCatalogAction::TableCommit,
            fault: PublicationCatalogFault::AfterCommitBeforeResponse,
        });
        assert!(
            validate_publication_catalog_directives(
                "lnp-3d-mv-accelerator",
                std::slice::from_ref(&case),
                true,
                ClusterMode::CrossProcess,
                Mode::Verify
            )
            .is_err()
        );
        case.steps[0].meta.publication_catalog_fault = Some(PublicationCatalogFaultDirective {
            action: PublicationCatalogAction::TableLoad,
            fault: PublicationCatalogFault::CorruptPackage,
        });
        assert!(
            validate_publication_catalog_directives(
                "lnp-3d-mv-accelerator",
                std::slice::from_ref(&case),
                true,
                ClusterMode::CrossProcess,
                Mode::Verify
            )
            .is_ok()
        );
        assert!(
            validate_publication_catalog_directives(
                "lnp-3d-mv-accelerator",
                std::slice::from_ref(&case),
                false,
                ClusterMode::CrossProcess,
                Mode::Verify
            )
            .is_err()
        );
    }

    #[test]
    fn dml_statements_are_recognized_through_leading_comments() {
        for sql in [
            "DELETE FROM t WHERE id = 1",
            "  update t set v = 1",
            "-- a comment\nMERGE INTO t USING s ON t.id = s.id",
            "-- one\n-- two\nINSERT INTO t VALUES (1)",
            "ALTER TABLE t ADD EQUALITY DELETE (id) VALUES (1)",
        ] {
            assert!(
                statement_starts_dml_operation(sql),
                "should be treated as DML: {sql}"
            );
        }
        for sql in [
            "SELECT 1",
            "-- delete is only mentioned here\nSELECT * FROM t",
            "CREATE TABLE t (id INT)",
            "DROP TABLE IF EXISTS t",
        ] {
            assert!(
                !statement_starts_dml_operation(sql),
                "should not be treated as DML: {sql}"
            );
        }
    }

    #[test]
    fn dml_suites_require_serial_jobs_on_a_cross_process_cluster() {
        let dml = vec![test_case_with_sql("DELETE FROM t WHERE id = 1")];

        let err = validate_dml_cluster_jobs(&dml, 4, ClusterMode::CrossProcess)
            .expect_err("parallel DML on a shared cluster must be rejected");
        assert!(
            err.to_string().contains("CoordinationUnresolved"),
            "the error should name the symptom it prevents: {err}"
        );

        validate_dml_cluster_jobs(&dml, 1, ClusterMode::CrossProcess)
            .expect("serial jobs are the supported way to run a DML suite");
        // Only the shared cross-process cluster has one coordination journal to
        // contend over; all-in-one runs get their own.
        validate_dml_cluster_jobs(&dml, 4, ClusterMode::AllInOne)
            .expect("all-in-one has no shared journal to contend over");
        validate_dml_cluster_jobs(
            &[test_case_with_sql("SELECT 1")],
            8,
            ClusterMode::CrossProcess,
        )
        .expect("read-only suites stay parallel");
    }

    #[test]
    fn alter_job_poll_uses_structured_state_and_preserves_failure_message() {
        let execution = |state: &str, message: &str| QueryExecution {
            header: vec!["JobId".to_string(), "State".to_string(), "Msg".to_string()],
            rows: vec![vec![
                "1".to_string(),
                state.to_string(),
                message.to_string(),
            ]],
            text_output: format!("JobId\tState\tMsg\n1\t{state}\t{message}"),
            elapsed: Duration::ZERO,
        };

        assert_eq!(
            classify_alter_job_poll(&execution("RUNNING", "")),
            AlterJobPollState::Pending
        );
        assert_eq!(
            classify_alter_job_poll(&execution("FINISHED", "done")),
            AlterJobPollState::Finished
        );
        assert_eq!(
            classify_alter_job_poll(&execution("FAILED", "exact engine failure")),
            AlterJobPollState::Failed("exact engine failure".to_string())
        );
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
    fn lifecycle_fault_preflight_matches_faults_and_lifecycle_evidence() {
        assert!(sql_text_has_query_lifecycle_fault_directive(
            "-- @query_lifecycle_fault=runtime-filter-contribution-ack-drop,1\nSELECT 1;"
        ));
        assert!(sql_text_has_query_lifecycle_fault_directive(
            "-- @kill_query_after_be_log_contains=NOVAROCKS_TASK_CREATE_APPLIED\nSELECT 1;"
        ));
        assert!(sql_text_has_query_lifecycle_fault_directive(
            "-- @query_control_fragment_backend_limit=2\nSELECT 1;"
        ));
        assert!(sql_text_has_query_lifecycle_fault_directive(
            "-- @restart_be_after_establish_context_index=1\nSELECT 1;"
        ));
        assert!(sql_text_has_query_lifecycle_fault_directive(
            "-- @be_log_be_count_at_least=NOVAROCKS_QUERY_LIFECYCLE_TERMINATED,3\nSELECT 1;"
        ));
        for directive in [
            "expect_runtime_filter_available=available",
            "expect_runtime_filter_detail=completed-channel",
            "expect_runtime_filter_total_at_least=transport_acked_count,1",
        ] {
            assert!(
                sql_text_has_query_lifecycle_fault_directive(&format!(
                    "-- @{directive}\nSELECT 1;"
                )),
                "structured runtime-filter directive must enable lifecycle debug scope: {directive}"
            );
        }
        assert!(!sql_text_has_query_lifecycle_fault_directive(
            "-- query-control-heartbeat-loss is only a case name\nSELECT 1;"
        ));
    }

    #[test]
    fn delivered_consumer_assertion_does_not_require_apply() {
        use novarocks_cluster_harness as harness;

        let snapshot = harness::QueryLifecycleStructuredSnapshot {
            execution_id: Some("1:2:3".to_string()),
            process_namespace: 1,
            local_sequence: 2,
            attempt_id: 3,
            failure_fact: None,
            failure_detail: None,
            failure_site: None,
            worker_contexts_converged: false,
            runtime_filter: harness::RuntimeFilterTerminalRollup::Available {
                participants: vec![harness::RuntimeFilterParticipantTerminalTelemetry {
                    participant: harness::RuntimeFilterTerminalParticipant {
                        process_id: "018f3d8a-2b4c-7d6e-8f90-123456789abc"
                            .parse()
                            .expect("fixture UUIDv7 backend process id"),
                    },
                    telemetry: harness::RuntimeFilterParticipantTerminalTelemetryValue::Available(
                        harness::RuntimeFilterParticipantTerminalDetails {
                            channels: Vec::new(),
                            producer_streams: Vec::new(),
                            transport_routes: Vec::new(),
                            consumers: vec![harness::RuntimeFilterConsumerTerminalDetail {
                                channel_binding_id: 1,
                                channel_id: 2,
                                consumer_binding_id: 3,
                                fragment_instance_id: None,
                                latest_delivered_logical_version: Some(1),
                                latest_applied_logical_version: None,
                                subscription_terminal:
                                    harness::RuntimeFilterSubscriptionTerminal::Completed,
                                row_evaluations: 0,
                                input_rows: 0,
                                output_rows: 0,
                            }],
                        },
                    ),
                }],
                totals: harness::RuntimeFilterTerminalTotalsTelemetry::Unavailable(
                    harness::RuntimeFilterTerminalTotalsUnavailable::ParticipantTelemetryUnavailable,
                ),
            },
        };
        let delivered = QueryLifecycleStructuredAssertion {
            runtime_filter_availability: None,
            runtime_filter_details: vec![RuntimeFilterDetailExpectation::DeliveredConsumer],
            runtime_filter_totals_at_least: Vec::new(),
        };
        assert!(verify_runtime_filter_structured_assertion(&delivered, &snapshot).is_ok());

        let delivered_and_applied = QueryLifecycleStructuredAssertion {
            runtime_filter_details: vec![RuntimeFilterDetailExpectation::DeliveredAppliedConsumer],
            ..delivered
        };
        assert!(
            verify_runtime_filter_structured_assertion(&delivered_and_applied, &snapshot).is_err()
        );
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
    fn lake_publication_suites_require_native_serial_selection() {
        for suite in ["lake-publication", "lnp-3a-mv-rebuild"] {
            let suites = vec![suite.to_string()];
            validate_lake_publication_preflight(&suites, ClusterMode::CrossProcess, 3, 1)
                .expect("canonical native selection");
            for (mode, size, jobs) in [
                (ClusterMode::AllInOne, 1, 1),
                (ClusterMode::CrossProcess, 2, 1),
                (ClusterMode::CrossProcess, 3, 2),
            ] {
                assert!(
                    validate_lake_publication_preflight(&suites, mode, size, jobs).is_err(),
                    "noncanonical {suite} selection must fail"
                );
            }
        }
    }

    #[test]
    fn lnp_3d_mv_accelerator_requires_native_serial_selection() {
        let suites = vec!["lnp-3d-mv-accelerator".to_string()];
        validate_lnp_3d_mv_accelerator_preflight(&suites, ClusterMode::CrossProcess, 3, 1)
            .expect("canonical native selection");
        for (mode, size, jobs) in [
            (ClusterMode::AllInOne, 1, 1),
            (ClusterMode::CrossProcess, 2, 1),
            (ClusterMode::CrossProcess, 3, 2),
        ] {
            assert!(
                validate_lnp_3d_mv_accelerator_preflight(&suites, mode, size, jobs).is_err(),
                "noncanonical selection must fail"
            );
        }
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
    fn sql_error_assertions_match_code_phase_and_location() {
        let meta = QueryMeta {
            expect_sql_code: Some("sql.parse.unsupported_statement".to_string()),
            expect_sql_phase: Some(SqlErrorPhase::Parse),
            expect_error_at: Some(SqlErrorLocation {
                line: 7,
                column: 11,
            }),
            expect_error_tier: Some(SqlErrorTier::Target),
            ..QueryMeta::default()
        };
        let error = "ERROR 1064 (42000): [sql.parse.unsupported_statement] at line 7, column 11: unsupported statement";

        assert_eq!(
            evaluate_expected_error_branch_with_sql_error_descriptors(
                &meta,
                false,
                error,
                crate::production_sql_error_descriptors(),
            ),
            Some(Ok(()))
        );
    }

    #[test]
    fn sql_error_assertions_report_code_phase_and_location_mismatches() {
        let code_meta = QueryMeta {
            expect_sql_code: Some("sql.test.fixture".to_string()),
            ..QueryMeta::default()
        };
        let error = evaluate_expected_error_branch_with_sql_error_descriptors(
            &code_meta,
            false,
            "[sql.test.analyze] wrong code",
            crate::sql_error_codes::TEST_SQL_ERROR_DESCRIPTORS,
        )
        .expect("SQL assertion branch should be active")
        .expect_err("different code must fail");
        assert!(error.contains("expected SQL error code"), "{error}");

        let phase_meta = QueryMeta {
            expect_sql_code: Some("sql.test.analyze".to_string()),
            expect_sql_phase: Some(SqlErrorPhase::Parse),
            ..QueryMeta::default()
        };
        let error = evaluate_expected_error_branch_with_sql_error_descriptors(
            &phase_meta,
            false,
            "[sql.test.analyze] wrong phase",
            crate::sql_error_codes::TEST_SQL_ERROR_DESCRIPTORS,
        )
        .expect("SQL assertion branch should be active")
        .expect_err("different phase must fail");
        assert!(error.contains("expected SQL error phase Parse"), "{error}");

        let location_meta = QueryMeta {
            expect_error_at: Some(SqlErrorLocation { line: 2, column: 3 }),
            ..QueryMeta::default()
        };
        let error = evaluate_expected_error_branch_with_sql_error_descriptors(
            &location_meta,
            false,
            "[sql.test.fixture] at line 2, column 4: wrong location",
            crate::sql_error_codes::TEST_SQL_ERROR_DESCRIPTORS,
        )
        .expect("SQL assertion branch should be active")
        .expect_err("different location must fail");
        assert!(error.contains("expected error at 2:3"), "{error}");
    }

    #[test]
    fn sql_error_assertion_fails_when_query_succeeds() {
        let meta = QueryMeta {
            expect_sql_code: Some("sql.test.fixture".to_string()),
            ..QueryMeta::default()
        };

        let error = evaluate_expected_error_branch_with_sql_error_descriptors(
            &meta,
            true,
            "",
            crate::sql_error_codes::TEST_SQL_ERROR_DESCRIPTORS,
        )
        .expect("SQL assertion branch should be active")
        .expect_err("unexpected success must fail");
        assert!(
            error.contains("expected SQL error code \"sql.test.fixture\", but query succeeded"),
            "{error}"
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
    fn failure_log_includes_sql_error_code_when_available() {
        let msg = "target execute failed: FAIL (0.00s): ERROR 1105 (HY000): [sql.analyze.unavailable_implementation] compiled implementation unavailable";

        assert_eq!(
            annotate_failure_with_engine_error_code(msg, msg),
            format!("sql_error_code=sql.analyze.unavailable_implementation {msg}")
        );
        assert_eq!(
            annotate_failure_with_engine_error_code(
                "target execute failed",
                "ERROR 1105 (HY000): [sql.analyze.unavailable_implementation] compiled implementation unavailable"
            ),
            "sql_error_code=sql.analyze.unavailable_implementation target execute failed"
        );
    }

    #[test]
    fn failure_log_preserves_existing_code_tags_without_duplicates() {
        for msg in [
            "engine_error_code=QueryTimeout target execute failed: [QueryTimeout] slow query",
            "sql_error_code=sql.analyze.unavailable_implementation target execute failed",
            "engine_error_code=UnsupportedCompiledShape sql_error_code=sql.analyze.unavailable_implementation target execute failed",
        ] {
            assert_eq!(annotate_failure_with_engine_error_code(msg, msg), msg);
        }
    }

    #[test]
    fn failure_log_adds_missing_code_tag_without_replacing_existing_tag() {
        let msg = "engine_error_code=UnsupportedCompiledShape target execute failed";
        let annotated = annotate_failure_with_engine_error_code(
            msg,
            "[sql.analyze.unavailable_implementation] compiled implementation unavailable",
        );
        assert_eq!(
            annotated,
            format!("sql_error_code=sql.analyze.unavailable_implementation {msg}")
        );
        assert_eq!(
            annotate_failure_with_engine_error_code(&annotated, &annotated),
            annotated
        );

        let msg = "sql_error_code=sql.analyze.unavailable_implementation target execute failed";
        assert_eq!(
            annotate_failure_with_engine_error_code(
                msg,
                "[UnsupportedCompiledShape] unsupported plan"
            ),
            format!("engine_error_code=UnsupportedCompiledShape {msg}")
        );
    }

    #[test]
    fn failure_log_leaves_plain_errors_unclassified() {
        let msg = "target execute failed: plain execution error";

        assert_eq!(annotate_failure_with_engine_error_code(msg, msg), msg);
    }

    struct MissingBeLogEvidenceServer;

    impl crate::cluster::ServerHandle for MissingBeLogEvidenceServer {
        fn target_host(&self) -> Option<&str> {
            Some("127.0.0.1")
        }

        fn target_port(&self) -> Option<u16> {
            Some(9030)
        }

        fn be_count(&self) -> usize {
            1
        }

        fn be_log_count(&self, _index: usize, _needle: &str) -> anyhow::Result<usize> {
            Ok(0)
        }
    }

    #[test]
    fn expected_error_be_log_directive_failure_prevents_pass_marker() {
        let step = SqlStep {
            query_number: 1,
            sql: "SELECT rejected".to_string(),
            meta: QueryMeta {
                expect_error: Some("planned rejection".to_string()),
                be_log_contains: vec!["be_log_ingress rejected".to_string()],
                ..QueryMeta::default()
            },
        };
        let server_handle: Arc<Mutex<Box<dyn crate::cluster::ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(MissingBeLogEvidenceServer)));
        let mut log = String::new();

        let passed = finish_expected_error_step(
            &step,
            true,
            "planned rejection",
            &server_handle,
            &crate::be_log_directive::BeLogSnapshot::default(),
            &mut log,
        );

        assert!(!passed, "directive failure must fail the step");
        assert!(log.contains("BE log directive failed"), "{log}");
        assert!(!log.contains("PASS (expected error matched)"), "{log}");
    }

    struct PresentBeLogEvidenceServer(MissingBeLogEvidenceServer);

    impl crate::cluster::ServerHandle for PresentBeLogEvidenceServer {
        fn target_host(&self) -> Option<&str> {
            self.0.target_host()
        }

        fn target_port(&self) -> Option<u16> {
            self.0.target_port()
        }

        fn be_count(&self) -> usize {
            self.0.be_count()
        }

        fn be_log_count(&self, _index: usize, _needle: &str) -> anyhow::Result<usize> {
            Ok(1)
        }
    }

    #[test]
    fn expected_error_runs_be_log_directives_before_pass_marker() {
        let step = SqlStep {
            query_number: 1,
            sql: "SELECT rejected".to_string(),
            meta: QueryMeta {
                expect_error_code: Some("ProtocolDecodeError".to_string()),
                be_log_contains: vec!["be_log_ingress rejected".to_string()],
                ..QueryMeta::default()
            },
        };
        let server_handle: Arc<Mutex<Box<dyn crate::cluster::ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(PresentBeLogEvidenceServer(
                MissingBeLogEvidenceServer,
            ))));
        let mut log = String::new();

        assert!(finish_expected_error_step(
            &step,
            true,
            "[ProtocolDecodeError] planned rejection",
            &server_handle,
            &crate::be_log_directive::BeLogSnapshot::default(),
            &mut log,
        ));

        let directive = log.find("@be_log_contains PASS").expect("directive PASS");
        let expected_error = log
            .find("PASS (expected error matched)")
            .expect("expected-error PASS");
        assert!(directive < expected_error, "{log}");
    }

    #[test]
    fn correctness_cli_refuses_retired_measurement_options() {
        let help = <crate::Cli as clap::CommandFactory>::command()
            .render_long_help()
            .to_string();

        assert!(!help.contains("--no-auto-bootstrap-benchmark-data"));
        assert!(!help.contains("--benchmark-scale <BENCHMARK_SCALE>"));
        assert!(!help.contains("--benchmark-bootstrap-rebuild"));
        assert!(help.contains("--list-suites"));
        for option in [
            "--case-timing-output",
            "--process-resource-output",
            "--benchmark-warehouse",
            "--benchmark-profile-dir",
            "--benchmark-external-cluster",
            "--benchmark-skip-init",
            "--benchmark-skip-cleanup",
            "--benchmark-cleanup-only",
            "--benchmark-run-id",
            "--no-auto-bootstrap-benchmark-data",
            "--benchmark-scale",
            "--benchmark-bootstrap-rebuild",
        ] {
            let error =
                crate::Cli::try_parse_from(["novarocks-sql-test", "--suite", "decimal", option])
                    .expect_err("retired measurement options must fail during argument parsing");
            assert_eq!(
                error.kind(),
                clap::error::ErrorKind::UnknownArgument,
                "{option}"
            );
        }
    }

    #[test]
    fn retired_physical_wire_candidate_cli_is_rejected() {
        use clap::Parser;
        let ordinary = crate::Cli::try_parse_from(["runner", "--suite", "filter"]);
        assert!(ordinary.is_ok());
        let error = crate::Cli::try_parse_from([
            "runner",
            "--suite",
            "filter",
            "--physical-wire-candidate-binary",
            "/retired",
            "--cluster-mode",
            "cross-process",
            "--mode",
            "verify",
        ])
        .unwrap_err();
        assert_eq!(error.kind(), clap::error::ErrorKind::UnknownArgument);
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
    fn release_cluster_launch_profile_preserves_fault_gates() {
        use novarocks_cluster_harness::LaunchProfile;
        let cli = crate::Cli::try_parse_from([
            "novarocks-sql-test",
            "--suite",
            "decimal",
            "--cluster-launch-profile",
            "performance",
        ])
        .expect("parse release-compatible launch profile");
        assert_eq!(cli.cluster_launch_profile, Some(LaunchProfile::Performance));
        assert_eq!(
            super::select_cluster_launch_profile(cli.cluster_launch_profile, false, false,)
                .unwrap(),
            LaunchProfile::Performance
        );
        for (query_faults, cleanup_faults) in [(true, false), (false, true), (true, true)] {
            assert!(
                super::select_cluster_launch_profile(
                    cli.cluster_launch_profile,
                    query_faults,
                    cleanup_faults,
                )
                .is_err()
            );
        }
        assert_eq!(
            super::select_cluster_launch_profile(None, false, false).unwrap(),
            LaunchProfile::FaultScenario
        );
    }

    #[test]
    fn cli_allows_extension_listing_without_a_suite() {
        let cli = crate::Cli::try_parse_from(["novarocks-sql-test", "--list-extensions"])
            .expect("extension listing should not require a suite");

        assert!(cli.list_extensions);
        assert_eq!(cli.suite, None);
    }

    #[test]
    fn cli_allows_correctness_suite_listing_without_a_suite() {
        let cli = crate::Cli::try_parse_from(["novarocks-sql-test", "--list-suites"])
            .expect("suite listing should not require a suite");

        assert!(cli.list_suites);
        assert_eq!(cli.suite, None);
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
        let cli = crate::Cli::parse_from(["novarocks-sql-test", "--suite", "ssb"]);
        assert_eq!(cli.cluster_mode, ClusterMode::AllInOne);
    }

    #[test]
    fn target_session_sql_defaults_to_empty() {
        let cli = crate::Cli::parse_from(["novarocks-sql-test", "--suite", "ssb"]);

        assert!(cli.target_session_sql.is_empty());
        assert!(cli.rewrite_explain_contains_as_not_contains.is_empty());
    }

    #[test]
    fn rewrite_explain_contains_flag_preserves_requested_substrings() {
        let cli = crate::Cli::parse_from([
            "novarocks-sql-test",
            "--suite",
            "ssb",
            "--rewrite-explain-contains-as-not-contains",
            "producer binding",
            "--rewrite-explain-contains-as-not-contains",
            "consumer binding",
        ]);

        assert_eq!(
            cli.rewrite_explain_contains_as_not_contains,
            ["producer binding", "consumer binding"]
        );
    }

    #[test]
    fn target_session_sql_repeated_arguments_preserve_order() {
        let cli = crate::Cli::parse_from([
            "novarocks-sql-test",
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
            "novarocks-sql-test",
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
            "novarocks-sql-test",
            "--suite",
            "ssb",
            "--cluster-mode",
            "cross-process",
        ]);
        assert_eq!(cli.cluster_mode, ClusterMode::CrossProcess);
    }

    #[test]
    fn cli_accepts_failure_artifact_directory_override() {
        let cli = crate::Cli::parse_from([
            "novarocks-sql-test",
            "--suite",
            "ssb",
            "--failure-artifact-dir",
            "logs/custom-sql-failures",
        ]);
        assert_eq!(
            cli.failure_artifact_dir.as_deref(),
            Some("logs/custom-sql-failures")
        );
    }

    #[test]
    fn default_empty_process_diagnostics_do_not_prove_writer_exit() {
        // This owner uses the trait defaults for shutdown, residual IDs and
        // writer settlement; successful control alone grants no cleanup proof.
        let server: Arc<Mutex<Box<dyn crate::cluster::ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(ArtifactCapableServer)));
        let root = tempfile::TempDir::new().unwrap();
        let artifacts = crate::failure_artifacts::FailureArtifactRecorder::new(
            crate::failure_artifacts::FailureArtifactContext {
                root: root.path().join("artifacts"),
                lane: "correctness".into(),
                suites: vec!["analytic".into()],
            },
        );
        let path = root.path().join("results.jsonl");
        let results =
            crate::results_record::ResultsWriter::create(root.path(), path.to_str()).unwrap();
        let writer = AtomicBool::new(true);
        assert_eq!(
            super::finish_run_with_server_cleanup_recorded(
                server,
                Ok(0),
                &artifacts,
                Some(&results),
                false,
                Some(&writer),
            )
            .unwrap(),
            0
        );
        assert!(!writer.load(Ordering::Acquire));
        let record: serde_json::Value =
            serde_json::from_str(fs::read_to_string(path).unwrap().lines().next().unwrap())
                .unwrap();
        assert_eq!(record["cleanup"]["status"], "ok");
        assert_eq!(record["cleanup"]["residual_pids"], serde_json::json!([]));
        assert_eq!(record["cleanup"]["writer_exit_observed"], false);
    }

    struct WriterExitServer {
        observed: bool,
        shutdown_fails: bool,
        residual: bool,
        shutdown_started: bool,
    }

    impl crate::cluster::ServerHandle for WriterExitServer {
        fn target_host(&self) -> Option<&str> {
            None
        }
        fn target_port(&self) -> Option<u16> {
            None
        }
        fn be_count(&self) -> usize {
            3
        }
        fn writer_exit_observed(&self) -> bool {
            self.shutdown_started && self.observed
        }
        fn residual_process_ids(&self) -> Vec<u32> {
            if self.residual { vec![4242] } else { vec![] }
        }
        fn shutdown(&mut self) -> anyhow::Result<()> {
            self.shutdown_started = true;
            if self.shutdown_fails {
                anyhow::bail!("original lifecycle control failure");
            }
            Ok(())
        }
    }

    #[test]
    fn writer_exit_fact_is_separate_from_original_run_and_shutdown_failures() {
        for observed in [false, true] {
            for shutdown_fails in [false, true] {
                for external in [false, true] {
                    for residual in [false, true] {
                        let server: Arc<Mutex<Box<dyn crate::cluster::ServerHandle>>> =
                            Arc::new(Mutex::new(Box::new(WriterExitServer {
                                observed,
                                shutdown_fails,
                                residual,
                                shutdown_started: false,
                            })));
                        let root = tempfile::TempDir::new().unwrap();
                        let artifacts = crate::failure_artifacts::FailureArtifactRecorder::new(
                            crate::failure_artifacts::FailureArtifactContext {
                                root: root.path().join("artifacts"),
                                lane: "correctness".into(),
                                suites: vec!["analytic".into()],
                            },
                        );
                        let path = root.path().join("results.jsonl");
                        let results = crate::results_record::ResultsWriter::create(
                            root.path(),
                            path.to_str(),
                        )
                        .unwrap();
                        let writer = AtomicBool::new(!observed);
                        let error = super::finish_run_with_server_cleanup_recorded(
                            server,
                            Err(anyhow::anyhow!("original SQL failure")),
                            &artifacts,
                            Some(&results),
                            external,
                            Some(&writer),
                        )
                        .unwrap_err();
                        let message = format!("{error:#}");
                        assert!(message.contains("original SQL failure"), "{message}");
                        if shutdown_fails {
                            assert!(
                                message.contains("original lifecycle control failure"),
                                "{message}"
                            );
                        }
                        let expected = observed && !external && !residual;
                        assert_eq!(writer.load(Ordering::Acquire), expected);
                        let records: Vec<serde_json::Value> = fs::read_to_string(path)
                            .unwrap()
                            .lines()
                            .map(|line| serde_json::from_str(line).unwrap())
                            .collect();
                        assert_eq!(records.len(), 1);
                        assert_eq!(records[0]["cleanup"]["writer_exit_observed"], expected);
                        let expected_status = if shutdown_fails || residual {
                            "failed"
                        } else if external {
                            "external"
                        } else {
                            "ok"
                        };
                        assert_eq!(records[0]["cleanup"]["status"], expected_status);
                    }
                }
            }
        }
    }

    struct CleanupFailureServer {
        shutdown_started: bool,
    }

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

        fn be_count(&self) -> usize {
            1
        }

        fn failure_log_sources(
            &self,
            max_backend_logs: usize,
            max_history_tail_bytes: usize,
        ) -> anyhow::Result<Option<crate::cluster::ServerFailureLogSources>> {
            if self.shutdown_started {
                anyhow::bail!("runtime logs were removed during shutdown");
            }
            Ok(Some(crate::cluster::ServerFailureLogSources::from_inline(
                1,
                "FE shutdown failure".to_string(),
                (max_backend_logs > 0)
                    .then(|| "BE shutdown failure".to_string())
                    .into_iter()
                    .collect(),
                max_history_tail_bytes,
            )))
        }

        fn shutdown(&mut self) -> anyhow::Result<()> {
            self.shutdown_started = true;
            anyhow::bail!("injected shutdown failure")
        }
    }

    #[test]
    fn post_launch_cleanup_reports_primary_shutdown_and_residual_failures() {
        let server: std::sync::Arc<std::sync::Mutex<Box<dyn crate::cluster::ServerHandle>>> =
            std::sync::Arc::new(std::sync::Mutex::new(Box::new(CleanupFailureServer {
                shutdown_started: false,
            })));
        let temp = tempfile::TempDir::new().expect("temp dir");
        let failure_artifacts = crate::failure_artifacts::FailureArtifactRecorder::new(
            crate::failure_artifacts::FailureArtifactContext {
                root: temp.path().join("artifacts"),
                lane: "correctness".to_string(),
                suites: vec!["analytic".to_string()],
            },
        );
        let error = super::finish_run_with_server_cleanup(
            server,
            Err(anyhow::anyhow!("injected execution failure")),
            &failure_artifacts,
        )
        .expect_err("primary and cleanup failures must be returned together");
        let message = format!("{error:#}");
        assert!(message.contains("injected execution failure"), "{message}");
        assert!(message.contains("injected shutdown failure"), "{message}");
        assert!(message.contains("4242"), "{message}");
        assert_eq!(
            fs::read_dir(temp.path().join("artifacts"))
                .expect("cleanup failure artifacts")
                .count(),
            1
        );
    }

    #[test]
    fn server_cleanup_failure_captures_a_terminal_snapshot() {
        let server: Arc<Mutex<Box<dyn crate::cluster::ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(CleanupFailureServer {
                shutdown_started: false,
            })));
        let temp = tempfile::TempDir::new().expect("temp dir");
        let artifact_root = temp.path().join("artifacts");
        let failure_artifacts = crate::failure_artifacts::FailureArtifactRecorder::new(
            crate::failure_artifacts::FailureArtifactContext {
                root: artifact_root.clone(),
                lane: "correctness".to_string(),
                suites: vec!["analytic".to_string()],
            },
        );

        let results_path = temp.path().join("results.jsonl");
        let results =
            crate::results_record::ResultsWriter::create(temp.path(), results_path.to_str())
                .unwrap();
        let observed_exit = AtomicBool::new(true);
        let error = super::finish_run_with_server_cleanup_recorded(
            server,
            Ok(0),
            &failure_artifacts,
            Some(&results),
            false,
            Some(&observed_exit),
        )
        .expect_err("cleanup failure must fail a successful run");
        assert!(!observed_exit.load(Ordering::Acquire));
        let records = fs::read_to_string(results_path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["kind"], "server");
        assert_eq!(records[0]["cleanup"]["status"], "failed");
        assert!(
            records[0]["cleanup"]["error"]
                .as_str()
                .unwrap()
                .contains("injected shutdown failure")
        );
        assert_eq!(
            records[0]["cleanup"]["residual_pids"],
            serde_json::json!([])
        );
        assert!(
            format!("{error:#}").contains("injected shutdown failure"),
            "{error:#}"
        );
        let artifact_dirs = fs::read_dir(&artifact_root)
            .expect("cleanup failure artifacts")
            .map(|entry| entry.expect("artifact entry").path())
            .collect::<Vec<_>>();
        assert_eq!(artifact_dirs.len(), 1);
        assert!(
            artifact_dirs[0]
                .file_name()
                .unwrap()
                .to_string_lossy()
                .contains("terminal")
        );
    }

    struct ArtifactCapableServer;

    impl crate::cluster::ServerHandle for ArtifactCapableServer {
        fn target_host(&self) -> Option<&str> {
            Some("127.0.0.1")
        }

        fn target_port(&self) -> Option<u16> {
            Some(9030)
        }

        fn be_count(&self) -> usize {
            1
        }

        fn failure_log_sources(
            &self,
            max_backend_logs: usize,
            max_history_tail_bytes: usize,
        ) -> anyhow::Result<Option<crate::cluster::ServerFailureLogSources>> {
            Ok(Some(crate::cluster::ServerFailureLogSources::from_inline(
                1,
                "FE log".to_string(),
                (max_backend_logs > 0)
                    .then(|| "BE log".to_string())
                    .into_iter()
                    .collect(),
                max_history_tail_bytes,
            )))
        }
    }

    struct RetentionTrackingServer {
        events: Arc<Mutex<Vec<&'static str>>>,
        fail_snapshot: bool,
    }

    impl crate::cluster::ServerHandle for RetentionTrackingServer {
        fn target_host(&self) -> Option<&str> {
            None
        }

        fn target_port(&self) -> Option<u16> {
            None
        }

        fn failure_log_sources(
            &self,
            max_backend_logs: usize,
            max_history_tail_bytes: usize,
        ) -> anyhow::Result<Option<crate::cluster::ServerFailureLogSources>> {
            if self.fail_snapshot {
                anyhow::bail!("injected original snapshot failure");
            }
            crate::cluster::ServerHandle::failure_log_sources(
                &ArtifactCapableServer,
                max_backend_logs,
                max_history_tail_bytes,
            )
        }

        fn retain_runtime_artifacts(&mut self) {
            self.events.lock().unwrap().push("retain");
        }

        fn shutdown(&mut self) -> anyhow::Result<()> {
            self.events.lock().unwrap().push("shutdown");
            Ok(())
        }
    }

    #[test]
    fn failed_run_retains_original_diagnostics_before_server_cleanup() {
        let directory = tempfile::TempDir::new().unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let server: Arc<Mutex<Box<dyn crate::cluster::ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(RetentionTrackingServer {
                events: Arc::clone(&events),
                fail_snapshot: false,
            })));
        let artifacts = crate::failure_artifacts::FailureArtifactRecorder::new(
            crate::failure_artifacts::FailureArtifactContext {
                root: directory.path().join("failures"),
                lane: "correctness".into(),
                suites: vec!["analytic".into()],
            },
        );
        assert_eq!(
            super::finish_run_with_server_cleanup(server, Ok(1), &artifacts).unwrap(),
            1
        );
        assert_eq!(*events.lock().unwrap(), ["retain", "shutdown"]);
    }

    #[test]
    fn successful_run_does_not_retain_original_diagnostics_during_cleanup() {
        let directory = tempfile::TempDir::new().unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let server: Arc<Mutex<Box<dyn crate::cluster::ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(RetentionTrackingServer {
                events: Arc::clone(&events),
                fail_snapshot: false,
            })));
        let root = directory.path().join("must-not-exist");
        let artifacts = crate::failure_artifacts::FailureArtifactRecorder::new(
            crate::failure_artifacts::FailureArtifactContext {
                root: root.clone(),
                lane: "correctness".into(),
                suites: vec!["analytic".into()],
            },
        );
        assert_eq!(
            super::finish_run_with_server_cleanup(server, Ok(0), &artifacts).unwrap(),
            0
        );
        assert_eq!(*events.lock().unwrap(), ["shutdown"]);
        assert!(!root.exists());
    }

    #[test]
    fn failed_terminal_candidate_retains_original_diagnostics_before_cleanup() {
        let directory = tempfile::TempDir::new().unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let server: Arc<Mutex<Box<dyn crate::cluster::ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(RetentionTrackingServer {
                events: Arc::clone(&events),
                fail_snapshot: true,
            })));
        let artifacts = crate::failure_artifacts::FailureArtifactRecorder::new(
            crate::failure_artifacts::FailureArtifactContext {
                root: directory.path().join("failures"),
                lane: "correctness".into(),
                suites: vec!["analytic".into()],
            },
        );
        // The staged candidate is consumed only if shutdown fails. Preserve
        // the existing successful-run verdict, while keeping its uncaptured
        // originals instead of releasing them during normal process cleanup.
        assert_eq!(
            super::finish_run_with_server_cleanup(server, Ok(0), &artifacts).unwrap(),
            0
        );
        assert_eq!(*events.lock().unwrap(), ["retain", "shutdown"]);
    }

    #[test]
    fn failed_suite_init_runs_owned_cleanup_once_and_preserves_both_errors() {
        let temp = tempfile::TempDir::new().unwrap();
        let conn = ConnectionConfig {
            mysql: "unused".into(),
            host: "localhost".into(),
            port: "1".into(),
            user: "root".into(),
            password: None,
            catalog: None,
            db: None,
        };
        let artifacts = Arc::new(crate::failure_artifacts::FailureArtifactRecorder::new(
            crate::failure_artifacts::FailureArtifactContext {
                root: temp.path().join("failures"),
                lane: "correctness".into(),
                suites: vec!["test".into()],
            },
        ));
        let ctx = super::SuiteRunContext {
            fixture_storage: None,
            fixture_writers_settled: Arc::new(AtomicBool::new(true)),
            namespace_catalogs: crate::namespace_guard::CatalogFacts::default(),
            fixture_budget: novarocks_cluster_harness::fixture_run::RunBudget::from_environment(
                3600,
            )
            .unwrap(),
            controls: None,
            suite_name: "test".into(),
            mode: Mode::Verify,
            record_from: RecordFrom::Target,
            target_conn_base: conn.clone(),
            reference_conn_base: conn.clone(),
            target_admin_conn: conn.clone(),
            reference_admin_conn: conn,
            result_dir: None,
            actual_artifact_dir: None,
            verify_enabled: true,
            query_timeout: 1,
            reference_required: false,
            auto_case_db: false,
            order_sensitive_default: false,
            float_epsilon: None,
            preview_lines: 0,
            update_expected: false,
            target_session_sql: vec![],
            rewrite_explain_contains_as_not_contains: vec![],
            marker_re: Regex::new("query").unwrap(),
            fail_fast: false,
            server_handle: Arc::new(Mutex::new(Box::new(ArtifactCapableServer))),
            failure_artifacts: artifacts,
            publication_catalog_control: None,
            publication_service_control: None,
            results: None,
        };
        let hook = |name: &str| SuiteHook {
            path: temp.path().join(name),
            sql: name.into(),
            catalog: None,
            db: None,
        };
        let prepared = super::PreparedSuite {
            ctx,
            cases: vec![SqlCase {
                expected_variables: Default::default(),
                namespace_rewrites: Default::default(),
                auto_db: String::new(),
                source_file: temp.path().join("case.sql"),
                case_id: "case".into(),
                steps: vec![],
                case_dbs: vec![],
                sequential: false,
            }],
            load_failures: vec![],
            init_hook: Some(hook("init")),
            cleanup_hook: Some(hook("cleanup")),
        };
        let mut calls = Vec::new();
        let outcome = super::run_suite_with_hooks(
            &prepared,
            &std::sync::atomic::AtomicBool::new(false),
            &Mutex::new(()),
            |_, _, hook, _| {
                calls.push(hook.sql.clone());
                anyhow::bail!("injected {} failure", hook.sql)
            },
        );
        assert_eq!(calls, ["init", "cleanup"]);
        assert_eq!(outcome.outcomes[0].status, CaseStatus::Fail);
        assert!(outcome.cleanup_errors[0].contains("injected cleanup failure"));
        let snapshots = fs::read_dir(temp.path().join("failures"))
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect::<Vec<_>>();
        assert_eq!(snapshots.len(), 1);
        let provenance = fs::read_to_string(snapshots[0].join("provenance.json")).unwrap();
        assert!(provenance.contains("injected init failure"));
    }

    #[test]
    fn native_listing_returns_before_invalid_runtime_configuration() {
        for args in [
            vec![
                "novarocks-sql-test",
                "--list-suites",
                "--config",
                "/no/runtime/config",
            ],
            vec![
                "novarocks-sql-test",
                "--list-suites",
                "--suite",
                "all",
                "--config",
                "/no/runtime/config",
            ],
        ] {
            assert_eq!(super::run_cli(Cli::parse_from(args)).unwrap(), 0);
        }
    }

    #[test]
    fn dataset_launch_profile_is_explicit_and_incompatible_selection_fails() {
        let suites =
            crate::config::build_suite_configs(&crate::config::resolve_repo_root().unwrap())
                .unwrap();
        let selected = vec!["ssb".to_string()];
        assert!(
            super::validate_dataset_launch_profile(
                &selected,
                &suites,
                novarocks_cluster_harness::LaunchProfile::FaultScenario
            )
            .is_err()
        );
        assert!(
            super::validate_dataset_launch_profile(
                &selected,
                &suites,
                novarocks_cluster_harness::LaunchProfile::Performance
            )
            .is_ok()
        );
    }

    #[test]
    fn successful_run_does_not_create_failure_artifacts() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let artifact_root = temp.path().join("must-not-exist");
        let context = crate::failure_artifacts::FailureArtifactRecorder::new(
            crate::failure_artifacts::FailureArtifactContext {
                root: artifact_root.clone(),
                lane: "correctness".to_string(),
                suites: vec!["analytic".to_string()],
            },
        );
        let server: Arc<Mutex<Box<dyn crate::cluster::ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(ArtifactCapableServer)));

        assert_eq!(
            super::finish_run_with_server_cleanup(server, Ok(0), &context)
                .expect("successful cleanup"),
            0
        );
        assert!(!artifact_root.exists());
    }

    #[test]
    fn failed_run_persists_logs_before_server_cleanup() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let artifact_root = temp.path().join("failures");
        let context = crate::failure_artifacts::FailureArtifactRecorder::new(
            crate::failure_artifacts::FailureArtifactContext {
                root: artifact_root.clone(),
                lane: "correctness".to_string(),
                suites: vec!["analytic".to_string()],
            },
        );
        let server: Arc<Mutex<Box<dyn crate::cluster::ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(ArtifactCapableServer)));

        assert_eq!(
            super::finish_run_with_server_cleanup(server, Ok(1), &context)
                .expect("failed test run retains its exit code"),
            1
        );
        let artifact_dirs = fs::read_dir(&artifact_root)
            .expect("artifact root")
            .map(|entry| entry.expect("artifact entry").path())
            .collect::<Vec<_>>();
        assert_eq!(artifact_dirs.len(), 1);
        assert_eq!(
            fs::read_to_string(artifact_dirs[0].join("fe.log")).unwrap(),
            "FE log"
        );
        assert_eq!(
            fs::read_to_string(artifact_dirs[0].join("be-000.log")).unwrap(),
            "BE log"
        );
    }

    #[test]
    fn failed_run_preserves_terminal_snapshot_after_a_case_snapshot() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let artifact_root = temp.path().join("failures");
        let context = crate::failure_artifacts::FailureArtifactRecorder::new(
            crate::failure_artifacts::FailureArtifactContext {
                root: artifact_root.clone(),
                lane: "correctness".to_string(),
                suites: vec!["analytic".to_string()],
            },
        );
        let server: Arc<Mutex<Box<dyn crate::cluster::ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(ArtifactCapableServer)));

        context
            .persist_case_failure(server.as_ref(), "analytic", "case_one", Some(9))
            .expect("capture first causal snapshot")
            .expect("cross-process case snapshot");
        assert_eq!(
            super::finish_run_with_server_cleanup(server, Ok(1), &context)
                .expect("failed test run retains its exit code"),
            1
        );

        let mut artifact_dirs = fs::read_dir(&artifact_root)
            .expect("artifact root")
            .map(|entry| entry.expect("artifact entry").path())
            .collect::<Vec<_>>();
        artifact_dirs.sort();
        assert_eq!(artifact_dirs.len(), 2);
        assert!(artifact_dirs.iter().any(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .contains("terminal")
        }));
    }

    #[test]
    fn cross_process_configs_preserve_base_sections_and_patch_cluster_ports() {
        let runtime = CrossProcessRuntime {
            be: vec![BePorts {
                http: 18080,
                grpc: 19070,
                control_grpc: 19071,
            }],
            fe_http_port: 28080,
            fe_grpc_port: 29070,
            fe_mysql_port: 29030,
        };
        let base = r#"
[standalone_server]
mysql_port = 9030

[[connector.credentials]]
purpose = "object-store-data"
name = "test-data"
generation = "v1"
kind = "s3"
access_key_id = "admin"
access_key_secret = "admin123"

[[connector.credentials]]
purpose = "object-store-metadata"
name = "test-metadata"
generation = "v1"
kind = "s3"
access_key_id = "admin"
access_key_secret = "admin123"
"#;

        let fe = render_cross_process_config(base, ClusterProcessRole::Fe, 0, &runtime)
            .expect("render fe config");
        let be = render_cross_process_config(base, ClusterProcessRole::Be, 0, &runtime)
            .expect("render be config");

        let fe_value: toml::Value = fe.parse().expect("parse fe toml");
        let be_value: toml::Value = be.parse().expect("parse be toml");

        assert!(fe_value.get("metadata").is_none());
        assert!(be_value.get("metadata").is_none());
        assert_eq!(
            fe_value["connector"]["credentials"][0]["purpose"].as_str(),
            Some("object-store-metadata")
        );
        assert_eq!(
            fe_value["connector"]["credentials"][0]["name"].as_str(),
            Some("test-metadata")
        );
        assert_eq!(
            fe_value["connector"]["credentials"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            fe_value["standalone_server"]["mysql_port"].as_integer(),
            Some(29030)
        );
        assert_eq!(fe_value["cluster"]["role"].as_str(), Some("fe"));
        assert!(fe_value["cluster"].get("backends").is_none());

        assert_eq!(be_value["cluster"]["role"].as_str(), Some("be"));
        assert_eq!(be_value["server"]["grpc_port"].as_integer(), Some(19070));
        assert!(
            be_value
                .get("connector")
                .and_then(|value| value.get("credentials"))
                .is_some()
        );
        assert_eq!(
            be_value["connector"]["credentials"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn discover_novarocks_binary_prefers_env_override() {
        let repo_root = crate::resolve_repo_root().expect("repo root");
        let test_root = repo_root.join("tests/sql/.runtime/unit/discover-bin");
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
    fn discover_novarocks_binary_requires_explicit_input_even_with_an_existing_target() {
        let repository = tempfile::tempdir().unwrap();
        let target = repository.path().join("target/debug");
        fs::create_dir_all(&target).unwrap();
        fs::write(target.join("novarocks"), b"old build").unwrap();
        assert!(
            discover_novarocks_binary_with_override(repository.path(), None)
                .unwrap_err()
                .to_string()
                .contains("explicit NOVAROCKS_BIN")
        );
        assert!(
            discover_novarocks_binary_with_override(
                repository.path(),
                Some(target.join("missing"))
            )
            .is_err()
        );
    }

    #[test]
    fn cross_process_command_keeps_network_fixture_and_scrubs_ambient_test_authority() {
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
        assert_eq!(imv_stateless_rebuild, None);
        let removed = command
            .get_envs()
            .find(|(key, _)| key.to_str() == Some("NOVAROCKS_ENABLE_TEST_IMV_STATELESS_REBUILD"))
            .expect("legacy authority is explicitly scrubbed");
        assert!(removed.1.is_none());
        assert!(
            !command
                .get_args()
                .any(|argument| argument == "--test-session")
        );
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
            &crate::fixture_context::test_variables(&[path.file_stem().unwrap().to_str().unwrap()]),
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
        let cli = Cli::try_parse_from(["novarocks-sql-test", "--suite", "ssb"]).expect("parse cli");
        assert_eq!(cli.cluster_size, None);
    }

    #[test]
    fn cli_cluster_size_2_cross_process() {
        let cli = Cli::try_parse_from([
            "novarocks-sql-test",
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
            "novarocks-sql-test",
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
        let cli = Cli::try_parse_from([
            "novarocks-sql-test",
            "--suite",
            "ssb",
            "--cluster-size",
            "2",
        ])
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

    #[test]
    fn publication_catalog_fixture_rejects_mixed_suite_selection() {
        let mut config = crate::types::RunnerConfig::default();
        config.values.insert(
            "iceberg_rest_uri".to_string(),
            "http://127.0.0.1:8181".to_string(),
        );
        let result = super::start_publication_catalog_fixture(
            &mut config,
            &[
                "lake-publication".to_string(),
                "iceberg-compatibility".to_string(),
            ],
        );
        let error = match result {
            Ok(_) => panic!("ordinary compatibility suite must not use the publication fixture"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("acceptance-only"));
    }

    #[test]
    fn publication_catalog_fixture_requires_a_rest_endpoint() {
        let mut config = crate::types::RunnerConfig::default();
        config
            .values
            .insert("iceberg_rest_uri".to_string(), "".to_string());
        let result = super::start_publication_catalog_fixture(
            &mut config,
            &["lake-publication".to_string()],
        );
        let error = match result {
            Ok(_) => panic!("publication fixture must fail closed without a REST endpoint"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("requires iceberg_rest_uri"));
    }

    #[test]
    fn publication_proxy_overlays_all_sql_aliases_without_rebinding_provider_processes() {
        let directory = tempfile::tempdir().unwrap();
        let env_file = directory.path().join("env.sh");
        std::fs::write(&env_file, "export NOVA_ENV_READY=true\n").unwrap();
        let config_file = directory.path().join("sql-test.toml");
        std::fs::write(
            &config_file,
            format!(
                r#"
[env]
fixture_env_file = {env_file:?}
iceberg_rest_uri = "http://127.0.0.1:38181"
iceberg_rest_warehouse = "s3://warehouse/private/rest"
iceberg_rest_mv_uri = "http://127.0.0.1:38182"
iceberg_rest_mv_warehouse = "s3://warehouse/private/rest-mv"
oss_endpoint = "http://127.0.0.1:38000"
oss_ak = "private-key"
oss_sk = "private-secret"
"#
            ),
        )
        .unwrap();
        let mut config = crate::config::load_runner_config(Some(&config_file)).unwrap();
        let physical = crate::config::FixtureEndpoints::from_runner_config(&config).unwrap();
        let child_environment = physical.project(&mut config);
        let process_environment_before: std::collections::BTreeMap<_, _> = child_environment
            .keys()
            .map(|key| (key, std::env::var_os(key)))
            .collect();
        let proxy = super::start_publication_catalog_fixture(
            &mut config,
            &["lake-publication".to_string()],
        )
        .unwrap()
        .unwrap();
        for alias in [
            "iceberg_rest_uri",
            "env.iceberg_rest_uri",
            "env.oss.iceberg_rest_uri",
        ] {
            assert_eq!(config.values[alias], proxy.uri());
        }
        for alias in ["oss_endpoint", "env.oss_endpoint", "env.oss.oss_endpoint"] {
            assert_eq!(config.values[alias], "http://127.0.0.1:38000");
        }
        let process_environment_after: std::collections::BTreeMap<_, _> = child_environment
            .keys()
            .map(|key| (key, std::env::var_os(key)))
            .collect();
        assert_eq!(process_environment_before, process_environment_after);
        assert_eq!(
            child_environment["NOVAROCKS_ICEBERG_REST_URI"],
            "http://127.0.0.1:38181"
        );
        assert_eq!(
            PathBuf::from(&child_environment["NOVA_ENV_REST_ENV_FILE"]),
            env_file.canonicalize().unwrap()
        );
    }

    fn test_fixture_budget() -> novarocks_cluster_harness::fixture_run::RunBudget {
        use novarocks_cluster_harness::fixture_run::{FixturePolicy, RunBudget, ScratchIdentity};
        RunBudget::admit(
            FixturePolicy::default(),
            ScratchIdentity::create("s3://novarocks/sql-test").unwrap(),
            600,
            None,
        )
        .unwrap()
    }

    #[test]
    fn an_ordinary_suite_never_silently_gets_the_isolated_catalog() {
        let result = super::start_isolated_rest_catalog(
            &[
                "mv-storage-contract".to_string(),
                "iceberg-compatibility".to_string(),
            ],
            &test_fixture_budget(),
        );
        let error = match result {
            Ok(_) => panic!("a shared-environment suite must not be redirected silently"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("cannot share this run"),
            "{error}"
        );
    }

    #[test]
    fn suites_that_want_the_shared_environment_start_no_container() {
        let mut config = crate::types::RunnerConfig::default();
        config.values.insert(
            "iceberg_rest_uri".to_string(),
            "http://127.0.0.1:8181".to_string(),
        );
        let (fixture, control_uri) = super::start_isolated_rest_catalog(
            &["iceberg-compatibility".to_string(), "join".to_string()],
            &test_fixture_budget(),
        )
        .expect("an ordinary selection is not an error");

        assert!(fixture.is_none());
        assert!(control_uri.is_none());
        assert_eq!(
            config.values.get("iceberg_rest_uri").map(String::as_str),
            Some("http://127.0.0.1:8181"),
            "the shared endpoint must survive untouched"
        );
    }

    #[test]
    fn unloadable_case_files_become_failing_cases_of_their_own() {
        let failures = vec![
            super::CaseLoadFailure {
                case_id: "bad_meta".to_string(),
                source_file: std::path::PathBuf::from("/suite/sql/bad_meta.sql"),
                error: "unknown expect_sql_code: sql.does.not.exist".to_string(),
            },
            super::CaseLoadFailure {
                case_id: "also_bad".to_string(),
                source_file: std::path::PathBuf::from("/suite/sql/also_bad.sql"),
                error: "invalid expect_error_at: 0:0".to_string(),
            },
        ];

        let outcomes = super::load_failure_outcomes("demo", &failures);

        // Every unloadable file is one failing case, so a suite reports the
        // rest of its cases instead of aborting on the first bad one.
        assert_eq!(outcomes.len(), failures.len());
        assert!(
            outcomes
                .iter()
                .all(|outcome| outcome.status == crate::CaseStatus::Fail)
        );
        assert_eq!(
            outcomes
                .iter()
                .map(|outcome| outcome.case_id.as_str())
                .collect::<Vec<_>>(),
            vec!["bad_meta", "also_bad"],
        );
        // The log must name the file and carry the parser's own reason.
        assert!(outcomes[0].log.contains("/suite/sql/bad_meta.sql"));
        assert!(
            outcomes[0]
                .log
                .contains("unknown expect_sql_code: sql.does.not.exist")
        );
    }
}
