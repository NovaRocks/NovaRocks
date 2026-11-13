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

// Design: ADR-0210 (docs/adr/ADR-0210-sql-golden-is-canonical-rendering-of-typed-trino-results.md)
use crate::types::*;
use anyhow::{Context, Result, bail, ensure};
use novarocks_test_trino_client::{
    CancelHandle, ClientConfig, ClientSession, PendingStatement, StatementFailure, StatementResult,
    StatementTrace, TrinoClient,
};
use sha2::{Digest, Sha256};
use std::time::{Duration, Instant};

pub struct TrinoCaseSession {
    namespace_guard: Option<crate::namespace_guard::NamespaceGuard>,
    client: TrinoClient,
    pub state: ClientSession,
    base_config: ConnectionConfig,
    deadline: Option<Instant>,
    operation_deadline: Option<Instant>,
    cancel_slot: crate::fault_injection::QueryCancelSlot,
    conversion_mode: bool,
}
impl TrinoCaseSession {
    pub(crate) fn new_with_deadline(
        conn: &ConnectionConfig,
        deadline: Option<Instant>,
    ) -> Result<Self> {
        Self::new_with_session_defaults(
            conn,
            deadline,
            &crate::suite_manifest::SuiteSessionDefaults::default(),
        )
    }
    pub(crate) fn new_with_session_defaults(
        conn: &ConnectionConfig,
        deadline: Option<Instant>,
        defaults: &crate::suite_manifest::SuiteSessionDefaults,
    ) -> Result<Self> {
        defaults.validate()?;
        let client = Self::client(conn)?;
        let mut state = ClientSession::new(&conn.time_zone);
        if let Some(dop) = defaults.pipeline_dop {
            state
                .properties
                .insert("pipeline_dop".into(), dop.to_string());
        }
        state.catalog = conn.catalog.clone().filter(|v| !v.is_empty());
        if let Some(database) = conn.db.as_deref().filter(|value| !value.is_empty()) {
            Self::set_database_context(&mut state, database)?;
        }
        Ok(Self {
            namespace_guard: None,
            client,
            state,
            base_config: conn.clone(),
            deadline,
            operation_deadline: None,
            cancel_slot: crate::fault_injection::QueryCancelSlot::default(),
            conversion_mode: false,
        })
    }
    fn client(conn: &ConnectionConfig) -> Result<TrinoClient> {
        TrinoClient::new(ClientConfig {
            host: conn.host.clone(),
            port: conn.port.parse().context("invalid Trino port")?,
            user: conn.user.clone(),
            source: "novarocks-sql-test".into(),
        })
        .map_err(Into::into)
    }
    // Runner database controls have the same syntax as USE. The two names
    // belong in separate Trino headers, never in one schema header.
    fn set_database_context(state: &mut ClientSession, database: &str) -> Result<()> {
        use novarocks_parser::ast::{SessionStatement, Statement};
        let statements = novarocks_parser::parse(&format!("USE {database}"))
            .map_err(|error| anyhow::anyhow!("invalid runner database context: {error:?}"))?;
        let [Statement::Session(SessionStatement::Use(context))] = statements.as_slice() else {
            bail!("runner database context must be exactly one USE namespace");
        };
        let schema = novarocks_types::naming::normalize_identifier(&context.database.value)
            .map_err(anyhow::Error::msg)?;
        let catalog = context
            .catalog
            .as_ref()
            .map(|name| novarocks_types::naming::normalize_identifier(&name.value))
            .transpose()
            .map_err(anyhow::Error::msg)?;
        // Validate the entire control before changing either current name.
        if let Some(catalog) = catalog {
            state.catalog = Some(catalog);
        }
        state.schema = Some(schema);
        Ok(())
    }
    pub(crate) fn set_namespace_guard(&mut self, guard: crate::namespace_guard::NamespaceGuard) {
        self.namespace_guard = Some(guard);
        self.sync_guard();
    }
    fn sync_guard(&mut self) {
        if let Some(guard) = &mut self.namespace_guard {
            guard.set_session_context(self.state.catalog.clone(), self.state.schema.clone());
        }
    }
    pub(crate) fn set_conversion_mode(&mut self, enabled: bool) {
        self.conversion_mode = enabled;
    }
    pub fn reconnect(&mut self) -> Result<()> {
        self.client = Self::client(&self.base_config)?;
        Ok(())
    }
    pub(crate) fn begin_cleanup(&mut self, deadline: Instant) -> Result<()> {
        self.reconnect()?;
        self.deadline = Some(deadline);
        self.operation_deadline = None;
        if let Some(guard) = &self.namespace_guard {
            self.namespace_guard = Some(guard.for_cleanup(deadline));
        }
        Ok(())
    }
    pub(crate) fn cancel_slot(&self) -> crate::fault_injection::QueryCancelSlot {
        self.cancel_slot.clone()
    }
    pub(crate) fn original_deadline(&self) -> Option<Instant> {
        self.deadline
    }
    pub(crate) fn set_operation_deadline(&mut self, deadline: Option<Instant>) {
        self.operation_deadline = deadline;
    }
    pub(crate) fn before_operation(&self) -> Result<()> {
        ensure!(
            self.deadline.is_none_or(|d| Instant::now() < d)
                && self.operation_deadline.is_none_or(|d| Instant::now() < d),
            "original SQL deadline expired"
        );
        Ok(())
    }
    pub(crate) fn finish(self) -> Result<()> {
        drop(self);
        Ok(())
    }
    pub fn execute_query(
        &mut self,
        query_timeout: u64,
        sql: &str,
        db: Option<&str>,
    ) -> SqlExecutionOutcome {
        self.execute_query_with_before_execute(query_timeout, sql, db, true, |_, _, _, _, _| Ok(()))
    }
    /// POST obtains the query identity before controls are armed. The first GET
    /// starts the statement only after the caller has frozen its observation.
    pub(crate) fn execute_query_with_before_execute(
        &mut self,
        query_timeout: u64,
        sql: &str,
        db: Option<&str>,
        allow_transient_commit_retries: bool,
        mut before: impl FnMut(usize, usize, &str, &str, CancelHandle) -> Result<()>,
    ) -> SqlExecutionOutcome {
        let mut attempts = Vec::new();
        for ordinal in 0..2 {
            let attempt = self.execute_query_once(
                ordinal,
                query_timeout,
                sql,
                db,
                |index, query_id, submitted, cancel| {
                    before(ordinal, index, query_id, submitted, cancel)
                },
            );
            let mut outcome = SqlExecutionOutcome::from_attempt(attempt);
            let retry = !outcome.0
                && ordinal == 0
                && !self.conversion_mode
                && allow_transient_commit_retries
                && outcome.failure_is_statement()
                && crate::runner::is_transient_iceberg_commit_error(&outcome.2);
            attempts.append(&mut outcome.3);
            outcome.3 = attempts;
            if !retry {
                return outcome;
            }
            if let Err(error) = self.before_operation() {
                let message = format!("SQL retry deadline failed: {error:#}");
                outcome
                    .3
                    .last_mut()
                    .unwrap()
                    .local_failures
                    .push(message.clone());
                outcome.2 = format!("{}\n{message}", outcome.2);
                return outcome;
            }
            let delay = self
                .deadline
                .map_or(Duration::from_millis(300), |deadline| {
                    deadline
                        .saturating_duration_since(Instant::now())
                        .min(Duration::from_millis(300))
                });
            std::thread::sleep(delay);
            attempts = outcome.3;
        }
        unreachable!("finite retry loop always returns")
    }
    fn execute_query_once(
        &mut self,
        ordinal: usize,
        query_timeout: u64,
        sql: &str,
        db: Option<&str>,
        mut before: impl FnMut(usize, &str, &str, CancelHandle) -> Result<()>,
    ) -> SqlExecutionAttempt {
        let started = Instant::now();
        let mut attempt = SqlExecutionAttempt {
            ordinal,
            statement_count: 0,
            statements: Vec::new(),
            local_failures: Vec::new(),
            execution: None,
            message: String::new(),
            succeeded: false,
        };
        let statements = match split_sql_statements(sql) {
            Ok(statements) => statements,
            Err(error) => {
                attempt.message = format!("ERROR (0.00s): {error:#}");
                attempt.local_failures.push(format!("{error:#}"));
                return attempt;
            }
        };
        attempt.statement_count = statements.len();
        if let Some(db) = db.filter(|value| !value.is_empty())
            && let Err(error) = Self::set_database_context(&mut self.state, db)
        {
            attempt.message = format!("ERROR (runner database context): {error:#}");
            attempt.local_failures.push(format!("{error:#}"));
            return attempt;
        }
        self.state
            .properties
            .insert("query_timeout".into(), query_timeout.to_string());
        self.sync_guard();
        let mut results = Vec::new();
        let mut result_statements = Vec::new();
        let mut messages = Vec::new();
        for (index, statement) in statements.iter().enumerate() {
            let mut facts = SqlStatementOutcome {
                ordinal: index,
                sql: statement.clone(),
                submission_started: false,
                query_id: None,
                completed: false,
                result: None,
                failures: Vec::new(),
            };
            let mut client_error = None;
            let outcome = (|| -> Result<StatementResult> {
                self.before_operation()?;
                if let Some(guard) = &self.namespace_guard {
                    guard.before(statement)?;
                }
                let deadline = match self
                    .deadline
                    .into_iter()
                    .chain(self.operation_deadline)
                    .min()
                {
                    Some(deadline) => deadline,
                    None => Instant::now()
                        .checked_add(Duration::from_secs(query_timeout))
                        .context("SQL statement deadline overflow")?,
                };
                self.cancel_slot.clear().map_err(anyhow::Error::msg)?;
                facts.submission_started = true;
                let pending = self
                    .client
                    .submit_observed(&self.state, statement, deadline, emit_statement_trace)
                    .inspect_err(|error| {
                        client_error = Some(error.clone());
                    })?;
                facts.query_id = Some(pending.query_id.clone());
                let cancel = pending.cancel_handle();
                if let Err(error) = self
                    .cancel_slot
                    .install(cancel.clone())
                    .map_err(anyhow::Error::msg)
                {
                    emit_control_failure_trace(&pending, &error);
                    return Err(error);
                }
                if let Err(error) = before(index, &pending.query_id, statement, cancel.clone()) {
                    emit_control_failure_trace(&pending, &error);
                    let error = error.context("runner before-execute control failed");
                    facts
                        .failures
                        .push(SqlExecutionFailure::Local(format!("{error:#}")));
                    if let Err(error) = cancel.delete(deadline) {
                        facts.failures.push(SqlExecutionFailure::Client(error));
                    }
                    if let Err(error) = self.cancel_slot.clear() {
                        facts.failures.push(SqlExecutionFailure::Local(error));
                    }
                    return Err(error);
                }
                let outcome = pending.run_observed(deadline, emit_statement_trace);
                match &outcome {
                    Ok((result, _)) => {
                        facts.completed = true;
                        facts.result = Some(result.clone());
                    }
                    Err(error) => {
                        client_error = Some(error.clone());
                    }
                }
                if let Err(error) = self.cancel_slot.clear().map_err(anyhow::Error::msg) {
                    facts
                        .failures
                        .push(SqlExecutionFailure::Local(format!("{error:#}")));
                    return Err(error);
                }
                let (result, effects) = outcome?;
                self.state.apply(&effects);
                self.sync_guard();
                if let Some(guard) = &mut self.namespace_guard {
                    guard.after(statement)?;
                }
                Ok(result)
            })();
            match outcome {
                Ok(result) if result.is_result_set() => {
                    result_statements.push(statement.clone());
                    results.push(result);
                }
                Ok(_) => {}
                Err(error) => {
                    if let Some(failure) = client_error {
                        facts.completed |= matches!(failure, StatementFailure::Trino(_));
                        facts
                            .failures
                            .insert(0, SqlExecutionFailure::Client(failure));
                    } else if facts.failures.is_empty() {
                        facts
                            .failures
                            .push(SqlExecutionFailure::Local(format!("{error:#}")));
                    }
                    let is_statement = facts.failures.iter().all(|failure| {
                        matches!(
                            failure,
                            SqlExecutionFailure::Client(StatementFailure::Trino(_))
                        )
                    });
                    let diagnostic = facts
                        .failures
                        .iter()
                        .map(|failure| match failure {
                            SqlExecutionFailure::Client(error) => error.to_string(),
                            SqlExecutionFailure::Local(message) => message.clone(),
                        })
                        .collect::<Vec<_>>()
                        .join("; ");
                    messages.push(format_statement_failure(
                        started.elapsed(),
                        index,
                        statements.len(),
                        statement,
                        &diagnostic,
                    ));
                    attempt.statements.push(facts);
                    if !self.conversion_mode {
                        attempt.message = messages.join("\n");
                        return attempt;
                    }
                    if !is_statement {
                        break;
                    }
                    continue;
                }
            }
            attempt.statements.push(facts);
        }
        // Per-statement facts retain raw typed results even when projection or
        // a later statement fails. Conversion cannot bless this partial batch.
        let mut execution = QueryExecution {
            shell_result: None,
            header: Vec::new(),
            rows: Vec::new(),
            text_output: String::new(),
            elapsed: started.elapsed(),
            result_statements,
            typed_results: results,
        };
        let projection = (|| -> Result<()> {
            let blocks = crate::golden::execution::blocks_from_results(&execution.typed_results)
                .context("typed result rendering")?;
            let projected = crate::results::project_golden_blocks(&blocks)
                .context("typed result projection")?;
            let text_output =
                crate::golden::execution::blocks_text(&blocks).context("typed result text")?;
            execution.header = projected.header;
            execution.rows = projected.rows;
            execution.text_output = text_output;
            Ok(())
        })();
        if let Err(error) = projection {
            let message = format!("FAIL ({error:#})");
            attempt.local_failures.push(message.clone());
            messages.push(message);
            attempt.execution = self.conversion_mode.then_some(execution);
        } else {
            attempt.execution = Some(execution);
        }
        attempt.succeeded = messages.is_empty();
        attempt.message = messages.join("\n");
        attempt
    }
}

fn emit_statement_trace(trace: StatementTrace<'_>) {
    use std::io::Write;
    let mut stderr = std::io::stderr().lock();
    let _ = stderr.write_all(b"Trino statement ");
    let _ = serde_json::to_writer(&mut stderr, &trace);
    let _ = stderr.write_all(b"\n");
}

fn emit_control_failure_trace(pending: &PendingStatement, error: &anyhow::Error) {
    use std::io::Write;
    let trace = pending.submission_observation();
    let event = serde_json::json!({
        "query_id": trace.query_id,
        "initial_next_uri": trace.initial_next_uri,
        "pages": trace.pages,
        "update_type": trace.update_type,
        "update_count": trace.update_count,
        "state": "runner_control_failed",
        "failure": {"RunnerControl": format!("{error:#}")},
    });
    let _ = writeln!(std::io::stderr().lock(), "Trino statement {event}");
}

fn statement_sha256(statement: &str) -> String {
    format!("{:x}", Sha256::digest(statement.as_bytes()))
}

fn format_statement_failure(
    elapsed: Duration,
    statement_index: usize,
    statement_count: usize,
    statement: &str,
    error: &str,
) -> String {
    format!(
        "FAIL ({:.2}s): statement {}/{} (sha256={}): {}",
        elapsed.as_secs_f64(),
        statement_index + 1,
        statement_count,
        statement_sha256(statement),
        error
    )
}

pub(crate) fn split_sql_statements(sql: &str) -> Result<Vec<String>> {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum State {
        Normal,
        SingleQuote,
        DoubleQuote,
        Backtick,
        LineComment,
        BlockComment,
        // MySQL optimizer hint `/*+ ... */` looks like a block comment but is
        // semantically part of the statement (parsed by the server). Preserve
        // its full text — stripping it would silently drop SET_VAR hints like
        // `recursive_cte_max_depth=N` and change query behavior.
        OptimizerHint,
    }

    let mut statements = Vec::new();
    let mut buffer = String::new();
    let mut state = State::Normal;
    let bytes = sql.as_bytes();
    let mut i = 0usize;

    while i < sql.len() {
        // Safe: we only advance `i` on character boundaries (we never
        // step inside a multi-byte char body; the state-transition
        // cases below use sql[i..].chars().next() or byte peeks that
        // are valid at i because we got here via char_indices logic.)
        let ch = match sql[i..].chars().next() {
            Some(c) => c,
            None => break,
        };
        let char_len = ch.len_utf8();

        match state {
            State::Normal => match ch {
                '\'' => {
                    state = State::SingleQuote;
                    buffer.push(ch);
                }
                '"' => {
                    state = State::DoubleQuote;
                    buffer.push(ch);
                }
                '`' => {
                    state = State::Backtick;
                    buffer.push(ch);
                }
                '-' if i + 1 < sql.len() && bytes[i + 1] == b'-' => {
                    // MySQL rule: `--` is a comment only when followed by
                    // whitespace, a control character, or end of line.
                    let after = i + 2;
                    let next_is_ws_or_eol = after >= sql.len()
                        || matches!(bytes[after], b' ' | b'\t' | b'\n' | b'\r')
                        || bytes[after] < 0x20;
                    if next_is_ws_or_eol {
                        state = State::LineComment;
                        i += 2;
                        continue;
                    }
                    buffer.push(ch);
                }
                '/' if i + 1 < sql.len() && bytes[i + 1] == b'*' => {
                    if i + 2 < sql.len() && bytes[i + 2] == b'+' {
                        buffer.push_str("/*+");
                        state = State::OptimizerHint;
                        i += 3;
                        continue;
                    }
                    state = State::BlockComment;
                    i += 2;
                    continue;
                }
                ';' => {
                    if let Some(statement) = normalize_statement_fragment(&buffer) {
                        statements.push(statement);
                    }
                    buffer.clear();
                }
                _ => buffer.push(ch),
            },
            State::SingleQuote => {
                buffer.push(ch);
                if ch == '\'' {
                    state = State::Normal;
                }
            }
            State::DoubleQuote => {
                buffer.push(ch);
                if ch == '"' {
                    state = State::Normal;
                }
            }
            State::Backtick => {
                buffer.push(ch);
                if ch == '`' {
                    state = State::Normal;
                }
            }
            State::LineComment => {
                if ch == '\n' {
                    state = State::Normal;
                    buffer.push(ch);
                }
            }
            State::BlockComment => {
                if ch == '*' && i + 1 < sql.len() && bytes[i + 1] == b'/' {
                    state = State::Normal;
                    i += 2;
                    continue;
                }
            }
            State::OptimizerHint => {
                if ch == '*' && i + 1 < sql.len() && bytes[i + 1] == b'/' {
                    buffer.push_str("*/");
                    state = State::Normal;
                    i += 2;
                    continue;
                }
                buffer.push(ch);
            }
        }
        i += char_len;
    }

    match state {
        State::SingleQuote | State::DoubleQuote | State::Backtick => {
            bail!("unterminated quoted string in SQL batch");
        }
        State::BlockComment => bail!("unterminated /* */ block comment in SQL batch"),
        State::OptimizerHint => bail!("unterminated /*+ */ optimizer hint in SQL batch"),
        _ => {}
    }

    if let Some(trailing) = normalize_statement_fragment(&buffer) {
        statements.push(trailing);
    }
    Ok(statements)
}

fn normalize_statement_fragment(fragment: &str) -> Option<String> {
    let mut lines = Vec::new();
    let mut started = false;
    for line in fragment.lines() {
        let trimmed = line.trim();
        if !started && (trimmed.is_empty() || trimmed.starts_with("--") || trimmed.starts_with('#'))
        {
            continue;
        }
        if !trimmed.is_empty() {
            started = true;
        }
        lines.push(line.trim_end());
    }
    let normalized = lines.join("\n").trim().to_string();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

pub(crate) fn run_trino_sql_until(
    conn: &ConnectionConfig,
    query_timeout: u64,
    sql: &str,
    deadline: Option<Instant>,
) -> Result<String> {
    run_trino_sql_scoped_until(conn, query_timeout, sql, deadline, None)
}
pub(crate) fn run_trino_sql_scoped_until(
    conn: &ConnectionConfig,
    query_timeout: u64,
    sql: &str,
    deadline: Option<Instant>,
    guard: Option<crate::namespace_guard::NamespaceGuard>,
) -> Result<String> {
    let mut session = TrinoCaseSession::new_with_deadline(conn, deadline)?;
    if let Some(guard) = guard {
        session.set_namespace_guard(guard);
    }
    let outcome = session.execute_query(query_timeout, sql, None);
    let (ok, execution, message) = outcome.summary();
    let cleanup = session.finish();
    match (ok, cleanup) {
        (false, Err(cleanup)) => {
            bail!("Trino protocol command failed: {message}; client cleanup: {cleanup:#}")
        }
        (false, Ok(())) => bail!("Trino protocol command failed: {message}"),
        (true, Err(cleanup)) => return Err(cleanup.context("Trino client cleanup")),
        (true, Ok(())) => {}
    }
    Ok(execution.map_or_else(String::new, |execution| execution.text_output))
}

fn case_table_names_sql(catalog: &str, db_name: &str) -> String {
    // Views remain owned by each case's explicit cleanup statements.
    format!(
        "SELECT table_name FROM `{catalog}`.information_schema.tables WHERE table_schema = '{db_name}' AND table_type = 'BASE TABLE';"
    )
}

/// Names of the tables a case database currently holds.
///
/// `DROP DATABASE ... FORCE` expands into a listing of the namespace's
/// children, so a catalog that cannot enumerate one kind of child refuses it.
/// The harness therefore enumerates and drops explicitly, which works on every
/// catalog.
///
/// The catalog qualifier is required: a two-part `information_schema` name
/// binds to the session catalog. A case database in another catalog must use
/// that catalog's exact qualifier.
fn case_table_names(
    conn: &ConnectionConfig,
    query_timeout: u64,
    db_name: &str,
    deadline: Option<Instant>,
) -> Result<Vec<String>> {
    let Some(catalog) = conn.catalog.as_deref() else {
        return Ok(Vec::new());
    };
    let sql = case_table_names_sql(catalog, db_name);
    let output = match run_trino_sql_until(conn, query_timeout, &sql, deadline) {
        Ok(output) => output,
        Err(error) => {
            return Err(error.context("case cleanup could not enumerate the exact namespace"));
        }
    };
    Ok(output
        .lines()
        .skip(1)
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect())
}

pub(crate) fn reset_case_database_scoped_until(
    conn: &ConnectionConfig,
    query_timeout: u64,
    db_name: &str,
    label: &str,
    deadline: Option<Instant>,
    guard: Option<crate::namespace_guard::NamespaceGuard>,
) -> Result<()> {
    drop_case_database_until(conn, query_timeout, db_name, label, deadline)?;
    run_trino_sql_scoped_until(
        conn,
        query_timeout,
        &format!("CREATE DATABASE `{db_name}`;"),
        deadline,
        guard,
    )
    .with_context(|| format!("{} case database reset failed: {}", label, db_name))?;
    Ok(())
}

pub(crate) fn drop_case_database_until(
    conn: &ConnectionConfig,
    query_timeout: u64,
    db_name: &str,
    label: &str,
    deadline: Option<Instant>,
) -> Result<()> {
    // Without a catalog there is nothing to enumerate against, so the explicit
    // path cannot see the children it would need to drop first. Fall back to
    // FORCE, which is what the local catalog still supports and what this did
    // everywhere before enumeration existed.
    if conn.catalog.is_none() {
        let sql = format!("DROP DATABASE IF EXISTS `{db_name}` FORCE;");
        run_trino_sql_until(conn, query_timeout, &sql, deadline)
            .with_context(|| format!("{} case database cleanup failed: {}", label, db_name))?;
        return Ok(());
    }
    let mut sql = String::new();
    for table in case_table_names(conn, query_timeout, db_name, deadline)? {
        sql.push_str(&format!("DROP TABLE IF EXISTS `{db_name}`.`{table}`;\n"));
    }
    sql.push_str(&format!("DROP DATABASE IF EXISTS `{db_name}`;"));
    run_trino_sql_until(conn, query_timeout, &sql, deadline)
        .with_context(|| format!("{} case database cleanup failed: {}", label, db_name))?;
    Ok(())
}

#[cfg(test)]
mod splitter_tests {
    use super::{format_statement_failure, split_sql_statements, statement_sha256};
    use std::time::Duration;

    #[test]
    fn case_cleanup_enumerates_only_base_tables_in_the_exact_catalog_namespace() {
        assert_eq!(
            super::case_table_names_sql("external", "owned_db"),
            "SELECT table_name FROM `external`.information_schema.tables WHERE table_schema = 'owned_db' AND table_type = 'BASE TABLE';"
        );
    }
    #[test]
    fn statement_hash_is_stable_and_does_not_reveal_statement_text() {
        let statement = "SELECT 1";
        let digest = statement_sha256(statement);
        assert_eq!(
            digest,
            "e004ebd5b5532a4b85984a62f8ad48a81aa3460c1ca07701f386135d72cdecf5"
        );
        assert!(!digest.contains(statement));
    }

    #[test]
    fn multi_statement_failure_identifies_ordinal_count_and_hash_only() {
        let statement = "SELECT 'private-token'";
        let rendered =
            format_statement_failure(Duration::from_secs(2), 1, 3, statement, "query timed out");
        assert!(rendered.contains("statement 2/3"), "{rendered}");
        assert!(
            rendered.contains(&format!("sha256={}", statement_sha256(statement))),
            "{rendered}"
        );
        assert!(!rendered.contains("private-token"), "{rendered}");
    }

    #[test]
    fn line_comment_semicolon_does_not_split() {
        let sql = "DELETE FROM t WHERE c = '2020-01-01 00:00:00';
-- '00:00:00.0' is same as '00:00:00'; rows already gone
DELETE FROM t WHERE c = '2020-01-01 00:00:00.0';";
        let parts = split_sql_statements(sql).expect("split");
        assert_eq!(parts.len(), 2, "expected 2 statements, got {:?}", parts);
        assert!(parts[0].starts_with("DELETE FROM t WHERE c = '2020-01-01 00:00:00'"));
        assert!(parts[1].starts_with("DELETE FROM t WHERE c = '2020-01-01 00:00:00.0'"));
    }

    #[test]
    fn block_comment_semicolon_does_not_split() {
        let sql = "SELECT 1; /* note; with ; semicolons */ SELECT 2;";
        let parts = split_sql_statements(sql).expect("split");
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "SELECT 1");
        assert_eq!(parts[1], "SELECT 2");
    }

    #[test]
    fn double_dash_without_trailing_whitespace_is_not_a_comment() {
        // MySQL rule: `--` is a comment only when followed by whitespace,
        // a control character, or end of line.
        let sql = "SELECT a--b FROM t;";
        let parts = split_sql_statements(sql).expect("split");
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0], "SELECT a--b FROM t");
    }

    #[test]
    fn comment_markers_inside_string_literal_are_inert() {
        let sql = "SELECT '-- not a comment'; SELECT '/* also not */';";
        let parts = split_sql_statements(sql).expect("split");
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "SELECT '-- not a comment'");
        assert_eq!(parts[1], "SELECT '/* also not */'");
    }

    #[test]
    fn optimizer_hint_is_preserved() {
        // MySQL `/*+ ... */` optimizer hints are part of the statement (the
        // server parses them) — stripping them would silently drop SET_VAR
        // values such as `recursive_cte_max_depth`.
        let sql = "SELECT /*+ SET_VAR(recursive_cte_max_depth=10) */ n FROM fib; SELECT 2;";
        let parts = split_sql_statements(sql).expect("split");
        assert_eq!(parts.len(), 2);
        assert_eq!(
            parts[0],
            "SELECT /*+ SET_VAR(recursive_cte_max_depth=10) */ n FROM fib"
        );
        assert_eq!(parts[1], "SELECT 2");
    }

    #[test]
    fn nested_block_comment_is_not_supported() {
        // MySQL treats /* ... /* ... */ as ending at the first */, leaving
        // the trailing `... */` outside the comment. We match that.
        let sql = "SELECT 1; /* outer /* inner */ tail */; SELECT 2;";
        let parts = split_sql_statements(sql).expect("split");
        // After first */ at offset of "inner */", `tail */` is outside.
        // The bare `;` after the `*/` closes the second statement.
        assert!(
            parts.len() >= 2,
            "nested-block parsing produced {:?}",
            parts
        );
    }
}

#[cfg(test)]
mod session_tests {
    //! The HTTP peer is an independent protocol oracle. It owns its listener,
    //! accepted sockets and a bounded thread; Drop stops and joins even when a
    //! client assertion fails. No external fixture or product service is used.
    use super::*;
    use novarocks_test_trino_client::TrinoValue;
    use std::{
        collections::BTreeMap,
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        thread::{self, JoinHandle},
    };

    #[derive(Clone, Debug)]
    struct Request {
        method: String,
        path: String,
        headers: BTreeMap<String, String>,
        body: String,
        connection: usize,
    }
    #[derive(Default)]
    struct FaultPeerGate {
        request_seen: AtomicBool,
        action_done: AtomicBool,
        wait_for_action: bool,
    }

    struct Reply {
        method: &'static str,
        path: &'static str,
        body: String,
        headers: Vec<(&'static str, &'static str)>,
        keep_alive: bool,
        status: u16,
        drop_before_response: bool,
        fault_gate: Option<Arc<FaultPeerGate>>,
    }
    impl Reply {
        fn json(method: &'static str, path: &'static str, body: &str) -> Self {
            Self {
                method,
                path,
                body: body.into(),
                headers: Vec::new(),
                keep_alive: false,
                status: 200,
                drop_before_response: false,
                fault_gate: None,
            }
        }
        fn queued(path: &'static str, next: &str) -> Self {
            Self::json(
                "POST",
                path,
                &format!(r#"{{"id":"q","nextUri":"$BASE{next}"}}"#),
            )
        }
        fn header(mut self, name: &'static str, value: &'static str) -> Self {
            self.headers.push((name, value));
            self
        }
        fn keep_alive(mut self) -> Self {
            self.keep_alive = true;
            self
        }
    }
    struct Peer {
        port: u16,
        stop: Arc<AtomicBool>,
        requests: Arc<Mutex<Vec<Request>>>,
        worker: Option<JoinHandle<()>>,
    }
    impl Peer {
        fn start(replies: Vec<Reply>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            listener.set_nonblocking(true).unwrap();
            let stop = Arc::new(AtomicBool::new(false));
            let stopping = Arc::clone(&stop);
            let requests = Arc::new(Mutex::new(Vec::new()));
            let recorded = Arc::clone(&requests);
            let worker = thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(10);
                let mut active: Option<TcpStream> = None;
                let mut connection = 0;
                for reply in replies {
                    let mut request = loop {
                        if stopping.load(Ordering::Relaxed) {
                            return;
                        }
                        assert!(
                            Instant::now() < deadline,
                            "session test HTTP peer deadline expired"
                        );
                        if active.is_none() {
                            match listener.accept() {
                                Ok((peer, _)) => {
                                    peer.set_nonblocking(false).unwrap();
                                    peer.set_read_timeout(Some(Duration::from_millis(20)))
                                        .unwrap();
                                    peer.set_write_timeout(Some(Duration::from_secs(2)))
                                        .unwrap();
                                    active = Some(peer);
                                    connection += 1;
                                }
                                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                                    thread::sleep(Duration::from_millis(1));
                                    continue;
                                }
                                Err(error) => panic!("accept session test HTTP peer: {error}"),
                            }
                        }
                        if let Some(request) =
                            read_request(active.as_mut().unwrap(), &stopping, deadline)
                        {
                            break request;
                        }
                        active = None;
                    };
                    request.connection = connection;
                    assert_eq!(request.method, reply.method);
                    assert_eq!(request.path, reply.path);
                    recorded.lock().unwrap().push(request);
                    if let Some(gate) = reply.fault_gate.as_ref() {
                        gate.request_seen.store(true, Ordering::Release);
                        while gate.wait_for_action && !gate.action_done.load(Ordering::Acquire) {
                            if stopping.load(Ordering::Relaxed) {
                                return;
                            }
                            assert!(
                                Instant::now() < deadline,
                                "fault action did not release the HTTP peer"
                            );
                            thread::sleep(Duration::from_millis(1));
                        }
                    }
                    if reply.drop_before_response {
                        active = None;
                        continue;
                    }
                    let body = reply
                        .body
                        .replace("$BASE", &format!("http://127.0.0.1:{port}"));
                    let stream = active.as_mut().unwrap();
                    write!(stream,"HTTP/1.1 {} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: {}\r\n",reply.status,body.len(),if reply.keep_alive {"keep-alive"} else {"close"}).unwrap();
                    for (name, value) in reply.headers {
                        write!(stream, "{name}: {value}\r\n").unwrap();
                    }
                    write!(stream, "\r\n{body}").unwrap();
                    stream.flush().unwrap();
                    if !reply.keep_alive {
                        active = None;
                    }
                }
            });
            Self {
                port,
                stop,
                requests,
                worker: Some(worker),
            }
        }
        fn config(&self) -> ConnectionConfig {
            ConnectionConfig {
                time_zone: "Asia/Shanghai".into(),
                host: "127.0.0.1".into(),
                port: self.port.to_string(),
                user: "root".into(),
                catalog: None,
                db: None,
            }
        }
        fn session(&self) -> TrinoCaseSession {
            TrinoCaseSession::new_with_deadline(
                &self.config(),
                Some(Instant::now() + Duration::from_secs(10)),
            )
            .unwrap()
        }
        fn finish(mut self) -> Vec<Request> {
            self.stop.store(true, Ordering::Relaxed);
            self.worker.take().unwrap().join().unwrap();
            self.requests.lock().unwrap().clone()
        }
    }
    impl Drop for Peer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            if let Some(worker) = self.worker.take() {
                let _ = worker.join();
            }
        }
    }
    fn read_byte(peer: &mut TcpStream, stop: &AtomicBool, deadline: Instant) -> Option<u8> {
        let mut byte = [0];
        loop {
            if stop.load(Ordering::Relaxed) {
                return None;
            }
            assert!(
                Instant::now() < deadline,
                "session test HTTP request deadline expired"
            );
            match peer.read(&mut byte) {
                Ok(0) => return None,
                Ok(_) => return Some(byte[0]),
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) => {}
                Err(error) => panic!("read session test HTTP request: {error}"),
            }
        }
    }
    fn read_request(peer: &mut TcpStream, stop: &AtomicBool, deadline: Instant) -> Option<Request> {
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            head.push(read_byte(peer, stop, deadline)?);
        }
        let head = String::from_utf8(head).unwrap();
        let mut lines = head.split("\r\n");
        let mut line = lines.next().unwrap().split_whitespace();
        let method = line.next().unwrap().into();
        let path = line.next().unwrap().into();
        let headers = lines
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().into()))
            .collect::<BTreeMap<String, String>>();
        let length = headers
            .get("content-length")
            .map_or(0, |value| value.parse::<usize>().unwrap());
        let body = (0..length)
            .map(|_| read_byte(peer, stop, deadline))
            .collect::<Option<Vec<_>>>()?;
        Some(Request {
            method,
            path,
            headers,
            body: String::from_utf8(body).unwrap(),
            connection: 0,
        })
    }
    fn integer_result(name: &str, value: i64) -> String {
        format!(
            r#"{{"id":"q","columns":[{{"name":"{name}","type":"bigint","typeSignature":{{"rawType":"bigint","arguments":[]}}}}],"data":[[{value}]]}}"#
        )
    }

    #[test]
    fn initial_qualified_database_sets_separate_trino_headers() {
        let peer = Peer::start(boundary_success_replies(1));
        let mut config = peer.config();
        config.catalog = Some("initial".into());
        config.db = Some("`selected`.`first_namespace`".into());
        let mut session = TrinoCaseSession::new_with_deadline(&config, None).unwrap();
        let outcome = session.execute_query(2, "SELECT 7", None);
        let (ok, _, message) = outcome.summary();
        assert!(ok, "{message}");
        let requests = peer.finish();
        assert_eq!(requests[0].headers["x-trino-catalog"], "selected");
        assert_eq!(requests[0].headers["x-trino-schema"], "first_namespace");
    }

    #[test]
    fn database_override_preserves_qualified_and_unqualified_use_context() {
        for database in ["selected.first_namespace", "`selected`.`first_namespace`"] {
            let peer = Peer::start(boundary_success_replies(2));
            let mut session = peer.session();
            session.state.catalog = Some("initial".into());
            let outcome = session.execute_query(2, "SELECT 7", Some(database));
            let (ok, _, message) = outcome.summary();
            assert!(ok, "{message}");
            let outcome = session.execute_query(2, "SELECT 7", Some("second_namespace"));
            let (ok, _, message) = outcome.summary();
            assert!(ok, "{message}");
            let requests = peer.finish();
            assert_eq!(requests[0].headers["x-trino-catalog"], "selected");
            assert_eq!(requests[0].headers["x-trino-schema"], "first_namespace");
            assert_eq!(requests[2].headers["x-trino-catalog"], "selected");
            assert_eq!(requests[2].headers["x-trino-schema"], "second_namespace");
        }
    }

    #[test]
    fn invalid_initial_database_context_never_sends_a_request() {
        let peer = Peer::start(vec![]);
        for database in ["a.b.c", "a; SELECT 1", "a; USE b", "`a.b`"] {
            let mut config = peer.config();
            config.db = Some(database.into());
            assert!(TrinoCaseSession::new_with_deadline(&config, None).is_err());
        }
        assert!(peer.finish().is_empty());
    }

    #[test]
    fn invalid_database_override_keeps_context_and_never_sends_a_request() {
        let peer = Peer::start(vec![]);
        let mut session = peer.session();
        session.state.catalog = Some("original_catalog".into());
        session.state.schema = Some("original_namespace".into());
        let properties = session.state.properties.clone();
        for database in ["a.b.c", "a; SELECT 1", "a; USE b", "`a.b`"] {
            let outcome = session.execute_query(2, "SELECT 7", Some(database));
            let (ok, execution, message) = outcome.summary();
            assert!(!ok, "{database}: {message}");
            assert!(execution.is_none());
            assert!(!outcome.failure_is_statement());
            assert_eq!(session.state.catalog.as_deref(), Some("original_catalog"));
            assert_eq!(session.state.schema.as_deref(), Some("original_namespace"));
            assert_eq!(session.state.properties, properties);
        }
        assert!(peer.finish().is_empty());
    }

    #[test]
    fn completed_set_effects_are_sent_with_the_next_statement() {
        let peer = Peer::start(vec![
            Reply::queued("/v1/statement", "/set"),
            Reply::json("GET", "/set", r#"{"id":"q","updateType":"SET SESSION"}"#)
                .header("X-Trino-Set-Session", "pipeline_dop=4")
                .header("X-Trino-Set-Catalog", "cat")
                .header("X-Trino-Set-Schema", "db"),
            Reply::queued("/v1/statement", "/select"),
            Reply::json("GET", "/select", &integer_result("v", 42)),
        ]);
        let mut session = peer.session();
        let outcome = session.execute_query(9, "SET pipeline_dop = 4; SELECT 42 AS v;", None);
        let (ok, execution, message) = outcome.summary();
        assert!(ok, "{message}");
        assert_eq!(execution.unwrap().typed_results.len(), 1);
        assert_eq!(session.state.properties["pipeline_dop"], "4");
        let requests = peer.finish();
        assert_eq!(requests[2].body, "SELECT 42 AS v");
        assert_eq!(requests[2].headers["x-trino-catalog"], "cat");
        assert_eq!(requests[2].headers["x-trino-schema"], "db");
        assert!(requests[2].headers["x-trino-session"].contains("pipeline_dop=4"));
        assert!(requests[2].headers["x-trino-session"].contains("query_timeout=9"));
        assert_eq!(requests[2].headers["x-trino-time-zone"], "Asia/Shanghai");
    }
    #[test]
    fn multiple_returning_statements_keep_typed_results_and_original_flattening() {
        let peer = Peer::start(vec![
            Reply::queued("/v1/statement", "/one"),
            Reply::json("GET", "/one", &integer_result("a", 1)),
            Reply::queued("/v1/statement", "/two"),
            Reply::json("GET", "/two", &integer_result("b", 2)),
        ]);
        let outcome = peer
            .session()
            .execute_query(9, "SELECT 1 AS a; SELECT 2 AS b", None);
        let (ok, execution, message) = outcome.summary();
        assert!(ok, "{message}");
        let execution = execution.unwrap();
        assert_eq!(execution.typed_results.len(), 2);
        assert_eq!(
            execution.typed_results[0].rows,
            vec![vec![TrinoValue::Integer(1)]]
        );
        assert_eq!(execution.typed_results[1].columns[0].name, "b");
        assert_eq!(execution.header, vec!["a"]);
        assert_eq!(execution.rows, vec![vec!["1"], vec!["b"], vec!["2"]]);
        assert_eq!(execution.text_output, "a\n1\nb\n2");
        peer.finish();
    }
    #[test]
    fn before_execute_observes_the_query_after_post_and_before_first_get() {
        let peer = Peer::start(vec![
            Reply::queued("/v1/statement", "/one"),
            Reply::json("GET", "/one", &integer_result("v", 1)),
        ]);
        let requests = Arc::clone(&peer.requests);
        let mut observed = false;
        let outcome = peer.session().execute_query_with_before_execute(
            9,
            "SELECT 1",
            None,
            true,
            |attempt, index, query_id, sql, _| {
                assert_eq!((attempt, index), (0, 0));
                assert_eq!(query_id, "q");
                assert_eq!(sql, "SELECT 1");
                let requests = requests.lock().unwrap();
                assert_eq!(requests.len(), 1);
                assert_eq!(requests[0].method, "POST");
                observed = true;
                Ok(())
            },
        );
        let (ok, _, message) = outcome.summary();
        assert!(ok, "{message}");
        assert!(observed);
        assert_eq!(peer.finish()[1].method, "GET");
    }
    #[test]
    fn callback_denial_cancels_the_pending_query_without_a_get() {
        let mut canceled = Reply::json("DELETE", "/one", "");
        canceled.status = 204;
        let peer = Peer::start(vec![Reply::queued("/v1/statement", "/one"), canceled]);
        let outcome = peer.session().execute_query_with_before_execute(
            9,
            "SELECT 1",
            None,
            true,
            |_, _, _, _, _| bail!("observation denied"),
        );
        let (ok, execution, message) = outcome.summary();
        assert!(!ok);
        assert!(execution.is_none());
        assert!(message.contains("runner before-execute control failed"));
        assert!(message.contains("observation denied"));
        let requests = peer.finish();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].method, "DELETE");
    }
    #[test]
    fn suite_session_default_uses_headers_without_hidden_sql_and_survives_reconnect() {
        let peer = Peer::start(vec![
            Reply::queued("/v1/statement", "/one").keep_alive(),
            Reply::json("GET", "/one", &integer_result("v", 1)).keep_alive(),
            Reply::queued("/v1/statement", "/two").keep_alive(),
            Reply::json("GET", "/two", &integer_result("v", 2)).keep_alive(),
        ]);
        let defaults = crate::suite_manifest::SuiteSessionDefaults {
            pipeline_dop: Some(5),
        };
        let mut session = TrinoCaseSession::new_with_session_defaults(
            &peer.config(),
            Some(Instant::now() + Duration::from_secs(10)),
            &defaults,
        )
        .unwrap();
        let outcome = session.execute_query(9, "SELECT 1", None);
        let (ok, _, message) = outcome.summary();
        assert!(ok, "{message}");
        session.reconnect().unwrap();
        let outcome = session.execute_query(9, "SELECT 2", None);
        let (ok, _, message) = outcome.summary();
        assert!(ok, "{message}");
        let requests = peer.finish();
        assert_eq!(requests.len(), 4);
        assert_ne!(requests[0].connection, requests[2].connection);
        for request in &requests {
            assert!(request.headers["x-trino-session"].contains("pipeline_dop=5"));
        }
        let submitted: Vec<_> = requests
            .iter()
            .filter(|request| request.method == "POST")
            .map(|request| request.body.as_str())
            .collect();
        assert_eq!(submitted, vec!["SELECT 1", "SELECT 2"]);
    }

    #[test]
    fn successful_set_overrides_the_suite_default_across_reconnect() {
        let peer = Peer::start(vec![
            Reply::queued("/v1/statement", "/set").keep_alive(),
            Reply::json("GET", "/set", r#"{"id":"q"}"#)
                .header("X-Trino-Set-Session", "pipeline_dop=4")
                .keep_alive(),
            Reply::queued("/v1/statement", "/two").keep_alive(),
            Reply::json("GET", "/two", &integer_result("v", 2)).keep_alive(),
        ]);
        let defaults = crate::suite_manifest::SuiteSessionDefaults {
            pipeline_dop: Some(5),
        };
        let mut session = TrinoCaseSession::new_with_session_defaults(
            &peer.config(),
            Some(Instant::now() + Duration::from_secs(10)),
            &defaults,
        )
        .unwrap();
        let outcome = session.execute_query(9, "SET pipeline_dop = 4", None);
        let (ok, _, message) = outcome.summary();
        assert!(ok, "{message}");
        assert_eq!(session.state.properties["pipeline_dop"], "4");
        session.reconnect().unwrap();
        let outcome = session.execute_query(9, "SELECT 2", None);
        let (ok, _, message) = outcome.summary();
        assert!(ok, "{message}");
        let requests = peer.finish();
        assert_eq!(requests.len(), 4);
        assert!(requests[0].headers["x-trino-session"].contains("pipeline_dop=5"));
        assert!(requests[2].headers["x-trino-session"].contains("pipeline_dop=4"));
        assert_ne!(requests[0].connection, requests[2].connection);
        let submitted: Vec<_> = requests
            .iter()
            .filter(|request| request.method == "POST")
            .map(|request| request.body.as_str())
            .collect();
        assert_eq!(submitted, vec!["SET pipeline_dop = 4", "SELECT 2"]);
    }

    #[test]
    fn reconnect_replaces_the_http_pool_and_preserves_the_client_session() {
        let peer = Peer::start(vec![
            Reply::queued("/v1/statement", "/one").keep_alive(),
            Reply::json("GET", "/one", &integer_result("v", 1)).keep_alive(),
            Reply::queued("/v1/statement", "/two").keep_alive(),
            Reply::json("GET", "/two", &integer_result("v", 2)).keep_alive(),
        ]);
        let mut session = peer.session();
        session
            .state
            .properties
            .insert("pipeline_dop".into(), "4".into());
        let outcome = session.execute_query(9, "SELECT 1", Some("chosen"));
        let (ok, _, message) = outcome.summary();
        assert!(ok, "{message}");
        let original = session.state.clone();
        session.reconnect().unwrap();
        assert_eq!(session.state, original);
        let outcome = session.execute_query(9, "SELECT 2", None);
        let (ok, _, message) = outcome.summary();
        assert!(ok, "{message}");
        let requests = peer.finish();
        assert_eq!(requests[0].connection, requests[1].connection);
        assert_eq!(requests[2].connection, requests[3].connection);
        assert_ne!(requests[0].connection, requests[2].connection);
        assert_eq!(requests[2].headers["x-trino-schema"], "chosen");
        assert!(requests[2].headers["x-trino-session"].contains("pipeline_dop=4"));
    }
    #[test]
    fn full_utf8_trino_error_is_preserved_without_panicking() {
        let error = serde_json::json!({"id":"q","error":{"message":"中".repeat(1000),"errorCode":1,"errorName":"GENERIC_USER_ERROR","errorType":"USER_ERROR"}});
        let peer = Peer::start(vec![
            Reply::queued("/v1/statement", "/one"),
            Reply::json("GET", "/one", &error.to_string()),
        ]);
        let mut session = peer.session();
        let outcome = session.execute_query(9, "SELECT bad; SELECT 7", None);
        let (ok, execution, message) = outcome.summary();
        assert!(!ok);
        assert!(execution.is_none());
        assert!(outcome.failure_is_statement());
        assert!(message.contains("statement 1/2"));
        assert!(message.ends_with('中'));
        assert_eq!(message.matches('中').count(), 1000);
        assert_eq!(peer.finish().len(), 2);
    }
    fn trino_error(message: &str) -> String {
        serde_json::json!({"id":"q","error":{"message":message,"errorCode":1,"errorName":"GENERIC_USER_ERROR","errorType":"USER_ERROR"}}).to_string()
    }
    #[test]
    fn returned_outcomes_retain_complete_attempts_without_ambient_session_state() {
        let retry = "metadata file for version 1 is missing under /metadata";
        let peer = Peer::start(vec![
            Reply::queued("/v1/statement", "/first"),
            Reply::json("GET", "/first", &trino_error(retry)),
            Reply::queued("/v1/statement", "/second"),
            Reply::json("GET", "/second", &integer_result("v", 7)),
            Reply::queued("/v1/statement", "/third"),
            Reply::json("GET", "/third", &trino_error("later failure")),
            Reply::queued("/v1/statement", "/fourth"),
            Reply::json("GET", "/fourth", &integer_result("v", 9)),
        ]);
        let mut session = peer.session();
        let first = session.execute_query(9, "SELECT 7", None);
        let original = first.evidence();
        assert!(first.0);
        assert_eq!(first.3.len(), 2);
        assert!(matches!(
            first.3[0].statements[0].failures.as_slice(),
            [SqlExecutionFailure::Client(StatementFailure::Trino(_))]
        ));
        assert_eq!(
            first.3[1].execution.as_ref().unwrap().typed_results.len(),
            1
        );
        let later = session.execute_query(9, "SELECT missing", None);
        assert!(later.failure_is_statement());
        let retained_failure = later.evidence();
        assert!(session.execute_query(9, "SELECT 9", None).0);
        assert_eq!(later.evidence(), retained_failure);
        assert_eq!(
            crate::SqlStepOutcome::from_session(later).3,
            Some(crate::ExecutionFailureOrigin::Statement)
        );
        assert_eq!(
            first.evidence(),
            original,
            "later calls cannot rewrite an earlier outcome"
        );
        assert_eq!(peer.finish().len(), 8);
    }

    fn generic_user_expectation() -> QueryMeta {
        QueryMeta {
            expect_trino_error: Some(TrinoErrorExpectation {
                name: "GENERIC_USER_ERROR".into(),
                error_type: "USER_ERROR".into(),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn trino_error_expectation_consumes_one_real_complete_statement_outcome() {
        let peer = Peer::start(vec![
            Reply::queued("/v1/statement", "/bad"),
            Reply::json("GET", "/bad", &trino_error("marker")),
            Reply::queued("/v1/statement", "/second"),
            Reply::json("GET", "/second", &trino_error("extra failure")),
        ]);
        let mut session = peer.session();
        session.set_conversion_mode(true);
        let outcome = session.execute_query(9, "SELECT bad; SELECT also_bad", None);
        assert_eq!(outcome.3[0].statement_count, 2);
        assert_eq!(outcome.final_failures().len(), 2);
        assert!(
            crate::evaluate_expected_error_outcome(
                &generic_user_expectation(),
                false,
                &outcome.2,
                Some(&outcome)
            )
            .unwrap()
            .is_err()
        );
        let legacy = QueryMeta {
            expect_error: Some("marker".into()),
            ..Default::default()
        };
        assert!(
            crate::evaluate_expected_error_outcome(&legacy, false, &outcome.2, Some(&outcome))
                .unwrap()
                .is_err()
        );
        assert_eq!(peer.finish().len(), 4);
    }

    #[test]
    fn trino_error_expectation_never_retries_and_verify_record_share_typed_assertions() {
        for (mode, convert) in [
            (crate::Mode::Verify, false),
            (crate::Mode::Record, false),
            (crate::Mode::Record, true),
        ] {
            for kind in ["match", "name", "type", "success", "http", "socket"] {
                let mut meta = generic_user_expectation();
                meta.retry_count = Some(3);
                meta.retry_interval_ms = Some(0);
                let mut replies = match kind {
                    "success" => boundary_success_replies(1),
                    "http" => {
                        let mut reply =
                            Reply::json("POST", "/v1/statement", "GENERIC_USER_ERROR USER_ERROR");
                        reply.status = 503;
                        vec![reply]
                    }
                    "socket" => {
                        let mut reply = Reply::json("POST", "/v1/statement", "");
                        reply.drop_before_response = true;
                        vec![reply]
                    }
                    _ => vec![
                        Reply::queued("/v1/statement", "/bad"),
                        Reply::json(
                            "GET",
                            "/bad",
                            &trino_error("metadata file for version 1 is missing under /metadata"),
                        ),
                    ],
                };
                if kind == "name" {
                    meta.expect_trino_error.as_mut().unwrap().name = "OTHER".into();
                }
                if kind == "type" {
                    meta.expect_trino_error.as_mut().unwrap().error_type = "OTHER".into();
                }
                replies.extend(boundary_success_replies(1));
                let peer = Peer::start(replies);
                let temp = tempfile::TempDir::new().unwrap();
                let steps = vec![boundary_step(1, "SELECT bad", meta), boundary_cleanup(2)];
                let (case, evidence, _) = run_boundary_steps(
                    &peer,
                    &temp,
                    mode,
                    convert,
                    steps,
                    None,
                    Box::new(BoundaryServer),
                );
                assert_eq!(
                    matches!(case.status, crate::CaseStatus::Pass),
                    kind == "match",
                    "{mode:?} {kind}: {}",
                    case.log
                );
                assert!(oracle_recorded(
                    &evidence,
                    1,
                    "expected_error",
                    if kind == "match" { "pass" } else { "fail" }
                ));
                assert_eq!(
                    posted_requests(&peer.finish()),
                    ["SELECT bad", "SELECT 99"],
                    "no outer or transient retry"
                );
            }
        }
    }

    #[test]
    fn conversion_continues_after_domain_failure_with_unblessed_raw_results_and_success_effects_only()
     {
        let peer = Peer::start(vec![
            Reply::queued("/v1/statement", "/bad").header("X-Trino-Set-Session", "failed_post=1"),
            Reply::json("GET", "/bad", &trino_error("ordinary statement failure"))
                .header("X-Trino-Set-Session", "failed_get=2")
                .header("X-Trino-Set-Schema", "failed_schema"),
            Reply::queued("/v1/statement", "/seven"),
            Reply::json("GET", "/seven", &integer_result("v", 7))
                .header("X-Trino-Set-Session", "successful=3"),
        ]);
        let mut session = peer.session();
        session.set_conversion_mode(true);
        let outcome = session.execute_query(9, "SELECT bad; SELECT 7", None);
        let (ok, execution, message) = outcome.summary();
        assert!(!ok, "a partial conversion batch cannot succeed");
        assert!(outcome.failure_is_statement());
        assert!(message.contains("ordinary statement failure"));
        assert!(message.contains("statement 1/2"));
        let execution = execution.expect("raw successful suffix is retained for evidence");
        assert_eq!(execution.result_statements, vec!["SELECT 7"]);
        assert_eq!(execution.typed_results.len(), 1);
        assert_eq!(
            execution.typed_results[0].rows,
            vec![vec![TrinoValue::Integer(7)]]
        );
        assert!(!session.state.properties.contains_key("failed_post"));
        assert!(!session.state.properties.contains_key("failed_get"));
        assert!(session.state.schema.is_none());
        assert_eq!(session.state.properties["successful"], "3");
        let requests = peer.finish();
        assert_eq!(requests.len(), 4);
        assert_eq!(requests[2].body, "SELECT 7");
        assert!(!requests[2].headers["x-trino-session"].contains("failed_"));
        assert!(!requests[2].headers.contains_key("x-trino-schema"));
    }
    #[test]
    fn conversion_keeps_all_domain_failures_before_infrastructure_stops_the_batch() {
        let mut unavailable = Reply::json("POST", "/v1/statement", "upstream TrinoError text");
        unavailable.status = 503;
        let peer = Peer::start(vec![
            Reply::queued("/v1/statement", "/first"),
            Reply::json("GET", "/first", &trino_error("first domain failure")),
            Reply::queued("/v1/statement", "/second"),
            Reply::json("GET", "/second", &trino_error("second domain failure")),
            Reply::queued("/v1/statement", "/seven"),
            Reply::json("GET", "/seven", &integer_result("v", 7)),
            unavailable,
        ]);
        let mut session = peer.session();
        session.set_conversion_mode(true);
        let outcome = session.execute_query(
            9,
            "SELECT bad1; SELECT bad2; SELECT 7; SELECT unavailable; SELECT 9",
            None,
        );
        let (ok, execution, message) = outcome.summary();
        assert!(!ok);
        assert!(
            !outcome.failure_is_statement(),
            "HTTP failure overrides earlier domain errors"
        );
        let first = message.find("first domain failure").unwrap();
        let second = message.find("second domain failure").unwrap();
        let infrastructure = message.find("status=503").unwrap();
        assert!(first < second && second < infrastructure, "{message}");
        assert!(message.contains("statement 1/5"));
        assert!(message.contains("statement 2/5"));
        assert!(message.contains("statement 4/5"));
        let execution = execution.unwrap();
        assert_eq!(execution.result_statements, vec!["SELECT 7"]);
        assert_eq!(
            execution.typed_results[0].rows,
            vec![vec![TrinoValue::Integer(7)]]
        );
        assert_eq!(peer.finish().len(), 7);
    }
    #[test]
    fn http_error_body_cannot_impersonate_a_retryable_trino_statement_error() {
        let mut unavailable = Reply::json(
            "POST",
            "/v1/statement",
            "TrinoError metadata file for version 1 is missing under /metadata",
        );
        unavailable.status = 503;
        let peer = Peer::start(vec![unavailable]);
        let mut session = peer.session();
        session.set_conversion_mode(true);
        let outcome = session.execute_query(9, "SELECT 1; SELECT 7", None);
        let (ok, execution, message) = outcome.summary();
        assert!(!ok);
        assert!(!outcome.failure_is_statement());
        assert!(execution.unwrap().typed_results.is_empty());
        assert!(message.contains("status=503"), "{message}");
        assert_eq!(peer.finish().len(), 1);
    }
    #[test]
    fn normal_mode_keeps_the_two_attempt_typed_commit_retry_and_resets_failure_on_success() {
        for succeeds in [false, true] {
            let message = "metadata file for version 1 is missing under /metadata";
            let peer = Peer::start(vec![
                Reply::queued("/v1/statement", "/first"),
                Reply::json("GET", "/first", &trino_error(message)),
                Reply::queued("/v1/statement", "/second"),
                Reply::json(
                    "GET",
                    "/second",
                    &if succeeds {
                        integer_result("v", 7)
                    } else {
                        trino_error(message)
                    },
                ),
            ]);
            let mut session = peer.session();
            let outcome = session.execute_query(9, "SELECT 7", None);
            let (ok, execution, failure) = outcome.summary();
            assert_eq!(ok, succeeds, "{failure}");
            assert_eq!(outcome.failure_is_statement(), !succeeds);
            assert_eq!(execution.is_some(), succeeds);
            assert_eq!(
                peer.finish().len(),
                4,
                "retry budget remains exactly two attempts"
            );
        }
    }
    #[test]
    fn controlled_statement_does_not_replay_a_retryable_commit_failure() {
        let message = "metadata file for version 1 is missing under /metadata";
        let peer = Peer::start(vec![
            Reply::queued("/v1/statement", "/first"),
            Reply::json("GET", "/first", &trino_error(message)),
        ]);
        let mut session = peer.session();
        let mut observations = Vec::new();
        let outcome = session.execute_query_with_before_execute(
            9,
            "INSERT INTO t VALUES (1)",
            None,
            false,
            |attempt, statement, _, _, _| {
                observations.push((attempt, statement));
                Ok(())
            },
        );
        let (ok, _, failure) = outcome.summary();
        assert!(!ok);
        assert!(outcome.failure_is_statement(), "{failure}");
        assert!(failure.contains(message), "{failure}");
        assert_eq!(observations, vec![(0, 0)]);
        let requests = peer.finish();
        assert_eq!(
            requests.len(),
            2,
            "a cancellation target must not be replayed"
        );
        assert_eq!(requests[0].method, "POST");
        assert_eq!(requests[1].method, "GET");
    }

    #[test]
    fn conversion_does_not_replay_successful_suffix_after_retryable_domain_failure() {
        let peer = Peer::start(vec![
            Reply::queued("/v1/statement", "/first"),
            Reply::json(
                "GET",
                "/first",
                &trino_error("metadata file for version 1 is missing under /metadata"),
            ),
            Reply::queued("/v1/statement", "/seven"),
            Reply::json("GET", "/seven", &integer_result("v", 7)),
        ]);
        let mut session = peer.session();
        session.set_conversion_mode(true);
        let outcome = session.execute_query(9, "SELECT bad; SELECT 7", None);
        let (ok, execution, _) = outcome.summary();
        assert!(!ok);
        assert!(outcome.failure_is_statement());
        assert_eq!(execution.unwrap().result_statements, vec!["SELECT 7"]);
        assert_eq!(peer.finish().len(), 4);
    }
    #[test]
    fn conversion_control_failure_cannot_impersonate_a_statement_error_and_still_cancels() {
        let mut canceled = Reply::json("DELETE", "/one", "");
        canceled.status = 204;
        let peer = Peer::start(vec![Reply::queued("/v1/statement", "/one"), canceled]);
        let mut session = peer.session();
        session.set_conversion_mode(true);
        let outcome = session.execute_query_with_before_execute(
            9,
            "SELECT 1; SELECT 7",
            None,
            true,
            |_, _, _, _, _| {
                Err(
                    StatementFailure::Trino(
                        serde_json::from_str(
                            r#"{"message":"control used a typed statement error","errorCode":1,"errorName":"CONTROL","errorType":"USER_ERROR"}"#,
                        )
                        .unwrap(),
                    )
                    .into(),
                )
            },
        );
        let (ok, execution, message) = outcome.summary();
        assert!(!ok);
        assert!(!outcome.failure_is_statement());
        assert!(execution.unwrap().typed_results.is_empty());
        assert!(message.contains("runner before-execute control failed"));
        assert!(
            crate::evaluate_expected_error_outcome(
                &generic_user_expectation(),
                false,
                &outcome.2,
                Some(&outcome)
            )
            .unwrap()
            .is_err()
        );
        let requests = peer.finish();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].method, "DELETE");
    }
    // Exercise the real outer case loop with the existing private HTTP peer.
    // No second SQL owner, public bridge, feature, or dependency is introduced.
    struct BoundaryServer;
    impl crate::cluster::TestClusterOwner for BoundaryServer {
        fn target_host(&self) -> Option<&str> {
            None
        }
        fn target_port(&self) -> Option<u16> {
            None
        }
    }

    fn boundary_success_replies(count: usize) -> Vec<Reply> {
        (0..count)
            .flat_map(|_| {
                [
                    Reply::queued("/v1/statement", "/boundary-ok"),
                    Reply::json("GET", "/boundary-ok", &integer_result("v", 7)),
                ]
            })
            .collect()
    }

    fn boundary_step(
        query: usize,
        sql: &str,
        meta: crate::types::QueryMeta,
    ) -> crate::types::SqlStep {
        crate::types::SqlStep {
            query_number: query,
            sql: sql.into(),
            meta,
        }
    }

    fn run_boundary_steps(
        peer: &Peer,
        temp: &tempfile::TempDir,
        mode: crate::Mode,
        convert: bool,
        steps: Vec<crate::types::SqlStep>,
        initial_golden: Option<&str>,
        server: Box<dyn crate::cluster::TestClusterOwner>,
    ) -> (
        crate::CaseOutcome,
        crate::results_record::CaseEvidence,
        std::path::PathBuf,
    ) {
        run_boundary_steps_configured(
            peer,
            temp,
            mode,
            convert,
            steps,
            initial_golden,
            (server, |_| {}),
        )
    }

    fn run_boundary_steps_configured(
        peer: &Peer,
        temp: &tempfile::TempDir,
        mode: crate::Mode,
        convert: bool,
        steps: Vec<crate::types::SqlStep>,
        initial_golden: Option<&str>,
        owner_and_setup: (
            Box<dyn crate::cluster::TestClusterOwner>,
            impl FnOnce(&mut crate::SuiteRunContext),
        ),
    ) -> (
        crate::CaseOutcome,
        crate::results_record::CaseEvidence,
        std::path::PathBuf,
    ) {
        let (server, configure) = owner_and_setup;
        use crate::types::SqlCase;
        use novarocks_cluster_harness::fixture_run::{FixturePolicy, RunBudget, ScratchIdentity};
        let results = temp.path().join("results");
        std::fs::create_dir(&results).unwrap();
        let golden = results.join("boundary.result");
        if let Some(initial) = initial_golden {
            std::fs::write(&golden, initial).unwrap();
        }
        let source = temp.path().join("boundary.sql");
        std::fs::write(
            &source,
            steps
                .iter()
                .map(|s| s.sql.as_str())
                .collect::<Vec<_>>()
                .join(";\n"),
        )
        .unwrap();
        let config = peer.config();
        let mut ctx = crate::SuiteRunContext {
            case_input: None,
            reference: Default::default(),
            fixture_budget: RunBudget::admit(
                FixturePolicy::default(),
                ScratchIdentity::create("s3://novarocks/sql-test").unwrap(),
                600,
                None,
            )
            .unwrap(),
            fixture_storage: None,
            fixture_writers_settled: Arc::new(AtomicBool::new(true)),
            namespace_catalogs: Default::default(),
            suite_name: "boundary".into(),
            controls: None,
            mode,
            convert_legacy: convert,
            legacy_variant_offset_seconds: convert.then_some(0),
            target_conn_base: config.clone(),
            target_admin_conn: config,
            result_dir: Some(results),
            actual_artifact_dir: None,
            verify_enabled: true,
            query_timeout: 2,
            auto_case_db: false,
            order_sensitive_default: false,
            float_epsilon: None,
            preview_lines: 0,
            update_expected: false,
            session_defaults: Default::default(),
            target_session_sql: vec![],
            rewrite_explain_contains_as_not_contains: vec![],
            fail_fast: false,
            cluster_owner: Arc::new(Mutex::new(server)),
            failure_artifacts: Arc::new(crate::failure_artifacts::FailureArtifactRecorder::new(
                crate::failure_artifacts::FailureArtifactContext {
                    root: temp.path().join("failures"),
                    lane: "correctness".into(),
                    suites: vec!["boundary".into()],
                },
            )),
            publication_catalog_control: None,
            publication_service_control: None,
            results: None,
        };
        configure(&mut ctx);
        let case = SqlCase {
            expected_variables: Default::default(),
            namespace_rewrites: Default::default(),
            auto_db: String::new(),
            source_file: source,
            case_id: "boundary".into(),
            steps,
            case_dbs: vec![],
            sequential: false,
        };
        let mut evidence = crate::results_record::CaseEvidence::default();
        let outcome = crate::run_case_observed(&ctx, &case, &AtomicBool::new(false), &mut evidence);
        (outcome, evidence, golden)
    }

    fn run_boundary_case(
        peer: &Peer,
        temp: &tempfile::TempDir,
        mode: crate::Mode,
        convert: bool,
        setup: Option<&str>,
        failed_sql: &str,
        expected: &str,
    ) -> (
        crate::CaseOutcome,
        crate::results_record::CaseEvidence,
        std::path::PathBuf,
        usize,
    ) {
        use crate::types::QueryMeta;
        let mut steps = Vec::new();
        if let Some(sql) = setup {
            steps.push(boundary_step(
                1,
                sql,
                QueryMeta {
                    skip_result_check: true,
                    ..Default::default()
                },
            ));
        }
        let failed = steps.len() + 1;
        steps.push(boundary_step(
            failed,
            failed_sql,
            QueryMeta {
                expect_error: Some(expected.into()),
                retry_count: Some(2),
                ..Default::default()
            },
        ));
        let ordinary = failed + 1;
        steps.push(boundary_step(ordinary, "SELECT 7", QueryMeta::default()));
        steps.push(boundary_step(
            ordinary + 1,
            "SELECT 99",
            QueryMeta {
                cleanup: true,
                skip_result_check: true,
                ..Default::default()
            },
        ));
        let block = if convert {
            "-- legacy\nv\n7\n"
        } else {
            "-- types\tbigint\nv\n7\n"
        };
        let initial = format!("-- query {ordinary}\n{block}");
        let (outcome, evidence, golden) = run_boundary_steps(
            peer,
            temp,
            mode,
            convert,
            steps,
            Some(&initial),
            Box::new(BoundaryServer),
        );
        (outcome, evidence, golden, failed)
    }

    fn assert_outer_infrastructure_rejected(
        peer: Peer,
        mode: crate::Mode,
        convert: bool,
        setup: Option<&str>,
        failed_sql: &str,
        expected: &str,
        failed_statement_was_submitted: bool,
    ) {
        let temp = tempfile::TempDir::new().unwrap();
        let (outcome, evidence, golden, failed) =
            run_boundary_case(&peer, &temp, mode, convert, setup, failed_sql, expected);
        assert!(
            matches!(outcome.status, crate::CaseStatus::Fail),
            "{}",
            outcome.log
        );
        assert!(
            evidence
                .executed
                .iter()
                .any(|e| e["kind"] == "expected_error"
                    && e["step"] == failed
                    && e["outcome"] == "fail")
        );
        assert!(
            !evidence
                .executed
                .iter()
                .any(|e| e["kind"] == "expected_error"
                    && e["step"] == failed
                    && e["outcome"] == "pass")
        );
        let requests = peer.finish();
        let posted = requests
            .iter()
            .filter(|r| r.method == "POST")
            .map(|r| r.body.as_str())
            .collect::<Vec<_>>();
        let mut expected_posts = setup.into_iter().collect::<Vec<_>>();
        if failed_statement_was_submitted {
            expected_posts.push(failed_sql);
        }
        expected_posts.push("SELECT 99");
        assert_eq!(
            posted, expected_posts,
            "ordinary successor or retry must not execute; cleanup must execute"
        );
        if convert {
            let text = std::fs::read_to_string(golden).unwrap();
            assert!(text.contains("-- legacy\nv\n7"));
            assert!(
                !text.contains("-- types"),
                "unissued successor cannot upgrade its oracle"
            );
        }
    }

    #[test]
    fn outer_verify_and_conversion_http_errors_cannot_match_sql_expected_error() {
        for (mode, convert) in [(crate::Mode::Verify, false), (crate::Mode::Record, true)] {
            let mut unavailable = Reply::json("POST", "/v1/statement", "expected marker");
            unavailable.status = 503;
            let mut replies = vec![unavailable];
            replies.extend(boundary_success_replies(2));
            assert_outer_infrastructure_rejected(
                Peer::start(replies),
                mode,
                convert,
                None,
                "SELECT unavailable",
                "expected marker",
                true,
            );
        }
    }

    #[test]
    fn outer_verify_and_conversion_socket_errors_cannot_match_sql_expected_error() {
        for (mode, convert) in [(crate::Mode::Verify, false), (crate::Mode::Record, true)] {
            let mut disconnected = Reply::json("POST", "/v1/statement", "");
            disconnected.drop_before_response = true;
            let mut replies = vec![disconnected];
            replies.extend(boundary_success_replies(2));
            // The common diagnostic wrapper contains this exact text even for
            // a socket EOF. Diagnostic text cannot confer a statement author.
            assert_outer_infrastructure_rejected(
                Peer::start(replies),
                mode,
                convert,
                None,
                "SELECT disconnected",
                "statement 1/1",
                true,
            );
        }
    }

    #[test]
    fn outer_verify_and_conversion_local_control_cannot_match_sql_expected_error() {
        let setup = r#"CREATE EXTERNAL CATALOG boundary_catalog PROPERTIES ("type"="iceberg", "iceberg.catalog.type"="rest", "uri"="http://127.0.0.1:38001")"#;
        for (mode, convert) in [(crate::Mode::Verify, false), (crate::Mode::Record, true)] {
            // The setup publishes only local catalog facts through the real
            // session guard. The next namespace write is rejected locally,
            // before POST; no test-only execution hook is required.
            assert_outer_infrastructure_rejected(
                Peer::start(boundary_success_replies(3)),
                mode,
                convert,
                Some(setup),
                "CREATE DATABASE boundary_catalog.foreign",
                "shared REST write requires an exact namespace",
                false,
            );
        }
    }

    #[test]
    fn outer_conversion_keeps_real_statement_error_matching_and_continues() {
        let mut replies = vec![
            Reply::queued("/v1/statement", "/boundary-bad"),
            Reply::json("GET", "/boundary-bad", &trino_error("expected marker")),
        ];
        replies.extend(boundary_success_replies(2));
        let peer = Peer::start(replies);
        let temp = tempfile::TempDir::new().unwrap();
        let (outcome, evidence, golden, failed) = run_boundary_case(
            &peer,
            &temp,
            crate::Mode::Record,
            true,
            None,
            "SELECT bad",
            "expected marker",
        );
        assert!(
            matches!(outcome.status, crate::CaseStatus::Pass),
            "{}",
            outcome.log
        );
        assert!(
            evidence
                .executed
                .iter()
                .any(|e| e["kind"] == "expected_error"
                    && e["step"] == failed
                    && e["outcome"] == "pass")
        );
        assert!(
            evidence
                .executed
                .iter()
                .any(|e| e["kind"] == "legacy_conversion"
                    && e["step"] == failed + 1
                    && e["outcome"] == "pass")
        );
        let requests = peer.finish();
        assert_eq!(
            requests
                .iter()
                .filter(|r| r.method == "POST")
                .map(|r| r.body.as_str())
                .collect::<Vec<_>>(),
            vec!["SELECT bad", "SELECT 7", "SELECT 99"]
        );
        assert!(
            std::fs::read_to_string(golden)
                .unwrap()
                .contains("-- types\tbigint")
        );
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum FrontendFaultMode {
        Confirmed,
        MissingMarker,
        FailKill,
        FailClear,
        FailRestart,
    }

    struct FrontendBoundaryServer {
        mode: FrontendFaultMode,
        gate: Arc<FaultPeerGate>,
        calls: Arc<Mutex<[usize; 3]>>,
    }
    impl crate::cluster::TestClusterOwner for FrontendBoundaryServer {
        fn target_host(&self) -> Option<&str> {
            None
        }
        fn target_port(&self) -> Option<u16> {
            None
        }
        fn supports_fault_injection(&self) -> bool {
            true
        }
        fn be_count(&self) -> usize {
            1
        }
        fn be_log_count(&self, _index: usize, _needle: &str) -> anyhow::Result<usize> {
            Ok(
                if self.mode != FrontendFaultMode::MissingMarker
                    && self.gate.request_seen.load(Ordering::Acquire)
                {
                    1
                } else {
                    0
                },
            )
        }
        fn be_log_contents(&self, _index: usize) -> anyhow::Result<String> {
            Ok(String::new())
        }
        fn fe_log_contents(&self) -> anyhow::Result<String> {
            Ok(String::new())
        }
        fn kill_fe(&mut self) -> anyhow::Result<()> {
            self.calls.lock().unwrap()[0] += 1;
            if self.mode == FrontendFaultMode::FailKill {
                self.gate.action_done.store(true, Ordering::Release);
                anyhow::bail!("boundary original FE kill failed");
            }
            Ok(())
        }
        fn clear_query_lifecycle_faults(&mut self) -> anyhow::Result<()> {
            self.calls.lock().unwrap()[1] += 1;
            if self.mode == FrontendFaultMode::FailClear {
                self.gate.action_done.store(true, Ordering::Release);
                anyhow::bail!("boundary original fault clear failed");
            }
            Ok(())
        }
        fn restart_fe_until(&mut self, _deadline: Instant) -> anyhow::Result<()> {
            self.calls.lock().unwrap()[2] += 1;
            self.gate.action_done.store(true, Ordering::Release);
            if self.mode == FrontendFaultMode::FailRestart {
                anyhow::bail!("boundary original FE restart failed");
            }
            Ok(())
        }
    }

    fn boundary_cleanup(query: usize) -> crate::types::SqlStep {
        boundary_step(
            query,
            "SELECT 99",
            crate::types::QueryMeta {
                cleanup: true,
                skip_result_check: true,
                ..Default::default()
            },
        )
    }
    fn posted_requests(requests: &[Request]) -> Vec<&str> {
        requests
            .iter()
            .filter(|r| r.method == "POST")
            .map(|r| r.body.as_str())
            .collect()
    }
    fn oracle_recorded(
        evidence: &crate::results_record::CaseEvidence,
        step: usize,
        kind: &str,
        outcome: &str,
    ) -> bool {
        evidence
            .executed
            .iter()
            .any(|e| e["step"] == step && e["kind"] == kind && e["outcome"] == outcome)
    }
    fn boundary_values(values: &[i64]) -> Vec<Reply> {
        values
            .iter()
            .flat_map(|v| {
                [
                    Reply::queued("/v1/statement", "/boundary-value"),
                    Reply::json("GET", "/boundary-value", &integer_result("v", *v)),
                ]
            })
            .collect()
    }

    #[test]
    fn outer_confirmed_fe_disconnect_keeps_expected_error_and_successor_oracles() {
        for (mode, convert) in [(crate::Mode::Verify, false), (crate::Mode::Record, true)] {
            let gate = Arc::new(FaultPeerGate {
                wait_for_action: true,
                ..Default::default()
            });
            let calls = Arc::new(Mutex::new([0; 3]));
            let mut disconnected = Reply::json("POST", "/v1/statement", "");
            disconnected.drop_before_response = true;
            disconnected.fault_gate = Some(Arc::clone(&gate));
            let mut replies = vec![disconnected];
            replies.extend(boundary_success_replies(2));
            let peer = Peer::start(replies);
            let temp = tempfile::TempDir::new().unwrap();
            let steps = vec![
                boundary_step(
                    1,
                    "SELECT fault",
                    crate::types::QueryMeta {
                        kill_fe_after_be_log_contains: Some("BOUNDARY_QUERY_ACTIVE".into()),
                        expect_error: Some("Trino transport failure".into()),
                        retry_count: Some(2),
                        retry_interval_ms: Some(0),
                        ..Default::default()
                    },
                ),
                boundary_step(2, "SELECT 7", Default::default()),
                boundary_cleanup(3),
            ];
            let initial = if convert {
                "-- query 2\n-- legacy\nv\n7\n"
            } else {
                "-- query 2\n-- types\tbigint\nv\n7\n"
            };
            let (outcome, evidence, golden) = run_boundary_steps(
                &peer,
                &temp,
                mode,
                convert,
                steps,
                Some(initial),
                Box::new(FrontendBoundaryServer {
                    mode: FrontendFaultMode::Confirmed,
                    gate,
                    calls: Arc::clone(&calls),
                }),
            );
            assert!(
                matches!(outcome.status, crate::CaseStatus::Pass),
                "{}",
                outcome.log
            );
            assert!(oracle_recorded(&evidence, 1, "expected_error", "pass"));
            if convert {
                assert!(oracle_recorded(&evidence, 2, "legacy_conversion", "pass"));
                assert!(
                    std::fs::read_to_string(golden)
                        .unwrap()
                        .contains("-- types\tbigint")
                );
            }
            // The fault owner clears once; the outer step guard clears at exit.
            assert_eq!(*calls.lock().unwrap(), [1, 2, 1]);
            assert_eq!(
                posted_requests(&peer.finish()),
                ["SELECT fault", "SELECT 7", "SELECT 99"]
            );
        }
    }

    #[test]
    fn outer_unconfirmed_or_failed_fe_fault_never_authorizes_expected_disconnect() {
        for mode in [
            FrontendFaultMode::MissingMarker,
            FrontendFaultMode::FailKill,
            FrontendFaultMode::FailClear,
            FrontendFaultMode::FailRestart,
        ] {
            for (runner_mode, convert) in
                [(crate::Mode::Verify, false), (crate::Mode::Record, true)]
            {
                let gate = Arc::new(FaultPeerGate {
                    wait_for_action: mode != FrontendFaultMode::MissingMarker,
                    ..Default::default()
                });
                let calls = Arc::new(Mutex::new([0; 3]));
                let mut disconnected = Reply::json("POST", "/v1/statement", "");
                disconnected.drop_before_response = true;
                disconnected.fault_gate = Some(Arc::clone(&gate));
                let mut replies = vec![disconnected];
                replies.extend(boundary_success_replies(2));
                let peer = Peer::start(replies);
                let temp = tempfile::TempDir::new().unwrap();
                let steps = vec![
                    boundary_step(
                        1,
                        "SELECT fault",
                        crate::types::QueryMeta {
                            kill_fe_after_be_log_contains: Some("BOUNDARY_QUERY_ACTIVE".into()),
                            expect_error: Some("runner fault injection".into()),
                            retry_count: Some(2),
                            retry_interval_ms: Some(0),
                            ..Default::default()
                        },
                    ),
                    boundary_step(2, "SELECT 7", Default::default()),
                    boundary_cleanup(3),
                ];
                let initial = if convert {
                    "-- query 2\n-- legacy\nv\n7\n"
                } else {
                    "-- query 2\n-- types\tbigint\nv\n7\n"
                };
                let (outcome, evidence, golden) = run_boundary_steps(
                    &peer,
                    &temp,
                    runner_mode,
                    convert,
                    steps,
                    Some(initial),
                    Box::new(FrontendBoundaryServer {
                        mode,
                        gate,
                        calls: Arc::clone(&calls),
                    }),
                );
                assert!(
                    matches!(outcome.status, crate::CaseStatus::Fail),
                    "{mode:?}: {}",
                    outcome.log
                );
                assert!(oracle_recorded(&evidence, 1, "expected_error", "fail"));
                assert!(!oracle_recorded(&evidence, 1, "expected_error", "pass"));
                assert_eq!(
                    posted_requests(&peer.finish()),
                    ["SELECT fault", "SELECT 99"]
                );
                assert_eq!(std::fs::read_to_string(golden).unwrap(), initial);
                // The outer step guard also clears after any failed fault.
                let expected = match mode {
                    FrontendFaultMode::MissingMarker => [0, 1, 0],
                    FrontendFaultMode::FailKill => [1, 1, 0],
                    FrontendFaultMode::FailClear => [1, 2, 0],
                    FrontendFaultMode::FailRestart => [1, 2, 1],
                    FrontendFaultMode::Confirmed => unreachable!(),
                };
                assert_eq!(*calls.lock().unwrap(), expected);
            }
        }
    }

    #[test]
    fn outer_confirmed_fe_fault_does_not_authorize_http_or_protocol_failure() {
        for http_status in [503, 200] {
            for (mode, convert) in [(crate::Mode::Verify, false), (crate::Mode::Record, true)] {
                let gate = Arc::new(FaultPeerGate {
                    wait_for_action: true,
                    ..Default::default()
                });
                let calls = Arc::new(Mutex::new([0; 3]));
                let mut reply = Reply::json("POST", "/v1/statement", "invalid response");
                reply.status = http_status;
                reply.fault_gate = Some(Arc::clone(&gate));
                let mut replies = vec![reply];
                replies.extend(boundary_success_replies(2));
                let peer = Peer::start(replies);
                let temp = tempfile::TempDir::new().unwrap();
                let steps = vec![
                    boundary_step(
                        1,
                        "SELECT fault",
                        crate::types::QueryMeta {
                            kill_fe_after_be_log_contains: Some("BOUNDARY_QUERY_ACTIVE".into()),
                            expect_error: Some("statement 1/1".into()),
                            retry_count: Some(2),
                            retry_interval_ms: Some(0),
                            ..Default::default()
                        },
                    ),
                    boundary_step(2, "SELECT 7", Default::default()),
                    boundary_cleanup(3),
                ];
                let initial = if convert {
                    "-- query 2\n-- legacy\nv\n7\n"
                } else {
                    "-- query 2\n-- types\tbigint\nv\n7\n"
                };
                let (outcome, evidence, golden) = run_boundary_steps(
                    &peer,
                    &temp,
                    mode,
                    convert,
                    steps,
                    Some(initial),
                    Box::new(FrontendBoundaryServer {
                        mode: FrontendFaultMode::Confirmed,
                        gate,
                        calls: Arc::clone(&calls),
                    }),
                );
                assert!(
                    matches!(outcome.status, crate::CaseStatus::Fail),
                    "{}",
                    outcome.log
                );
                assert!(oracle_recorded(&evidence, 1, "expected_error", "fail"));
                // The fault owner clears once; the outer step guard clears at exit.
                assert_eq!(*calls.lock().unwrap(), [1, 2, 1]);
                assert_eq!(
                    posted_requests(&peer.finish()),
                    ["SELECT fault", "SELECT 99"]
                );
                assert_eq!(std::fs::read_to_string(golden).unwrap(), initial);
            }
        }
    }

    #[test]
    fn outer_conversion_persists_existing_legacy_when_every_result_is_optional() {
        for meta in [
            crate::types::QueryMeta {
                result_contains: vec!["7".into()],
                ..Default::default()
            },
            crate::types::QueryMeta {
                result_rows_count: Some(1),
                ..Default::default()
            },
            crate::types::QueryMeta {
                skip_result_check: true,
                ..Default::default()
            },
        ] {
            let peer = Peer::start(boundary_success_replies(2));
            let temp = tempfile::TempDir::new().unwrap();
            let steps = vec![boundary_step(1, "SELECT 7", meta), boundary_cleanup(2)];
            let (outcome, evidence, golden) = run_boundary_steps(
                &peer,
                &temp,
                crate::Mode::Record,
                true,
                steps,
                Some("-- query 1\n-- legacy\nv\n7\n"),
                Box::new(BoundaryServer),
            );
            assert!(
                matches!(outcome.status, crate::CaseStatus::Pass),
                "{}",
                outcome.log
            );
            assert!(oracle_recorded(&evidence, 1, "legacy_conversion", "pass"));
            assert!(
                std::fs::read_to_string(golden)
                    .unwrap()
                    .contains("-- types\tbigint\nv\n7")
            );
            assert_eq!(posted_requests(&peer.finish()), ["SELECT 7", "SELECT 99"]);
        }
    }

    #[test]
    fn outer_conversion_text_match_does_not_replace_a_full_old_result_mismatch() {
        let peer = Peer::start(boundary_success_replies(3));
        let temp = tempfile::TempDir::new().unwrap();
        let steps = vec![
            boundary_step(
                1,
                "SELECT 7",
                crate::types::QueryMeta {
                    result_contains: vec!["7".into()],
                    ..Default::default()
                },
            ),
            boundary_step(2, "SELECT 7", Default::default()),
            boundary_cleanup(3),
        ];
        let (outcome, evidence, golden) = run_boundary_steps(
            &peer,
            &temp,
            crate::Mode::Record,
            true,
            steps,
            Some("-- query 1\n-- legacy\nv\n8\n\n-- query 2\n-- legacy\nv\n7\n"),
            Box::new(BoundaryServer),
        );
        assert!(
            matches!(outcome.status, crate::CaseStatus::Fail),
            "{}",
            outcome.log
        );
        assert!(oracle_recorded(&evidence, 1, "text_assertions", "pass"));
        assert!(oracle_recorded(&evidence, 1, "legacy_conversion", "fail"));
        assert!(oracle_recorded(&evidence, 2, "legacy_conversion", "pass"));
        let sets =
            crate::results::load_expected_results_with_variables(&golden, &Default::default())
                .unwrap();
        assert!(matches!(
            sets[&1].golden.as_ref().unwrap().source.blocks.as_slice(),
            [crate::golden::Block::Legacy { .. }]
        ));
        assert_eq!(sets[&1].rows, [vec!["8".to_string()]]);
        assert!(matches!(
            sets[&2].golden.as_ref().unwrap().source.blocks.as_slice(),
            [crate::golden::Block::Typed { .. }]
        ));
        assert_eq!(
            posted_requests(&peer.finish()),
            ["SELECT 7", "SELECT 7", "SELECT 99"]
        );
    }

    #[test]
    fn outer_conversion_proves_select_before_set_or_drop_as_a_complete_result() {
        for suffix in ["SET pipeline_dop = 5", "DROP TABLE boundary_table"] {
            let mut replies = boundary_success_replies(1);
            replies.extend([
                Reply::queued("/v1/statement", "/boundary-update"),
                Reply::json(
                    "GET",
                    "/boundary-update",
                    r#"{"id":"q","updateType":"CONTROL"}"#,
                ),
            ]);
            replies.extend(boundary_success_replies(1));
            let peer = Peer::start(replies);
            let temp = tempfile::TempDir::new().unwrap();
            let sql = format!("SELECT 7; {suffix}");
            let steps = vec![
                boundary_step(1, &sql, Default::default()),
                boundary_cleanup(2),
            ];
            let (outcome, evidence, golden) = run_boundary_steps(
                &peer,
                &temp,
                crate::Mode::Record,
                true,
                steps,
                Some("-- query 1\n-- legacy\nv\n7\n"),
                Box::new(BoundaryServer),
            );
            assert!(
                matches!(outcome.status, crate::CaseStatus::Pass),
                "{}",
                outcome.log
            );
            assert!(oracle_recorded(&evidence, 1, "legacy_conversion", "pass"));
            assert!(
                std::fs::read_to_string(golden)
                    .unwrap()
                    .contains("-- types\tbigint\nv\n7")
            );
            assert_eq!(
                posted_requests(&peer.finish()),
                ["SELECT 7", suffix, "SELECT 99"]
            );
        }
    }

    #[test]
    fn outer_conversion_does_not_invent_an_optional_section_or_a_missing_file() {
        for existing_file in [false, true] {
            let peer = Peer::start(boundary_success_replies(if existing_file { 3 } else { 2 }));
            let temp = tempfile::TempDir::new().unwrap();
            let mut steps = vec![boundary_step(
                1,
                "SELECT 7",
                crate::types::QueryMeta {
                    result_contains: vec!["7".into()],
                    ..Default::default()
                },
            )];
            if existing_file {
                steps.push(boundary_step(2, "SELECT 7", Default::default()));
            }
            steps.push(boundary_cleanup(steps.len() + 1));
            let initial = existing_file.then_some("-- query 2\n-- legacy\nv\n7\n");
            let (outcome, evidence, golden) = run_boundary_steps(
                &peer,
                &temp,
                crate::Mode::Record,
                true,
                steps,
                initial,
                Box::new(BoundaryServer),
            );
            assert!(
                matches!(outcome.status, crate::CaseStatus::Pass),
                "{}",
                outcome.log
            );
            assert!(!oracle_recorded(&evidence, 1, "legacy_conversion", "pass"));
            if existing_file {
                let sets = crate::results::load_expected_results_with_variables(
                    &golden,
                    &Default::default(),
                )
                .unwrap();
                assert_eq!(sets.keys().copied().collect::<Vec<_>>(), [2]);
            } else {
                assert!(!golden.exists());
            }
            let requests = peer.finish();
            assert_eq!(
                requests.iter().filter(|r| r.method == "POST").count(),
                if existing_file { 3 } else { 2 }
            );
        }
    }

    #[test]
    fn outer_ordinary_record_optional_oracle_does_not_gain_conversion_authoring() {
        let peer = Peer::start(boundary_success_replies(2));
        let temp = tempfile::TempDir::new().unwrap();
        let initial = "-- query 1\n-- legacy\nv\n7\n";
        let steps = vec![
            boundary_step(
                1,
                "SELECT 7",
                crate::types::QueryMeta {
                    result_contains: vec!["7".into()],
                    ..Default::default()
                },
            ),
            boundary_cleanup(2),
        ];
        let (outcome, evidence, golden) = run_boundary_steps(
            &peer,
            &temp,
            crate::Mode::Record,
            false,
            steps,
            Some(initial),
            Box::new(BoundaryServer),
        );
        assert!(
            matches!(outcome.status, crate::CaseStatus::Pass),
            "{}",
            outcome.log
        );
        assert!(
            !evidence
                .executed
                .iter()
                .any(|e| e["kind"] == "legacy_conversion")
        );
        assert_eq!(std::fs::read_to_string(golden).unwrap(), initial);
        assert_eq!(posted_requests(&peer.finish()), ["SELECT 7", "SELECT 99"]);
    }

    #[test]
    fn outer_conversion_retries_text_and_rowset_without_freezing_early_failure() {
        for text in [false, true] {
            let peer = Peer::start(boundary_values(&[0, 2, 7, 7]));
            let temp = tempfile::TempDir::new().unwrap();
            let steps = vec![
                boundary_step(
                    1,
                    "SELECT poll",
                    crate::types::QueryMeta {
                        result_contains: if text { vec!["2".into()] } else { vec![] },
                        retry_count: Some(2),
                        retry_interval_ms: Some(0),
                        ..Default::default()
                    },
                ),
                boundary_step(
                    2,
                    "SELECT 7",
                    crate::types::QueryMeta {
                        skip_result_check: true,
                        ..Default::default()
                    },
                ),
                boundary_cleanup(3),
            ];
            let (outcome, evidence, golden) = run_boundary_steps(
                &peer,
                &temp,
                crate::Mode::Record,
                true,
                steps,
                Some("-- query 1\n-- legacy\nv\n2\n"),
                Box::new(BoundaryServer),
            );
            assert!(
                matches!(outcome.status, crate::CaseStatus::Pass),
                "{}",
                outcome.log
            );
            assert!(evidence.failure.is_none());
            assert!(oracle_recorded(&evidence, 1, "legacy_conversion", "pass"));
            assert!(
                std::fs::read_to_string(golden)
                    .unwrap()
                    .contains("-- types\tbigint\nv\n2")
            );
            assert_eq!(
                posted_requests(&peer.finish()),
                ["SELECT poll", "SELECT poll", "SELECT 7", "SELECT 99"]
            );
        }
    }

    #[test]
    fn outer_conversion_exhausted_rowset_retry_keeps_original_and_last_actual_failure() {
        let peer = Peer::start(boundary_values(&[0, 1, 7, 7]));
        let temp = tempfile::TempDir::new().unwrap();
        let initial = "-- query 1\n-- legacy\nv\n2\n";
        let steps = vec![
            boundary_step(
                1,
                "SELECT poll",
                crate::types::QueryMeta {
                    retry_count: Some(2),
                    retry_interval_ms: Some(0),
                    ..Default::default()
                },
            ),
            boundary_step(
                2,
                "SELECT 7",
                crate::types::QueryMeta {
                    skip_result_check: true,
                    ..Default::default()
                },
            ),
            boundary_cleanup(3),
        ];
        let (outcome, evidence, golden) = run_boundary_steps(
            &peer,
            &temp,
            crate::Mode::Record,
            true,
            steps,
            Some(initial),
            Box::new(BoundaryServer),
        );
        assert!(
            matches!(outcome.status, crate::CaseStatus::Fail),
            "{}",
            outcome.log
        );
        assert!(oracle_recorded(&evidence, 1, "legacy_conversion", "fail"));
        assert_eq!(std::fs::read_to_string(golden).unwrap(), initial);
        let actual = crate::golden::Section {
            step: 1,
            blocks: vec![crate::golden::Block::Typed {
                types: vec!["bigint".into()],
                names: vec!["v".into()],
                rows: vec![vec!["1".into()]],
            }],
        };
        assert_eq!(
            evidence.failure.as_ref().unwrap()["actual_digest"],
            crate::results::golden_actual_digest(&actual, false).unwrap()
        );
        assert_eq!(
            posted_requests(&peer.finish()),
            ["SELECT poll", "SELECT poll", "SELECT 7", "SELECT 99"]
        );
    }

    #[test]
    fn outer_conversion_exhausted_text_retry_retains_last_execution_and_original_failure() {
        let peer = Peer::start(boundary_values(&[0, 2, 7, 7]));
        let temp = tempfile::TempDir::new().unwrap();
        let steps = vec![
            boundary_step(
                1,
                "SELECT poll",
                crate::types::QueryMeta {
                    result_contains: vec!["9".into()],
                    retry_count: Some(2),
                    retry_interval_ms: Some(0),
                    ..Default::default()
                },
            ),
            boundary_step(
                2,
                "SELECT 7",
                crate::types::QueryMeta {
                    skip_result_check: true,
                    ..Default::default()
                },
            ),
            boundary_cleanup(3),
        ];
        let (outcome, evidence, golden) = run_boundary_steps(
            &peer,
            &temp,
            crate::Mode::Record,
            true,
            steps,
            Some("-- query 1\n-- legacy\nv\n2\n"),
            Box::new(BoundaryServer),
        );
        assert!(
            matches!(outcome.status, crate::CaseStatus::Fail),
            "{}",
            outcome.log
        );
        assert!(oracle_recorded(&evidence, 1, "text_assertions", "fail"));
        assert!(oracle_recorded(&evidence, 1, "legacy_conversion", "pass"));
        assert!(evidence.failure.is_some());
        assert!(
            std::fs::read_to_string(golden)
                .unwrap()
                .contains("-- types\tbigint\nv\n2")
        );
        assert_eq!(
            posted_requests(&peer.finish()),
            ["SELECT poll", "SELECT poll", "SELECT 7", "SELECT 99"]
        );
    }
    struct PublicationRestartServer {
        proxy: String,
        events: Arc<Mutex<Vec<&'static str>>>,
        fail_restart: bool,
        wrong_target: bool,
    }
    impl crate::cluster::TestClusterOwner for PublicationRestartServer {
        fn target_host(&self) -> Option<&str> {
            None
        }
        fn target_port(&self) -> Option<u16> {
            None
        }
        fn be_count(&self) -> usize {
            3
        }
        fn restart_fe_with_after_exit_hook(
            &mut self,
            after_exit: &mut dyn FnMut() -> anyhow::Result<()>,
        ) -> anyhow::Result<()> {
            self.events.lock().unwrap().push("original-exit-confirmed");
            after_exit()?;
            self.events.lock().unwrap().push("original-hook-armed");
            if self.fail_restart {
                anyhow::bail!("original successor startup failed");
            }
            let table = if self.wrong_target {
                "wrong_mv"
            } else {
                "a_mv"
            };
            let body = reqwest::blocking::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(2))
                .build()?
                .get(format!(
                    "{}/v1/namespaces/owned_ns/tables/{table}",
                    self.proxy
                ))
                .send()?
                .text()?;
            self.events.lock().unwrap().push("startup-target-read");
            anyhow::ensure!(
                body == "{corrupt-package",
                "exact startup target did not consume the original arm"
            );
            Ok(())
        }
    }

    #[test]
    fn outer_publication_negative_conversion_runs_original_after_exit_hook_and_exact_arm() {
        use crate::types::*;
        for (mode, convert) in [(crate::Mode::Verify, false), (crate::Mode::Record, true)] {
            let target = Peer::start(vec![Reply::json(
                "GET",
                "/v1/namespaces/owned_ns/tables/a_mv",
                "{}",
            )]);
            let mut proxy = crate::publication_catalog::PublicationCatalogFixture::start(format!(
                "http://127.0.0.1:{}",
                target.port
            ))
            .unwrap();
            let control = proxy.control().unwrap();
            let events = Arc::new(Mutex::new(Vec::new()));
            let mut replies = vec![
                Reply::queued("/v1/statement", "/negative"),
                Reply::json(
                    "GET",
                    "/negative",
                    &trino_error("durable cleanup response lost"),
                ),
            ];
            replies.extend(boundary_success_replies(2));
            let peer = Peer::start(replies);
            let temp = tempfile::TempDir::new().unwrap();
            let steps = vec![
                boundary_step(
                    1,
                    "CALL maintenance",
                    QueryMeta {
                        expect_error: Some("durable cleanup response lost".into()),
                        restart_fe_after_step: true,
                        publication_catalog_fault: Some(PublicationCatalogFaultDirective {
                            action: PublicationCatalogAction::TableLoad,
                            fault: PublicationCatalogFault::CorruptPackage,
                        }),
                        publication_catalog_fault_target: Some(PublicationCatalogFaultTarget {
                            namespace: "owned_ns".into(),
                            table: "a_mv".into(),
                        }),
                        publication_catalog_fault_timing: Some(
                            PublicationCatalogFaultTiming::AfterFrontendExit,
                        ),
                        ..Default::default()
                    },
                ),
                boundary_step(2, "SELECT 7", Default::default()),
                boundary_cleanup(3),
            ];
            let initial = if convert {
                "-- query 2\n-- legacy\nv\n7\n"
            } else {
                "-- query 2\n-- types\tbigint\nv\n7\n"
            };
            let (outcome, evidence, golden) = run_boundary_steps_configured(
                &peer,
                &temp,
                mode,
                convert,
                steps,
                Some(initial),
                (
                    Box::new(PublicationRestartServer {
                        proxy: proxy.uri().into(),
                        events: Arc::clone(&events),
                        fail_restart: false,
                        wrong_target: false,
                    }),
                    move |ctx| {
                        ctx.publication_catalog_control = Some(control);
                    },
                ),
            );
            assert!(
                matches!(outcome.status, crate::CaseStatus::Pass),
                "{}",
                outcome.log
            );
            assert_eq!(
                *events.lock().unwrap(),
                [
                    "original-exit-confirmed",
                    "original-hook-armed",
                    "startup-target-read"
                ]
            );
            assert_eq!(
                outcome
                    .log
                    .matches("armed after original frontend exit")
                    .count(),
                1
            );
            assert!(outcome.log.contains("package-response-corrupted"));
            assert!(oracle_recorded(&evidence, 1, "expected_error", "pass"));
            if convert {
                assert!(oracle_recorded(&evidence, 2, "legacy_conversion", "pass"));
                assert!(
                    std::fs::read_to_string(golden)
                        .unwrap()
                        .contains("-- types\tbigint")
                );
            }
            assert_eq!(
                posted_requests(&peer.finish()),
                ["CALL maintenance", "SELECT 7", "SELECT 99"]
            );
            assert_eq!(target.finish().len(), 1);
            proxy.shutdown().unwrap();
        }
    }

    #[test]
    fn outer_publication_negative_failed_restart_or_wrong_target_keeps_legacy_and_cleanup_only() {
        use crate::types::*;
        for wrong_target in [false, true] {
            let replies = if wrong_target {
                vec![Reply::json(
                    "GET",
                    "/v1/namespaces/owned_ns/tables/wrong_mv",
                    "{}",
                )]
            } else {
                vec![]
            };
            let target = Peer::start(replies);
            let mut proxy = crate::publication_catalog::PublicationCatalogFixture::start(format!(
                "http://127.0.0.1:{}",
                target.port
            ))
            .unwrap();
            let control = proxy.control().unwrap();
            let events = Arc::new(Mutex::new(Vec::new()));
            let mut replies = vec![
                Reply::queued("/v1/statement", "/negative"),
                Reply::json(
                    "GET",
                    "/negative",
                    &trino_error("durable cleanup response lost"),
                ),
            ];
            replies.extend(boundary_success_replies(1));
            let peer = Peer::start(replies);
            let temp = tempfile::TempDir::new().unwrap();
            let steps = vec![
                boundary_step(
                    1,
                    "CALL maintenance",
                    QueryMeta {
                        expect_error: Some("durable cleanup response lost".into()),
                        restart_fe_after_step: true,
                        publication_catalog_fault: Some(PublicationCatalogFaultDirective {
                            action: PublicationCatalogAction::TableLoad,
                            fault: PublicationCatalogFault::CorruptPackage,
                        }),
                        publication_catalog_fault_target: Some(PublicationCatalogFaultTarget {
                            namespace: "owned_ns".into(),
                            table: "a_mv".into(),
                        }),
                        publication_catalog_fault_timing: Some(
                            PublicationCatalogFaultTiming::AfterFrontendExit,
                        ),
                        ..Default::default()
                    },
                ),
                boundary_step(2, "SELECT 7", Default::default()),
                boundary_cleanup(3),
            ];
            let initial = "-- query 2\n-- legacy\nv\n7\n";
            let (outcome, evidence, golden) = run_boundary_steps_configured(
                &peer,
                &temp,
                crate::Mode::Record,
                true,
                steps,
                Some(initial),
                (
                    Box::new(PublicationRestartServer {
                        proxy: proxy.uri().into(),
                        events: Arc::clone(&events),
                        fail_restart: !wrong_target,
                        wrong_target,
                    }),
                    move |ctx| {
                        ctx.publication_catalog_control = Some(control);
                    },
                ),
            );
            assert!(matches!(outcome.status, crate::CaseStatus::Fail));
            assert_eq!(
                &events.lock().unwrap()[..2],
                ["original-exit-confirmed", "original-hook-armed"]
            );
            assert!(!oracle_recorded(&evidence, 2, "legacy_conversion", "pass"));
            assert_eq!(std::fs::read_to_string(golden).unwrap(), initial);
            assert_eq!(
                posted_requests(&peer.finish()),
                ["CALL maintenance", "SELECT 99"]
            );
            target.finish();
            proxy.shutdown().unwrap();
        }
    }

    #[test]
    fn outer_publication_concurrent_dispatch_runs_same_companion_before_release_and_conversion_proof()
     {
        use crate::types::*;
        for (mode, convert) in [(crate::Mode::Verify, false), (crate::Mode::Record, true)] {
            for companion_fails in [false, true] {
                let temp = tempfile::TempDir::new().unwrap();
                let marker = temp.path().join("companion-ran");
                let gate = Arc::new(FaultPeerGate {
                    wait_for_action: true,
                    ..Default::default()
                });
                let mut held = Reply::json("GET", "/held", &integer_result("v", 7));
                held.fault_gate = Some(Arc::clone(&gate));
                let mut replies = vec![Reply::queued("/v1/statement", "/held"), held];
                replies.extend(boundary_success_replies(if companion_fails {
                    1
                } else {
                    2
                }));
                let peer = Peer::start(replies);
                let target = Peer::start(vec![Reply::json(
                    "POST",
                    "/v1/namespaces/owned_ns/tables/t",
                    "{}",
                )]);
                let mut proxy = crate::publication_catalog::PublicationCatalogFixture::start(
                    format!("http://127.0.0.1:{}", target.port),
                )
                .unwrap();
                let control = proxy.control().unwrap();
                let uri = proxy.uri().to_string();
                let actor_gate = Arc::clone(&gate);
                let original_request = thread::spawn(move || {
                    let deadline = Instant::now() + Duration::from_secs(5);
                    while !actor_gate.request_seen.load(Ordering::Acquire) {
                        assert!(Instant::now() < deadline);
                        thread::sleep(Duration::from_millis(1));
                    }
                    let response = reqwest::blocking::Client::builder()
                        .no_proxy()
                        .timeout(Duration::from_secs(5))
                        .build()
                        .unwrap()
                        .post(format!("{uri}/v1/namespaces/owned_ns/tables/t"))
                        .json(&serde_json::json!({"requirements":[],"updates":[]}))
                        .send();
                    actor_gate.action_done.store(true, Ordering::Release);
                    assert!(response.unwrap().status().is_success());
                });
                let command = format!(
                    "printf ran > '{}'; exit {}",
                    marker.display(),
                    if companion_fails { 7 } else { 0 }
                );
                let steps = vec![boundary_step(1, "SELECT held", QueryMeta {
                    publication_catalog_fault: Some(PublicationCatalogFaultDirective { action: PublicationCatalogAction::TableCommit, fault: PublicationCatalogFault::BeforeRequirementCheckHoldForConcurrentShell }),
                    publication_catalog_concurrent_shell: Some(command),
                    expect_error: companion_fails.then(|| "runner concurrent publication shell".into()),
                    retry_count: Some(2), retry_interval_ms: Some(0), ..Default::default()
                }), boundary_step(2, "SELECT 7", Default::default()), boundary_cleanup(3)];
                let block = if convert {
                    "-- legacy\nv\n7\n"
                } else {
                    "-- types\tbigint\nv\n7\n"
                };
                let initial = format!("-- query 1\n{block}\n-- query 2\n{block}");
                let (outcome, evidence, golden) = run_boundary_steps_configured(
                    &peer,
                    &temp,
                    mode,
                    convert,
                    steps,
                    Some(&initial),
                    (Box::new(BoundaryServer), move |ctx| {
                        ctx.publication_catalog_control = Some(control);
                    }),
                );
                original_request.join().unwrap();
                assert_eq!(std::fs::read_to_string(marker).unwrap(), "ran");
                assert!(outcome.log.contains("before-dispatch hold reached"));
                if companion_fails {
                    assert!(matches!(outcome.status, crate::CaseStatus::Fail));
                    assert!(!oracle_recorded(&evidence, 1, "expected_error", "pass"));
                    assert_eq!(std::fs::read_to_string(golden).unwrap(), initial);
                    assert_eq!(
                        posted_requests(&peer.finish()),
                        ["SELECT held", "SELECT 99"]
                    );
                } else {
                    assert!(
                        matches!(outcome.status, crate::CaseStatus::Pass),
                        "{}",
                        outcome.log
                    );
                    assert!(outcome.log.contains("completed before release"));
                    assert!(outcome.log.contains("consumed and cleared"));
                    if convert {
                        assert!(oracle_recorded(&evidence, 1, "legacy_conversion", "pass"));
                        assert!(oracle_recorded(&evidence, 2, "legacy_conversion", "pass"));
                    }
                    assert_eq!(
                        posted_requests(&peer.finish()),
                        ["SELECT held", "SELECT 7", "SELECT 99"]
                    );
                }
                assert_eq!(target.finish().len(), 1);
                proxy.shutdown().unwrap();
            }
        }
    }

    #[test]
    fn outer_conversion_graph_unclassified_failure_cannot_author_or_cross_successor() {
        let peer = Peer::start(boundary_success_replies(2));
        let temp = tempfile::TempDir::new().unwrap();
        let steps = vec![
            boundary_step(
                1,
                "SELECT 7",
                crate::types::QueryMeta {
                    mv_rest_document_graph: Some("not-an-exact-expectation".into()),
                    ..Default::default()
                },
            ),
            boundary_step(2, "SELECT 7", Default::default()),
            boundary_cleanup(3),
        ];
        let initial = "-- query 1\n-- legacy\nv\n7\n\n-- query 2\n-- legacy\nv\n7\n";
        let (outcome, evidence, golden) = run_boundary_steps_configured(
            &peer,
            &temp,
            crate::Mode::Record,
            true,
            steps,
            Some(initial),
            (Box::new(BoundaryServer), |ctx| {
                ctx.suite_name = "mv-publication-v11".into();
            }),
        );
        assert!(matches!(outcome.status, crate::CaseStatus::Fail));
        assert!(evidence.executed.iter().any(|e| e["step"] == 1
            && e["kind"] == "mv_rest_document_graph"
            && e["policy"]["failure_origin"] == "unclassified"
            && e["policy"]["authoring_stopped"] == true));
        assert!(!oracle_recorded(&evidence, 1, "legacy_conversion", "pass"));
        assert!(!oracle_recorded(&evidence, 2, "legacy_conversion", "pass"));
        assert_eq!(std::fs::read_to_string(golden).unwrap(), initial);
        let requests = peer.finish();
        assert_eq!(posted_requests(&requests), ["SELECT 7", "SELECT 99"]);
        assert_eq!(requests.iter().filter(|r| r.method == "GET").count(), 2);
    }

    #[test]
    fn outer_conversion_equivalence_assertion_uses_extra_queries_and_preserves_failure() {
        let mut replies = boundary_values(&[7, 2]);
        replies.extend([Reply::queued("/v1/statement", "/mv-show"), Reply::json("GET", "/mv-show", r#"{"id":"q","columns":[{"name":"Name","type":"varchar","typeSignature":{"rawType":"varchar","arguments":[{"kind":"LONG","value":2147483647}]}},{"name":"SelectText","type":"varchar","typeSignature":{"rawType":"varchar","arguments":[{"kind":"LONG","value":2147483647}]}}],"data":[["target_mv","SELECT recompute"]]}"#)]);
        replies.extend(boundary_values(&[3, 7, 7]));
        let peer = Peer::start(replies);
        let temp = tempfile::TempDir::new().unwrap();
        let steps = vec![
            boundary_step(
                1,
                "SELECT 7",
                crate::types::QueryMeta {
                    imv_equivalence_check: Some("target_mv".into()),
                    ..Default::default()
                },
            ),
            boundary_step(2, "SELECT 7", Default::default()),
            boundary_cleanup(3),
        ];
        let initial = "-- query 1\n-- legacy\nv\n7\n\n-- query 2\n-- legacy\nv\n7\n";
        let (outcome, evidence, _) = run_boundary_steps(
            &peer,
            &temp,
            crate::Mode::Record,
            true,
            steps,
            Some(initial),
            Box::new(BoundaryServer),
        );
        assert!(matches!(outcome.status, crate::CaseStatus::Fail));
        assert!(oracle_recorded(&evidence, 1, "imv_equivalence", "fail"));
        assert_eq!(
            posted_requests(&peer.finish()),
            [
                "SELECT 7",
                "SELECT * FROM target_mv",
                "SHOW MATERIALIZED VIEWS",
                "SELECT recompute",
                "SELECT 7",
                "SELECT 99"
            ]
        );
    }

    #[test]
    fn outer_conversion_stateless_available_level_assertion_is_not_skipped() {
        let mut replies = boundary_values(&[7, 7]);
        replies.extend([Reply::queued("/v1/statement", "/available"), Reply::json("GET", "/available", r#"{"id":"q","columns":[{"name":"AvailableLevel","type":"varchar","typeSignature":{"rawType":"varchar","arguments":[{"kind":"LONG","value":2147483647}]}}],"data":[["baseline"]]}"#)]);
        replies.extend(boundary_success_replies(2));
        let peer = Peer::start(replies);
        let temp = tempfile::TempDir::new().unwrap();
        let steps = vec![
            boundary_step(
                1,
                "SELECT 7",
                crate::types::QueryMeta {
                    imv_stateless_rebuild: Some(crate::types::ImvStatelessDirective {
                        catalog: None,
                        mv: "target_mv".into(),
                        level: crate::types::ImvStatelessLevel::Provenance,
                    }),
                    ..Default::default()
                },
            ),
            boundary_step(2, "SELECT 7", Default::default()),
            boundary_cleanup(3),
        ];
        let initial = "-- query 1\n-- legacy\nv\n7\n\n-- query 2\n-- legacy\nv\n7\n";
        let (outcome, evidence, _) = run_boundary_steps(
            &peer,
            &temp,
            crate::Mode::Record,
            true,
            steps,
            Some(initial),
            Box::new(BoundaryServer),
        );
        assert!(matches!(outcome.status, crate::CaseStatus::Fail));
        assert!(oracle_recorded(
            &evidence,
            1,
            "imv_stateless_rebuild",
            "fail"
        ));
        assert_eq!(
            posted_requests(&peer.finish()),
            [
                "SELECT 7",
                "SELECT * FROM target_mv",
                "CALL ice.system.novarocks_imv_stateless_rebuild(table => 'target_mv', level => 'provenance')",
                "SELECT 7",
                "SELECT 99"
            ]
        );
    }

    #[test]
    fn outer_conversion_oracle_typed_infrastructure_stops_without_ordinary_successor_or_authoring()
    {
        for stateless in [false, true] {
            let mut replies = boundary_values(&[7]);
            replies.push(Reply::json("POST", "/v1/statement", "not-json"));
            replies.extend(boundary_success_replies(1));
            let peer = Peer::start(replies);
            let temp = tempfile::TempDir::new().unwrap();
            let mut meta = crate::types::QueryMeta::default();
            if stateless {
                meta.imv_stateless_rebuild = Some(crate::types::ImvStatelessDirective {
                    catalog: None,
                    mv: "target_mv".into(),
                    level: crate::types::ImvStatelessLevel::Baseline,
                });
            } else {
                meta.imv_equivalence_check = Some("target_mv".into());
            }
            let steps = vec![
                boundary_step(1, "SELECT 7", meta),
                boundary_step(2, "SELECT 7", Default::default()),
                boundary_cleanup(3),
            ];
            let initial = "-- query 1\n-- legacy\nv\n7\n\n-- query 2\n-- legacy\nv\n7\n";
            let (outcome, evidence, golden) = run_boundary_steps(
                &peer,
                &temp,
                crate::Mode::Record,
                true,
                steps,
                Some(initial),
                Box::new(BoundaryServer),
            );
            assert!(matches!(outcome.status, crate::CaseStatus::Fail));
            assert_eq!(std::fs::read_to_string(golden).unwrap(), initial);
            assert!(!oracle_recorded(&evidence, 1, "legacy_conversion", "pass"));
            assert!(!oracle_recorded(&evidence, 2, "legacy_conversion", "pass"));
            assert_eq!(
                posted_requests(&peer.finish()),
                ["SELECT 7", "SELECT * FROM target_mv", "SELECT 99"]
            );
        }
    }
    #[test]
    fn outer_conversion_service_hold_failure_is_infrastructure_before_expected_error_or_authoring()
    {
        for fail_after_primary_result in [false, true] {
            let mut service_replies = vec![
                Reply::json(
                    "POST",
                    "/arm?table=owned_ns.t&stage=before-persistent-commit",
                    r#"{"arm_id":"original-arm"}"#,
                ),
                Reply::json(
                    "GET",
                    "/status?arm_id=original-arm",
                    if fail_after_primary_result {
                        r#"{"phase":"held"}"#
                    } else {
                        r#"{"phase":"failed"}"#
                    },
                ),
                Reply::json("POST", "/release?arm_id=original-arm&cancel=true", "{}"),
            ];
            if fail_after_primary_result {
                service_replies.push(Reply::json("GET", "/trace", "invalid original trace"));
            }
            let service = Peer::start(service_replies);
            let service_control = crate::publication_service::Control::new(format!(
                "http://127.0.0.1:{}",
                service.port
            ))
            .unwrap();
            let peer = Peer::start(boundary_success_replies(2));
            let temp = tempfile::TempDir::new().unwrap();
            let marker = temp.path().join("companion");
            let steps = vec![
                boundary_step(
                    1,
                    "SELECT held",
                    crate::types::QueryMeta {
                        publication_service_hold: Some(
                            crate::types::PublicationServiceHoldDirective {
                                table: "owned_ns.t".into(),
                                actor: crate::types::PublicationServiceActor::Sql,
                            },
                        ),
                        publication_catalog_concurrent_shell: Some(format!(
                            "printf ran > '{}'",
                            marker.display()
                        )),
                        expect_error: Some("runner service hold".into()),
                        retry_count: Some(1),
                        ..Default::default()
                    },
                ),
                boundary_step(2, "SELECT 7", Default::default()),
                boundary_cleanup(3),
            ];
            let initial = "-- query 1\n-- legacy\nv\n7\n\n-- query 2\n-- legacy\nv\n7\n";
            let (outcome, evidence, golden) = run_boundary_steps_configured(
                &peer,
                &temp,
                crate::Mode::Record,
                true,
                steps,
                Some(initial),
                (Box::new(BoundaryServer), move |ctx| {
                    ctx.publication_service_control = Some(service_control);
                }),
            );
            assert!(matches!(outcome.status, crate::CaseStatus::Fail));
            assert_eq!(marker.exists(), fail_after_primary_result);
            assert!(outcome.log.contains("target infrastructure failure"));
            assert!(!oracle_recorded(&evidence, 1, "expected_error", "pass"));
            assert!(!oracle_recorded(&evidence, 1, "legacy_conversion", "pass"));
            assert!(!oracle_recorded(&evidence, 2, "legacy_conversion", "pass"));
            assert_eq!(std::fs::read_to_string(golden).unwrap(), initial);
            if fail_after_primary_result {
                assert!(
                    evidence
                        .artifacts
                        .iter()
                        .filter_map(|p| std::fs::read_to_string(p.join("actual.json")).ok())
                        .map(|s| serde_json::from_str::<serde_json::Value>(&s).unwrap())
                        .any(|a| a["query"] == 1
                            && a["complete"] == false
                            && a["rows"] == serde_json::json!([["7"]]))
                );
            }
            assert_eq!(
                posted_requests(&peer.finish()),
                ["SELECT held", "SELECT 99"]
            );
            assert_eq!(
                service.finish().len(),
                if fail_after_primary_result { 4 } else { 3 }
            );
        }
    }

    struct PlainPostNegativeRestartServer(Arc<Mutex<usize>>);
    impl crate::cluster::TestClusterOwner for PlainPostNegativeRestartServer {
        fn target_host(&self) -> Option<&str> {
            None
        }
        fn target_port(&self) -> Option<u16> {
            None
        }
        fn be_count(&self) -> usize {
            3
        }
        fn restart_fe(&mut self) -> anyhow::Result<()> {
            *self.0.lock().unwrap() += 1;
            Ok(())
        }
    }
    #[test]
    fn outer_ordinary_record_keeps_negative_post_actions_and_domain_oracles_disabled() {
        let mut replies = vec![
            Reply::queued("/v1/statement", "/negative"),
            Reply::json(
                "GET",
                "/negative",
                &trino_error("expected statement rejection"),
            ),
        ];
        replies.extend(boundary_success_replies(2));
        let peer = Peer::start(replies);
        let temp = tempfile::TempDir::new().unwrap();
        let restarts = Arc::new(Mutex::new(0));
        let steps = vec![
            boundary_step(
                1,
                "CALL negative",
                crate::types::QueryMeta {
                    expect_error: Some("expected statement rejection".into()),
                    restart_fe_after_step: true,
                    ..Default::default()
                },
            ),
            boundary_step(
                2,
                "SELECT 7",
                crate::types::QueryMeta {
                    imv_equivalence_check: Some("unsubmitted_mv".into()),
                    mv_rest_document_graph: Some("unparsed-graph".into()),
                    ..Default::default()
                },
            ),
            boundary_cleanup(3),
        ];
        let (outcome, evidence, _) = run_boundary_steps(
            &peer,
            &temp,
            crate::Mode::Record,
            false,
            steps,
            None,
            Box::new(PlainPostNegativeRestartServer(Arc::clone(&restarts))),
        );
        assert!(
            matches!(outcome.status, crate::CaseStatus::Pass),
            "{}",
            outcome.log
        );
        assert_eq!(*restarts.lock().unwrap(), 0);
        assert!(
            !evidence
                .executed
                .iter()
                .any(|e| e["kind"] == "imv_equivalence" || e["kind"] == "mv_rest_document_graph")
        );
        assert_eq!(
            posted_requests(&peer.finish()),
            ["CALL negative", "SELECT 7", "SELECT 99"]
        );
    }
    #[test]
    fn outer_conversion_oracle_retains_its_own_partial_instead_of_primary_result() {
        let mut replies = boundary_values(&[7, 7]);
        replies.extend([
            Reply::queued("/v1/statement", "/mv-show"),
            Reply::json("GET", "/mv-show", r#"{"id":"q","columns":[{"name":"Name","type":"varchar","typeSignature":{"rawType":"varchar","arguments":[{"kind":"LONG","value":2147483647}]}},{"name":"SelectText","type":"varchar","typeSignature":{"rawType":"varchar","arguments":[{"kind":"LONG","value":2147483647}]}}],"data":[["target_mv","SELECT 11; SELECT broken"]]}"#),
        ]);
        replies.extend(boundary_values(&[11]));
        replies.push(Reply::json(
            "POST",
            "/v1/statement",
            "invalid original recompute reply",
        ));
        replies.extend(boundary_success_replies(1));
        let peer = Peer::start(replies);
        let temp = tempfile::TempDir::new().unwrap();
        let steps = vec![
            boundary_step(
                1,
                "SELECT 7",
                crate::types::QueryMeta {
                    imv_equivalence_check: Some("target_mv".into()),
                    ..Default::default()
                },
            ),
            boundary_step(2, "SELECT 7", Default::default()),
            boundary_cleanup(3),
        ];
        let initial = "-- query 1\n-- legacy\nv\n7\n\n-- query 2\n-- legacy\nv\n7\n";
        let (outcome, evidence, golden) = run_boundary_steps(
            &peer,
            &temp,
            crate::Mode::Record,
            true,
            steps,
            Some(initial),
            Box::new(BoundaryServer),
        );
        assert!(matches!(outcome.status, crate::CaseStatus::Fail));
        assert_eq!(std::fs::read_to_string(golden).unwrap(), initial);
        assert!(evidence.executed.iter().any(|e| e["step"] == 1
            && e["kind"] == "domain_oracle_execution"
            && e["policy"]["failure_origin"] == "infrastructure"
            && e["policy"]["has_original_partial"] == true
            && e["policy"]["original_sql"] == "SELECT 11; SELECT broken"));
        assert!(
            evidence
                .artifacts
                .iter()
                .filter_map(|p| std::fs::read_to_string(p.join("actual.json")).ok())
                .map(|s| serde_json::from_str::<serde_json::Value>(&s).unwrap())
                .any(|a| a["query"] == 1
                    && a["complete"] == false
                    && a["rows"] == serde_json::json!([["11"]]))
        );
        assert!(!oracle_recorded(&evidence, 1, "legacy_conversion", "pass"));
        assert!(!oracle_recorded(&evidence, 2, "legacy_conversion", "pass"));
        assert_eq!(
            posted_requests(&peer.finish()),
            [
                "SELECT 7",
                "SELECT * FROM target_mv",
                "SHOW MATERIALIZED VIEWS",
                "SELECT 11",
                "SELECT broken",
                "SELECT 99"
            ]
        );
    }
    #[test]
    fn outer_conversion_external_service_actor_failure_preserves_remote_exit_uncertainty() {
        let service = Peer::start(vec![
            Reply::json(
                "POST",
                "/arm?table=owned_ns.t&stage=before-persistent-commit",
                r#"{"arm_id":"external-arm"}"#,
            ),
            Reply::json(
                "GET",
                "/status?arm_id=external-arm",
                r#"{"phase":"failed"}"#,
            ),
            Reply::json("POST", "/release?arm_id=external-arm&cancel=true", "{}"),
        ]);
        let control =
            crate::publication_service::Control::new(format!("http://127.0.0.1:{}", service.port))
                .unwrap();
        let peer = Peer::start(boundary_success_replies(1));
        let temp = tempfile::TempDir::new().unwrap();
        let marker = temp.path().join("external-actor-ran");
        let writers = Arc::new(AtomicBool::new(true));
        let original_writers = Arc::clone(&writers);
        let steps = vec![
            boundary_step(
                1,
                "SELECT must-not-start",
                crate::types::QueryMeta {
                    publication_service_hold: Some(crate::types::PublicationServiceHoldDirective {
                        table: "owned_ns.t".into(),
                        actor: crate::types::PublicationServiceActor::Shell,
                    }),
                    publication_catalog_concurrent_shell: Some(format!(
                        "printf ran > '{}'",
                        marker.display()
                    )),
                    expect_error: Some("external request did not reach hold".into()),
                    retry_count: Some(1),
                    ..Default::default()
                },
            ),
            boundary_step(2, "SELECT must-not-follow", Default::default()),
            boundary_cleanup(3),
        ];
        let initial = "-- query 1\n-- legacy\nv\n7\n\n-- query 2\n-- legacy\nv\n7\n";
        let (outcome, evidence, golden) = run_boundary_steps_configured(
            &peer,
            &temp,
            crate::Mode::Record,
            true,
            steps,
            Some(initial),
            (Box::new(BoundaryServer), move |ctx| {
                ctx.publication_service_control = Some(control);
                ctx.fixture_writers_settled = original_writers;
            }),
        );
        assert!(matches!(outcome.status, crate::CaseStatus::Fail));
        assert_eq!(std::fs::read_to_string(marker).unwrap(), "ran");
        assert!(!writers.load(Ordering::Acquire));
        assert!(!oracle_recorded(&evidence, 1, "expected_error", "pass"));
        assert_eq!(std::fs::read_to_string(golden).unwrap(), initial);
        assert_eq!(posted_requests(&peer.finish()), ["SELECT 99"]);
        assert_eq!(service.finish().len(), 3);
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum PublicationKillReply {
        Transport,
        Success,
        Http,
        Protocol,
        Statement,
        MixedStatementThenTransport,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum PublicationKillOwner {
        Confirmed,
        KillFailed,
        RestartFailed,
    }
    struct PublicationKillBoundaryServer {
        mode: PublicationKillOwner,
        gate: Arc<FaultPeerGate>,
        calls: Arc<Mutex<[usize; 2]>>,
    }
    impl crate::cluster::TestClusterOwner for PublicationKillBoundaryServer {
        fn target_host(&self) -> Option<&str> {
            None
        }
        fn target_port(&self) -> Option<u16> {
            None
        }
        fn be_count(&self) -> usize {
            3
        }
        fn kill_fe(&mut self) -> anyhow::Result<()> {
            self.calls.lock().unwrap()[0] += 1;
            // The independent HTTP continuation exits on the fake FE action,
            // even when that action fails; a partial alone must not authorize it.
            self.gate.action_done.store(true, Ordering::Release);
            anyhow::ensure!(
                self.mode != PublicationKillOwner::KillFailed,
                "original publication FE kill failed"
            );
            Ok(())
        }
        fn restart_fe_until(&mut self, _deadline: Instant) -> anyhow::Result<()> {
            self.calls.lock().unwrap()[1] += 1;
            anyhow::ensure!(
                self.mode != PublicationKillOwner::RestartFailed,
                "original publication FE restart failed"
            );
            Ok(())
        }
    }

    // Exercise the real outer dispatch and local REST hold, not a direct call
    // with a synthetic successful SqlStepOutcome. No native FE is claimed here.
    fn run_publication_kill_boundary(
        reply: PublicationKillReply,
        owner: PublicationKillOwner,
        convert: bool,
    ) -> (
        crate::CaseOutcome,
        crate::results_record::CaseEvidence,
        String,
        Vec<Request>,
        Vec<serde_json::Value>,
        [usize; 2],
    ) {
        use crate::types::*;
        let temp = tempfile::TempDir::new().unwrap();
        let gate = Arc::new(FaultPeerGate {
            wait_for_action: true,
            ..Default::default()
        });
        let calls = Arc::new(Mutex::new([0; 2]));
        let mut replies = Vec::new();
        if reply == PublicationKillReply::MixedStatementThenTransport {
            replies.extend([
                Reply::queued("/v1/statement", "/first-error"),
                Reply::json(
                    "GET",
                    "/first-error",
                    &trino_error("earlier original SQL error"),
                ),
            ]);
        }
        replies.extend(boundary_values(&[11]));
        let mut held = Reply::json("GET", "/publication-held", &integer_result("v", 7));
        held.fault_gate = Some(Arc::clone(&gate));
        match reply {
            PublicationKillReply::Transport | PublicationKillReply::MixedStatementThenTransport => {
                held.drop_before_response = true
            }
            PublicationKillReply::Success => {}
            PublicationKillReply::Http => held.status = 503,
            PublicationKillReply::Protocol => held.body = "invalid original protocol reply".into(),
            PublicationKillReply::Statement => {
                held.body = trino_error("original statement rejection")
            }
        }
        replies.extend([Reply::queued("/v1/statement", "/publication-held"), held]);
        if reply == PublicationKillReply::Protocol {
            // The exact original query continuation is canceled by the strict
            // decoder. Do not ignore or substitute that real DELETE.
            let mut canceled = Reply::json("DELETE", "/publication-held", "");
            canceled.status = 204;
            replies.push(canceled);
        }
        let accepted =
            owner == PublicationKillOwner::Confirmed && reply == PublicationKillReply::Transport;
        replies.extend(boundary_success_replies(if accepted { 2 } else { 1 }));
        let peer = Peer::start(replies);
        let target = Peer::start(vec![Reply::json(
            "POST",
            "/v1/namespaces/owned_ns/tables/t",
            "{}",
        )]);
        let mut proxy = crate::publication_catalog::PublicationCatalogFixture::start(format!(
            "http://127.0.0.1:{}",
            target.port
        ))
        .unwrap();
        let control = proxy.control().unwrap();
        let uri = proxy.uri().to_string();
        let request_gate = Arc::clone(&gate);
        let original_request = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            while !request_gate.request_seen.load(Ordering::Acquire) {
                assert!(
                    Instant::now() < deadline,
                    "original query never reached its continuation"
                );
                thread::sleep(Duration::from_millis(1));
            }
            let response = reqwest::blocking::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap()
                .post(format!("{uri}/v1/namespaces/owned_ns/tables/t"))
                .json(&serde_json::json!({"requirements":[],"updates":[]}))
                .send()
                .unwrap();
            // The actual local proxy releases its held success as a 503 only
            // after the original controller releases its exact arm.
            assert_eq!(response.status().as_u16(), 503);
        });
        let sql = if reply == PublicationKillReply::MixedStatementThenTransport {
            "CALL failed; SELECT 11; SELECT held"
        } else {
            "SELECT 11; SELECT held"
        };
        let steps = vec![
            boundary_step(
                1,
                sql,
                QueryMeta {
                    publication_catalog_fault: Some(PublicationCatalogFaultDirective {
                        action: PublicationCatalogAction::TableCommit,
                        fault: PublicationCatalogFault::AfterCommitHoldForFrontendKill,
                    }),
                    skip_result_check: !convert,
                    // No retry can mint a later success or replace the tuple.
                    retry_count: Some(1),
                    retry_interval_ms: Some(0),
                    ..Default::default()
                },
            ),
            boundary_step(2, "SELECT 7", Default::default()),
            boundary_cleanup(3),
        ];
        let initial = if convert {
            "-- query 1\n-- legacy\nv\n11\n\n-- query 2\n-- legacy\nv\n7\n"
        } else {
            "-- query 2\n-- types\tbigint\nv\n7\n"
        };
        let (outcome, evidence, golden) = run_boundary_steps_configured(
            &peer,
            &temp,
            if convert {
                crate::Mode::Record
            } else {
                crate::Mode::Verify
            },
            convert,
            steps,
            Some(initial),
            (
                Box::new(PublicationKillBoundaryServer {
                    mode: owner,
                    gate,
                    calls: Arc::clone(&calls),
                }),
                move |ctx| {
                    ctx.publication_catalog_control = Some(control);
                },
            ),
        );
        original_request.join().unwrap();
        let text = std::fs::read_to_string(golden).unwrap();
        let actuals = evidence
            .artifacts
            .iter()
            .filter_map(|p| std::fs::read_to_string(p.join("actual.json")).ok())
            .map(|s| serde_json::from_str(&s).unwrap())
            .collect();
        let requests = peer.finish();
        assert_eq!(target.finish().len(), 1);
        proxy.shutdown().unwrap();
        let counts = *calls.lock().unwrap();
        (outcome, evidence, text, requests, actuals, counts)
    }

    fn publication_kill_diagnostic(
        evidence: &crate::results_record::CaseEvidence,
    ) -> &serde_json::Value {
        &evidence
            .diagnostics
            .iter()
            .find(|event| {
                event["step"] == 1 && event["kind"] == "publication_kill_query_diagnostic"
            })
            .expect("original joined query diagnostic missing")["details"]
    }

    #[test]
    fn outer_publication_kill_partial_transport_is_diagnostic_and_cannot_become_full_oracle() {
        let (outcome, evidence, golden, requests, actuals, calls) = run_publication_kill_boundary(
            PublicationKillReply::Transport,
            PublicationKillOwner::Confirmed,
            true,
        );
        // The original empty control success deliberately mismatches the old
        // row 11. Accepting its partial as complete would make this test pass.
        assert!(matches!(outcome.status, crate::CaseStatus::Fail));
        assert!(
            outcome.log.contains("client disconnect observed"),
            "{}",
            outcome.log
        );
        assert!(
            !outcome.log.contains("target infrastructure failure"),
            "{}",
            outcome.log
        );
        assert!(!oracle_recorded(&evidence, 1, "legacy_conversion", "pass"));
        assert!(oracle_recorded(&evidence, 2, "legacy_conversion", "pass"));
        assert!(golden.starts_with("-- query 1\n-- legacy\nv\n11\n"));
        assert!(golden.contains("-- query 2\n-- types\tbigint\nv\n7\n"));
        let diagnostic = publication_kill_diagnostic(&evidence);
        assert_eq!(diagnostic["original_query_ok"], false);
        assert_eq!(diagnostic["original_transport_only"], true);
        assert_eq!(diagnostic["authorized_as_complete_R_old_input"], false);
        assert!(
            diagnostic["original_error"]
                .as_str()
                .unwrap()
                .contains("Trino transport failure")
        );
        assert!(actuals.iter().any(|a| a["query"] == 1
            && a["complete"] == false
            && a["rows"] == serde_json::json!([["11"]])));
        assert_eq!(calls, [1, 1]);
        assert_eq!(
            posted_requests(&requests),
            ["SELECT 11", "SELECT held", "SELECT 7", "SELECT 99"]
        );
        assert!(outcome.log.contains("consumed and cleared"));
        assert!(
            outcome
                .log
                .contains("response-held-after-downstream-success")
        );
    }

    #[test]
    fn outer_publication_kill_ordinary_verify_false_none_keeps_original_control_contract() {
        let (outcome, evidence, golden, requests, actuals, calls) = run_publication_kill_boundary(
            PublicationKillReply::Transport,
            PublicationKillOwner::Confirmed,
            false,
        );
        assert!(
            matches!(outcome.status, crate::CaseStatus::Pass),
            "{}",
            outcome.log
        );
        let diagnostic = publication_kill_diagnostic(&evidence);
        assert_eq!(
            evidence
                .diagnostics
                .iter()
                .filter(|diagnostic| diagnostic["kind"] == "publication_kill_query_diagnostic")
                .count(),
            1
        );
        assert!(
            evidence
                .executed
                .iter()
                .all(|event| { event["kind"] != "publication_kill_query_diagnostic" })
        );
        assert_eq!(diagnostic["original_query_ok"], false);
        assert_eq!(diagnostic["has_original_execution"], false);
        assert_eq!(diagnostic["original_transport_only"], true);
        assert!(actuals.is_empty());
        assert_eq!(golden, "-- query 2\n-- types\tbigint\nv\n7\n");
        assert_eq!(calls, [1, 1]);
        assert_eq!(
            posted_requests(&requests),
            ["SELECT 11", "SELECT held", "SELECT 7", "SELECT 99"]
        );
    }

    #[test]
    fn outer_publication_kill_failed_original_owner_keeps_partial_and_stops_authoring() {
        for owner in [
            PublicationKillOwner::KillFailed,
            PublicationKillOwner::RestartFailed,
        ] {
            let (outcome, evidence, golden, requests, actuals, calls) =
                run_publication_kill_boundary(PublicationKillReply::Transport, owner, true);
            assert!(matches!(outcome.status, crate::CaseStatus::Fail));
            assert!(
                outcome.log.contains("target infrastructure failure"),
                "{}",
                outcome.log
            );
            let diagnostic = publication_kill_diagnostic(&evidence);
            assert_eq!(diagnostic["original_transport_only"], true);
            assert_eq!(diagnostic["has_original_execution"], true);
            assert!(!oracle_recorded(&evidence, 1, "legacy_conversion", "pass"));
            assert!(!oracle_recorded(&evidence, 2, "legacy_conversion", "pass"));
            assert_eq!(
                golden,
                "-- query 1\n-- legacy\nv\n11\n\n-- query 2\n-- legacy\nv\n7\n"
            );
            assert!(
                actuals
                    .iter()
                    .any(|a| a["complete"] == false && a["rows"] == serde_json::json!([["11"]]))
            );
            assert_eq!(
                calls,
                if owner == PublicationKillOwner::KillFailed {
                    [1, 0]
                } else {
                    [1, 1]
                }
            );
            assert_eq!(
                posted_requests(&requests),
                ["SELECT 11", "SELECT held", "SELECT 99"]
            );
        }
    }

    #[test]
    fn outer_publication_kill_real_query_success_still_fails_and_retains_complete_actual() {
        for convert in [false, true] {
            let (outcome, evidence, _, requests, actuals, calls) = run_publication_kill_boundary(
                PublicationKillReply::Success,
                PublicationKillOwner::Confirmed,
                convert,
            );
            assert!(matches!(outcome.status, crate::CaseStatus::Fail));
            assert!(
                outcome
                    .log
                    .contains("statement unexpectedly survived frontend kill")
            );
            assert_eq!(
                publication_kill_diagnostic(&evidence)["original_query_ok"],
                true
            );
            assert!(actuals.iter().any(|a| a["query"] == 1
                && a["complete"] == true
                && a["result_statements"] == serde_json::json!(["SELECT 11", "SELECT held"])));
            assert!(!oracle_recorded(&evidence, 2, "legacy_conversion", "pass"));
            assert_eq!(calls, [1, 1]);
            assert_eq!(
                posted_requests(&requests),
                ["SELECT 11", "SELECT held", "SELECT 99"]
            );
        }
    }

    #[test]
    fn outer_publication_kill_http_protocol_and_statement_are_not_controlled_transport() {
        for reply in [
            PublicationKillReply::Http,
            PublicationKillReply::Protocol,
            PublicationKillReply::Statement,
        ] {
            let (outcome, evidence, golden, requests, actuals, calls) =
                run_publication_kill_boundary(reply, PublicationKillOwner::Confirmed, true);
            assert!(matches!(outcome.status, crate::CaseStatus::Fail));
            assert!(
                outcome.log.contains("target infrastructure failure"),
                "{}",
                outcome.log
            );
            assert_eq!(
                publication_kill_diagnostic(&evidence)["original_transport_only"],
                false
            );
            assert!(!oracle_recorded(&evidence, 1, "expected_error", "pass"));
            assert!(!oracle_recorded(&evidence, 2, "legacy_conversion", "pass"));
            assert_eq!(
                golden,
                "-- query 1\n-- legacy\nv\n11\n\n-- query 2\n-- legacy\nv\n7\n"
            );
            assert!(
                actuals
                    .iter()
                    .any(|a| a["complete"] == false && a["rows"] == serde_json::json!([["11"]]))
            );
            assert_eq!(calls, [1, 1]);
            assert_eq!(
                posted_requests(&requests),
                ["SELECT 11", "SELECT held", "SELECT 99"]
            );
            assert_eq!(
                requests.iter().filter(|r| r.method == "DELETE").count(),
                usize::from(reply == PublicationKillReply::Protocol)
            );
        }
    }

    #[test]
    fn outer_publication_kill_prior_sql_failure_plus_transport_cannot_become_control_success() {
        let (outcome, evidence, golden, requests, actuals, calls) = run_publication_kill_boundary(
            PublicationKillReply::MixedStatementThenTransport,
            PublicationKillOwner::Confirmed,
            true,
        );
        assert!(matches!(outcome.status, crate::CaseStatus::Fail));
        let diagnostic = publication_kill_diagnostic(&evidence);
        assert_eq!(diagnostic["original_transport_only"], false);
        assert_eq!(diagnostic["original_failure_origin"], "infrastructure");
        let error = diagnostic["original_error"].as_str().unwrap();
        assert!(
            error.contains("earlier original SQL error")
                && error.contains("Trino transport failure")
        );
        assert!(!oracle_recorded(&evidence, 2, "legacy_conversion", "pass"));
        assert_eq!(
            golden,
            "-- query 1\n-- legacy\nv\n11\n\n-- query 2\n-- legacy\nv\n7\n"
        );
        assert!(
            actuals
                .iter()
                .any(|a| a["complete"] == false && a["rows"] == serde_json::json!([["11"]]))
        );
        assert_eq!(calls, [1, 1]);
        assert_eq!(
            posted_requests(&requests),
            ["CALL failed", "SELECT 11", "SELECT held", "SELECT 99"]
        );
    }

    #[test]
    fn transport_only_invocation_resets_and_local_control_or_render_refusal_never_mints_it() {
        let mut dropped = Reply::json("POST", "/v1/statement", "");
        dropped.drop_before_response = true;
        let mut canceled = Reply::json("DELETE", "/local", "");
        canceled.status = 204;
        let mut replies = vec![dropped, Reply::queued("/v1/statement", "/local"), canceled];
        replies.extend(boundary_values(&[7]));
        let peer = Peer::start(replies);
        let mut session = peer.session();
        session.set_conversion_mode(true);
        let raw = session.execute_query(2, "SELECT transport", None);
        assert!(!raw.0 && raw.1.is_some());
        assert!(raw.failure_is_only_transport());
        assert_eq!(
            crate::SqlStepOutcome::from_session(raw).3,
            Some(crate::ExecutionFailureOrigin::Transport)
        );
        let raw = session.execute_query_with_before_execute(
            2,
            "SELECT local",
            None,
            true,
            |_, _, _, _, _| anyhow::bail!("original before-operation refusal"),
        );
        assert!(!raw.0 && !raw.failure_is_only_transport());
        assert_eq!(
            crate::SqlStepOutcome::from_session(raw).3,
            Some(crate::ExecutionFailureOrigin::Infrastructure)
        );
        session.set_operation_deadline(Some(Instant::now()));
        let raw = session.execute_query(2, "SELECT expired", None);
        assert!(!raw.0 && !raw.failure_is_only_transport());
        assert_eq!(
            crate::SqlStepOutcome::from_session(raw).3,
            Some(crate::ExecutionFailureOrigin::Infrastructure)
        );
        session.set_operation_deadline(None);
        let raw = session.execute_query(2, "SELECT 7", Some("a.b.c"));
        assert!(!raw.0 && !raw.failure_is_only_transport());
        assert_eq!(
            crate::SqlStepOutcome::from_session(raw).3,
            Some(crate::ExecutionFailureOrigin::Infrastructure)
        );
        let raw = session.execute_query(2, "SELECT 'unterminated", None);
        assert!(!raw.0 && !raw.failure_is_only_transport());
        assert_eq!(
            crate::SqlStepOutcome::from_session(raw).3,
            Some(crate::ExecutionFailureOrigin::Infrastructure)
        );
        let raw = session.execute_query(2, "SELECT 7", None);
        assert!(raw.0 && !raw.failure_is_only_transport());
        assert_eq!(crate::SqlStepOutcome::from_session(raw).3, None);
        assert_eq!(
            posted_requests(&peer.finish()),
            ["SELECT transport", "SELECT local", "SELECT 7"]
        );
        // This valid wire signature is intentionally different from the
        // frozen canonical unbounded VARCHAR declaration. Only the renderer
        // rejects it, after real protocol decoding has produced a typed row.
        let peer = Peer::start(vec![Reply::json(
            "POST",
            "/v1/statement",
            r#"{"id":"q","columns":[{"name":"v","type":"varchar","typeSignature":{"rawType":"varchar","arguments":[]}}],"data":[["text"]]}"#,
        )]);
        let mut session = peer.session();
        session.set_conversion_mode(true);
        let raw = session.execute_query(2, "SELECT text", None);
        assert!(!raw.0 && raw.1.is_some());
        assert!(raw.2.contains("typed result rendering"), "{}", raw.2);
        assert!(!raw.failure_is_only_transport());
        assert_eq!(
            crate::SqlStepOutcome::from_session(raw).3,
            Some(crate::ExecutionFailureOrigin::Infrastructure)
        );
        peer.finish();
    }

    #[test]
    fn outer_uncontrolled_partial_transport_stays_infrastructure_before_negative_or_authoring() {
        let mut dropped = Reply::json("POST", "/v1/statement", "");
        dropped.drop_before_response = true;
        let mut replies = boundary_values(&[11]);
        replies.push(dropped);
        replies.extend(boundary_success_replies(1));
        let peer = Peer::start(replies);
        let temp = tempfile::TempDir::new().unwrap();
        let initial = "-- query 1\n-- legacy\nv\n11\n\n-- query 2\n-- legacy\nv\n7\n";
        let steps = vec![
            boundary_step(
                1,
                "SELECT 11; SELECT transport",
                crate::types::QueryMeta {
                    expect_error: Some("Trino transport failure".into()),
                    retry_count: Some(2),
                    retry_interval_ms: Some(0),
                    ..Default::default()
                },
            ),
            boundary_step(2, "SELECT 7", Default::default()),
            boundary_cleanup(3),
        ];
        let (outcome, evidence, golden) = run_boundary_steps(
            &peer,
            &temp,
            crate::Mode::Record,
            true,
            steps,
            Some(initial),
            Box::new(BoundaryServer),
        );
        assert!(matches!(outcome.status, crate::CaseStatus::Fail));
        assert!(
            outcome.log.contains("target infrastructure failure"),
            "{}",
            outcome.log
        );
        assert!(!oracle_recorded(&evidence, 1, "expected_error", "pass"));
        assert!(!oracle_recorded(&evidence, 1, "legacy_conversion", "pass"));
        assert!(!oracle_recorded(&evidence, 2, "legacy_conversion", "pass"));
        assert_eq!(std::fs::read_to_string(golden).unwrap(), initial);
        assert!(
            evidence
                .artifacts
                .iter()
                .filter_map(|p| std::fs::read_to_string(p.join("actual.json")).ok())
                .map(|s| serde_json::from_str::<serde_json::Value>(&s).unwrap())
                .any(|a| a["complete"] == false && a["rows"] == serde_json::json!([["11"]]))
        );
        assert_eq!(
            posted_requests(&peer.finish()),
            ["SELECT 11", "SELECT transport", "SELECT 99"]
        );
    }

    #[test]
    fn outer_domain_oracle_transport_remains_infrastructure_policy_and_stops_same_step() {
        for stateless in [false, true] {
            let mut replies = boundary_values(&[7]);
            let mut dropped = Reply::json("POST", "/v1/statement", "");
            dropped.drop_before_response = true;
            replies.push(dropped);
            replies.extend(boundary_success_replies(1));
            let peer = Peer::start(replies);
            let temp = tempfile::TempDir::new().unwrap();
            let mut meta = crate::types::QueryMeta::default();
            if stateless {
                meta.imv_stateless_rebuild = Some(crate::types::ImvStatelessDirective {
                    catalog: None,
                    mv: "target_mv".into(),
                    level: crate::types::ImvStatelessLevel::Baseline,
                });
            } else {
                meta.imv_equivalence_check = Some("target_mv".into());
            }
            let steps = vec![
                boundary_step(1, "SELECT 7", meta),
                boundary_step(2, "SELECT 7", Default::default()),
                boundary_cleanup(3),
            ];
            let initial = "-- query 1\n-- legacy\nv\n7\n\n-- query 2\n-- legacy\nv\n7\n";
            let (outcome, evidence, golden) = run_boundary_steps(
                &peer,
                &temp,
                crate::Mode::Record,
                true,
                steps,
                Some(initial),
                Box::new(BoundaryServer),
            );
            assert!(matches!(outcome.status, crate::CaseStatus::Fail));
            assert!(
                evidence
                    .executed
                    .iter()
                    .any(|e| e["kind"] == "domain_oracle_execution"
                        && e["step"] == 1
                        && e["policy"]["failure_origin"] == "infrastructure"
                        && e["policy"]["authoring_stopped"] == true)
            );
            assert!(!oracle_recorded(&evidence, 1, "legacy_conversion", "pass"));
            assert!(!oracle_recorded(&evidence, 2, "legacy_conversion", "pass"));
            assert_eq!(std::fs::read_to_string(golden).unwrap(), initial);
            assert_eq!(
                posted_requests(&peer.finish()),
                ["SELECT 7", "SELECT * FROM target_mv", "SELECT 99"]
            );
        }
    }

    #[test]
    fn outer_generic_fe_control_cannot_reclassify_mixed_sql_then_transport_as_injected_disconnect()
    {
        let gate = Arc::new(FaultPeerGate {
            wait_for_action: true,
            ..Default::default()
        });
        let calls = Arc::new(Mutex::new([0; 3]));
        let mut dropped = Reply::json("POST", "/v1/statement", "");
        dropped.drop_before_response = true;
        dropped.fault_gate = Some(Arc::clone(&gate));
        let mut replies = vec![
            Reply::queued("/v1/statement", "/first-error"),
            Reply::json(
                "GET",
                "/first-error",
                &trino_error("earlier original SQL error"),
            ),
            dropped,
        ];
        replies.extend(boundary_success_replies(1));
        let peer = Peer::start(replies);
        let temp = tempfile::TempDir::new().unwrap();
        let initial = "-- query 2\n-- legacy\nv\n7\n";
        let steps = vec![
            boundary_step(
                1,
                "CALL failed; SELECT transport",
                crate::types::QueryMeta {
                    kill_fe_after_be_log_contains: Some("BOUNDARY_QUERY_ACTIVE".into()),
                    expect_error: Some("Trino transport failure".into()),
                    retry_count: Some(2),
                    retry_interval_ms: Some(0),
                    ..Default::default()
                },
            ),
            boundary_step(2, "SELECT 7", Default::default()),
            boundary_cleanup(3),
        ];
        let (outcome, evidence, golden) = run_boundary_steps(
            &peer,
            &temp,
            crate::Mode::Record,
            true,
            steps,
            Some(initial),
            Box::new(FrontendBoundaryServer {
                mode: FrontendFaultMode::Confirmed,
                gate,
                calls: Arc::clone(&calls),
            }),
        );
        assert!(matches!(outcome.status, crate::CaseStatus::Fail));
        assert!(
            outcome.log.contains("target infrastructure failure"),
            "{}",
            outcome.log
        );
        assert!(outcome.log.contains("earlier original SQL error"));
        assert!(!oracle_recorded(&evidence, 1, "expected_error", "pass"));
        assert!(!oracle_recorded(&evidence, 2, "legacy_conversion", "pass"));
        assert_eq!(std::fs::read_to_string(golden).unwrap(), initial);
        assert_eq!(*calls.lock().unwrap(), [1, 2, 1]);
        assert_eq!(
            posted_requests(&peer.finish()),
            ["CALL failed", "SELECT transport", "SELECT 99"]
        );
    }

    fn release_delayed_http_reply(gate: Arc<FaultPeerGate>) -> JoinHandle<()> {
        thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            while !gate.request_seen.load(Ordering::Acquire) {
                assert!(
                    Instant::now() < deadline,
                    "timed HTTP request was never observed"
                );
                thread::sleep(Duration::from_millis(1));
            }
            thread::sleep(Duration::from_millis(1200));
            gate.action_done.store(true, Ordering::Release);
        })
    }

    #[test]
    fn original_owner_keeps_server_query_timeout_separate_from_client_wire_deadline() {
        for convert in [false, true] {
            let gate = Arc::new(FaultPeerGate {
                wait_for_action: true,
                ..Default::default()
            });
            // This is the real frozen Trino error DTO/code. The test server
            // sends it over HTTP after the one-second server property budget;
            // no client Deadline is relabeled as a statement failure.
            let body = serde_json::json!({"id":"server-timeout","error":{
                "message":"query execution deadline exceeded",
                "errorCode":131075,"errorName":"EXCEEDED_TIME_LIMIT",
                "errorType":"INSUFFICIENT_RESOURCES"}})
            .to_string();
            let mut reply = Reply::json("POST", "/v1/statement", &body);
            reply.fault_gate = Some(Arc::clone(&gate));
            let peer = Peer::start(vec![reply]);
            let release = release_delayed_http_reply(gate);
            let owner = Instant::now() + Duration::from_secs(5);
            let mut session =
                TrinoCaseSession::new_with_deadline(&peer.config(), Some(owner)).unwrap();
            session.set_conversion_mode(convert);
            let started = Instant::now();
            let raw = session.execute_query(1, "SELECT server_timeout", None);
            assert!(!raw.0 && raw.1.is_some() == convert);
            assert!(started.elapsed() >= Duration::from_secs(1));
            assert!(
                matches!(raw.final_failures().first().map(|failure| match failure { SqlExecutionFailure::Client(failure) => failure, SqlExecutionFailure::Local(_) => panic!("expected client failure") }),Some(StatementFailure::Trino(e))
                if e.error_code==131075 && e.error_name=="EXCEEDED_TIME_LIMIT"
                && e.error_type=="INSUFFICIENT_RESOURCES")
            );
            assert!(raw.failure_is_statement());
            assert!(!raw.failure_is_only_transport());
            assert_eq!(
                crate::SqlStepOutcome::from_session(raw).3,
                Some(crate::ExecutionFailureOrigin::Statement)
            );
            assert_eq!(session.original_deadline(), Some(owner));
            assert_eq!(session.operation_deadline, None);
            assert_eq!(session.state.properties["query_timeout"], "1");
            release.join().unwrap();
            let requests = peer.finish();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].method, "POST");
            assert!(requests[0].headers["x-trino-session"].contains("query_timeout=1"));
        }
    }

    fn assert_original_client_deadline_and_existing_cancel(
        original_owner: Option<Duration>,
        operation_owner: Option<Duration>,
        server_query_timeout: u64,
    ) {
        for convert in [false, true] {
            let gate = Arc::new(FaultPeerGate {
                wait_for_action: true,
                ..Default::default()
            });
            let mut held = Reply::json("GET", "/original-deadline", "");
            held.fault_gate = Some(Arc::clone(&gate));
            held.drop_before_response = true;
            let mut canceled = Reply::json("DELETE", "/original-deadline", "");
            canceled.status = 204;
            let peer = Peer::start(vec![
                Reply::queued("/v1/statement", "/original-deadline"),
                held,
                canceled,
            ]);
            let release = release_delayed_http_reply(gate);
            let admitted = Instant::now();
            let original_deadline = original_owner.map(|duration| admitted + duration);
            let operation_deadline = operation_owner.map(|duration| admitted + duration);
            let mut session =
                TrinoCaseSession::new_with_deadline(&peer.config(), original_deadline).unwrap();
            session.set_operation_deadline(operation_deadline);
            session.set_conversion_mode(convert);
            let raw = session.execute_query(server_query_timeout, "SELECT client_timeout", None);
            assert!(!raw.0 && raw.1.is_some() == convert);
            assert!(matches!(
                raw.final_failures().first().map(|failure| match failure {
                    SqlExecutionFailure::Client(failure) => failure,
                    SqlExecutionFailure::Local(_) => panic!("expected client failure"),
                }),
                Some(StatementFailure::Deadline)
            ));
            assert!(!raw.failure_is_statement() && !raw.failure_is_only_transport());
            assert_eq!(
                crate::SqlStepOutcome::from_session(raw).3,
                Some(crate::ExecutionFailureOrigin::Infrastructure)
            );
            assert_eq!(session.original_deadline(), original_deadline);
            assert_eq!(session.operation_deadline, operation_deadline);
            assert_eq!(
                session.state.properties["query_timeout"],
                server_query_timeout.to_string()
            );
            if original_owner.is_some() || operation_owner.is_some() {
                assert!(session.before_operation().is_err());
            }
            release.join().unwrap();
            let requests = peer.finish();
            assert_eq!(
                requests
                    .iter()
                    .map(|r| r.method.as_str())
                    .collect::<Vec<_>>(),
                ["POST", "GET", "DELETE"]
            );
            assert_eq!(requests[2].path, "/original-deadline");
            // PendingStatement's existing one-second best-effort cancellation
            // grace permits this DELETE. The original wire verdict and runner
            // absolute deadline remain expired and are never extended/reset.
            let expected_property = format!("query_timeout={server_query_timeout}");
            assert!(
                requests
                    .iter()
                    .all(|r| { r.headers["x-trino-session"].contains(expected_property.as_str()) })
            );
        }
    }

    #[test]
    fn earlier_original_owner_still_produces_client_deadline_and_exact_cancellation() {
        assert_original_client_deadline_and_existing_cancel(
            Some(Duration::from_millis(750)),
            None,
            5,
        );
    }

    #[test]
    fn earlier_fault_operation_owner_still_produces_client_deadline_and_exact_cancellation() {
        assert_original_client_deadline_and_existing_cancel(
            Some(Duration::from_secs(5)),
            Some(Duration::from_millis(750)),
            5,
        );
    }

    #[test]
    fn ownerless_utility_keeps_finite_query_timeout_fallback_and_exact_cancellation() {
        assert_original_client_deadline_and_existing_cancel(None, None, 1);
    }
    #[test]
    fn outer_record_conversion_emits_each_successful_explain_requirement_once() {
        for (mode, convert) in [(crate::Mode::Verify, false), (crate::Mode::Record, true)] {
            let peer = Peer::start(boundary_success_replies(3));
            let temp = tempfile::TempDir::new().unwrap();
            let steps = vec![
                boundary_step(
                    1,
                    "SELECT 7",
                    crate::types::QueryMeta {
                        explain_contains: vec!["7".into()],
                        explain_not_contains: vec!["absent-plan-token".into()],
                        ..Default::default()
                    },
                ),
                boundary_cleanup(2),
            ];
            let (outcome, evidence, _) = run_boundary_steps(
                &peer,
                &temp,
                mode,
                convert,
                steps,
                Some(if convert {
                    "-- query 1\n-- legacy\nv\n7\n"
                } else {
                    "-- query 1\n-- types\tbigint\nv\n7\n"
                }),
                Box::new(BoundaryServer),
            );
            assert!(
                matches!(outcome.status, crate::CaseStatus::Pass),
                "{}",
                outcome.log
            );
            for kind in ["explain_contains", "explain_not_contains"] {
                let events = evidence
                    .executed
                    .iter()
                    .filter(|event| event["step"] == 1 && event["kind"] == kind)
                    .collect::<Vec<_>>();
                assert_eq!(
                    events.len(),
                    1,
                    "each required EXPLAIN check must have one author"
                );
                assert_eq!(events[0]["outcome"], "pass");
                assert_eq!(events[0]["policy"]["explain_rewritten"], false);
            }
            if convert {
                assert!(oracle_recorded(&evidence, 1, "legacy_conversion", "pass"));
            }
            assert_eq!(
                posted_requests(&peer.finish()),
                ["SELECT 7", "EXPLAIN VERBOSE SELECT 7", "SELECT 99"]
            );
        }
    }

    #[test]
    fn outer_record_conversion_retains_failed_explain_requirement_outcomes() {
        for fail_contains in [true, false] {
            for (mode, convert) in [(crate::Mode::Verify, false), (crate::Mode::Record, true)] {
                let peer = Peer::start(boundary_success_replies(3));
                let temp = tempfile::TempDir::new().unwrap();
                let steps = vec![
                    boundary_step(
                        1,
                        "SELECT 7",
                        crate::types::QueryMeta {
                            explain_contains: vec![
                                if fail_contains {
                                    "missing-plan-token"
                                } else {
                                    "7"
                                }
                                .into(),
                            ],
                            explain_not_contains: vec![
                                if fail_contains {
                                    "absent-plan-token"
                                } else {
                                    "7"
                                }
                                .into(),
                            ],
                            ..Default::default()
                        },
                    ),
                    boundary_cleanup(2),
                ];
                let (outcome, evidence, _) = run_boundary_steps(
                    &peer,
                    &temp,
                    mode,
                    convert,
                    steps,
                    Some(if convert {
                        "-- query 1\n-- legacy\nv\n7\n"
                    } else {
                        "-- query 1\n-- types\tbigint\nv\n7\n"
                    }),
                    Box::new(BoundaryServer),
                );
                assert!(
                    matches!(outcome.status, crate::CaseStatus::Fail),
                    "{}",
                    outcome.log
                );
                assert!(outcome.log.contains(if fail_contains {
                    "@explain_contains assertion failed"
                } else {
                    "@explain_not_contains assertion failed"
                }));
                for kind in ["explain_contains", "explain_not_contains"] {
                    let events = evidence
                        .executed
                        .iter()
                        .filter(|event| event["step"] == 1 && event["kind"] == kind)
                        .collect::<Vec<_>>();
                    assert_eq!(
                        events.len(),
                        1,
                        "failed EXPLAIN must not gain duplicate positive evidence"
                    );
                    assert_eq!(events[0]["outcome"], "fail");
                }
                if convert {
                    assert!(
                        oracle_recorded(&evidence, 1, "legacy_conversion", "pass"),
                        "existing conversion proof policy must remain unchanged"
                    );
                }
                assert_eq!(
                    posted_requests(&peer.finish()),
                    ["SELECT 7", "EXPLAIN VERBOSE SELECT 7", "SELECT 99"]
                );
            }
        }
    }
}
