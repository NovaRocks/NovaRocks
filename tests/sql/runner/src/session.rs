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

use crate::results::render_output;
use crate::runner::is_transient_iceberg_commit_error;
use crate::types::*;
use anyhow::{Context, Result, bail};
use mysql::prelude::Queryable;
use mysql::{Conn as MysqlConn, OptsBuilder, Row as MysqlRow, Value as MysqlValue};
use sha2::{Digest, Sha256};
use std::time::{Duration, Instant};

pub fn mysql_value_to_string(value: &MysqlValue) -> String {
    match value {
        MysqlValue::NULL => "NULL".to_string(),
        MysqlValue::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        MysqlValue::Int(v) => v.to_string(),
        MysqlValue::UInt(v) => v.to_string(),
        MysqlValue::Float(v) => v.to_string(),
        MysqlValue::Double(v) => v.to_string(),
        MysqlValue::Date(year, mon, day, hour, min, sec, usec) => {
            if *hour == 0 && *min == 0 && *sec == 0 && *usec == 0 {
                format!("{year:04}-{mon:02}-{day:02}")
            } else if *usec == 0 {
                format!("{year:04}-{mon:02}-{day:02} {hour:02}:{min:02}:{sec:02}")
            } else {
                format!("{year:04}-{mon:02}-{day:02} {hour:02}:{min:02}:{sec:02}.{usec:06}")
            }
        }
        MysqlValue::Time(is_neg, days, hours, mins, secs, usec) => {
            let total_hours = days * 24 + u32::from(*hours);
            let sign = if *is_neg { "-" } else { "" };
            if *usec == 0 {
                format!("{sign}{total_hours:02}:{mins:02}:{secs:02}")
            } else {
                format!("{sign}{total_hours:02}:{mins:02}:{secs:02}.{usec:06}")
            }
        }
    }
}

pub fn mysql_row_to_strings(row: MysqlRow) -> Vec<String> {
    (0..row.len())
        .map(|idx| {
            row.as_ref(idx)
                .map(mysql_value_to_string)
                .unwrap_or_else(|| "NULL".to_string())
        })
        .collect()
}

pub struct MysqlSession {
    pub conn: MysqlConn,
    base_config: ConnectionConfig,
}

impl MysqlSession {
    pub fn new(conn: &ConnectionConfig) -> Result<Self> {
        let port = conn
            .port
            .parse::<u16>()
            .with_context(|| format!("invalid mysql port: {}", conn.port))?;
        let builder = OptsBuilder::new()
            .ip_or_hostname(Some(conn.host.clone()))
            .tcp_port(port)
            .prefer_socket(false)
            .user(Some(conn.user.clone()))
            .pass(conn.password.clone());
        let mut session = Self {
            conn: MysqlConn::new(builder).with_context(|| {
                format!(
                    "failed to establish mysql protocol session to {}:{}",
                    conn.host, conn.port
                )
            })?,
            base_config: conn.clone(),
        };

        session.apply_base_context(conn)?;
        Ok(session)
    }

    pub fn apply_base_context(&mut self, conn: &ConnectionConfig) -> Result<()> {
        // Align SQL-test sessions with the default dev/test harness so FE planner timeouts do not
        // dominate correctness runs under suite-wide load.
        self.conn
            .query_drop("SET new_planner_optimize_timeout = 10000")
            .context("failed to set new_planner_optimize_timeout")?;
        if let Some(catalog) = conn.catalog.as_deref()
            && !catalog.is_empty()
        {
            self.conn
                .query_drop(format!("SET catalog {}", catalog))
                .with_context(|| format!("failed to set catalog {}", catalog))?;
        }
        if let Some(db) = conn.db.as_deref()
            && !db.is_empty()
        {
            self.conn
                .query_drop(format!("USE {}", db))
                .with_context(|| format!("failed to USE {}", db))?;
        }
        Ok(())
    }

    pub fn connection_id(&self) -> u32 {
        self.conn.connection_id()
    }

    pub fn reconnect(&mut self) -> Result<()> {
        let config = self.base_config.clone();
        *self = Self::new(&config)?;
        Ok(())
    }

    pub fn execute_query(
        &mut self,
        query_timeout: u64,
        sql: &str,
        db_override: Option<&str>,
    ) -> (bool, Option<QueryExecution>, String) {
        const MAX_TRANSIENT_ATTEMPTS: usize = 2;
        const TRANSIENT_RETRY_DELAY_MS: u64 = 300;
        let statements = match split_sql_statements(sql) {
            Ok(statements) => statements,
            Err(err) => {
                return (false, None, format!("ERROR (0.00s): {}", err));
            }
        };

        for attempt in 0..MAX_TRANSIENT_ATTEMPTS {
            let started = Instant::now();
            if let Some(db) = db_override
                && !db.is_empty()
                && let Err(exc) = self.conn.query_drop(format!("USE {}", db))
            {
                let elapsed = started.elapsed();
                return (
                    false,
                    None,
                    format!("ERROR ({:.2}s): {}", elapsed.as_secs_f64(), exc),
                );
            }

            if let Err(exc) = self
                .conn
                .query_drop(format!("SET query_timeout={}", query_timeout))
            {
                let elapsed = started.elapsed();
                return (
                    false,
                    None,
                    format!("ERROR ({:.2}s): {}", elapsed.as_secs_f64(), exc),
                );
            }

            let mut last_header: Vec<String> = Vec::new();
            let mut last_rows: Vec<Vec<String>> = Vec::new();
            let mut saw_tabular_result = false;
            let mut failed = None;

            for (statement_index, statement) in statements.iter().enumerate() {
                match self.conn.query_iter(statement) {
                    Ok(mut query_result) => {
                        while let Some(mut result_set) = query_result.iter() {
                            let header: Vec<String> = result_set
                                .columns()
                                .as_ref()
                                .iter()
                                .map(|column| column.name_str().to_string())
                                .collect();
                            let mut rows: Vec<Vec<String>> = Vec::new();

                            for row_result in result_set.by_ref() {
                                match row_result {
                                    Ok(row) => rows.push(mysql_row_to_strings(row)),
                                    Err(exc) => {
                                        failed = Some((statement_index, exc.to_string()));
                                        break;
                                    }
                                }
                            }
                            if failed.is_some() {
                                break;
                            }

                            if !header.is_empty() {
                                if !saw_tabular_result {
                                    // First tabular result — use its header
                                    // (matches `mysql --batch` behavior where
                                    // the first header line wins).
                                    saw_tabular_result = true;
                                    last_header = header;
                                    last_rows = rows;
                                } else {
                                    // Subsequent result sets: the header row
                                    // becomes a data row in `mysql --batch`
                                    // output, so append it plus the data rows.
                                    last_rows.push(header);
                                    last_rows.extend(rows);
                                }
                            } else if !saw_tabular_result {
                                last_header = header;
                                last_rows = rows;
                            }
                        }
                    }
                    Err(exc) => {
                        failed = Some((statement_index, exc.to_string()));
                    }
                }

                if failed.is_some() {
                    break;
                }
            }

            if let Some((statement_index, message)) = failed {
                let elapsed = started.elapsed();
                if attempt + 1 < MAX_TRANSIENT_ATTEMPTS
                    && is_transient_iceberg_commit_error(&message)
                {
                    std::thread::sleep(Duration::from_millis(TRANSIENT_RETRY_DELAY_MS));
                    continue;
                }
                let clipped = if message.len() > 500 {
                    message[..500].to_string()
                } else {
                    message
                };
                return (
                    false,
                    None,
                    format_statement_failure(
                        elapsed,
                        statement_index,
                        statements.len(),
                        &statements[statement_index],
                        &clipped,
                    ),
                );
            }

            let elapsed = started.elapsed();
            let execution = QueryExecution {
                text_output: render_output(&last_header, &last_rows),
                header: last_header,
                rows: last_rows,
                elapsed,
            };
            return (true, Some(execution), String::new());
        }

        (
            false,
            None,
            "FAIL (0.00s): exhausted query attempts unexpectedly".to_string(),
        )
    }
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

fn split_sql_statements(sql: &str) -> Result<Vec<String>> {
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

pub fn run_mysql_sql(conn: &ConnectionConfig, query_timeout: u64, sql: &str) -> Result<String> {
    let mut session = MysqlSession::new(conn)?;
    let (ok, execution, message) = session.execute_query(query_timeout, sql, None);
    if !ok {
        bail!("mysql protocol command failed: {message}");
    }
    Ok(execution.map_or_else(String::new, |execution| execution.text_output))
}

pub fn execute_suite_hook(
    conn: &ConnectionConfig,
    query_timeout: u64,
    hook: &SuiteHook,
    label: &str,
) -> Result<()> {
    run_mysql_sql(conn, query_timeout, &hook.sql)
        .with_context(|| format!("{} suite hook failed: {}", label, hook.path.display()))?;
    Ok(())
}

/// Names of the tables a case database currently holds.
///
/// `DROP DATABASE ... FORCE` expands into a listing of the namespace's
/// children, so a catalog that cannot enumerate one kind of child refuses it.
/// The harness therefore enumerates and drops explicitly, which works on every
/// catalog.
///
/// The catalog qualifier is required: an unqualified `information_schema` name
/// resolves against the local catalog, and a case database in an external
/// catalog would come back empty.
fn case_table_names(
    conn: &ConnectionConfig,
    query_timeout: u64,
    db_name: &str,
) -> Result<Vec<String>> {
    let Some(catalog) = conn.catalog.as_deref() else {
        return Ok(Vec::new());
    };
    let sql = format!(
        "SELECT table_name FROM `{catalog}`.information_schema.tables \
         WHERE table_schema = '{db_name}';"
    );
    let output = match run_mysql_sql(conn, query_timeout, &sql) {
        Ok(output) => output,
        // A database that does not exist has no tables to drop, and the drop
        // below is idempotent either way.
        Err(_) => return Ok(Vec::new()),
    };
    Ok(output
        .lines()
        .skip(1)
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect())
}

pub fn reset_case_database(
    conn: &ConnectionConfig,
    query_timeout: u64,
    db_name: &str,
    label: &str,
) -> Result<()> {
    drop_case_database(conn, query_timeout, db_name, label)?;
    run_mysql_sql(
        conn,
        query_timeout,
        &format!("CREATE DATABASE `{db_name}`;"),
    )
    .with_context(|| format!("{} case database reset failed: {}", label, db_name))?;
    Ok(())
}

pub fn drop_case_database(
    conn: &ConnectionConfig,
    query_timeout: u64,
    db_name: &str,
    label: &str,
) -> Result<()> {
    // Without a catalog there is nothing to enumerate against, so the explicit
    // path cannot see the children it would need to drop first. Fall back to
    // FORCE, which is what the local catalog still supports and what this did
    // everywhere before enumeration existed.
    if conn.catalog.is_none() {
        let sql = format!("DROP DATABASE IF EXISTS `{db_name}` FORCE;");
        run_mysql_sql(conn, query_timeout, &sql)
            .with_context(|| format!("{} case database cleanup failed: {}", label, db_name))?;
        return Ok(());
    }
    let mut sql = String::new();
    for table in case_table_names(conn, query_timeout, db_name)? {
        sql.push_str(&format!("DROP TABLE IF EXISTS `{db_name}`.`{table}`;\n"));
    }
    sql.push_str(&format!("DROP DATABASE IF EXISTS `{db_name}`;"));
    run_mysql_sql(conn, query_timeout, &sql)
        .with_context(|| format!("{} case database cleanup failed: {}", label, db_name))?;
    Ok(())
}

#[cfg(test)]
mod splitter_tests {
    use super::{format_statement_failure, split_sql_statements, statement_sha256};
    use std::time::Duration;

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
