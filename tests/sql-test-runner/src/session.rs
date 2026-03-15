use crate::results::{parse_output, render_output};
use crate::runner::is_transient_iceberg_commit_error;
use crate::types::*;
use anyhow::{Context, Result, bail};
use mysql::prelude::Queryable;
use mysql::{Conn as MysqlConn, OptsBuilder, Row as MysqlRow, Value as MysqlValue};
use std::process::Command;
use std::time::{Duration, Instant};

pub fn build_statements(
    sql: &str,
    query_timeout: u64,
    catalog: Option<&str>,
    db: Option<&str>,
) -> String {
    let mut statements = Vec::new();
    if let Some(c) = catalog {
        statements.push(format!("SET catalog {};", c));
    }
    if let Some(d) = db {
        if !d.is_empty() {
            statements.push(format!("USE {};", d));
        }
    }
    statements.push(format!("SET query_timeout={};", query_timeout));
    statements.push(sql.to_string());
    statements.join("\n")
}

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
        };

        session.apply_base_context(conn)?;
        Ok(session)
    }

    pub fn apply_base_context(&mut self, conn: &ConnectionConfig) -> Result<()> {
        // Align sql-tests sessions with the default dev/test harness so FE planner timeouts do not
        // dominate correctness runs under suite-wide load.
        self.conn
            .query_drop("SET new_planner_optimize_timeout = 10000")
            .context("failed to set new_planner_optimize_timeout")?;
        if let Some(catalog) = conn.catalog.as_deref() {
            if !catalog.is_empty() {
                self.conn
                    .query_drop(format!("SET catalog {}", catalog))
                    .with_context(|| format!("failed to set catalog {}", catalog))?;
            }
        }
        if let Some(db) = conn.db.as_deref() {
            if !db.is_empty() {
                self.conn
                    .query_drop(format!("USE {}", db))
                    .with_context(|| format!("failed to USE {}", db))?;
            }
        }
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

        for attempt in 0..MAX_TRANSIENT_ATTEMPTS {
            let started = Instant::now();
            if let Some(db) = db_override {
                if !db.is_empty() {
                    if let Err(exc) = self.conn.query_drop(format!("USE {}", db)) {
                        let elapsed = started.elapsed();
                        return (
                            false,
                            None,
                            format!("ERROR ({:.2}s): {}", elapsed.as_secs_f64(), exc),
                        );
                    }
                }
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

            match self.conn.query_iter(sql) {
                Ok(mut query_result) => {
                    let mut last_header: Vec<String> = Vec::new();
                    let mut last_rows: Vec<Vec<String>> = Vec::new();
                    let mut saw_tabular_result = false;

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
                                    let elapsed = started.elapsed();
                                    let message = exc.to_string();
                                    let clipped = if message.len() > 500 {
                                        message[..500].to_string()
                                    } else {
                                        message
                                    };
                                    return (
                                        false,
                                        None,
                                        format!("FAIL ({:.2}s): {}", elapsed.as_secs_f64(), clipped),
                                    );
                                }
                            }
                        }

                        if !header.is_empty() {
                            saw_tabular_result = true;
                            last_header = header;
                            last_rows = rows;
                        } else if !saw_tabular_result {
                            last_header = header;
                            last_rows = rows;
                        }
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
                Err(exc) => {
                    let elapsed = started.elapsed();
                    let message = exc.to_string();
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
                        format!("FAIL ({:.2}s): {}", elapsed.as_secs_f64(), clipped),
                    );
                }
            }
        }

        (
            false,
            None,
            "FAIL (0.00s): exhausted query attempts unexpectedly".to_string(),
        )
    }
}

pub fn run_mysql_sql(conn: &ConnectionConfig, sql: &str, skip_column_names: bool) -> Result<String> {
    let mut cmd = Command::new(&conn.mysql);
    cmd.arg(format!("-h{}", conn.host))
        .arg(format!("-P{}", conn.port))
        .arg(format!("-u{}", conn.user))
        .arg("--batch")
        .arg("--raw")
        .arg("--default-character-set=utf8mb4");
    if let Some(password) = conn.password.as_deref() {
        if !password.is_empty() {
            cmd.arg(format!("-p{}", password));
        }
    }
    if skip_column_names {
        cmd.arg("--skip-column-names");
    }
    cmd.arg("-e").arg(sql);

    let output = cmd
        .output()
        .with_context(|| format!("failed to execute mysql command: {}", conn.mysql))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let detail = if !stderr.is_empty() { stderr } else { stdout };
        bail!("mysql command failed: {}", detail);
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

pub fn execute_suite_hook(
    conn: &ConnectionConfig,
    query_timeout: u64,
    hook: &SuiteHook,
    label: &str,
) -> Result<()> {
    let sql = build_statements(&hook.sql, query_timeout, None, None);
    run_mysql_sql(conn, &sql, true)
        .with_context(|| format!("{} suite hook failed: {}", label, hook.path.display()))?;
    Ok(())
}

pub fn reset_case_database(
    conn: &ConnectionConfig,
    query_timeout: u64,
    db_name: &str,
    label: &str,
) -> Result<()> {
    let sql = build_statements(
        &format!(
            "DROP DATABASE IF EXISTS `{db_name}` FORCE;\nCREATE DATABASE `{db_name}`;"
        ),
        query_timeout,
        None,
        None,
    );
    run_mysql_sql(conn, &sql, true)
        .with_context(|| format!("{} case database reset failed: {}", label, db_name))?;
    Ok(())
}

pub fn drop_case_database(
    conn: &ConnectionConfig,
    query_timeout: u64,
    db_name: &str,
    label: &str,
) -> Result<()> {
    let sql = build_statements(
        &format!("DROP DATABASE IF EXISTS `{db_name}` FORCE;"),
        query_timeout,
        None,
        None,
    );
    run_mysql_sql(conn, &sql, true)
        .with_context(|| format!("{} case database cleanup failed: {}", label, db_name))?;
    Ok(())
}

pub fn execute_query_via_cli(
    conn: &ConnectionConfig,
    query_timeout: u64,
    sql: &str,
) -> (bool, Option<QueryExecution>, String) {
    let full_sql = build_statements(
        sql,
        query_timeout,
        conn.catalog.as_deref(),
        conn.db.as_deref(),
    );

    const MAX_TRANSIENT_ATTEMPTS: usize = 2;
    const TRANSIENT_RETRY_DELAY_MS: u64 = 300;

    for attempt in 0..MAX_TRANSIENT_ATTEMPTS {
        let started = Instant::now();
        let output = match Command::new(&conn.mysql)
            .arg(format!("-h{}", conn.host))
            .arg(format!("-P{}", conn.port))
            .arg(format!("-u{}", conn.user))
            .arg("--batch")
            .arg("--raw")
            .arg("--default-character-set=utf8mb4")
            .args(
                conn.password
                    .as_deref()
                    .filter(|password| !password.is_empty())
                    .map(|password| vec![format!("-p{}", password)])
                    .unwrap_or_default(),
            )
            .arg("-e")
            .arg(&full_sql)
            .output()
        {
            Ok(out) => out,
            Err(exc) => {
                let elapsed = started.elapsed();
                return (
                    false,
                    None,
                    format!("ERROR ({:.2}s): {}", elapsed.as_secs_f64(), exc),
                );
            }
        };

        let elapsed = started.elapsed();
        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout).to_string();
            let (header, rows) = parse_output(&stdout);
            let execution = QueryExecution {
                text_output: render_output(&header, &rows),
                header,
                rows,
                elapsed,
            };
            return (true, Some(execution), String::new());
        }

        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let message = if !stderr.is_empty() { stderr } else { stdout };
        if attempt + 1 < MAX_TRANSIENT_ATTEMPTS && is_transient_iceberg_commit_error(&message) {
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
            format!("FAIL ({:.2}s): {}", elapsed.as_secs_f64(), clipped),
        );
    }

    (
        false,
        None,
        "FAIL (0.00s): exhausted query attempts unexpectedly".to_string(),
    )
}
