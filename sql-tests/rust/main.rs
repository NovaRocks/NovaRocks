use anyhow::{Context, Result, bail};
use clap::{ArgAction, Parser, ValueEnum};
use regex::Regex;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
struct SuiteConfig {
    name: String,
    sql_dir: PathBuf,
    result_dir: Option<PathBuf>,
    sql_glob: String,
    default_db: String,
    verify_default: bool,
}

#[derive(Debug, Default, Clone)]
struct QueryMeta {
    order_sensitive: Option<bool>,
    float_epsilon: Option<f64>,
    db: Option<String>,
    tags: Vec<String>,
}

#[derive(Debug, Clone)]
struct QueryCase {
    source_file: PathBuf,
    query_id: String,
    sql: String,
    meta: QueryMeta,
}

#[derive(Debug, Clone)]
struct ConnectionConfig {
    mysql: String,
    host: String,
    port: String,
    user: String,
    catalog: Option<String>,
    db: Option<String>,
}

#[derive(Debug, Clone)]
struct QueryExecution {
    header: Vec<String>,
    rows: Vec<Vec<String>>,
    elapsed: Duration,
}

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
    catalog: Option<String>,

    #[arg(long)]
    db: Option<String>,

    #[arg(long)]
    ref_mysql: Option<String>,

    #[arg(long)]
    ref_host: Option<String>,

    #[arg(long)]
    ref_port: Option<String>,

    #[arg(long)]
    ref_user: Option<String>,

    #[arg(long)]
    ref_catalog: Option<String>,

    #[arg(long)]
    ref_db: Option<String>,

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

fn env_or_default(key: &str, default: &str) -> String {
    env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn env_optional(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn detect_local_target_port(base_dir: &Path) -> Result<Option<String>> {
    let fe_conf = base_dir.join("dev").join("fe").join("conf").join("fe.conf");
    if !fe_conf.exists() {
        return Ok(None);
    }

    let content = fs::read_to_string(&fe_conf)
        .with_context(|| format!("read failed: {}", fe_conf.display()))?;
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((key, value)) = trimmed.split_once('=') else {
            continue;
        };
        if key.trim() != "query_port" {
            continue;
        }
        let port = value.trim();
        if port.is_empty() {
            break;
        }
        return Ok(Some(port.to_string()));
    }

    Ok(None)
}

fn resolve_target_port(cli_port: Option<&str>, base_dir: &Path) -> Result<String> {
    if let Some(port) = cli_port.filter(|v| !v.trim().is_empty()) {
        return Ok(port.trim().to_string());
    }
    if let Some(port) = env_optional("STARUST_TEST_PORT") {
        return Ok(port);
    }
    if let Some(port) = detect_local_target_port(base_dir)? {
        return Ok(port);
    }
    bail!(
        "target port is not set; provide --port or STARUST_TEST_PORT, or configure dev/fe/conf/fe.conf with query_port"
    );
}

fn resolve_reference_port(
    cli_ref_port: Option<&str>,
    target_port: &str,
    reference_required: bool,
) -> Result<String> {
    if let Some(port) = cli_ref_port.filter(|v| !v.trim().is_empty()) {
        return Ok(port.trim().to_string());
    }
    if let Some(port) = env_optional("STARUST_REF_PORT") {
        return Ok(port);
    }
    if reference_required {
        bail!("reference port is required for this mode; provide --ref-port or STARUST_REF_PORT");
    }
    Ok(target_port.to_string())
}

fn resolve_path(path_value: Option<&str>, base_dir: &Path) -> Option<PathBuf> {
    let raw = path_value?;
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        Some(path)
    } else {
        Some(base_dir.join(path))
    }
}

fn parse_bool(raw: &str) -> Result<bool> {
    let lowered = raw.trim().to_lowercase();
    match lowered.as_str() {
        "1" | "true" | "yes" | "y" | "on" => Ok(true),
        "0" | "false" | "no" | "n" | "off" => Ok(false),
        _ => bail!("invalid boolean value: {}", raw),
    }
}

fn parse_meta_line(line: &str, meta_re: &Regex) -> Option<(String, String)> {
    let captures = meta_re.captures(line.trim())?;
    let key = captures.get(1)?.as_str().to_lowercase();
    let value = captures.get(2)?.as_str().trim().to_string();
    Some((key, value))
}

fn parse_meta(lines: &[String], meta_re: &Regex) -> Result<QueryMeta> {
    let mut meta = QueryMeta::default();
    for line in lines {
        let Some((key, raw_value)) = parse_meta_line(line, meta_re) else {
            continue;
        };
        match key.as_str() {
            "order_sensitive" => {
                meta.order_sensitive = Some(parse_bool(&raw_value)?);
            }
            "float_epsilon" => {
                let value: f64 = raw_value
                    .parse()
                    .with_context(|| format!("invalid float_epsilon: {}", raw_value))?;
                if value <= 0.0 {
                    bail!("float_epsilon must be > 0, got {}", value);
                }
                meta.float_epsilon = Some(value);
            }
            "db" => {
                meta.db = Some(raw_value);
            }
            "catalog" => {
                bail!(
                    "@catalog metadata is no longer supported; use runner --catalog/--ref-catalog options"
                );
            }
            "tags" => {
                meta.tags = raw_value
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(ToString::to_string)
                    .collect();
            }
            _ => {}
        }
    }
    Ok(meta)
}

fn merge_meta(base: &QueryMeta, override_meta: &QueryMeta) -> QueryMeta {
    QueryMeta {
        order_sensitive: override_meta.order_sensitive.or(base.order_sensitive),
        float_epsilon: override_meta.float_epsilon.or(base.float_epsilon),
        db: override_meta.db.clone().or_else(|| base.db.clone()),
        tags: if override_meta.tags.is_empty() {
            base.tags.clone()
        } else {
            override_meta.tags.clone()
        },
    }
}

fn extract_meta_and_sql(lines: &[String], meta_re: &Regex) -> Result<(QueryMeta, String)> {
    let mut preface_meta_lines: Vec<String> = Vec::new();
    let mut sql_lines: Vec<String> = Vec::new();
    let mut started = false;

    for line in lines {
        let stripped = line.trim();
        if !started {
            if stripped.is_empty() {
                continue;
            }
            if stripped.starts_with("--") {
                preface_meta_lines.push(line.clone());
                continue;
            }
            started = true;
        }
        sql_lines.push(line.trim_end().to_string());
    }

    let meta = parse_meta(&preface_meta_lines, meta_re)?;
    let sql = sql_lines.join("\n").trim().to_string();
    Ok((meta, sql))
}

fn split_queries(lines: &[String], marker_re: &Regex) -> Vec<Vec<String>> {
    let mut marker_indexes: Vec<usize> = Vec::new();
    for (idx, line) in lines.iter().enumerate() {
        if marker_re.is_match(line.trim()) {
            marker_indexes.push(idx);
        }
    }

    if marker_indexes.len() <= 1 {
        return vec![lines.to_vec()];
    }

    let mut sections: Vec<Vec<String>> = Vec::new();
    for (i, start) in marker_indexes.iter().enumerate() {
        let end = marker_indexes.get(i + 1).copied().unwrap_or(lines.len());
        sections.push(lines[*start..end].to_vec());
    }
    sections
}

fn load_sql_queries_from_file(
    sql_path: &Path,
    meta_re: &Regex,
    marker_re: &Regex,
) -> Result<Vec<QueryCase>> {
    let content = match fs::read_to_string(sql_path) {
        Ok(c) => c,
        Err(exc) => {
            println!(
                "Warning: failed to read SQL file {}: {}",
                sql_path.display(),
                exc
            );
            return Ok(vec![]);
        }
    };

    let base_name = sql_path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow::anyhow!("invalid SQL file name: {}", sql_path.display()))?
        .to_string();

    let lines: Vec<String> = content.lines().map(ToString::to_string).collect();
    let sections = split_queries(&lines, marker_re);
    let (file_meta, _) = extract_meta_and_sql(&lines, meta_re)
        .with_context(|| format!("{}: invalid file-level metadata", sql_path.display()))?;

    let mut cases = Vec::new();
    for (idx, section) in sections.iter().enumerate() {
        let section_id = if idx == 0 {
            base_name.clone()
        } else {
            format!("{}-{}", base_name, idx + 1)
        };

        let (section_meta, sql) = extract_meta_and_sql(section, meta_re).with_context(|| {
            format!("{} ({}): invalid metadata", sql_path.display(), section_id)
        })?;

        if sql.is_empty() {
            continue;
        }

        let merged_meta = merge_meta(&file_meta, &section_meta);
        cases.push(QueryCase {
            source_file: sql_path.to_path_buf(),
            query_id: section_id,
            sql,
            meta: merged_meta,
        });
    }

    Ok(cases)
}

fn load_expected_result(result_path: &Path) -> Option<(Vec<String>, Vec<Vec<String>>)> {
    let content = match fs::read_to_string(result_path) {
        Ok(c) => c,
        Err(exc) => {
            println!(
                "Warning: failed to load expected result from {}: {}",
                result_path.display(),
                exc
            );
            return None;
        }
    };

    // Treat an empty file as a valid empty result set.
    // This matches target output when mysql --batch returns no header for 0-row results.
    if content.trim().is_empty() {
        return Some((vec![], vec![]));
    }

    let lines: Vec<String> = content
        .lines()
        .map(str::trim_end)
        .filter(|line| !line.trim().is_empty())
        .map(ToString::to_string)
        .collect();

    if lines.is_empty() {
        return Some((vec![], vec![]));
    }

    let header = split_row(&lines[0]);
    let rows = lines[1..].iter().map(|line| split_row(line)).collect();
    Some((header, rows))
}

fn write_result_file(path: &Path, header: &[String], rows: &[Vec<String>]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create parent dir failed: {}", parent.display()))?;
    }

    if header.is_empty() && rows.is_empty() {
        fs::write(path, "").with_context(|| format!("write file failed: {}", path.display()))?;
        return Ok(());
    }

    let mut out_lines = Vec::with_capacity(rows.len() + 1);
    out_lines.push(header.join("\t"));
    out_lines.extend(rows.iter().map(|row| row.join("\t")));
    let content = format!("{}\n", out_lines.join("\n"));
    fs::write(path, content).with_context(|| format!("write file failed: {}", path.display()))?;
    Ok(())
}

fn split_row(line: &str) -> Vec<String> {
    line.split('\t').map(ToString::to_string).collect()
}

#[cfg(test)]
mod tests {
    use super::{load_expected_result, write_result_file};
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
        let loaded = load_expected_result(&path).expect("must parse empty result file");
        assert!(loaded.0.is_empty());
        assert!(loaded.1.is_empty());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn write_result_file_persists_empty_result_set() {
        let path = temp_result_path("empty_write");
        write_result_file(&path, &[], &[]).expect("write empty result file");
        let content = fs::read_to_string(&path).expect("read empty result file");
        assert_eq!(content, "");
        let loaded = load_expected_result(&path).expect("must parse empty result file");
        assert!(loaded.0.is_empty());
        assert!(loaded.1.is_empty());
        let _ = fs::remove_file(path);
    }
}

fn parse_output(stdout: &str) -> (Vec<String>, Vec<Vec<String>>) {
    let lines: Vec<&str> = stdout
        .lines()
        .map(str::trim_end)
        .filter(|line| !line.trim().is_empty())
        .collect();

    if lines.is_empty() {
        return (vec![], vec![]);
    }

    let header = split_row(lines[0]);
    let rows = lines[1..].iter().map(|line| split_row(line)).collect();
    (header, rows)
}

fn parse_float(cell: &str) -> Option<f64> {
    let value = cell.parse::<f64>().ok()?;
    if value.is_finite() { Some(value) } else { None }
}

fn cell_equal(expected: &str, actual: &str, epsilon: Option<f64>) -> bool {
    if expected == actual {
        return true;
    }
    let Some(eps) = epsilon else {
        return false;
    };

    let Some(left) = parse_float(expected) else {
        return false;
    };
    let Some(right) = parse_float(actual) else {
        return false;
    };

    (left - right).abs() <= eps
}

fn compare_headers(expected: &[String], actual: &[String]) -> (bool, String) {
    if expected == actual {
        (true, String::new())
    } else {
        (
            false,
            format!(
                "header mismatch (actual={:?}, expected={:?})",
                actual, expected
            ),
        )
    }
}

fn compare_rows_ordered(
    expected_rows: &[Vec<String>],
    actual_rows: &[Vec<String>],
    epsilon: Option<f64>,
) -> (bool, String) {
    if expected_rows.len() != actual_rows.len() {
        return (
            false,
            format!(
                "row count mismatch (actual={}, expected={})",
                actual_rows.len(),
                expected_rows.len()
            ),
        );
    }

    for (row_idx, (expected_row, actual_row)) in
        expected_rows.iter().zip(actual_rows.iter()).enumerate()
    {
        if expected_row.len() != actual_row.len() {
            return (
                false,
                format!(
                    "column count mismatch at row {} (actual={}, expected={})",
                    row_idx,
                    actual_row.len(),
                    expected_row.len()
                ),
            );
        }

        for (col_idx, (expected_cell, actual_cell)) in
            expected_row.iter().zip(actual_row.iter()).enumerate()
        {
            if !cell_equal(expected_cell, actual_cell, epsilon) {
                return (
                    false,
                    format!(
                        "value mismatch at row {}, col {} (actual={}, expected={})",
                        row_idx, col_idx, actual_cell, expected_cell
                    ),
                );
            }
        }
    }

    (true, String::new())
}

fn normalized_cell_for_sort(cell: &str, epsilon: Option<f64>) -> String {
    let Some(eps) = epsilon else {
        return format!("s:{}", cell);
    };

    let Some(value) = parse_float(cell) else {
        return format!("s:{}", cell);
    };

    let bucket = (value / eps).round() as i64;
    format!("f:{}", bucket)
}

fn compare_rows_unordered(
    expected_rows: &[Vec<String>],
    actual_rows: &[Vec<String>],
    epsilon: Option<f64>,
) -> (bool, String) {
    if expected_rows.len() != actual_rows.len() {
        return (
            false,
            format!(
                "row count mismatch (actual={}, expected={})",
                actual_rows.len(),
                expected_rows.len()
            ),
        );
    }

    if epsilon.is_none() {
        let mut expected_counter: HashMap<Vec<String>, usize> = HashMap::new();
        let mut actual_counter: HashMap<Vec<String>, usize> = HashMap::new();

        for row in expected_rows {
            *expected_counter.entry(row.clone()).or_insert(0) += 1;
        }
        for row in actual_rows {
            *actual_counter.entry(row.clone()).or_insert(0) += 1;
        }

        if expected_counter == actual_counter {
            return (true, String::new());
        }

        let mut missing_detail = None;
        for (row, count) in &expected_counter {
            let actual_count = actual_counter.get(row).copied().unwrap_or(0);
            if *count > actual_count {
                missing_detail = Some(format!("missing row x{}: {:?}", count - actual_count, row));
                break;
            }
        }

        let mut extra_detail = None;
        for (row, count) in &actual_counter {
            let expected_count = expected_counter.get(row).copied().unwrap_or(0);
            if *count > expected_count {
                extra_detail = Some(format!(
                    "unexpected row x{}: {:?}",
                    count - expected_count,
                    row
                ));
                break;
            }
        }

        let mut details = Vec::new();
        if let Some(d) = missing_detail {
            details.push(d);
        }
        if let Some(d) = extra_detail {
            details.push(d);
        }

        return (false, details.join("; "));
    }

    let mut expected_sorted = expected_rows.to_vec();
    let mut actual_sorted = actual_rows.to_vec();

    let sort_by_epsilon = |a: &Vec<String>, b: &Vec<String>| -> Ordering {
        let ka: Vec<String> = a
            .iter()
            .map(|cell| normalized_cell_for_sort(cell, epsilon))
            .collect();
        let kb: Vec<String> = b
            .iter()
            .map(|cell| normalized_cell_for_sort(cell, epsilon))
            .collect();
        ka.cmp(&kb)
    };

    expected_sorted.sort_by(sort_by_epsilon);
    actual_sorted.sort_by(sort_by_epsilon);
    compare_rows_ordered(&expected_sorted, &actual_sorted, epsilon)
}

fn compare_result_sets(
    expected_header: &[String],
    expected_rows: &[Vec<String>],
    actual_header: &[String],
    actual_rows: &[Vec<String>],
    order_sensitive: bool,
    epsilon: Option<f64>,
) -> (bool, String) {
    let (ok, msg) = compare_headers(expected_header, actual_header);
    if !ok {
        return (false, msg);
    }

    if order_sensitive {
        compare_rows_ordered(expected_rows, actual_rows, epsilon)
    } else {
        compare_rows_unordered(expected_rows, actual_rows, epsilon)
    }
}

fn expected_result_path(result_dir: &Path, query_id: &str, base_query_id: &str) -> Option<PathBuf> {
    let variant = result_dir.join(format!("{}.result", query_id));
    if variant.exists() {
        return Some(variant);
    }

    if query_id != base_query_id {
        return None;
    }

    let base = result_dir.join(format!("{}.result", base_query_id));
    if base.exists() { Some(base) } else { None }
}

fn target_result_path(result_dir: &Path, query_id: &str) -> PathBuf {
    result_dir.join(format!("{}.result", query_id))
}

fn build_statements(
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

fn run_mysql_sql(conn: &ConnectionConfig, sql: &str, skip_column_names: bool) -> Result<String> {
    let mut cmd = Command::new(&conn.mysql);
    cmd.arg(format!("-h{}", conn.host))
        .arg(format!("-P{}", conn.port))
        .arg(format!("-u{}", conn.user))
        .arg("--batch")
        .arg("--raw")
        .arg("--default-character-set=utf8mb4");
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

const DEFAULT_TEST_CATALOG: &str = "sql_test_catalog";

fn catalog_exists(conn: &ConnectionConfig, catalog_name: &str) -> Result<bool> {
    let mut check_conn = conn.clone();
    check_conn.catalog = None;
    check_conn.db = None;
    let output = run_mysql_sql(&check_conn, "SHOW CATALOGS;", false)?;
    let (_, rows) = parse_output(&output);
    Ok(rows
        .iter()
        .any(|row| row.first().is_some_and(|name| name == catalog_name)))
}

fn create_target_catalog_if_missing(
    conn: &ConnectionConfig,
    base_dir: &Path,
    catalog_name: &str,
) -> Result<()> {
    if catalog_exists(conn, catalog_name)? {
        return Ok(());
    }
    if catalog_name != DEFAULT_TEST_CATALOG {
        bail!(
            "target catalog '{}' does not exist; only '{}' can be auto-created",
            catalog_name,
            DEFAULT_TEST_CATALOG
        );
    }

    let warehouse_path = base_dir.join("sql-tests").join(".sql_test_catalog");
    fs::create_dir_all(&warehouse_path).with_context(|| {
        format!(
            "failed to create warehouse dir: {}",
            warehouse_path.display()
        )
    })?;
    let warehouse_uri = format!("file://{}", warehouse_path.display());
    let create_sql = format!(
        "CREATE EXTERNAL CATALOG IF NOT EXISTS `{}` PROPERTIES (\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"hadoop\",\"iceberg.catalog.warehouse\"=\"{}\");",
        catalog_name, warehouse_uri
    );
    run_mysql_sql(conn, &create_sql, true)?;
    Ok(())
}

fn ensure_reference_catalog_exists(conn: &ConnectionConfig, catalog_name: &str) -> Result<()> {
    if catalog_exists(conn, catalog_name)? {
        return Ok(());
    }
    bail!(
        "reference catalog '{}' does not exist; please create it on the reference StarRocks cluster and rerun",
        catalog_name
    );
}

fn execute_query(
    conn: &ConnectionConfig,
    query_timeout: u64,
    sql: &str,
) -> (bool, Option<QueryExecution>, String) {
    let started = Instant::now();
    let full_sql = build_statements(
        sql,
        query_timeout,
        conn.catalog.as_deref(),
        conn.db.as_deref(),
    );

    let output = match Command::new(&conn.mysql)
        .arg(format!("-h{}", conn.host))
        .arg(format!("-P{}", conn.port))
        .arg(format!("-u{}", conn.user))
        .arg("--batch")
        .arg("--raw")
        .arg("--default-character-set=utf8mb4")
        .arg("-e")
        .arg(full_sql)
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
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let message = if !stderr.is_empty() { stderr } else { stdout };
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

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let (header, rows) = parse_output(&stdout);
    let execution = QueryExecution {
        header,
        rows,
        elapsed,
    };
    (true, Some(execution), String::new())
}

fn parse_query_list(value: Option<&str>) -> HashSet<String> {
    value
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn suite_sql_glob(suite_name: &str) -> String {
    if suite_name == "tpc-h" || suite_name == "tpc-ds" {
        "q*.sql".to_string()
    } else {
        "*.sql".to_string()
    }
}

fn suite_default_db(suite_name: &str) -> String {
    match suite_name {
        "ssb" => "ssb".to_string(),
        "tpc-h" => "tpch".to_string(),
        "tpc-ds" => "tpcds".to_string(),
        _ => String::new(),
    }
}

fn build_suite_configs(base_dir: &Path) -> Result<BTreeMap<String, SuiteConfig>> {
    let sql_tests_dir = base_dir.join("sql-tests");
    let entries = fs::read_dir(&sql_tests_dir)
        .with_context(|| format!("failed to read {}", sql_tests_dir.display()))?;

    let mut suite_configs: BTreeMap<String, SuiteConfig> = BTreeMap::new();
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with('_') || name == "rust" {
            continue;
        }

        let sql_dir = path.join("sql");
        if !sql_dir.exists() || !sql_dir.is_dir() {
            continue;
        }

        let config = SuiteConfig {
            name: name.clone(),
            sql_dir,
            result_dir: Some(path.join("result")),
            sql_glob: suite_sql_glob(&name),
            default_db: suite_default_db(&name),
            verify_default: true,
        };
        suite_configs.insert(name, config);
    }

    Ok(suite_configs)
}

fn wildcard_match(name: &str, pattern: &str) -> bool {
    if pattern == "*.sql" {
        return name.ends_with(".sql");
    }
    if pattern == "q*.sql" {
        return name.starts_with('q') && name.ends_with(".sql");
    }

    let escaped = regex::escape(pattern)
        .replace("\\*", ".*")
        .replace("\\?", ".");
    let expr = format!("^{}$", escaped);
    Regex::new(&expr)
        .map(|re| re.is_match(name))
        .unwrap_or(false)
}

fn list_sql_files(sql_dir: &Path, pattern: &str) -> Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = Vec::new();
    for entry in
        fs::read_dir(sql_dir).with_context(|| format!("read dir failed: {}", sql_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if wildcard_match(name, pattern) {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn summarize_connection(label: &str, conn: &ConnectionConfig) -> String {
    let catalog = conn.catalog.as_deref().unwrap_or("");
    let db = conn.db.as_deref().unwrap_or("");
    format!(
        "{}: mysql={}, host={}:{}, user={}, catalog={}, db={}",
        label, conn.mysql, conn.host, conn.port, conn.user, catalog, db
    )
}

fn query_order_sensitive(case: &QueryCase, cli: &Cli) -> bool {
    case.meta
        .order_sensitive
        .unwrap_or(cli.order_sensitive_default)
}

fn query_float_epsilon(case: &QueryCase, cli: &Cli) -> Option<f64> {
    case.meta.float_epsilon.or(cli.float_epsilon)
}

fn query_db(case: &QueryCase, fallback: Option<&str>) -> Option<String> {
    case.meta
        .db
        .clone()
        .or_else(|| fallback.map(ToString::to_string))
}

fn write_mismatch_artifacts(
    root_dir: &Path,
    suite_name: &str,
    query_id: &str,
    expected_header: &[String],
    expected_rows: &[Vec<String>],
    actual_header: &[String],
    actual_rows: &[Vec<String>],
    reason: &str,
) -> Result<()> {
    let out_dir = root_dir.join(suite_name).join(query_id);
    fs::create_dir_all(&out_dir)
        .with_context(|| format!("create dir failed: {}", out_dir.display()))?;

    write_result_file(
        &out_dir.join("expected.tsv"),
        expected_header,
        expected_rows,
    )?;
    write_result_file(&out_dir.join("actual.tsv"), actual_header, actual_rows)?;
    fs::write(out_dir.join("diff.txt"), format!("{}\n", reason))
        .with_context(|| format!("write diff failed: {}", out_dir.display()))?;
    Ok(())
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

fn main() -> Result<()> {
    let cli = Cli::parse();
    let base_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(Path::to_path_buf)
        .context("failed to resolve repo root from sql-tests manifest path")?;

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

    let target_db_default = cli.db.clone().unwrap_or_else(|| suite.default_db.clone());
    let ref_db_default = cli
        .ref_db
        .clone()
        .unwrap_or_else(|| suite.default_db.clone());

    let verify_enabled = verify_override(&cli).unwrap_or(suite.verify_default);
    let query_timeout = cli.query_timeout.unwrap_or_else(|| {
        env_or_default("STARUST_TEST_TIMEOUT", "120")
            .parse()
            .unwrap_or(120)
    });
    let reference_required = cli.mode == Mode::Diff
        || (cli.mode == Mode::Record && cli.record_from == RecordFrom::Reference);
    let target_port = resolve_target_port(cli.port.as_deref(), &base_dir)?;
    let reference_port =
        resolve_reference_port(cli.ref_port.as_deref(), &target_port, reference_required)?;
    let target_catalog_name = cli
        .catalog
        .clone()
        .or_else(|| env_optional("STARUST_TEST_CATALOG"))
        .unwrap_or_else(|| DEFAULT_TEST_CATALOG.to_string());
    let reference_catalog_name = cli
        .ref_catalog
        .clone()
        .or_else(|| env_optional("STARUST_REF_CATALOG"))
        .unwrap_or_else(|| DEFAULT_TEST_CATALOG.to_string());

    let target_conn_base = ConnectionConfig {
        mysql: cli.mysql.clone().unwrap_or_else(|| {
            env_or_default(
                "STARUST_TEST_MYSQL",
                "mysql",
            )
        }),
        host: cli
            .host
            .clone()
            .unwrap_or_else(|| env_or_default("STARUST_TEST_HOST", "127.0.0.1")),
        port: target_port,
        user: cli
            .user
            .clone()
            .unwrap_or_else(|| env_or_default("STARUST_TEST_USER", "root")),
        catalog: Some(target_catalog_name.clone()),
        db: if target_db_default.is_empty() {
            None
        } else {
            Some(target_db_default)
        },
    };

    create_target_catalog_if_missing(&target_conn_base, &base_dir, &target_catalog_name)
        .with_context(|| {
            format!(
                "failed to ensure target catalog '{}' on {}:{}",
                target_catalog_name, target_conn_base.host, target_conn_base.port
            )
        })?;

    let reference_conn_base = ConnectionConfig {
        mysql: cli.ref_mysql.clone().unwrap_or_else(|| {
            env_or_default(
                "STARUST_REF_MYSQL",
                "mysql",
            )
        }),
        host: cli
            .ref_host
            .clone()
            .unwrap_or_else(|| env_or_default("STARUST_REF_HOST", "127.0.0.1")),
        port: reference_port,
        user: cli
            .ref_user
            .clone()
            .unwrap_or_else(|| env_or_default("STARUST_REF_USER", "root")),
        catalog: Some(reference_catalog_name.clone()),
        db: if ref_db_default.is_empty() {
            None
        } else {
            Some(ref_db_default)
        },
    };

    if cli.mode == Mode::Diff
        || (cli.mode == Mode::Record && cli.record_from == RecordFrom::Reference)
    {
        ensure_reference_catalog_exists(&reference_conn_base, &reference_catalog_name)
            .with_context(|| {
                format!(
                    "failed to ensure reference catalog '{}' on {}:{}",
                    reference_catalog_name, reference_conn_base.host, reference_conn_base.port
                )
            })?;
    }

    println!("{}", "=".repeat(72));
    println!("📋 {} correctness runner", suite.name.to_uppercase());
    println!("{}", "=".repeat(72));
    println!("mode={}", mode_name(cli.mode));
    println!("sql_dir={}", sql_dir.display());
    println!("sql_glob={}", sql_glob);
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

    let only_set = parse_query_list(cli.only.as_deref());
    let skip_set = parse_query_list(cli.skip.as_deref());

    let meta_re = Regex::new(r"^--\s*@([a-zA-Z0-9_]+)\s*=\s*(.+?)\s*$")?;
    let marker_re = Regex::new(r"(?i)^--\s*query\s+\d+(?:\s+.*)?$")?;

    let mut cases: Vec<QueryCase> = Vec::new();
    let mut failed_query_ids: Vec<String> = Vec::new();

    for sql_file in sql_files {
        let loaded_cases = match load_sql_queries_from_file(&sql_file, &meta_re, &marker_re) {
            Ok(c) => c,
            Err(exc) => {
                println!("❌ ERROR: {}", exc);
                std::process::exit(1);
            }
        };

        if loaded_cases.is_empty() {
            if let Some(stem) = sql_file.file_stem().and_then(|s| s.to_str()) {
                failed_query_ids.push(stem.to_string());
            }
            continue;
        }

        for case in loaded_cases {
            let base_id = case
                .source_file
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_default()
                .to_string();

            if !only_set.is_empty()
                && !only_set.contains(&case.query_id)
                && !only_set.contains(&base_id)
            {
                continue;
            }
            if skip_set.contains(&case.query_id) || skip_set.contains(&base_id) {
                continue;
            }

            cases.push(case);
        }
    }

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
        println!("selected queries:");
        for case in &cases {
            let file_name = case
                .source_file
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or_default();
            println!("  {} ({})", case.query_id, file_name);
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

    let actual_artifact_dir = resolve_path(cli.write_actual_dir.as_deref(), &base_dir);

    let total = cases.len();
    let mut passed = 0usize;
    let mut failed = 0usize;
    let mut total_time = Duration::from_secs(0);
    let mut per_query_times: Vec<(String, Duration, bool)> = Vec::new();

    for (idx, case) in cases.iter().enumerate() {
        let order_sensitive = query_order_sensitive(case, &cli);
        let epsilon = query_float_epsilon(case, &cli);

        let target_conn = ConnectionConfig {
            mysql: target_conn_base.mysql.clone(),
            host: target_conn_base.host.clone(),
            port: target_conn_base.port.clone(),
            user: target_conn_base.user.clone(),
            catalog: target_conn_base.catalog.clone(),
            db: query_db(case, target_conn_base.db.as_deref()),
        };

        let reference_conn = ConnectionConfig {
            mysql: reference_conn_base.mysql.clone(),
            host: reference_conn_base.host.clone(),
            port: reference_conn_base.port.clone(),
            user: reference_conn_base.user.clone(),
            catalog: reference_conn_base.catalog.clone(),
            db: query_db(case, reference_conn_base.db.as_deref()),
        };

        println!(
            "\n[{}/{}] {} (order_sensitive={}, epsilon={:?})",
            idx + 1,
            total,
            case.query_id,
            order_sensitive,
            epsilon
        );

        match cli.mode {
            Mode::Verify => {
                let (ok, execution, err_msg) =
                    execute_query(&target_conn, query_timeout, &case.sql);
                if !ok || execution.is_none() {
                    failed += 1;
                    failed_query_ids.push(case.query_id.clone());
                    per_query_times.push((case.query_id.clone(), Duration::from_secs(0), false));
                    println!("    ❌ target execute failed: {}", err_msg);
                    if cli.fail_fast {
                        break;
                    }
                    continue;
                }

                let execution = execution.expect("checked above");
                per_query_times.push((case.query_id.clone(), execution.elapsed, true));
                total_time += execution.elapsed;

                if !verify_enabled {
                    passed += 1;
                    println!(
                        "    ✅ PASS (verify disabled) ({:.2}s)",
                        execution.elapsed.as_secs_f64()
                    );
                    continue;
                }

                let Some(result_dir) = &result_dir else {
                    println!("    ❌ missing result_dir in verify mode");
                    failed += 1;
                    failed_query_ids.push(case.query_id.clone());
                    if cli.fail_fast {
                        break;
                    }
                    continue;
                };

                let base_query_id = case
                    .source_file
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or_default();
                let expected_path = expected_result_path(result_dir, &case.query_id, base_query_id);
                let Some(expected_path) = expected_path else {
                    failed += 1;
                    failed_query_ids.push(case.query_id.clone());
                    println!("    ❌ missing expected result file");
                    if cli.fail_fast {
                        break;
                    }
                    continue;
                };

                let expected = load_expected_result(&expected_path);
                let Some((expected_header, expected_rows)) = expected else {
                    failed += 1;
                    failed_query_ids.push(case.query_id.clone());
                    println!(
                        "    ❌ failed to load expected result: {}",
                        expected_path.display()
                    );
                    if cli.fail_fast {
                        break;
                    }
                    continue;
                };

                let (same, reason) = compare_result_sets(
                    &expected_header,
                    &expected_rows,
                    &execution.header,
                    &execution.rows,
                    order_sensitive,
                    epsilon,
                );

                if same {
                    passed += 1;
                    println!(
                        "    ✅ PASS ({:.2}s, rows={})",
                        execution.elapsed.as_secs_f64(),
                        execution.rows.len()
                    );
                    for row in execution.rows.iter().take(cli.preview_lines) {
                        println!("    {:?}", row);
                    }
                } else {
                    failed += 1;
                    failed_query_ids.push(case.query_id.clone());
                    println!("    ❌ VERIFY FAILED: {}", reason);
                    if let Some(root) = &actual_artifact_dir {
                        if let Err(exc) = write_mismatch_artifacts(
                            root,
                            &suite.name,
                            &case.query_id,
                            &expected_header,
                            &expected_rows,
                            &execution.header,
                            &execution.rows,
                            &reason,
                        ) {
                            println!("    ⚠️ failed to write mismatch artifacts: {}", exc);
                        }
                    }
                    if cli.fail_fast {
                        break;
                    }
                }
            }
            Mode::Record => {
                let record_conn = if cli.record_from == RecordFrom::Target {
                    &target_conn
                } else {
                    &reference_conn
                };

                let (ok, execution, err_msg) = execute_query(record_conn, query_timeout, &case.sql);
                if !ok || execution.is_none() {
                    failed += 1;
                    failed_query_ids.push(case.query_id.clone());
                    per_query_times.push((case.query_id.clone(), Duration::from_secs(0), false));
                    println!("    ❌ record source execute failed: {}", err_msg);
                    if cli.fail_fast {
                        break;
                    }
                    continue;
                }

                let execution = execution.expect("checked above");
                per_query_times.push((case.query_id.clone(), execution.elapsed, true));
                total_time += execution.elapsed;

                let Some(result_dir) = &result_dir else {
                    failed += 1;
                    failed_query_ids.push(case.query_id.clone());
                    println!("    ❌ missing result_dir in record mode");
                    if cli.fail_fast {
                        break;
                    }
                    continue;
                };

                let out_path = target_result_path(result_dir, &case.query_id);
                if out_path.exists() && !cli.update_expected {
                    failed += 1;
                    failed_query_ids.push(case.query_id.clone());
                    println!(
                        "    ❌ expected file exists ({}); rerun with --update-expected",
                        out_path.display()
                    );
                    if cli.fail_fast {
                        break;
                    }
                    continue;
                }

                if let Err(exc) = write_result_file(&out_path, &execution.header, &execution.rows) {
                    failed += 1;
                    failed_query_ids.push(case.query_id.clone());
                    println!("    ❌ failed to write expected result: {}", exc);
                    if cli.fail_fast {
                        break;
                    }
                    continue;
                }

                passed += 1;
                println!(
                    "    ✅ RECORDED ({:.2}s, rows={}) -> {}",
                    execution.elapsed.as_secs_f64(),
                    execution.rows.len(),
                    out_path.display()
                );
            }
            Mode::Diff => {
                let (ok_t, execution_t, err_t) =
                    execute_query(&target_conn, query_timeout, &case.sql);
                if !ok_t || execution_t.is_none() {
                    failed += 1;
                    failed_query_ids.push(case.query_id.clone());
                    per_query_times.push((case.query_id.clone(), Duration::from_secs(0), false));
                    println!("    ❌ target execute failed: {}", err_t);
                    if cli.fail_fast {
                        break;
                    }
                    continue;
                }
                let execution_t = execution_t.expect("checked above");

                let (ok_r, execution_r, err_r) =
                    execute_query(&reference_conn, query_timeout, &case.sql);
                if !ok_r || execution_r.is_none() {
                    failed += 1;
                    failed_query_ids.push(case.query_id.clone());
                    per_query_times.push((case.query_id.clone(), execution_t.elapsed, false));
                    total_time += execution_t.elapsed;
                    println!("    ❌ reference execute failed: {}", err_r);
                    if cli.fail_fast {
                        break;
                    }
                    continue;
                }
                let execution_r = execution_r.expect("checked above");

                let elapsed = execution_t.elapsed + execution_r.elapsed;
                total_time += elapsed;
                per_query_times.push((case.query_id.clone(), elapsed, true));

                let (same, reason) = compare_result_sets(
                    &execution_r.header,
                    &execution_r.rows,
                    &execution_t.header,
                    &execution_t.rows,
                    order_sensitive,
                    epsilon,
                );

                if same {
                    passed += 1;
                    println!(
                        "    ✅ DIFF PASS (target={:.2}s, reference={:.2}s)",
                        execution_t.elapsed.as_secs_f64(),
                        execution_r.elapsed.as_secs_f64()
                    );
                } else {
                    failed += 1;
                    failed_query_ids.push(case.query_id.clone());
                    println!("    ❌ DIFF FAILED: {}", reason);
                    if let Some(root) = &actual_artifact_dir {
                        if let Err(exc) = write_mismatch_artifacts(
                            root,
                            &suite.name,
                            &case.query_id,
                            &execution_r.header,
                            &execution_r.rows,
                            &execution_t.header,
                            &execution_t.rows,
                            &reason,
                        ) {
                            println!("    ⚠️ failed to write mismatch artifacts: {}", exc);
                        }
                    }
                    if cli.fail_fast {
                        break;
                    }
                }
            }
        }

        println!(
            "    progress: pass={}, fail={}, elapsed={:.2}s",
            passed,
            failed,
            total_time.as_secs_f64()
        );
    }

    println!("\n{}", "=".repeat(72));
    println!("summary ({}, mode={})", suite.name, mode_name(cli.mode));
    println!("{}", "=".repeat(72));
    println!("total={}", total);
    println!("pass={}", passed);
    println!("fail={}", failed);
    println!("elapsed={:.2}s", total_time.as_secs_f64());

    per_query_times.sort_by(|a, b| b.1.cmp(&a.1));
    println!("\nslowest queries (top 5):");
    for (query_id, elapsed, _) in per_query_times.iter().take(5) {
        println!("  {}: {:.2}s", query_id, elapsed.as_secs_f64());
    }

    if !failed_query_ids.is_empty() {
        println!("\nfailed queries:");
        for id in &failed_query_ids {
            println!("  {}", id);
        }
    }
    println!("{}", "=".repeat(72));

    if failed > 0 {
        std::process::exit(1);
    }

    Ok(())
}
