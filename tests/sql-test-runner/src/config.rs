use crate::types::*;
use anyhow::{Context, Result, bail};
use regex::Regex;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

pub fn env_or_default(key: &str, default: &str) -> String {
    env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_string())
}

pub fn env_optional(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

pub fn strip_optional_quotes(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.len() >= 2 {
        let quoted = (trimmed.starts_with('"') && trimmed.ends_with('"'))
            || (trimmed.starts_with('\'') && trimmed.ends_with('\''));
        if quoted {
            return trimmed[1..trimmed.len() - 1].to_string();
        }
    }
    trimmed.to_string()
}

pub fn detect_default_config(base_dir: &Path) -> Option<PathBuf> {
    let sr_conf = base_dir
        .join("tests")
        .join("sql-test-runner")
        .join("conf")
        .join("sr.conf");
    sr_conf.exists().then_some(sr_conf)
}

pub fn resolve_config_path(cli_path: Option<&str>, base_dir: &Path) -> Option<PathBuf> {
    if let Some(path) = resolve_path(cli_path, base_dir) {
        return Some(path);
    }
    if let Some(raw) = env_optional("STARUST_TEST_CONFIG") {
        return resolve_path(Some(&raw), base_dir).or_else(|| Some(PathBuf::from(raw)));
    }
    detect_default_config(base_dir)
}

pub fn load_runner_config(path: Option<&Path>) -> Result<RunnerConfig> {
    let Some(path) = path else {
        return Ok(RunnerConfig::default());
    };

    let content =
        fs::read_to_string(path).with_context(|| format!("read failed: {}", path.display()))?;
    let mut config = RunnerConfig {
        path: Some(path.to_path_buf()),
        ..RunnerConfig::default()
    };

    let mut current_section: Option<String> = None;
    let mut current_scope: Option<String> = None;
    for (line_no, line) in content.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with(';') {
            continue;
        }

        if trimmed.starts_with('[') && trimmed.ends_with(']') {
            let section_name = trimmed[1..trimmed.len() - 1].trim();
            if let Some(stripped) = section_name.strip_prefix('.') {
                let Some(parent) = current_section.as_deref() else {
                    bail!(
                        "{}:{} subsection [{}] missing parent section",
                        path.display(),
                        line_no + 1,
                        section_name
                    );
                };
                current_scope = Some(format!("{}.{}", parent, stripped));
            } else {
                current_section = Some(section_name.to_string());
                current_scope = Some(section_name.to_string());
            }
            continue;
        }

        let Some((key, raw_value)) = trimmed.split_once('=') else {
            continue;
        };
        let key = key.trim().to_string();
        if key.is_empty() {
            continue;
        }

        let value = strip_optional_quotes(raw_value);
        if let Some(scope) = current_scope.as_deref() {
            config
                .values
                .insert(format!("{}.{}", scope, key), value.clone());
        }
        if current_section.as_deref() == Some("env") {
            config.values.insert(key.clone(), value.clone());
        }
        if current_section.as_deref() == Some("cluster") {
            config.cluster.insert(key, value);
        }
    }

    Ok(config)
}

pub(crate) fn insert_placeholder_default(
    variables: &mut HashMap<String, String>,
    key: &str,
    value: impl Into<String>,
) {
    let should_insert = variables
        .get(key)
        .map(|existing| existing.trim().is_empty())
        .unwrap_or(true);
    if should_insert {
        variables.insert(key.to_string(), value.into());
    }
}

pub fn apply_suite_placeholder_defaults(variables: &mut HashMap<String, String>, suite_name: &str) {
    if suite_name != "iceberg" && suite_name != "mv-on-iceberg" {
        return;
    }

    // Keep local suites that exercise Iceberg catalogs aligned with bootstrap
    // defaults so they run out of the box against the MinIO-backed dev setup.
    insert_placeholder_default(variables, "iceberg_catalog_type", "hadoop");
    insert_placeholder_default(
        variables,
        "iceberg_catalog_warehouse",
        env_or_default("CATALOG_WAREHOUSE_URI", "s3://novarocks/iceberg-catalog"),
    );
    insert_placeholder_default(
        variables,
        "oss_ak",
        env_or_default("MINIO_ROOT_USER", "admin"),
    );
    insert_placeholder_default(
        variables,
        "oss_sk",
        env_or_default("MINIO_ROOT_PASSWORD", "admin123"),
    );
    insert_placeholder_default(
        variables,
        "oss_endpoint",
        env_or_default("AWS_S3_ENDPOINT", "http://127.0.0.1:9000"),
    );
}

pub fn placeholder_variables(
    runner_config: &RunnerConfig,
    suite_name: &str,
) -> HashMap<String, String> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let run_id = format!("sqlt_{:x}_{}", nanos, std::process::id());

    let mut variables = runner_config.values.clone();
    apply_suite_placeholder_defaults(&mut variables, suite_name);
    variables.insert("run_id".to_string(), run_id.clone());
    variables.insert("suite".to_string(), suite_name.to_string());
    for idx in 0..10 {
        variables.insert(format!("uuid{}", idx), format!("{}_{}", run_id, idx));
    }
    variables
}

pub fn stable_hash_hex(input: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    input.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

pub fn case_auto_db_name(case_id: &str) -> String {
    format!("db_sqlt_{}", stable_hash_hex(case_id))
}

pub fn case_auto_db_name_n(case_id: &str, n: usize) -> String {
    if n == 0 {
        case_auto_db_name(case_id)
    } else {
        format!("db_sqlt_{}_{}", stable_hash_hex(case_id), n + 1)
    }
}

pub fn case_placeholder_variables(
    base_variables: &HashMap<String, String>,
    case_id: &str,
) -> HashMap<String, String> {
    let mut variables = base_variables.clone();
    let suite_run_id = base_variables
        .get("run_id")
        .cloned()
        .unwrap_or_else(|| "sqlt".to_string());
    let case_run_id = format!("{}_{}", suite_run_id, stable_hash_hex(case_id));
    variables.insert("case_id".to_string(), case_id.to_string());
    variables.insert("run_id".to_string(), case_run_id.clone());
    for idx in 0..10 {
        variables.insert(format!("uuid{}", idx), format!("{}_{}", case_run_id, idx));
    }
    // Per-case database placeholders for parallel isolation.
    let primary_db = case_auto_db_name(case_id);
    variables.insert("case_db".to_string(), primary_db);
    for idx in 2..=9 {
        variables.insert(
            format!("case_db_{}", idx),
            case_auto_db_name_n(case_id, idx - 1),
        );
    }
    variables
}

pub fn substitute_placeholders(
    raw: &str,
    variables: &HashMap<String, String>,
    context: &str,
) -> Result<String> {
    let placeholder_re =
        Regex::new(r"\$\{([A-Za-z0-9_.-]+)\}").context("failed to compile placeholder regex")?;
    let mut substituted = String::with_capacity(raw.len());
    let mut last = 0usize;
    for captures in placeholder_re.captures_iter(raw) {
        let matched = captures.get(0).expect("placeholder match");
        let key = captures.get(1).expect("placeholder key").as_str();
        substituted.push_str(&raw[last..matched.start()]);
        let Some(value) = variables.get(key) else {
            bail!("{}: missing placeholder variable '{}'", context, key);
        };
        substituted.push_str(value);
        last = matched.end();
    }
    substituted.push_str(&raw[last..]);
    Ok(substituted)
}

pub fn resolve_target_port(cli_port: Option<&str>, runner_config: &RunnerConfig) -> Result<String> {
    if let Some(port) = cli_port.filter(|v| !v.trim().is_empty()) {
        return Ok(port.trim().to_string());
    }
    if let Some(port) = env_optional("STARUST_TEST_PORT") {
        return Ok(port);
    }
    if let Some(port) = runner_config
        .cluster
        .get("port")
        .filter(|v| !v.trim().is_empty())
    {
        return Ok(port.trim().to_string());
    }
    bail!(
        "target port is not set; provide --port or STARUST_TEST_PORT, or configure tests/sql-test-runner/conf/sr.conf with [cluster].port"
    );
}

pub fn resolve_repo_root() -> Result<PathBuf> {
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    loop {
        if dir.join("sql-tests").is_dir() && dir.join("Cargo.toml").is_file() {
            return Ok(dir);
        }
        if !dir.pop() {
            break;
        }
    }
    bail!(
        "failed to resolve repo root from manifest directory {}",
        env!("CARGO_MANIFEST_DIR")
    )
}

pub fn resolve_reference_port(
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

pub fn resolve_path(path_value: Option<&str>, base_dir: &Path) -> Option<PathBuf> {
    let raw = path_value?;
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        Some(path)
    } else {
        Some(base_dir.join(path))
    }
}

pub fn parse_bool(raw: &str) -> Result<bool> {
    let lowered = raw.trim().to_lowercase();
    match lowered.as_str() {
        "1" | "true" | "yes" | "y" | "on" => Ok(true),
        "0" | "false" | "no" | "n" | "off" => Ok(false),
        _ => bail!("invalid boolean value: {}", raw),
    }
}

pub fn suite_sql_glob(suite_name: &str) -> String {
    if suite_name == "tpc-h" || suite_name == "tpc-ds" {
        "q*.sql".to_string()
    } else {
        "*.sql".to_string()
    }
}

pub fn suite_default_db(suite_name: &str) -> String {
    match suite_name {
        "ssb" => "ssb".to_string(),
        "tpc-h" => "tpch".to_string(),
        "tpc-ds" => "tpcds".to_string(),
        _ => String::new(),
    }
}

pub fn suite_default_catalog(suite_name: &str) -> String {
    let _ = suite_name;
    "default_catalog".to_string()
}

pub fn suite_auto_case_db(suite_name: &str) -> bool {
    matches!(suite_name, "materialized-view")
}

pub fn suite_default_query_timeout(suite_name: &str) -> u64 {
    match suite_name {
        "materialized-view" => 300,
        _ => 120,
    }
}

pub fn build_suite_configs(base_dir: &Path) -> Result<BTreeMap<String, SuiteConfig>> {
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
            default_catalog: suite_default_catalog(&name),
            default_db: suite_default_db(&name),
            auto_case_db: suite_auto_case_db(&name),
            verify_default: true,
            init_sql: path
                .join("init.sql")
                .exists()
                .then(|| path.join("init.sql")),
            cleanup_sql: path
                .join("cleanup.sql")
                .exists()
                .then(|| path.join("cleanup.sql")),
        };
        suite_configs.insert(name, config);
    }

    Ok(suite_configs)
}

pub fn wildcard_match(name: &str, pattern: &str) -> bool {
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

pub fn list_sql_files(sql_dir: &Path, pattern: &str) -> Result<Vec<PathBuf>> {
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
