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

use crate::suite_manifest::SuiteManifest;
use crate::types::*;
use anyhow::{Context, Result, bail};
use regex::Regex;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestLane {
    Correctness,
    Benchmark,
}

impl TestLane {
    pub fn suite_root(self, base_dir: &Path) -> PathBuf {
        let lane = match self {
            Self::Correctness => "correctness",
            Self::Benchmark => "benchmarks",
        };
        base_dir.join("tests").join("sql").join(lane)
    }
}

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

pub fn detect_default_config(base_dir: &Path) -> Option<PathBuf> {
    let default_config = base_dir
        .join("tests")
        .join("sql")
        .join("runner")
        .join("conf")
        .join("default.toml");
    default_config.exists().then_some(default_config)
}

pub fn resolve_config_path(cli_path: Option<&str>, base_dir: &Path) -> Option<PathBuf> {
    if let Some(path) = resolve_path(cli_path, base_dir) {
        return Some(path);
    }
    if let Some(raw) = env_optional("NOVAROCKS_SQL_TEST_CONFIG") {
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
    let document: toml::Table = toml::from_str(&content)
        .with_context(|| format!("invalid TOML configuration: {}", path.display()))?;
    let mut config = RunnerConfig {
        path: Some(path.to_path_buf()),
        ..RunnerConfig::default()
    };

    if let Some(cluster) = document.get("cluster").and_then(toml::Value::as_table) {
        for (key, value) in cluster {
            config
                .cluster
                .insert(key.clone(), toml_value_to_string(value));
        }
    }
    if let Some(environment) = document.get("env").and_then(toml::Value::as_table) {
        flatten_environment_values(environment, "env", &mut config.values);
    }

    Ok(config)
}

fn toml_value_to_string(value: &toml::Value) -> String {
    value
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| value.to_string())
}

fn flatten_environment_values(
    table: &toml::Table,
    scope: &str,
    values: &mut HashMap<String, String>,
) {
    for (key, value) in table {
        let scoped_key = format!("{scope}.{key}");
        if let Some(child) = value.as_table() {
            flatten_environment_values(child, &scoped_key, values);
            continue;
        }

        let value = toml_value_to_string(value);
        values.insert(scoped_key, value.clone());
        values.insert(key.clone(), value);
    }
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
    match suite_name {
        "iceberg"
        | "iceberg-ddl"
        | "iceberg-dml"
        | "iceberg-ivm"
        | "iceberg-mv-apply"
        | "iceberg-mv-scheduler"
        | "materialized-view"
        | "mv-rewrite" => {
            // Keep local suites that exercise Iceberg catalogs aligned with bootstrap
            // defaults so they run out of the box against the MinIO-backed dev setup.
            insert_placeholder_default(variables, "iceberg_catalog_type", "hadoop");
            insert_placeholder_default(
                variables,
                "iceberg_catalog_warehouse",
                env_or_default("CATALOG_WAREHOUSE_URI", "s3://novarocks/iceberg-catalog"),
            );
            let rest_warehouse_default = env_or_default(
                "NOVA_ENV_REST_WAREHOUSE_URI",
                "s3://warehouse/novarocks-sql-test-rest",
            );
            insert_placeholder_default(
                variables,
                "iceberg_rest_uri",
                env_or_default("NOVAROCKS_ICEBERG_REST_URI", "http://127.0.0.1:8181"),
            );
            insert_placeholder_default(
                variables,
                "iceberg_rest_warehouse",
                env_or_default("NOVAROCKS_ICEBERG_REST_WAREHOUSE", &rest_warehouse_default),
            );
        }
        "iceberg-compatibility" | "statistics" => {
            let rest_warehouse_default = env_or_default(
                "NOVA_ENV_REST_WAREHOUSE_URI",
                "s3://warehouse/novarocks-sql-test-rest",
            );
            insert_placeholder_default(
                variables,
                "iceberg_rest_uri",
                env_or_default("NOVAROCKS_ICEBERG_REST_URI", "http://127.0.0.1:8181"),
            );
            insert_placeholder_default(
                variables,
                "iceberg_rest_warehouse",
                env_or_default("NOVAROCKS_ICEBERG_REST_WAREHOUSE", &rest_warehouse_default),
            );
        }
        "iceberg-hms" | "iceberg-hms-compatibility" => {
            insert_placeholder_default(
                variables,
                "iceberg_hms_uris",
                env_or_default("NOVAROCKS_ICEBERG_HMS_URI", "thrift://127.0.0.1:9083"),
            );
            insert_placeholder_default(
                variables,
                "iceberg_hms_warehouse",
                env_or_default(
                    "NOVA_ENV_SHARED_HMS_WAREHOUSE_URI",
                    "s3://warehouse/shared/hms",
                ),
            );
        }
        _ => return,
    }

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

pub fn placeholder_variables_with_run_id(
    runner_config: &RunnerConfig,
    suite_name: &str,
    run_id_override: Option<&str>,
) -> HashMap<String, String> {
    let generated_run_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let run_id = run_id_override
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("sqlt_{generated_run_id:x}_{}", std::process::id()));

    let mut variables = runner_config.values.clone();
    apply_suite_placeholder_defaults(&mut variables, suite_name);
    variables.insert("run_id".to_string(), run_id.clone());
    variables.insert("suite_run_id".to_string(), run_id.clone());
    variables.insert("suite".to_string(), suite_name.to_string());
    for idx in 0..10 {
        let value = format!("{}_{}", run_id, idx);
        variables.insert(format!("uuid{}", idx), value.clone());
        variables.insert(format!("suite_uuid{}", idx), value);
    }
    substitute_known_variable_values(&mut variables);
    variables
}

fn substitute_known_variable_values(variables: &mut HashMap<String, String>) {
    let Ok(placeholder_re) = Regex::new(r"\$\{([A-Za-z0-9_.-]+)\}") else {
        return;
    };
    let snapshot = variables.clone();
    for (current_key, value) in variables.iter_mut() {
        if !value.contains("${") {
            continue;
        }
        let mut substituted = String::with_capacity(value.len());
        let mut last = 0usize;
        for captures in placeholder_re.captures_iter(value) {
            let matched = captures.get(0).expect("placeholder match");
            let key = captures.get(1).expect("placeholder key").as_str();
            substituted.push_str(&value[last..matched.start()]);
            if key == current_key {
                substituted.push_str(matched.as_str());
            } else if let Some(replacement) = snapshot.get(key) {
                substituted.push_str(replacement);
            } else {
                substituted.push_str(matched.as_str());
            }
            last = matched.end();
        }
        substituted.push_str(&value[last..]);
        *value = substituted;
    }
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
        .get("suite_run_id")
        .or_else(|| base_variables.get("run_id"))
        .cloned()
        .unwrap_or_else(|| "sqlt".to_string());
    let case_run_id = format!("{}_{}", suite_run_id, stable_hash_hex(case_id));
    variables.insert("case_id".to_string(), case_id.to_string());
    variables.insert("run_id".to_string(), case_run_id.clone());
    variables.insert("suite_run_id".to_string(), suite_run_id);
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
    if let Some(port) = env_optional("NOVAROCKS_SQL_TEST_PORT") {
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
        "target port is not set; provide --port or NOVAROCKS_SQL_TEST_PORT, or configure tests/sql/runner/conf/default.toml with [cluster].port"
    );
}

pub fn resolve_repo_root() -> Result<PathBuf> {
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    loop {
        if dir.join("tests/sql/correctness").is_dir()
            && dir.join("tests/sql/benchmarks").is_dir()
            && dir.join("Cargo.toml").is_file()
        {
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
    if let Some(port) = env_optional("NOVAROCKS_SQL_TEST_REF_PORT") {
        return Ok(port);
    }
    if reference_required {
        bail!(
            "reference port is required for this mode; provide --ref-port or NOVAROCKS_SQL_TEST_REF_PORT"
        );
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
    match suite_name {
        // The optimizer suite uses iceberg base tables so that ANALYZE-derived
        // NDV (Puffin statistics) reaches the cost-based optimizer. Native
        // internal tables do not exist. The
        // `iceberg_opt` catalog is created by `tests/sql/correctness/optimizer/init.sql`. A
        // stable catalog name is safe: each worktree's standalone has its
        // own in-memory catalog registry, and per-case `${case_db}` reset
        // isolates data between cases.
        "optimizer" | "optimizer-dist" => "iceberg_opt".to_string(),
        _ => "default_catalog".to_string(),
    }
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

pub fn build_suite_configs(
    base_dir: &Path,
    lane: TestLane,
) -> Result<BTreeMap<String, SuiteConfig>> {
    let suites_dir = lane.suite_root(base_dir);
    let entries = fs::read_dir(&suites_dir)
        .with_context(|| format!("failed to read {}", suites_dir.display()))?;

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
            manifest: SuiteManifest::load(&path.join("suite.toml"))?,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn iceberg_dml_default_timeout_covers_expensive_boundary_cases() {
        assert_eq!(suite_default_query_timeout("iceberg-dml"), 120);
    }

    #[test]
    fn suite_discovery_is_scoped_to_the_selected_physical_lane() {
        let temp = tempdir().expect("temporary repository root");
        let correctness_sql = temp.path().join("tests/sql/correctness/filter/sql");
        let benchmark_sql = temp.path().join("tests/sql/benchmarks/ssb/sql");
        fs::create_dir_all(&correctness_sql).expect("create correctness suite");
        fs::create_dir_all(&benchmark_sql).expect("create benchmark workload");

        let correctness = build_suite_configs(temp.path(), TestLane::Correctness)
            .expect("discover correctness suites");
        let benchmarks = build_suite_configs(temp.path(), TestLane::Benchmark)
            .expect("discover benchmark workloads");

        assert_eq!(correctness.keys().collect::<Vec<_>>(), vec!["filter"]);
        assert_eq!(benchmarks.keys().collect::<Vec<_>>(), vec!["ssb"]);
    }

    #[test]
    fn suite_discovery_fails_when_the_selected_lane_root_is_absent() {
        let temp = tempdir().expect("temporary repository root");

        let error = build_suite_configs(temp.path(), TestLane::Correctness)
            .expect_err("missing correctness root must not fall back");

        assert!(format!("{error:#}").contains("tests/sql/correctness"));
    }

    #[test]
    fn loads_toml_config_and_flattens_nested_environment_values() {
        let directory = tempdir().expect("temporary config directory");
        let path = directory.path().join("runner.toml");
        fs::write(
            &path,
            r#"
[cluster]
host = "127.0.0.1"
port = "9030"

[env]
iceberg_catalog_type = "hadoop"

[env.oss]
oss_endpoint = "http://127.0.0.1:9000"
"#,
        )
        .expect("write runner configuration");

        let config = load_runner_config(Some(&path)).expect("load TOML configuration");

        assert_eq!(config.cluster.get("host"), Some(&"127.0.0.1".to_string()));
        assert_eq!(
            config.values.get("iceberg_catalog_type"),
            Some(&"hadoop".to_string())
        );
        assert_eq!(
            config.values.get("oss_endpoint"),
            Some(&"http://127.0.0.1:9000".to_string())
        );
        assert_eq!(
            config.values.get("env.oss.oss_endpoint"),
            Some(&"http://127.0.0.1:9000".to_string())
        );
    }

    #[test]
    fn shipped_toml_configurations_load_from_the_runner_directory() {
        let repo_root = resolve_repo_root().expect("repo root");
        for name in ["default.toml", "iceberg.toml", "iceberg-local.toml"] {
            let path = repo_root.join("tests/sql/runner/conf").join(name);
            let config = load_runner_config(Some(&path)).expect("load shipped TOML configuration");
            assert!(config.cluster.contains_key("host"), "{name}");
            assert!(config.cluster.contains_key("port"), "{name}");
        }
    }

    #[test]
    fn placeholder_variables_substitute_config_env_values_after_run_ids_exist() {
        let mut runner_config = RunnerConfig::default();
        runner_config.values.insert(
            "iceberg_catalog_warehouse".to_string(),
            "/tmp/novarocks-sql-test/${suite_uuid0}/${run_id}/".to_string(),
        );

        let variables = placeholder_variables_with_run_id(&runner_config, "iceberg", None);
        let warehouse = variables
            .get("iceberg_catalog_warehouse")
            .expect("warehouse");
        let suite_uuid0 = variables.get("suite_uuid0").expect("suite uuid");
        let run_id = variables.get("run_id").expect("run id");

        assert_eq!(
            warehouse,
            &format!("/tmp/novarocks-sql-test/{suite_uuid0}/{run_id}/")
        );
        assert!(!warehouse.contains("${"));
    }

    #[test]
    fn placeholder_variables_preserve_explicit_benchmark_run_identity() {
        let variables = placeholder_variables_with_run_id(
            &RunnerConfig::default(),
            "ssb",
            Some("sqlb_ssb_stable"),
        );

        assert_eq!(
            variables.get("run_id"),
            Some(&"sqlb_ssb_stable".to_string())
        );
        assert_eq!(
            variables.get("suite_uuid0"),
            Some(&"sqlb_ssb_stable_0".to_string())
        );
    }

    #[test]
    fn hms_suite_defaults_populate_uris_warehouse_and_oss() {
        // Env-independent: the crate's tests avoid touching process env (Rust
        // 2024 `std::env::{set,remove}_var` is `unsafe`), so assert only that
        // the HMS arm populates the expected keys plus the shared oss_* block.
        // The concrete values come from `env_or_default` and so vary with the
        // ambient environment; presence is what this arm guarantees.
        let mut vars = std::collections::HashMap::new();
        apply_suite_placeholder_defaults(&mut vars, "iceberg-hms");
        assert!(vars.contains_key("iceberg_hms_uris"));
        assert!(vars.contains_key("iceberg_hms_warehouse"));
        assert!(vars.contains_key("oss_ak"));
        assert!(vars.contains_key("oss_sk"));
        assert!(vars.contains_key("oss_endpoint"));

        // The compatibility suite shares the same arm.
        let mut compat_vars = std::collections::HashMap::new();
        apply_suite_placeholder_defaults(&mut compat_vars, "iceberg-hms-compatibility");
        assert!(compat_vars.contains_key("iceberg_hms_uris"));
        assert!(compat_vars.contains_key("iceberg_hms_warehouse"));
        assert!(compat_vars.contains_key("oss_endpoint"));
    }
}
