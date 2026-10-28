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
use novarocks_cluster_harness::isolated_iceberg_rest::{
    IsolatedIcebergRestEndpoints, IsolatedS3Identity,
};
use regex::Regex;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::env;
use std::fs;
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

#[cfg(test)]
const FIXTURE_ENDPOINT_KEYS: [&str; 7] = [
    "iceberg_rest_uri",
    "iceberg_rest_warehouse",
    "iceberg_rest_mv_uri",
    "iceberg_rest_mv_warehouse",
    "oss_endpoint",
    "oss_ak",
    "oss_sk",
];
const FIXTURE_COMMON_KEYS: [&str; 5] = [
    "iceberg_rest_uri",
    "iceberg_rest_warehouse",
    "iceberg_rest_mv_uri",
    "iceberg_rest_mv_warehouse",
    "oss_endpoint",
];

pub(crate) fn project_fixture_value(config: &mut RunnerConfig, key: &str, value: &str) {
    for alias in [
        key.to_string(),
        format!("env.{key}"),
        format!("env.oss.{key}"),
    ] {
        config.values.insert(alias, value.to_string());
    }
}

fn fixture_config_value(config: &RunnerConfig, key: &str) -> Result<Option<String>> {
    let mut selected: Option<&String> = None;
    for alias in [
        key.to_string(),
        format!("env.{key}"),
        format!("env.oss.{key}"),
    ] {
        if let Some(value) = config.values.get(&alias) {
            anyhow::ensure!(
                !value.trim().is_empty(),
                "fixture runtime configuration required: {key} is empty; use the generated sql-test.toml after docker/iceberg-rest/up.sh"
            );
            anyhow::ensure!(
                selected.is_none_or(|prior| prior == value),
                "fixture runtime configuration has conflicting aliases for {key}"
            );
            selected = Some(value);
        }
    }
    Ok(selected.cloned())
}

/// Temporary material cannot inhabit the node-local fixture branch.
enum FixtureObjectStore {
    Static {
        access_key: String,
        secret_key: String,
    },
    NodeLocal,
}

/// One endpoint projection for generated configurations and private fixtures.
/// Secrets deliberately have no Debug implementation.
pub(crate) struct FixtureEndpoints {
    values: [String; 5],
    object_store: FixtureObjectStore,
    region: String,
    path_style: String,
    metadata_name: String,
    data_name: String,
    generation: String,
    env_file: PathBuf,
}

impl FixtureEndpoints {
    pub(crate) fn from_runner_config(config: &RunnerConfig) -> Result<Self> {
        Self::from_runner_config_with_environment(config, |key| env::var(key).ok())
    }

    fn from_runner_config_with_environment(
        config: &RunnerConfig,
        environment: impl Fn(&str) -> Option<String>,
    ) -> Result<Self> {
        let required = |key: &str| -> Result<String> {
            let value = fixture_config_value(config, key)?.with_context(|| format!(
                "fixture runtime configuration required: missing {key}; use --config with the generated sql-test.toml after docker/iceberg-rest/up.sh"
            ))?;
            if let Some(reference) = value
                .strip_prefix("${ENV:")
                .and_then(|v| v.strip_suffix('}'))
            {
                anyhow::ensure!(
                    !reference.is_empty()
                        && reference
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b == b'_'),
                    "invalid fixture environment reference for {key}"
                );
                return environment(reference).filter(|v| !v.trim().is_empty()).with_context(|| format!("fixture runtime configuration requires environment variable {reference} declared by {key}"));
            }
            anyhow::ensure!(
                !value.contains("${ENV"),
                "fixture {key} must use a complete environment reference"
            );
            Ok(value)
        };
        let values = FIXTURE_COMMON_KEYS
            .map(required)
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        let kind = fixture_config_value(config, "fixture_object_store_kind")?
            .unwrap_or_else(|| "minio".into());
        let node_local = match kind.as_str() {
            "minio" => false,
            "aliyun-oss" => true,
            _ => bail!("unsupported fixture_object_store_kind {kind}"),
        };
        let optional = |key: &str, default: &str| -> Result<String> {
            Ok(fixture_config_value(config, key)?.unwrap_or_else(|| default.into()))
        };
        let mode = optional("iceberg_object_store_credential_mode", "static")?;
        anyhow::ensure!(
            mode == if node_local { "node-local" } else { "static" },
            "fixture object-store kind and credential mode disagree"
        );
        let object_store = if node_local {
            for key in ["oss_ak", "oss_sk", "oss_token", "oss_session_token"] {
                anyhow::ensure!(
                    fixture_config_value(config, key)?.is_none(),
                    "node-local fixture configuration must omit {key}"
                );
            }
            FixtureObjectStore::NodeLocal
        } else {
            FixtureObjectStore::Static {
                access_key: required("oss_ak")?,
                secret_key: required("oss_sk")?,
            }
        };
        let legacy_name = optional("iceberg_object_store_credential_name", "iceberg-test-data")?;
        let metadata_name = if node_local {
            required("iceberg_object_store_metadata_credential_name")?
        } else {
            optional(
                "iceberg_object_store_metadata_credential_name",
                &legacy_name,
            )?
        };
        let data_name = if node_local {
            required("iceberg_object_store_data_credential_name")?
        } else {
            optional("iceberg_object_store_data_credential_name", &legacy_name)?
        };
        let generation = if node_local {
            required("iceberg_object_store_credential_generation")?
        } else {
            optional("iceberg_object_store_credential_generation", "v1")?
        };
        let region = if node_local {
            required("oss_region")?
        } else {
            optional("oss_region", "us-east-1")?
        };
        let path_style = if node_local {
            required("oss_path_style")?
        } else {
            optional("oss_path_style", "true")?
        };
        anyhow::ensure!(
            matches!(path_style.as_str(), "true" | "false"),
            "oss_path_style must be true or false"
        );
        if node_local {
            anyhow::ensure!(
                !region.is_empty()
                    && region
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
                "invalid OSS fixture region"
            );
            for name in [&metadata_name, &data_name] {
                novarocks_spi::connector::NodeLocalCredentialReference::try_new(name, &generation)
                    .map_err(|_| anyhow::anyhow!("invalid OSS fixture source reference"))?;
            }
            anyhow::ensure!(
                path_style == "false"
                    && values[4] == format!("https://s3.oss-{region}-internal.aliyuncs.com"),
                "OSS fixture requires its exact regional internal S3 endpoint and virtual-host addressing"
            );
        }
        Self::new(
            values.try_into().expect("five endpoint keys"),
            object_store,
            region,
            path_style,
            metadata_name,
            data_name,
            generation,
            PathBuf::from(required("fixture_env_file")?),
        )
    }

    pub(crate) fn from_isolated(
        endpoints: &IsolatedIcebergRestEndpoints,
        identity: &IsolatedS3Identity,
        env_file: PathBuf,
    ) -> Result<Self> {
        Self::new(
            [
                endpoints.rest_uri.clone(),
                endpoints.rest_warehouse.clone(),
                endpoints.rest_mv_uri.clone(),
                endpoints.rest_mv_warehouse.clone(),
                endpoints.minio_endpoint.clone(),
            ],
            FixtureObjectStore::Static {
                access_key: identity.access_key_id.clone(),
                secret_key: identity.secret_access_key.clone(),
            },
            "us-east-1".into(),
            "true".into(),
            "iceberg-test-data".into(),
            "iceberg-test-data".into(),
            "v1".into(),
            env_file,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        values: [String; 5],
        object_store: FixtureObjectStore,
        region: String,
        path_style: String,
        metadata_name: String,
        data_name: String,
        generation: String,
        env_file: PathBuf,
    ) -> Result<Self> {
        for (key, value) in FIXTURE_COMMON_KEYS.iter().zip(&values) {
            anyhow::ensure!(
                !value.trim().is_empty(),
                "fixture runtime configuration required: missing {key}; use the generated sql-test.toml after docker/iceberg-rest/up.sh"
            );
        }
        anyhow::ensure!(
            env_file.is_absolute() && env_file.is_file(),
            "fixture runtime configuration requires an existing absolute fixture_env_file"
        );
        let env_file = env_file
            .canonicalize()
            .context("resolve fixture publication environment")?;
        Ok(Self {
            values,
            object_store,
            region,
            path_style,
            metadata_name,
            data_name,
            generation,
            env_file,
        })
    }

    pub(crate) fn is_node_local(&self) -> bool {
        matches!(self.object_store, FixtureObjectStore::NodeLocal)
    }

    pub(crate) fn preflight_cloud(config: &RunnerConfig, suites: &[String]) -> Result<()> {
        match fixture_config_value(config, "fixture_object_store_kind")?.as_deref() {
            Some("aliyun-oss") => {
                Self::from_runner_config(config)?.reject_unsupported_suites(suites)
            }
            None | Some("minio") => Ok(()),
            Some(kind) => bail!("unsupported fixture_object_store_kind {kind}"),
        }
    }

    pub(crate) fn reject_unsupported_sources(
        config: &RunnerConfig,
        sources: &BTreeMap<String, Vec<PathBuf>>,
    ) -> Result<()> {
        if fixture_config_value(config, "fixture_object_store_kind")?.as_deref()
            != Some("aliyun-oss")
        {
            return Ok(());
        }
        let vended = Regex::new(
            r#"(?i)["']credential\.(?:object-store-data|data-credential-vending)\.mode["']\s*=\s*["']vended["']"#,
        )?;
        for source in sources.values().flatten() {
            let text = fs::read_to_string(source)?;
            anyhow::ensure!(
                !vended.is_match(&text),
                "OSS node-local fixtures cannot run vended-credential scenario {}",
                source.display()
            );
        }
        Ok(())
    }

    pub(crate) fn reject_unsupported_suites(&self, suites: &[String]) -> Result<()> {
        if self.is_node_local() {
            for suite in suites {
                anyhow::ensure!(
                    !crate::isolated_rest_catalog_suite(suite) && suite != "paimon",
                    "OSS node-local fixtures do not support private MinIO, IAM or vended-credential suite {suite}"
                );
            }
        }
        Ok(())
    }

    /// Replaces every supported spelling, including aliases left by TOML flattening.
    pub(crate) fn project(&self, config: &mut RunnerConfig) -> BTreeMap<String, String> {
        for (key, value) in FIXTURE_COMMON_KEYS.iter().zip(&self.values) {
            project_fixture_value(config, key, value);
        }
        let kind = if self.is_node_local() {
            "aliyun-oss"
        } else {
            "minio"
        };
        let mode = if self.is_node_local() {
            "node-local"
        } else {
            "static"
        };
        for (key, value) in [
            ("fixture_object_store_kind", kind),
            ("iceberg_object_store_credential_mode", mode),
            ("oss_region", &self.region),
            ("oss_path_style", &self.path_style),
            (
                "iceberg_object_store_metadata_credential_name",
                &self.metadata_name,
            ),
            ("iceberg_object_store_data_credential_name", &self.data_name),
            (
                "iceberg_object_store_credential_generation",
                &self.generation,
            ),
        ] {
            project_fixture_value(config, key, value);
        }
        project_fixture_value(config, "fixture_env_file", &self.env_file.to_string_lossy());
        let [rest, warehouse, rest_mv, rest_mv_warehouse, endpoint] = &self.values;
        let mut child: BTreeMap<String, String> = [
            ("AWS_S3_ENDPOINT", endpoint.clone()),
            ("AWS_S3_REGION", self.region.clone()),
            ("NOVAROCKS_ICEBERG_REST_URI", rest.clone()),
            ("NOVAROCKS_ICEBERG_REST_WAREHOUSE", warehouse.clone()),
            ("NOVAROCKS_ICEBERG_REST_MV_URI", rest_mv.clone()),
            (
                "NOVAROCKS_ICEBERG_REST_MV_WAREHOUSE",
                rest_mv_warehouse.clone(),
            ),
            ("NOVA_ENV_REST_WAREHOUSE_URI", warehouse.clone()),
            (
                "NOVA_ENV_REST_ENV_FILE",
                self.env_file.to_string_lossy().into_owned(),
            ),
        ]
        .into_iter()
        .map(|(key, value)| (key.into(), value))
        .collect();
        if let FixtureObjectStore::Static {
            access_key,
            secret_key,
        } = &self.object_store
        {
            project_fixture_value(config, "oss_ak", access_key);
            project_fixture_value(config, "oss_sk", secret_key);
            for (key, value) in [
                ("AWS_S3_ACCESS_KEY_ID", access_key),
                ("AWS_S3_SECRET_ACCESS_KEY", secret_key),
                ("MINIO_ROOT_USER", access_key),
                ("MINIO_ROOT_PASSWORD", secret_key),
            ] {
                child.insert(key.into(), value.clone());
            }
        }
        child
    }
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
    // These are the original local fixture constants. Cloud publications must
    // explicitly declare their source facts and never inherit local defaults.
    if variables
        .get("fixture_object_store_kind")
        .map(String::as_str)
        != Some("aliyun-oss")
    {
        let name = variables
            .get("iceberg_object_store_credential_name")
            .cloned()
            .unwrap_or_else(|| "iceberg-test-data".into());
        for (key, value) in [
            ("iceberg_object_store_credential_mode", "static"),
            (
                "iceberg_object_store_metadata_credential_name",
                name.as_str(),
            ),
            ("iceberg_object_store_data_credential_name", name.as_str()),
            ("oss_region", "us-east-1"),
            ("oss_path_style", "true"),
        ] {
            insert_placeholder_default(variables, key, value);
        }
    }
    match suite_name {
        "iceberg"
        | "iceberg-ddl"
        | "iceberg-dml"
        | "iceberg-ivm"
        | "iceberg-mv-apply"
        | "iceberg-mv-scheduler"
        | "materialized-view"
        | "mv-rewrite" => {
            // Catalog semantics may have defaults; runtime endpoints never do.
            insert_placeholder_default(variables, "iceberg_catalog_type", "hadoop");
            insert_placeholder_default(
                variables,
                "iceberg_catalog_warehouse",
                env_or_default("CATALOG_WAREHOUSE_URI", "s3://novarocks/iceberg-catalog"),
            );
        }
        "iceberg-hms" | "iceberg-hms-compatibility" => {
            for (key, environment) in [
                ("iceberg_hms_uris", "NOVAROCKS_ICEBERG_HMS_URI"),
                ("iceberg_hms_warehouse", "NOVA_ENV_SHARED_HMS_WAREHOUSE_URI"),
            ] {
                if let Some(value) = env_optional(environment) {
                    insert_placeholder_default(variables, key, value);
                }
            }
        }
        _ => {}
    }
}

pub fn placeholder_variables(
    runner_config: &RunnerConfig,
    suite_name: &str,
) -> Result<HashMap<String, String>> {
    let mut variables = runner_config.values.clone();
    apply_suite_placeholder_defaults(&mut variables, suite_name);
    runner_config
        .fixture_names
        .as_ref()
        .context("SQL naming requires an admitted invocation")?
        .extend_suite_variables(suite_name, &mut variables)?;
    variables.insert("suite".to_string(), suite_name.to_string());
    if let Some(root) = variables.get("__scratch_scope").cloned() {
        if variables
            .get("iceberg_catalog_warehouse")
            .is_some_and(|v| v.starts_with("s3://"))
        {
            variables.insert(
                "iceberg_catalog_warehouse".into(),
                format!(
                    "{root}/hadoop/s{}",
                    variables
                        .get("__suite_ordinal")
                        .context("missing suite ordinal")?
                ),
            );
        }
        // This is a mutable case-author warehouse, separate from READY datasets.
        if variables
            .get("iceberg_test_warehouse")
            .is_some_and(|value| value.starts_with("s3://"))
        {
            variables.insert(
                "iceberg_test_warehouse".into(),
                format!("{root}/hadoop/extra/s{}", variables["__suite_ordinal"]),
            );
        }
    }
    // Only the admitted shared REST binding changes explicit CTAS warehouses.
    // Private fixtures and READY dataset roots retain their actual projection.
    if let Some(root) = runner_config.values.get("__shared_rest_run_root") {
        for (name, suffix) in [
            ("iceberg_rest_warehouse", "rest"),
            ("iceberg_rest_mv_warehouse", "rest-mv"),
        ] {
            variables.insert(name.into(), format!("{root}/{suffix}"));
        }
    }
    substitute_known_variable_values(&mut variables);
    Ok(variables)
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

pub fn case_auto_db_name(run: &str, ordinal: usize) -> Result<String> {
    crate::fixture_context::resource(run, 'd', ordinal, 0)
}

pub fn case_placeholder_variables(
    base_variables: &HashMap<String, String>,
    case_id: &str,
) -> Result<HashMap<String, String>> {
    let mut variables = base_variables.clone();
    let run = base_variables
        .get("suite_run_id")
        .context("SQL case omitted admitted run identity")?;
    let index = base_variables
        .get(&format!("__case_ordinal.{case_id}"))
        .context("SQL case omitted its frozen ordinal")?
        .parse::<usize>()?;
    variables.insert("case_id".into(), case_id.to_string());
    variables.insert("run_id".into(), run.clone());
    for slot in 0..10 {
        variables.insert(
            format!("uuid{slot}"),
            crate::fixture_context::resource(run, 'u', index, slot)?,
        );
    }
    variables.insert("case_db".into(), case_auto_db_name(run, index)?);
    for slot in 1..9 {
        variables.insert(
            format!("case_db_{}", slot + 1),
            crate::fixture_context::resource(run, 'd', index, slot)?,
        );
    }
    Ok(variables)
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
        if dir.join("tests/sql/correctness").is_dir() && dir.join("Cargo.toml").is_file() {
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

pub fn build_suite_configs(base_dir: &Path) -> Result<BTreeMap<String, SuiteConfig>> {
    let suites_dir = base_dir.join("tests/sql/correctness");
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
    fn suite_discovery_uses_only_the_correctness_root() {
        let temp = tempdir().expect("temporary repository root");
        let correctness_sql = temp.path().join("tests/sql/correctness/filter/sql");
        let benchmark_sql = temp.path().join("tests/sql/benchmarks/ssb/sql");
        fs::create_dir_all(&correctness_sql).expect("create correctness suite");
        fs::create_dir_all(&benchmark_sql).expect("create benchmark workload");

        let correctness = build_suite_configs(temp.path()).expect("discover correctness suites");

        assert_eq!(correctness.keys().collect::<Vec<_>>(), vec!["filter"]);
    }

    #[test]
    fn suite_discovery_fails_when_the_correctness_root_is_absent() {
        let temp = tempdir().expect("temporary repository root");

        let error = build_suite_configs(temp.path())
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

        runner_config.fixture_names = Some(std::sync::Arc::new(
            crate::fixture_context::test_names("iceberg", &["case"]),
        ));
        let variables = placeholder_variables(&runner_config, "iceberg").unwrap();
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
    fn mutable_extra_warehouse_uses_run_scope_without_rebinding_ready_datasets() {
        let mut config = RunnerConfig {
            fixture_names: Some(std::sync::Arc::new(crate::fixture_context::test_names(
                "iceberg",
                &["case"],
            ))),
            ..RunnerConfig::default()
        };
        for (key, value) in [
            ("iceberg_test_warehouse", "s3://novarocks/legacy-extra"),
            (
                "dataset_iceberg_catalog_warehouse",
                "s3://novarocks/benchmark/ready",
            ),
            (
                "iceberg_rest_warehouse",
                "s3://novarocks/readonly-server-root",
            ),
        ] {
            config.values.insert(key.into(), value.into());
        }
        let values = placeholder_variables(&config, "iceberg").unwrap();
        assert_eq!(
            values["iceberg_test_warehouse"],
            format!("{}/hadoop/extra/s0", values["__scratch_scope"])
        );
        assert_eq!(
            values["dataset_iceberg_catalog_warehouse"],
            "s3://novarocks/benchmark/ready"
        );
        assert_eq!(
            values["iceberg_rest_warehouse"],
            "s3://novarocks/readonly-server-root"
        );
        assert_eq!(
            config.values["iceberg_test_warehouse"],
            "s3://novarocks/legacy-extra"
        );
        config
            .values
            .insert("iceberg_test_warehouse".into(), "/tmp/private-extra".into());
        assert_eq!(
            placeholder_variables(&config, "iceberg").unwrap()["iceberg_test_warehouse"],
            "/tmp/private-extra"
        );
    }
    #[test]
    fn shared_rest_ctas_warehouses_use_run_root_without_changing_ready_or_private_bindings() {
        let mut config = RunnerConfig {
            fixture_names: Some(std::sync::Arc::new(crate::fixture_context::test_names(
                "iceberg",
                &["case"],
            ))),
            ..RunnerConfig::default()
        };
        config.values.insert(
            "iceberg_rest_warehouse".into(),
            "s3://warehouse/private/actual".into(),
        );
        config.values.insert(
            "iceberg_rest_mv_warehouse".into(),
            "s3://warehouse/private/mv".into(),
        );
        config.values.insert(
            "dataset_iceberg_rest_warehouse".into(),
            "s3://warehouse/ready".into(),
        );
        let private = placeholder_variables(&config, "iceberg").unwrap();
        assert_eq!(
            private["iceberg_rest_warehouse"],
            "s3://warehouse/private/actual"
        );
        config.values.insert(
            "__shared_rest_run_root".into(),
            private["__scratch_scope"].clone(),
        );
        let shared = placeholder_variables(&config, "iceberg").unwrap();
        assert_eq!(
            shared["iceberg_rest_warehouse"],
            format!("{}/rest", shared["__scratch_scope"])
        );
        assert_eq!(
            shared["iceberg_rest_mv_warehouse"],
            format!("{}/rest-mv", shared["__scratch_scope"])
        );
        assert_eq!(
            shared["dataset_iceberg_rest_warehouse"],
            "s3://warehouse/ready"
        );
    }
    #[test]
    fn suite_defaults_never_invent_fixture_endpoints_or_credentials() {
        for suite in [
            "iceberg",
            "iceberg-compatibility",
            "statistics",
            "iceberg-hms",
            "iceberg-mv-apply",
        ] {
            let mut variables = HashMap::new();
            apply_suite_placeholder_defaults(&mut variables, suite);
            for key in FIXTURE_ENDPOINT_KEYS {
                assert!(!variables.contains_key(key), "{suite} invented {key}");
            }
        }
    }

    fn configured_fixture() -> (tempfile::TempDir, PathBuf) {
        let directory = tempdir().unwrap();
        let env_file = directory.path().join("env.sh");
        fs::write(&env_file, "export NOVA_ENV_READY=true\n").unwrap();
        let path = directory.path().join("sql-test.toml");
        fs::write(
            &path,
            format!(
                r#"
[cluster]
host = "127.0.0.1"
port = "23223"

[env]
fixture_env_file = {env_file:?}
iceberg_rest_uri = "http://127.0.0.1:38181"
iceberg_rest_warehouse = "s3://warehouse/worktree/rest"
iceberg_rest_mv_uri = "http://127.0.0.1:38182"
iceberg_rest_mv_warehouse = "s3://warehouse/worktree/rest-mv"
oss_endpoint = "http://127.0.0.1:38000"
oss_ak = "fixture-key"
oss_sk = "fixture-secret"
"#
            ),
        )
        .unwrap();
        (directory, path)
    }

    #[test]
    fn loaded_toml_projection_replaces_all_aliases_and_subprocess_values() {
        let (_directory, path) = configured_fixture();
        // The real TOML loader retains scoped spellings alongside plain keys.
        // Include a conflicting nested legacy scope: a private fixture replaces all.
        let mut source = fs::read_to_string(&path).unwrap();
        source.push_str("\n[env.oss]\n");
        for key in FIXTURE_ENDPOINT_KEYS {
            source.push_str(&format!("{key} = \"foreign\"\n"));
        }
        fs::write(&path, source).unwrap();
        let mut config = load_runner_config(Some(&path)).unwrap();
        assert_eq!(
            config.values["env.iceberg_rest_uri"],
            "http://127.0.0.1:38181"
        );
        assert_eq!(config.values["env.oss.iceberg_rest_uri"], "foreign");
        let endpoints = IsolatedIcebergRestEndpoints {
            rest_uri: "http://127.0.0.1:39181".into(),
            rest_warehouse: "s3://warehouse/isolated/rest".into(),
            rest_mv_uri: "http://127.0.0.1:39182".into(),
            rest_mv_warehouse: "s3://warehouse/isolated/rest-mv".into(),
            minio_endpoint: "http://127.0.0.1:39000".into(),
            compose_project: "nr-isolated-rest-test".into(),
        };
        let identity = IsolatedS3Identity {
            access_key_id: "private-key".into(),
            secret_access_key: "private-secret".into(),
        };
        let env_file = path.parent().unwrap().join("private-env.sh");
        fs::write(&env_file, "export NOVA_ENV_READY=true\n").unwrap();
        let projection =
            FixtureEndpoints::from_isolated(&endpoints, &identity, env_file.clone()).unwrap();
        let child = projection.project(&mut config);
        for (key, expected) in FIXTURE_COMMON_KEYS.iter().zip(&projection.values) {
            for alias in [
                key.to_string(),
                format!("env.{key}"),
                format!("env.oss.{key}"),
            ] {
                assert_eq!(config.values.get(&alias), Some(expected), "{alias}");
            }
        }
        let output = std::process::Command::new("/usr/bin/env")
            .env_clear()
            .envs(&child)
            .output()
            .unwrap();
        assert!(output.status.success());
        let actual = String::from_utf8(output.stdout).unwrap();
        let captured: BTreeMap<_, _> = actual
            .lines()
            .map(|line| {
                let (key, value) = line.split_once('=').unwrap();
                (key.to_string(), value.to_string())
            })
            .collect();
        assert_eq!(captured, child);
        assert_eq!(captured["AWS_S3_ENDPOINT"], endpoints.minio_endpoint);
        assert_eq!(captured["AWS_S3_ACCESS_KEY_ID"], identity.access_key_id);
        assert_eq!(captured["MINIO_ROOT_USER"], identity.access_key_id);
        assert_eq!(
            captured["AWS_S3_SECRET_ACCESS_KEY"],
            identity.secret_access_key
        );
        assert_eq!(captured["MINIO_ROOT_PASSWORD"], identity.secret_access_key);
        assert_eq!(captured["NOVAROCKS_ICEBERG_REST_URI"], endpoints.rest_uri);
        assert_eq!(
            captured["NOVAROCKS_ICEBERG_REST_MV_URI"],
            endpoints.rest_mv_uri
        );
        assert_eq!(
            captured["NOVAROCKS_ICEBERG_REST_MV_WAREHOUSE"],
            endpoints.rest_mv_warehouse
        );
        assert_eq!(
            captured["NOVAROCKS_ICEBERG_REST_WAREHOUSE"],
            endpoints.rest_warehouse
        );
        assert_eq!(
            captured["NOVA_ENV_REST_WAREHOUSE_URI"],
            endpoints.rest_warehouse
        );
        assert_eq!(
            Path::new(&captured["NOVA_ENV_REST_ENV_FILE"]),
            env_file.canonicalize().unwrap()
        );
    }

    #[test]
    fn loaded_fixture_requires_complete_unambiguous_runtime_configuration() {
        let (_directory, path) = configured_fixture();
        let configured = load_runner_config(Some(&path)).unwrap();
        for key in FIXTURE_ENDPOINT_KEYS
            .into_iter()
            .chain(["fixture_env_file"])
        {
            let mut missing = configured.clone();
            for alias in [
                key.to_string(),
                format!("env.{key}"),
                format!("env.oss.{key}"),
            ] {
                missing.values.remove(&alias);
            }
            let error = FixtureEndpoints::from_runner_config(&missing)
                .err()
                .expect("missing input must fail");
            assert!(error.to_string().contains(key), "{error}");
            assert!(
                error.to_string().contains("docker/iceberg-rest/up.sh"),
                "{error}"
            );
        }
        let mut conflict = configured.clone();
        conflict
            .values
            .insert("env.oss.oss_endpoint".into(), "foreign".into());
        assert!(
            FixtureEndpoints::from_runner_config(&conflict)
                .err()
                .unwrap()
                .to_string()
                .contains("conflicting aliases")
        );
        let mut blank = configured.clone();
        blank.values.insert("oss_ak".into(), String::new());
        assert!(FixtureEndpoints::from_runner_config(&blank).is_err());
        let mut projected = configured.clone();
        FixtureEndpoints::from_runner_config(&configured)
            .unwrap()
            .project(&mut projected);
        assert!(FixtureEndpoints::from_runner_config(&projected).is_ok());
    }

    #[test]
    fn shipped_configs_cannot_resolve_a_fixture_without_runtime_configuration() {
        let repo_root = resolve_repo_root().unwrap();
        for name in ["default.toml", "iceberg.toml", "iceberg-local.toml"] {
            let config =
                load_runner_config(Some(&repo_root.join("tests/sql/runner/conf").join(name)))
                    .unwrap();
            let error = FixtureEndpoints::from_runner_config(&config)
                .err()
                .expect("no endpoint fallback");
            assert!(
                error
                    .to_string()
                    .contains("fixture runtime configuration required")
            );
        }
    }

    // Retained while Paimon-specific tests are suspended.
    #[cfg(any())]
    #[test]
    fn paimon_keys_append_to_the_generated_final_env_table() {
        let (_directory, path) = configured_fixture();
        let mut source = fs::read_to_string(&path)
            .unwrap()
            .replace("fixture-key", "${ENV:AWS_S3_ACCESS_KEY_ID}")
            .replace("fixture-secret", "${ENV:AWS_S3_SECRET_ACCESS_KEY}");
        // These are the fields appended by docker/paimon-read/fixture.py.
        source.push_str(
            r#"
paimon_fixture_manifest = "/tmp/paimon/manifest.json"
paimon_warehouse = "s3://warehouse/private/paimon"
paimon_catalog_sql = "/tmp/paimon/catalog.sql"
paimon_object_store_credential_name = "iceberg-test-data"
paimon_object_store_credential_generation = "v1"
"#,
        );
        fs::write(&path, source).unwrap();
        let mut config = load_runner_config(Some(&path)).unwrap();
        let physical =
            FixtureEndpoints::from_runner_config_with_environment(&config, |key| match key {
                "AWS_S3_ACCESS_KEY_ID" => Some("paimon-key".into()),
                "AWS_S3_SECRET_ACCESS_KEY" => Some("paimon-secret".into()),
                _ => panic!("literal configuration unexpectedly read {key}"),
            })
            .unwrap();
        let child = physical.project(&mut config);
        assert_eq!(
            config.values["paimon_warehouse"],
            "s3://warehouse/private/paimon"
        );
        assert_eq!(
            config.values["env.paimon_object_store_credential_name"],
            "iceberg-test-data"
        );
        assert_eq!(config.values["env.oss.oss_ak"], "paimon-key");
        assert_eq!(config.values["env.oss_sk"], "paimon-secret");
        assert_eq!(child["AWS_S3_ACCESS_KEY_ID"], "paimon-key");
        assert_eq!(child["AWS_S3_SECRET_ACCESS_KEY"], "paimon-secret");
    }

    #[test]
    fn explicit_environment_references_reject_missing_empty_or_malformed_inputs() {
        let (_directory, path) = configured_fixture();
        let mut config = load_runner_config(Some(&path)).unwrap();
        FixtureEndpoints::from_runner_config_with_environment(&config, |_| {
            panic!("literal value must not read process environment")
        })
        .unwrap();
        project_fixture_value(&mut config, "oss_sk", "${ENV:FIXTURE_SECRET}");
        for value in [None, Some(String::new()), Some("  ".into())] {
            let error =
                FixtureEndpoints::from_runner_config_with_environment(&config, |_| value.clone())
                    .err()
                    .expect("missing referenced credential must fail");
            assert!(error.to_string().contains("FIXTURE_SECRET"));
        }
        for malformed in [
            "${ENV}",
            "${ENV:}",
            "${ENV:BAD-NAME}",
            "${ENV:SECRET",
            "prefix${ENV:SECRET}",
            "${ENV:SECRET}suffix",
        ] {
            project_fixture_value(&mut config, "oss_sk", malformed);
            assert!(
                FixtureEndpoints::from_runner_config_with_environment(&config, |_| panic!(
                    "malformed reference must not be read"
                ))
                .is_err()
            );
        }
    }
    fn node_local_config() -> (tempfile::TempDir, RunnerConfig) {
        let (directory, path) = configured_fixture();
        let mut config = load_runner_config(Some(&path)).unwrap();
        for key in ["oss_ak", "oss_sk"] {
            for alias in [
                key.to_string(),
                format!("env.{key}"),
                format!("env.oss.{key}"),
            ] {
                config.values.remove(&alias);
            }
        }
        for (key, value) in [
            ("fixture_object_store_kind", "aliyun-oss"),
            (
                "oss_endpoint",
                "https://s3.oss-cn-beijing-internal.aliyuncs.com",
            ),
            ("oss_region", "cn-beijing"),
            ("oss_path_style", "false"),
            ("iceberg_object_store_credential_mode", "node-local"),
            (
                "iceberg_object_store_metadata_credential_name",
                "iceberg-oss-metadata",
            ),
            (
                "iceberg_object_store_data_credential_name",
                "iceberg-oss-data",
            ),
            ("iceberg_object_store_credential_generation", "v1-fixture"),
        ] {
            project_fixture_value(&mut config, key, value);
        }
        (directory, config)
    }

    #[test]
    fn node_local_fixture_projection_has_no_material_or_ambient_lookup() {
        let (_directory, mut config) = node_local_config();
        let endpoints = FixtureEndpoints::from_runner_config_with_environment(&config, |_| {
            panic!("node-local fixture must not read ambient material")
        })
        .unwrap();
        assert!(endpoints.is_node_local());
        let child = endpoints.project(&mut config);
        assert_eq!(
            config.values["iceberg_object_store_credential_mode"],
            "node-local"
        );
        assert_eq!(
            config.values["iceberg_object_store_metadata_credential_name"],
            "iceberg-oss-metadata"
        );
        assert_eq!(
            config.values["iceberg_object_store_data_credential_name"],
            "iceberg-oss-data"
        );
        assert_eq!(config.values["oss_region"], "cn-beijing");
        assert_eq!(config.values["oss_path_style"], "false");
        for key in [
            "AWS_S3_ACCESS_KEY_ID",
            "AWS_S3_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "MINIO_ROOT_USER",
            "MINIO_ROOT_PASSWORD",
        ] {
            assert!(!child.contains_key(key));
        }
        for key in ["oss_ak", "oss_sk"] {
            assert!(fixture_config_value(&config, key).unwrap().is_none());
        }
        assert!(FixtureEndpoints::from_runner_config(&config).is_ok());
    }

    #[test]
    fn node_local_fixture_refuses_incomplete_mixed_or_wrong_route_inputs() {
        let (_directory, config) = node_local_config();
        for (key, value) in [
            ("oss_ak", "snapshot"),
            ("oss_sk", "snapshot"),
            ("oss_session_token", "snapshot"),
            ("oss_region", "other/region"),
            ("oss_endpoint", "http://minio:9000"),
            ("oss_path_style", "true"),
            ("iceberg_object_store_credential_mode", "static"),
            ("iceberg_object_store_data_credential_name", "invalid/name"),
        ] {
            let mut changed = config.clone();
            project_fixture_value(&mut changed, key, value);
            assert!(
                FixtureEndpoints::from_runner_config(&changed).is_err(),
                "{key}"
            );
        }
        for key in [
            "oss_region",
            "oss_path_style",
            "iceberg_object_store_metadata_credential_name",
            "iceberg_object_store_data_credential_name",
            "iceberg_object_store_credential_generation",
        ] {
            let mut changed = config.clone();
            for alias in [
                key.to_string(),
                format!("env.{key}"),
                format!("env.oss.{key}"),
            ] {
                changed.values.remove(&alias);
            }
            assert!(
                FixtureEndpoints::from_runner_config(&changed).is_err(),
                "{key}"
            );
        }
    }

    #[test]
    fn cloud_fixture_rejects_minio_and_vended_scenarios_before_launch() {
        let (directory, config) = node_local_config();
        assert!(FixtureEndpoints::preflight_cloud(&config, &["iceberg-rest".into()]).is_ok());
        for suite in ["iceberg-ivm", "mv-publication-v11", "paimon"] {
            assert!(FixtureEndpoints::preflight_cloud(&config, &[suite.into()]).is_err());
        }
        let source = directory.path().join("vended.sql");
        fs::write(&source, r#"CREATE EXTERNAL CATALOG c PROPERTIES ("credential.object-store-data.mode" = "vended");"#).unwrap();
        assert!(
            FixtureEndpoints::reject_unsupported_sources(
                &config,
                &BTreeMap::from([("custom".into(), vec![source])])
            )
            .is_err()
        );
    }
}
