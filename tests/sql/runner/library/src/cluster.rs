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

//! SQL-runner adapter for cluster modes and runner-specific environment
//! resolution. The cross-process lifecycle itself lives in cluster-harness.

use crate::types::RunnerConfig;
use anyhow::{Context, Result, bail};
use clap::ValueEnum;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[allow(unused_imports)]
pub(crate) use novarocks_cluster_harness::{
    BePorts, ClusterProcessRole, CrossProcessRuntime, QueryLifecyclePhase, ServerFailureLogSources,
    ServerHandle, build_novarocks_command, render_cross_process_config, startup_timeout_from_env,
};
use novarocks_cluster_harness::{
    CrossProcessClusterOptions, CrossProcessConfigOverlay, CrossProcessServerHandle, LaunchProfile,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum ClusterMode {
    AllInOne,
    CrossProcess,
}

struct NoopServerHandle;

impl ServerHandle for NoopServerHandle {
    fn target_host(&self) -> Option<&str> {
        None
    }

    fn target_port(&self) -> Option<u16> {
        None
    }
}

pub(crate) fn launch_server(
    mode: ClusterMode,
    cluster_size: usize,
    repo_root: &Path,
    runner_config: &RunnerConfig,
    launch_profile: LaunchProfile,
    test_sessions: Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>,
    budget: &novarocks_cluster_harness::fixture_run::RunBudget,
) -> Result<Box<dyn ServerHandle>> {
    match mode {
        ClusterMode::AllInOne => Ok(Box::new(NoopServerHandle)),
        ClusterMode::CrossProcess => {
            let options = CrossProcessClusterOptions {
                binary: discover_novarocks_binary(repo_root)?,
                fe_binary: None,
                be_binaries: Vec::new(),
                expected_eligible_backend_count: None,
                base_config_path: resolve_base_frontend_config_path(repo_root, runner_config)?,
                runtime_root: repo_root
                    .join("tests/sql/.runtime/cluster")
                    .join(&budget.identity().run_id),
                cluster_size,
                launch_profile,
                startup_timeout: startup_timeout(),
                child_environment: resolve_fixture_credential_environment()?,
                config_overlay: resolve_role_scoped_connector_overlay()?,
                native_trust_fixture: Default::default(),
            };
            Ok(Box::new(
                CrossProcessServerHandle::launch_with_fixture_budget(
                    options,
                    test_sessions,
                    Default::default(),
                    None,
                    None,
                    None,
                    budget.clone(),
                )?,
            ))
        }
    }
}

/// Bind only declared fixture secrets after the runner's final fixture projection.
/// Performance children clear their ambient environment, so the exact role
/// config references need explicit bindings rather than an earlier fixture's
/// literals. Runtime and fault knobs remain excluded.
fn resolve_fixture_credential_environment()
-> Result<novarocks_cluster_harness::CrossProcessChildEnvironment> {
    let mut bindings = novarocks_cluster_harness::CrossProcessChildEnvironment::default();
    for (config_key, purpose, destination) in [
        (
            "NOVAROCKS_FE_CONFIG",
            "object-store-metadata",
            &mut bindings.fe,
        ),
        ("NOVAROCKS_BE_CONFIG", "object-store-data", &mut bindings.be),
    ] {
        let Some(path) = std::env::var_os(config_key) else {
            continue;
        };
        let source = fs::read_to_string(PathBuf::from(path))
            .with_context(|| format!("read declared {config_key} fixture credentials"))?;
        *destination =
            fixture_credential_environment(&source, purpose, |name| std::env::var(name).ok())?;
    }
    Ok(bindings)
}

fn fixture_credential_environment(
    source: &str,
    purpose: &str,
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<std::collections::BTreeMap<String, String>> {
    let config = source
        .parse::<toml::Value>()
        .context("parse fixture role config")?;
    let mut bindings = std::collections::BTreeMap::new();
    let credentials = config
        .get("connector")
        .and_then(|value| value.get("credentials"))
        .and_then(toml::Value::as_array);
    for credential in credentials.into_iter().flatten() {
        if credential.get("purpose").and_then(toml::Value::as_str) != Some(purpose) {
            continue;
        }
        if credential.get("kind").and_then(toml::Value::as_str) == Some("aliyun-ecs-ram-role") {
            anyhow::ensure!(
                ["access_key_id", "access_key_secret", "session_token"]
                    .iter()
                    .all(|key| credential.get(*key).is_none()),
                "node-local fixture sources cannot contain secret material or environment references"
            );
            continue;
        }
        for key in ["access_key_id", "access_key_secret"] {
            let Some(value) = credential.get(key).and_then(toml::Value::as_str) else {
                continue;
            };
            if !value.contains("${ENV") {
                continue;
            }
            let reference = value
                .strip_prefix("${ENV:")
                .and_then(|v| v.strip_suffix('}'));
            let Some(name @ ("AWS_S3_ACCESS_KEY_ID" | "AWS_S3_SECRET_ACCESS_KEY")) = reference
            else {
                bail!(
                    "performance fixture credentials require an exact supported secret reference"
                );
            };
            let secret = lookup(name)
                .filter(|value| !value.is_empty())
                .with_context(|| {
                    format!("declared fixture credential requires environment variable {name}")
                })?;
            bindings.insert(name.to_string(), secret);
        }
    }
    Ok(bindings)
}

/// Validate cluster CLI arguments. Returns an error string on failure.
pub(crate) fn validate_cluster_args(mode: ClusterMode, cluster_size: usize) -> Result<()> {
    if cluster_size == 0 {
        bail!("--cluster-size must be >= 1");
    }
    if mode == ClusterMode::AllInOne && cluster_size > 1 {
        bail!(
            "all-in-one mode requires --cluster-size 1 (got {})",
            cluster_size
        );
    }
    Ok(())
}

pub(crate) fn discover_novarocks_binary(repo_root: &Path) -> Result<PathBuf> {
    discover_novarocks_binary_with_override(
        repo_root,
        std::env::var_os("NOVAROCKS_BIN").map(PathBuf::from),
    )
}

pub(crate) fn discover_novarocks_binary_with_override(
    repo_root: &Path,
    env_override: Option<PathBuf>,
) -> Result<PathBuf> {
    if let Some(path) = env_override {
        if path.is_file() {
            return Ok(path);
        }
        bail!(
            "NOVAROCKS_BIN points to {}, but the file does not exist",
            path.display()
        );
    }

    bail!(
        "SQL cluster launch requires an explicit NOVAROCKS_BIN (repository {})",
        repo_root.display()
    )
}

pub(crate) fn resolve_base_frontend_config_path(
    repo_root: &Path,
    runner_config: &RunnerConfig,
) -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("NOVAROCKS_FE_CONFIG") {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Ok(path);
        }
        bail!(
            "NOVAROCKS_FE_CONFIG points to {}, but the file does not exist",
            path.display()
        );
    }

    if let Some(path) = runner_config.path.as_ref() {
        let sibling = path.with_extension("toml");
        if sibling.is_file() {
            return Ok(sibling);
        }
    }

    bail!(
        "failed to locate frontend config for cross-process mode under {}",
        repo_root.display()
    )
}

/// Reintroduce the BE-only connector section after the cross-process harness
/// renders from the FE config. Deployable FE and BE configs deliberately keep
/// their credential registries separate, while the harness accepts one base
/// config and projects credentials by role. Without this overlay a generated
/// FE config supplies only metadata credentials, so its BE projection has no
/// data credential to resolve.
fn resolve_role_scoped_connector_overlay() -> Result<CrossProcessConfigOverlay> {
    let Some(path) = std::env::var_os("NOVAROCKS_BE_CONFIG") else {
        return Ok(CrossProcessConfigOverlay::default());
    };
    let path = PathBuf::from(path);
    if !path.is_file() {
        bail!(
            "NOVAROCKS_BE_CONFIG points to {}, but the file does not exist",
            path.display()
        );
    }
    let source = fs::read_to_string(&path)
        .map_err(anyhow::Error::from)
        .map_err(|error| error.context(format!("read backend config {}", path.display())))?;
    let connector = connector_overlay_from_config(&source).map_err(|error| {
        error.context(format!("extract connector section from {}", path.display()))
    })?;
    Ok(CrossProcessConfigOverlay {
        be: connector,
        ..Default::default()
    })
}

fn connector_overlay_from_config(source: &str) -> Result<Option<String>> {
    let config = source
        .parse::<toml::Value>()
        .map_err(anyhow::Error::from)
        .context("parse backend config TOML")?;
    let Some(connector) = config.get("connector") else {
        return Ok(None);
    };
    let mut overlay = toml::map::Map::new();
    overlay.insert("connector".to_string(), connector.clone());
    toml::to_string(&toml::Value::Table(overlay))
        .map(Some)
        .map_err(anyhow::Error::from)
        .context("serialize backend connector overlay")
}

pub(crate) fn startup_timeout() -> Duration {
    startup_timeout_from_env(
        std::env::var("NOVAROCKS_STARTUP_TIMEOUT_SECS")
            .ok()
            .as_deref(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_server_handle_rejects_be_process_controls() {
        let mut handle = NoopServerHandle;
        assert!(handle.kill_be(0).is_err());
        assert!(handle.restart_be(0).is_err());
    }

    #[test]
    fn all_in_one_rejects_multiple_backends() {
        let error = validate_cluster_args(ClusterMode::AllInOne, 2).unwrap_err();
        assert!(format!("{error:#}").contains("requires --cluster-size 1"));
    }

    #[test]
    fn backend_connector_overlay_preserves_only_the_connector_section() {
        let overlay = connector_overlay_from_config(
            r#"
[cluster]
role = "be"

[[connector.credentials]]
purpose = "object-store-data"
name = "warehouse-data"
generation = "v1"
kind = "s3"
access_key_id = "${ENV:AWS_S3_ACCESS_KEY_ID}"
access_key_secret = "${ENV:AWS_S3_SECRET_ACCESS_KEY}"
"#,
        )
        .expect("extract backend connector overlay")
        .expect("backend connector section");
        let value = overlay.parse::<toml::Value>().expect("parse overlay");
        assert!(value.get("cluster").is_none());
        assert_eq!(
            value["connector"]["credentials"][0]["purpose"].as_str(),
            Some("object-store-data")
        );
    }

    #[test]
    fn backend_config_without_connector_needs_no_overlay() {
        assert!(
            connector_overlay_from_config("[cluster]\nrole = 'be'\n")
                .expect("parse backend config")
                .is_none()
        );
    }

    #[test]
    fn performance_fixture_secrets_follow_final_projection_and_exact_role() {
        let source = r#"
[[connector.credentials]]
purpose = "object-store-metadata"
access_key_id = "${ENV:AWS_S3_ACCESS_KEY_ID}"
access_key_secret = "${ENV:AWS_S3_SECRET_ACCESS_KEY}"

[[connector.credentials]]
purpose = "object-store-data"
access_key_id = "${ENV:UNCLASSIFIED_DATA_SECRET}"

[runtime]
knob = "${ENV:UNCLASSIFIED_RUNTIME_KNOB}"
"#;
        let bindings =
            fixture_credential_environment(source, "object-store-metadata", |name| match name {
                "AWS_S3_ACCESS_KEY_ID" => Some("final-fixture-key".to_string()),
                "AWS_S3_SECRET_ACCESS_KEY" => Some("final-fixture-secret".to_string()),
                _ => panic!("unrelated role or runtime input must not be read"),
            })
            .expect("bind final fixture identity");
        assert_eq!(bindings.len(), 2);
        assert_eq!(bindings["AWS_S3_ACCESS_KEY_ID"], "final-fixture-key");
        assert_eq!(bindings["AWS_S3_SECRET_ACCESS_KEY"], "final-fixture-secret");
        assert!(
            fixture_credential_environment(source, "object-store-data", |_| {
                panic!("unclassified secret must be refused before lookup")
            })
            .is_err()
        );
        assert!(fixture_credential_environment(source, "object-store-metadata", |_| None).is_err());
    }

    #[test]
    fn performance_fixture_literals_do_not_inherit_ambient_secrets() {
        let source = r#"
[[connector.credentials]]
purpose = "object-store-data"
access_key_id = "literal-key"
access_key_secret = "literal-secret"
"#;
        assert!(
            fixture_credential_environment(source, "object-store-data", |_| {
                panic!("literal credentials must not read ambient values")
            })
            .expect("literal config")
            .is_empty()
        );
    }
    #[test]
    fn node_local_fixture_source_never_resolves_ambient_material() {
        let source = r#"
[[connector.credentials]]
kind = "aliyun-ecs-ram-role"
purpose = "object-store-data"
name = "iceberg-oss-data"
generation = "v1-fixture"
role_name = "fixture-role"
scope_prefixes = ["s3://bucket/root/"]
"#;
        assert!(
            fixture_credential_environment(source, "object-store-data", |_| panic!(
                "node-local source must not resolve environment secrets"
            ))
            .unwrap()
            .is_empty()
        );
        let invalid = format!("{source}access_key_id = '${{ENV:AWS_S3_ACCESS_KEY_ID}}'\n");
        assert!(
            fixture_credential_environment(&invalid, "object-store-data", |_| panic!(
                "invalid snapshot rejected before lookup"
            ))
            .is_err()
        );
    }
}
