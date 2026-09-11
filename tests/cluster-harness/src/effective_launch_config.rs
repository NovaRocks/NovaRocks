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

use super::{CrossProcessNativeFaultProxyConfig, CrossProcessRuntime, LaunchProfile};
use anyhow::{Context, Result, ensure};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use toml::Value;

const ARTIFACT_SCHEMA_VERSION: u8 = 1;
const FIXED_NO_PROXY: &str = "127.0.0.1,localhost";
const FORMAL_SECRET_ENVIRONMENT: &[&str] = &[
    "NOVAROCKS_PREPARATION_DIAGNOSTIC_SECRET",
    "NOVAROCKS_SYSTEM_NATIVE_TRUST_SECRET",
    "NOVAROCKS_UEA1_PERF_S3_ACCESS_KEY_ID",
    "NOVAROCKS_UEA1_PERF_S3_SECRET_ACCESS_KEY",
];

/// Secret-free evidence derived from the exact TOML passed to every process.
///
/// The artifact bytes contain the complete semantic projection. Runtime-owned
/// ports and paths are replaced only after their exact values have been
/// checked. Environment values are never serialized.
#[derive(Debug, Clone)]
pub struct EffectiveLaunchConfigEvidence {
    artifact_bytes: Vec<u8>,
    artifact_sha256: String,
    semantics_sha256: String,
}

impl EffectiveLaunchConfigEvidence {
    pub fn artifact_bytes(&self) -> &[u8] {
        &self.artifact_bytes
    }

    pub fn artifact_sha256(&self) -> &str {
        &self.artifact_sha256
    }

    pub fn semantics_sha256(&self) -> &str {
        &self.semantics_sha256
    }
}

pub(crate) struct EffectiveLaunchConfigInput<'a> {
    pub launch_profile: LaunchProfile,
    pub cluster_size: usize,
    pub expected_eligible_backend_count: usize,
    pub runtime: &'a CrossProcessRuntime,
    pub runtime_dir: &'a Path,
    pub advertise_host: &'a str,
    pub frontend_config: &'a str,
    pub backend_configs: &'a [String],
    pub frontend_environment: &'a BTreeMap<String, String>,
    pub backend_environments: &'a [BTreeMap<String, String>],
    pub native_proxy_config: &'a CrossProcessNativeFaultProxyConfig,
    pub advertised_backend_grpc_ports: &'a [u16],
}

#[derive(Debug, Serialize)]
struct Artifact<'a> {
    schema_version: u8,
    semantics: &'a EffectiveLaunchConfigSemantics,
}

#[derive(Debug, Serialize)]
struct EffectiveLaunchConfigSemantics {
    launch_profile: &'static str,
    cluster_size: usize,
    expected_eligible_backend_count: usize,
    fixed_environment: BTreeMap<String, String>,
    child_environment: ChildEnvironmentContract,
    native_proxy: NativeProxyContract,
    roles: Vec<RoleConfigSemantics>,
}

#[derive(Debug, Serialize)]
struct ChildEnvironmentContract {
    frontend: Vec<EnvironmentBinding>,
    backends: Vec<Vec<EnvironmentBinding>>,
}

#[derive(Debug, Serialize)]
struct EnvironmentBinding {
    name: String,
    classification: EnvironmentClassification,
    present: bool,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "kebab-case")]
enum EnvironmentClassification {
    SecretPresence,
    NonFormalUnsealed,
}

#[derive(Debug, Serialize)]
struct NativeProxyContract {
    backends: Vec<NativeProxyBackendContract>,
}

#[derive(Debug, Serialize)]
struct NativeProxyBackendContract {
    backend_index: usize,
    retained_byte_limit: u64,
}

#[derive(Debug, Serialize)]
struct RoleConfigSemantics {
    role: &'static str,
    backend_index: Option<usize>,
    effective_config: CanonicalToml,
    attachments: Vec<ConfigAttachment>,
}

#[derive(Debug, Serialize)]
struct ConfigAttachment {
    kind: &'static str,
    sha256: String,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", content = "value", rename_all = "kebab-case")]
enum CanonicalToml {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Datetime(String),
    Array(Vec<CanonicalToml>),
    Table(BTreeMap<String, CanonicalToml>),
}

pub(crate) fn build_effective_launch_config_evidence(
    input: EffectiveLaunchConfigInput<'_>,
) -> Result<EffectiveLaunchConfigEvidence> {
    ensure!(
        input.backend_configs.len() == input.cluster_size
            && input.backend_environments.len() == input.cluster_size
            && input.advertised_backend_grpc_ports.len() == input.cluster_size
            && input.runtime.be.len() == input.cluster_size,
        "effective launch config requires one config, environment, runtime, and advertised endpoint per backend"
    );

    let child_environment = ChildEnvironmentContract {
        frontend: environment_contract(input.launch_profile, input.frontend_environment)?,
        backends: input
            .backend_environments
            .iter()
            .map(|environment| environment_contract(input.launch_profile, environment))
            .collect::<Result<Vec<_>>>()?,
    };
    let native_proxy = NativeProxyContract {
        backends: input
            .native_proxy_config
            .backend_retained_byte_limits
            .iter()
            .map(
                |(&backend_index, &retained_byte_limit)| NativeProxyBackendContract {
                    backend_index,
                    retained_byte_limit,
                },
            )
            .collect(),
    };

    let mut roles = Vec::with_capacity(input.cluster_size + 1);
    roles.push(normalize_role_config(
        RoleConfigInput {
            role: "fe",
            backend_index: None,
            config: input.frontend_config,
        },
        &input,
    )?);
    for (backend_index, config) in input.backend_configs.iter().enumerate() {
        roles.push(normalize_role_config(
            RoleConfigInput {
                role: "be",
                backend_index: Some(backend_index),
                config,
            },
            &input,
        )?);
    }

    let semantics = EffectiveLaunchConfigSemantics {
        launch_profile: match input.launch_profile {
            LaunchProfile::FaultScenario => "fault-scenario",
            LaunchProfile::Performance => "performance",
        },
        cluster_size: input.cluster_size,
        expected_eligible_backend_count: input.expected_eligible_backend_count,
        fixed_environment: BTreeMap::from([("NO_PROXY".to_string(), FIXED_NO_PROXY.to_string())]),
        child_environment,
        native_proxy,
        roles,
    };
    let artifact_bytes = serde_json::to_vec_pretty(&Artifact {
        schema_version: ARTIFACT_SCHEMA_VERSION,
        semantics: &semantics,
    })
    .context("serialize effective launch config evidence")?;
    let artifact_sha256 = sha256_bytes(&artifact_bytes);
    // Schema 1 is deliberately semantics-only. Keeping both API names makes
    // the distinction explicit for callers while the identical value lets an
    // independent extractor recompute it from the exact artifact bytes.
    let semantics_sha256 = artifact_sha256.clone();
    Ok(EffectiveLaunchConfigEvidence {
        artifact_bytes,
        artifact_sha256,
        semantics_sha256,
    })
}

struct RoleConfigInput<'a> {
    role: &'static str,
    backend_index: Option<usize>,
    config: &'a str,
}

fn normalize_role_config(
    role: RoleConfigInput<'_>,
    input: &EffectiveLaunchConfigInput<'_>,
) -> Result<RoleConfigSemantics> {
    let mut config = role
        .config
        .parse::<Value>()
        .with_context(|| format!("parse rendered {} effective config", role.role))?;
    let root = config
        .as_table_mut()
        .context("rendered effective config root must be a TOML table")?;
    let (http_port, grpc_port) = match role.backend_index {
        None => (input.runtime.fe_http_port, input.runtime.fe_grpc_port),
        Some(index) => (input.runtime.be[index].http, input.runtime.be[index].grpc),
    };
    replace_exact_integer(root, &["server", "http_port"], http_port, "$ROLE_HTTP")?;
    replace_exact_integer(root, &["server", "grpc_port"], grpc_port, "$ROLE_GRPC")?;

    match role.backend_index {
        None => {
            replace_exact_integer(
                root,
                &["standalone_server", "mysql_port"],
                input.runtime.fe_mysql_port,
                "$FE_MYSQL",
            )?;
            ensure!(
                get_path(root, &["cluster", "frontend_endpoint"]).is_none(),
                "effective FE config must not contain cluster.frontend_endpoint"
            );
            ensure!(
                get_path(root, &["cluster", "advertise_port"]).is_none(),
                "effective FE config must not contain cluster.advertise_port"
            );
            replace_exact_path(
                root,
                &["state_store", "path"],
                &input.runtime_dir.join("frontend-state.sqlite"),
                "$RUNTIME/frontend-state.sqlite",
            )?;
        }
        Some(index) => {
            ensure!(
                get_path(root, &["standalone_server", "mysql_port"]).is_none(),
                "effective BE config must not contain standalone_server.mysql_port"
            );
            replace_exact_string(
                root,
                &["cluster", "frontend_endpoint"],
                &format!("{}:{}", input.advertise_host, input.runtime.fe_grpc_port),
                "$FE_GRPC_ENDPOINT",
            )?;
            let proxy_limit = input
                .native_proxy_config
                .backend_retained_byte_limits
                .get(&index);
            match proxy_limit {
                Some(_) => replace_exact_integer(
                    root,
                    &["cluster", "advertise_port"],
                    input.advertised_backend_grpc_ports[index],
                    "$BE_ADVERTISE_GRPC",
                )?,
                None => ensure!(
                    get_path(root, &["cluster", "advertise_port"]).is_none()
                        && input.advertised_backend_grpc_ports[index]
                            == input.runtime.be[index].grpc,
                    "effective BE config unexpectedly changes its unproxied advertised endpoint"
                ),
            }
            ensure!(
                get_path(root, &["state_store"]).is_none(),
                "effective BE config must not contain state_store"
            );
        }
    }

    normalize_optional_runtime_path(
        root,
        &["debug", "cleanup_fault_dir"],
        input.runtime_dir,
        "$RUNTIME/connector-cleanup-faults",
    )?;
    for (name, token) in [
        (
            "certificate_chain_path",
            "$RUNTIME/native-trust-material/leaf.pem",
        ),
        (
            "private_key_path",
            "$RUNTIME/native-trust-material/leaf-key.pem",
        ),
        (
            "trust_roots_path",
            "$RUNTIME/native-trust-material/roots.pem",
        ),
    ] {
        normalize_optional_runtime_path(
            root,
            &["native_trust", "transport", name],
            input.runtime_dir,
            token,
        )?;
    }

    let mut attachments = Vec::new();
    if get_path(root, &["catalog_source", "mode"]).and_then(Value::as_str) == Some("static-file") {
        let path = get_path(root, &["catalog_source", "static_file_path"])
            .and_then(Value::as_str)
            .context("static-file catalog source requires static_file_path")?;
        let expected_path = input.runtime_dir.join("catalogs.toml");
        ensure!(
            path == "catalogs.toml" || Path::new(path) == expected_path,
            "effective static catalog path is not the harness-owned materialization"
        );
        let bytes = fs::read(&expected_path).with_context(|| {
            format!(
                "read materialized static catalog {}",
                expected_path.display()
            )
        })?;
        replace_string(
            root,
            &["catalog_source", "static_file_path"],
            "$STATIC_CATALOG",
        )?;
        attachments.push(ConfigAttachment {
            kind: "static-catalog",
            sha256: sha256_bytes(&bytes),
        });
    }

    Ok(RoleConfigSemantics {
        role: role.role,
        backend_index: role.backend_index,
        effective_config: canonical_toml(config),
        attachments,
    })
}

fn environment_contract(
    launch_profile: LaunchProfile,
    environment: &BTreeMap<String, String>,
) -> Result<Vec<EnvironmentBinding>> {
    environment
        .iter()
        .map(|(name, value)| {
            ensure!(
                !value.is_empty(),
                "child environment {name} must not be empty"
            );
            let classification = match launch_profile {
                LaunchProfile::Performance => classify_performance_environment(name)
                    .with_context(|| {
                        format!(
                            "performance child environment {name} has no typed allowlist classification"
                        )
                    })?,
                LaunchProfile::FaultScenario => EnvironmentClassification::NonFormalUnsealed,
            };
            Ok(EnvironmentBinding {
                name: name.clone(),
                classification,
                present: true,
            })
        })
        .collect()
}

fn classify_performance_environment(name: &str) -> Option<EnvironmentClassification> {
    FORMAL_SECRET_ENVIRONMENT
        .contains(&name)
        .then_some(EnvironmentClassification::SecretPresence)
}

fn replace_exact_integer(
    root: &mut toml::map::Map<String, Value>,
    path: &[&str],
    expected: u16,
    token: &str,
) -> Result<()> {
    let actual = get_path(root, path)
        .and_then(Value::as_integer)
        .with_context(|| format!("effective config {} is not an integer", path.join(".")))?;
    ensure!(
        actual == i64::from(expected),
        "effective config {} does not match its harness-owned port",
        path.join(".")
    );
    replace_string(root, path, token)
}

fn replace_exact_string(
    root: &mut toml::map::Map<String, Value>,
    path: &[&str],
    expected: &str,
    token: &str,
) -> Result<()> {
    let actual = get_path(root, path)
        .and_then(Value::as_str)
        .with_context(|| format!("effective config {} is not a string", path.join(".")))?;
    ensure!(
        actual == expected,
        "effective config {} does not match its harness-owned endpoint",
        path.join(".")
    );
    replace_string(root, path, token)
}

fn replace_exact_path(
    root: &mut toml::map::Map<String, Value>,
    path: &[&str],
    expected: &Path,
    token: &str,
) -> Result<()> {
    let actual = get_path(root, path)
        .and_then(Value::as_str)
        .with_context(|| format!("effective config {} is not a path", path.join(".")))?;
    ensure!(
        Path::new(actual) == expected,
        "effective config {} does not match its harness-owned path",
        path.join(".")
    );
    replace_string(root, path, token)
}

fn normalize_optional_runtime_path(
    root: &mut toml::map::Map<String, Value>,
    path: &[&str],
    runtime_dir: &Path,
    token: &str,
) -> Result<()> {
    let Some(actual) = get_path(root, path).and_then(Value::as_str) else {
        return Ok(());
    };
    let expected_suffix = token
        .strip_prefix("$RUNTIME/")
        .context("runtime path token must start with $RUNTIME/")?;
    ensure!(
        Path::new(actual) == runtime_dir.join(expected_suffix),
        "effective config {} is outside its exact harness-owned runtime path",
        path.join(".")
    );
    replace_string(root, path, token)
}

fn get_path<'a>(root: &'a toml::map::Map<String, Value>, path: &[&str]) -> Option<&'a Value> {
    let (last, parents) = path.split_last()?;
    let mut table = root;
    for component in parents {
        table = table.get(*component)?.as_table()?;
    }
    table.get(*last)
}

fn replace_string(
    root: &mut toml::map::Map<String, Value>,
    path: &[&str],
    replacement: &str,
) -> Result<()> {
    let (last, parents) = path
        .split_last()
        .context("effective config replacement path is empty")?;
    let mut table = root;
    for component in parents {
        table = table
            .get_mut(*component)
            .and_then(Value::as_table_mut)
            .with_context(|| format!("effective config {} is missing", path.join(".")))?;
    }
    let value = table
        .get_mut(*last)
        .with_context(|| format!("effective config {} is missing", path.join(".")))?;
    *value = Value::String(replacement.to_string());
    Ok(())
}

fn canonical_toml(value: Value) -> CanonicalToml {
    match value {
        Value::String(value) => CanonicalToml::String(value),
        Value::Integer(value) => CanonicalToml::Integer(value),
        Value::Float(value) => CanonicalToml::Float(value),
        Value::Boolean(value) => CanonicalToml::Boolean(value),
        Value::Datetime(value) => CanonicalToml::Datetime(value.to_string()),
        Value::Array(values) => {
            CanonicalToml::Array(values.into_iter().map(canonical_toml).collect())
        }
        Value::Table(values) => CanonicalToml::Table(
            values
                .into_iter()
                .map(|(name, value)| (name, canonical_toml(value)))
                .collect(),
        ),
    }
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BePorts, CrossProcessRuntime};
    use std::path::PathBuf;

    fn config(
        role: &str,
        http: u16,
        grpc: u16,
        mysql: Option<u16>,
        fe_grpc: u16,
        runtime: &Path,
        extra: &str,
        transport_mode: &str,
        advertised_port: Option<u16>,
    ) -> String {
        let mysql = mysql
            .map(|port| format!("mysql_port = {port}"))
            .unwrap_or_default();
        let role_fields = if role == "fe" {
            format!(
                "[state_store]\npath = {:?}\n",
                runtime.join("frontend-state.sqlite").to_string_lossy()
            )
        } else {
            let advertise = advertised_port
                .map(|port| format!("advertise_port = {port}\n"))
                .unwrap_or_default();
            format!("frontend_endpoint = \"127.0.0.1:{fe_grpc}\"\n{advertise}")
        };
        format!(
            "[server]\nhost = \"127.0.0.1\"\nhttp_port = {http}\ngrpc_port = {grpc}\n\
             [standalone_server]\n{mysql}\n\
             [cluster]\nrole = \"{role}\"\n{role_fields}\
             [runtime]\nexchange_wait_ms = 1000\n{extra}\n\
             [native_trust]\ndeployment_id = \"novarocks-system-tests\"\nshared_secret = \"${{ENV:NOVAROCKS_SYSTEM_NATIVE_TRUST_SECRET}}\"\n\
             [native_trust.transport]\nmode = \"{transport_mode}\"\n"
        )
    }

    fn evidence(
        root: &Path,
        ports: (u16, u16, u16, u16, u16),
        secret: &str,
        extra: &str,
        profile: LaunchProfile,
    ) -> Result<EffectiveLaunchConfigEvidence> {
        evidence_with_options(root, ports, secret, extra, profile, "disabled", None, None)
    }

    #[allow(clippy::too_many_arguments)]
    fn evidence_with_options(
        root: &Path,
        ports: (u16, u16, u16, u16, u16),
        secret: &str,
        extra: &str,
        profile: LaunchProfile,
        transport_mode: &str,
        additional_environment: Option<(&str, &str)>,
        proxy_limit: Option<u64>,
    ) -> Result<EffectiveLaunchConfigEvidence> {
        let runtime = CrossProcessRuntime {
            be: vec![BePorts {
                http: ports.3,
                grpc: ports.4,
            }],
            fe_http_port: ports.0,
            fe_grpc_port: ports.1,
            fe_mysql_port: ports.2,
        };
        let advertised_port = proxy_limit.map(|_| ports.4 + 1);
        let frontend = config(
            "fe",
            ports.0,
            ports.1,
            Some(ports.2),
            ports.1,
            root,
            extra,
            transport_mode,
            None,
        );
        let backend = config(
            "be",
            ports.3,
            ports.4,
            None,
            ports.1,
            root,
            extra,
            transport_mode,
            advertised_port,
        );
        let mut environments = BTreeMap::from([(
            "NOVAROCKS_SYSTEM_NATIVE_TRUST_SECRET".to_string(),
            secret.to_string(),
        )]);
        if let Some((name, value)) = additional_environment {
            environments.insert(name.to_string(), value.to_string());
        }
        let native_proxy_config = CrossProcessNativeFaultProxyConfig {
            backend_retained_byte_limits: proxy_limit
                .map(|limit| BTreeMap::from([(0, limit)]))
                .unwrap_or_default(),
        };
        build_effective_launch_config_evidence(EffectiveLaunchConfigInput {
            launch_profile: profile,
            cluster_size: 1,
            expected_eligible_backend_count: 1,
            runtime: &runtime,
            runtime_dir: root,
            advertise_host: "127.0.0.1",
            frontend_config: &frontend,
            backend_configs: &[backend],
            frontend_environment: &environments,
            backend_environments: &[environments.clone()],
            native_proxy_config: &native_proxy_config,
            advertised_backend_grpc_ports: &[advertised_port.unwrap_or(ports.4)],
        })
    }

    #[test]
    fn runtime_ports_paths_and_secret_values_do_not_change_semantics() {
        let first_root = PathBuf::from("/tmp/effective-config-first");
        let second_root = PathBuf::from("/tmp/effective-config-second");
        let first = evidence(
            &first_root,
            (1101, 1102, 1103, 1104, 1105),
            "secret-canary-first",
            "worker_threads = 4",
            LaunchProfile::Performance,
        )
        .unwrap();
        let second = evidence(
            &second_root,
            (2101, 2102, 2103, 2104, 2105),
            "secret-canary-second",
            "worker_threads = 4",
            LaunchProfile::Performance,
        )
        .unwrap();
        assert_eq!(first.semantics_sha256(), second.semantics_sha256());
        assert_eq!(
            first.artifact_sha256(),
            sha256_bytes(first.artifact_bytes())
        );
        assert_eq!(first.artifact_sha256(), first.semantics_sha256());
        let artifact = String::from_utf8(first.artifact_bytes().to_vec()).unwrap();
        assert!(!artifact.contains("secret-canary"));
        assert!(!artifact.contains(first_root.to_string_lossy().as_ref()));
    }

    #[test]
    fn runtime_and_profile_changes_change_semantics() {
        let root = PathBuf::from("/tmp/effective-config-stable");
        let base = evidence(
            &root,
            (1101, 1102, 1103, 1104, 1105),
            "secret-a",
            "worker_threads = 4",
            LaunchProfile::Performance,
        )
        .unwrap();
        let runtime_changed = evidence(
            &root,
            (1101, 1102, 1103, 1104, 1105),
            "secret-a",
            "worker_threads = 5",
            LaunchProfile::Performance,
        )
        .unwrap();
        let profile_changed = evidence(
            &root,
            (1101, 1102, 1103, 1104, 1105),
            "secret-a",
            "worker_threads = 4",
            LaunchProfile::FaultScenario,
        )
        .unwrap();
        let timeout_changed = evidence(
            &root,
            (1101, 1102, 1103, 1104, 1105),
            "secret-a",
            "worker_threads = 4\ncontrol_timeout_ms = 9000",
            LaunchProfile::Performance,
        )
        .unwrap();
        assert_ne!(base.semantics_sha256(), runtime_changed.semantics_sha256());
        assert_ne!(base.semantics_sha256(), profile_changed.semantics_sha256());
        assert_ne!(base.semantics_sha256(), timeout_changed.semantics_sha256());
    }

    #[test]
    fn tls_credential_environment_and_proxy_changes_change_semantics() {
        let root = PathBuf::from("/tmp/effective-config-contract");
        let ports = (1101, 1102, 1103, 1104, 1105);
        let base = evidence_with_options(
            &root,
            ports,
            "secret-a",
            "worker_threads = 4\n[connector.object_store]\ngeneration = \"v1\"",
            LaunchProfile::Performance,
            "disabled",
            None,
            None,
        )
        .unwrap();
        let tls = evidence_with_options(
            &root,
            ports,
            "secret-a",
            "worker_threads = 4\n[connector.object_store]\ngeneration = \"v1\"",
            LaunchProfile::Performance,
            "automatic",
            None,
            None,
        )
        .unwrap();
        let credential = evidence_with_options(
            &root,
            ports,
            "secret-a",
            "worker_threads = 4\n[connector.object_store]\ngeneration = \"v2\"",
            LaunchProfile::Performance,
            "disabled",
            None,
            None,
        )
        .unwrap();
        let environment = evidence_with_options(
            &root,
            ports,
            "secret-a",
            "worker_threads = 4\n[connector.object_store]\ngeneration = \"v1\"",
            LaunchProfile::Performance,
            "disabled",
            Some(("NOVAROCKS_PREPARATION_DIAGNOSTIC_SECRET", "secret-b")),
            None,
        )
        .unwrap();
        let proxy = evidence_with_options(
            &root,
            ports,
            "secret-a",
            "worker_threads = 4\n[connector.object_store]\ngeneration = \"v1\"",
            LaunchProfile::Performance,
            "disabled",
            None,
            Some(4096),
        )
        .unwrap();
        for changed in [&tls, &credential, &environment, &proxy] {
            assert_ne!(base.semantics_sha256(), changed.semantics_sha256());
        }
    }

    #[test]
    fn unknown_performance_environment_fails_closed() {
        let mut environment = BTreeMap::new();
        environment.insert("RUST_LOG".to_string(), "trace".to_string());
        let error = environment_contract(LaunchProfile::Performance, &environment).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("no typed allowlist classification")
        );
    }

    #[test]
    fn mismatched_harness_port_fails_closed() {
        let mut value = "[server]\nhttp_port = 1200".parse::<Value>().unwrap();
        let root = value.as_table_mut().unwrap();
        let error =
            replace_exact_integer(root, &["server", "http_port"], 1201, "$ROLE_HTTP").unwrap_err();
        assert!(error.to_string().contains("harness-owned port"));
    }

    #[test]
    fn static_catalog_attachment_binds_content_not_runtime_path() {
        let runtime_dir = std::env::temp_dir().join(format!(
            "novarocks-effective-config-static-{}",
            std::process::id()
        ));
        fs::create_dir_all(&runtime_dir).unwrap();
        let catalog_path = runtime_dir.join("catalogs.toml");
        fs::write(&catalog_path, "format_version = 1\ncatalogs = []\n").unwrap();
        let runtime = CrossProcessRuntime {
            be: vec![BePorts {
                http: 1104,
                grpc: 1105,
            }],
            fe_http_port: 1101,
            fe_grpc_port: 1102,
            fe_mysql_port: 1103,
        };
        let frontend = format!(
            "{}\n[catalog_source]\nmode = \"static-file\"\nstatic_file_path = \"catalogs.toml\"\n",
            config(
                "fe",
                1101,
                1102,
                Some(1103),
                1102,
                &runtime_dir,
                "worker_threads = 4",
                "disabled",
                None,
            )
        );
        let environments = BTreeMap::from([(
            "NOVAROCKS_SYSTEM_NATIVE_TRUST_SECRET".to_string(),
            "secret".to_string(),
        )]);
        let backend_configs = vec![config(
            "be",
            1104,
            1105,
            None,
            1102,
            &runtime_dir,
            "worker_threads = 4",
            "disabled",
            None,
        )];
        let proxy = CrossProcessNativeFaultProxyConfig::default();
        let input = EffectiveLaunchConfigInput {
            launch_profile: LaunchProfile::Performance,
            cluster_size: 1,
            expected_eligible_backend_count: 1,
            runtime: &runtime,
            runtime_dir: &runtime_dir,
            advertise_host: "127.0.0.1",
            frontend_config: &frontend,
            backend_configs: &backend_configs,
            frontend_environment: &environments,
            backend_environments: &[environments.clone()],
            native_proxy_config: &proxy,
            advertised_backend_grpc_ports: &[1105],
        };
        let first = normalize_role_config(
            RoleConfigInput {
                role: "fe",
                backend_index: None,
                config: &frontend,
            },
            &input,
        )
        .unwrap();
        fs::write(
            &catalog_path,
            "format_version = 1\n[[catalogs]]\nname = \"changed\"\n",
        )
        .unwrap();
        let second = normalize_role_config(
            RoleConfigInput {
                role: "fe",
                backend_index: None,
                config: &frontend,
            },
            &input,
        )
        .unwrap();
        assert_ne!(
            serde_json::to_vec(&first).unwrap(),
            serde_json::to_vec(&second).unwrap()
        );
        fs::remove_dir_all(runtime_dir).unwrap();
    }
}
