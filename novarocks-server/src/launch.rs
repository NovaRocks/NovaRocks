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

//! Pure server launch resolution. This module performs no logging, runtime,
//! StateStore, directory, or listener side effect.

use std::net::{SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use novarocks_types::ClusterRole;

use crate::{
    app_config::NovaRocksConfig,
    native_trust::{
        NativeTrustSnapshot, build_role_native_trust_snapshot, ensure_all_in_one_trust_homogeneous,
    },
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServerLaunchMode {
    Fe,
    Be,
    AllInOne,
}

impl ServerLaunchMode {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "fe" => Ok(Self::Fe),
            "be" => Ok(Self::Be),
            "all-in-one" => Ok(Self::AllInOne),
            other => Err(format!(
                "invalid --role value `{other}`; expected one of: fe, be, all-in-one"
            )),
        }
    }

    fn expected_cluster_role(self) -> Option<ClusterRole> {
        match self {
            Self::Fe => Some(ClusterRole::Fe),
            Self::Be => Some(ClusterRole::Be),
            Self::AllInOne => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StandaloneLaunchArgs {
    pub mode: ServerLaunchMode,
    pub config_path: Option<PathBuf>,
    pub fe_config_path: Option<PathBuf>,
    pub be_config_path: Option<PathBuf>,
}

pub fn parse_standalone_launch_args(
    args: &[String],
) -> Result<Option<StandaloneLaunchArgs>, String> {
    let mut index = 0;
    let mut mode = None;
    let mut config_path = None;
    let mut fe_config_path = None;
    let mut be_config_path = None;

    while let Some(arg) = args.get(index) {
        match arg.as_str() {
            "--role" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "missing value for --role".to_string())?;
                if mode.replace(ServerLaunchMode::parse(value)?).is_some() {
                    return Err("--role may only be supplied once".to_string());
                }
            }
            "--config" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "missing value for --config".to_string())?;
                if config_path.replace(PathBuf::from(value)).is_some() {
                    return Err("--config may only be supplied once".to_string());
                }
            }
            "--fe-config" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "missing value for --fe-config".to_string())?;
                if fe_config_path.replace(PathBuf::from(value)).is_some() {
                    return Err("--fe-config may only be supplied once".to_string());
                }
            }
            "--be-config" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "missing value for --be-config".to_string())?;
                if be_config_path.replace(PathBuf::from(value)).is_some() {
                    return Err("--be-config may only be supplied once".to_string());
                }
            }
            "--help" | "-h" => return Ok(None),
            "--port" | "-c" => {
                return Err(format!(
                    "{arg} is not supported; use the explicit NWT-2 standalone command shape"
                ));
            }
            other => return Err(format!("unknown standalone arg: {other}")),
        }
        index += 1;
    }

    let mode = mode.ok_or_else(|| "missing required --role <fe|be|all-in-one>".to_string())?;
    match mode {
        ServerLaunchMode::Fe | ServerLaunchMode::Be => {
            if config_path.is_none() {
                return Err("role=fe|be requires --config <path>".to_string());
            }
            if fe_config_path.is_some() || be_config_path.is_some() {
                return Err("role=fe|be accepts only --config <path>".to_string());
            }
        }
        ServerLaunchMode::AllInOne => {
            if config_path.is_some() {
                return Err(
                    "role=all-in-one does not accept --config; use --fe-config and --be-config"
                        .to_string(),
                );
            }
            if fe_config_path.is_none() || be_config_path.is_none() {
                return Err(
                    "role=all-in-one requires both --fe-config <path> and --be-config <path>"
                        .to_string(),
                );
            }
        }
    }

    Ok(Some(StandaloneLaunchArgs {
        mode,
        config_path,
        fe_config_path,
        be_config_path,
    }))
}

#[derive(Clone)]
pub struct RoleConfig {
    pub role: ClusterRole,
    pub path: PathBuf,
    pub config: NovaRocksConfig,
    pub native_trust: NativeTrustSnapshot,
    endpoints: Vec<BindEndpoint>,
    process: ProcessCompatibilityProjection,
}

#[derive(Clone)]
pub enum ResolvedServerLaunch {
    Fe(RoleConfig),
    Be(RoleConfig),
    AllInOne { fe: RoleConfig, be: RoleConfig },
}

pub fn resolve_server_launch(args: StandaloneLaunchArgs) -> Result<ResolvedServerLaunch> {
    // Design: ADR-0108 (docs/adr/ADR-0108-native-role-launch-and-management-surfaces.md)
    match args.mode {
        ServerLaunchMode::Fe => Ok(ResolvedServerLaunch::Fe(load_role_config(
            ServerLaunchMode::Fe,
            args.config_path.expect("validated --config"),
        )?)),
        ServerLaunchMode::Be => Ok(ResolvedServerLaunch::Be(load_role_config(
            ServerLaunchMode::Be,
            args.config_path.expect("validated --config"),
        )?)),
        ServerLaunchMode::AllInOne => {
            let fe = load_role_config(
                ServerLaunchMode::Fe,
                args.fe_config_path.expect("validated --fe-config"),
            )?;
            let be = load_role_config(
                ServerLaunchMode::Be,
                args.be_config_path.expect("validated --be-config"),
            )?;
            ensure_no_endpoint_overlap(&fe.endpoints, &be.endpoints)?;
            if fe.process != be.process {
                bail!(
                    "all-in-one process configuration mismatch between {} and {}: logging and data runtime sizing must match",
                    fe.path.display(),
                    be.path.display()
                );
            }
            ensure_all_in_one_trust_homogeneous(&fe.config, &be.config)?;
            Ok(ResolvedServerLaunch::AllInOne { fe, be })
        }
    }
}

fn load_role_config(mode: ServerLaunchMode, path: PathBuf) -> Result<RoleConfig> {
    let expected = mode.expected_cluster_role().expect("role config mode");
    let config = NovaRocksConfig::load_deployable_from_file(&path)
        .with_context(|| format!("load {mode:?} config {}", path.display()))?;
    if config.cluster.role != expected {
        bail!(
            "{mode:?} launch requires [cluster].role={}, but {} declares {}",
            role_name(expected),
            path.display(),
            role_name(config.cluster.role)
        );
    }
    let endpoints = role_bind_endpoints(expected, &config, &path)?;
    ensure_no_endpoint_overlap(&endpoints, &[])?;
    let native_trust = build_role_native_trust_snapshot(expected, &config)
        .with_context(|| format!("construct {mode:?} native trust {}", path.display()))?;
    Ok(RoleConfig {
        role: expected,
        path,
        process: ProcessCompatibilityProjection::from_config(&config),
        config,
        native_trust,
        endpoints,
    })
}

fn role_name(role: ClusterRole) -> &'static str {
    match role {
        ClusterRole::Fe => "fe",
        ClusterRole::Be => "be",
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProcessCompatibilityProjection {
    log_filter: String,
    log_dir: String,
    log_roll_mode: String,
    log_roll_num: usize,
    data_runtime_worker_threads: usize,
    data_runtime_max_blocking_threads: usize,
}

impl ProcessCompatibilityProjection {
    fn from_config(config: &NovaRocksConfig) -> Self {
        let log_filter =
            config
                .log_filter
                .clone()
                .unwrap_or_else(|| match config.log_level.as_str() {
                    "debug" => "info,novarocks=debug".to_string(),
                    "trace" => "info,novarocks=trace".to_string(),
                    other => other.to_string(),
                });
        Self {
            log_filter,
            log_dir: config.sys_log_dir.clone(),
            log_roll_mode: config.sys_log_roll_mode.clone(),
            log_roll_num: config.sys_log_roll_num,
            data_runtime_worker_threads: config.runtime.actual_data_runtime_threads().max(1),
            data_runtime_max_blocking_threads: config
                .runtime
                .data_runtime_max_blocking_threads
                .max(1),
        }
    }
}

#[derive(Clone, Debug)]
struct BindEndpoint {
    role: ClusterRole,
    kind: &'static str,
    addresses: Vec<SocketAddr>,
}

fn role_bind_endpoints(
    role: ClusterRole,
    config: &NovaRocksConfig,
    path: &Path,
) -> Result<Vec<BindEndpoint>> {
    if config.server.grpc_port == config.server.http_port {
        bail!(
            "{} config {}: server.grpc_port and server.http_port must differ (both {})",
            role_name(role),
            path.display(),
            config.server.grpc_port
        );
    }
    let mut endpoints = vec![
        resolve_endpoint(
            role,
            "native gRPC",
            &config.server.host,
            config.server.grpc_port,
            path,
        )?,
        resolve_endpoint(
            role,
            "management HTTP",
            &config.server.host,
            config.server.http_port,
            path,
        )?,
    ];
    if role == ClusterRole::Fe {
        let mysql_port = config
            .standalone_server
            .as_ref()
            .map(|server| server.mysql_port)
            .unwrap_or_else(|| crate::app_config::StandaloneServerConfig::default().mysql_port);
        endpoints.push(resolve_endpoint(
            role,
            "MySQL",
            &config.server.host,
            mysql_port,
            path,
        )?);
    }
    Ok(endpoints)
}

fn resolve_endpoint(
    role: ClusterRole,
    kind: &'static str,
    host: &str,
    port: u16,
    path: &Path,
) -> Result<BindEndpoint> {
    let input = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    let addresses = input
        .to_socket_addrs()
        .with_context(|| {
            format!(
                "{} config {}: resolve {kind} bind endpoint {input}",
                role_name(role),
                path.display()
            )
        })?
        .collect::<Vec<_>>();
    if addresses.is_empty() {
        bail!(
            "{} config {}: resolve {kind} bind endpoint {input} returned no addresses",
            role_name(role),
            path.display()
        );
    }
    Ok(BindEndpoint {
        role,
        kind,
        addresses,
    })
}

fn ensure_no_endpoint_overlap(left: &[BindEndpoint], right: &[BindEndpoint]) -> Result<()> {
    let mut all = Vec::with_capacity(left.len() + right.len());
    all.extend(left);
    all.extend(right);
    for (index, first) in all.iter().enumerate() {
        for second in all.iter().skip(index + 1) {
            if endpoints_overlap(first, second) {
                bail!(
                    "bind endpoint conflict: {} {} overlaps {} {}",
                    role_name(first.role),
                    first.kind,
                    role_name(second.role),
                    second.kind
                );
            }
        }
    }
    Ok(())
}

fn endpoints_overlap(first: &BindEndpoint, second: &BindEndpoint) -> bool {
    first.addresses.iter().any(|left| {
        second.addresses.iter().any(|right| {
            left.port() == right.port()
                && left.is_ipv4() == right.is_ipv4()
                && (left.ip().is_unspecified()
                    || right.ip().is_unspecified()
                    || left.ip() == right.ip())
        })
    })
}

#[cfg(test)]
mod tests {
    use super::{ResolvedServerLaunch, parse_standalone_launch_args, resolve_server_launch};
    use std::path::Path;

    fn write_config(
        path: &Path,
        role: &str,
        host: &str,
        grpc: u16,
        http: u16,
        mysql: Option<u16>,
        extra: &str,
    ) {
        let mysql = mysql
            .map(|port| format!("\n[standalone_server]\nmysql_port = {port}\n"))
            .unwrap_or_default();
        let frontend_endpoint = (role == "be")
            .then_some("frontend_endpoint = \"127.0.0.1:19080\"\n")
            .unwrap_or_default();
        let catalog_source = if role == "fe" {
            let catalogs = path.with_extension("catalogs.toml");
            std::fs::write(&catalogs, "format_version = 1\ncatalogs = []\n")
                .expect("write catalog snapshot");
            format!(
                "\n[catalog_source]\nstatic_file_path = \"{}\"\n",
                catalogs
                    .file_name()
                    .expect("catalog file name")
                    .to_string_lossy()
            )
        } else {
            String::new()
        };
        std::fs::write(path, format!("{extra}\n[native_trust]\ndeployment_id = \"test-deployment\"\nshared_secret = \"0123456789abcdef0123456789abcdef\"\n\n[server]\nhost = \"{host}\"\ngrpc_port = {grpc}\nhttp_port = {http}\n\n[cluster]\nrole = \"{role}\"\n{frontend_endpoint}{mysql}{catalog_source}")).expect("write config");
    }

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(ToString::to_string).collect()
    }

    fn launch_error(args: super::StandaloneLaunchArgs) -> String {
        match resolve_server_launch(args) {
            Ok(_) => panic!("launch unexpectedly resolved"),
            Err(error) => format!("{error:#}"),
        }
    }

    #[test]
    fn exact_cli_shapes_are_enforced() {
        assert!(
            parse_standalone_launch_args(&args(&["--role", "fe", "--config", "fe.toml"])).is_ok()
        );
        assert!(
            parse_standalone_launch_args(&args(&[
                "--role",
                "all-in-one",
                "--fe-config",
                "fe.toml",
                "--be-config",
                "be.toml"
            ]))
            .is_ok()
        );
        for invalid in [
            args(&[]),
            args(&["--role", "fe"]),
            args(&["--role", "all-in-one", "--config", "one.toml"]),
            args(&["--role", "all-in-one", "--fe-config", "fe.toml"]),
            args(&["--role", "be", "--config", "be.toml", "--port", "9030"]),
            args(&["--role", "fe", "-c", "fe.toml"]),
        ] {
            assert!(
                parse_standalone_launch_args(&invalid).is_err(),
                "{invalid:?} must fail"
            );
        }
    }

    #[test]
    fn deployable_role_is_explicit_and_exact() {
        let fixture = tempfile::tempdir().expect("tempdir");
        let missing = fixture.path().join("missing.toml");
        std::fs::write(&missing, "[server]\nhost = \"127.0.0.1\"\n").expect("write");
        let parsed = parse_standalone_launch_args(&args(&[
            "--role",
            "fe",
            "--config",
            missing.to_str().unwrap(),
        ]))
        .unwrap()
        .unwrap();
        assert!(launch_error(parsed).contains("missing required [cluster]"));

        let missing_role = fixture.path().join("missing-role.toml");
        std::fs::write(
            &missing_role,
            "[server]\nhost = \"127.0.0.1\"\n\n[cluster]\n",
        )
        .expect("write");
        let parsed = parse_standalone_launch_args(&args(&[
            "--role",
            "fe",
            "--config",
            missing_role.to_str().unwrap(),
        ]))
        .unwrap()
        .unwrap();
        assert!(launch_error(parsed).contains("missing required [cluster].role"));

        let all = fixture.path().join("all.toml");
        write_config(
            &all,
            "all-in-one",
            "127.0.0.1",
            19080,
            18040,
            Some(19030),
            "",
        );
        let parsed = parse_standalone_launch_args(&args(&[
            "--role",
            "fe",
            "--config",
            all.to_str().unwrap(),
        ]))
        .unwrap()
        .unwrap();
        assert!(launch_error(parsed).contains("must be `fe` or `be`"));

        let be = fixture.path().join("be.toml");
        write_config(&be, "be", "127.0.0.1", 19081, 18041, None, "");
        let parsed = parse_standalone_launch_args(&args(&[
            "--role",
            "fe",
            "--config",
            be.to_str().unwrap(),
        ]))
        .unwrap()
        .unwrap();
        assert!(launch_error(parsed).contains("requires [cluster].role=fe"));
    }

    #[test]
    fn endpoint_conflicts_fail_before_runtime_setup() {
        let fixture = tempfile::tempdir().expect("tempdir");
        let single = fixture.path().join("single.toml");
        write_config(&single, "fe", "127.0.0.1", 19080, 19080, Some(19030), "");
        let parsed = parse_standalone_launch_args(&args(&[
            "--role",
            "fe",
            "--config",
            single.to_str().unwrap(),
        ]))
        .unwrap()
        .unwrap();
        assert!(launch_error(parsed).contains("must differ"));

        let fe = fixture.path().join("fe.toml");
        let be = fixture.path().join("be.toml");
        write_config(&fe, "fe", "0.0.0.0", 19080, 18040, Some(19030), "");
        write_config(&be, "be", "127.0.0.1", 19080, 18041, None, "");
        let parsed = parse_standalone_launch_args(&args(&[
            "--role",
            "all-in-one",
            "--fe-config",
            fe.to_str().unwrap(),
            "--be-config",
            be.to_str().unwrap(),
        ]))
        .unwrap()
        .unwrap();
        assert!(launch_error(parsed).contains("bind endpoint conflict"));
    }

    #[test]
    fn process_mismatch_and_preflight_failure_leave_no_side_effects() {
        let fixture = tempfile::tempdir().expect("tempdir");
        let log_dir = fixture.path().join("must-not-exist");
        let state_store = fixture.path().join("must-not-exist.sqlite");
        let fe = fixture.path().join("fe.toml");
        let be = fixture.path().join("be.toml");
        write_config(
            &fe,
            "fe",
            "127.0.0.1",
            19080,
            18040,
            Some(19030),
            &format!(
                "sys_log_dir = \"{}\"\n\n[state_store]\nprovider = \"sqlite\"\ncluster_id = \"test\"\npath = \"{}\"\n",
                log_dir.display(),
                state_store.display()
            ),
        );
        write_config(
            &be,
            "be",
            "127.0.0.1",
            19081,
            18041,
            None,
            "log_level = \"debug\"\n",
        );
        let parsed = parse_standalone_launch_args(&args(&[
            "--role",
            "all-in-one",
            "--fe-config",
            fe.to_str().unwrap(),
            "--be-config",
            be.to_str().unwrap(),
        ]))
        .unwrap()
        .unwrap();
        assert!(launch_error(parsed).contains("process configuration mismatch"));
        assert!(!log_dir.exists());
        assert!(!state_store.exists());
    }

    #[test]
    fn accepts_a_disjoint_role_pair() {
        let fixture = tempfile::tempdir().expect("tempdir");
        let fe = fixture.path().join("fe.toml");
        let be = fixture.path().join("be.toml");
        write_config(&fe, "fe", "127.0.0.1", 19080, 18040, Some(19030), "");
        write_config(&be, "be", "127.0.0.1", 19081, 18041, None, "");
        let parsed = parse_standalone_launch_args(&args(&[
            "--role",
            "all-in-one",
            "--fe-config",
            fe.to_str().unwrap(),
            "--be-config",
            be.to_str().unwrap(),
        ]))
        .unwrap()
        .unwrap();
        assert!(matches!(
            resolve_server_launch(parsed).expect("resolve"),
            ResolvedServerLaunch::AllInOne { .. }
        ));
    }
}
