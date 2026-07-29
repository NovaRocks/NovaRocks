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

use crate::compat_artifact::CompatArtifact;
use crate::managed_process::{ManagedProcess, ReadyMarker};
use crate::starrocks_compat_cluster::StarRocksCompatServerHandle;
use crate::types::{CompatBeEndpoint, RunnerConfig};
use anyhow::{Context, Result, bail};
use clap::ValueEnum;
use mysql::prelude::Queryable;
use mysql::{Conn as MysqlConn, OptsBuilder};
use std::fs;
use std::io::Write;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use toml::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum ClusterMode {
    AllInOne,
    CrossProcess,
    #[value(skip)]
    StarRocksCompat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClusterProcessRole {
    Fe,
    Be,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BePorts {
    pub(crate) http: u16,
    pub(crate) grpc: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CrossProcessRuntime {
    pub(crate) be: Vec<BePorts>,
    pub(crate) fe_http_port: u16,
    pub(crate) fe_grpc_port: u16,
    pub(crate) fe_mysql_port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BackendTopologyRow {
    grpc_port: u16,
    state: String,
    alive: bool,
    scheduled_fragments: u64,
}

fn parse_frontend_show_backends_values(values: &[String]) -> Result<BackendTopologyRow> {
    let grpc_port = values
        .get(2)
        .context("SHOW BACKENDS row missing GrpcPort")?
        .parse::<u16>()
        .context("parse SHOW BACKENDS GrpcPort")?;
    let state = values
        .get(3)
        .context("SHOW BACKENDS row missing State")?
        .clone();
    let alive = state.eq_ignore_ascii_case("Live");
    let scheduled_fragments = values
        .get(4)
        .context("SHOW BACKENDS row missing ScheduledFragments")?
        .parse::<u64>()
        .context("parse SHOW BACKENDS ScheduledFragments")?;
    Ok(BackendTopologyRow {
        grpc_port,
        state,
        alive,
        scheduled_fragments,
    })
}

fn query_frontend_backend_topology(
    mysql_user: &str,
    host: &str,
    port: u16,
    io_timeout: Duration,
) -> Result<Vec<BackendTopologyRow>> {
    let builder = OptsBuilder::new()
        .ip_or_hostname(Some(host))
        .tcp_port(port)
        .prefer_socket(false)
        .user(Some(mysql_user))
        .tcp_connect_timeout(Some(io_timeout))
        .read_timeout(Some(io_timeout))
        .write_timeout(Some(io_timeout));
    let mut conn = MysqlConn::new(builder)
        .with_context(|| format!("connect to cross-process FE MySQL at {host}:{port}"))?;
    let rows: Vec<mysql::Row> = conn
        .query("SHOW BACKENDS")
        .context("query SHOW BACKENDS from cross-process FE")?;
    rows.into_iter()
        .map(|row| {
            let values = (0..5)
                .map(|index| {
                    row.get::<String, usize>(index)
                        .with_context(|| format!("SHOW BACKENDS row missing column {index}"))
                })
                .collect::<Result<Vec<_>>>()?;
            parse_frontend_show_backends_values(&values)
        })
        .collect()
}

const BACKEND_TOPOLOGY_TIMEOUT_CAP: Duration = Duration::from_secs(120);
const TOPOLOGY_MYSQL_IO_TIMEOUT_CAP: Duration = Duration::from_secs(2);
const TOPOLOGY_MYSQL_IO_TIMEOUT_MIN: Duration = Duration::from_millis(1);

fn bounded_backend_topology_timeout(requested: Duration) -> Duration {
    requested.min(BACKEND_TOPOLOGY_TIMEOUT_CAP)
}

fn backend_topology_deadline(now: Instant, requested: Duration) -> Instant {
    now.checked_add(bounded_backend_topology_timeout(requested))
        .unwrap_or(now)
}

fn topology_mysql_io_timeout(remaining: Duration) -> Duration {
    remaining
        .min(TOPOLOGY_MYSQL_IO_TIMEOUT_CAP)
        .max(TOPOLOGY_MYSQL_IO_TIMEOUT_MIN)
}

fn validate_live_backend_topology(
    expected_ports: &[u16],
    rows: &[BackendTopologyRow],
) -> Result<()> {
    let expected = expected_ports.len();
    let live = rows
        .iter()
        .filter(|row| row.state == "Live" && row.alive)
        .count();
    let mut configured_ports = expected_ports.to_vec();
    configured_ports.sort_unstable();
    let mut observed_ports = rows.iter().map(|row| row.grpc_port).collect::<Vec<_>>();
    observed_ports.sort_unstable();
    if rows.len() == expected && live == expected && observed_ports == configured_ports {
        return Ok(());
    }

    let observed = rows
        .iter()
        .map(|row| format!("{}:{}:{}", row.grpc_port, row.state, row.alive))
        .collect::<Vec<_>>()
        .join(",");
    bail!(
        "SHOW BACKENDS topology is not ready: registered={} expected={}; live={} expected={}; configured_ports={configured_ports:?} observed_ports={observed_ports:?}; rows=[{}]",
        rows.len(),
        expected,
        live,
        expected,
        observed
    )
}

fn wait_for_live_backend_topology_with<Q, S, H>(
    expected_ports: &[u16],
    timeout: Duration,
    mut process_health: H,
    mut query: Q,
    mut sleep: S,
) -> Result<Vec<BackendTopologyRow>>
where
    Q: FnMut(Duration) -> Result<Vec<BackendTopologyRow>>,
    S: FnMut(Duration),
    H: FnMut() -> Result<String>,
{
    let expected = expected_ports.len();
    let deadline = backend_topology_deadline(Instant::now(), timeout);
    loop {
        process_health()
            .context("cross-process FE/BE exited before SHOW BACKENDS topology became ready")?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        let io_timeout = topology_mysql_io_timeout(remaining);
        let last_observation = match query(io_timeout) {
            Ok(rows) => match validate_live_backend_topology(expected_ports, &rows) {
                Ok(()) => return Ok(rows),
                Err(error) => error.to_string(),
            },
            Err(error) => format!("SHOW BACKENDS query failed: {error:#}"),
        };

        if Instant::now() >= deadline {
            let process_diagnostics = process_health()
                .context("cross-process FE/BE exited during the bounded SHOW BACKENDS query")?;
            bail!(
                "timed out waiting for SHOW BACKENDS {expected}/{expected} Live; last_observation={last_observation}; {}",
                process_diagnostics
            );
        }
        sleep(
            deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(100)),
        );
    }
}

fn wait_for_live_backend_topology(
    mysql_user: &str,
    runtime: &CrossProcessRuntime,
    fe_config_path: &Path,
    be_config_paths: &[PathBuf],
    fe_process: &mut ManagedProcess,
    be_processes: &mut [ManagedProcess],
) -> Result<()> {
    let expected_ports = runtime.be.iter().map(|be| be.grpc).collect::<Vec<_>>();
    let expected = expected_ports.len();
    let host = "127.0.0.1";
    let port = runtime.fe_mysql_port;
    let rows = wait_for_live_backend_topology_with(
        &expected_ports,
        startup_timeout(),
        || {
            process_runtime_diagnostics(
                fe_process,
                be_processes,
                fe_config_path,
                be_config_paths,
                runtime,
            )
        },
        |io_timeout| query_frontend_backend_topology(mysql_user, host, port, io_timeout),
        thread::sleep,
    )?;
    let diagnostics = process_runtime_diagnostics(
        fe_process,
        be_processes,
        fe_config_path,
        be_config_paths,
        runtime,
    )?;
    println!(
        "cross-process topology barrier PASS: SHOW BACKENDS {}/{} Live; {}",
        rows.len(),
        expected,
        diagnostics
    );
    Ok(())
}

pub(crate) trait ServerHandle: Send {
    fn target_host(&self) -> Option<&str>;
    fn target_port(&self) -> Option<u16>;
    fn supports_fault_injection(&self) -> bool {
        false
    }
    fn kill_be(&mut self, index: usize) -> Result<()> {
        bail!("BE kill is unsupported by this server mode (index={index})")
    }
    fn restart_be(&mut self, index: usize) -> Result<()> {
        bail!("BE restart is unsupported by this server mode (index={index})")
    }
    fn be_count(&self) -> usize {
        self.be_endpoints().len()
    }
    fn scheduled_fragment_count(&self, index: usize) -> Result<u64> {
        bail!("scheduled fragment telemetry is unsupported by this server mode (index={index})")
    }
    fn arm_fragment_executor_failure(&mut self, index: usize) -> Result<()> {
        bail!(
            "fragment executor failure injection is unsupported by this server mode (index={index})"
        )
    }
    fn release_fragment_executor_failure(&mut self, index: usize) -> Result<()> {
        bail!(
            "fragment executor failure release is unsupported by this server mode (index={index})"
        )
    }
    fn disarm_fragment_executor_failure(&mut self, index: usize) -> Result<()> {
        bail!(
            "fragment executor failure cleanup is unsupported by this server mode (index={index})"
        )
    }
    fn armed_fragment_failure_token(&self, index: usize) -> Result<Option<String>> {
        bail!("fragment failure token is unsupported by this server mode (index={index})")
    }
    #[allow(dead_code)]
    fn be_endpoints(&self) -> &[CompatBeEndpoint] {
        &[]
    }
    #[allow(dead_code)]
    fn assert_be_log(&self, index: usize, _needle: &str) -> Result<()> {
        bail!("BE log assertions are unsupported by this server mode (index={index})")
    }
    #[allow(dead_code)]
    fn be_log_count(&self, index: usize, needle: &str) -> Result<usize> {
        bail!(
            "BE log counting is unsupported by this server mode (index={index}, pattern={needle:?})"
        )
    }
    #[allow(dead_code)]
    fn be_log_contents(&self, index: usize) -> Result<String> {
        bail!("BE log reading is unsupported by this server mode (index={index})")
    }
    #[allow(dead_code)]
    fn run_compat_probe(&self, probe: &str, _endpoint: &CompatBeEndpoint) -> Result<()> {
        bail!("compatibility probes are unsupported by this server mode (probe={probe})")
    }
    fn residual_process_ids(&self) -> Vec<u32> {
        Vec::new()
    }
    fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}

pub(crate) fn launch_server(
    mode: ClusterMode,
    cluster_size: usize,
    repo_root: &Path,
    runner_config: &RunnerConfig,
    compat_artifact: Option<CompatArtifact>,
) -> Result<Box<dyn ServerHandle>> {
    match mode {
        ClusterMode::AllInOne => Ok(Box::new(NoopServerHandle)),
        ClusterMode::CrossProcess => Ok(Box::new(CrossProcessServerHandle::launch(
            cluster_size,
            repo_root,
            runner_config,
        )?)),
        ClusterMode::StarRocksCompat => {
            let artifact = compat_artifact.context(
                "StarRocks compatibility cluster requires a proven compatibility artifact",
            )?;
            Ok(Box::new(StarRocksCompatServerHandle::launch(
                repo_root,
                runner_config,
                artifact,
            )?))
        }
    }
}

/// Validate cluster CLI arguments.  Returns an error string on failure.
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
    if mode == ClusterMode::StarRocksCompat && cluster_size != 3 {
        bail!(
            "starrocks-compat mode requires --cluster-size 3 (got {})",
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
        let path = PathBuf::from(path);
        if path.is_file() {
            return Ok(path);
        }
        bail!(
            "NOVAROCKS_BIN points to {}, but the file does not exist",
            path.display()
        );
    }

    for candidate in [
        repo_root.join("target/debug/novarocks"),
        repo_root.join("target/release/novarocks"),
    ] {
        if candidate.is_file() {
            return Ok(candidate);
        }
    }

    bail!(
        "failed to locate novarocks binary; set NOVAROCKS_BIN or run `cargo build --quiet` from {}",
        repo_root.display()
    )
}

pub(crate) fn resolve_base_app_config_path(
    repo_root: &Path,
    runner_config: &RunnerConfig,
) -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("NOVAROCKS_STANDALONE_CONFIG") {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Ok(path);
        }
        bail!(
            "NOVAROCKS_STANDALONE_CONFIG points to {}, but the file does not exist",
            path.display()
        );
    }

    if let Some(path) = runner_config.path.as_ref() {
        let sibling = path.with_extension("toml");
        if sibling.is_file() {
            return Ok(sibling);
        }
    }

    let fallback = repo_root.join("tests/sql-test-runner/conf/standalone_managed_lake.toml");
    if fallback.is_file() {
        return Ok(fallback);
    }

    bail!("failed to locate standalone config for cross-process mode")
}

/// Render the per-process TOML config for cross-process mode.
///
/// `be_index` is used when `role == Be` to select which BE's ports to use.
/// It is ignored for `role == Fe`.
pub(crate) fn render_cross_process_config(
    base_config: &str,
    role: ClusterProcessRole,
    be_index: usize,
    runtime: &CrossProcessRuntime,
) -> Result<String> {
    let mut value = if base_config.trim().is_empty() {
        Value::Table(Default::default())
    } else {
        base_config
            .parse::<Value>()
            .context("parse standalone config for cross-process mode")?
    };
    let root = value
        .as_table_mut()
        .ok_or_else(|| anyhow::anyhow!("standalone config root must be a TOML table"))?;

    let server = table_mut(root, "server");
    server.insert("host".to_string(), Value::String("127.0.0.1".to_string()));
    match role {
        ClusterProcessRole::Fe => {
            server.insert(
                "http_port".to_string(),
                Value::Integer(i64::from(runtime.fe_http_port)),
            );
            server.insert(
                "grpc_port".to_string(),
                Value::Integer(i64::from(runtime.fe_grpc_port)),
            );
        }
        ClusterProcessRole::Be => {
            let be = &runtime.be[be_index];
            server.insert("http_port".to_string(), Value::Integer(i64::from(be.http)));
            server.insert("grpc_port".to_string(), Value::Integer(i64::from(be.grpc)));
        }
    }

    match role {
        ClusterProcessRole::Fe => {
            let standalone_server = table_mut(root, "standalone_server");
            standalone_server.insert(
                "mysql_port".to_string(),
                Value::Integer(i64::from(runtime.fe_mysql_port)),
            );
        }
        ClusterProcessRole::Be => {
            if let Some(standalone_server) = root
                .get_mut("standalone_server")
                .and_then(Value::as_table_mut)
            {
                standalone_server.remove("mysql_port");
            }
        }
    }

    let cluster = table_mut(root, "cluster");
    match role {
        ClusterProcessRole::Fe => {
            cluster.insert("role".to_string(), Value::String("fe".to_string()));
            cluster.insert("heartbeat_interval_ms".to_string(), Value::Integer(500));
            cluster.insert("heartbeat_timeout_retries".to_string(), Value::Integer(2));
            let backends: Vec<Value> = runtime
                .be
                .iter()
                .map(|be| Value::String(format!("127.0.0.1:{}", be.grpc)))
                .collect();
            cluster.insert("backends".to_string(), Value::Array(backends));
        }
        ClusterProcessRole::Be => {
            cluster.insert("role".to_string(), Value::String("be".to_string()));
            cluster.remove("backends");
        }
    }

    if role == ClusterProcessRole::Be {
        root.remove("state_store");
    }

    toml::to_string(&value).context("serialize cross-process standalone config")
}

/// Render the per-process TOML config for cross-process mode, then override
/// the IMV metadata store's SQLite path.
///
/// The override targets `[metadata].path` — the key read by
/// `open_metadata_provider` via `MetadataConfig { provider, path }` — because
/// that is where the IMV definition cache (and other standalone metadata)
/// actually lives. This is deliberately **not**
/// `[standalone_server].metadata_db_path`, which is a different, legacy
/// managed-lake key unrelated to the `[metadata]` SQLite store that
/// `restore_metadata_if_needed` / `rebuild_imv_cache_from_lake` operate on.
///
/// Used by the L2 cross-process empty-metadata statelessness harness to point
/// a second FE launch at a fresh, empty SQLite path while keeping every other
/// section (server ports, object store, warehouse) identical to a normal
/// `render_cross_process_config` render — so the second launch talks to the
/// same lake but starts with no cached IMV definitions. Launch-only rendering
/// also replaces the FE StateStore path with a runtime-local SQLite file so
/// membership rows cannot leak across ordinary SQL-test clusters.
pub(crate) fn render_cross_process_config_with_metadata_db_override(
    base_config: &str,
    role: ClusterProcessRole,
    be_index: usize,
    runtime: &CrossProcessRuntime,
    metadata_db_path: &str,
) -> Result<String> {
    let rendered = render_cross_process_config(base_config, role, be_index, runtime)?;

    let mut value = rendered
        .parse::<Value>()
        .context("parse rendered cross-process config for metadata override")?;
    let root = value.as_table_mut().ok_or_else(|| {
        anyhow::anyhow!("rendered cross-process config root must be a TOML table")
    })?;

    let metadata = table_mut(root, "metadata");
    metadata.insert(
        "path".to_string(),
        Value::String(metadata_db_path.to_string()),
    );

    toml::to_string(&value).context("serialize cross-process config with metadata db override")
}

fn render_cross_process_launch_config(
    base_config: &str,
    role: ClusterProcessRole,
    be_index: usize,
    runtime: &CrossProcessRuntime,
    runtime_dir: &Path,
    metadata_mode: CrossProcessMetadataMode<'_>,
) -> Result<String> {
    let rendered = match metadata_mode {
        CrossProcessMetadataMode::Isolated => {
            let metadata_path = runtime_dir.join("metadata.sqlite");
            let metadata_path = metadata_path
                .to_str()
                .context("cross-process runtime metadata path must be valid UTF-8")?;
            render_cross_process_config_with_metadata_db_override(
                base_config,
                role,
                be_index,
                runtime,
                metadata_path,
            )
        }
        CrossProcessMetadataMode::Explicit(metadata_path) => {
            render_cross_process_config_with_metadata_db_override(
                base_config,
                role,
                be_index,
                runtime,
                metadata_path,
            )
        }
    }?;

    let mut value = rendered
        .parse::<Value>()
        .context("parse rendered cross-process config for launch StateStore isolation")?;
    let root = value.as_table_mut().ok_or_else(|| {
        anyhow::anyhow!("rendered cross-process config root must be a TOML table")
    })?;
    if role == ClusterProcessRole::Fe {
        let state_store_path = runtime_dir.join("state-store.sqlite");
        let state_store_path = state_store_path
            .to_str()
            .context("cross-process runtime StateStore path must be valid UTF-8")?;
        let state_store = table_mut(root, "state_store");
        state_store
            .entry("provider".to_string())
            .or_insert_with(|| Value::String("sqlite".to_string()));
        state_store.insert(
            "path".to_string(),
            Value::String(state_store_path.to_string()),
        );
        state_store
            .entry("cluster_id".to_string())
            .or_insert_with(|| Value::String("sql-tests-cross-process".to_string()));
        state_store
            .entry("deployment_owner".to_string())
            .or_insert_with(|| Value::String("fe-1".to_string()));
    }

    toml::to_string(&value)
        .context("serialize cross-process launch config with isolated StateStore")
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

pub(crate) struct CrossProcessServerHandle {
    target_host: String,
    target_port: u16,
    mysql_user: String,
    be_grpc_ports: Vec<u16>,
    fragment_failure_trigger_paths: Vec<PathBuf>,
    fragment_failure_tokens: Vec<Option<String>>,
    runtime_dir: PathBuf,
    novarocks_bin: PathBuf,
    be_config_paths: Vec<PathBuf>,
    be_processes: Vec<ManagedProcess>,
    fe_process: ManagedProcess,
}

#[derive(Clone, Copy)]
enum CrossProcessMetadataMode<'a> {
    /// Ephemeral SQL-test clusters, including IMV L2 cluster A, must never
    /// restore metadata or backend membership from another launch whose
    /// dynamically reserved endpoints are already stale.
    Isolated,
    Explicit(&'a str),
}

struct RuntimeDirGuard {
    runtime_dir: Option<PathBuf>,
}

impl RuntimeDirGuard {
    fn new(runtime_dir: PathBuf) -> Self {
        Self {
            runtime_dir: Some(runtime_dir),
        }
    }

    fn path(&self) -> &Path {
        self.runtime_dir.as_deref().expect("runtime dir available")
    }

    fn into_path(mut self) -> PathBuf {
        self.runtime_dir.take().expect("runtime dir available")
    }
}

impl Drop for RuntimeDirGuard {
    fn drop(&mut self) {
        if let Some(runtime_dir) = self.runtime_dir.take() {
            let _ = fs::remove_dir_all(runtime_dir);
        }
    }
}

impl CrossProcessServerHandle {
    /// Launch the normal ephemeral cross-process cluster used by sql-test
    /// suites through `launch_server`.
    pub(crate) fn launch(
        cluster_size: usize,
        repo_root: &Path,
        runner_config: &RunnerConfig,
    ) -> Result<Self> {
        Self::launch_impl(
            cluster_size,
            repo_root,
            runner_config,
            CrossProcessMetadataMode::Isolated,
        )
    }

    /// Same as [`Self::launch`], but every rendered process config (FE and
    /// each BE) has its `[metadata].path` overridden to `metadata_db_path`
    /// instead of inheriting the base config's value.
    ///
    /// This is the launch primitive the L2 cross-process empty-metadata
    /// statelessness harness (`crate::imv_stateless`) uses for its second
    /// cluster: same lake/object-store/warehouse config as the first launch,
    /// but a fresh, empty SQLite metadata path, so the FE's IMV definition
    /// cache starts empty and must be rebuilt from the lake at startup (see
    /// `restore_metadata_if_needed` / `rebuild_imv_cache_from_lake` in
    /// `src/engine/mod.rs`).
    pub(crate) fn launch_with_metadata_db_override(
        cluster_size: usize,
        repo_root: &Path,
        runner_config: &RunnerConfig,
        metadata_db_path: &str,
    ) -> Result<Self> {
        Self::launch_impl(
            cluster_size,
            repo_root,
            runner_config,
            CrossProcessMetadataMode::Explicit(metadata_db_path),
        )
    }

    fn launch_impl(
        cluster_size: usize,
        repo_root: &Path,
        runner_config: &RunnerConfig,
        metadata_mode: CrossProcessMetadataMode<'_>,
    ) -> Result<Self> {
        let runtime_dir = RuntimeDirGuard::new(create_runtime_dir(repo_root)?);
        let reserved = ReservedRuntimePorts::new(cluster_size)?;

        // Build runtime port record from reserved ports (before releasing any).
        let runtime = CrossProcessRuntime {
            be: reserved
                .be_ports
                .iter()
                .map(|bp| BePorts {
                    http: bp.http.port(),
                    grpc: bp.grpc.port(),
                })
                .collect(),
            fe_http_port: reserved.fe_http_port.port(),
            fe_grpc_port: reserved.fe_grpc_port.port(),
            fe_mysql_port: reserved.fe_mysql_port.port(),
        };

        let novarocks_bin = discover_novarocks_binary(repo_root)?;
        let base_config_path = resolve_base_app_config_path(repo_root, runner_config)?;
        let base_config = fs::read_to_string(&base_config_path).with_context(|| {
            format!(
                "read standalone config for cross-process mode: {}",
                base_config_path.display()
            )
        })?;
        let mysql_user = base_config
            .parse::<Value>()
            .ok()
            .and_then(|value| {
                value
                    .get("standalone_server")
                    .and_then(|server| server.get("user"))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
            .unwrap_or_else(|| "root".to_string());

        let render = |role: ClusterProcessRole, be_index: usize| -> Result<String> {
            render_cross_process_launch_config(
                &base_config,
                role,
                be_index,
                &runtime,
                runtime_dir.path(),
                metadata_mode,
            )
        };

        // Write per-BE configs.
        let mut be_config_paths: Vec<PathBuf> = Vec::with_capacity(cluster_size);
        let fragment_failure_trigger_paths = (0..cluster_size)
            .map(|index| {
                runtime_dir
                    .path()
                    .join(format!("be_{index}.fragment_failure_trigger"))
            })
            .collect::<Vec<_>>();
        for i in 0..cluster_size {
            let be_config_path = runtime_dir.path().join(format!("be_{i}.toml"));
            fs::write(&be_config_path, render(ClusterProcessRole::Be, i)?)
                .with_context(|| format!("write {}", be_config_path.display()))?;
            be_config_paths.push(be_config_path);
        }

        // Write FE config.
        let fe_config_path = runtime_dir.path().join("fe.toml");
        fs::write(&fe_config_path, render(ClusterProcessRole::Fe, 0)?)
            .with_context(|| format!("write {}", fe_config_path.display()))?;

        // Spawn all BEs: release each BE's ports immediately before spawning it.
        let mut be_processes: Vec<ManagedProcess> = Vec::with_capacity(cluster_size);
        for (i, (reserved_be, be_config_path)) in reserved
            .be_ports
            .into_iter()
            .zip(be_config_paths.iter())
            .enumerate()
        {
            let grpc_port = reserved_be.grpc.port();
            let _ = reserved_be.http.release();
            let _ = reserved_be.grpc.release();
            let be_process = spawn_novarocks_process(
                &novarocks_bin,
                "be",
                be_config_path,
                "NOVAROCKS_READY role=be",
                runtime_dir.path().join(format!("be_{i}.log")),
                Some(&fragment_failure_trigger_paths[i]),
            )?;
            println!(
                "started cross-process BE[{i}] pid={} grpc_port={} config={}",
                be_process.pid(),
                grpc_port,
                be_config_path.display()
            );
            be_processes.push(be_process);
        }

        // Spawn FE.
        let _ = reserved.fe_http_port.release();
        let _ = reserved.fe_grpc_port.release();
        let _ = reserved.fe_mysql_port.release();
        let mut fe_process = spawn_novarocks_process(
            &novarocks_bin,
            "fe",
            &fe_config_path,
            "NOVAROCKS_READY mysql_port=",
            runtime_dir.path().join("fe.log"),
            None,
        )?;
        println!(
            "started cross-process FE pid={} mysql_port={} config={}",
            fe_process.pid(),
            runtime.fe_mysql_port,
            fe_config_path.display()
        );
        wait_for_live_backend_topology(
            &mysql_user,
            &runtime,
            &fe_config_path,
            &be_config_paths,
            &mut fe_process,
            &mut be_processes,
        )
        .context("cross-process backend topology barrier")?;

        Ok(Self {
            target_host: "127.0.0.1".to_string(),
            target_port: runtime.fe_mysql_port,
            mysql_user,
            be_grpc_ports: runtime.be.iter().map(|be| be.grpc).collect(),
            fragment_failure_trigger_paths,
            fragment_failure_tokens: vec![None; cluster_size],
            runtime_dir: runtime_dir.into_path(),
            novarocks_bin,
            be_config_paths,
            be_processes,
            fe_process,
        })
    }

    fn ensure_be_index(&self, index: usize) -> Result<()> {
        if index >= self.be_processes.len() {
            bail!(
                "BE index {} is out of bounds for cross-process cluster with {} BE(s)",
                index,
                self.be_processes.len()
            );
        }
        Ok(())
    }
}

impl ServerHandle for CrossProcessServerHandle {
    fn target_host(&self) -> Option<&str> {
        Some(self.target_host.as_str())
    }

    fn target_port(&self) -> Option<u16> {
        Some(self.target_port)
    }

    fn supports_fault_injection(&self) -> bool {
        true
    }

    fn be_count(&self) -> usize {
        self.be_processes.len()
    }

    fn scheduled_fragment_count(&self, index: usize) -> Result<u64> {
        self.ensure_be_index(index)?;
        let grpc_port = self.be_grpc_ports[index];
        let rows = query_frontend_backend_topology(
            &self.mysql_user,
            &self.target_host,
            self.target_port,
            TOPOLOGY_MYSQL_IO_TIMEOUT_CAP,
        )?;
        rows.into_iter()
            .find(|row| row.grpc_port == grpc_port)
            .map(|row| row.scheduled_fragments)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "SHOW BACKENDS has no row for cross-process BE[{index}] grpc_port={grpc_port}"
                )
            })
    }

    fn arm_fragment_executor_failure(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        if self.fragment_failure_tokens[index].is_some() {
            bail!("cross-process BE[{index}] already has an armed fragment executor failure token");
        }
        let trigger_path = &self.fragment_failure_trigger_paths[index];
        remove_fragment_failure_file(&fragment_failure_release_path(trigger_path)).with_context(
            || {
                format!(
                    "clear stale fragment executor failure release for cross-process BE[{index}]"
                )
            },
        )?;
        let token = next_fragment_failure_token(index);
        publish_fragment_failure_token(trigger_path, &token).with_context(|| {
            format!(
                "arm fragment executor failure for cross-process BE[{index}] at {}",
                trigger_path.display()
            )
        })?;
        self.fragment_failure_tokens[index] = Some(token.clone());
        println!(
            "armed fragment executor failure for cross-process BE[{index}] trigger={} token={token}",
            trigger_path.display(),
        );
        Ok(())
    }

    fn release_fragment_executor_failure(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        let token = self.fragment_failure_tokens[index]
            .as_deref()
            .with_context(|| {
                format!("cross-process BE[{index}] has no armed fragment executor failure token")
            })?;
        let release_path =
            fragment_failure_release_path(&self.fragment_failure_trigger_paths[index]);
        publish_fragment_failure_token(&release_path, token).with_context(|| {
            format!(
                "release fragment executor failure for cross-process BE[{index}] at {}",
                release_path.display()
            )
        })?;
        println!(
            "released fragment executor failure for cross-process BE[{index}] release={} token={token}",
            release_path.display(),
        );
        Ok(())
    }

    fn disarm_fragment_executor_failure(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        let trigger_path = &self.fragment_failure_trigger_paths[index];
        remove_fragment_failure_file(trigger_path).with_context(|| {
            format!(
                "disarm fragment executor failure trigger for cross-process BE[{index}] at {}",
                trigger_path.display()
            )
        })?;
        let release_path = fragment_failure_release_path(trigger_path);
        remove_fragment_failure_file(&release_path).with_context(|| {
            format!(
                "disarm fragment executor failure release for cross-process BE[{index}] at {}",
                release_path.display()
            )
        })?;
        self.fragment_failure_tokens[index] = None;
        Ok(())
    }

    fn armed_fragment_failure_token(&self, index: usize) -> Result<Option<String>> {
        self.ensure_be_index(index)?;
        Ok(self.fragment_failure_tokens[index].clone())
    }

    fn assert_be_log(&self, index: usize, needle: &str) -> Result<()> {
        self.ensure_be_index(index)?;
        self.be_processes[index].assert_log_contains(needle)
    }

    fn be_log_count(&self, index: usize, needle: &str) -> Result<usize> {
        self.ensure_be_index(index)?;
        self.be_processes[index].log_count(needle)
    }

    fn be_log_contents(&self, index: usize) -> Result<String> {
        self.ensure_be_index(index)?;
        self.be_processes[index].log_contents()
    }

    fn kill_be(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        let be_process = self
            .be_processes
            .get_mut(index)
            .expect("BE index checked above");
        be_process
            .kill_now()
            .with_context(|| format!("kill cross-process BE[{index}]"))?;
        println!("killed cross-process BE[{index}]");
        Ok(())
    }

    fn restart_be(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        {
            let be_process = self
                .be_processes
                .get_mut(index)
                .expect("BE index checked above");
            be_process
                .kill_now()
                .with_context(|| format!("stop old cross-process BE[{index}] before restart"))?;
        }

        let config_path = self
            .be_config_paths
            .get(index)
            .ok_or_else(|| {
                anyhow::anyhow!("missing config path for cross-process BE[{index}] during restart")
            })?
            .clone();
        let marker = "NOVAROCKS_READY role=be";
        let mut command = build_novarocks_command(&self.novarocks_bin, "be", &config_path);
        command.env(
            "NOVAROCKS_SQL_TEST_FRAGMENT_FAILURE_TRIGGER_FILE",
            &self.fragment_failure_trigger_paths[index],
        );
        let log_path = self.runtime_dir.join(format!("be_{index}.log"));
        let be_process = self
            .be_processes
            .get_mut(index)
            .expect("BE index checked above");
        be_process
            .restart(
                command,
                ReadyMarker::StdoutContains(marker.to_string()),
                startup_timeout(),
                log_path,
            )
            .map_err(|error| map_novarocks_process_error(&self.novarocks_bin, "be", marker, error))
            .with_context(|| format!("restart cross-process BE[{index}]"))?;
        println!(
            "restarted cross-process BE[{index}] pid={} config={}",
            be_process.pid(),
            config_path.display()
        );
        Ok(())
    }
}

impl Drop for CrossProcessServerHandle {
    fn drop(&mut self) {
        let _ = self.fe_process.stop();
        for be_process in &mut self.be_processes {
            let _ = be_process.stop();
        }
        let _ = fs::remove_dir_all(&self.runtime_dir);
    }
}

fn process_runtime_diagnostics(
    fe_process: &mut ManagedProcess,
    be_processes: &mut [ManagedProcess],
    fe_config_path: &Path,
    be_config_paths: &[PathBuf],
    runtime: &CrossProcessRuntime,
) -> Result<String> {
    if be_processes.len() != runtime.be.len() || be_config_paths.len() != runtime.be.len() {
        bail!(
            "cross-process diagnostic cardinality mismatch: processes={} configs={} endpoints={}",
            be_processes.len(),
            be_config_paths.len(),
            runtime.be.len()
        );
    }

    let mut diagnostics = Vec::with_capacity(be_processes.len() + 1);
    let mut exited = false;
    match fe_process.runtime_diagnostic(
        "FE",
        &format!("mysql://127.0.0.1:{}", runtime.fe_mysql_port),
        fe_config_path,
    ) {
        Ok(diagnostic) => diagnostics.push(diagnostic),
        Err(error) => {
            exited = true;
            diagnostics.push(format!("{error:#}"));
        }
    }
    for (index, ((process, config_path), ports)) in be_processes
        .iter_mut()
        .zip(be_config_paths.iter())
        .zip(runtime.be.iter())
        .enumerate()
    {
        match process.runtime_diagnostic(
            &format!("BE[{index}]"),
            &format!("grpc://127.0.0.1:{}", ports.grpc),
            config_path,
        ) {
            Ok(diagnostic) => diagnostics.push(diagnostic),
            Err(error) => {
                exited = true;
                diagnostics.push(format!("{error:#}"));
            }
        }
    }
    let diagnostics = diagnostics.join("; ");
    if exited {
        bail!("cross-process process exited: {diagnostics}");
    }
    Ok(diagnostics)
}

pub(crate) fn build_novarocks_command(binary: &Path, role: &str, config_path: &Path) -> Command {
    let mut command = Command::new(binary);
    command
        .arg("standalone")
        .arg("--role")
        .arg(role)
        .arg("--config")
        .arg(config_path)
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("NOVAROCKS_ENABLE_TEST_IMV_STATELESS_REBUILD", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

fn spawn_novarocks_process(
    binary: &Path,
    role: &str,
    config_path: &Path,
    marker: &str,
    log_path: PathBuf,
    fragment_failure_trigger: Option<&Path>,
) -> Result<ManagedProcess> {
    let mut command = build_novarocks_command(binary, role, config_path);
    if let Some(trigger_path) = fragment_failure_trigger {
        command.env(
            "NOVAROCKS_SQL_TEST_FRAGMENT_FAILURE_TRIGGER_FILE",
            trigger_path,
        );
    }
    let result = ManagedProcess::spawn(
        "novarocks".to_string(),
        command,
        ReadyMarker::StdoutContains(marker.to_string()),
        startup_timeout(),
        log_path,
    );
    match result {
        Ok(process) => Ok(process),
        Err(error) => Err(map_novarocks_process_error(binary, role, marker, error)),
    }
}

fn next_fragment_failure_token(index: usize) -> String {
    static NEXT_TOKEN: AtomicU64 = AtomicU64::new(1);
    let sequence = NEXT_TOKEN.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{}-{index}-{nanos}-{sequence}", std::process::id())
}

fn fragment_failure_release_path(trigger_path: &Path) -> PathBuf {
    trigger_path.with_extension("release")
}

fn remove_fragment_failure_file(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn publish_fragment_failure_token(trigger_path: &Path, token: &str) -> Result<()> {
    let staging_path = trigger_path.with_extension(format!("arming-{token}"));
    let mut staging = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staging_path)
        .with_context(|| {
            format!(
                "create fragment executor failure staging file {}",
                staging_path.display()
            )
        })?;
    if let Err(error) = staging.write_all(token.as_bytes()) {
        let _ = fs::remove_file(&staging_path);
        return Err(error).with_context(|| {
            format!(
                "write fragment executor failure token to staging file {}",
                staging_path.display()
            )
        });
    }
    drop(staging);

    if let Err(error) = fs::hard_link(&staging_path, trigger_path) {
        let _ = fs::remove_file(&staging_path);
        return Err(error).with_context(|| {
            format!(
                "publish fragment executor failure trigger {}",
                trigger_path.display()
            )
        });
    }
    let _ = fs::remove_file(staging_path);
    Ok(())
}

fn map_novarocks_process_error(
    binary: &Path,
    role: &str,
    marker: &str,
    error: anyhow::Error,
) -> anyhow::Error {
    if format!("{error:#}").starts_with("spawn novarocks;") {
        return error.context(format!("spawn novarocks {role} from {}", binary.display()));
    }
    managed_novarocks_startup_error(marker, error)
}

fn managed_novarocks_startup_error(marker: &str, error: anyhow::Error) -> anyhow::Error {
    let message = format!("{error:#}");
    anyhow::anyhow!(format_startup_failure(marker, &message, &message))
}

pub(crate) fn startup_timeout() -> Duration {
    startup_timeout_from_env(
        std::env::var("NOVAROCKS_STARTUP_TIMEOUT_SECS")
            .ok()
            .as_deref(),
    )
}

pub(crate) fn startup_timeout_from_env(raw: Option<&str>) -> Duration {
    let timeout_secs = raw
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .unwrap_or(120);
    bounded_backend_topology_timeout(Duration::from_secs(timeout_secs))
}

struct ReservedBePorts {
    http: ReservedPort,
    grpc: ReservedPort,
}

struct ReservedRuntimePorts {
    be_ports: Vec<ReservedBePorts>,
    fe_http_port: ReservedPort,
    fe_grpc_port: ReservedPort,
    fe_mysql_port: ReservedPort,
}

impl ReservedRuntimePorts {
    fn new(cluster_size: usize) -> Result<Self> {
        assert!(cluster_size >= 1, "cluster_size must be >= 1");
        let mut be_ports = Vec::with_capacity(cluster_size);
        for _ in 0..cluster_size {
            be_ports.push(ReservedBePorts {
                http: ReservedPort::new()?,
                grpc: ReservedPort::new()?,
            });
        }
        Ok(Self {
            be_ports,
            fe_http_port: ReservedPort::new()?,
            fe_grpc_port: ReservedPort::new()?,
            fe_mysql_port: ReservedPort::new()?,
        })
    }
}

struct ReservedPort {
    _listener: TcpListener,
    port: u16,
}

impl ReservedPort {
    fn new() -> Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).context("bind ephemeral port")?;
        let port = listener.local_addr().context("read ephemeral port")?.port();
        Ok(Self {
            _listener: listener,
            port,
        })
    }

    fn port(&self) -> u16 {
        self.port
    }

    fn release(self) -> u16 {
        self.port
    }
}

fn format_startup_failure(marker: &str, message: &str, stderr: &str) -> String {
    if is_bind_conflict(stderr) {
        format!(
            "{message}; probable port bind conflict while starting cross-process mode. Retry the run or inspect processes already using the reserved ports (readiness marker `{marker}`)."
        )
    } else {
        format!("{message} (readiness marker `{marker}`)")
    }
}

fn is_bind_conflict(stderr: &str) -> bool {
    let stderr = stderr.to_ascii_lowercase();
    stderr.contains("address already in use")
        || stderr.contains("addrinuse")
        || stderr.contains("eaddrinuse")
        || stderr.contains("os error 48")
        || (stderr.contains("bind") && stderr.contains("in use"))
}

fn create_runtime_dir(repo_root: &Path) -> Result<PathBuf> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let path = repo_root.join(format!(
        ".sql-test-runner-runtime/{}_{}",
        std::process::id(),
        nanos
    ));
    fs::create_dir_all(&path).with_context(|| format!("create {}", path.display()))?;
    Ok(path)
}

fn table_mut<'a>(
    table: &'a mut toml::map::Map<String, Value>,
    key: &str,
) -> &'a mut toml::map::Map<String, Value> {
    if !matches!(table.get(key), Some(Value::Table(_))) {
        table.insert(key.to_string(), Value::Table(Default::default()));
    }
    table
        .get_mut(key)
        .and_then(Value::as_table_mut)
        .expect("table inserted")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn backend_row(grpc_port: u16, state: &str, alive: bool) -> BackendTopologyRow {
        BackendTopologyRow {
            grpc_port,
            state: state.to_string(),
            alive,
            scheduled_fragments: 0,
        }
    }

    #[test]
    fn frontend_five_column_show_backends_derives_alive_from_state() {
        let row = parse_frontend_show_backends_values(&[
            "0".to_string(),
            "127.0.0.1".to_string(),
            "19070".to_string(),
            "Live".to_string(),
            "41".to_string(),
        ])
        .expect("parse frontend SHOW BACKENDS row");

        assert_eq!(row.grpc_port, 19070);
        assert_eq!(row.state, "Live");
        assert!(row.alive);
        assert_eq!(row.scheduled_fragments, 41);
    }

    #[test]
    fn live_backend_topology_requires_exact_configured_count_and_all_live() {
        let expected = [19070, 19071];
        let ready = vec![
            backend_row(19070, "Live", true),
            backend_row(19071, "Live", true),
        ];
        validate_live_backend_topology(&expected, &ready).expect("2/2 Live should pass");

        let extra = vec![
            backend_row(19070, "Live", true),
            backend_row(19071, "Live", true),
            backend_row(19072, "Live", true),
        ];
        let err = validate_live_backend_topology(&expected, &extra)
            .expect_err("an extra registered backend must fail the exact topology");
        assert!(err.to_string().contains("registered=3 expected=2"), "{err}");

        let registering = vec![
            backend_row(19070, "Live", true),
            backend_row(19071, "Registering", false),
        ];
        let err = validate_live_backend_topology(&expected, &registering)
            .expect_err("a non-Live configured backend must fail readiness");
        assert!(err.to_string().contains("live=1 expected=2"), "{err}");
        assert!(err.to_string().contains("19071:Registering:false"), "{err}");

        let stale_replacement = vec![
            backend_row(19070, "Live", true),
            backend_row(19072, "Live", true),
        ];
        let err = validate_live_backend_topology(&expected, &stale_replacement)
            .expect_err("a stale Live backend must not replace a configured endpoint");
        assert!(
            err.to_string()
                .contains("configured_ports=[19070, 19071] observed_ports=[19070, 19072]"),
            "{err}"
        );
    }

    #[test]
    fn backend_topology_barrier_retries_until_general_n_is_live() {
        let mut attempts = 0;
        let mut io_timeouts = Vec::new();
        let snapshot = wait_for_live_backend_topology_with(
            &[19070, 19071],
            Duration::from_secs(1),
            || Ok("fe=running be=[running,running]".to_string()),
            |io_timeout| {
                io_timeouts.push(io_timeout);
                attempts += 1;
                if attempts == 1 {
                    Ok(vec![
                        backend_row(19070, "Live", true),
                        backend_row(19071, "Registering", false),
                    ])
                } else {
                    Ok(vec![
                        backend_row(19070, "Live", true),
                        backend_row(19071, "Live", true),
                    ])
                }
            },
            |_| {},
        )
        .expect("barrier should retry until 2/2 Live");

        assert_eq!(attempts, 2);
        assert_eq!(snapshot.len(), 2);
        assert!(
            io_timeouts
                .iter()
                .all(|timeout| *timeout > Duration::ZERO && *timeout <= Duration::from_secs(2)),
            "unexpected per-attempt MySQL timeouts: {io_timeouts:?}"
        );
    }

    #[test]
    fn backend_topology_barrier_timeout_includes_pid_and_endpoint_diagnostics() {
        let err = wait_for_live_backend_topology_with(
            &[19070, 19071, 19072],
            Duration::ZERO,
            || Ok("fe_pid=11 be_pids=[21,22,23] fe_mysql=127.0.0.1:29030 be_grpc=[127.0.0.1:19070,127.0.0.1:19071,127.0.0.1:19072]".to_string()),
            |_| Ok(vec![backend_row(19070, "Live", true)]),
            |_| {},
        )
        .expect_err("incomplete topology must time out");

        let message = format!("{err:#}");
        assert!(
            message.contains("timed out waiting for SHOW BACKENDS 3/3 Live"),
            "{message}"
        );
        assert!(message.contains("registered=1 expected=3"), "{message}");
        assert!(message.contains("fe_pid=11"), "{message}");
        assert!(message.contains("be_pids=[21,22,23]"), "{message}");
        assert!(message.contains("fe_mysql=127.0.0.1:29030"), "{message}");
        assert!(message.contains("be_grpc=[127.0.0.1:19070"), "{message}");
    }

    #[test]
    fn backend_topology_barrier_fails_before_query_when_a_process_exits() {
        let mut queries = 0;
        let err = wait_for_live_backend_topology_with(
            &[19070],
            Duration::from_secs(30),
            || {
                bail!(
                    "FE exited status=exit status: 9 pid=11 endpoint=mysql://127.0.0.1:29030 config=/tmp/fe.toml stdout_tail=ready stderr_tail=fatal"
                )
            },
            |_| {
                queries += 1;
                Ok(vec![backend_row(19070, "Live", true)])
            },
            |_| {},
        )
        .expect_err("a dead FE must fail without waiting for the topology timeout");

        assert_eq!(queries, 0, "SHOW BACKENDS must not run after process exit");
        let message = format!("{err:#}");
        assert!(
            message.contains("FE exited status=exit status: 9"),
            "{message}"
        );
        assert!(message.contains("config=/tmp/fe.toml"), "{message}");
        assert!(message.contains("stderr_tail=fatal"), "{message}");
    }

    #[test]
    fn backend_topology_timeout_refreshes_process_health_after_query() {
        let mut health_checks = 0;
        let err = wait_for_live_backend_topology_with(
            &[19070],
            Duration::ZERO,
            || {
                health_checks += 1;
                if health_checks == 1 {
                    Ok("FE=running before query".to_string())
                } else {
                    bail!(
                        "FE exited post-query status=exit status: 7 pid=11 config=/tmp/fe.toml stderr_tail=post-query-fatal"
                    )
                }
            },
            |_| Ok(vec![backend_row(19070, "Registering", false)]),
            |_| {},
        )
        .expect_err("timeout must refresh process health after the bounded query");

        assert_eq!(
            health_checks, 2,
            "health must be sampled before and after query"
        );
        let message = format!("{err:#}");
        assert!(
            message.contains("FE exited post-query status=exit status: 7"),
            "{message}"
        );
        assert!(message.contains("post-query-fatal"), "{message}");
    }

    #[test]
    fn topology_timeouts_are_bounded_and_deadline_addition_cannot_panic() {
        assert_eq!(
            bounded_backend_topology_timeout(Duration::MAX),
            Duration::from_secs(120)
        );
        assert_eq!(
            topology_mysql_io_timeout(Duration::from_secs(30)),
            Duration::from_secs(2)
        );
        assert_eq!(
            topology_mysql_io_timeout(Duration::from_millis(250)),
            Duration::from_millis(250)
        );
        assert_eq!(
            topology_mysql_io_timeout(Duration::ZERO),
            Duration::from_millis(1)
        );
        let now = Instant::now();
        let deadline = backend_topology_deadline(now, Duration::MAX);
        assert!(deadline >= now);
        assert!(deadline.duration_since(now) <= Duration::from_secs(120));
    }

    #[test]
    fn noop_server_handle_rejects_be_process_controls() {
        let mut handle = NoopServerHandle;

        let kill_err = handle.kill_be(0).expect_err("noop kill should fail");
        assert!(
            kill_err.to_string().contains("BE kill is unsupported"),
            "unexpected error: {kill_err}"
        );

        let restart_err = handle.restart_be(0).expect_err("noop restart should fail");
        assert!(
            restart_err
                .to_string()
                .contains("BE restart is unsupported"),
            "unexpected error: {restart_err}"
        );
    }

    fn make_runtime_1be() -> CrossProcessRuntime {
        CrossProcessRuntime {
            be: vec![BePorts {
                http: 18080,
                grpc: 19070,
            }],
            fe_http_port: 28080,
            fe_grpc_port: 29070,
            fe_mysql_port: 29030,
        }
    }

    fn make_runtime_2be() -> CrossProcessRuntime {
        CrossProcessRuntime {
            be: vec![
                BePorts {
                    http: 18080,
                    grpc: 19070,
                },
                BePorts {
                    http: 18081,
                    grpc: 19071,
                },
            ],
            fe_http_port: 28080,
            fe_grpc_port: 29070,
            fe_mysql_port: 29030,
        }
    }

    static BASE_CONFIG: &str = r#"
[metadata]
provider = "sqlite"
path = "tmp/sql-tests.sqlite"

[state_store]
provider = "sqlite"
cluster_id = "sql-tests-cross-process"
path = "tmp/sql-tests-state-store.sqlite"
deployment_owner = "fe-1"

[standalone_server]
mysql_port = 9030
warehouse_uri = "s3://warehouse/sql-tests"
user = "root"

[standalone_server.object_store]
endpoint = "http://127.0.0.1:9000"
access_key_id = "admin"
enable_path_style_access = true

[connector.object_store]
endpoint = "http://127.0.0.1:9000"
access_key_id = "admin"
enable_path_style_access = true

[debug]
exec_node_output = true
"#;

    #[test]
    fn render_cross_process_config_patches_fe_and_be_independently() {
        let runtime = make_runtime_1be();

        let fe = render_cross_process_config(BASE_CONFIG, ClusterProcessRole::Fe, 0, &runtime)
            .expect("render fe config");
        let be = render_cross_process_config(BASE_CONFIG, ClusterProcessRole::Be, 0, &runtime)
            .expect("render be config");

        let fe_value: toml::Value = fe.parse().expect("parse fe toml");
        let be_value: toml::Value = be.parse().expect("parse be toml");

        assert_eq!(
            fe_value["metadata"]["path"].as_str(),
            Some("tmp/sql-tests.sqlite")
        );
        assert_eq!(
            fe_value["state_store"]["path"].as_str(),
            Some("tmp/sql-tests-state-store.sqlite")
        );
        assert_ne!(
            fe_value["metadata"]["path"].as_str(),
            fe_value["state_store"]["path"].as_str(),
            "MV StateStore must not share the legacy metadata SQLite path"
        );
        assert_eq!(
            fe_value["standalone_server"]["object_store"]["endpoint"].as_str(),
            Some("http://127.0.0.1:9000")
        );
        assert_eq!(
            fe_value["connector"]["object_store"]["endpoint"].as_str(),
            Some("http://127.0.0.1:9000")
        );
        assert_eq!(fe_value["debug"]["exec_node_output"].as_bool(), Some(true));
        assert_eq!(fe_value["server"]["host"].as_str(), Some("127.0.0.1"));
        assert_eq!(fe_value["server"]["http_port"].as_integer(), Some(28080));
        assert_eq!(fe_value["server"]["grpc_port"].as_integer(), Some(29070));
        assert_eq!(
            fe_value["standalone_server"]["mysql_port"].as_integer(),
            Some(29030)
        );
        assert_eq!(fe_value["standalone_server"]["user"].as_str(), Some("root"));
        assert_eq!(fe_value["cluster"]["role"].as_str(), Some("fe"));
        assert_eq!(
            fe_value["cluster"]["heartbeat_interval_ms"].as_integer(),
            Some(500)
        );
        assert_eq!(
            fe_value["cluster"]["heartbeat_timeout_retries"].as_integer(),
            Some(2)
        );
        // 1-BE: FE backends list has exactly one entry pointing at the single BE's grpc port.
        let fe_backends = fe_value["cluster"]["backends"]
            .as_array()
            .expect("fe backends array");
        assert_eq!(fe_backends.len(), 1);
        assert_eq!(fe_backends[0].as_str(), Some("127.0.0.1:19070"));

        assert_eq!(
            be_value["metadata"]["path"].as_str(),
            Some("tmp/sql-tests.sqlite")
        );
        assert!(
            be_value.get("state_store").is_none(),
            "BE rendering must not invent a separate MV StateStore"
        );
        assert_eq!(
            be_value["standalone_server"]["object_store"]["endpoint"].as_str(),
            Some("http://127.0.0.1:9000")
        );
        assert_eq!(
            be_value["connector"]["object_store"]["endpoint"].as_str(),
            Some("http://127.0.0.1:9000")
        );
        assert_eq!(be_value["debug"]["exec_node_output"].as_bool(), Some(true));
        assert_eq!(be_value["server"]["host"].as_str(), Some("127.0.0.1"));
        assert_eq!(be_value["server"]["http_port"].as_integer(), Some(18080));
        assert_eq!(be_value["server"]["grpc_port"].as_integer(), Some(19070));
        assert_eq!(be_value["standalone_server"]["user"].as_str(), Some("root"));
        assert!(
            be_value
                .get("standalone_server")
                .and_then(|value| value.get("mysql_port"))
                .is_none()
        );
        assert_eq!(be_value["cluster"]["role"].as_str(), Some("be"));
        assert!(
            be_value
                .get("cluster")
                .and_then(|value| value.get("backends"))
                .is_none()
        );
        assert!(
            be_value
                .get("cluster")
                .and_then(|value| value.get("heartbeat_interval_ms"))
                .is_none()
        );
        assert!(
            be_value
                .get("cluster")
                .and_then(|value| value.get("heartbeat_timeout_retries"))
                .is_none()
        );
    }

    #[test]
    fn render_cross_process_config_does_not_add_runtime_selector() {
        let runtime = make_runtime_1be();

        let fe = render_cross_process_config(BASE_CONFIG, ClusterProcessRole::Fe, 0, &runtime)
            .expect("render fe config");
        let be = render_cross_process_config(BASE_CONFIG, ClusterProcessRole::Be, 0, &runtime)
            .expect("render be config");

        let fe_value: toml::Value = fe.parse().expect("parse fe toml");
        let be_value: toml::Value = be.parse().expect("parse be toml");

        assert!(
            fe_value.get("runtime").is_none(),
            "FE config must not add a runtime selector"
        );
        assert!(
            be_value.get("runtime").is_none(),
            "BE config must not add a runtime selector"
        );
    }

    #[test]
    fn render_cross_process_config_preserves_retired_base_runtime_key() {
        let runtime = make_runtime_1be();
        let retired_key = ["plan", "wire", "format"].join("_");
        let base_config = format!("{}\n[runtime]\n{retired_key} = \"thrift\"\n", BASE_CONFIG);

        let fe = render_cross_process_config(&base_config, ClusterProcessRole::Fe, 0, &runtime)
            .expect("render fe config");
        let be = render_cross_process_config(&base_config, ClusterProcessRole::Be, 0, &runtime)
            .expect("render be config");

        let fe_value: toml::Value = fe.parse().expect("parse fe toml");
        let be_value: toml::Value = be.parse().expect("parse be toml");

        assert_eq!(
            fe_value["runtime"]
                .get(&retired_key)
                .and_then(Value::as_str),
            Some("thrift"),
            "renderer must leave retired base keys for the product loader to reject"
        );
        assert_eq!(
            be_value["runtime"]
                .get(&retired_key)
                .and_then(Value::as_str),
            Some("thrift"),
            "renderer must leave retired base keys for the product loader to reject"
        );
    }

    /// Locally-validated unit test for the M7 L2 harness helper: confirms the
    /// override lands on `[metadata].path` (the key `open_metadata_provider`
    /// actually reads via `MetadataConfig { provider, path }`) and leaves every
    /// other section — server ports, cluster role/backends, object store,
    /// warehouse — exactly as `render_cross_process_config` would have
    /// produced them. This is the piece the harness can prove correct without
    /// a live cluster; the L2 e2e (two cross-process launches over the same
    /// lake) is exercised in CI via `imv_stateless::run_imv_stateless_l2_case`.
    #[test]
    fn render_cross_process_config_with_metadata_db_override_overrides_only_metadata_path() {
        let runtime = make_runtime_1be();

        let fe = render_cross_process_config_with_metadata_db_override(
            BASE_CONFIG,
            ClusterProcessRole::Fe,
            0,
            &runtime,
            "/new/empty.sqlite",
        )
        .expect("render fe config with metadata override");
        let fe_value: toml::Value = fe.parse().expect("parse fe toml");

        // The override key: [metadata].path, NOT
        // [standalone_server].metadata_db_path (a different, legacy
        // managed-lake key from the archived W0 plan).
        assert_eq!(
            fe_value["metadata"]["path"].as_str(),
            Some("/new/empty.sqlite")
        );
        assert_eq!(fe_value["metadata"]["provider"].as_str(), Some("sqlite"));
        assert!(
            fe_value
                .get("standalone_server")
                .and_then(|s| s.get("metadata_db_path"))
                .is_none(),
            "override must not write the legacy standalone_server.metadata_db_path key"
        );

        // Every other section must be untouched relative to a normal render.
        let plain_fe =
            render_cross_process_config(BASE_CONFIG, ClusterProcessRole::Fe, 0, &runtime)
                .expect("render plain fe config");
        let plain_fe_value: toml::Value = plain_fe.parse().expect("parse plain fe toml");

        assert_eq!(fe_value["server"], plain_fe_value["server"]);
        assert_eq!(fe_value["cluster"], plain_fe_value["cluster"]);
        assert_eq!(
            fe_value["standalone_server"]["mysql_port"],
            plain_fe_value["standalone_server"]["mysql_port"]
        );
        assert_eq!(
            fe_value["standalone_server"]["warehouse_uri"],
            plain_fe_value["standalone_server"]["warehouse_uri"]
        );
        assert_eq!(
            fe_value["standalone_server"]["object_store"],
            plain_fe_value["standalone_server"]["object_store"]
        );
        assert_eq!(fe_value["debug"], plain_fe_value["debug"]);

        // BE role also gets the override, independent of FE.
        let be = render_cross_process_config_with_metadata_db_override(
            BASE_CONFIG,
            ClusterProcessRole::Be,
            0,
            &runtime,
            "/new/empty.sqlite",
        )
        .expect("render be config with metadata override");
        let be_value: toml::Value = be.parse().expect("parse be toml");
        assert_eq!(
            be_value["metadata"]["path"].as_str(),
            Some("/new/empty.sqlite")
        );
    }

    #[test]
    fn ordinary_cross_process_launches_isolate_metadata_and_backend_state_store() {
        let runtime = make_runtime_1be();
        let first_runtime = Path::new("/tmp/novarocks-cross-process-run-a");
        let second_runtime = Path::new("/tmp/novarocks-cross-process-run-b");

        let first = render_cross_process_launch_config(
            BASE_CONFIG,
            ClusterProcessRole::Fe,
            0,
            &runtime,
            first_runtime,
            CrossProcessMetadataMode::Isolated,
        )
        .unwrap()
        .parse::<Value>()
        .unwrap();
        let second = render_cross_process_launch_config(
            BASE_CONFIG,
            ClusterProcessRole::Fe,
            0,
            &runtime,
            second_runtime,
            CrossProcessMetadataMode::Isolated,
        )
        .unwrap()
        .parse::<Value>()
        .unwrap();

        assert_eq!(
            first["metadata"]["path"].as_str(),
            first_runtime.join("metadata.sqlite").to_str()
        );
        assert_eq!(
            second["metadata"]["path"].as_str(),
            second_runtime.join("metadata.sqlite").to_str()
        );
        assert_ne!(
            first["metadata"]["path"], second["metadata"]["path"],
            "ephemeral clusters must not restore stale metadata from another launch"
        );
        assert_eq!(
            first["state_store"]["path"].as_str(),
            first_runtime.join("state-store.sqlite").to_str()
        );
        assert_eq!(
            second["state_store"]["path"].as_str(),
            second_runtime.join("state-store.sqlite").to_str()
        );
        assert_ne!(
            first["state_store"]["path"], second["state_store"]["path"],
            "ephemeral clusters must not restore stale backend membership from another launch"
        );
    }

    #[test]
    fn imv_l2_metadata_isolation_preserves_shared_lake_fixture() {
        let runtime = make_runtime_1be();
        let cluster_a_runtime = Path::new("/tmp/novarocks-imv-l2-cluster-a");
        let cluster_b_metadata = "/tmp/novarocks-imv-l2-cluster-b.sqlite";
        let base = BASE_CONFIG.parse::<Value>().unwrap();

        let cluster_a = render_cross_process_launch_config(
            BASE_CONFIG,
            ClusterProcessRole::Fe,
            0,
            &runtime,
            cluster_a_runtime,
            CrossProcessMetadataMode::Isolated,
        )
        .unwrap()
        .parse::<Value>()
        .unwrap();
        let cluster_b = render_cross_process_launch_config(
            BASE_CONFIG,
            ClusterProcessRole::Fe,
            0,
            &runtime,
            Path::new("/tmp/novarocks-imv-l2-cluster-b"),
            CrossProcessMetadataMode::Explicit(cluster_b_metadata),
        )
        .unwrap()
        .parse::<Value>()
        .unwrap();

        assert_eq!(
            cluster_a["metadata"]["path"].as_str(),
            cluster_a_runtime.join("metadata.sqlite").to_str(),
            "cluster A must not inherit metadata from the base database"
        );
        assert_eq!(
            cluster_b["metadata"]["path"].as_str(),
            Some(cluster_b_metadata)
        );
        assert_ne!(cluster_a["metadata"]["path"], base["metadata"]["path"]);
        assert_ne!(cluster_b["metadata"]["path"], base["metadata"]["path"]);
        assert_ne!(cluster_a["metadata"]["path"], cluster_b["metadata"]["path"]);
        assert_eq!(
            cluster_a["state_store"]["path"].as_str(),
            cluster_a_runtime.join("state-store.sqlite").to_str()
        );
        assert_eq!(
            cluster_b["state_store"]["path"].as_str(),
            Some("/tmp/novarocks-imv-l2-cluster-b/state-store.sqlite")
        );
        assert_ne!(
            cluster_a["state_store"]["path"], cluster_b["state_store"]["path"],
            "independent launch runtime directories must isolate backend membership"
        );
        assert_eq!(
            cluster_a["standalone_server"]["warehouse_uri"],
            base["standalone_server"]["warehouse_uri"]
        );
        assert_eq!(
            cluster_b["standalone_server"]["warehouse_uri"],
            base["standalone_server"]["warehouse_uri"]
        );
        assert_eq!(
            cluster_a["standalone_server"]["object_store"],
            base["standalone_server"]["object_store"]
        );
        assert_eq!(
            cluster_b["standalone_server"]["object_store"],
            base["standalone_server"]["object_store"]
        );
    }

    #[test]
    fn render_cross_process_config_with_metadata_override_does_not_add_runtime_selector() {
        let runtime = make_runtime_1be();

        let fe = render_cross_process_config_with_metadata_db_override(
            BASE_CONFIG,
            ClusterProcessRole::Fe,
            0,
            &runtime,
            "/new/empty.sqlite",
        )
        .expect("render fe config with metadata override");
        let be = render_cross_process_config_with_metadata_db_override(
            BASE_CONFIG,
            ClusterProcessRole::Be,
            0,
            &runtime,
            "/new/empty.sqlite",
        )
        .expect("render be config with metadata override");

        let fe_value: toml::Value = fe.parse().expect("parse fe toml");
        let be_value: toml::Value = be.parse().expect("parse be toml");

        assert_eq!(
            fe_value["metadata"]["path"].as_str(),
            Some("/new/empty.sqlite")
        );
        assert!(fe_value.get("runtime").is_none());
        assert_eq!(
            be_value["metadata"]["path"].as_str(),
            Some("/new/empty.sqlite")
        );
        assert!(be_value.get("runtime").is_none());
    }

    #[test]
    fn render_cross_process_config_empty_base_patches_fe_heartbeat_only() {
        let runtime = make_runtime_1be();

        let fe = render_cross_process_config("", ClusterProcessRole::Fe, 0, &runtime)
            .expect("render fe config");
        let be = render_cross_process_config("", ClusterProcessRole::Be, 0, &runtime)
            .expect("render be config");

        let fe_value: toml::Value = fe.parse().expect("parse fe toml");
        let be_value: toml::Value = be.parse().expect("parse be toml");

        assert_eq!(fe_value["cluster"]["role"].as_str(), Some("fe"));
        assert_eq!(
            fe_value["cluster"]["heartbeat_interval_ms"].as_integer(),
            Some(500)
        );
        assert_eq!(
            fe_value["cluster"]["heartbeat_timeout_retries"].as_integer(),
            Some(2)
        );
        let fe_backends = fe_value["cluster"]["backends"]
            .as_array()
            .expect("fe backends array");
        assert_eq!(fe_backends.len(), 1);
        assert_eq!(fe_backends[0].as_str(), Some("127.0.0.1:19070"));

        assert_eq!(be_value["cluster"]["role"].as_str(), Some("be"));
        assert!(
            be_value
                .get("cluster")
                .and_then(|value| value.get("heartbeat_interval_ms"))
                .is_none()
        );
        assert!(
            be_value
                .get("cluster")
                .and_then(|value| value.get("heartbeat_timeout_retries"))
                .is_none()
        );
    }

    #[test]
    fn render_cross_process_config_2be_fe_has_both_backends() {
        let runtime = make_runtime_2be();

        let fe = render_cross_process_config(BASE_CONFIG, ClusterProcessRole::Fe, 0, &runtime)
            .expect("render fe config");
        let fe_value: toml::Value = fe.parse().expect("parse fe toml");

        assert_eq!(fe_value["cluster"]["role"].as_str(), Some("fe"));
        assert_eq!(
            fe_value["cluster"]["heartbeat_interval_ms"].as_integer(),
            Some(500)
        );
        assert_eq!(
            fe_value["cluster"]["heartbeat_timeout_retries"].as_integer(),
            Some(2)
        );
        let backends = fe_value["cluster"]["backends"]
            .as_array()
            .expect("fe backends array");
        assert_eq!(backends.len(), 2, "FE backends must list all 2 BEs");
        assert_eq!(backends[0].as_str(), Some("127.0.0.1:19070"));
        assert_eq!(backends[1].as_str(), Some("127.0.0.1:19071"));
    }

    #[test]
    fn render_cross_process_config_2be_each_be_has_own_ports() {
        let runtime = make_runtime_2be();

        let be0 = render_cross_process_config(BASE_CONFIG, ClusterProcessRole::Be, 0, &runtime)
            .expect("render be0 config");
        let be1 = render_cross_process_config(BASE_CONFIG, ClusterProcessRole::Be, 1, &runtime)
            .expect("render be1 config");

        let be0_value: toml::Value = be0.parse().expect("parse be0 toml");
        let be1_value: toml::Value = be1.parse().expect("parse be1 toml");

        // BE[0]
        assert_eq!(be0_value["cluster"]["role"].as_str(), Some("be"));
        assert!(
            be0_value
                .get("cluster")
                .and_then(|c| c.get("backends"))
                .is_none()
        );
        assert_eq!(be0_value["server"]["http_port"].as_integer(), Some(18080));
        assert_eq!(be0_value["server"]["grpc_port"].as_integer(), Some(19070));

        // BE[1]
        assert_eq!(be1_value["cluster"]["role"].as_str(), Some("be"));
        assert!(
            be1_value
                .get("cluster")
                .and_then(|c| c.get("backends"))
                .is_none()
        );
        assert_eq!(be1_value["server"]["http_port"].as_integer(), Some(18081));
        assert_eq!(be1_value["server"]["grpc_port"].as_integer(), Some(19071));

        // Ports must differ between the two BEs.
        assert_ne!(
            be0_value["server"]["http_port"].as_integer(),
            be1_value["server"]["http_port"].as_integer()
        );
        assert_ne!(
            be0_value["server"]["grpc_port"].as_integer(),
            be1_value["server"]["grpc_port"].as_integer()
        );
    }

    #[test]
    fn reserved_runtime_ports_new_2_yields_two_distinct_be_port_pairs() {
        let reserved = ReservedRuntimePorts::new(2).expect("reserve 2 BE port pairs");
        assert_eq!(reserved.be_ports.len(), 2);
        let http0 = reserved.be_ports[0].http.port();
        let grpc0 = reserved.be_ports[0].grpc.port();
        let http1 = reserved.be_ports[1].http.port();
        let grpc1 = reserved.be_ports[1].grpc.port();
        // All four ports must be distinct.
        let ports = [http0, grpc0, http1, grpc1];
        for i in 0..ports.len() {
            for j in (i + 1)..ports.len() {
                assert_ne!(
                    ports[i], ports[j],
                    "BE port pair ports must all be distinct: {:?}",
                    ports
                );
            }
        }
    }

    #[test]
    fn validate_cluster_args_size_zero_rejected() {
        let err = validate_cluster_args(ClusterMode::CrossProcess, 0).unwrap_err();
        assert!(
            err.to_string().contains("--cluster-size must be >= 1"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_cluster_args_all_in_one_with_size_2_rejected() {
        let err = validate_cluster_args(ClusterMode::AllInOne, 2).unwrap_err();
        assert!(
            err.to_string()
                .contains("all-in-one mode requires --cluster-size 1"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_cluster_args_cross_process_size_2_ok() {
        validate_cluster_args(ClusterMode::CrossProcess, 2)
            .expect("cluster_size=2 should be valid for cross-process");
    }

    #[test]
    fn validate_cluster_args_starrocks_compat_requires_exactly_three() {
        validate_cluster_args(ClusterMode::StarRocksCompat, 3)
            .expect("starrocks-compat requires exactly three BEs");
        for invalid in [1, 2, 4] {
            let error = validate_cluster_args(ClusterMode::StarRocksCompat, invalid)
                .expect_err("non-three compatibility topology must fail");
            assert!(
                error
                    .to_string()
                    .contains("starrocks-compat mode requires --cluster-size 3"),
                "{error:#}"
            );
        }
    }

    #[test]
    fn validate_cluster_args_all_in_one_size_1_ok() {
        validate_cluster_args(ClusterMode::AllInOne, 1)
            .expect("cluster_size=1 should be valid for all-in-one");
    }

    #[test]
    fn reserved_port_blocks_rebinding_until_release() {
        let reserved = ReservedPort::new().expect("reserve port");
        let port = reserved.port();
        assert!(TcpListener::bind(("127.0.0.1", port)).is_err());

        assert_eq!(reserved.release(), port);
    }

    #[test]
    fn runtime_dir_guard_removes_directory_on_drop_and_keeps_it_when_disarmed() {
        let repo_root = std::env::current_dir().expect("current dir");
        let runtime_root = repo_root.join("tests/sql-test-runner/.test-runtime");
        fs::create_dir_all(&runtime_root).expect("create runtime root");

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before unix epoch")
            .as_nanos();
        let dir = runtime_root.join(format!(
            "runtime_dir_guard_{}_{}",
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&dir).expect("create runtime dir");

        {
            let guard = RuntimeDirGuard::new(dir.clone());
            drop(guard);
        }
        assert!(!dir.exists(), "runtime dir should be removed on drop");

        fs::create_dir_all(&dir).expect("recreate runtime dir");
        let guard = RuntimeDirGuard::new(dir.clone());
        let dir = guard.into_path();
        assert!(
            dir.exists(),
            "disarmed runtime dir should remain for caller cleanup"
        );

        fs::remove_dir_all(&dir).expect("cleanup runtime dir");
    }

    #[test]
    fn fragment_failure_token_publish_is_complete_and_does_not_clobber_an_existing_arm() {
        let repo_root = std::env::current_dir().expect("current dir");
        let runtime_root = repo_root.join("tests/sql-test-runner/.test-runtime");
        fs::create_dir_all(&runtime_root).expect("create runtime root");
        let dir = runtime_root.join(format!(
            "fragment_failure_publish_{}_{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock before unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&dir).expect("create fragment failure test dir");
        let trigger = dir.join("be_1.fragment_failure_trigger");

        publish_fragment_failure_token(&trigger, "step-token-17")
            .expect("publish complete fragment failure token");
        assert_eq!(
            fs::read_to_string(&trigger).expect("read published trigger"),
            "step-token-17"
        );

        let error = publish_fragment_failure_token(&trigger, "replacement-token")
            .expect_err("a second arm must not replace the active trigger");
        assert!(
            format!("{error:#}").contains("publish fragment executor failure trigger"),
            "{error:#}"
        );
        assert_eq!(
            fs::read_to_string(&trigger).expect("read original trigger"),
            "step-token-17"
        );
        fs::remove_dir_all(dir).expect("cleanup fragment failure test dir");
    }
}
