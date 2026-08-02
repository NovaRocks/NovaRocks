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

use crate::managed_process::{ManagedProcess, ReadyMarker};
use crate::types::{QueryLifecyclePhase, RunnerConfig};
use anyhow::{Context, Result, bail};
use clap::ValueEnum;
use mysql::prelude::Queryable;
use mysql::{Conn as MysqlConn, OptsBuilder};
use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
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
    start_epoch: u64,
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
    let start_epoch = values
        .get(5)
        .context("SHOW BACKENDS row missing StartEpoch")?
        .parse::<u64>()
        .context("parse SHOW BACKENDS StartEpoch")?;
    Ok(BackendTopologyRow {
        grpc_port,
        state,
        alive,
        scheduled_fragments,
        start_epoch,
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
        // The synchronous mysql client maps macOS socket read/write timeouts
        // to EAGAIN while decoding a valid response. The enclosing topology
        // barrier owns the deadline; retain only a bounded connect timeout.
        .tcp_connect_timeout(Some(io_timeout));
    let mut conn = MysqlConn::new(builder)
        .with_context(|| format!("connect to cross-process FE MySQL at {host}:{port}"))?;
    let rows: Vec<mysql::Row> = conn
        .query("SHOW BACKENDS")
        .context("query SHOW BACKENDS from cross-process FE")?;
    rows.into_iter()
        .map(|row| {
            let values = (0..6)
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
const RESOURCE_CONVERGENCE_POLL_INTERVAL: Duration = Duration::from_millis(100);
const QUERY_EXECUTION_RESOURCE_METRIC: &str = "novarocks_backend_query_execution_resources";
const QUERY_LIFECYCLE_TERMINAL_METRIC: &str = "novarocks_backend_query_lifecycle_terminal_total";

const HEAVY_QUERY_EXECUTION_RESOURCES: [&str; 10] = [
    "stage_active_builders",
    "stage_encoded_bytes",
    "stage_dormant_workers",
    "fragment_controls_reserved",
    "fragment_controls_running",
    "native_query_contexts_active",
    "native_query_contexts_second_chance",
    "native_query_active_fragments",
    "native_runtime_filter_services",
    "connector_query_leases",
];

const QUERY_EXECUTION_RESOURCE_BINDING_LEASE: &str = "connector_binding_leases";
const TERMINAL_RETAINED_OUTCOME: &str = "terminal_retained";
const TERMINAL_RETAINED_BYTES_OUTCOME: &str = "terminal_retained_bytes";
const TERMINAL_RETAINED_CAPACITY_OUTCOME: &str = "terminal_retained_capacity";
const TERMINAL_MAX_RETAINED_BYTES_OUTCOME: &str = "terminal_max_retained_bytes";

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BackendResourceSnapshot {
    pub(crate) index: usize,
    pub(crate) process_running: bool,
    pub(crate) resources: BTreeMap<String, f64>,
    pub(crate) terminal_retained: f64,
    pub(crate) terminal_retained_bytes: f64,
    pub(crate) terminal_retained_capacity: f64,
    pub(crate) terminal_max_retained_bytes: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct QueryExecutionResourceSnapshot {
    pub(crate) fe_running: bool,
    pub(crate) backends: Vec<BackendResourceSnapshot>,
}

impl QueryExecutionResourceSnapshot {
    fn convergence_failure(&self, baseline: &Self, permits_terminal_retention: bool) -> Option<String> {
        if self.backends.len() != baseline.backends.len() {
            return Some(format!(
                "backend cardinality changed: before={} current={}",
                baseline.backends.len(),
                self.backends.len()
            ));
        }
        let mut deltas = Vec::new();
        for (before, current) in baseline.backends.iter().zip(&self.backends) {
            if before.index != current.index {
                return Some(format!(
                    "backend ordering changed: before BE[{}] current BE[{}]",
                    before.index, current.index
                ));
            }
            if !current.process_running {
                // A killed BE that has not restarted proves heavy-resource release by
                // process exit; do not misclassify that as a metrics scrape failure.
                continue;
            }
            for (resource, before_value) in &before.resources {
                let current_value = current.resources.get(resource).copied().unwrap_or(f64::NAN);
                if current_value != *before_value {
                    deltas.push(format!(
                        "BE[{}] {resource}: before={before_value} current={current_value} delta={}",
                        current.index,
                        current_value - before_value
                    ));
                }
            }
            if self.fe_running
                && !permits_terminal_retention
                && (current.terminal_retained > before.terminal_retained
                    || current.terminal_retained_bytes > before.terminal_retained_bytes)
            {
                deltas.push(format!(
                    "BE[{}] terminal retention grew above baseline: before=({}, {}) current=({}, {})",
                    current.index,
                    before.terminal_retained,
                    before.terminal_retained_bytes,
                    current.terminal_retained,
                    current.terminal_retained_bytes
                ));
            }
            if (!self.fe_running || permits_terminal_retention)
                && (current.terminal_retained > current.terminal_retained_capacity
                    || current.terminal_retained_bytes > current.terminal_max_retained_bytes)
            {
                deltas.push(format!(
                    "BE[{}] terminal retention exceeds published limit: retained=({}, {}) limits=({}, {})",
                    current.index,
                    current.terminal_retained,
                    current.terminal_retained_bytes,
                    current.terminal_retained_capacity,
                    current.terminal_max_retained_bytes
                ));
            }
        }
        (!deltas.is_empty()).then(|| deltas.join("; "))
    }
}

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

fn remaining_until(deadline: Instant, operation: &str) -> Result<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        bail!("30s query lifecycle fault deadline expired before {operation}");
    }
    Ok(remaining)
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
    timeout: Duration,
) -> Result<()> {
    let expected_ports = runtime.be.iter().map(|be| be.grpc).collect::<Vec<_>>();
    let expected = expected_ports.len();
    let host = "127.0.0.1";
    let port = runtime.fe_mysql_port;
    let rows = wait_for_live_backend_topology_with(
        &expected_ports,
        timeout,
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

fn scrape_prometheus_metrics(port: u16) -> Result<String> {
    let address = format!("127.0.0.1:{port}");
    let mut stream = TcpStream::connect_timeout(
        &address
            .parse()
            .with_context(|| format!("parse BE metrics address {address}"))?,
        TOPOLOGY_MYSQL_IO_TIMEOUT_CAP,
    )
    .with_context(|| format!("connect BE metrics endpoint {address}"))?;
    stream
        .set_read_timeout(Some(TOPOLOGY_MYSQL_IO_TIMEOUT_CAP))
        .context("set BE metrics read timeout")?;
    stream
        .set_write_timeout(Some(TOPOLOGY_MYSQL_IO_TIMEOUT_CAP))
        .context("set BE metrics write timeout")?;
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
        .context("request BE /metrics")?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .context("read BE /metrics response")?;
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .context("malformed BE /metrics HTTP response")?;
    if !headers.starts_with("HTTP/1.1 200") && !headers.starts_with("HTTP/1.0 200") {
        bail!(
            "BE /metrics returned non-success status: {}",
            headers.lines().next().unwrap_or("<missing status>")
        );
    }
    Ok(body.to_string())
}

fn prometheus_labeled_gauge(
    body: &str,
    metric: &str,
    label_name: &str,
    label_value: &str,
) -> Result<f64> {
    let label = format!("{label_name}=\"{label_value}\"");
    let mut values = body
        .lines()
        .filter(|line| line.starts_with(metric))
        .filter(|line| line.contains(&label))
        .filter_map(|line| line.split_whitespace().nth(1))
        .map(|value| value.parse::<f64>())
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("parse {metric} label {label}"))?;
    match values.len() {
        1 => Ok(values.remove(0)),
        0 => bail!("missing required {metric}{{{label}}} gauge in BE /metrics"),
        count => bail!("ambiguous {metric}{{{label}}} gauge in BE /metrics: {count} samples"),
    }
}

pub(crate) trait ServerHandle: Send {
    fn target_host(&self) -> Option<&str>;
    fn target_port(&self) -> Option<u16>;
    fn supports_fault_injection(&self) -> bool {
        false
    }
    fn supports_query_execution_resource_oracle(&self) -> bool {
        false
    }
    fn query_execution_resource_snapshot(
        &mut self,
    ) -> Result<Option<QueryExecutionResourceSnapshot>> {
        Ok(None)
    }
    fn query_execution_resource_diagnostics(&self) -> String {
        "resource diagnostics unavailable for this server mode".to_string()
    }
    fn await_query_execution_resource_convergence(
        &mut self,
        baseline: &QueryExecutionResourceSnapshot,
        permits_terminal_retention: bool,
        deadline: Instant,
    ) -> Result<()> {
        loop {
            let current = match self.query_execution_resource_snapshot() {
                Ok(Some(snapshot)) => snapshot,
                Ok(None) => {
                    let error = anyhow::anyhow!("query execution resource oracle is unavailable");
                    if Instant::now() >= deadline {
                        return Err(error.context(self.query_execution_resource_diagnostics()));
                    }
                    thread::sleep(
                        deadline
                            .saturating_duration_since(Instant::now())
                            .min(RESOURCE_CONVERGENCE_POLL_INTERVAL),
                    );
                    continue;
                }
                Err(error) => {
                    if Instant::now() >= deadline {
                        return Err(error.context(self.query_execution_resource_diagnostics()));
                    }
                    thread::sleep(
                        deadline
                            .saturating_duration_since(Instant::now())
                            .min(RESOURCE_CONVERGENCE_POLL_INTERVAL),
                    );
                    continue;
                }
            };
            if let Some(failure) = current.convergence_failure(baseline, permits_terminal_retention) {
                if Instant::now() < deadline {
                    thread::sleep(
                        deadline
                            .saturating_duration_since(Instant::now())
                            .min(RESOURCE_CONVERGENCE_POLL_INTERVAL),
                    );
                    continue;
                }
                bail!(
                    "query execution resources did not converge before deadline: {failure}; baseline={baseline:?}; current={current:?}; {}",
                    self.query_execution_resource_diagnostics()
                );
            }
            return Ok(());
        }
    }
    fn kill_be(&mut self, index: usize) -> Result<()> {
        bail!("BE kill is unsupported by this server mode (index={index})")
    }
    fn restart_be(&mut self, index: usize) -> Result<()> {
        bail!("BE restart is unsupported by this server mode (index={index})")
    }
    fn restart_be_until(&mut self, index: usize, _deadline: Instant) -> Result<()> {
        self.restart_be(index)
    }
    fn kill_fe(&mut self) -> Result<()> {
        bail!("FE kill is unsupported by this server mode")
    }
    fn restart_fe(&mut self) -> Result<()> {
        bail!("FE restart is unsupported by this server mode")
    }
    fn restart_fe_until(&mut self, _deadline: Instant) -> Result<()> {
        self.restart_fe()
    }
    fn kill_query(&mut self, connection_id: u32) -> Result<()> {
        bail!("KILL QUERY is unsupported by this server mode (connection_id={connection_id})")
    }
    fn kill_query_until(&mut self, connection_id: u32, _deadline: Instant) -> Result<()> {
        self.kill_query(connection_id)
    }
    fn backend_start_epoch(&self, index: usize) -> Result<u64> {
        bail!("backend start epoch is unsupported by this server mode (index={index})")
    }
    fn fe_log_count(&self, needle: &str) -> Result<usize> {
        bail!("FE log counting is unsupported by this server mode (pattern={needle:?})")
    }
    fn fe_log_contents(&self) -> Result<String> {
        bail!("FE log reading is unsupported by this server mode")
    }
    fn clear_query_lifecycle_faults(&mut self) -> Result<()> {
        Ok(())
    }
    fn release_query_lifecycle_phase_fault(
        &mut self,
        phase: QueryLifecyclePhase,
        fe_crash: bool,
    ) -> Result<()> {
        bail!(
            "lifecycle phase fault release is unsupported by this server mode (phase={}, fe_crash={fe_crash})",
            phase.as_str()
        )
    }
    fn armed_query_lifecycle_fault_token(
        &self,
        index: usize,
        kind: &'static str,
    ) -> Result<Option<String>> {
        bail!(
            "query lifecycle fault token is unsupported by this server mode (index={index}, kind={kind})"
        )
    }
    fn arm_init_ack_drop(&mut self, index: usize) -> Result<()> {
        bail!("InitAck drop is unsupported by this server mode (index={index})")
    }
    fn arm_query_control_heartbeat_stop(&mut self, index: usize) -> Result<()> {
        bail!("query-control heartbeat stop is unsupported by this server mode (index={index})")
    }
    fn arm_fe_crash_after_control_ready(&mut self, count: usize) -> Result<()> {
        bail!("FE crash is unsupported by this server mode (ready_count={count})")
    }
    fn arm_be_restart_after_init_ack(&mut self, index: usize) -> Result<()> {
        bail!("BE restart-after-InitAck is unsupported by this server mode (index={index})")
    }
    fn arm_stage_prepare_failure(&mut self, ordinal: usize) -> Result<()> {
        bail!("Stage prepare failure is unsupported by this server mode (ordinal={ordinal})")
    }
    fn arm_stage_ack_drop(&mut self, index: usize) -> Result<()> {
        bail!("StageAck drop is unsupported by this server mode (index={index})")
    }
    fn arm_start_ack_drop(&mut self, index: usize) -> Result<()> {
        bail!("StartAck drop is unsupported by this server mode (index={index})")
    }
    fn arm_start_ack_suppress(&mut self, index: usize) -> Result<()> {
        bail!("StartAck suppression is unsupported by this server mode (index={index})")
    }
    fn arm_terminal_ack_drop(&mut self, index: usize) -> Result<()> {
        bail!("TerminalAck drop is unsupported by this server mode (index={index})")
    }
    fn arm_terminal_snapshot_stream_drop(&mut self, index: usize) -> Result<()> {
        bail!("TerminalSnapshot stream drop is unsupported by this server mode (index={index})")
    }
    fn arm_terminal_snapshot_conflict(&mut self, index: usize) -> Result<()> {
        bail!("TerminalSnapshot conflict is unsupported by this server mode (index={index})")
    }
    fn arm_kill_query_at_lifecycle_phase(&mut self, phase: QueryLifecyclePhase) -> Result<()> {
        bail!(
            "KILL QUERY lifecycle phase fault is unsupported by this server mode (phase={})",
            phase.as_str()
        )
    }
    fn arm_fe_crash_at_lifecycle_phase(&mut self, phase: QueryLifecyclePhase) -> Result<()> {
        bail!(
            "FE lifecycle phase fault is unsupported by this server mode (phase={})",
            phase.as_str()
        )
    }
    fn arm_query_control_heartbeat_stop_after_stage(&mut self, index: usize) -> Result<()> {
        bail!(
            "query-control heartbeat stop-after-stage is unsupported by this server mode (index={index})"
        )
    }
    fn arm_hold_start_until_early_ingress(&mut self) -> Result<()> {
        bail!("Start hold until early ingress is unsupported by this server mode")
    }
    fn arm_query_control_fragment_backend_limit(&mut self, limit: usize) -> Result<()> {
        bail!(
            "query-control fragment backend limit is unsupported by this server mode (limit={limit})"
        )
    }
    fn be_count(&self) -> usize {
        0
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
    fn be_current_log_contents(&self, index: usize) -> Result<String> {
        self.be_log_contents(index)
    }
    #[allow(dead_code)]
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
    query_lifecycle_faults_enabled: bool,
) -> Result<Box<dyn ServerHandle>> {
    match mode {
        ClusterMode::AllInOne => Ok(Box::new(NoopServerHandle)),
        ClusterMode::CrossProcess => Ok(Box::new(CrossProcessServerHandle::launch(
            cluster_size,
            repo_root,
            runner_config,
            query_lifecycle_faults_enabled,
        )?)),
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

    bail!(
        "failed to locate standalone config for cross-process mode under {}",
        repo_root.display()
    )
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
/// actually lives. This is deliberately **not** the retired
/// `[standalone_server].metadata_db_path` internal-table key.
///
/// Used by the L2 cross-process empty-metadata statelessness harness to point
/// a second FE launch at a fresh, empty SQLite path while keeping every other
/// section (server ports and connector configuration) identical to a normal
/// `render_cross_process_config` render — so the second launch talks to the
/// same lake but starts with no cached IMV definitions.
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
    query_lifecycle_faults_enabled: bool,
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
        .context("parse rendered cross-process launch config")?;
    let root = value
        .as_table_mut()
        .context("rendered cross-process launch config root must be a TOML table")?;
    // `role = fe` persists backend membership in StateStore.  Isolating only
    // `[metadata].path` leaves an ephemeral SQL-test FE restoring membership
    // rows from a previous launch, whose dynamically allocated BE endpoints
    // are necessarily stale.
    if role == ClusterProcessRole::Fe {
        let state_store = root
            .get_mut("state_store")
            .and_then(Value::as_table_mut)
            .context("cross-process FE config requires [state_store]")?;
        state_store.insert(
            "path".to_string(),
            Value::String(
                runtime_dir
                    .join("frontend-state.sqlite")
                    .to_string_lossy()
                    .into_owned(),
            ),
        );
    }
    if query_lifecycle_faults_enabled {
        let debug = table_mut(root, "debug");
        debug.insert(
            "query_lifecycle_fault_dir".to_string(),
            Value::String(
                runtime_dir
                    .join("query-lifecycle-faults")
                    .to_string_lossy()
                    .into_owned(),
            ),
        );
        // The production terminal-retention contract remains 120s.  Runner
        // fault scenarios use a short, self-contained lease so a deliberately
        // crashed FE proves BE runtime release and bounded record reclamation
        // without turning the distributed suite into a two-minute sleep.
        let runtime_table = table_mut(root, "runtime");
        runtime_table.insert(
            "query_control_terminal_ack_timeout_ms".to_string(),
            Value::Integer(500),
        );
        runtime_table.insert(
            "query_control_terminal_fallback_rpc_timeout_ms".to_string(),
            Value::Integer(500),
        );
        runtime_table.insert(
            "query_control_terminal_fallback_initial_backoff_ms".to_string(),
            Value::Integer(50),
        );
        runtime_table.insert(
            "query_control_terminal_fallback_max_backoff_ms".to_string(),
            Value::Integer(100),
        );
        runtime_table.insert(
            "query_control_terminal_retention_ms".to_string(),
            Value::Integer(2_000),
        );
    }
    toml::to_string(&value).context("serialize cross-process launch config")
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

struct QueryLifecycleFaultFiles {
    root: PathBuf,
    be_count: usize,
}

impl QueryLifecycleFaultFiles {
    fn new(root: &Path, be_count: usize) -> Result<Self> {
        if be_count == 0 {
            bail!("query lifecycle fault scope requires at least one BE");
        }
        fs::create_dir_all(root)
            .with_context(|| format!("create query lifecycle fault scope {}", root.display()))?;
        Ok(Self {
            root: root.to_path_buf(),
            be_count,
        })
    }

    fn root(&self) -> &Path {
        &self.root
    }

    fn init_ack_drop_path(&self, index: usize) -> Result<PathBuf> {
        self.be_path(index, "init-ack-drop")
    }

    fn heartbeat_stop_path(&self, index: usize) -> Result<PathBuf> {
        self.be_path(index, "heartbeat-stop")
    }

    fn restart_after_init_ack_path(&self, index: usize) -> Result<PathBuf> {
        self.be_path(index, "restart-after-init-ack")
    }

    fn stage_ack_drop_path(&self, index: usize) -> Result<PathBuf> {
        self.be_path(index, "stage-ack-drop")
    }

    fn start_ack_drop_path(&self, index: usize) -> Result<PathBuf> {
        self.be_path(index, "start-ack-drop")
    }

    fn start_ack_suppress_path(&self, index: usize) -> Result<PathBuf> {
        self.be_path(index, "start-ack-suppress")
    }

    fn terminal_ack_drop_path(&self, index: usize) -> Result<PathBuf> {
        self.be_path(index, "terminal-ack-drop")
    }

    fn terminal_snapshot_stream_drop_path(&self, index: usize) -> Result<PathBuf> {
        self.be_path(index, "terminal-snapshot-stream-drop")
    }

    fn terminal_snapshot_conflict_path(&self, index: usize) -> Result<PathBuf> {
        self.be_path(index, "terminal-snapshot-conflict")
    }

    fn heartbeat_stop_after_stage_path(&self, index: usize) -> Result<PathBuf> {
        self.be_path(index, "heartbeat-stop-after-stage")
    }

    fn fe_crash_path(&self) -> PathBuf {
        self.root.join("fe-crash-after-control-ready.trigger")
    }

    fn fragment_backend_limit_path(&self) -> PathBuf {
        self.root.join("fragment-backend-limit.trigger")
    }

    fn stage_prepare_failure_path(&self) -> PathBuf {
        self.root.join("stage-prepare-fail.trigger")
    }

    fn kill_query_at_phase_path(&self, phase: QueryLifecyclePhase) -> PathBuf {
        self.root
            .join(format!("kill-query-at-{}.trigger", phase.as_str()))
    }

    fn fe_crash_at_phase_path(&self, phase: QueryLifecyclePhase) -> PathBuf {
        self.root
            .join(format!("fe-crash-at-{}.trigger", phase.as_str()))
    }

    fn hold_start_until_early_ingress_path(&self) -> PathBuf {
        self.root.join("hold-start-until-early-ingress.trigger")
    }

    fn publish_init_ack_drop(&self, index: usize) -> Result<String> {
        self.publish(self.init_ack_drop_path(index)?, index, None)
    }

    fn publish_heartbeat_stop(&self, index: usize) -> Result<String> {
        self.publish(self.heartbeat_stop_path(index)?, index, None)
    }

    fn publish_restart_after_init_ack(&self, index: usize) -> Result<String> {
        self.publish(self.restart_after_init_ack_path(index)?, index, None)
    }

    fn publish_stage_ack_drop(&self, index: usize) -> Result<String> {
        self.publish(self.stage_ack_drop_path(index)?, index, None)
    }

    fn publish_start_ack_drop(&self, index: usize) -> Result<String> {
        self.publish(self.start_ack_drop_path(index)?, index, None)
    }

    fn publish_start_ack_suppress(&self, index: usize) -> Result<String> {
        self.publish(self.start_ack_suppress_path(index)?, index, None)
    }

    fn publish_terminal_ack_drop(&self, index: usize) -> Result<String> {
        self.publish(self.terminal_ack_drop_path(index)?, index, None)
    }

    fn publish_terminal_snapshot_stream_drop(&self, index: usize) -> Result<String> {
        self.publish(self.terminal_snapshot_stream_drop_path(index)?, index, None)
    }

    fn publish_terminal_snapshot_conflict(&self, index: usize) -> Result<String> {
        self.publish(self.terminal_snapshot_conflict_path(index)?, index, None)
    }

    fn publish_heartbeat_stop_after_stage(&self, index: usize) -> Result<String> {
        self.publish(self.heartbeat_stop_after_stage_path(index)?, index, None)
    }

    fn publish_fe_crash(&self, count: usize) -> Result<String> {
        self.publish(self.fe_crash_path(), self.be_count, Some(count))
    }

    fn publish_fragment_backend_limit(&self, limit: usize) -> Result<String> {
        self.publish(
            self.fragment_backend_limit_path(),
            self.be_count,
            Some(limit),
        )
    }

    fn publish_stage_prepare_failure(&self, ordinal: usize) -> Result<String> {
        self.publish_fields(
            self.stage_prepare_failure_path(),
            self.be_count,
            "ordinal",
            ordinal,
        )
    }

    fn publish_kill_query_at_phase(&self, phase: QueryLifecyclePhase) -> Result<String> {
        self.publish_fields(
            self.kill_query_at_phase_path(phase),
            self.be_count,
            "phase",
            phase.as_str(),
        )
    }

    fn publish_fe_crash_at_phase(&self, phase: QueryLifecyclePhase) -> Result<String> {
        self.publish_fields(
            self.fe_crash_at_phase_path(phase),
            self.be_count,
            "phase",
            phase.as_str(),
        )
    }

    fn publish_hold_start_until_early_ingress(&self) -> Result<String> {
        self.publish_fields(
            self.hold_start_until_early_ingress_path(),
            self.be_count,
            "enabled",
            "true",
        )
    }

    fn publish(&self, path: PathBuf, identity: usize, value: Option<usize>) -> Result<String> {
        let token = next_fragment_failure_token(identity);
        let contents = match value {
            Some(value) => format!("{token}\n{value}\n"),
            None => format!("token={token}\nbackend_index={identity}\n"),
        };
        publish_query_lifecycle_fault_token(&path, &token, contents.as_bytes())?;
        Ok(token)
    }

    fn publish_fields(
        &self,
        path: PathBuf,
        identity: usize,
        field: &str,
        value: impl std::fmt::Display,
    ) -> Result<String> {
        let token = next_fragment_failure_token(identity);
        let contents = format!("token={token}\n{field}={value}\n");
        publish_query_lifecycle_fault_token(&path, &token, contents.as_bytes())?;
        Ok(token)
    }

    fn clear(&self) -> Result<()> {
        for entry in fs::read_dir(&self.root)
            .with_context(|| format!("read query lifecycle fault scope {}", self.root.display()))?
        {
            let path = entry?.path();
            if path.is_file() {
                remove_fragment_failure_file(&path).with_context(|| {
                    format!("remove query lifecycle fault trigger {}", path.display())
                })?;
            }
        }
        Ok(())
    }

    fn be_path(&self, index: usize, kind: &str) -> Result<PathBuf> {
        if index >= self.be_count {
            bail!(
                "BE index {index} is out of bounds for query lifecycle fault scope with {} BE(s)",
                self.be_count
            );
        }
        Ok(self.root.join(format!("be-{index}.{kind}.arm")))
    }
}

impl Drop for QueryLifecycleFaultFiles {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

pub(crate) struct CrossProcessServerHandle {
    target_host: String,
    target_port: u16,
    mysql_user: String,
    be_grpc_ports: Vec<u16>,
    fragment_failure_trigger_paths: Vec<PathBuf>,
    fragment_failure_tokens: Vec<Option<String>>,
    query_lifecycle_fault_files: QueryLifecycleFaultFiles,
    query_lifecycle_fault_tokens: BTreeMap<(usize, &'static str), String>,
    query_lifecycle_faults_enabled: bool,
    runtime_dir: PathBuf,
    runtime: CrossProcessRuntime,
    novarocks_bin: PathBuf,
    be_config_paths: Vec<PathBuf>,
    fe_config_path: PathBuf,
    be_processes: Vec<ManagedProcess>,
    fe_process: ManagedProcess,
    be_log_history: Vec<String>,
    fe_log_history: String,
}

#[derive(Clone, Copy)]
enum CrossProcessMetadataMode<'a> {
    /// Ephemeral SQL-test clusters, including IMV L2 cluster A, must never
    /// restore backend rows from another launch whose dynamically reserved
    /// endpoints are already stale.
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
        query_lifecycle_faults_enabled: bool,
    ) -> Result<Self> {
        Self::launch_impl(
            cluster_size,
            repo_root,
            runner_config,
            CrossProcessMetadataMode::Isolated,
            query_lifecycle_faults_enabled,
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
            false,
        )
    }

    fn launch_impl(
        cluster_size: usize,
        repo_root: &Path,
        runner_config: &RunnerConfig,
        metadata_mode: CrossProcessMetadataMode<'_>,
        query_lifecycle_faults_enabled: bool,
    ) -> Result<Self> {
        let runtime_dir = RuntimeDirGuard::new(create_runtime_dir(repo_root)?);
        let reserved = ReservedRuntimePorts::new(cluster_size)?;
        let query_lifecycle_fault_files = QueryLifecycleFaultFiles::new(
            &runtime_dir.path().join("query-lifecycle-faults"),
            cluster_size,
        )?;

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
                query_lifecycle_faults_enabled,
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
                query_lifecycle_faults_enabled
                    .then_some((query_lifecycle_fault_files.root(), Some(i))),
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
            query_lifecycle_faults_enabled.then_some((query_lifecycle_fault_files.root(), None)),
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
            startup_timeout(),
        )
        .context("cross-process backend topology barrier")?;

        Ok(Self {
            target_host: "127.0.0.1".to_string(),
            target_port: runtime.fe_mysql_port,
            mysql_user,
            be_grpc_ports: runtime.be.iter().map(|be| be.grpc).collect(),
            fragment_failure_trigger_paths,
            fragment_failure_tokens: vec![None; cluster_size],
            query_lifecycle_fault_files,
            query_lifecycle_fault_tokens: BTreeMap::new(),
            query_lifecycle_faults_enabled,
            runtime_dir: runtime_dir.into_path(),
            runtime,
            novarocks_bin,
            be_config_paths,
            fe_config_path,
            be_processes,
            fe_process,
            be_log_history: vec![String::new(); cluster_size],
            fe_log_history: String::new(),
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

    fn query_execution_resource_snapshot_impl(&mut self) -> Result<QueryExecutionResourceSnapshot> {
        let fe_running = self
            .fe_process
            .is_running()
            .context("inspect FE process state")?;
        let mut backends = Vec::with_capacity(self.be_processes.len());
        for (index, (process, ports)) in self
            .be_processes
            .iter()
            .zip(self.runtime.be.iter())
            .enumerate()
        {
            let process_running = process
                .is_running()
                .with_context(|| format!("inspect cross-process BE[{index}] state"))?;
            if !process_running {
                backends.push(BackendResourceSnapshot {
                    index,
                    process_running,
                    resources: BTreeMap::new(),
                    terminal_retained: 0.0,
                    terminal_retained_bytes: 0.0,
                    terminal_retained_capacity: 0.0,
                    terminal_max_retained_bytes: 0.0,
                });
                continue;
            }

            let metrics = scrape_prometheus_metrics(ports.http)
                .with_context(|| format!("scrape cross-process BE[{index}] /metrics"))?;
            let mut resources = BTreeMap::new();
            for resource in HEAVY_QUERY_EXECUTION_RESOURCES
                .into_iter()
                .chain(std::iter::once(QUERY_EXECUTION_RESOURCE_BINDING_LEASE))
            {
                let value = prometheus_labeled_gauge(
                    &metrics,
                    QUERY_EXECUTION_RESOURCE_METRIC,
                    "resource",
                    resource,
                )
                .with_context(|| format!("read BE[{index}] heavy resource {resource}"))?;
                resources.insert(resource.to_string(), value);
            }
            backends.push(BackendResourceSnapshot {
                index,
                process_running,
                resources,
                terminal_retained: prometheus_labeled_gauge(
                    &metrics,
                    QUERY_LIFECYCLE_TERMINAL_METRIC,
                    "outcome",
                    TERMINAL_RETAINED_OUTCOME,
                )
                .with_context(|| format!("read BE[{index}] terminal retained count"))?,
                terminal_retained_bytes: prometheus_labeled_gauge(
                    &metrics,
                    QUERY_LIFECYCLE_TERMINAL_METRIC,
                    "outcome",
                    TERMINAL_RETAINED_BYTES_OUTCOME,
                )
                .with_context(|| format!("read BE[{index}] terminal retained bytes"))?,
                terminal_retained_capacity: prometheus_labeled_gauge(
                    &metrics,
                    QUERY_LIFECYCLE_TERMINAL_METRIC,
                    "outcome",
                    TERMINAL_RETAINED_CAPACITY_OUTCOME,
                )
                .with_context(|| format!("read BE[{index}] terminal retained capacity"))?,
                terminal_max_retained_bytes: prometheus_labeled_gauge(
                    &metrics,
                    QUERY_LIFECYCLE_TERMINAL_METRIC,
                    "outcome",
                    TERMINAL_MAX_RETAINED_BYTES_OUTCOME,
                )
                .with_context(|| format!("read BE[{index}] terminal retained byte limit"))?,
            });
        }
        Ok(QueryExecutionResourceSnapshot {
            fe_running,
            backends,
        })
    }

    fn query_execution_resource_diagnostics_impl(&self) -> String {
        let tail = |contents: Result<String>| match contents {
            Ok(contents) => contents
                .lines()
                .rev()
                .take(20)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>()
                .join("\\n"),
            Err(error) => format!("<read failed: {error:#}>"),
        };
        let fe_state = self
            .fe_process
            .is_running()
            .map(|running| if running { "running" } else { "exited" })
            .unwrap_or("unknown");
        let be = self
            .be_processes
            .iter()
            .enumerate()
            .map(|(index, process)| {
                let state = process
                    .is_running()
                    .map(|running| if running { "running" } else { "exited" })
                    .unwrap_or("unknown");
                format!(
                    "BE[{index}]={state} log_tail={:?}",
                    tail(process.log_contents())
                )
            })
            .collect::<Vec<_>>();
        format!(
            "FE={fe_state} log_tail={:?}; {}",
            tail(self.fe_process.log_contents()),
            be.join("; ")
        )
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

    fn supports_query_execution_resource_oracle(&self) -> bool {
        true
    }

    fn query_execution_resource_snapshot(
        &mut self,
    ) -> Result<Option<QueryExecutionResourceSnapshot>> {
        self.query_execution_resource_snapshot_impl().map(Some)
    }

    fn query_execution_resource_diagnostics(&self) -> String {
        self.query_execution_resource_diagnostics_impl()
    }

    fn be_count(&self) -> usize {
        self.be_processes.len()
    }

    fn arm_init_ack_drop(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        let token = self
            .query_lifecycle_fault_files
            .publish_init_ack_drop(index)?;
        self.query_lifecycle_fault_tokens
            .insert((index, "init-ack-drop"), token.clone());
        println!(
            "armed InitAck drop for cross-process BE[{index}] token={token} trigger={}",
            self.query_lifecycle_fault_files
                .init_ack_drop_path(index)?
                .display()
        );
        Ok(())
    }

    fn arm_query_control_heartbeat_stop(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        let token = self
            .query_lifecycle_fault_files
            .publish_heartbeat_stop(index)?;
        self.query_lifecycle_fault_tokens
            .insert((index, "heartbeat-stop"), token.clone());
        println!(
            "armed query-control heartbeat stop for cross-process BE[{index}] token={token} trigger={}",
            self.query_lifecycle_fault_files
                .heartbeat_stop_path(index)?
                .display()
        );
        Ok(())
    }

    fn arm_fe_crash_after_control_ready(&mut self, count: usize) -> Result<()> {
        if !(1..=self.be_processes.len()).contains(&count) {
            bail!(
                "FE crash ControlReady count {count} is outside 1..={}",
                self.be_processes.len()
            );
        }
        let token = self.query_lifecycle_fault_files.publish_fe_crash(count)?;
        println!(
            "armed FE crash after {count} ControlReady marker(s) token={token} trigger={}",
            self.query_lifecycle_fault_files.fe_crash_path().display()
        );
        Ok(())
    }

    fn arm_be_restart_after_init_ack(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        let token = self
            .query_lifecycle_fault_files
            .publish_restart_after_init_ack(index)?;
        self.query_lifecycle_fault_tokens
            .insert((index, "restart-after-init-ack"), token.clone());
        println!(
            "armed BE[{index}] restart after InitAck token={token} trigger={}",
            self.query_lifecycle_fault_files
                .restart_after_init_ack_path(index)?
                .display()
        );
        Ok(())
    }

    fn arm_stage_prepare_failure(&mut self, ordinal: usize) -> Result<()> {
        if ordinal == 0 {
            bail!("Stage prepare ordinal must be at least 1");
        }
        let token = self
            .query_lifecycle_fault_files
            .publish_stage_prepare_failure(ordinal)?;
        println!(
            "armed Stage prepare failure at ordinal={ordinal} token={token} trigger={}",
            self.query_lifecycle_fault_files
                .stage_prepare_failure_path()
                .display()
        );
        Ok(())
    }

    fn arm_stage_ack_drop(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        let token = self
            .query_lifecycle_fault_files
            .publish_stage_ack_drop(index)?;
        self.query_lifecycle_fault_tokens
            .insert((index, "stage-ack-drop"), token.clone());
        println!(
            "armed StageAck drop for cross-process BE[{index}] token={token} trigger={}",
            self.query_lifecycle_fault_files
                .stage_ack_drop_path(index)?
                .display()
        );
        Ok(())
    }

    fn arm_start_ack_drop(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        let token = self
            .query_lifecycle_fault_files
            .publish_start_ack_drop(index)?;
        self.query_lifecycle_fault_tokens
            .insert((index, "start-ack-drop"), token.clone());
        println!(
            "armed StartAck drop for cross-process BE[{index}] token={token} trigger={}",
            self.query_lifecycle_fault_files
                .start_ack_drop_path(index)?
                .display()
        );
        Ok(())
    }

    fn arm_start_ack_suppress(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        let token = self
            .query_lifecycle_fault_files
            .publish_start_ack_suppress(index)?;
        self.query_lifecycle_fault_tokens
            .insert((index, "start-ack-suppress"), token.clone());
        println!(
            "armed StartAck suppression for cross-process BE[{index}] token={token} trigger={}",
            self.query_lifecycle_fault_files
                .start_ack_suppress_path(index)?
                .display()
        );
        Ok(())
    }

    fn arm_terminal_ack_drop(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        let token = self
            .query_lifecycle_fault_files
            .publish_terminal_ack_drop(index)?;
        self.query_lifecycle_fault_tokens
            .insert((index, "terminal-ack-drop"), token.clone());
        println!(
            "armed TerminalAck drop for cross-process BE[{index}] token={token} trigger={}",
            self.query_lifecycle_fault_files
                .terminal_ack_drop_path(index)?
                .display()
        );
        Ok(())
    }

    fn arm_terminal_snapshot_stream_drop(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        let token = self
            .query_lifecycle_fault_files
            .publish_terminal_snapshot_stream_drop(index)?;
        self.query_lifecycle_fault_tokens
            .insert((index, "terminal-snapshot-stream-drop"), token.clone());
        println!(
            "armed TerminalSnapshot stream drop for cross-process BE[{index}] token={token} trigger={}",
            self.query_lifecycle_fault_files
                .terminal_snapshot_stream_drop_path(index)?
                .display()
        );
        Ok(())
    }

    fn arm_terminal_snapshot_conflict(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        let token = self
            .query_lifecycle_fault_files
            .publish_terminal_snapshot_conflict(index)?;
        self.query_lifecycle_fault_tokens
            .insert((index, "terminal-snapshot-conflict"), token.clone());
        println!(
            "armed TerminalSnapshot conflict for cross-process BE[{index}] token={token} trigger={}",
            self.query_lifecycle_fault_files
                .terminal_snapshot_conflict_path(index)?
                .display()
        );
        Ok(())
    }

    fn arm_kill_query_at_lifecycle_phase(&mut self, phase: QueryLifecyclePhase) -> Result<()> {
        let token = self
            .query_lifecycle_fault_files
            .publish_kill_query_at_phase(phase)?;
        println!(
            "armed KILL QUERY at lifecycle phase={} token={token} trigger={}",
            phase.as_str(),
            self.query_lifecycle_fault_files
                .kill_query_at_phase_path(phase)
                .display()
        );
        Ok(())
    }

    fn arm_fe_crash_at_lifecycle_phase(&mut self, phase: QueryLifecyclePhase) -> Result<()> {
        let token = self
            .query_lifecycle_fault_files
            .publish_fe_crash_at_phase(phase)?;
        println!(
            "armed FE crash at lifecycle phase={} token={token} trigger={}",
            phase.as_str(),
            self.query_lifecycle_fault_files
                .fe_crash_at_phase_path(phase)
                .display()
        );
        Ok(())
    }

    fn arm_query_control_heartbeat_stop_after_stage(&mut self, index: usize) -> Result<()> {
        self.ensure_be_index(index)?;
        let token = self
            .query_lifecycle_fault_files
            .publish_heartbeat_stop_after_stage(index)?;
        self.query_lifecycle_fault_tokens
            .insert((index, "heartbeat-stop-after-stage"), token.clone());
        println!(
            "armed query-control heartbeat stop after Stage for cross-process BE[{index}] token={token} trigger={}",
            self.query_lifecycle_fault_files
                .heartbeat_stop_after_stage_path(index)?
                .display()
        );
        Ok(())
    }

    fn arm_hold_start_until_early_ingress(&mut self) -> Result<()> {
        let token = self
            .query_lifecycle_fault_files
            .publish_hold_start_until_early_ingress()?;
        println!(
            "armed Start hold until early ingress token={token} trigger={}",
            self.query_lifecycle_fault_files
                .hold_start_until_early_ingress_path()
                .display()
        );
        Ok(())
    }

    fn release_query_lifecycle_phase_fault(
        &mut self,
        phase: QueryLifecyclePhase,
        fe_crash: bool,
    ) -> Result<()> {
        let path = if fe_crash {
            self.query_lifecycle_fault_files
                .fe_crash_at_phase_path(phase)
        } else {
            self.query_lifecycle_fault_files
                .kill_query_at_phase_path(phase)
        };
        remove_fragment_failure_file(&path).with_context(|| {
            format!(
                "release {} lifecycle phase fault {}",
                if fe_crash { "FE crash" } else { "KILL QUERY" },
                phase.as_str()
            )
        })
    }

    fn arm_query_control_fragment_backend_limit(&mut self, limit: usize) -> Result<()> {
        if !(1..=self.be_processes.len()).contains(&limit) {
            bail!(
                "query-control fragment backend limit {limit} is outside 1..={}",
                self.be_processes.len()
            );
        }
        let token = self
            .query_lifecycle_fault_files
            .publish_fragment_backend_limit(limit)?;
        println!(
            "armed query-control fragment backend limit={limit} token={token} trigger={}",
            self.query_lifecycle_fault_files
                .fragment_backend_limit_path()
                .display()
        );
        Ok(())
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

    fn backend_start_epoch(&self, index: usize) -> Result<u64> {
        self.ensure_be_index(index)?;
        let grpc_port = self.be_grpc_ports[index];
        query_frontend_backend_topology(
            &self.mysql_user,
            &self.target_host,
            self.target_port,
            TOPOLOGY_MYSQL_IO_TIMEOUT_CAP,
        )?
        .into_iter()
        .find(|row| row.grpc_port == grpc_port)
        .map(|row| row.start_epoch)
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
        if self.be_log_history[index].contains(needle) {
            return Ok(());
        }
        self.be_processes[index].assert_log_contains(needle)
    }

    fn be_log_count(&self, index: usize, needle: &str) -> Result<usize> {
        self.ensure_be_index(index)?;
        Ok(self.be_log_history[index].match_indices(needle).count()
            + self.be_processes[index].log_count(needle)?)
    }

    fn be_log_contents(&self, index: usize) -> Result<String> {
        self.ensure_be_index(index)?;
        let current = self.be_processes[index].log_contents()?;
        Ok(format!("{}{}", self.be_log_history[index], current))
    }

    fn be_current_log_contents(&self, index: usize) -> Result<String> {
        self.ensure_be_index(index)?;
        self.be_processes[index].log_contents()
    }

    fn fe_log_count(&self, needle: &str) -> Result<usize> {
        Ok(
            self.fe_log_history.match_indices(needle).count()
                + self.fe_process.log_count(needle)?,
        )
    }

    fn fe_log_contents(&self) -> Result<String> {
        let current = self.fe_process.log_contents()?;
        Ok(format!("{}{}", self.fe_log_history, current))
    }

    fn clear_query_lifecycle_faults(&mut self) -> Result<()> {
        self.query_lifecycle_fault_tokens.clear();
        self.query_lifecycle_fault_files.clear()
    }

    fn armed_query_lifecycle_fault_token(
        &self,
        index: usize,
        kind: &'static str,
    ) -> Result<Option<String>> {
        self.ensure_be_index(index)?;
        Ok(self
            .query_lifecycle_fault_tokens
            .get(&(index, kind))
            .cloned())
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
        self.restart_be_until(index, Instant::now() + startup_timeout())
    }

    fn restart_be_until(&mut self, index: usize, deadline: Instant) -> Result<()> {
        self.ensure_be_index(index)?;
        let old_start_epoch = self.backend_start_epoch(index)?;
        let prior_log = self.be_processes[index]
            .log_contents()
            .with_context(|| format!("preserve cross-process BE[{index}] log before restart"))?;
        self.be_log_history[index].push_str(&prior_log);
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
        if self.query_lifecycle_faults_enabled {
            command
                .env(
                    "NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_FAULT_DIR",
                    self.query_lifecycle_fault_files.root(),
                )
                .env(
                    "NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_BACKEND_INDEX",
                    index.to_string(),
                );
        }
        let log_path = self.runtime_dir.join(format!("be_{index}.log"));
        let be_process = self
            .be_processes
            .get_mut(index)
            .expect("BE index checked above");
        be_process
            .restart(
                command,
                ReadyMarker::StdoutContains(marker.to_string()),
                remaining_until(deadline, "BE readiness")?,
                log_path,
            )
            .map_err(|error| map_novarocks_process_error(&self.novarocks_bin, "be", marker, error))
            .with_context(|| format!("restart cross-process BE[{index}]"))?;
        println!(
            "restarted cross-process BE[{index}] pid={} config={}",
            be_process.pid(),
            config_path.display()
        );
        wait_for_live_backend_topology(
            &self.mysql_user,
            &self.runtime,
            &self.fe_config_path,
            &self.be_config_paths,
            &mut self.fe_process,
            &mut self.be_processes,
            remaining_until(deadline, "BE topology barrier")?,
        )
        .context("cross-process backend topology barrier after BE restart")?;
        loop {
            let remaining = remaining_until(deadline, "BE start-epoch barrier")?;
            let observed = query_frontend_backend_topology(
                &self.mysql_user,
                &self.target_host,
                self.target_port,
                topology_mysql_io_timeout(remaining),
            )
            .ok()
            .and_then(|rows| {
                rows.into_iter()
                    .find(|row| row.grpc_port == self.be_grpc_ports[index])
            });
            if observed.as_ref().is_some_and(|row| {
                row.alive && row.start_epoch != 0 && row.start_epoch != old_start_epoch
            }) {
                println!(
                    "cross-process BE[{index}] start-epoch barrier PASS: old_epoch={old_start_epoch} new_epoch={}",
                    observed.expect("observed row checked").start_epoch
                );
                break;
            }
            if Instant::now() >= deadline {
                let diagnostics = process_runtime_diagnostics(
                    &mut self.fe_process,
                    &mut self.be_processes,
                    &self.fe_config_path,
                    &self.be_config_paths,
                    &self.runtime,
                )?;
                bail!(
                    "timed out waiting for BE[{index}] start epoch to change from {old_start_epoch}; observed={observed:?}; {diagnostics}"
                );
            }
            thread::sleep(
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(100)),
            );
        }
        Ok(())
    }

    fn kill_fe(&mut self) -> Result<()> {
        self.fe_process
            .kill_now()
            .context("kill cross-process FE")?;
        println!("killed cross-process FE");
        Ok(())
    }

    fn restart_fe(&mut self) -> Result<()> {
        self.restart_fe_until(Instant::now() + startup_timeout())
    }

    fn restart_fe_until(&mut self, deadline: Instant) -> Result<()> {
        let prior_log = self
            .fe_process
            .log_contents()
            .context("preserve cross-process FE log before restart")?;
        self.fe_log_history.push_str(&prior_log);
        let marker = "NOVAROCKS_READY mysql_port=";
        let mut command = build_novarocks_command(&self.novarocks_bin, "fe", &self.fe_config_path);
        if self.query_lifecycle_faults_enabled {
            command.env(
                "NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_FAULT_DIR",
                self.query_lifecycle_fault_files.root(),
            );
        }
        self.fe_process
            .restart(
                command,
                ReadyMarker::StdoutContains(marker.to_string()),
                remaining_until(deadline, "FE readiness")?,
                self.runtime_dir.join("fe.log"),
            )
            .map_err(|error| map_novarocks_process_error(&self.novarocks_bin, "fe", marker, error))
            .context("restart cross-process FE")?;
        println!(
            "restarted cross-process FE pid={} config={}",
            self.fe_process.pid(),
            self.fe_config_path.display()
        );
        wait_for_live_backend_topology(
            &self.mysql_user,
            &self.runtime,
            &self.fe_config_path,
            &self.be_config_paths,
            &mut self.fe_process,
            &mut self.be_processes,
            remaining_until(deadline, "FE topology barrier")?,
        )
        .context("cross-process backend topology barrier after FE restart")?;
        Ok(())
    }

    fn kill_query(&mut self, connection_id: u32) -> Result<()> {
        self.kill_query_until(connection_id, Instant::now() + startup_timeout())
    }

    fn kill_query_until(&mut self, connection_id: u32, deadline: Instant) -> Result<()> {
        let io_timeout =
            topology_mysql_io_timeout(remaining_until(deadline, "KILL QUERY connect")?);
        let builder = OptsBuilder::new()
            .ip_or_hostname(Some(self.target_host.clone()))
            .tcp_port(self.target_port)
            .prefer_socket(false)
            .user(Some(self.mysql_user.clone()))
            .tcp_connect_timeout(Some(io_timeout))
            .read_timeout(Some(io_timeout))
            .write_timeout(Some(io_timeout));
        let mut control = MysqlConn::new(builder).with_context(|| {
            format!(
                "connect KILL QUERY control session to {}:{}",
                self.target_host, self.target_port
            )
        })?;
        control
            .query_drop(format!("KILL QUERY {connection_id}"))
            .with_context(|| format!("execute KILL QUERY {connection_id}"))?;
        println!("executed KILL QUERY {connection_id} through a separate control session");
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
    query_lifecycle_fault_scope: Option<(&Path, Option<usize>)>,
) -> Result<ManagedProcess> {
    let mut command = build_novarocks_command(binary, role, config_path);
    if let Some(trigger_path) = fragment_failure_trigger {
        command.env(
            "NOVAROCKS_SQL_TEST_FRAGMENT_FAILURE_TRIGGER_FILE",
            trigger_path,
        );
    }
    if let Some((fault_dir, backend_index)) = query_lifecycle_fault_scope {
        command.env("NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_FAULT_DIR", fault_dir);
        if let Some(backend_index) = backend_index {
            command.env(
                "NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_BACKEND_INDEX",
                backend_index.to_string(),
            );
        }
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

fn publish_query_lifecycle_fault_token(
    trigger_path: &Path,
    token: &str,
    contents: &[u8],
) -> Result<()> {
    let staging_path = trigger_path.with_extension(format!("arming-{token}"));
    let mut staging = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staging_path)
        .with_context(|| {
            format!(
                "create query lifecycle fault staging file {}",
                staging_path.display()
            )
        })?;
    if let Err(error) = staging.write_all(contents) {
        let _ = fs::remove_file(&staging_path);
        return Err(error).with_context(|| {
            format!(
                "write query lifecycle fault token to staging file {}",
                staging_path.display()
            )
        });
    }
    drop(staging);
    if let Err(error) = fs::hard_link(&staging_path, trigger_path) {
        let _ = fs::remove_file(&staging_path);
        return Err(error).with_context(|| {
            format!(
                "publish query lifecycle fault trigger {}",
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
            start_epoch: 17,
        }
    }

    #[test]
    fn frontend_six_column_show_backends_includes_start_epoch() {
        let row = parse_frontend_show_backends_values(&[
            "0".to_string(),
            "127.0.0.1".to_string(),
            "19070".to_string(),
            "Live".to_string(),
            "41".to_string(),
            "17".to_string(),
        ])
        .expect("parse frontend SHOW BACKENDS row");

        assert_eq!(row.grpc_port, 19070);
        assert_eq!(row.state, "Live");
        assert!(row.alive);
        assert_eq!(row.scheduled_fragments, 41);
        assert_eq!(row.start_epoch, 17);
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

        let kill_fe_err = handle.kill_fe().expect_err("noop FE kill should fail");
        assert!(
            kill_fe_err.to_string().contains("FE kill is unsupported"),
            "unexpected error: {kill_fe_err}"
        );

        let restart_fe_err = handle
            .restart_fe()
            .expect_err("noop FE restart should fail");
        assert!(
            restart_fe_err
                .to_string()
                .contains("FE restart is unsupported"),
            "unexpected error: {restart_fe_err}"
        );
    }

    #[test]
    fn query_lifecycle_fault_files_publish_isolated_tokens_and_clean_up_on_drop() {
        let root = std::env::temp_dir().join(format!(
            "novarocks-query-lifecycle-fault-test-{}",
            next_fragment_failure_token(99)
        ));
        fs::create_dir_all(&root).expect("create temp root");
        let trigger_dir = root.join("query-lifecycle-faults");
        let paths = QueryLifecycleFaultFiles::new(&trigger_dir, 3)
            .expect("create query lifecycle fault paths");
        let init_token = paths
            .publish_init_ack_drop(1)
            .expect("publish init ack token");
        let heartbeat_token = paths
            .publish_heartbeat_stop(2)
            .expect("publish heartbeat stop token");
        let stage_ack_token = paths
            .publish_stage_ack_drop(0)
            .expect("publish stage ack token");
        let start_ack_token = paths
            .publish_start_ack_suppress(1)
            .expect("publish start ack token");
        let stage_prepare_token = paths
            .publish_stage_prepare_failure(2)
            .expect("publish stage prepare failure");
        let phase_token = paths
            .publish_kill_query_at_phase(QueryLifecyclePhase::Starting)
            .expect("publish phase fault");
        let hold_token = paths
            .publish_hold_start_until_early_ingress()
            .expect("publish early ingress hold");

        assert_ne!(init_token, heartbeat_token);
        assert_eq!(
            fs::read_to_string(paths.init_ack_drop_path(1).expect("init path"))
                .expect("read init token"),
            format!("token={init_token}\nbackend_index=1\n")
        );
        assert_eq!(
            fs::read_to_string(paths.heartbeat_stop_path(2).expect("heartbeat path"))
                .expect("read heartbeat token"),
            format!("token={heartbeat_token}\nbackend_index=2\n")
        );
        assert!(!paths.init_ack_drop_path(0).expect("init path 0").exists());
        assert!(
            !paths
                .heartbeat_stop_path(1)
                .expect("heartbeat path 1")
                .exists()
        );
        assert_eq!(
            fs::read_to_string(paths.stage_ack_drop_path(0).expect("stage ack path"))
                .expect("read stage ack token"),
            format!("token={stage_ack_token}\nbackend_index=0\n")
        );
        assert_eq!(
            fs::read_to_string(paths.start_ack_suppress_path(1).expect("start ack path"))
                .expect("read start ack token"),
            format!("token={start_ack_token}\nbackend_index=1\n")
        );
        assert_eq!(
            fs::read_to_string(paths.stage_prepare_failure_path())
                .expect("read stage prepare token"),
            format!("token={stage_prepare_token}\nordinal=2\n")
        );
        assert_eq!(
            fs::read_to_string(paths.kill_query_at_phase_path(QueryLifecyclePhase::Starting))
                .expect("read phase token"),
            format!("token={phase_token}\nphase=starting\n")
        );
        assert_eq!(
            fs::read_to_string(paths.hold_start_until_early_ingress_path())
                .expect("read hold token"),
            format!("token={hold_token}\nenabled=true\n")
        );

        let duplicate = paths
            .publish_init_ack_drop(1)
            .expect_err("an armed trigger must not be clobbered");
        assert!(
            format!("{duplicate:#}").contains("publish query lifecycle fault trigger"),
            "unexpected duplicate error: {duplicate:#}"
        );

        drop(paths);
        assert!(
            !trigger_dir.exists(),
            "dropping the runner-owned fault scope must remove every trigger"
        );
        fs::remove_dir(&root).expect("remove empty temp root");
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
user = "root"

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
    /// other section — server ports, cluster role/backends, and connector
    /// object-store settings — exactly as `render_cross_process_config` would
    /// have produced them. This is the piece the harness can prove correct without
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
        // [standalone_server].metadata_db_path (a retired internal-table key).
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
        assert_eq!(fe_value["connector"], plain_fe_value["connector"]);
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
    fn ordinary_cross_process_launches_do_not_share_persisted_backend_rows() {
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
            false,
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
            false,
        )
        .unwrap()
        .parse::<Value>()
        .unwrap();

        assert_eq!(
            first["metadata"]["path"].as_str(),
            first_runtime.join("metadata.sqlite").to_str()
        );
        assert_eq!(
            first["state_store"]["path"].as_str(),
            first_runtime.join("frontend-state.sqlite").to_str()
        );
        assert_eq!(
            second["metadata"]["path"].as_str(),
            second_runtime.join("metadata.sqlite").to_str()
        );
        assert_eq!(
            second["state_store"]["path"].as_str(),
            second_runtime.join("frontend-state.sqlite").to_str()
        );
        assert_ne!(
            first["state_store"]["path"], second["state_store"]["path"],
            "ephemeral clusters must not restore stale backend rows from another launch"
        );
        assert!(
            first["debug"].get("query_lifecycle_fault_dir").is_none(),
            "ordinary cross-process config must remain usable by release binaries"
        );
        assert!(
            second["debug"].get("query_lifecycle_fault_dir").is_none(),
            "ordinary cross-process config must not enable lifecycle fault hooks"
        );
    }

    #[test]
    fn lifecycle_fault_cross_process_config_requires_explicit_debug_preflight() {
        let runtime = make_runtime_1be();
        let runtime_dir = Path::new("/tmp/novarocks-cross-process-lifecycle-fault");
        let rendered = render_cross_process_launch_config(
            BASE_CONFIG,
            ClusterProcessRole::Be,
            0,
            &runtime,
            runtime_dir,
            CrossProcessMetadataMode::Isolated,
            true,
        )
        .expect("render explicit lifecycle fault config")
        .parse::<Value>()
        .expect("parse lifecycle fault config");

        assert_eq!(
            rendered["debug"]["query_lifecycle_fault_dir"].as_str(),
            runtime_dir.join("query-lifecycle-faults").to_str()
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
            false,
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
            false,
        )
        .unwrap()
        .parse::<Value>()
        .unwrap();

        assert_eq!(
            cluster_a["metadata"]["path"].as_str(),
            cluster_a_runtime.join("metadata.sqlite").to_str(),
            "cluster A must not inherit backend topology rows from the base metadata database"
        );
        assert_eq!(
            cluster_b["metadata"]["path"].as_str(),
            Some(cluster_b_metadata)
        );
        assert_ne!(cluster_a["metadata"]["path"], base["metadata"]["path"]);
        assert_ne!(cluster_b["metadata"]["path"], base["metadata"]["path"]);
        assert_ne!(cluster_a["metadata"]["path"], cluster_b["metadata"]["path"]);
        assert_eq!(cluster_a["connector"], base["connector"]);
        assert_eq!(cluster_b["connector"], base["connector"]);
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

    #[test]
    fn prometheus_labeled_gauge_requires_one_exact_sample() {
        let metrics = concat!(
            "novarocks_backend_query_execution_resources{resource=\"stage_active_builders\"} 7\n",
            "novarocks_backend_query_execution_resources{resource=\"stage_encoded_bytes\"} 11\n",
        );
        assert_eq!(
            prometheus_labeled_gauge(
                metrics,
                QUERY_EXECUTION_RESOURCE_METRIC,
                "resource",
                "stage_active_builders"
            )
            .expect("read exact resource sample"),
            7.0
        );
        assert!(
            prometheus_labeled_gauge(
                metrics,
                QUERY_EXECUTION_RESOURCE_METRIC,
                "resource",
                "native_query_contexts_active"
            )
            .is_err()
        );
    }

    #[test]
    fn resource_convergence_allows_a_killed_backend_but_not_a_live_leak() {
        let baseline = QueryExecutionResourceSnapshot {
            fe_running: true,
            backends: vec![BackendResourceSnapshot {
                index: 0,
                process_running: true,
                resources: BTreeMap::from([("native_query_contexts_active".to_string(), 0.0)]),
                terminal_retained: 0.0,
                terminal_retained_bytes: 0.0,
                terminal_retained_capacity: 4_096.0,
                terminal_max_retained_bytes: 268_435_456.0,
            }],
        };
        let exited = QueryExecutionResourceSnapshot {
            fe_running: true,
            backends: vec![BackendResourceSnapshot {
                index: 0,
                process_running: false,
                resources: BTreeMap::new(),
                terminal_retained: 0.0,
                terminal_retained_bytes: 0.0,
                terminal_retained_capacity: 0.0,
                terminal_max_retained_bytes: 0.0,
            }],
        };
        assert!(exited.convergence_failure(&baseline, false).is_none());

        let leaked = QueryExecutionResourceSnapshot {
            fe_running: true,
            backends: vec![BackendResourceSnapshot {
                index: 0,
                process_running: true,
                resources: BTreeMap::from([("native_query_contexts_active".to_string(), 1.0)]),
                terminal_retained: 0.0,
                terminal_retained_bytes: 0.0,
                terminal_retained_capacity: 4_096.0,
                terminal_max_retained_bytes: 268_435_456.0,
            }],
        };
        assert!(
            leaked
                .convergence_failure(&baseline, false)
                .expect("live leak must be reported")
                .contains("native_query_contexts_active")
        );
    }

    #[test]
    fn resource_convergence_allows_bounded_terminal_retention_after_frontend_crash() {
        let baseline = QueryExecutionResourceSnapshot {
            fe_running: true,
            backends: vec![BackendResourceSnapshot {
                index: 0,
                process_running: true,
                resources: BTreeMap::from([("native_query_contexts_active".to_string(), 0.0)]),
                terminal_retained: 0.0,
                terminal_retained_bytes: 0.0,
                terminal_retained_capacity: 4_096.0,
                terminal_max_retained_bytes: 268_435_456.0,
            }],
        };
        let retained = QueryExecutionResourceSnapshot {
            fe_running: true,
            backends: vec![BackendResourceSnapshot {
                index: 0,
                process_running: true,
                resources: BTreeMap::from([("native_query_contexts_active".to_string(), 0.0)]),
                terminal_retained: 2.0,
                terminal_retained_bytes: 512.0,
                terminal_retained_capacity: 4_096.0,
                terminal_max_retained_bytes: 268_435_456.0,
            }],
        };

        assert!(retained.convergence_failure(&baseline, true).is_none());
        assert!(retained.convergence_failure(&baseline, false).is_some());
    }

    #[test]
    fn resource_convergence_allows_existing_terminal_retention_to_expire() {
        let baseline = QueryExecutionResourceSnapshot {
            fe_running: true,
            backends: vec![BackendResourceSnapshot {
                index: 0,
                process_running: true,
                resources: BTreeMap::from([("native_query_contexts_active".to_string(), 0.0)]),
                terminal_retained: 2.0,
                terminal_retained_bytes: 512.0,
                terminal_retained_capacity: 4_096.0,
                terminal_max_retained_bytes: 268_435_456.0,
            }],
        };
        let expired = QueryExecutionResourceSnapshot {
            fe_running: true,
            backends: vec![BackendResourceSnapshot {
                index: 0,
                process_running: true,
                resources: BTreeMap::from([("native_query_contexts_active".to_string(), 0.0)]),
                terminal_retained: 1.0,
                terminal_retained_bytes: 256.0,
                terminal_retained_capacity: 4_096.0,
                terminal_max_retained_bytes: 268_435_456.0,
            }],
        };

        assert!(expired.convergence_failure(&baseline, false).is_none());
    }
}
