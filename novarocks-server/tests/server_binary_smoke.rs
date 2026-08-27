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

//! External-contract smoke coverage for the `novarocks` server binary.
//!
//! The fixtures deliberately build a normal FE/BE deployable pair. That exact
//! pair must work both as two processes and under the Server-owned all-in-one
//! supervisor; this target must never revive a third application role or a
//! single all-in-one configuration shape.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use novarocks_test_support::{ManagedProcess, ReadyMarker, ReservedTcpPort};
use tempfile::{Builder as TempFileBuilder, NamedTempFile, TempDir};

static SERVER_BINARY_SMOKE_LOCK: Mutex<()> = Mutex::new(());

const PROCESS_READY_TIMEOUT: Duration = Duration::from_secs(30);
const LIFECYCLE_DEBUG_PATH: &str = "/debug/query-lifecycle/latest";

fn lock_server_binary_smoke() -> MutexGuard<'static, ()> {
    SERVER_BINARY_SMOKE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn runtime_dir() -> PathBuf {
    let dir = PathBuf::from(".server-binary-smoke-runtime");
    std::fs::create_dir_all(&dir).expect("create server binary smoke runtime dir");
    dir
}

/// Freeze a port through the canonical reservation owner. The reservation is
/// released immediately before its owning role process starts.
fn reserve_port() -> ReservedTcpPort {
    ReservedTcpPort::new().expect("reserve TCP port")
}

fn write_config(dir: &Path, name: &str, content: &str) -> NamedTempFile {
    let file = TempFileBuilder::new()
        .prefix(name)
        .suffix(".toml")
        .tempfile_in(dir)
        .expect("create config temp file");
    std::fs::write(file.path(), content).expect("write config");
    file
}

struct ConfigPair {
    runtime: TempDir,
    fe_config: NamedTempFile,
    be_config: NamedTempFile,
    fe_mysql_port: u16,
    fe_http_port: u16,
    fe_grpc_port: u16,
    be_http_port: u16,
    be_grpc_port: u16,
    fe_mysql: Option<ReservedTcpPort>,
    fe_http: Option<ReservedTcpPort>,
    fe_grpc: Option<ReservedTcpPort>,
    be_http: Option<ReservedTcpPort>,
    be_grpc: Option<ReservedTcpPort>,
    lifecycle_fault_dir: PathBuf,
}

impl ConfigPair {
    fn new() -> Self {
        let runtime = tempfile::tempdir_in(runtime_dir()).expect("create smoke runtime");
        let fe_mysql = reserve_port();
        let fe_http = reserve_port();
        let fe_grpc = reserve_port();
        let be_http = reserve_port();
        let be_grpc = reserve_port();
        let fe_mysql_port = fe_mysql.port();
        let fe_http_port = fe_http.port();
        let fe_grpc_port = fe_grpc.port();
        let be_http_port = be_http.port();
        let be_grpc_port = be_grpc.port();
        let state_store = runtime.path().join("frontend-state.sqlite");
        let log_dir = runtime.path().join("logs");
        let lifecycle_fault_dir = runtime.path().join("query-lifecycle-faults");
        std::fs::create_dir_all(&lifecycle_fault_dir).expect("create lifecycle fault directory");

        let fe_config = write_config(
            runtime.path(),
            "fe",
            &format!(
                r#"
sys_log_dir = "{}"

[native_trust]
deployment_id = "server-binary-smoke"
shared_secret = "0123456789abcdef0123456789abcdef"

[server]
host = "127.0.0.1"
http_port = {fe_http_port}
grpc_port = {fe_grpc_port}

[standalone_server]
mysql_port = {fe_mysql_port}
user = "root"

[cluster]
role = "fe"
heartbeat_interval_ms = 100
heartbeat_timeout_retries = 10

[catalog_source]
mode = "dynamic-state-store"

[state_store]
provider = "sqlite"
cluster_id = "server-binary-smoke"
path = "{}"
"#,
                log_dir.display(),
                state_store.display(),
            ),
        );
        let be_config = write_config(
            runtime.path(),
            "be",
            &format!(
                r#"
sys_log_dir = "{}"

[native_trust]
deployment_id = "server-binary-smoke"
shared_secret = "0123456789abcdef0123456789abcdef"

[server]
host = "127.0.0.1"
http_port = {be_http_port}
grpc_port = {be_grpc_port}

[cluster]
role = "be"
frontend_endpoint = "127.0.0.1:{fe_grpc_port}"
"#,
                log_dir.display(),
            ),
        );
        Self {
            runtime,
            fe_config,
            be_config,
            fe_mysql_port,
            fe_http_port,
            fe_grpc_port,
            be_http_port,
            be_grpc_port,
            fe_mysql: Some(fe_mysql),
            fe_http: Some(fe_http),
            fe_grpc: Some(fe_grpc),
            be_http: Some(be_http),
            be_grpc: Some(be_grpc),
            lifecycle_fault_dir,
        }
    }

    fn release_be(&mut self) {
        drop(self.be_http.take());
        drop(self.be_grpc.take());
    }

    fn release_fe(&mut self) {
        drop(self.fe_mysql.take());
        drop(self.fe_http.take());
        drop(self.fe_grpc.take());
    }

    fn release_all(&mut self) {
        self.release_be();
        self.release_fe();
    }
}

fn spawn_novarocks(
    role: &str,
    config_path: &Path,
    ready_marker: &str,
    debug_env: &[(&str, &Path)],
) -> ManagedProcess {
    let mut command = Command::new(env!("CARGO_BIN_EXE_novarocks"));
    command
        .arg("standalone")
        .arg("--role")
        .arg(role)
        .arg("--config")
        .arg(config_path)
        .env("NO_PROXY", "127.0.0.1,localhost");
    for (name, value) in debug_env {
        command.env(name, value);
    }
    ManagedProcess::spawn(
        format!("novarocks role={role} config={}", config_path.display()),
        command,
        ReadyMarker::StdoutContains(ready_marker.to_string()),
        PROCESS_READY_TIMEOUT,
        config_path.with_extension(format!("{role}.process.log")),
    )
    .unwrap_or_else(|error| panic!("spawn novarocks role={role}: {error:#}"))
}

fn spawn_all_in_one(pair: &ConfigPair, debug_env: &[(&str, &Path)]) -> ManagedProcess {
    let mut command = Command::new(env!("CARGO_BIN_EXE_novarocks"));
    command
        .arg("standalone")
        .arg("--role")
        .arg("all-in-one")
        .arg("--fe-config")
        .arg(pair.fe_config.path())
        .arg("--be-config")
        .arg(pair.be_config.path())
        .env("NO_PROXY", "127.0.0.1,localhost");
    for (name, value) in debug_env {
        command.env(name, value);
    }
    ManagedProcess::spawn(
        format!(
            "novarocks role=all-in-one fe={} be={}",
            pair.fe_config.path().display(),
            pair.be_config.path().display()
        ),
        command,
        ReadyMarker::StdoutContains("NOVAROCKS_READY mysql_port=".to_string()),
        PROCESS_READY_TIMEOUT,
        pair.fe_config
            .path()
            .with_extension("all-in-one.process.log"),
    )
    .unwrap_or_else(|error| panic!("spawn novarocks role=all-in-one: {error:#}"))
}

fn http_request(port: u16, method: &str, path: &str) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect HTTP listener");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set HTTP read timeout");
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .expect("set HTTP write timeout");
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
    )
    .expect("write HTTP request");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read HTTP response");
    response
}

fn scrape_metrics(port: u16) -> String {
    let response = http_request(port, "GET", "/metrics");
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .expect("split metrics HTTP response");
    assert!(
        headers.starts_with("HTTP/1.1 200") || headers.starts_with("HTTP/1.0 200"),
        "metrics endpoint returned {headers}"
    );
    body.to_string()
}

fn assert_native_rejects_management(port: u16, path: &str) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect native listener");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set native listener read timeout");
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
    )
    .expect("write unauthenticated native request");
    let mut response = String::new();
    match stream.read_to_string(&mut response) {
        Ok(_) => {
            assert!(
                response.contains("grpc-status: 16"),
                "native response: {response}"
            );
            assert!(
                !response.contains("novarocks_"),
                "native listener must not render management data: {response}"
            );
        }
        Err(error) => assert_eq!(
            error.kind(),
            std::io::ErrorKind::ConnectionReset,
            "native listener must reject unauthenticated HTTP/1 traffic: {error}"
        ),
    }
}

fn assert_role_scoped_surfaces(pair: &ConfigPair, lifecycle_debug_enabled: bool) {
    assert_native_rejects_management(pair.fe_grpc_port, "/metrics");
    assert_native_rejects_management(pair.be_grpc_port, "/metrics");
    assert_native_rejects_management(pair.fe_grpc_port, LIFECYCLE_DEBUG_PATH);

    let fe_metrics = scrape_metrics(pair.fe_http_port);
    assert!(
        fe_metrics.contains("novarocks_backend_registry_entries"),
        "FE metrics: {fe_metrics}"
    );
    assert!(
        !fe_metrics.contains("novarocks_backend_query_lifecycle_entries"),
        "FE management leaked BE metrics: {fe_metrics}"
    );
    let be_metrics = scrape_metrics(pair.be_http_port);
    assert!(
        be_metrics.contains("novarocks_backend_query_lifecycle_entries"),
        "BE metrics: {be_metrics}"
    );
    assert!(
        !be_metrics.contains("novarocks_backend_registry_entries"),
        "BE management leaked FE metrics: {be_metrics}"
    );

    let debug_response = http_request(pair.fe_http_port, "POST", LIFECYCLE_DEBUG_PATH);
    let expected_status = if lifecycle_debug_enabled {
        "405"
    } else {
        "404"
    };
    assert!(
        debug_response.starts_with(&format!("HTTP/1.1 {expected_status}"))
            || debug_response.starts_with(&format!("HTTP/1.0 {expected_status}")),
        "FE management debug gate expected {expected_status}: {debug_response}"
    );
}

fn run_binary(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_novarocks"))
        .args(args)
        .output()
        .expect("run novarocks")
}

fn assert_rejected(args: &[&str], expected: &str) {
    let output = run_binary(args);
    assert!(
        !output.status.success(),
        "args unexpectedly succeeded: {args:?}"
    );
    let diagnostics = format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        diagnostics.contains(expected),
        "expected {expected:?} for {args:?}, got {diagnostics}"
    );
}

/// The same normal FE/BE config pair must bind its role-local listeners first
/// as independent roles and then through the all-in-one supervisor. Dynamic
/// backend admission is intentionally outside this NWT-2 smoke: LNP-5 refuses
/// to restore static membership while the authenticated announce carrier is
/// still owned by NWT-3.
#[test]
fn same_config_pair_has_cross_process_and_all_in_one_listener_parity_without_static_membership() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _lock = lock_server_binary_smoke();
    let mut pair = ConfigPair::new();
    let lifecycle_fault_dir = pair.lifecycle_fault_dir.clone();
    let frontend_debug_env = [(
        novarocks_failpoint::QUERY_LIFECYCLE_FAULT_DIR_ENV,
        lifecycle_fault_dir.as_path(),
    )];

    pair.release_be();
    let mut backend = spawn_novarocks("be", pair.be_config.path(), "NOVAROCKS_READY role=be", &[]);
    pair.release_fe();
    let mut frontend = spawn_novarocks(
        "fe",
        pair.fe_config.path(),
        "NOVAROCKS_READY mysql_port=",
        &frontend_debug_env,
    );
    assert_role_scoped_surfaces(&pair, true);
    frontend
        .interrupt_and_wait(Duration::from_secs(10))
        .expect("shut down FE cleanly");
    backend
        .interrupt_and_wait(Duration::from_secs(10))
        .expect("shut down BE cleanly");

    let mut all_in_one = spawn_all_in_one(&pair, &[]);
    assert_role_scoped_surfaces(&pair, false);
    all_in_one
        .request_termination()
        .expect("send SIGTERM to all-in-one roles");
    all_in_one
        .wait_for_successful_exit_until(Instant::now() + Duration::from_secs(10))
        .expect("drain and stop all-in-one roles cleanly after SIGTERM");

    for port in [
        pair.fe_mysql_port,
        pair.fe_http_port,
        pair.fe_grpc_port,
        pair.be_http_port,
        pair.be_grpc_port,
    ] {
        let rebound = TcpListener::bind(("127.0.0.1", port)).unwrap_or_else(|error| {
            panic!("port {port} must be reusable after parity smoke: {error}")
        });
        drop(rebound);
    }
}

/// CLI shape is a binary contract, not merely a parser unit. These cases reject
/// the removed aliases and every incomplete or mismatched launch group before a
/// role runtime is started.
#[test]
fn binary_rejects_removed_and_ambiguous_launch_shapes() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _lock = lock_server_binary_smoke();
    let pair = ConfigPair::new();
    let missing_role = write_config(
        pair.runtime.path(),
        "missing-role",
        "[server]\nhost = \"127.0.0.1\"\n",
    );
    let legacy_all_in_one = write_config(
        pair.runtime.path(),
        "legacy-all-in-one",
        "[cluster]\nrole = \"all-in-one\"\n",
    );

    assert_rejected(&["standalone"], "missing required --role");
    assert_rejected(
        &["standalone", "--role", "fe", "--port", "9030"],
        "--port is not supported",
    );
    assert_rejected(
        &[
            "standalone",
            "--role",
            "all-in-one",
            "--config",
            pair.fe_config.path().to_str().expect("utf8 config path"),
        ],
        "does not accept --config",
    );
    assert_rejected(
        &[
            "standalone",
            "--role",
            "all-in-one",
            "--fe-config",
            pair.fe_config.path().to_str().expect("utf8 config path"),
        ],
        "requires both --fe-config",
    );
    assert_rejected(
        &[
            "standalone",
            "--role",
            "be",
            "--config",
            pair.fe_config.path().to_str().expect("utf8 config path"),
        ],
        "requires [cluster].role=be",
    );
    assert_rejected(
        &[
            "standalone",
            "--role",
            "fe",
            "--config",
            missing_role.path().to_str().expect("utf8 config path"),
        ],
        "missing required [cluster] table",
    );
    assert_rejected(
        &[
            "standalone",
            "--role",
            "fe",
            "--config",
            legacy_all_in_one.path().to_str().expect("utf8 config path"),
        ],
        "must be `fe` or `be`",
    );
}

/// Cross-role bind overlap must fail in launch preflight, before logging,
/// StateStore creation, or a listener side effect. The reserved port is
/// released solely so a mistaken late bind would be observable here.
#[test]
fn all_in_one_preflight_conflict_has_no_startup_side_effects() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _lock = lock_server_binary_smoke();
    let mut pair = ConfigPair::new();
    let conflict_port = pair.fe_grpc_port;
    let state_store = pair.runtime.path().join("preflight-state.sqlite");
    let log_dir = pair.runtime.path().join("preflight-logs");
    let conflicting_be = write_config(
        pair.runtime.path(),
        "conflicting-be",
        &format!(
            r#"
sys_log_dir = "{}"

[native_trust]
deployment_id = "server-binary-smoke"
shared_secret = "0123456789abcdef0123456789abcdef"

[server]
host = "127.0.0.1"
http_port = {}
grpc_port = {conflict_port}

[cluster]
role = "be"
frontend_endpoint = "127.0.0.1:{}"
"#,
            log_dir.display(),
            pair.be_http_port,
            pair.fe_grpc_port,
        ),
    );
    let conflicting_fe = write_config(
        pair.runtime.path(),
        "conflicting-fe",
        &format!(
            r#"
sys_log_dir = "{}"

[native_trust]
deployment_id = "server-binary-smoke"
shared_secret = "0123456789abcdef0123456789abcdef"

[server]
host = "127.0.0.1"
http_port = {}
grpc_port = {}

[standalone_server]
mysql_port = {}

[cluster]
role = "fe"

[catalog_source]
mode = "dynamic-state-store"

[state_store]
provider = "sqlite"
cluster_id = "preflight"
path = "{}"
"#,
            log_dir.display(),
            pair.fe_http_port,
            pair.fe_grpc_port,
            pair.fe_mysql_port,
            state_store.display(),
        ),
    );
    pair.release_all();
    let output = run_binary(&[
        "standalone",
        "--role",
        "all-in-one",
        "--fe-config",
        conflicting_fe.path().to_str().expect("utf8 config path"),
        "--be-config",
        conflicting_be.path().to_str().expect("utf8 config path"),
    ]);
    assert!(
        !output.status.success(),
        "conflicting all-in-one launch succeeded"
    );
    let diagnostics = format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        diagnostics.contains("bind endpoint conflict"),
        "{diagnostics}"
    );
    assert!(!state_store.exists(), "preflight created StateStore");
    assert!(!log_dir.exists(), "preflight created log directory");
    let rebound = TcpListener::bind(("127.0.0.1", conflict_port))
        .expect("preflight must not start conflicting native listener");
    drop(rebound);
}
