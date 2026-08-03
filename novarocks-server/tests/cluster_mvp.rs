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

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, MutexGuard, mpsc};
use std::time::{Duration, Instant};

use mysql::prelude::Queryable;
use mysql::{Conn as MysqlConn, OptsBuilder, Row};
use novarocks_frontend::dml::{
    CtasSagaPhase, ExternalFactOutcome, OperationKind, OperationPayload, OperationState,
    StatementNextAction, TruncateLifecyclePhase,
};
use novarocks_frontend::{
    ClusterBackendOpenConfig, FrontendApplicationHost, FrontendExecutionConfig,
};
use novarocks_state_store::{
    StateStoreAppConfig, StateStoreConfig, StateStoreHostConfig, StateStoreLimitOverrides,
    StateStoreProviderConfig,
};
use tempfile::{Builder as TempFileBuilder, NamedTempFile, TempDir};

static CLUSTER_MVP_TEST_LOCK: Mutex<()> = Mutex::new(());

fn lock_cluster_mvp() -> MutexGuard<'static, ()> {
    CLUSTER_MVP_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct ReservedPort {
    _listener: TcpListener,
    port: u16,
}

impl ReservedPort {
    fn new() -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind ephemeral port");
        let port = listener.local_addr().expect("local addr").port();
        Self {
            _listener: listener,
            port,
        }
    }

    fn port(&self) -> u16 {
        self.port
    }

    fn release(self) -> u16 {
        self.port
    }
}

fn runtime_dir() -> PathBuf {
    let dir = PathBuf::from(".cluster_mvp_runtime");
    std::fs::create_dir_all(&dir).expect("create cluster mvp runtime dir");
    dir
}

struct EnvironmentValueGuard {
    key: &'static str,
    previous: Option<std::ffi::OsString>,
}

impl EnvironmentValueGuard {
    fn set_path(key: &'static str, value: &Path) -> Self {
        let previous = std::env::var_os(key);
        // SAFETY: this integration target serializes its process-spawning
        // tests with `CLUSTER_MVP_TEST_LOCK`; the guard restores the caller
        // environment before the test returns.
        unsafe { std::env::set_var(key, value) };
        Self { key, previous }
    }
}

impl Drop for EnvironmentValueGuard {
    fn drop(&mut self) {
        // SAFETY: see `set_path`; restoring the inherited environment is part
        // of the runner-owned fault scope cleanup.
        unsafe {
            if let Some(value) = self.previous.as_ref() {
                std::env::set_var(self.key, value);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }
}

fn write_config(name: &str, content: &str) -> NamedTempFile {
    let file = TempFileBuilder::new()
        .prefix(name)
        .suffix(".toml")
        .tempfile_in(runtime_dir())
        .expect("create config temp file");
    std::fs::write(file.path(), content).expect("write config");
    file
}

struct ProcessGuard {
    child: Child,
    stdout_rx: mpsc::Receiver<String>,
    _stdout_thread: std::thread::JoinHandle<()>,
    _stderr_thread: std::thread::JoinHandle<()>,
}

impl ProcessGuard {
    fn spawn(config_path: &Path) -> Self {
        Self::spawn_with_backend_index(config_path, None)
    }

    fn spawn_backend(config_path: &Path, backend_index: usize) -> Self {
        Self::spawn_with_backend_index(config_path, Some(backend_index))
    }

    fn spawn_with_backend_index(config_path: &Path, backend_index: Option<usize>) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_novarocks"));
        command
            .arg("standalone")
            .arg("--config")
            .arg(config_path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(backend_index) = backend_index {
            command.env(
                "NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_BACKEND_INDEX",
                backend_index.to_string(),
            );
        }
        let mut child = command.spawn().expect("spawn novarocks");
        let stdout = child.stdout.take().expect("child stdout");
        let stderr = child.stderr.take().expect("child stderr pipe");
        let (tx, rx) = mpsc::channel();
        let stdout_tx = tx.clone();
        let stdout_thread = std::thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                let Ok(line) = line else {
                    break;
                };
                if stdout_tx.send(line).is_err() {
                    break;
                }
            }
        });
        let stderr_thread = std::thread::spawn(move || {
            let reader = BufReader::new(stderr);
            for line in reader.lines() {
                let Ok(line) = line else {
                    break;
                };
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        Self {
            child,
            stdout_rx: rx,
            _stdout_thread: stdout_thread,
            _stderr_thread: stderr_thread,
        }
    }

    fn wait_for_ready(&mut self, marker: &str) {
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut stdout = Vec::new();
        loop {
            if let Some(status) = self.child.try_wait().expect("poll child") {
                panic!(
                    "novarocks exited before readiness marker `{marker}` with status {status}; stdout={stdout:?}; stderr={}",
                    self.read_stderr()
                );
            }
            match self.stdout_rx.recv_timeout(Duration::from_millis(100)) {
                Ok(line) => {
                    if line.contains(marker) {
                        return;
                    }
                    stdout.push(line);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    let status = self
                        .child
                        .try_wait()
                        .expect("poll child after stdout close");
                    panic!(
                        "stdout closed before readiness marker `{marker}`; status={status:?}; stdout={stdout:?}; stderr={}",
                        self.read_stderr()
                    );
                }
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                let _ = self.child.wait();
                panic!(
                    "timed out waiting for readiness marker `{marker}`; stdout={stdout:?}; stderr={}",
                    self.read_stderr()
                );
            }
        }
    }

    fn read_stderr(&mut self) -> String {
        self.stdout_rx.try_iter().collect::<Vec<_>>().join("\n")
    }

    fn wait_for_output_contains(&mut self, marker: &str, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        let mut stdout = Vec::new();
        loop {
            if let Some(status) = self.child.try_wait().expect("poll child") {
                panic!(
                    "novarocks exited before marker `{marker}` with status {status}; stdout={stdout:?}; stderr={}",
                    self.read_stderr()
                );
            }
            match self.stdout_rx.recv_timeout(Duration::from_millis(100)) {
                Ok(line) => {
                    if line.contains(marker) {
                        return;
                    }
                    stdout.push(line);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    panic!("stdout closed before marker `{marker}`; stdout={stdout:?}");
                }
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                let _ = self.child.wait();
                panic!(
                    "timed out waiting for marker `{marker}`; stdout={stdout:?}; stderr={}",
                    self.read_stderr()
                );
            }
        }
    }

    #[cfg(unix)]
    fn shutdown_cleanly(&mut self, timeout: Duration) {
        let pid = i32::try_from(self.child.id()).expect("child PID fits i32");
        // SAFETY: `pid` belongs to the child owned by this guard, and SIGINT is
        // the server's supported graceful-shutdown signal on Unix.
        let signal_result = unsafe { libc::kill(pid, libc::SIGINT) };
        assert_eq!(
            signal_result,
            0,
            "send SIGINT to novarocks pid {pid}: {}",
            std::io::Error::last_os_error()
        );

        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait().expect("poll child after SIGINT") {
                assert!(
                    status.success(),
                    "novarocks did not exit cleanly after SIGINT: status={status}; stderr={}",
                    self.read_stderr()
                );
                return;
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                let _ = self.child.wait();
                panic!(
                    "timed out after {timeout:?} waiting for novarocks to exit after SIGINT; stderr={}",
                    self.read_stderr()
                );
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn connect_mysql(port: u16) -> MysqlConn {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let builder = OptsBuilder::new()
            .ip_or_hostname(Some("127.0.0.1".to_string()))
            .tcp_port(port)
            .prefer_socket(false)
            .user(Some("root".to_string()))
            .read_timeout(Some(Duration::from_secs(10)))
            .write_timeout(Some(Duration::from_secs(10)));
        match MysqlConn::new(builder) {
            Ok(conn) => return conn,
            Err(err) => {
                if Instant::now() >= deadline {
                    panic!("mysql connection failed: {err}");
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

fn start_all_in_one(extra: &str) -> (ProcessGuard, u16) {
    let mysql = ReservedPort::new();
    let http = ReservedPort::new();
    let grpc = ReservedPort::new();
    let mysql_port = mysql.port();
    let http_port = http.port();
    let grpc_port = grpc.port();
    let config = write_config(
        "all-in-one",
        &format!(
            r#"
[server]
host = "127.0.0.1"
http_port = {http_port}
grpc_port = {grpc_port}

[standalone_server]
mysql_port = {mysql_port}

[cluster]
role = "all-in-one"

{extra}
"#
        ),
    );
    let _ = mysql.release();
    let _ = http.release();
    let _ = grpc.release();
    let mut process = ProcessGuard::spawn(config.path());
    process.wait_for_ready("NOVAROCKS_READY mysql_port=");
    (process, mysql_port)
}

struct ClusterHarness {
    be: ProcessGuard,
    _fe: ProcessGuard,
    fe_mysql: u16,
    be_http: u16,
    _state_store_root: TempDir,
}

impl ClusterHarness {
    fn start(be_debug: &str, fe_extra: &str) -> Self {
        let be_http = ReservedPort::new();
        let be_grpc = ReservedPort::new();
        let fe_mysql = ReservedPort::new();
        let fe_http = ReservedPort::new();
        let fe_grpc = ReservedPort::new();
        let be_http_port = be_http.port();
        let be_grpc_port = be_grpc.port();
        let fe_mysql_port = fe_mysql.port();
        let fe_http_port = fe_http.port();
        let fe_grpc_port = fe_grpc.port();
        let state_store_root = TempFileBuilder::new()
            .prefix("cluster-state-store-")
            .tempdir_in(runtime_dir())
            .expect("create cluster StateStore root");
        let state_store_path = state_store_root.path().join("state.sqlite");

        let be_config = write_config(
            "be",
            &format!(
                r#"
[server]
host = "127.0.0.1"
http_port = {be_http_port}
grpc_port = {be_grpc_port}

[cluster]
role = "be"
{be_debug}
"#
            ),
        );
        let fe_config = write_config(
            "fe",
            &format!(
                r#"
[server]
host = "127.0.0.1"
http_port = {fe_http_port}
grpc_port = {fe_grpc_port}

[standalone_server]
mysql_port = {fe_mysql_port}

[cluster]
role = "fe"
backends = ["127.0.0.1:{be_grpc_port}"]

[state_store]
provider = "sqlite"
path = "{}"
cluster_id = "cluster-harness-{fe_mysql_port}"
deployment_owner = "fe-1"
{fe_extra}
"#,
                state_store_path.display()
            ),
        );

        let _ = be_http.release();
        let _ = be_grpc.release();
        let mut be = ProcessGuard::spawn(be_config.path());
        be.wait_for_ready("NOVAROCKS_READY role=be");

        let _ = fe_mysql.release();
        let _ = fe_http.release();
        let _ = fe_grpc.release();
        let mut fe = ProcessGuard::spawn(fe_config.path());
        fe.wait_for_ready("NOVAROCKS_READY mysql_port=");

        Self {
            be,
            _fe: fe,
            fe_mysql: fe_mysql_port,
            be_http: be_http_port,
            _state_store_root: state_store_root,
        }
    }
}

struct MultiBeClusterHarness {
    #[allow(dead_code)]
    bes: Vec<ProcessGuard>,
    fe: Option<ProcessGuard>,
    fe_mysql: u16,
    be_http_ports: Vec<u16>,
    #[allow(dead_code)]
    _be_configs: Vec<NamedTempFile>,
    fe_config: NamedTempFile,
    be_log_dirs: Vec<PathBuf>,
    fe_log_dir: PathBuf,
    _log_root: TempDir,
}

impl MultiBeClusterHarness {
    fn start_n_be(n: usize, be_debug: &str, fe_extra: &str) -> Self {
        // Callers that explicitly exercise StateStore lifecycle supply their
        // own durable backend; do not render a duplicate TOML table for them.
        Self::start_n_be_with_options(n, be_debug, fe_extra, !fe_extra.contains("[state_store]"))
    }

    fn start_n_be_without_state_store(n: usize, be_debug: &str, fe_extra: &str) -> Self {
        Self::start_n_be_with_options(n, be_debug, fe_extra, false)
    }

    fn start_n_be_with_options(
        n: usize,
        be_debug: &str,
        fe_extra: &str,
        default_state_store: bool,
    ) -> Self {
        Self::start_n_be_with_options_and_standalone_extra(
            n,
            be_debug,
            fe_extra,
            default_state_store,
            "",
        )
    }

    fn start_n_be_with_options_and_standalone_extra(
        n: usize,
        be_debug: &str,
        fe_extra: &str,
        default_state_store: bool,
        standalone_server_extra: &str,
    ) -> Self {
        assert!(n >= 1, "must spawn at least one BE");

        // Reserve all ports up front before releasing any of them.
        struct BePortSet {
            http: ReservedPort,
            grpc: ReservedPort,
        }
        let mut be_port_sets: Vec<BePortSet> = (0..n)
            .map(|_| BePortSet {
                http: ReservedPort::new(),
                grpc: ReservedPort::new(),
            })
            .collect();
        let fe_mysql = ReservedPort::new();
        let fe_http = ReservedPort::new();
        let fe_grpc = ReservedPort::new();

        // Collect port numbers before consuming the ReservedPort structs.
        let be_http_ports: Vec<u16> = be_port_sets.iter().map(|s| s.http.port()).collect();
        let be_grpc_ports: Vec<u16> = be_port_sets.iter().map(|s| s.grpc.port()).collect();
        let fe_mysql_port = fe_mysql.port();
        let fe_http_port = fe_http.port();
        let fe_grpc_port = fe_grpc.port();
        let log_root = TempFileBuilder::new()
            .prefix("cluster-logs-")
            .tempdir_in(runtime_dir())
            .expect("create cluster log root");
        let be_log_dirs = (0..n)
            .map(|index| log_root.path().join(format!("be-{index}")))
            .collect::<Vec<_>>();
        let fe_log_dir = log_root.path().join("fe");
        let default_state_store_config = if default_state_store {
            format!(
                r#"
[state_store]
provider = "sqlite"
path = "{}"
cluster_id = "cluster-mvp-{}"
deployment_owner = "fe-1"
"#,
                log_root.path().join("frontend-state.sqlite").display(),
                fe_mysql_port,
            )
        } else {
            String::new()
        };

        // Write all BE configs (while ports are still reserved).
        let be_configs: Vec<NamedTempFile> = be_port_sets
            .iter()
            .enumerate()
            .map(|(i, _)| {
                let http_port = be_http_ports[i];
                let grpc_port = be_grpc_ports[i];
                write_config(
                    &format!("be{i}"),
                    &format!(
                        r#"
sys_log_dir = "{}"

[server]
host = "127.0.0.1"
http_port = {http_port}
grpc_port = {grpc_port}

[cluster]
role = "be"
{be_debug}
"#,
                        be_log_dirs[i].display()
                    ),
                )
            })
            .collect();

        // Build the backends list for the FE config.
        let backends_list: String = be_grpc_ports
            .iter()
            .map(|p| format!("\"127.0.0.1:{p}\""))
            .collect::<Vec<_>>()
            .join(", ");
        let fe_config = write_config(
            "fe",
            &format!(
                r#"
sys_log_dir = "{}"

[server]
host = "127.0.0.1"
http_port = {fe_http_port}
grpc_port = {fe_grpc_port}

[standalone_server]
mysql_port = {fe_mysql_port}
{standalone_server_extra}

[cluster]
role = "fe"
backends = [{backends_list}]
{default_state_store_config}
{fe_extra}
"#,
                fe_log_dir.display()
            ),
        );

        // Spawn all BEs first (releasing each BE's reserved ports immediately
        // before its own spawn), then wait for all readiness in a second pass.
        let mut bes: Vec<ProcessGuard> = Vec::with_capacity(n);
        for (i, port_set) in be_port_sets.drain(..).enumerate() {
            let _ = port_set.http.release();
            let _ = port_set.grpc.release();
            bes.push(ProcessGuard::spawn_backend(be_configs[i].path(), i));
        }
        for be in &mut bes {
            be.wait_for_ready("NOVAROCKS_READY role=be");
        }

        // Release FE ports and spawn FE.
        let _ = fe_mysql.release();
        let _ = fe_http.release();
        let _ = fe_grpc.release();
        let mut fe = ProcessGuard::spawn(fe_config.path());
        fe.wait_for_ready("NOVAROCKS_READY mysql_port=");

        Self {
            bes,
            fe: Some(fe),
            fe_mysql: fe_mysql_port,
            be_http_ports,
            _be_configs: be_configs,
            fe_config,
            be_log_dirs,
            fe_log_dir,
            _log_root: log_root,
        }
    }

    fn start_three_be_sqlite_state_store(state_store_path: &Path, cluster_id: &str) -> Self {
        Self::start_three_be_sqlite_state_store_with_extra(state_store_path, cluster_id, "")
    }

    fn start_three_be_sqlite_state_store_with_extra(
        state_store_path: &Path,
        cluster_id: &str,
        fe_extra: &str,
    ) -> Self {
        assert!(
            state_store_path.is_absolute(),
            "SQLite StateStore path must be absolute: {}",
            state_store_path.display()
        );
        let state_store_config = format!(
            r#"
[state_store]
provider = "sqlite"
path = "{}"
cluster_id = "{cluster_id}"
deployment_owner = "fe-1"

{fe_extra}
"#,
            state_store_path.display()
        );
        Self::start_n_be_with_options(3, "", &state_store_config, false)
    }

    fn start_three_be_sqlite_state_store_with_metadata(
        state_store_path: &Path,
        metadata_path: &Path,
        cluster_id: &str,
    ) -> Self {
        Self::start_three_be_sqlite_state_store_with_metadata_and_be_extra(
            state_store_path,
            metadata_path,
            cluster_id,
            "",
        )
    }

    fn start_three_be_sqlite_state_store_with_metadata_and_be_extra(
        state_store_path: &Path,
        metadata_path: &Path,
        cluster_id: &str,
        be_extra: &str,
    ) -> Self {
        Self::start_three_be_sqlite_state_store_with_metadata_and_extras(
            state_store_path,
            metadata_path,
            cluster_id,
            be_extra,
            "",
        )
    }

    fn start_three_be_sqlite_state_store_with_metadata_and_extras(
        state_store_path: &Path,
        metadata_path: &Path,
        cluster_id: &str,
        be_extra: &str,
        frontend_extra: &str,
    ) -> Self {
        assert!(
            state_store_path.is_absolute(),
            "SQLite StateStore path must be absolute: {}",
            state_store_path.display()
        );
        assert!(
            metadata_path.is_absolute(),
            "SQLite metadata path must be absolute: {}",
            metadata_path.display()
        );
        let fe_extra = format!(
            r#"
[metadata]
provider = "sqlite"
path = "{}"

[state_store]
provider = "sqlite"
path = "{}"
cluster_id = "{cluster_id}"
deployment_owner = "fe-1"
"#,
            metadata_path.display(),
            state_store_path.display(),
        );
        Self::start_n_be_with_options_and_standalone_extra(
            3,
            be_extra,
            &fe_extra,
            false,
            frontend_extra,
        )
    }

    fn start_three_be_sqlite_state_store_with_metadata_and_fault_dir(
        state_store_path: &Path,
        metadata_path: &Path,
        cluster_id: &str,
        fault_dir: &Path,
    ) -> Self {
        let debug = format!(
            r#"
[debug]
query_lifecycle_fault_dir = "{}"
"#,
            fault_dir.display()
        );
        let fe_extra = format!(
            r#"
[metadata]
provider = "sqlite"
path = "{}"

[state_store]
provider = "sqlite"
path = "{}"
cluster_id = "{cluster_id}"
deployment_owner = "fe-1"

{debug}
"#,
            metadata_path.display(),
            state_store_path.display(),
        );
        Self::start_n_be_with_options(3, &debug, &fe_extra, false)
    }

    fn fe_mysql_port(&self) -> u16 {
        self.fe_mysql
    }

    fn log_diagnostics(&self) -> String {
        format!(
            "FE log dir={}; BE log dirs={:?}",
            self.fe_log_dir.display(),
            self.be_log_dirs
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
        )
    }

    #[cfg(unix)]
    fn shutdown_fe_cleanly(&mut self, timeout: Duration) {
        let mut fe = self.fe.take().expect("FE process must be running");
        fe.shutdown_cleanly(timeout);
    }

    #[cfg(unix)]
    fn kill_fe(&mut self) {
        let mut fe = self.fe.take().expect("FE process must be running");
        fe.child.kill().expect("kill frontend process");
        let status = fe.child.wait().expect("reap killed frontend process");
        assert!(
            !status.success(),
            "explicit recovery fault must terminate the frontend process"
        );
    }

    #[cfg(unix)]
    fn restart_fe(&mut self) {
        assert!(self.fe.is_none(), "old FE process must be stopped");
        let mut fe = ProcessGuard::spawn(self.fe_config.path());
        fe.wait_for_ready("NOVAROCKS_READY mysql_port=");
        self.fe = Some(fe);
    }

    #[cfg(unix)]
    fn wait_for_fe_output_contains(&mut self, marker: &str, timeout: Duration) {
        self.fe
            .as_mut()
            .expect("FE process must be running")
            .wait_for_output_contains(marker, timeout);
    }

    fn wait_for_every_be_output_contains(
        &mut self,
        marker: &str,
        timeout: Duration,
    ) -> Vec<Vec<String>> {
        let deadline = Instant::now() + timeout;
        let mut stdout = vec![Vec::new(); self.bes.len()];
        let mut observed = vec![false; self.bes.len()];
        loop {
            for (index, be) in self.bes.iter_mut().enumerate() {
                if let Some(status) = be.child.try_wait().expect("poll BE child") {
                    panic!(
                        "BE {index} exited before marker `{marker}` with status {status}; stdout={:?}; stderr={}",
                        stdout[index],
                        be.read_stderr()
                    );
                }
                while let Ok(line) = be.stdout_rx.try_recv() {
                    observed[index] |= line.contains(marker);
                    stdout[index].push(line);
                }
            }
            if observed.iter().all(|seen| *seen) {
                return stdout;
            }
            assert!(
                Instant::now() < deadline,
                "every BE must emit `{marker}`; observed={observed:?}; stdout={stdout:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn wait_for_connector_readers_to_close(
        &mut self,
        mut stdout: Vec<Vec<String>>,
        timeout: Duration,
    ) -> Vec<Vec<String>> {
        let deadline = Instant::now() + timeout;
        loop {
            for (index, be) in self.bes.iter_mut().enumerate() {
                if let Some(status) = be.child.try_wait().expect("poll BE child") {
                    panic!(
                        "BE {index} exited before connector readers closed with status {status}; stdout={:?}; stderr={}",
                        stdout[index],
                        be.read_stderr()
                    );
                }
                while let Ok(line) = be.stdout_rx.try_recv() {
                    stdout[index].push(line);
                }
            }
            let counts = stdout
                .iter()
                .map(|lines| {
                    let opens = lines
                        .iter()
                        .filter(|line| line.contains("NOVAROCKS_CONNECTOR_UNIT_READER_OPEN"))
                        .count();
                    let closes = lines
                        .iter()
                        .filter(|line| line.contains("NOVAROCKS_CONNECTOR_UNIT_READER_CLOSE"))
                        .count();
                    (opens, closes)
                })
                .collect::<Vec<_>>();
            if counts
                .iter()
                .all(|(opens, closes)| *opens > 0 && opens == closes)
            {
                return stdout;
            }
            assert!(
                Instant::now() < deadline,
                "connector reader open/close markers did not balance after terminal cancellation: counts={counts:?}; stdout={stdout:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

fn coordinated_query_sql() -> &'static str {
    "SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 2) t ORDER BY v"
}

fn coordinated_sleep_query_sql() -> &'static str {
    "SELECT v FROM (SELECT sleep(2) AS v UNION ALL SELECT sleep(2)) t ORDER BY v"
}

fn disconnect_blocking_query_sql() -> &'static str {
    "SELECT v FROM (SELECT sleep(10) AS v UNION ALL SELECT sleep(10)) t ORDER BY v"
}

fn multi_submit_query_sql() -> &'static str {
    "WITH cte AS (SELECT 1 AS v UNION ALL SELECT 2) \
     SELECT a.v FROM cte a JOIN cte b ON a.v = b.v ORDER BY a.v"
}

fn cancellation_acceptance_query_sql() -> &'static str {
    "SELECT t.s \
     FROM ( \
       SELECT sleep(10) AS s \
       FROM qlc_cancel_catalog.qlc_cancel.cancellation_data \
     ) AS t \
     CROSS JOIN TABLE(generate_series(1, 1000000000)) AS gs(x)"
}

fn assert_mysql_server_error(error: mysql::Error, expected_code: u16) {
    match error {
        mysql::Error::MySqlError(error) => assert_eq!(
            error.code, expected_code,
            "expected MySQL error {expected_code}, got {error}"
        ),
        other => panic!("expected MySQL server error {expected_code}, got {other}"),
    }
}

fn read_packet(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut header = [0u8; 4];
    stream
        .read_exact(&mut header)
        .expect("read mysql packet header");
    let len =
        usize::from(header[0]) | (usize::from(header[1]) << 8) | (usize::from(header[2]) << 16);
    let mut payload = vec![0u8; len];
    stream
        .read_exact(&mut payload)
        .expect("read mysql packet payload");
    (header[3], payload)
}

fn write_packet(stream: &mut TcpStream, seq: u8, payload: &[u8]) {
    let len = u32::try_from(payload.len()).expect("payload fits u32");
    assert!(len <= 0x00ff_ffff, "payload too large");
    let header = [
        (len & 0xff) as u8,
        ((len >> 8) & 0xff) as u8,
        ((len >> 16) & 0xff) as u8,
        seq,
    ];
    stream
        .write_all(&header)
        .expect("write mysql packet header");
    stream
        .write_all(payload)
        .expect("write mysql packet payload");
    stream.flush().expect("flush mysql packet");
}

fn send_mysql_query(port: u16, sql: &str) -> TcpStream {
    const CLIENT_LONG_PASSWORD: u32 = 0x0000_0001;
    const CLIENT_LONG_FLAG: u32 = 0x0000_0004;
    const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
    const CLIENT_TRANSACTIONS: u32 = 0x0000_2000;
    const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
    const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;

    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect raw mysql client");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set read timeout");
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .expect("set write timeout");

    let (_seq, handshake) = read_packet(&mut stream);
    assert_eq!(handshake[0], 10, "expected protocol v10 handshake");

    let mut response = Vec::new();
    let client_flags = CLIENT_LONG_PASSWORD
        | CLIENT_LONG_FLAG
        | CLIENT_PROTOCOL_41
        | CLIENT_TRANSACTIONS
        | CLIENT_SECURE_CONNECTION
        | CLIENT_PLUGIN_AUTH;
    response.extend_from_slice(&client_flags.to_le_bytes());
    response.extend_from_slice(&(16_u32 * 1024 * 1024).to_le_bytes());
    response.push(45);
    response.extend_from_slice(&[0u8; 23]);
    response.extend_from_slice(b"root");
    response.push(0);
    response.push(0);
    response.extend_from_slice(b"mysql_native_password");
    response.push(0);
    write_packet(&mut stream, 1, &response);

    let (_seq, auth_result) = read_packet(&mut stream);
    assert_ne!(
        auth_result.first().copied(),
        Some(0xff),
        "authentication failed"
    );

    let mut query_payload = Vec::with_capacity(sql.len() + 1);
    query_payload.push(0x03);
    query_payload.extend_from_slice(sql.as_bytes());
    write_packet(&mut stream, 0, &query_payload);

    stream
}

fn show_backends(conn: &mut MysqlConn) -> Vec<Row> {
    conn.query("SHOW BACKENDS").expect("SHOW BACKENDS")
}

fn assert_exact_live_backends(conn: &mut MysqlConn, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let rows = show_backends(conn);
        if rows.len() == expected
            && rows
                .iter()
                .all(|row| row.get::<String, usize>(3).as_deref() == Some("Live"))
        {
            println!("SHOW BACKENDS {expected}/{expected} Live");
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected exactly {expected} Live backends; rows={rows:?}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn wait_for_mv_rows(conn: &mut MysqlConn, sql: &str, expected: &[(i32, i64)], diagnostics: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let rows: Result<Vec<(i32, i64)>, mysql::Error> = conn.query(sql);
        if matches!(&rows, Ok(rows) if rows.as_slice() == expected) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "scheduled MV did not converge; expected={expected:?}; observed={rows:?}; {diagnostics}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn wait_for_scheduler_marker_count(directory: &Path, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let count = std::fs::read_dir(directory)
            .expect("read MVX-4 scheduler barrier directory")
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("mvx4-scheduler-admitted-")
            })
            .count();
        if count == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected {expected} frontend MV scheduler barrier markers, observed {count}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn scheduled_fragments(conn: &mut MysqlConn) -> u64 {
    let rows = show_backends(conn);
    rows.iter()
        .filter(|row| row.get::<String, usize>(3).as_deref() == Some("Live"))
        .map(|row| {
            let value = row.get::<String, usize>(4).unwrap_or_else(|| {
                panic!("Live backend must expose ScheduledFragments; rows={rows:?}")
            });
            value.parse::<u64>().unwrap_or_else(|error| {
                panic!(
                    "Live backend ScheduledFragments must be an unsigned integer \
                     ({value:?}): {error}; rows={rows:?}"
                )
            })
        })
        .sum()
}

fn show_optimize_jobs(
    conn: &mut MysqlConn,
    catalog: &str,
    database: &str,
    table: &str,
) -> Vec<Row> {
    conn.query(format!(
        "SHOW ALTER TABLE OPTIMIZE FROM {catalog}.{database} \
         WHERE TableName = '{table}' ORDER BY CreateTime DESC"
    ))
    .expect("SHOW ALTER TABLE OPTIMIZE")
}

fn wait_for_latest_optimize_finished(
    conn: &mut MysqlConn,
    catalog: &str,
    database: &str,
    table: &str,
    minimum_job_count: usize,
    diagnostics: &str,
) -> String {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let rows = show_optimize_jobs(conn, catalog, database, table);
        let row_summaries = rows
            .iter()
            .map(|row| {
                (
                    row.get::<String, usize>(0),
                    row.get::<String, usize>(2),
                    row.get::<String, usize>(5),
                )
            })
            .collect::<Vec<_>>();
        if rows.len() >= minimum_job_count {
            let job_id = rows[0].get::<String, usize>(0).expect("optimize JobId");
            let state = rows[0].get::<String, usize>(2).expect("optimize State");
            match state.as_str() {
                "FINISHED" => {
                    println!(
                        "SHOW ALTER TABLE OPTIMIZE latest job {job_id} FINISHED ({}/{minimum_job_count} jobs)",
                        rows.len()
                    );
                    return job_id;
                }
                "FAILED" => {
                    panic!("optimize job {job_id} failed; rows={row_summaries:?}; {diagnostics}");
                }
                _ => {}
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for latest optimize job to finish; rows={row_summaries:?}; {diagnostics}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn backend_row_by_port(rows: &[Row], port: u16) -> Option<&Row> {
    let port = port.to_string();
    rows.iter()
        .find(|row| row.get::<String, usize>(2).as_deref() == Some(port.as_str()))
}

fn wait_for_backend_state(conn: &mut MysqlConn, port: u16, expected_state: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let rows = show_backends(conn);
        if let Some(row) = backend_row_by_port(&rows, port) {
            if row.get::<String, usize>(3).as_deref() == Some(expected_state) {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "backend {port} did not reach state {expected_state}; rows={rows:?}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn wait_until_backend_removed(conn: &mut MysqlConn, port: u16) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let rows = show_backends(conn);
        if backend_row_by_port(&rows, port).is_none() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "backend {port} was not removed; rows={rows:?}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn fetch_http_text(port: u16, path: &str) -> String {
    let url = format!("http://127.0.0.1:{port}{path}");
    reqwest::blocking::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build reqwest client")
        .get(&url)
        .send()
        .unwrap_or_else(|err| panic!("GET {url} failed: {err}"))
        .error_for_status()
        .unwrap_or_else(|err| panic!("GET {url} status failed: {err}"))
        .text()
        .unwrap_or_else(|err| panic!("read {url} text failed: {err}"))
}

fn backend_query_lifecycle_termination_count(port: u16, reason: &str) -> u64 {
    let metric = "novarocks_backend_query_lifecycle_terminations";
    let reason_label = format!("reason=\"{reason}\"");
    fetch_http_text(port, "/metrics")
        .lines()
        .find(|line| line.starts_with(metric) && line.contains(&reason_label))
        .and_then(|line| line.split_ascii_whitespace().last())
        .and_then(|value| value.parse::<f64>().ok())
        .map(|value| value as u64)
        .unwrap_or(0)
}

fn wait_for_backend_running_fragment_control(port: u16, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let metrics = fetch_http_text(port, "/metrics");
        let running = metrics
            .lines()
            .find(|line| {
                line.starts_with("novarocks_backend_query_execution_resources")
                    && line.contains("resource=\"fragment_controls_running\"")
            })
            .and_then(|line| line.split_ascii_whitespace().last())
            .and_then(|value| value.parse::<f64>().ok())
            .unwrap_or(0.0);
        if running >= 1.0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "backend {port} did not start a fragment control before client disconnect; metrics={:?}",
            metrics
                .lines()
                .filter(|line| line.starts_with("novarocks_backend_query_execution_resources"))
                .collect::<Vec<_>>()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn wait_for_backend_query_lifecycle_termination(port: u16, reason: &str, timeout: Duration) {
    wait_for_backend_query_lifecycle_termination_any(port, &[reason], timeout);
}

fn wait_for_backend_query_lifecycle_termination_any(
    port: u16,
    reasons: &[&str],
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    loop {
        if reasons
            .iter()
            .any(|reason| backend_query_lifecycle_termination_count(port, reason) >= 1)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "backend {port} did not publish query lifecycle termination reasons={reasons:?}; termination_metrics={:?}",
            fetch_http_text(port, "/metrics")
                .lines()
                .filter(|line| line.starts_with("novarocks_backend_query_lifecycle_terminations"))
                .collect::<Vec<_>>()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn all_in_one_loopback_stage_start_select_succeeds() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _lock = lock_cluster_mvp();

    let (mut srv, mysql_port) = start_all_in_one(
        r#"
[debug]
emit_grpc_fragment_marker = true
"#,
    );
    let mut conn = connect_mysql(mysql_port);
    let rows: Vec<i64> = conn.query("SELECT 1").expect("SELECT 1");
    assert_eq!(rows, vec![1]);
    srv.wait_for_output_contains("NOVAROCKS_GRPC_FETCH_TYPED status=", Duration::from_secs(3));
}

#[test]
fn cross_process_remote_dispatcher_smoke() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _lock = lock_cluster_mvp();

    let be_http = ReservedPort::new();
    let be_grpc = ReservedPort::new();
    let fe_mysql = ReservedPort::new();
    let fe_http = ReservedPort::new();
    let fe_grpc = ReservedPort::new();
    let be_http_port = be_http.port();
    let be_grpc_port = be_grpc.port();
    let fe_mysql_port = fe_mysql.port();
    let fe_http_port = fe_http.port();
    let fe_grpc_port = fe_grpc.port();
    let state_store_dir = tempfile::tempdir_in(runtime_dir()).expect("create StateStore tempdir");
    let state_store_path = state_store_dir.path().join("frontend-state.sqlite");

    let be_config = write_config(
        "be",
        &format!(
            r#"
[server]
host = "127.0.0.1"
http_port = {be_http_port}
grpc_port = {be_grpc_port}

[cluster]
role = "be"
"#
        ),
    );
    // Spec (PR-4): FE backends must point to be_grpc (the NovaRocksGrpc
    // service port for SubmitFragment/FetchResult on the standalone BE).
    let fe_config = write_config(
        "fe",
        &format!(
            r#"
[server]
host = "127.0.0.1"
http_port = {fe_http_port}
grpc_port = {fe_grpc_port}

[standalone_server]
mysql_port = {fe_mysql_port}

[cluster]
role = "fe"
backends = ["127.0.0.1:{be_grpc_port}"]

[state_store]
provider = "sqlite"
path = "{}"
cluster_id = "remote-dispatcher-{fe_mysql_port}"
deployment_owner = "fe-1"
"#,
            state_store_path.display()
        ),
    );

    let _ = be_http.release();
    let _ = be_grpc.release();
    let mut be = ProcessGuard::spawn(be_config.path());
    be.wait_for_ready("NOVAROCKS_READY role=be");

    let _ = fe_mysql.release();
    let _ = fe_http.release();
    let _ = fe_grpc.release();
    let mut fe = ProcessGuard::spawn(fe_config.path());
    fe.wait_for_ready("NOVAROCKS_READY mysql_port=");

    let mut conn = connect_mysql(fe_mysql_port);

    // Phase 1: run a query that forces a Coordinated (multi-fragment) plan.
    // SELECT + ORDER BY on a non-trivial UNION forces Sort(Distribution(Gather))
    // which splits into two fragments, routing through RemoteDispatcher to the BE.
    let rows: Vec<String> = conn
        .query(coordinated_query_sql())
        .expect("coordinated query must succeed while BE is running");
    assert_eq!(
        rows,
        vec!["1".to_string(), "2".to_string()],
        "coordinated query must return sorted results"
    );

    // Phase 2: kill the BE and prove the same query now fails.
    // If the query were executing locally (SingleFragment), it would succeed
    // even without the BE — the failure here is the proof that the BE was
    // actually involved in Phase 1.
    drop(be);
    std::thread::sleep(Duration::from_millis(300));

    let err = conn
        .query::<String, _>(coordinated_query_sql())
        .expect_err("coordinated query must fail once BE is down");
    let err_str = err.to_string();
    assert!(
        !err_str.is_empty(),
        "expected a non-empty error when BE is unreachable, got empty string"
    );
}

#[cfg(unix)]
#[test]
fn native_be_signal_shutdown_releases_port_for_restart() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _lock = lock_cluster_mvp();

    let grpc = ReservedPort::new();
    let grpc_port = grpc.port();
    let http = ReservedPort::new();
    let http_port = http.port();
    let config = write_config(
        "native-be-signal-restart",
        &format!(
            r#"
[server]
host = "127.0.0.1"
grpc_port = {grpc_port}
http_port = {http_port}

[cluster]
role = "be"
"#
        ),
    );

    let _ = grpc.release();
    let _ = http.release();
    let mut first = ProcessGuard::spawn(config.path());
    first.wait_for_ready("NOVAROCKS_READY role=be");
    first.shutdown_cleanly(Duration::from_secs(10));

    let rebound = TcpListener::bind(("127.0.0.1", grpc_port))
        .expect("native BE gRPC port must be reusable immediately after SIGINT shutdown");
    drop(rebound);

    let mut restarted = ProcessGuard::spawn(config.path());
    restarted.wait_for_ready("NOVAROCKS_READY role=be");
    restarted.shutdown_cleanly(Duration::from_secs(10));
}

#[test]
fn d4_dynamic_backend_sql_and_metrics_smoke() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _lock = lock_cluster_mvp();

    let be_http = ReservedPort::new();
    let be_grpc = ReservedPort::new();
    let fe_mysql = ReservedPort::new();
    let fe_http = ReservedPort::new();
    let fe_grpc = ReservedPort::new();
    let be_http_port = be_http.port();
    let be_grpc_port = be_grpc.port();
    let fe_mysql_port = fe_mysql.port();
    let fe_http_port = fe_http.port();
    let fe_grpc_port = fe_grpc.port();
    let state_store_dir = tempfile::tempdir_in(runtime_dir()).expect("create StateStore tempdir");
    let state_store_path = state_store_dir.path().join("frontend-state.sqlite");

    let be_config = write_config(
        "d4-be",
        &format!(
            r#"
[server]
host = "127.0.0.1"
http_port = {be_http_port}
grpc_port = {be_grpc_port}

[cluster]
role = "be"
"#
        ),
    );
    let fe_config = write_config(
        "d4-fe",
        &format!(
            r#"
[server]
host = "127.0.0.1"
http_port = {fe_http_port}
grpc_port = {fe_grpc_port}

[standalone_server]
mysql_port = {fe_mysql_port}

[cluster]
role = "fe"
backends = []
heartbeat_interval_ms = 200
heartbeat_timeout_retries = 2

[state_store]
provider = "sqlite"
path = "{}"
cluster_id = "d4-dynamic-backend-{fe_mysql_port}"
deployment_owner = "fe-1"
"#,
            state_store_path.display()
        ),
    );

    let _ = be_http.release();
    let _ = be_grpc.release();
    let mut be = ProcessGuard::spawn(be_config.path());
    be.wait_for_ready("NOVAROCKS_READY role=be");

    let _ = fe_mysql.release();
    let _ = fe_http.release();
    let _ = fe_grpc.release();
    let mut fe = ProcessGuard::spawn(fe_config.path());
    fe.wait_for_ready("NOVAROCKS_READY mysql_port=");

    let mut conn = connect_mysql(fe_mysql_port);
    assert!(
        show_backends(&mut conn).is_empty(),
        "FE should start with an empty dynamic backend registry"
    );

    let backend_addr = format!("127.0.0.1:{be_grpc_port}");
    conn.query_drop(format!("ADD BACKEND '{backend_addr}'"))
        .expect("ADD BACKEND");
    wait_for_backend_state(&mut conn, be_grpc_port, "Live");

    let rows: Vec<i64> = conn
        .query(coordinated_query_sql())
        .expect("coordinated query must succeed after ADD BACKEND");
    assert_eq!(rows, vec![1i64, 2i64]);

    let metrics = fetch_http_text(fe_grpc_port, "/metrics");
    for needle in [
        "novarocks_fragment_scheduled_total",
        "novarocks_exchange_shuffle_bytes_total",
        "novarocks_heartbeat_rtt_seconds",
        "novarocks_live_backends",
    ] {
        assert!(
            metrics.contains(needle),
            "metrics scrape must contain {needle}; body={metrics}"
        );
    }

    conn.query_drop(format!("DROP BACKEND '{backend_addr}' FORCE"))
        .expect("DROP BACKEND FORCE");
    wait_until_backend_removed(&mut conn, be_grpc_port);
}

#[test]
fn mysql_disconnect_triggers_cancel() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _lock = lock_cluster_mvp();

    let mut cluster = ClusterHarness::start("", "");

    let stream = send_mysql_query(cluster.fe_mysql, disconnect_blocking_query_sql());
    wait_for_backend_running_fragment_control(cluster.be_http, Duration::from_secs(3));
    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown raw mysql client");

    wait_for_backend_query_lifecycle_termination(
        cluster.be_http,
        "coordinator_abort",
        Duration::from_secs(3),
    );
}

#[test]
fn query_timeout_triggers_cancel() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _lock = lock_cluster_mvp();

    let cluster = ClusterHarness::start("", "");

    let mut conn = connect_mysql(cluster.fe_mysql);
    conn.query_drop("SET query_timeout = 1")
        .expect("set query timeout");
    let err = conn
        .query::<String, _>(coordinated_sleep_query_sql())
        .expect_err("query should time out while BE is still executing");
    let err_str = err.to_string();
    assert!(
        err_str.contains("timed out") || err_str.contains("timeout"),
        "expected timeout error, got: {err_str}"
    );

    wait_for_backend_query_lifecycle_termination_any(
        cluster.be_http,
        &["coordinator_abort", "local_failure"],
        Duration::from_secs(5),
    );
}

#[test]
fn cross_process_three_be_connector_read_distributes_splits_and_cancels() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _guard = lock_cluster_mvp();
    let fixture_dir =
        tempfile::tempdir_in(runtime_dir()).expect("create cancellation fixture directory");
    let mut cluster =
        MultiBeClusterHarness::start_three_be_sqlite_state_store_with_metadata_and_be_extra(
            &fixture_dir.path().join("frontend-state.sqlite"),
            &fixture_dir.path().join("frontend-metadata.sqlite"),
            "connector-cancellation",
            r#"
[debug]
emit_grpc_fragment_marker = true
emit_connector_reader_marker = true
emit_cancel_marker = true

[runtime]
operator_buffer_chunks = 1
"#,
        );
    let warehouse = tempfile::tempdir_in(runtime_dir()).expect("create cancellation warehouse");
    let mut control = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut control, 3);
    control
        .query_drop(format!(
            r#"CREATE EXTERNAL CATALOG qlc_cancel_catalog PROPERTIES("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        ))
        .expect("create cancellation catalog");
    control
        .query_drop("CREATE DATABASE qlc_cancel_catalog.qlc_cancel")
        .expect("create cancellation database");
    control
        .query_drop("CREATE TABLE qlc_cancel_catalog.qlc_cancel.cancellation_data (v BIGINT)")
        .expect("create cancellation table");
    // Each insert intentionally produces a file larger than the connector's
    // 4,096-row physical batch.  Together with the one-chunk scan buffer this
    // keeps every assigned reader live under ordinary output backpressure;
    // cancellation is then exercised against real data-file I/O rather than
    // a query that has already reached EOF and is only blocked downstream.
    for range in ["1, 100000", "100001, 200000", "200001, 300000"] {
        control
            .query_drop(format!(
                "INSERT INTO qlc_cancel_catalog.qlc_cancel.cancellation_data \
                 SELECT generate_series FROM TABLE(generate_series({range}))"
            ))
            .expect("write a distinct scan range for cancellation acceptance");
    }
    for be in &mut cluster.bes {
        while be.stdout_rx.try_recv().is_ok() {}
    }

    let (target_ready_tx, target_ready_rx) = mpsc::sync_channel(1);
    let (target_done_tx, target_done_rx) = mpsc::sync_channel(1);
    let (target_release_tx, target_release_rx) = mpsc::sync_channel(0);
    let fe_mysql = cluster.fe_mysql_port();
    let target = std::thread::spawn(move || {
        let mut conn = connect_mysql(fe_mysql);
        target_ready_tx
            .send(conn.connection_id())
            .expect("publish target connection id");
        let result = conn.query::<i64, _>(cancellation_acceptance_query_sql());
        target_done_tx.send(result).expect("publish target result");
        target_release_rx.recv().expect("release target connection");
    });
    let target_connection_id = target_ready_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("target connection id");

    let reader_open_output = cluster.wait_for_every_be_output_contains(
        "NOVAROCKS_CONNECTOR_UNIT_READER_OPEN provider=iceberg",
        Duration::from_secs(10),
    );
    let pre_kill_reader_counts = reader_open_output
        .iter()
        .map(|lines| {
            let opens = lines
                .iter()
                .filter(|line| line.contains("NOVAROCKS_CONNECTOR_UNIT_READER_OPEN"))
                .count();
            let closes = lines
                .iter()
                .filter(|line| line.contains("NOVAROCKS_CONNECTOR_UNIT_READER_CLOSE"))
                .count();
            (opens, closes)
        })
        .collect::<Vec<_>>();
    assert!(
        pre_kill_reader_counts
            .iter()
            .all(|(opens, closes)| *opens > *closes),
        "every BE must retain an in-flight connector reader until KILL QUERY: counts={pre_kill_reader_counts:?}; stdout={reader_open_output:?}"
    );
    if let Ok(result) = target_done_rx.try_recv() {
        panic!(
            "target query completed before KILL QUERY established connector-read cancellation: {result:?}"
        );
    }
    control
        .query_drop(format!("KILL QUERY {target_connection_id}"))
        .expect("KILL QUERY must acknowledge the active target statement");

    let target_error = target_done_rx
        .recv_timeout(Duration::from_secs(15))
        .expect("target query must terminate after KILL QUERY")
        .expect_err("target query must not succeed after KILL QUERY");
    assert_mysql_server_error(target_error, 1317);
    for port in &cluster.be_http_ports {
        wait_for_backend_query_lifecycle_termination(
            *port,
            "coordinator_abort",
            Duration::from_secs(10),
        );
    }
    let terminal_reader_output =
        cluster.wait_for_connector_readers_to_close(reader_open_output, Duration::from_secs(10));
    assert!(
        terminal_reader_output.iter().all(|lines| lines
            .iter()
            .any(|line| line.contains("NOVAROCKS_QUERY_LIFECYCLE_ABORT"))),
        "each BE must accept the lifecycle Abort cancellation request: {terminal_reader_output:?}"
    );
    for lines in &terminal_reader_output {
        let cancel_index = lines
            .iter()
            .position(|line| line.contains("NOVAROCKS_QUERY_LIFECYCLE_ABORT"))
            .expect("each BE cancellation marker was asserted above");
        assert!(
            lines[cancel_index + 1..]
                .iter()
                .all(|line| !line.contains("NOVAROCKS_CONNECTOR_UNIT_READER_OPEN")),
            "terminal cancellation must reject new connector reader opens: {lines:?}"
        );
    }

    let idle_error = control
        .query_drop(format!("KILL QUERY {target_connection_id}"))
        .expect_err("an idle target session has no active query");
    assert_mysql_server_error(idle_error, 1094);
    let unknown_error = control
        .query_drop("KILL QUERY 4000000000")
        .expect_err("an unknown connection must be rejected");
    assert_mysql_server_error(unknown_error, 1094);
    target_release_tx
        .send(())
        .expect("release target connection after idle assertion");
    target.join().expect("target thread must join");

    let rows: Vec<i64> = control
        .query(coordinated_query_sql())
        .expect("a later distributed query must succeed after cancellation cleanup");
    assert_eq!(rows, vec![1, 2]);
}

#[test]
fn cross_process_three_be_connector_catalog_mutation_is_visible_to_non_empty_reads() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _guard = lock_cluster_mvp();
    let metadata_dir =
        tempfile::tempdir_in(runtime_dir()).expect("create catalog mutation metadata directory");
    let metadata_config = format!(
        r#"
[metadata]
provider = "sqlite"
path = "{}"
"#,
        metadata_dir.path().join("catalog.db").display()
    );
    let mut cluster = MultiBeClusterHarness::start_n_be(
        3,
        r#"
[debug]
emit_connector_reader_marker = true
"#,
        &metadata_config,
    );
    let warehouse = tempfile::tempdir_in(runtime_dir()).expect("create catalog mutation warehouse");
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop(format!(
        r#"CREATE EXTERNAL CATALOG mutation_catalog PROPERTIES("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
        warehouse.path().display()
    ))
    .expect("create catalog mutation Iceberg catalog");
    conn.query_drop("CREATE DATABASE mutation_catalog.mutation_db")
        .expect("create catalog mutation namespace");
    conn.query_drop("CREATE DATABASE IF NOT EXISTS mutation_catalog.mutation_db")
        .expect("namespace IF NOT EXISTS must be a provider NoOp");
    conn.query_drop(
        "CREATE TABLE mutation_catalog.mutation_db.data (id BIGINT, value STRING) \
         TBLPROPERTIES ('format-version' = '3')",
    )
    .expect("create catalog mutation table");
    conn.query_drop(
        "CREATE TABLE IF NOT EXISTS mutation_catalog.mutation_db.data (id BIGINT, value STRING)",
    )
    .expect("table IF NOT EXISTS must be a provider NoOp");
    for range in ["1, 100000", "100001, 200000", "200001, 300000"] {
        conn.query_drop(format!(
            "INSERT INTO mutation_catalog.mutation_db.data \
             SELECT generate_series, CAST(generate_series AS STRING) \
             FROM TABLE(generate_series({range}))"
        ))
        .expect("write an independent Iceberg data file");
    }
    conn.query_drop("ALTER TABLE mutation_catalog.mutation_db.data ADD COLUMN category STRING DEFAULT 'catalog-mutation'")
        .expect("apply schema mutation");
    conn.query_drop(
        "ALTER TABLE mutation_catalog.mutation_db.data ADD PARTITION COLUMN bucket(id, 16)",
    )
    .expect("apply partition-spec mutation");
    conn.query_drop(
        "ALTER TABLE mutation_catalog.mutation_db.data SET TBLPROPERTIES ('spi-4b' = 'enabled')",
    )
    .expect("apply properties mutation");
    conn.query_drop("ALTER TABLE mutation_catalog.mutation_db.data CREATE BRANCH verify")
        .expect("create Iceberg branch through mutation SPI");
    conn.query_drop("ALTER TABLE mutation_catalog.mutation_db.data DROP BRANCH verify")
        .expect("drop Iceberg branch through mutation SPI");

    for be in &mut cluster.bes {
        while be.stdout_rx.try_recv().is_ok() {}
    }
    let rows: Vec<Row> = conn
        .query(
            "SELECT count(*), min(id), max(id), min(category) \
             FROM mutation_catalog.mutation_db.data",
        )
        .expect("read post-mutation Iceberg table through the distributed connector");
    assert_eq!(rows.len(), 1, "aggregate must return one row");
    let row = &rows[0];
    assert_eq!(row.get::<Option<i64>, usize>(0).flatten(), Some(300_000));
    assert_eq!(row.get::<Option<i64>, usize>(1).flatten(), Some(1));
    assert_eq!(row.get::<Option<i64>, usize>(2).flatten(), Some(300_000));
    assert_eq!(
        row.get::<Option<String>, usize>(3).flatten().as_deref(),
        Some("catalog-mutation")
    );
    let reader_output = cluster.wait_for_every_be_output_contains(
        "NOVAROCKS_CONNECTOR_UNIT_READER_OPEN provider=iceberg",
        Duration::from_secs(10),
    );
    for lines in &reader_output {
        assert!(
            lines
                .iter()
                .any(|line| line.contains("NOVAROCKS_CONNECTOR_READ_SOURCE")),
            "each BE must receive a real opaque Iceberg read source: {lines:?}"
        );
    }

    conn.query_drop("DROP TABLE IF EXISTS mutation_catalog.mutation_db.data")
        .expect("drop table through mutation SPI");
    conn.query_drop("DROP DATABASE IF EXISTS mutation_catalog.mutation_db")
        .expect("drop namespace through mutation SPI");
}

#[test]
fn cross_process_three_be_connector_static_predicate_prunes_files_and_row_groups() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _guard = lock_cluster_mvp();
    let metadata_dir =
        tempfile::tempdir_in(runtime_dir()).expect("create static-pruning metadata directory");
    let metadata_config = format!(
        r#"
[metadata]
provider = "sqlite"
path = "{}"
"#,
        metadata_dir.path().join("catalog.db").display()
    );
    let mut cluster = MultiBeClusterHarness::start_n_be(
        3,
        r#"
[debug]
emit_connector_reader_marker = true
"#,
        &metadata_config,
    );
    let warehouse = tempfile::tempdir_in(runtime_dir()).expect("create static-pruning warehouse");
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop(format!(
        r#"CREATE EXTERNAL CATALOG static_pruning_catalog PROPERTIES("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
        warehouse.path().display()
    ))
    .expect("create static-pruning catalog");
    conn.query_drop("CREATE DATABASE static_pruning_catalog.static_pruning")
        .expect("create static-pruning database");
    conn.query_drop("CREATE TABLE static_pruning_catalog.static_pruning.data (id BIGINT)")
        .expect("create static-pruning table");

    // Keep six non-overlapping data files. `id >= 175001` eliminates the first
    // three files, then prunes row groups in the first retained file while
    // retaining exactly one opaque split for every BE.
    for range in [
        "1, 50000",
        "50001, 100000",
        "100001, 150000",
        "150001, 200000",
        "200001, 250000",
        "250001, 300000",
    ] {
        conn.query_drop(format!(
            "INSERT INTO static_pruning_catalog.static_pruning.data \
             SELECT generate_series FROM TABLE(generate_series({range}))"
        ))
        .expect("write a distinct static-pruning data-file range");
    }

    let query = "SELECT count(*), min(id), max(id) \
                 FROM static_pruning_catalog.static_pruning.data WHERE id >= 175001";
    let prepared_unit_counts = |output: &[Vec<String>]| {
        output
            .iter()
            .map(|lines| {
                lines
                    .iter()
                    .find(|line| line.contains("NOVAROCKS_CONNECTOR_UNIT_SET_PREPARED"))
                    .and_then(|line| line.split("unit_count=").nth(1))
                    .and_then(|value| value.split_whitespace().next())
                    .and_then(|value| value.parse::<usize>().ok())
                    .expect("each BE prepared-unit marker must include a numeric unit count")
            })
            .collect::<Vec<_>>()
    };

    conn.query_drop("SET enable_connector_static_predicate_pushdown = false")
        .expect("disable static connector predicate pushdown");
    for be in &mut cluster.bes {
        while be.stdout_rx.try_recv().is_ok() {}
    }
    let disabled_rows: Vec<Row> = conn
        .query(query)
        .expect("disabled static pruning query must succeed");
    let disabled_output = cluster.wait_for_every_be_output_contains(
        "NOVAROCKS_CONNECTOR_UNIT_SET_PREPARED",
        Duration::from_secs(15),
    );
    let disabled_units = prepared_unit_counts(&disabled_output);
    assert!(
        disabled_units.iter().all(|count| *count > 0),
        "disabled path must still distribute real connector reads: {disabled_output:?}"
    );

    conn.query_drop("SET enable_connector_static_predicate_pushdown = true")
        .expect("enable static connector predicate pushdown");
    for be in &mut cluster.bes {
        while be.stdout_rx.try_recv().is_ok() {}
    }
    let enabled_rows: Vec<Row> = conn
        .query(query)
        .expect("enabled static pruning query must succeed");
    let enabled_output = cluster.wait_for_every_be_output_contains(
        "NOVAROCKS_CONNECTOR_UNIT_SET_PREPARED",
        Duration::from_secs(15),
    );
    let enabled_units = prepared_unit_counts(&enabled_output);
    assert_eq!(
        enabled_rows, disabled_rows,
        "the production rollback setting must preserve query results"
    );
    assert!(
        enabled_units.iter().all(|count| *count > 0),
        "enabled path must retain a non-empty prepared-unit set on every BE: {enabled_output:?}"
    );
    assert!(
        enabled_units.iter().sum::<usize>() < disabled_units.iter().sum::<usize>(),
        "PruningOnly must reduce sealed connector scan units without removing the core residual: disabled={disabled_units:?}, enabled={enabled_units:?}"
    );

    let profile: Vec<String> = conn
        .query(format!("EXPLAIN ANALYZE {query}"))
        .expect("EXPLAIN ANALYZE must render connector static-pruning evidence");
    let profile = profile.join("\n");
    assert!(
        profile.contains("ConnectorStaticPlanning:") && profile.contains("candidate_units_pruned="),
        "profile must include provider-neutral connector pruning metrics: {profile}"
    );
    assert!(
        profile.contains("ScanConjunctApply:"),
        "profile must show the core residual conjunct remains active for PruningOnly: {profile}"
    );
    let scan_conjunct_counter = |name: &str| {
        profile
            .lines()
            .find(|line| line.starts_with("ScanConjunctApply:"))
            .and_then(|line| {
                line.split_whitespace().find_map(|part| {
                    part.strip_prefix(name)
                        .and_then(|value| value.parse::<u64>().ok())
                })
            })
            .expect("scan-conjunct profile entry must include the requested counter")
    };
    let input_rows = scan_conjunct_counter("input_rows=");
    let output_rows = scan_conjunct_counter("output_rows=");
    assert!(
        profile.contains("ConnectorFileMetrics:")
            && profile.contains("ConnectorFileRowGroupsRead="),
        "profile must report connector row-group counters: {profile}"
    );
    assert!(
        input_rows > output_rows && input_rows - output_rows < 4_096,
        "row-group pruning must leave only the boundary group's non-matching rows for the core residual: input={input_rows}, output={output_rows}, profile={profile}"
    );
}

#[test]
fn cross_process_three_be_connector_read_applies_deletion_vectors() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _guard = lock_cluster_mvp();
    let fixture_dir = tempfile::tempdir_in(runtime_dir()).expect("create DV fixture directory");
    let mut cluster =
        MultiBeClusterHarness::start_three_be_sqlite_state_store_with_metadata_and_be_extra(
            &fixture_dir.path().join("frontend-state.sqlite"),
            &fixture_dir.path().join("frontend-metadata.sqlite"),
            "connector-deletion-vectors",
            r#"
[debug]
emit_connector_reader_marker = true
"#,
        );
    let warehouse = tempfile::tempdir_in(runtime_dir()).expect("create DV warehouse");
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop(format!(
        r#"CREATE EXTERNAL CATALOG dv_catalog PROPERTIES("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
        warehouse.path().display()
    ))
    .expect("create DV catalog");
    conn.query_drop("CREATE DATABASE dv_catalog.dv_db")
        .expect("create DV database");
    conn.query_drop(
        "CREATE TABLE dv_catalog.dv_db.data (id BIGINT) \
         TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
    )
    .expect("create Iceberg v3 DV table");
    for range in ["1, 100000", "100001, 200000", "200001, 300000"] {
        conn.query_drop(format!(
            "INSERT INTO dv_catalog.dv_db.data \
             SELECT generate_series FROM TABLE(generate_series({range}))"
        ))
        .expect("write a distinct Iceberg data-file range");
    }
    conn.query_drop("DELETE FROM dv_catalog.dv_db.data WHERE id IN (1, 100001, 200001)")
        .expect("write Iceberg v3 deletion vectors");
    for be in &mut cluster.bes {
        while be.stdout_rx.try_recv().is_ok() {}
    }

    let rows: Vec<Row> = conn
        .query(
            "SELECT count(*), sum(id), min(id), max(id) \
             FROM dv_catalog.dv_db.data",
        )
        .expect("read the v3 table through distributed connector reads");
    assert_eq!(rows.len(), 1, "aggregate must produce one row");
    let row = &rows[0];
    assert_eq!(row.get::<Option<i64>, usize>(0).flatten(), Some(299_997));
    assert_eq!(
        row.get::<Option<i64>, usize>(1).flatten(),
        Some(44_999_849_997)
    );
    assert_eq!(row.get::<Option<i64>, usize>(2).flatten(), Some(2));
    assert_eq!(row.get::<Option<i64>, usize>(3).flatten(), Some(300_000));

    let reader_output = cluster.wait_for_every_be_output_contains(
        "NOVAROCKS_CONNECTOR_UNIT_SET_PREPARED",
        Duration::from_secs(10),
    );
    for lines in &reader_output {
        assert!(
            lines
                .iter()
                .any(|line| line.contains("facts_conservative_units=")
                    && !line.contains("facts_conservative_units=0")),
            "Iceberg deletion-vector reads must expose conservative prepared facts: {lines:?}"
        );
        let source = lines
            .iter()
            .find(|line| line.contains("NOVAROCKS_CONNECTOR_READ_SOURCE"))
            .expect("each BE must decode an opaque connector read source");
        let split_count = source
            .split("splits=")
            .nth(1)
            .and_then(|value| value.parse::<usize>().ok())
            .expect("connector source marker must include a numeric split count");
        assert!(
            split_count > 0,
            "each BE must receive at least one opaque Iceberg split: {source}"
        );
    }
}

#[test]
fn cross_process_three_be_connector_generation_replacement_drains_old_readers() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _guard = lock_cluster_mvp();
    let fixture_dir =
        tempfile::tempdir_in(runtime_dir()).expect("create generation fixture directory");
    let mut cluster =
        MultiBeClusterHarness::start_three_be_sqlite_state_store_with_metadata_and_be_extra(
            &fixture_dir.path().join("frontend-state.sqlite"),
            &fixture_dir.path().join("frontend-metadata.sqlite"),
            "connector-generation",
            r#"
[debug]
emit_connector_reader_marker = true

[runtime]
operator_buffer_chunks = 1
"#,
        );
    let warehouse = tempfile::tempdir_in(runtime_dir()).expect("create generation warehouse");
    let mut control = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut control, 3);
    let create_catalog = || {
        format!(
            r#"CREATE EXTERNAL CATALOG generation_catalog PROPERTIES("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        )
    };
    control
        .query_drop(create_catalog())
        .expect("create first generation catalog");
    control
        .query_drop("CREATE DATABASE generation_catalog.generation_db")
        .expect("create generation database");
    control
        .query_drop("CREATE TABLE generation_catalog.generation_db.data (v BIGINT)")
        .expect("create generation table");
    for range in ["1, 100000", "100001, 200000", "200001, 300000"] {
        control
            .query_drop(format!(
                "INSERT INTO generation_catalog.generation_db.data \
                 SELECT generate_series FROM TABLE(generate_series({range}))"
            ))
            .expect("write generation data file");
    }
    for be in &mut cluster.bes {
        while be.stdout_rx.try_recv().is_ok() {}
    }

    let (target_ready_tx, target_ready_rx) = mpsc::sync_channel(1);
    let (target_done_tx, target_done_rx) = mpsc::sync_channel(1);
    let (target_release_tx, target_release_rx) = mpsc::sync_channel(0);
    let fe_mysql = cluster.fe_mysql_port();
    let target = std::thread::spawn(move || {
        let mut conn = connect_mysql(fe_mysql);
        target_ready_tx
            .send(conn.connection_id())
            .expect("publish old-generation target connection id");
        let result = conn.query::<i64, _>(
            "SELECT t.s FROM (SELECT sleep(10) AS s FROM generation_catalog.generation_db.data) AS t \
             CROSS JOIN TABLE(generate_series(1, 1000000000)) AS gs(x)",
        );
        target_done_tx
            .send(result)
            .expect("publish old-generation target result");
        target_release_rx
            .recv()
            .expect("release old-generation target connection");
    });
    let target_connection_id = target_ready_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("old-generation target connection id");
    let old_output = cluster.wait_for_every_be_output_contains(
        "NOVAROCKS_CONNECTOR_UNIT_READER_OPEN provider=iceberg instance=generation_catalog",
        Duration::from_secs(10),
    );
    let old_incarnations = old_output
        .iter()
        .map(|lines| {
            lines
                .iter()
                .find_map(|line| line.split("incarnation=").nth(1))
                .expect("old reader marker must identify its execution incarnation")
                .to_owned()
        })
        .collect::<Vec<_>>();
    let old_reader_counts = old_output
        .iter()
        .map(|lines| {
            let opens = lines
                .iter()
                .filter(|line| line.contains("NOVAROCKS_CONNECTOR_UNIT_READER_OPEN"))
                .count();
            let closes = lines
                .iter()
                .filter(|line| line.contains("NOVAROCKS_CONNECTOR_UNIT_READER_CLOSE"))
                .count();
            (opens, closes)
        })
        .collect::<Vec<_>>();
    assert!(
        old_reader_counts
            .iter()
            .all(|(opens, closes)| *opens > *closes),
        "each BE must retain an old-generation reader while the replacement is published: counts={old_reader_counts:?}; stdout={old_output:?}"
    );
    assert!(
        target_done_rx.try_recv().is_err(),
        "old-generation query must still hold readers while replacement is published"
    );

    control
        .query_drop("DROP CATALOG generation_catalog")
        .expect("retire first catalog generation while its readers drain");
    control
        .query_drop(create_catalog())
        .expect("create replacement catalog generation");
    let rows: Vec<i64> = control
        .query("SELECT count(*) FROM generation_catalog.generation_db.data")
        .expect("new generation must read existing Iceberg data");
    assert_eq!(rows, vec![300_000]);
    let new_output = cluster.wait_for_every_be_output_contains(
        "NOVAROCKS_CONNECTOR_UNIT_READER_OPEN provider=iceberg instance=generation_catalog",
        Duration::from_secs(10),
    );
    for (old, lines) in old_incarnations.iter().zip(&new_output) {
        let observed_replacement = lines
            .iter()
            .filter_map(|line| line.split("incarnation=").nth(1))
            .any(|incarnation| incarnation != old);
        assert!(
            observed_replacement,
            "each BE must resolve Q2 through the replacement execution generation: {lines:?}"
        );
    }

    control
        .query_drop(format!("KILL QUERY {target_connection_id}"))
        .expect("terminate old-generation query after replacement read");
    let target_error = target_done_rx
        .recv_timeout(Duration::from_secs(15))
        .expect("old-generation query must terminate")
        .expect_err("old-generation query must not succeed after KILL QUERY");
    assert_mysql_server_error(target_error, 1317);
    target_release_tx
        .send(())
        .expect("release old-generation target connection");
    target
        .join()
        .expect("old-generation target thread must join");
}

#[test]
fn be_kill9_during_query_fails_cleanly() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _lock = lock_cluster_mvp();

    let cluster = ClusterHarness::start(
        r#"
[debug]
fault_inject_fetch_not_ready_count = 1000
"#,
        "",
    );

    let (tx, rx) = mpsc::channel();
    let fe_mysql = cluster.fe_mysql;
    std::thread::spawn(move || {
        let mut conn = connect_mysql(fe_mysql);
        let result = conn.query::<String, _>(disconnect_blocking_query_sql());
        tx.send(result.map_err(|err| err.to_string()))
            .expect("send query result");
    });

    std::thread::sleep(Duration::from_millis(300));
    drop(cluster.be);

    let result = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("query should finish after BE dies");
    let err = result.expect_err("query should fail once BE is killed");
    assert!(
        !err.is_empty(),
        "expected a non-empty FE error after BE crash"
    );
}

#[test]
fn cross_process_two_be_coordinated_query() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _guard = lock_cluster_mvp();
    let cluster = MultiBeClusterHarness::start_n_be(2, "", "");
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 2);
    let rows: Vec<i64> = conn
        .query(coordinated_query_sql())
        .expect("coordinated query must succeed on 2-BE cluster");
    assert_eq!(
        rows,
        vec![1i64, 2i64],
        "2-BE coordinated query must return sorted results [1, 2]"
    );
}

#[test]
fn cross_process_two_be_multi_fragment() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _guard = lock_cluster_mvp();
    let cluster = MultiBeClusterHarness::start_n_be(2, "", "");
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 2);
    let rows: Vec<i64> = conn
        .query(multi_submit_query_sql())
        .expect("multi-fragment CTE+JOIN query must succeed on 2-BE cluster");
    assert_eq!(
        rows,
        vec![1i64, 2i64],
        "2-BE multi-fragment query must return sorted results [1, 2]"
    );
}

#[test]
fn cross_process_three_be_state_store_baseline() {
    let _guard = lock_cluster_mvp();
    let cluster = MultiBeClusterHarness::start_n_be(3, "", "");
    eprintln!("NOVAROCKS_CLUSTER_BASELINE_READY fe=1 be=3");
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    let rows: Vec<i64> = conn
        .query(multi_submit_query_sql())
        .expect("multi-fragment CTE+JOIN query must succeed on 3-BE cluster");
    assert_eq!(
        rows,
        vec![1i64, 2i64],
        "3-BE multi-fragment query must return sorted results [1, 2]"
    );
    let backend_rows = show_backends(&mut conn);
    let scheduled_fragments: u64 = backend_rows
        .iter()
        .filter(|row| row.get::<String, usize>(3).as_deref() == Some("Live"))
        .map(|row| {
            let value = row.get::<String, usize>(4).unwrap_or_else(|| {
                panic!("Live backend must expose ScheduledFragments; rows={backend_rows:?}")
            });
            value.parse::<u64>().unwrap_or_else(|err| {
                panic!(
                    "Live backend ScheduledFragments must be an unsigned integer ({value:?}): {err}; rows={backend_rows:?}"
                )
            })
        })
        .sum();
    assert!(
        scheduled_fragments > 0,
        "3 Live backends must report scheduled fragments after the multi-fragment query; rows={backend_rows:?}"
    );
    eprintln!("NOVAROCKS_CLUSTER_BASELINE_RESULT fragments=multi rows=[1,2]");
}

#[test]
fn cross_process_three_be_statistics_service() {
    let _guard = lock_cluster_mvp();
    let fixture_dir =
        tempfile::tempdir_in(runtime_dir()).expect("create statistics fixture directory");
    let mut cluster = MultiBeClusterHarness::start_three_be_sqlite_state_store_with_metadata(
        &fixture_dir.path().join("frontend-state.sqlite"),
        &fixture_dir.path().join("frontend-metadata.sqlite"),
        "statistics-service",
    );
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);

    let warehouse = tempfile::tempdir_in(runtime_dir()).expect("create statistics warehouse");
    conn.query_drop(format!(
        r#"CREATE EXTERNAL CATALOG feh5_stats_catalog PROPERTIES("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
        warehouse.path().display()
    ))
    .expect("create statistics catalog");
    conn.query_drop("SET catalog feh5_stats_catalog")
        .expect("use statistics catalog");
    conn.query_drop("CREATE DATABASE feh5_stats")
        .expect("create statistics database");
    conn.query_drop("CREATE TABLE feh5_stats.t (k INT)")
        .expect("create statistics table");
    for value in 1..=3 {
        conn.query_drop(format!("INSERT INTO feh5_stats.t VALUES ({value})"))
            .expect("insert one statistics data file");
    }
    let before_analyze: Vec<String> = conn
        .query("EXPLAIN COSTS SELECT * FROM feh5_stats.t")
        .expect("explain before statistics publication");
    assert!(
        before_analyze
            .iter()
            .any(|line| line.contains("source=IcebergManifest")),
        "append-only manifest evidence must be available before ANALYZE: {before_analyze:?}"
    );
    conn.query_drop("ANALYZE TABLE feh5_stats.t")
        .expect("analyze statistics table");

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let jobs: Vec<Row> = conn.query("SHOW ANALYZE JOBS").expect("show analyze jobs");
        let states = jobs
            .iter()
            .filter_map(|row| row.get::<String, _>(2))
            .collect::<Vec<_>>();
        if states.iter().any(|state| state == "SUCCEEDED") {
            break;
        }
        assert!(
            !states
                .iter()
                .any(|state| state == "FAILED" || state == "CANCELLED"),
            "ANALYZE must not reach a failed terminal state: {jobs:?}"
        );
        assert!(
            Instant::now() < deadline,
            "timed out waiting for durable ANALYZE completion: {jobs:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    cluster.wait_for_every_be_output_contains(
        "NOVAROCKS_STATISTICS_FRAGMENT_COLLECTED",
        Duration::from_secs(10),
    );

    let stats: Vec<(String, Option<String>, String)> = conn
        .query("SHOW TABLE STATS feh5_stats.t")
        .expect("show collected statistics");
    assert!(
        stats.iter().any(|(metric, value, status)| {
            metric == "row_count" && value.as_deref() == Some("3") && status == "AVAILABLE"
        }),
        "SHOW TABLE STATS must expose the collected row count: {stats:?}"
    );
    assert!(
        stats.iter().any(|(metric, value, status)| {
            metric == "theta_ndv:k" && value.as_deref() == Some("3") && status == "AVAILABLE"
        }),
        "SHOW TABLE STATS must expose the collected Theta NDV: {stats:?}"
    );

    let explain: Vec<String> = conn
        .query("EXPLAIN COSTS SELECT * FROM feh5_stats.t")
        .expect("explain with collected statistics");
    assert!(
        explain.iter().any(|line| line.contains("TABLE STATS")
            && line.contains("rows=3")
            && line.contains("source=IcebergPuffin")),
        "EXPLAIN COSTS must consume the published full table evidence: {explain:?}"
    );
    assert!(
        explain.iter().any(|line| line.contains("ndv=?")),
        "Theta NDV is approximate and must not be exposed as an exact optimizer cardinality: {explain:?}"
    );
    assert_exact_live_backends(&mut conn, 3);
}

#[cfg(unix)]
#[test]
fn cross_process_three_be_frontend_insert_service_lifecycle() {
    let _guard = lock_cluster_mvp();
    let fixture_dir =
        tempfile::tempdir_in(runtime_dir()).expect("create INSERT lifecycle fixture directory");
    let state_store_path = fixture_dir.path().join("frontend-state.sqlite");
    let metadata_path = fixture_dir.path().join("frontend-metadata.sqlite");
    let warehouse = tempfile::tempdir_in(runtime_dir()).expect("create INSERT lifecycle warehouse");
    let mut cluster = MultiBeClusterHarness::start_three_be_sqlite_state_store_with_metadata(
        &state_store_path,
        &metadata_path,
        "frontend-insert-lifecycle",
    );

    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop(format!(
        r#"CREATE EXTERNAL CATALOG insert_lifecycle_ice PROPERTIES("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
        warehouse.path().display()
    ))
    .expect("create INSERT lifecycle catalog");
    conn.query_drop("CREATE DATABASE insert_lifecycle_ice.ns")
        .expect("create INSERT lifecycle namespace");
    conn.query_drop(
        "CREATE TABLE insert_lifecycle_ice.ns.orders (id INT, amount INT) \
         TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
    )
    .expect("create INSERT lifecycle table");
    let scheduled_before = scheduled_fragments(&mut conn);

    conn.query_drop("INSERT INTO insert_lifecycle_ice.ns.orders VALUES (1, 10), (2, 20)")
        .expect("execute INSERT VALUES through frontend DML service");
    conn.query_drop(
        "INSERT INTO insert_lifecycle_ice.ns.orders \
         SELECT id + 2, amount + 20 FROM insert_lifecycle_ice.ns.orders",
    )
    .expect("execute INSERT SELECT through frontend DML service");
    let appended: Vec<(i32, i32)> = conn
        .query("SELECT id, amount FROM insert_lifecycle_ice.ns.orders ORDER BY id")
        .expect("read appended INSERT lifecycle rows");
    assert_eq!(appended, vec![(1, 10), (2, 20), (3, 30), (4, 40)]);

    conn.query_drop("INSERT OVERWRITE insert_lifecycle_ice.ns.orders VALUES (10, 100), (20, 200)")
        .expect("execute full INSERT OVERWRITE through frontend DML service");
    let snapshots_before_empty: Vec<i64> = conn
        .query(
            "SELECT count(*) \
             FROM insert_lifecycle_ice.ns.orders$snapshots",
        )
        .expect("count snapshots before empty overwrite");
    conn.query_drop(
        "INSERT OVERWRITE insert_lifecycle_ice.ns.orders \
         SELECT id, amount FROM insert_lifecycle_ice.ns.orders WHERE 1 = 0",
    )
    .expect("execute empty INSERT OVERWRITE through frontend DML service");
    let snapshots_after_empty: Vec<i64> = conn
        .query(
            "SELECT count(*) \
             FROM insert_lifecycle_ice.ns.orders$snapshots",
        )
        .expect("count snapshots after empty overwrite");
    assert_eq!(snapshots_before_empty.len(), 1);
    assert_eq!(snapshots_after_empty.len(), 1);
    assert_eq!(
        snapshots_after_empty[0],
        snapshots_before_empty[0] + 1,
        "empty full overwrite must commit a replacement snapshot"
    );
    let final_rows: Vec<(i32, i32)> = conn
        .query("SELECT id, amount FROM insert_lifecycle_ice.ns.orders ORDER BY id")
        .expect("read final INSERT lifecycle rows");
    assert!(
        final_rows.is_empty(),
        "empty full overwrite must replace all visible rows"
    );
    assert_exact_live_backends(&mut conn, 3);
    let scheduled_after = scheduled_fragments(&mut conn);
    assert!(
        scheduled_after > scheduled_before,
        "frontend INSERT lifecycle must schedule remote fragments: \
         before={scheduled_before}, after={scheduled_after}"
    );

    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));
    assert!(state_store_path.is_file(), "DML StateStore must persist");
    assert!(metadata_path.is_file(), "legacy metadata must persist");
    cluster.restart_fe();

    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    let restored_rows: Vec<(i32, i32)> = conn
        .query("SELECT id, amount FROM insert_lifecycle_ice.ns.orders ORDER BY id")
        .expect("read INSERT lifecycle table after FE restart");
    assert!(
        restored_rows.is_empty(),
        "empty overwrite result must survive FE restart"
    );
    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build DML StateStore inspection runtime");
    let host = runtime
        .block_on(FrontendApplicationHost::open(
            Some(sqlite_state_store_config(
                &state_store_path,
                "frontend-insert-lifecycle",
            )),
            frontend_execution_config(),
            ClusterBackendOpenConfig::new(
                novarocks::common::app_config::ClusterRole::AllInOne,
                Vec::new(),
                Duration::from_secs(1),
                1,
                Duration::from_secs(1),
            )
            .expect("valid DML StateStore inspection backend config"),
        ))
        .expect("reopen DML StateStore after clean FE shutdown");
    let dml = host.dml_service();
    let operations = dml
        .list_operations()
        .expect("list durable INSERT operations");
    assert_eq!(
        operations.len(),
        4,
        "VALUES, SELECT, full overwrite, and empty overwrite must each be journaled"
    );
    assert!(
        operations
            .iter()
            .all(|operation| operation.state == OperationState::Finalized),
        "every successful INSERT operation must be terminal: {operations:?}"
    );
    assert_eq!(
        operations
            .iter()
            .filter(|operation| operation.operation_kind == OperationKind::InsertAppend)
            .count(),
        2
    );
    assert_eq!(
        operations
            .iter()
            .filter(|operation| operation.operation_kind == OperationKind::InsertOverwrite)
            .count(),
        2
    );
    assert!(
        operations.iter().all(|operation| {
            operation.target.catalog == "insert_lifecycle_ice"
                && operation.target.namespace == "ns"
                && operation.target.table == "orders"
        }),
        "durable operations must preserve the INSERT target: {operations:?}"
    );
    assert!(
        dml.list_unfinished_operations()
            .expect("list unfinished INSERT operations")
            .is_empty(),
        "successful INSERT lifecycle must leave no recovery work"
    );
    drop(dml);
    runtime
        .block_on(host.shutdown())
        .expect("inspection host shutdown");
}

#[cfg(unix)]
#[test]
fn cross_process_three_be_frontend_delete_service_lifecycle() {
    let _guard = lock_cluster_mvp();
    let fixture_dir = tempfile::tempdir_in(runtime_dir()).expect("create DELETE lifecycle fixture");
    let state_store_path = fixture_dir.path().join("frontend-state.sqlite");
    let metadata_path = fixture_dir.path().join("frontend-metadata.sqlite");
    let warehouse = tempfile::tempdir_in(runtime_dir()).expect("create DELETE lifecycle warehouse");
    let mut cluster = MultiBeClusterHarness::start_three_be_sqlite_state_store_with_metadata(
        &state_store_path,
        &metadata_path,
        "frontend-delete-lifecycle",
    );

    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop(format!(
        r#"CREATE EXTERNAL CATALOG delete_lifecycle_ice PROPERTIES("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
        warehouse.path().display()
    )).expect("create DELETE lifecycle catalog");
    conn.query_drop("CREATE DATABASE delete_lifecycle_ice.ns")
        .expect("create namespace");
    conn.query_drop(
        "CREATE TABLE delete_lifecycle_ice.ns.orders (id INT, amount INT) \
         TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
    )
    .expect("create v3 row-lineage table");
    conn.query_drop("INSERT INTO delete_lifecycle_ice.ns.orders VALUES (1, 10), (2, 20), (3, 30)")
        .expect("seed DELETE lifecycle rows");
    let scheduled_before = scheduled_fragments(&mut conn);

    conn.query_drop("DELETE FROM delete_lifecycle_ice.ns.orders WHERE id = 1")
        .expect("execute standard DELETE through frontend DML service");
    let scheduled_after_standard = scheduled_fragments(&mut conn);
    assert!(
        scheduled_after_standard > scheduled_before,
        "standard DELETE must schedule fragments"
    );

    conn.query_drop(
        "ALTER TABLE delete_lifecycle_ice.ns.orders \
         ADD EQUALITY DELETE (id) VALUES (2)",
    )
    .expect("execute equality DELETE through frontend DML service");
    let scheduled_after_equality = scheduled_fragments(&mut conn);
    assert!(
        scheduled_after_equality > scheduled_after_standard,
        "equality DELETE must schedule fragments"
    );

    let snapshots_before_noop: Vec<i64> = conn
        .query("SELECT count(*) FROM delete_lifecycle_ice.ns.orders$snapshots")
        .expect("count snapshots before no-op DELETE");
    conn.query_drop("DELETE FROM delete_lifecycle_ice.ns.orders WHERE id = 999")
        .expect("execute no-op DELETE");
    let snapshots_after_noop: Vec<i64> = conn
        .query("SELECT count(*) FROM delete_lifecycle_ice.ns.orders$snapshots")
        .expect("count snapshots after no-op DELETE");
    assert_eq!(
        snapshots_after_noop, snapshots_before_noop,
        "no-match DELETE must not commit a snapshot"
    );
    let rows: Vec<(i32, i32)> = conn
        .query("SELECT id, amount FROM delete_lifecycle_ice.ns.orders ORDER BY id")
        .expect("read remaining DELETE lifecycle rows");
    assert_eq!(rows, vec![(3, 30)]);

    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));
    cluster.restart_fe();
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    let restored: Vec<(i32, i32)> = conn
        .query("SELECT id, amount FROM delete_lifecycle_ice.ns.orders ORDER BY id")
        .expect("read DELETE lifecycle table after FE restart");
    assert_eq!(restored, vec![(3, 30)]);
    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build StateStore inspection runtime");
    let host = runtime
        .block_on(FrontendApplicationHost::open(
            Some(sqlite_state_store_config(
                &state_store_path,
                "frontend-delete-lifecycle",
            )),
            frontend_execution_config(),
            ClusterBackendOpenConfig::new(
                novarocks::common::app_config::ClusterRole::AllInOne,
                Vec::new(),
                Duration::from_secs(1),
                1,
                Duration::from_secs(1),
            )
            .expect("valid StateStore inspection backend config"),
        ))
        .expect("reopen DML StateStore");
    let operations = host
        .dml_service()
        .list_operations()
        .expect("list durable DELETE operations");
    let row_deltas = operations
        .iter()
        .filter(|operation| operation.operation_kind == OperationKind::RowDelta)
        .collect::<Vec<_>>();
    assert_eq!(
        row_deltas.len(),
        3,
        "standard, equality, and no-op DELETE must be journaled"
    );
    assert_eq!(
        row_deltas
            .iter()
            .filter(|operation| operation.state == OperationState::Finalized)
            .count(),
        2
    );
    assert_eq!(
        row_deltas
            .iter()
            .filter(|operation| operation.state == OperationState::Aborted)
            .count(),
        1
    );
    runtime
        .block_on(host.shutdown())
        .expect("inspection host shutdown");
}

#[cfg(unix)]
#[test]
fn cross_process_three_be_frontend_ctas_truncate_lifecycle() {
    let Ok(rest_uri) = std::env::var("NOVAROCKS_ICEBERG_REST_URI") else {
        eprintln!(
            "SKIP cross_process_three_be_frontend_ctas_truncate_lifecycle: \
             NOVAROCKS_ICEBERG_REST_URI is not configured"
        );
        return;
    };
    let rest_warehouse = std::env::var("NOVAROCKS_ICEBERG_REST_WAREHOUSE")
        .expect("REST lifecycle acceptance requires NOVAROCKS_ICEBERG_REST_WAREHOUSE");
    let s3_endpoint = std::env::var("AWS_S3_ENDPOINT")
        .expect("REST lifecycle acceptance requires AWS_S3_ENDPOINT");
    let s3_access_key = std::env::var("AWS_S3_ACCESS_KEY_ID")
        .expect("REST lifecycle acceptance requires AWS_S3_ACCESS_KEY_ID");
    let s3_secret_key = std::env::var("AWS_S3_SECRET_ACCESS_KEY")
        .expect("REST lifecycle acceptance requires AWS_S3_SECRET_ACCESS_KEY");

    let _guard = lock_cluster_mvp();
    let fixture_dir = tempfile::tempdir_in(runtime_dir())
        .expect("create CTAS/TRUNCATE lifecycle fixture directory");
    let state_store_path = fixture_dir.path().join("frontend-state.sqlite");
    let metadata_path = fixture_dir.path().join("frontend-metadata.sqlite");
    let namespace = format!("dml3_cluster_{}", std::process::id());
    let catalog = "ctas_truncate_lifecycle_ice";
    let cluster_id = "frontend-ctas-truncate-lifecycle";
    let be_object_store = format!(
        r#"
[connector.object_store]
endpoint = "{s3_endpoint}"
access_key_id = "{s3_access_key}"
access_key_secret = "{s3_secret_key}"
enable_path_style_access = true
"#
    );
    let mut cluster =
        MultiBeClusterHarness::start_three_be_sqlite_state_store_with_metadata_and_be_extra(
            &state_store_path,
            &metadata_path,
            cluster_id,
            &be_object_store,
        );

    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop(format!(
        r#"CREATE EXTERNAL CATALOG {catalog} PROPERTIES(
            "type"="iceberg",
            "iceberg.catalog.type"="rest",
            "uri"="{rest_uri}",
            "warehouse"="{rest_warehouse}",
            "aws.s3.endpoint"="{s3_endpoint}",
            "aws.s3.access_key"="{s3_access_key}",
            "aws.s3.secret_key"="{s3_secret_key}",
            "aws.s3.region"="us-east-1",
            "aws.s3.enable_path_style_access"="true")"#,
    ))
    .expect("create REST CTAS/TRUNCATE lifecycle catalog");
    conn.query_drop(format!(
        "DROP DATABASE IF EXISTS {catalog}.{namespace} FORCE"
    ))
    .expect("remove stale REST lifecycle namespace");
    conn.query_drop(format!("CREATE DATABASE {catalog}.{namespace}"))
        .expect("create REST lifecycle namespace");
    conn.query_drop(format!(
        "CREATE TABLE {catalog}.{namespace}.source_orders (id INT, amount INT) \
         TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")"
    ))
    .expect("create CTAS source table");
    conn.query_drop(format!(
        "INSERT INTO {catalog}.{namespace}.source_orders VALUES (1, 10), (2, 20), (3, 30)"
    ))
    .expect("seed CTAS source exactly once");

    let scheduled_before_ctas = scheduled_fragments(&mut conn);
    conn.query_drop(format!(
        "CREATE TABLE {catalog}.{namespace}.published_orders AS \
         SELECT id, amount FROM {catalog}.{namespace}.source_orders"
    ))
    .expect("execute REST staged-publication CTAS through frontend DML service");
    let scheduled_after_ctas = scheduled_fragments(&mut conn);
    assert!(
        scheduled_after_ctas > scheduled_before_ctas,
        "CTAS must schedule real remote fragments: before={scheduled_before_ctas}, \
         after={scheduled_after_ctas}"
    );
    let ctas_rows: Vec<(i32, i32)> = conn
        .query(format!(
            "SELECT id, amount FROM {catalog}.{namespace}.published_orders ORDER BY id"
        ))
        .expect("read atomically published CTAS table");
    assert_eq!(
        ctas_rows,
        vec![(1, 10), (2, 20), (3, 30)],
        "the admitted CTAS source must execute exactly once"
    );

    conn.query_drop(format!(
        "CREATE TABLE {catalog}.{namespace}.protected_orders (id INT) \
         TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")"
    ))
    .expect("create visible table protected from CTAS failure compensation");
    conn.query_drop(format!(
        "INSERT INTO {catalog}.{namespace}.protected_orders VALUES (99)"
    ))
    .expect("seed protected visible table");
    let conflict = conn
        .query_drop(format!(
            "CREATE TABLE {catalog}.{namespace}.protected_orders AS \
             SELECT id FROM {catalog}.{namespace}.source_orders"
        ))
        .expect_err("CTAS must reject an existing target without destructive compensation");
    assert!(
        conflict.to_string().contains("already exists"),
        "unexpected CTAS conflict error: {conflict}"
    );
    let protected_rows: Vec<i32> = conn
        .query(format!(
            "SELECT id FROM {catalog}.{namespace}.protected_orders ORDER BY id"
        ))
        .expect("read protected visible table after CTAS conflict");
    assert_eq!(
        protected_rows,
        vec![99],
        "CTAS failure must never drop the visible target"
    );

    let rows_before_truncate: Vec<i64> = conn
        .query(format!(
            "SELECT count(*) FROM {catalog}.{namespace}.published_orders"
        ))
        .expect("count CTAS rows before TRUNCATE");
    assert_eq!(rows_before_truncate, vec![3]);
    let snapshots_before_truncate: Vec<i64> = conn
        .query(format!(
            "SELECT count(*) FROM {catalog}.{namespace}.published_orders$snapshots"
        ))
        .expect("count snapshots before TRUNCATE");
    let scheduled_before_truncate = scheduled_fragments(&mut conn);
    conn.query_drop(format!(
        "TRUNCATE TABLE {catalog}.{namespace}.published_orders"
    ))
    .expect("execute frontend direct-mutation TRUNCATE");
    let scheduled_after_truncate = scheduled_fragments(&mut conn);
    assert_eq!(
        scheduled_after_truncate, scheduled_before_truncate,
        "TRUNCATE must not initialize or schedule backend fragments"
    );
    let rows_after_truncate: Vec<i64> = conn
        .query(format!(
            "SELECT count(*) FROM {catalog}.{namespace}.published_orders"
        ))
        .expect("count rows after TRUNCATE");
    assert_eq!(rows_after_truncate, vec![0]);
    let snapshots_after_truncate: Vec<i64> = conn
        .query(format!(
            "SELECT count(*) FROM {catalog}.{namespace}.published_orders$snapshots"
        ))
        .expect("count snapshots after TRUNCATE");
    assert_eq!(snapshots_before_truncate.len(), 1);
    assert_eq!(snapshots_after_truncate.len(), 1);
    assert_eq!(
        snapshots_after_truncate[0],
        snapshots_before_truncate[0] + 1,
        "TRUNCATE must commit exactly one audit snapshot"
    );

    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));
    assert!(state_store_path.is_file(), "DML StateStore must persist");
    assert!(metadata_path.is_file(), "frontend metadata must persist");
    cluster.restart_fe();

    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    let restored_rows: Vec<i64> = conn
        .query(format!(
            "SELECT count(*) FROM {catalog}.{namespace}.published_orders"
        ))
        .expect("read truncated CTAS table after FE restart");
    assert_eq!(restored_rows, vec![0]);
    let restored_snapshots: Vec<i64> = conn
        .query(format!(
            "SELECT count(*) FROM {catalog}.{namespace}.published_orders$snapshots"
        ))
        .expect("read TRUNCATE snapshot chain after FE restart");
    assert_eq!(restored_snapshots, snapshots_after_truncate);
    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build CTAS/TRUNCATE StateStore inspection runtime");
    let host = runtime
        .block_on(FrontendApplicationHost::open(
            Some(sqlite_state_store_config(&state_store_path, cluster_id)),
            frontend_execution_config(),
            ClusterBackendOpenConfig::new(
                novarocks::common::app_config::ClusterRole::AllInOne,
                Vec::new(),
                Duration::from_secs(1),
                1,
                Duration::from_secs(1),
            )
            .expect("valid CTAS/TRUNCATE inspection backend config"),
        ))
        .expect("reopen CTAS/TRUNCATE DML StateStore");
    let dml = host.dml_service();
    let operations = dml
        .list_operations()
        .expect("list durable CTAS/TRUNCATE operations");
    let committed_ctas = operations
        .iter()
        .find(|operation| {
            operation.operation_kind == OperationKind::CreateTableAsSelect
                && operation.target.table == "published_orders"
        })
        .expect("durable successful CTAS operation");
    assert_eq!(committed_ctas.state, OperationState::Finalized);
    let OperationPayload::CtasSaga(ctas) = &committed_ctas.payload else {
        panic!("successful CTAS must persist a CTAS saga payload")
    };
    assert_eq!(ctas.phase, CtasSagaPhase::Committed);
    assert_eq!(ctas.next_action, StatementNextAction::None);
    assert!(
        ctas.source_plan_digest
            .as_deref()
            .is_some_and(|v| !v.is_empty())
    );
    assert!(
        ctas.source_schema_digest
            .as_deref()
            .is_some_and(|v| !v.is_empty())
    );
    assert!(
        ctas.source_execution_identity
            .as_deref()
            .is_some_and(|v| !v.is_empty())
    );
    assert!(
        ctas.write_cohort_id
            .as_deref()
            .is_some_and(|v| !v.is_empty())
    );
    assert!(
        ctas.aggregate_write_digest
            .as_deref()
            .is_some_and(|v| !v.is_empty())
    );
    assert!(
        ctas.prepare_fact.is_some() && ctas.write_fact.is_some() && ctas.publish_fact.is_some()
    );
    for fact in [
        ctas.prepare_fact.as_ref(),
        ctas.write_fact.as_ref(),
        ctas.publish_fact.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        assert_eq!(fact.outcome, ExternalFactOutcome::KnownCommitted);
        for encoded in [
            fact.receipt.as_ref(),
            fact.evidence.as_ref(),
            fact.finalization_failure.as_ref(),
            fact.failure.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            assert!(
                encoded.len() <= 8 * 1024,
                "durable CTAS fact must stay bounded"
            );
        }
    }
    let child_ids = [
        ctas.prepare_operation_id,
        ctas.write_operation_id,
        ctas.publish_operation_id,
        ctas.abort_staging_operation_id,
    ];
    for (index, child_id) in child_ids.iter().enumerate() {
        assert!(
            child_ids[index + 1..].iter().all(|other| other != child_id),
            "CTAS child operation IDs must be stable and distinct"
        );
    }

    let truncate = operations
        .iter()
        .find(|operation| operation.operation_kind == OperationKind::Truncate)
        .expect("durable TRUNCATE operation");
    assert_eq!(truncate.state, OperationState::Finalized);
    let OperationPayload::TruncateLifecycle(truncate_record) = &truncate.payload else {
        panic!("TRUNCATE must persist a direct-mutation lifecycle payload")
    };
    assert_eq!(truncate_record.phase, TruncateLifecyclePhase::Committed);
    assert_eq!(truncate_record.next_action, StatementNextAction::None);
    assert_eq!(
        truncate_record.outcome.as_ref().map(|fact| fact.outcome),
        Some(ExternalFactOutcome::KnownCommitted)
    );
    assert!(
        dml.list_unfinished_operations()
            .expect("list unfinished CTAS/TRUNCATE operations")
            .is_empty(),
        "successful lifecycle plus terminal conflict must leave no recovery work"
    );
    drop(dml);
    runtime
        .block_on(host.shutdown())
        .expect("CTAS/TRUNCATE inspection host shutdown");
}

#[cfg(unix)]
#[test]
fn cross_process_three_be_frontend_update_merge_lifecycle() {
    let rest_uri = std::env::var("NOVAROCKS_ICEBERG_REST_URI")
        .expect("UPDATE/MERGE lifecycle acceptance requires NOVAROCKS_ICEBERG_REST_URI");
    let rest_warehouse = std::env::var("NOVAROCKS_ICEBERG_REST_WAREHOUSE")
        .expect("UPDATE/MERGE lifecycle acceptance requires NOVAROCKS_ICEBERG_REST_WAREHOUSE");
    let s3_endpoint = std::env::var("AWS_S3_ENDPOINT")
        .expect("UPDATE/MERGE lifecycle acceptance requires AWS_S3_ENDPOINT");
    let s3_access_key = std::env::var("AWS_S3_ACCESS_KEY_ID")
        .expect("UPDATE/MERGE lifecycle acceptance requires AWS_S3_ACCESS_KEY_ID");
    let s3_secret_key = std::env::var("AWS_S3_SECRET_ACCESS_KEY")
        .expect("UPDATE/MERGE lifecycle acceptance requires AWS_S3_SECRET_ACCESS_KEY");

    let _guard = lock_cluster_mvp();
    let fixture_dir = tempfile::tempdir_in(runtime_dir())
        .expect("create UPDATE/MERGE lifecycle fixture directory");
    let state_store_path = fixture_dir.path().join("frontend-state.sqlite");
    let metadata_path = fixture_dir.path().join("frontend-metadata.sqlite");
    let namespace = format!("dml6_cluster_{}", std::process::id());
    let catalog = "update_merge_lifecycle_ice";
    let cluster_id = "frontend-update-merge-lifecycle";
    let be_object_store = format!(
        r#"
[connector.object_store]
endpoint = "{s3_endpoint}"
access_key_id = "{s3_access_key}"
access_key_secret = "{s3_secret_key}"
enable_path_style_access = true
"#
    );
    let mut cluster =
        MultiBeClusterHarness::start_three_be_sqlite_state_store_with_metadata_and_be_extra(
            &state_store_path,
            &metadata_path,
            cluster_id,
            &be_object_store,
        );
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop(format!(
        r#"CREATE EXTERNAL CATALOG {catalog} PROPERTIES(
            "type"="iceberg",
            "iceberg.catalog.type"="rest",
            "uri"="{rest_uri}",
            "warehouse"="{rest_warehouse}",
            "aws.s3.endpoint"="{s3_endpoint}",
            "aws.s3.access_key"="{s3_access_key}",
            "aws.s3.secret_key"="{s3_secret_key}",
            "aws.s3.region"="us-east-1",
            "aws.s3.enable_path_style_access"="true")"#,
    ))
    .expect("create REST UPDATE/MERGE catalog");
    conn.query_drop(format!(
        "DROP DATABASE IF EXISTS {catalog}.{namespace} FORCE"
    ))
    .expect("remove stale UPDATE/MERGE namespace");
    conn.query_drop(format!("CREATE DATABASE {catalog}.{namespace}"))
        .expect("create UPDATE/MERGE namespace");
    conn.query_drop(format!(
        "CREATE TABLE {catalog}.{namespace}.target_orders (id INT, amount INT) \
         TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\", \
         \"novarocks.update.mode\"=\"merge-on-read\")"
    ))
    .expect("create UPDATE/MERGE target table");
    conn.query_drop(format!(
        "CREATE TABLE {catalog}.{namespace}.source_orders (id INT, amount INT) \
         TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")"
    ))
    .expect("create UPDATE/MERGE source table");
    conn.query_drop(format!(
        "INSERT INTO {catalog}.{namespace}.target_orders VALUES (1, 10), (2, 20)"
    ))
    .expect("seed UPDATE/MERGE target");
    conn.query_drop(format!(
        "INSERT INTO {catalog}.{namespace}.source_orders VALUES (2, 200), (3, 300)"
    ))
    .expect("seed UPDATE/MERGE source");

    let snapshots_before_empty_update: Vec<i64> = conn
        .query(format!(
            "SELECT count(*) FROM {catalog}.{namespace}.target_orders$snapshots"
        ))
        .expect("count snapshots before zero-effect UPDATE");
    conn.query_drop(format!(
        "UPDATE {catalog}.{namespace}.target_orders SET amount = 999 WHERE id = 999"
    ))
    .expect("execute zero-effect MOR UPDATE");
    let snapshots_after_empty_update: Vec<i64> = conn
        .query(format!(
            "SELECT count(*) FROM {catalog}.{namespace}.target_orders$snapshots"
        ))
        .expect("count snapshots after zero-effect UPDATE");
    assert_eq!(
        snapshots_after_empty_update, snapshots_before_empty_update,
        "zero-effect MOR UPDATE must not create a snapshot"
    );

    let scheduled_before_update = scheduled_fragments(&mut conn);
    conn.query_drop(format!(
        "UPDATE {catalog}.{namespace}.target_orders SET amount = 100 WHERE id = 1"
    ))
    .expect("execute frontend UPDATE");
    let scheduled_after_update = scheduled_fragments(&mut conn);
    assert!(
        scheduled_after_update > scheduled_before_update,
        "UPDATE must schedule remote fragments: before={scheduled_before_update}, \
         after={scheduled_after_update}"
    );
    let snapshots_before_first_merge: Vec<i64> = conn
        .query(format!(
            "SELECT count(*) FROM {catalog}.{namespace}.target_orders$snapshots"
        ))
        .expect("count snapshots before MERGE");
    let scheduled_before_first_merge = scheduled_fragments(&mut conn);
    conn.query_drop(format!(
        "MERGE INTO {catalog}.{namespace}.target_orders AS t \
         USING {catalog}.{namespace}.source_orders AS s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET amount = s.amount \
         WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (s.id, s.amount)"
    ))
    .expect("execute frontend matched-update/not-matched-insert MERGE");
    let scheduled_after_first_merge = scheduled_fragments(&mut conn);
    assert!(
        scheduled_after_first_merge > scheduled_before_first_merge,
        "first MOR MERGE must schedule remote fragments: before={scheduled_before_first_merge}, \
         after={scheduled_after_first_merge}"
    );
    let snapshots_after_first_merge: Vec<i64> = conn
        .query(format!(
            "SELECT count(*) FROM {catalog}.{namespace}.target_orders$snapshots"
        ))
        .expect("count snapshots after MERGE");
    assert_eq!(snapshots_after_first_merge.len(), 1);
    assert_eq!(snapshots_before_first_merge.len(), 1);
    assert_eq!(
        snapshots_after_first_merge[0],
        snapshots_before_first_merge[0] + 1,
        "matched UPDATE and not-matched INSERT must produce one MERGE snapshot"
    );
    conn.query_drop(format!(
        "INSERT INTO {catalog}.{namespace}.source_orders VALUES (4, 400)"
    ))
    .expect("seed delete/insert MOR MERGE source row");
    let snapshots_before_second_merge: Vec<i64> = conn
        .query(format!(
            "SELECT count(*) FROM {catalog}.{namespace}.target_orders$snapshots"
        ))
        .expect("count snapshots before delete/insert MERGE");
    let scheduled_before_second_merge = scheduled_fragments(&mut conn);
    conn.query_drop(format!(
        "MERGE INTO {catalog}.{namespace}.target_orders AS t \
         USING {catalog}.{namespace}.source_orders AS s ON t.id = s.id \
         WHEN MATCHED AND s.id = 2 THEN DELETE \
         WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (s.id, s.amount)"
    ))
    .expect("execute frontend matched-delete/not-matched-insert MOR MERGE");
    let scheduled_after_second_merge = scheduled_fragments(&mut conn);
    assert!(
        scheduled_after_second_merge > scheduled_before_second_merge,
        "second MOR MERGE must schedule remote fragments: before={scheduled_before_second_merge}, \
         after={scheduled_after_second_merge}"
    );
    let snapshots_after_second_merge: Vec<i64> = conn
        .query(format!(
            "SELECT count(*) FROM {catalog}.{namespace}.target_orders$snapshots"
        ))
        .expect("count snapshots after delete/insert MERGE");
    assert_eq!(snapshots_after_second_merge.len(), 1);
    assert_eq!(snapshots_before_second_merge.len(), 1);
    assert_eq!(
        snapshots_after_second_merge[0],
        snapshots_before_second_merge[0] + 1,
        "matched DELETE and not-matched INSERT must produce one MERGE snapshot"
    );
    let rows: Vec<(i32, i32)> = conn
        .query(format!(
            "SELECT id, amount FROM {catalog}.{namespace}.target_orders ORDER BY id"
        ))
        .expect("read UPDATE/MERGE target rows");
    assert_eq!(rows, vec![(1, 100), (3, 300), (4, 400)]);

    let scheduled_before_compat = scheduled_fragments(&mut conn);
    let compat_error = conn
        .query_drop("UPDATE information_schema.be_configs SET Value = 'x'")
        .expect_err("UPDATE information_schema.be_configs must not retain a compatibility no-op");
    assert!(
        compat_error.to_string().contains("Iceberg")
            || compat_error.to_string().contains("unsupported")
            || compat_error.to_string().contains("Unsupported"),
        "be_configs UPDATE must fail as an unsupported/non-Iceberg target: {compat_error}"
    );
    assert_eq!(
        scheduled_fragments(&mut conn),
        scheduled_before_compat,
        "rejected be_configs UPDATE must not schedule fragments"
    );

    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));
    cluster.restart_fe();
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    let restored_rows: Vec<(i32, i32)> = conn
        .query(format!(
            "SELECT id, amount FROM {catalog}.{namespace}.target_orders ORDER BY id"
        ))
        .expect("read UPDATE/MERGE rows after FE restart");
    assert_eq!(restored_rows, rows);
    let restored_snapshots: Vec<i64> = conn
        .query(format!(
            "SELECT count(*) FROM {catalog}.{namespace}.target_orders$snapshots"
        ))
        .expect("count snapshots after FE restart");
    assert_eq!(restored_snapshots, snapshots_after_second_merge);
    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build UPDATE/MERGE StateStore inspection runtime");
    let host = runtime
        .block_on(FrontendApplicationHost::open(
            Some(sqlite_state_store_config(&state_store_path, cluster_id)),
            frontend_execution_config(),
            ClusterBackendOpenConfig::new(
                novarocks::common::app_config::ClusterRole::AllInOne,
                Vec::new(),
                Duration::from_secs(1),
                1,
                Duration::from_secs(1),
            )
            .expect("valid UPDATE/MERGE inspection backend config"),
        ))
        .expect("reopen UPDATE/MERGE StateStore");
    let dml = host.dml_service();
    let row_deltas = dml
        .list_operations()
        .expect("list durable UPDATE/MERGE operations")
        .into_iter()
        .filter(|operation| {
            operation.operation_kind == OperationKind::RowDelta
                && operation.target.catalog == catalog
                && operation.target.namespace == namespace
                && operation.target.table == "target_orders"
        })
        .collect::<Vec<_>>();
    assert_eq!(
        row_deltas.len(),
        4,
        "zero-effect UPDATE, non-empty UPDATE and two MERGEs must each be journaled"
    );
    assert!(
        row_deltas
            .iter()
            .filter(|operation| operation.state == OperationState::Finalized)
            .all(|operation| operation.commit_outcome.is_some()),
        "non-empty UPDATE/MERGE must retain commit outcomes: {row_deltas:?}"
    );
    assert!(
        row_deltas
            .iter()
            .filter(|operation| operation.state == OperationState::Aborted)
            .all(|operation| operation.commit_outcome.is_none()),
        "zero-effect UPDATE must be aborted without a commit outcome: {row_deltas:?}"
    );
    assert_eq!(
        row_deltas
            .iter()
            .filter(|operation| operation.state == OperationState::Finalized)
            .count(),
        3,
        "non-empty UPDATE and both MERGEs must be finalized: {row_deltas:?}"
    );
    assert_eq!(
        row_deltas
            .iter()
            .filter(|operation| operation.state == OperationState::Aborted)
            .count(),
        1,
        "zero-effect UPDATE must have one Aborted record: {row_deltas:?}"
    );
    assert!(
        row_deltas
            .iter()
            .any(|operation| operation.operation_subkind.as_deref() == Some("UPDATE"))
            && row_deltas
                .iter()
                .any(|operation| operation.operation_subkind.as_deref() == Some("MERGE")),
        "RowDelta records must retain UPDATE and MERGE subkinds: {row_deltas:?}"
    );
    assert!(
        dml.list_unfinished_operations()
            .expect("list unfinished UPDATE/MERGE operations")
            .is_empty(),
        "successful UPDATE/MERGE must leave no unresolved StateStore record"
    );
    drop(dml);
    runtime
        .block_on(host.shutdown())
        .expect("UPDATE/MERGE inspection host shutdown");
}

#[cfg(unix)]
#[test]
fn cross_process_three_be_insert_without_state_store_fails_before_side_effect() {
    let _guard = lock_cluster_mvp();
    let fixture_dir =
        tempfile::tempdir_in(runtime_dir()).expect("create no-StateStore fixture directory");
    let metadata_config = format!(
        r#"
[metadata]
provider = "sqlite"
path = "{}"
"#,
        fixture_dir
            .path()
            .join("frontend-metadata.sqlite")
            .display()
    );
    let failure = match std::panic::catch_unwind(|| {
        MultiBeClusterHarness::start_n_be_without_state_store(3, "", &metadata_config)
    }) {
        Ok(_cluster) => panic!("role=fe without StateStore must fail before serving SQL"),
        Err(failure) => failure,
    };
    let message = failure
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            failure
                .downcast_ref::<&'static str>()
                .map(|message| (*message).to_string())
        })
        .unwrap_or_else(|| "non-string panic payload".to_string());
    assert!(
        message.contains("role=fe requires StateStore for durable cluster backend membership"),
        "1FE+3BE must reject missing StateStore before the SQL endpoint opens: {message}"
    );
}

#[cfg(unix)]
#[test]
fn cross_process_three_be_sqlite_state_store_lifecycle() {
    let _guard = lock_cluster_mvp();
    let state_store_dir = tempfile::tempdir_in(runtime_dir()).expect("create state store tempdir");
    let state_store_path = state_store_dir.path().join("frontend-state.sqlite");
    assert!(
        state_store_path.is_absolute(),
        "SQLite StateStore path must be absolute: {}",
        state_store_path.display()
    );
    let state_store_config = format!(
        r#"
[state_store]
provider = "sqlite"
path = "{}"
cluster_id = "cluster-mvp"
deployment_owner = "fe-1"
"#,
        state_store_path.display()
    );
    let mut cluster = MultiBeClusterHarness::start_n_be(3, "", &state_store_config);

    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    let rows: Vec<i64> = conn
        .query(multi_submit_query_sql())
        .expect("multi-fragment CTE+JOIN query must succeed on 3-BE cluster");
    assert_eq!(
        rows,
        vec![1i64, 2i64],
        "3-BE multi-fragment query must return sorted results [1, 2]"
    );
    drop(conn);

    cluster.shutdown_fe_cleanly(Duration::from_secs(10));
    assert!(
        state_store_path.is_file(),
        "SQLite state store must exist after the first FE lifecycle"
    );

    cluster.restart_fe();
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    let rows: Vec<i64> = conn
        .query(multi_submit_query_sql())
        .expect("distributed query must succeed after immediate FE restart");
    assert_eq!(rows, vec![1i64, 2i64]);
    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));
}

#[cfg(unix)]
#[test]
fn cross_process_three_be_table_maintenance_lifecycle() {
    let _guard = lock_cluster_mvp();
    let state_store_dir = tempfile::tempdir_in(runtime_dir()).expect("create state store tempdir");
    let state_store_path = state_store_dir.path().join("frontend-state.sqlite");
    let metadata_path = state_store_dir.path().join("frontend-metadata.sqlite");
    let warehouse = tempfile::tempdir_in(runtime_dir()).expect("create fixture warehouse");
    let mut cluster = MultiBeClusterHarness::start_three_be_sqlite_state_store_with_metadata(
        &state_store_path,
        &metadata_path,
        "table-maintenance",
    );
    let diagnostics = cluster.log_diagnostics();

    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop(format!(
        r#"CREATE EXTERNAL CATALOG maintenance_ice PROPERTIES("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
        warehouse.path().display()
    ))
    .expect("create maintenance fixture catalog");
    conn.query_drop("CREATE DATABASE maintenance_ice.maintenance_db")
        .expect("create maintenance fixture database");
    conn.query_drop(
        "CREATE TABLE maintenance_ice.maintenance_db.orders (id INT, amount INT) \
         TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
    )
    .expect("create maintenance fixture table");
    conn.query_drop("INSERT INTO maintenance_ice.maintenance_db.orders VALUES (1, 10), (2, 20)")
        .expect("insert first maintenance fixture data file");
    conn.query_drop("INSERT INTO maintenance_ice.maintenance_db.orders VALUES (3, 30), (4, 40)")
        .expect("insert second maintenance fixture data file");
    conn.query_drop("DELETE FROM maintenance_ice.maintenance_db.orders WHERE id = 2")
        .expect("create deletion vector before optimize");

    let before_optimize: Vec<(Option<i32>, Option<i64>)> = conn
        .query("SELECT id, _row_id FROM maintenance_ice.maintenance_db.orders ORDER BY id")
        .expect("query fixture row lineage before optimize");
    conn.query_drop("ALTER TABLE maintenance_ice.maintenance_db.orders OPTIMIZE")
        .expect("submit first optimize job");
    let first_job_id = wait_for_latest_optimize_finished(
        &mut conn,
        "maintenance_ice",
        "maintenance_db",
        "orders",
        1,
        &diagnostics,
    );
    let after_optimize: Vec<(Option<i32>, Option<i64>)> = conn
        .query("SELECT id, _row_id FROM maintenance_ice.maintenance_db.orders ORDER BY id")
        .expect("query fixture row lineage after optimize");
    assert_eq!(
        after_optimize, before_optimize,
        "OPTIMIZE must preserve visible rows and row lineage"
    );

    conn.query_drop("ALTER TABLE maintenance_ice.maintenance_db.orders REWRITE MANIFESTS")
        .expect("rewrite manifests through frontend maintenance route");
    conn.query_drop(
        "ALTER TABLE maintenance_ice.maintenance_db.orders \
         EXPIRE SNAPSHOTS RETAIN LAST 1",
    )
    .expect("expire snapshots through frontend maintenance route");
    conn.query_drop(
        "ALTER TABLE maintenance_ice.maintenance_db.orders \
         REMOVE ORPHAN FILES OLDER THAN '2000-01-01 00:00:00'",
    )
    .expect("remove orphan files through frontend maintenance route");
    conn.query_drop("DELETE FROM maintenance_ice.maintenance_db.orders WHERE id = 3")
        .expect("create deletion vector before position-delete rewrite");
    let position_delete_result: Vec<Row> = conn
        .query(
            "CALL maintenance_ice.system.rewrite_position_delete_files(\
             table => 'maintenance_db.orders', options => map('rewrite-all', 'true'))",
        )
        .expect("rewrite position delete files through frontend maintenance route");
    assert_eq!(
        position_delete_result.len(),
        1,
        "rewrite position delete files must return one outcome row"
    );
    println!(
        "TABLE MAINTENANCE direct actions completed: REWRITE MANIFESTS, \
         EXPIRE SNAPSHOTS, REMOVE ORPHAN FILES, rewrite_position_delete_files"
    );
    assert_exact_live_backends(&mut conn, 3);

    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));
    assert!(
        state_store_path.is_file(),
        "SQLite StateStore must survive the first FE lifecycle"
    );

    cluster.restart_fe();
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    let restored_jobs =
        show_optimize_jobs(&mut conn, "maintenance_ice", "maintenance_db", "orders");
    assert!(
        restored_jobs.iter().any(|row| {
            row.get::<String, usize>(0).as_deref() == Some(first_job_id.as_str())
                && row.get::<String, usize>(2).as_deref() == Some("FINISHED")
        }),
        "terminal optimize history must survive FE restart; rows={restored_jobs:?}; {diagnostics}"
    );
    println!(
        "SHOW ALTER TABLE OPTIMIZE restored job {first_job_id} FINISHED after clean FE restart"
    );

    conn.query_drop("INSERT INTO maintenance_ice.maintenance_db.orders VALUES (5, 50), (6, 60)")
        .expect("persisted catalog must accept inserts after FE restart");
    conn.query_drop("ALTER TABLE maintenance_ice.maintenance_db.orders OPTIMIZE")
        .expect("submit second optimize job after FE restart");
    let second_job_id = wait_for_latest_optimize_finished(
        &mut conn,
        "maintenance_ice",
        "maintenance_db",
        "orders",
        2,
        &diagnostics,
    );
    assert_ne!(
        second_job_id, first_job_id,
        "FE restart must enqueue a distinct optimize job"
    );
    let final_rows: Vec<Option<i32>> = conn
        .query("SELECT id FROM maintenance_ice.maintenance_db.orders ORDER BY id")
        .expect("query maintenance fixture after second optimize");
    assert_eq!(
        final_rows,
        vec![Some(1), Some(4), Some(5), Some(6)],
        "maintenance lifecycle must preserve the expected visible row set"
    );
    assert_exact_live_backends(&mut conn, 3);

    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));
}

#[cfg(unix)]
#[test]
fn cross_process_three_be_mv_state_store_restart() {
    let _guard = lock_cluster_mvp();
    let state_store_dir = tempfile::tempdir_in(runtime_dir()).expect("create StateStore tempdir");
    let state_store_path = state_store_dir.path().join("frontend-mv.sqlite");
    let metadata_path = state_store_dir.path().join("frontend-metadata.sqlite");
    let mut cluster = MultiBeClusterHarness::start_three_be_sqlite_state_store_with_metadata(
        &state_store_path,
        &metadata_path,
        "mv-state-store-restart",
    );
    let warehouse = tempfile::tempdir_in(runtime_dir()).expect("create MV warehouse");

    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop(format!(
        "CREATE EXTERNAL CATALOG mv_restart_ice PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"hadoop\",\"iceberg.catalog.warehouse\"=\"{}\")",
        warehouse.path().display(),
    ))
    .expect("create restart Iceberg catalog");
    conn.query_drop("CREATE DATABASE mv_restart_ice.ns")
        .expect("create restart namespace");
    conn.query_drop("SET CATALOG mv_restart_ice")
        .expect("use restart catalog");
    conn.query_drop("USE ns").expect("use restart namespace");
    conn.query_drop(
        "CREATE TABLE orders (k1 INT, v2 BIGINT) \
         TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
    )
    .expect("create restart base table");
    conn.query_drop("INSERT INTO orders VALUES (1, 10), (2, 20)")
        .expect("seed restart base table");
    let base_rows: Vec<(i32, i64)> = conn
        .query("SELECT k1, v2 FROM orders ORDER BY k1")
        .expect("read restart base table before MV refresh");
    assert_eq!(base_rows, vec![(1, 10), (2, 20)]);
    let row_ids: Vec<i64> = conn
        .query("SELECT _row_id FROM orders ORDER BY k1")
        .expect("read restart base row lineage before MV refresh");
    assert_eq!(row_ids.len(), 2);
    let physical_rows: Vec<(i32, i64, i64)> = conn
        .query("SELECT k1, v2, _row_id AS __nova_base_row_id FROM orders ORDER BY k1")
        .expect("read restart base physical projection before MV refresh");
    assert_eq!(physical_rows.len(), 2);
    conn.query_drop(
        "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 \
         AS SELECT k1, v2 FROM orders",
    )
    .expect("create first MV through frontend StateStore service");
    conn.query_drop("REFRESH MATERIALIZED VIEW orders_mv")
        .expect("refresh first MV");
    let rows: Vec<(i32, i64)> = conn
        .query("SELECT k1, v2 FROM orders_mv ORDER BY k1")
        .expect("read first MV before FE restart");
    assert_eq!(rows, vec![(1, 10), (2, 20)]);
    drop(conn);

    cluster.shutdown_fe_cleanly(Duration::from_secs(10));
    assert!(
        state_store_path.is_file(),
        "MV StateStore persists across FE restart"
    );
    cluster.restart_fe();

    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop("SET CATALOG mv_restart_ice")
        .expect("restore restart catalog");
    conn.query_drop("USE ns")
        .expect("restore restart namespace");
    let restored: Vec<(i32, i64)> = conn
        .query("SELECT k1, v2 FROM orders_mv ORDER BY k1")
        .expect("read existing MV after FE restart");
    assert_eq!(restored, vec![(1, 10), (2, 20)]);
    conn.query_drop("REFRESH MATERIALIZED VIEW orders_mv")
        .expect("refresh existing MV after FE restart");
    conn.query_drop(
        "CREATE MATERIALIZED VIEW orders_mv_2 DISTRIBUTED BY HASH(k1) BUCKETS 2 \
         AS SELECT k1, v2 FROM orders",
    )
    .expect("create second MV after FE restart");
    let rows: Vec<Row> = conn
        .query("SHOW MATERIALIZED VIEWS FROM ns")
        .expect("show MVs after restart");
    let names: Vec<String> = rows
        .iter()
        .map(|row| row.get::<String, _>(0).expect("MV name column"))
        .collect();
    assert_eq!(names, vec!["orders_mv", "orders_mv_2"]);
    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build StateStore inspection runtime");
    let host = runtime
        .block_on(FrontendApplicationHost::open(
            Some(sqlite_state_store_config(
                &state_store_path,
                "mv-state-store-restart",
            )),
            frontend_execution_config(),
            ClusterBackendOpenConfig::new(
                novarocks::common::app_config::ClusterRole::AllInOne,
                Vec::new(),
                Duration::from_secs(1),
                1,
                Duration::from_secs(1),
            )
            .expect("valid StateStore inspection backend config"),
        ))
        .expect("reopen MV StateStore after clean FE shutdown");
    let definitions = host
        .mv_repository()
        .list_definitions()
        .expect("list MV definitions from StateStore");
    let first_id = definitions
        .iter()
        .find(|definition| definition.target_table.as_deref() == Some("orders_mv"))
        .map(|definition| definition.mv_id)
        .expect("first MV definition persists");
    let second_id = definitions
        .iter()
        .find(|definition| definition.target_table.as_deref() == Some("orders_mv_2"))
        .map(|definition| definition.mv_id)
        .expect("second MV definition persists");
    assert!(
        second_id > first_id,
        "StateStore-backed MV IDs must increase across FE restart: first={first_id}, second={second_id}"
    );
    runtime
        .block_on(host.shutdown())
        .expect("inspection host shutdown");
}

/// Exercises the frontend-owned scheduler in the native deployment shape.
///
/// The worker has no all-in-one branch: catalog facts are frozen by the FE and
/// refreshes are submitted through the three live BEs.  A debug-only FE
/// barrier holds the first admitted refresh, proving the configured permit is
/// an execution bound rather than merely queue bookkeeping.
#[cfg(unix)]
#[test]
#[ignore = "requires native 1FE+3BE processes and scheduler debug barriers"]
fn cross_process_three_be_mvx4_scheduler_catches_up_and_bounds_concurrency() {
    let _guard = lock_cluster_mvp();
    let barrier_dir = tempfile::tempdir_in(runtime_dir()).expect("create scheduler barrier dir");
    let _barrier_environment =
        EnvironmentValueGuard::set_path("NOVAROCKS_MVX4_SCHEDULER_TEST_DIR", barrier_dir.path());
    let hold_trigger = barrier_dir.path().join("mvx4-scheduler-hold.trigger");
    std::fs::write(&hold_trigger, "hold\n").expect("arm scheduler concurrency barrier");
    let state_store_dir = tempfile::tempdir_in(runtime_dir()).expect("create StateStore tempdir");
    let state_store_path = state_store_dir.path().join("frontend-mvx4.sqlite");
    let metadata_path = state_store_dir.path().join("frontend-metadata.sqlite");
    let scheduler_config = r#"
mv_refresh_scheduler_enabled = true
mv_refresh_scheduler_interval_ms = 100
mv_refresh_scheduler_max_concurrent = 1
mv_refresh_scheduler_failure_backoff_ms = 100
mv_refresh_scheduler_max_failure_backoff_ms = 1_000
"#;
    let mut cluster =
        MultiBeClusterHarness::start_three_be_sqlite_state_store_with_metadata_and_extras(
            &state_store_path,
            &metadata_path,
            "mvx4-scheduler",
            "",
            scheduler_config,
        );
    let diagnostics = cluster.log_diagnostics();
    let warehouse = tempfile::tempdir_in(runtime_dir()).expect("create MVX-4 warehouse");
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop(format!(
        "CREATE EXTERNAL CATALOG mvx4_sched_ice PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"hadoop\",\"iceberg.catalog.warehouse\"=\"{}\")",
        warehouse.path().display(),
    ))
    .expect("create MVX-4 scheduler catalog");
    conn.query_drop("CREATE DATABASE mvx4_sched_ice.ns")
        .expect("create MVX-4 scheduler namespace");
    conn.query_drop("SET CATALOG mvx4_sched_ice")
        .expect("select MVX-4 scheduler catalog");
    conn.query_drop("USE ns")
        .expect("select MVX-4 scheduler namespace");
    conn.query_drop(
        "CREATE TABLE orders (k1 INT, v2 BIGINT) TBLPROPERTIES (\"format-version\"=\"3\",\"write.row-lineage\"=\"true\")",
    )
    .expect("create MVX-4 scheduler base table");
    conn.query_drop(
        "CREATE MATERIALIZED VIEW orders_mv_a DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH ASYNC EVERY INTERVAL 1 SECOND AS SELECT k1, v2 FROM orders",
    )
    .expect("create first scheduled MV");
    conn.query_drop(
        "CREATE MATERIALIZED VIEW orders_mv_b DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH ASYNC EVERY INTERVAL 1 SECOND AS SELECT k1, v2 FROM orders",
    )
    .expect("create second scheduled MV");
    wait_for_scheduler_marker_count(barrier_dir.path(), 1);
    std::thread::sleep(Duration::from_millis(300));
    let admitted = std::fs::read_dir(barrier_dir.path())
        .expect("read scheduler barrier directory")
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("mvx4-scheduler-admitted-")
        })
        .count();
    assert_eq!(
        admitted, 1,
        "max_concurrent_refreshes=1 must admit only one real scheduler refresh"
    );
    conn.query_drop("INSERT INTO orders VALUES (1, 10), (2, 20)")
        .expect("seed scheduled MV base table");
    std::fs::remove_file(&hold_trigger).expect("release scheduler concurrency barrier");
    let initial = [(1, 10), (2, 20)];
    wait_for_mv_rows(
        &mut conn,
        "SELECT k1, v2 FROM orders_mv_a ORDER BY k1",
        &initial,
        &diagnostics,
    );
    wait_for_mv_rows(
        &mut conn,
        "SELECT k1, v2 FROM orders_mv_b ORDER BY k1",
        &initial,
        &diagnostics,
    );
    conn.query_drop("INSERT INTO orders VALUES (3, 30)")
        .expect("mutate scheduled MV base table");
    let caught_up = [(1, 10), (2, 20), (3, 30)];
    wait_for_mv_rows(
        &mut conn,
        "SELECT k1, v2 FROM orders_mv_a ORDER BY k1",
        &caught_up,
        &diagnostics,
    );
    wait_for_mv_rows(
        &mut conn,
        "SELECT k1, v2 FROM orders_mv_b ORDER BY k1",
        &caught_up,
        &diagnostics,
    );
    assert_exact_live_backends(&mut conn, 3);
    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));
}

/// Verifies that a clean FE shutdown cancels a frontend-owned, pre-dispatch
/// worker attempt and that restart rebinds only after StateStore/catalog
/// recovery before catching the durable watermark up.
#[cfg(unix)]
#[test]
#[ignore = "requires native 1FE+3BE processes and scheduler debug barriers"]
fn cross_process_three_be_mvx4_shutdown_cancels_and_recovers_background_work() {
    let _guard = lock_cluster_mvp();
    let barrier_dir = tempfile::tempdir_in(runtime_dir()).expect("create scheduler barrier dir");
    let _barrier_environment =
        EnvironmentValueGuard::set_path("NOVAROCKS_MVX4_SCHEDULER_TEST_DIR", barrier_dir.path());
    let hold_trigger = barrier_dir.path().join("mvx4-scheduler-hold.trigger");
    std::fs::write(&hold_trigger, "hold\n").expect("arm scheduler shutdown barrier");
    let state_store_dir = tempfile::tempdir_in(runtime_dir()).expect("create StateStore tempdir");
    let state_store_path = state_store_dir.path().join("frontend-mvx4.sqlite");
    let metadata_path = state_store_dir.path().join("frontend-metadata.sqlite");
    let scheduler_config = r#"
mv_refresh_scheduler_enabled = true
mv_refresh_scheduler_interval_ms = 100
mv_refresh_scheduler_max_concurrent = 1
mv_refresh_scheduler_failure_backoff_ms = 100
mv_refresh_scheduler_max_failure_backoff_ms = 1_000
"#;
    let mut cluster =
        MultiBeClusterHarness::start_three_be_sqlite_state_store_with_metadata_and_extras(
            &state_store_path,
            &metadata_path,
            "mvx4-shutdown-recovery",
            "",
            scheduler_config,
        );
    let diagnostics = cluster.log_diagnostics();
    let warehouse = tempfile::tempdir_in(runtime_dir()).expect("create MVX-4 warehouse");
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop(format!(
        "CREATE EXTERNAL CATALOG mvx4_recovery_ice PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"hadoop\",\"iceberg.catalog.warehouse\"=\"{}\")",
        warehouse.path().display(),
    ))
    .expect("create MVX-4 recovery catalog");
    conn.query_drop("CREATE DATABASE mvx4_recovery_ice.ns")
        .expect("create MVX-4 recovery namespace");
    conn.query_drop("SET CATALOG mvx4_recovery_ice")
        .expect("select MVX-4 recovery catalog");
    conn.query_drop("USE ns")
        .expect("select MVX-4 recovery namespace");
    conn.query_drop(
        "CREATE TABLE orders (k1 INT, v2 BIGINT) TBLPROPERTIES (\"format-version\"=\"3\",\"write.row-lineage\"=\"true\")",
    )
    .expect("create MVX-4 recovery base table");
    conn.query_drop(
        "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH ASYNC EVERY INTERVAL 1 SECOND AS SELECT k1, v2 FROM orders",
    )
    .expect("create scheduled MV for shutdown recovery");
    wait_for_scheduler_marker_count(barrier_dir.path(), 1);
    conn.query_drop("INSERT INTO orders VALUES (1, 10), (2, 20)")
        .expect("seed recovery base table");
    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));
    std::fs::remove_file(&hold_trigger).expect("release scheduler shutdown barrier");
    cluster.restart_fe();

    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop("SET CATALOG mvx4_recovery_ice")
        .expect("restore recovery catalog after FE restart");
    conn.query_drop("USE ns")
        .expect("restore recovery namespace after FE restart");
    let caught_up = [(1, 10), (2, 20)];
    wait_for_mv_rows(
        &mut conn,
        "SELECT k1, v2 FROM orders_mv ORDER BY k1",
        &caught_up,
        &diagnostics,
    );
    assert_exact_live_backends(&mut conn, 3);
    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));
}

/// Exercises the two crash windows that startup recovery must converge without
/// replaying a historical write or publication: a staged write before main
/// publication, and a published main snapshot before staging cleanup/finalize.
#[cfg(unix)]
#[test]
#[ignore = "requires native 1FE+3BE processes and debug recovery barriers"]
fn cross_process_three_be_mvx3_recovery_reconciles_staged_and_published_attempts() {
    let _guard = lock_cluster_mvp();
    let fault_dir = tempfile::tempdir_in(runtime_dir()).expect("create MV recovery fault dir");
    let _fault_environment = EnvironmentValueGuard::set_path(
        "NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_FAULT_DIR",
        fault_dir.path(),
    );
    let state_store_dir = tempfile::tempdir_in(runtime_dir()).expect("create StateStore tempdir");
    let state_store_path = state_store_dir.path().join("frontend-mv.sqlite");
    let metadata_path = state_store_dir.path().join("frontend-metadata.sqlite");
    let mut cluster =
        MultiBeClusterHarness::start_three_be_sqlite_state_store_with_metadata_and_fault_dir(
            &state_store_path,
            &metadata_path,
            "mvx3-recovery-reconciliation",
            fault_dir.path(),
        );
    let warehouse = tempfile::tempdir_in(runtime_dir()).expect("create MV recovery warehouse");
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop(format!(
        "CREATE EXTERNAL CATALOG mvx3_recovery_ice PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"hadoop\",\"iceberg.catalog.warehouse\"=\"{}\")",
        warehouse.path().display(),
    ))
    .expect("create recovery Iceberg catalog");
    conn.query_drop("CREATE DATABASE mvx3_recovery_ice.ns")
        .expect("create recovery namespace");
    conn.query_drop("SET CATALOG mvx3_recovery_ice")
        .expect("select recovery catalog");
    conn.query_drop("USE ns")
        .expect("select recovery namespace");
    conn.query_drop(
        "CREATE TABLE orders (k1 INT, v2 BIGINT) TBLPROPERTIES (\"format-version\"=\"3\",\"write.row-lineage\"=\"true\")",
    )
    .expect("create recovery base table");
    conn.query_drop("INSERT INTO orders VALUES (1, 10), (2, 20)")
        .expect("seed recovery base table");
    conn.query_drop(
        "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, v2 FROM orders",
    )
    .expect("create recovery MV");

    let write_trigger = fault_dir
        .path()
        .join("mv-refresh-at-write-committed.trigger");
    std::fs::write(&write_trigger, "token=staged-before-publication\n")
        .expect("arm staged recovery crash barrier");
    let mysql_port = cluster.fe_mysql_port();
    let (write_tx, write_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let result = (|| -> Result<(), String> {
            let mut refresh_conn = connect_mysql(mysql_port);
            refresh_conn
                .query_drop("SET CATALOG mvx3_recovery_ice")
                .map_err(|error| error.to_string())?;
            refresh_conn
                .query_drop("USE ns")
                .map_err(|error| error.to_string())?;
            refresh_conn
                .query_drop("REFRESH MATERIALIZED VIEW orders_mv")
                .map_err(|error| error.to_string())
        })();
        let _ = write_tx.send(result);
    });
    cluster.wait_for_fe_output_contains(
        "NOVAROCKS_MV_RECOVERY_PHASE phase=write-committed token=staged-before-publication",
        Duration::from_secs(30),
    );
    cluster.kill_fe();
    std::fs::remove_file(&write_trigger).expect("remove staged recovery crash barrier");
    assert!(
        write_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("staged refresh client must observe FE kill")
            .is_err(),
        "a killed frontend must not report staged refresh success"
    );
    cluster.restart_fe();
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    conn.query_drop("SET CATALOG mvx3_recovery_ice")
        .expect("restore recovery catalog after staged crash");
    conn.query_drop("USE ns")
        .expect("restore recovery namespace after staged crash");
    let after_staged_restart: Vec<(i32, i64)> = conn
        .query("SELECT k1, v2 FROM orders_mv ORDER BY k1")
        .expect("read MV after staged recovery");
    assert!(
        after_staged_restart.is_empty(),
        "staged-only recovery must not publish main: {after_staged_restart:?}"
    );
    conn.query_drop("REFRESH MATERIALIZED VIEW orders_mv")
        .expect("recovery must release staged attempt fence for a fresh refresh");
    let first_rows: Vec<(i32, i64)> = conn
        .query("SELECT k1, v2 FROM orders_mv ORDER BY k1")
        .expect("read recovered first refresh");
    assert_eq!(first_rows, vec![(1, 10), (2, 20)]);
    conn.query_drop("INSERT INTO orders VALUES (3, 30)")
        .expect("add incremental recovery source row");

    let publication_trigger = fault_dir
        .path()
        .join("mv-refresh-at-publication-committed.trigger");
    std::fs::write(&publication_trigger, "token=published-before-cleanup\n")
        .expect("arm published recovery crash barrier");
    let mysql_port = cluster.fe_mysql_port();
    let (publication_tx, publication_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let result = (|| -> Result<(), String> {
            let mut refresh_conn = connect_mysql(mysql_port);
            refresh_conn
                .query_drop("SET CATALOG mvx3_recovery_ice")
                .map_err(|error| error.to_string())?;
            refresh_conn
                .query_drop("USE ns")
                .map_err(|error| error.to_string())?;
            refresh_conn
                .query_drop("REFRESH MATERIALIZED VIEW orders_mv")
                .map_err(|error| error.to_string())
        })();
        let _ = publication_tx.send(result);
    });
    cluster.wait_for_fe_output_contains(
        "NOVAROCKS_MV_RECOVERY_PHASE phase=publication-committed token=published-before-cleanup",
        Duration::from_secs(30),
    );
    cluster.kill_fe();
    std::fs::remove_file(&publication_trigger).expect("remove published recovery crash barrier");
    assert!(
        publication_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("published refresh client must observe FE kill")
            .is_err(),
        "a killed frontend must not report publication refresh success"
    );
    cluster.restart_fe();

    let mut conn = connect_mysql(cluster.fe_mysql_port());
    conn.query_drop("SET CATALOG mvx3_recovery_ice")
        .expect("restore recovery catalog after publication crash");
    conn.query_drop("USE ns")
        .expect("restore recovery namespace after publication crash");
    let published_rows: Vec<(i32, i64)> = conn
        .query("SELECT k1, v2 FROM orders_mv ORDER BY k1")
        .expect("read MV after published recovery");
    assert_eq!(published_rows, vec![(1, 10), (2, 20), (3, 30)]);
    conn.query_drop("REFRESH MATERIALIZED VIEW orders_mv")
        .expect("published recovery must finalize durable MV metadata");
    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));
}

fn sqlite_state_store_config(state_store_path: &Path, cluster_id: &str) -> StateStoreHostConfig {
    StateStoreHostConfig {
        state_store: StateStoreAppConfig {
            store: StateStoreConfig {
                cluster_id: cluster_id.to_owned(),
                limits: StateStoreLimitOverrides::default(),
                provider: StateStoreProviderConfig::Sqlite {
                    path: state_store_path.to_owned(),
                    deployment_owner: "fe-1".to_owned(),
                },
            },
            mysql_client: None,
        },
        foundationdb_client: None,
    }
}

fn frontend_execution_config() -> FrontendExecutionConfig {
    FrontendExecutionConfig::new("127.0.0.1", 19090, std::num::NonZeroUsize::new(1).unwrap())
}

#[cfg(unix)]
#[test]
fn cross_process_three_be_session_view_lifecycle() {
    let _guard = lock_cluster_mvp();
    let state_store_dir = tempfile::tempdir_in(runtime_dir()).expect("create state store tempdir");
    let state_store_path = state_store_dir.path().join("frontend-state.sqlite");
    let mut cluster =
        MultiBeClusterHarness::start_three_be_sqlite_state_store(&state_store_path, "session-view");

    let warehouse = tempfile::tempdir_in(runtime_dir()).expect("create fixture warehouse");
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);

    conn.query_drop("SET catalog default_catalog")
        .expect("use session-view catalog");

    let base_rows: Vec<String> = conn
        .query(
            "SELECT schema_name FROM information_schema.schemata \
             WHERE schema_name = 'default'",
        )
        .expect("query session-view base table");
    assert_eq!(base_rows, vec!["default".to_string()]);

    conn.query_drop(
        "CREATE VIEW session_view_e2e.v AS \
         SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'default'",
    )
    .expect("create session view");
    let view_rows: Vec<String> = conn
        .query("SELECT schema_name FROM session_view_e2e.v")
        .expect("query session view");
    assert_eq!(
        view_rows, base_rows,
        "session view must match its direct base-table query"
    );
    let views: Vec<String> = conn
        .query("SHOW VIEWS FROM session_view_e2e")
        .expect("show session views");
    assert_eq!(views, vec!["v".to_string()]);

    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));
    assert!(
        state_store_path.is_file(),
        "SQLite state store must exist after persisting the session view"
    );

    cluster.restart_fe();
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop("SET catalog default_catalog")
        .expect("restore session-view catalog after FE restart");
    let restored_rows: Vec<String> = conn
        .query("SELECT schema_name FROM session_view_e2e.v")
        .expect("query restored session view");
    assert_eq!(
        restored_rows, base_rows,
        "session view query must survive FE restart"
    );
    let restored_views: Vec<String> = conn
        .query("SHOW VIEWS FROM session_view_e2e")
        .expect("show restored session views");
    assert_eq!(
        restored_views,
        vec!["v".to_string()],
        "SHOW VIEWS must restore the durable session view"
    );

    conn.query_drop(
        "CREATE OR REPLACE VIEW session_view_e2e.v AS \
         SELECT catalog_name FROM information_schema.schemata WHERE schema_name = 'default'",
    )
    .expect("replace session view");
    let replaced_direct_rows: Vec<String> = conn
        .query(
            "SELECT catalog_name FROM information_schema.schemata \
             WHERE schema_name = 'default'",
        )
        .expect("query replacement base-table projection");
    let replaced_view_rows: Vec<String> = conn
        .query("SELECT catalog_name FROM session_view_e2e.v")
        .expect("query replaced session view");
    assert_eq!(
        replaced_view_rows, replaced_direct_rows,
        "CREATE OR REPLACE VIEW must expose the replacement query"
    );

    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));
    cluster.restart_fe();
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop("SET catalog default_catalog")
        .expect("restore session-view catalog after replacement restart");
    let durable_replacement_rows: Vec<String> = conn
        .query("SELECT catalog_name FROM session_view_e2e.v")
        .expect("query durable replacement view");
    assert_eq!(
        durable_replacement_rows, replaced_direct_rows,
        "CREATE OR REPLACE VIEW definition must survive FE restart"
    );
    let durable_views: Vec<String> = conn
        .query("SHOW VIEWS FROM session_view_e2e")
        .expect("show session views after replacement restart");
    assert_eq!(durable_views, vec!["v".to_string()]);

    conn.query_drop(format!(
        r#"CREATE EXTERNAL CATALOG session_view_fixture PROPERTIES("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
        warehouse.path().display()
    ))
    .expect("create fixture catalog");
    conn.query_drop("CREATE DATABASE session_view_fixture.session_view_e2e")
        .expect("create same-name external database");
    conn.query_drop("DROP DATABASE session_view_fixture.session_view_e2e")
        .expect("drop same-name external database");
    conn.query_drop("SET catalog default_catalog")
        .expect("return to the default session-view catalog");
    let rows_after_external_drop: Vec<String> = conn
        .query("SELECT catalog_name FROM session_view_e2e.v")
        .expect("external database drop must preserve default-catalog view");
    assert_eq!(
        rows_after_external_drop, replaced_direct_rows,
        "external database cleanup must not cross into default_catalog"
    );

    conn.query_drop("DROP DATABASE default_catalog.session_view_e2e")
        .expect("drop default-catalog view database");
    conn.query_drop("DROP CATALOG session_view_fixture")
        .expect("clean up fixture catalog");

    drop(conn);
    cluster.shutdown_fe_cleanly(Duration::from_secs(10));
    cluster.restart_fe();
    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop("SET catalog default_catalog")
        .expect("restore default catalog after final FE restart");
    let views_after_drop: Vec<String> = conn
        .query("SHOW VIEWS FROM session_view_e2e")
        .expect("show session views after default database drop and restart");
    assert!(
        views_after_drop.is_empty(),
        "dropped default-catalog database must not restore views: {views_after_drop:?}"
    );
    conn.query_drop("SELECT catalog_name FROM session_view_e2e.v")
        .expect_err("dropped default-catalog view must remain absent after FE restart");
}

#[test]
fn reserved_port_blocks_rebinding_until_release() {
    let port = ReservedPort::new();
    let addr = ("127.0.0.1", port.port());

    assert!(
        std::net::TcpListener::bind(addr).is_err(),
        "reserved port must remain bound until release"
    );

    assert_eq!(port.release(), addr.1);
}
