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
use novarocks_frontend::{FrontendApplicationHost, FrontendExecutionConfig};
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
    stderr: Option<std::process::ChildStderr>,
    _stdout_thread: std::thread::JoinHandle<()>,
}

impl ProcessGuard {
    fn spawn(config_path: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_novarocks"))
            .arg("standalone")
            .arg("--config")
            .arg(config_path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn novarocks");
        let stdout = child.stdout.take().expect("child stdout");
        let stderr = child.stderr.take();
        let (tx, rx) = mpsc::channel();
        let stdout_thread = std::thread::spawn(move || {
            let reader = BufReader::new(stdout);
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
            stderr,
            _stdout_thread: stdout_thread,
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
        let mut stderr = String::new();
        if let Some(mut pipe) = self.stderr.take() {
            let _ = pipe.read_to_string(&mut stderr);
        }
        stderr
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

fn assert_fe_report_only_endpoint_rejects_local_submit(port: u16) {
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}")
        .parse()
        .expect("parse fe report endpoint addr");
    let client = novarocks::service::grpc_client::NovaRocksGrpcRemoteClient::new(addr)
        .expect("create grpc client for fe report endpoint");
    let err = client
        .blocking_submit_fragment(
            novarocks::service::grpc_client::proto::novarocks::SubmitFragmentRequest {
                plan: None,
                instance_params: None,
            },
        )
        .expect_err("role=fe report-only endpoint must reject local fragment submission");
    assert!(
        err.contains("FailedPrecondition") && err.contains("report-only"),
        "role=fe endpoint must reject local execution RPCs as report-only: {err}"
    );
}

struct ClusterHarness {
    be: ProcessGuard,
    _fe: ProcessGuard,
    fe_mysql: u16,
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
{fe_extra}
"#
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
        }
    }
}

struct MultiBeClusterHarness {
    #[allow(dead_code)]
    bes: Vec<ProcessGuard>,
    fe: Option<ProcessGuard>,
    fe_mysql: u16,
    #[allow(dead_code)]
    _be_configs: Vec<NamedTempFile>,
    fe_config: NamedTempFile,
    be_log_dirs: Vec<PathBuf>,
    fe_log_dir: PathBuf,
    _log_root: TempDir,
}

impl MultiBeClusterHarness {
    fn start_n_be(n: usize, be_debug: &str, fe_extra: &str) -> Self {
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

[cluster]
role = "fe"
backends = [{backends_list}]
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
            bes.push(ProcessGuard::spawn(be_configs[i].path()));
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
        Self::start_n_be(3, "", &state_store_config)
    }

    fn start_three_be_sqlite_state_store_with_metadata(
        state_store_path: &Path,
        metadata_path: &Path,
        cluster_id: &str,
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
        Self::start_n_be(3, "", &fe_extra)
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
    fn restart_fe(&mut self) {
        assert!(self.fe.is_none(), "old FE process must be stopped");
        let mut fe = ProcessGuard::spawn(self.fe_config.path());
        fe.wait_for_ready("NOVAROCKS_READY mysql_port=");
        self.fe = Some(fe);
    }

    fn wait_for_be_submit_cancel_coverage(
        &mut self,
        expected_submit_count: usize,
        expected_cancel_count: usize,
        cancel_detail: &str,
        timeout: Duration,
    ) {
        let deadline = Instant::now() + timeout;
        let mut stdout = vec![Vec::new(); self.bes.len()];
        let mut submitted = vec![0usize; self.bes.len()];
        let mut canceled = vec![0usize; self.bes.len()];
        loop {
            for (index, be) in self.bes.iter_mut().enumerate() {
                if let Some(status) = be.child.try_wait().expect("poll BE child") {
                    panic!(
                        "BE {index} exited before submit/cancel pairing completed with status {status}; stdout={:?}; stderr={}",
                        stdout[index],
                        be.read_stderr()
                    );
                }
                while let Ok(line) = be.stdout_rx.try_recv() {
                    if line.starts_with("NOVAROCKS_GRPC_SUBMIT call=") {
                        submitted[index] += 1;
                    }
                    if line.contains("NOVAROCKS_CANCEL") && line.contains(cancel_detail) {
                        let finsts = line
                            .split_ascii_whitespace()
                            .find_map(|field| field.strip_prefix("finsts="))
                            .and_then(|value| value.parse::<usize>().ok())
                            .unwrap_or_else(|| panic!("cancel marker lacks finsts count: {line}"));
                        canceled[index] += finsts;
                    }
                    stdout[index].push(line);
                }
            }
            let submitted_total = submitted.iter().sum::<usize>();
            let canceled_total = canceled.iter().sum::<usize>();
            if submitted_total == expected_submit_count
                && canceled_total == expected_cancel_count
                && submitted
                    .iter()
                    .zip(&canceled)
                    .all(|(submitted, canceled)| canceled >= submitted)
            {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "expected {expected_submit_count} submitted instances covered by {expected_cancel_count} canceled instances; submitted={submitted:?} canceled={canceled:?} stdout={stdout:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn wait_for_every_be_output_contains(&mut self, marker: &str, timeout: Duration) {
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
                return;
            }
            assert!(
                Instant::now() < deadline,
                "every BE must emit `{marker}`; observed={observed:?}; stdout={stdout:?}"
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
    "SELECT t.v \
     FROM qlc_cancel_catalog.qlc_cancel.cancellation_data AS t \
     CROSS JOIN TABLE(generate_series(1, 1000000000)) AS gs(x) \
     ORDER BY t.v DESC, x DESC LIMIT 1"
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

#[test]
fn all_in_one_select_uses_loopback_submit() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _lock = lock_cluster_mvp();

    let (_srv, mysql_port) = start_all_in_one(
        r#"
[debug]
fault_inject_submit_fail_after = 0
"#,
    );

    let mut conn = connect_mysql(mysql_port);
    let err = conn
        .query::<i64, _>("SELECT 1")
        .expect_err("SELECT 1 should hit RemoteDispatcher submit fault");
    let err = err.to_string();
    assert!(
        err.contains("debug submit fault injected"),
        "expected loopback submit fault, got: {err}"
    );
}

#[test]
fn all_in_one_loopback_select_succeeds() {
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
    srv.wait_for_output_contains("NOVAROCKS_GRPC_SUBMIT call=", Duration::from_secs(3));
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
"#
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

    // IW-4: role=fe exposes a report-capable NovaRocksGrpc endpoint, but it
    // must remain report-only. Local fragments still run on BE, not FE.
    assert_fe_report_only_endpoint_rejects_local_submit(fe_grpc_port);

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
    let config = write_config(
        "native-be-signal-restart",
        &format!(
            r#"
[server]
host = "127.0.0.1"
grpc_port = {grpc_port}

[cluster]
role = "be"
"#
        ),
    );

    let _ = grpc.release();
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
"#
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
fn submit_half_failure_cancels_attempted_submissions() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _lock = lock_cluster_mvp();

    let mut cluster = ClusterHarness::start(
        r#"
[debug]
emit_cancel_marker = true
"#,
        r#"
[debug]
fault_inject_submit_fail_after = 1
"#,
    );

    let mut conn = connect_mysql(cluster.fe_mysql);
    let err = conn
        .query::<String, _>(multi_submit_query_sql())
        .expect_err("second fragment submit should fail");
    let err_str = err.to_string();
    assert!(
        err_str.contains("submit_fragment") || err_str.contains("submit"),
        "expected submit failure, got: {err_str}"
    );
    assert!(
        err_str.contains("debug submit fault injected"),
        "expected injected submit failure, got: {err_str}"
    );
    cluster.be.wait_for_output_contains(
        "NOVAROCKS_CANCEL count=1 finsts=2 reason=coordinator cancel",
        Duration::from_secs(3),
    );
}

#[test]
fn mysql_disconnect_triggers_cancel() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _lock = lock_cluster_mvp();

    let mut cluster = ClusterHarness::start(
        r#"
[debug]
emit_cancel_marker = true
emit_grpc_fragment_marker = true
"#,
        "",
    );

    let stream = send_mysql_query(cluster.fe_mysql, disconnect_blocking_query_sql());
    cluster
        .be
        .wait_for_output_contains("NOVAROCKS_GRPC_SUBMIT call=", Duration::from_secs(3));
    stream
        .shutdown(Shutdown::Both)
        .expect("shutdown raw mysql client");

    cluster
        .be
        .wait_for_output_contains("NOVAROCKS_CANCEL count=1", Duration::from_secs(3));
}

#[test]
fn query_timeout_triggers_cancel() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _lock = lock_cluster_mvp();

    let mut cluster = ClusterHarness::start(
        r#"
[debug]
emit_cancel_marker = true
"#,
        "",
    );

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

    cluster
        .be
        .wait_for_output_contains("NOVAROCKS_CANCEL count=1", Duration::from_secs(5));
}

#[test]
fn three_be_query_timeout_cancels_remote_fragments() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _lock = lock_cluster_mvp();

    let mut cluster = MultiBeClusterHarness::start_n_be(
        3,
        r#"
[debug]
emit_cancel_marker = true
emit_grpc_fragment_marker = true
"#,
        "",
    );

    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    conn.query_drop("SET query_timeout = 1")
        .expect("set query timeout");
    let err = conn
        .query::<String, _>(coordinated_sleep_query_sql())
        .expect_err("query should time out while the 3-BE cluster is executing");
    let err_str = err.to_string();
    assert!(
        err_str.contains("timed out") || err_str.contains("timeout"),
        "expected timeout error, got: {err_str}"
    );

    cluster.wait_for_be_submit_cancel_coverage(
        2,
        2,
        "reason=coordinator cancel",
        Duration::from_secs(5),
    );
}

#[test]
fn three_be_kill_query_cancels_distributed_execution_from_a_control_connection() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _guard = lock_cluster_mvp();
    let metadata_dir =
        tempfile::tempdir_in(runtime_dir()).expect("create cancellation metadata directory");
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
emit_cancel_marker = true
emit_grpc_fragment_marker = true
"#,
        &metadata_config,
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
    for range in ["1, 1000", "1001, 2000", "2001, 3000"] {
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

    cluster.wait_for_every_be_output_contains(
        "NOVAROCKS_GRPC_SUBMIT_ACCEPTED",
        Duration::from_secs(10),
    );
    control
        .query_drop(format!("KILL QUERY {target_connection_id}"))
        .expect("KILL QUERY must acknowledge the active target statement");

    let target_error = target_done_rx
        .recv_timeout(Duration::from_secs(15))
        .expect("target query must terminate after KILL QUERY")
        .expect_err("target query must not succeed after KILL QUERY");
    assert_mysql_server_error(target_error, 1317);
    cluster.wait_for_every_be_output_contains("NOVAROCKS_CANCEL", Duration::from_secs(10));

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
fn three_be_partial_submit_failure_cancels_attempted_fragments() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _lock = lock_cluster_mvp();

    let mut cluster = MultiBeClusterHarness::start_n_be(
        3,
        r#"
[debug]
emit_cancel_marker = true
emit_grpc_fragment_marker = true
"#,
        r#"
[debug]
fault_inject_submit_fail_after = 2
"#,
    );

    let mut conn = connect_mysql(cluster.fe_mysql_port());
    assert_exact_live_backends(&mut conn, 3);
    let err = conn
        .query::<String, _>(multi_submit_query_sql())
        .expect_err("a later fragment submit should hit the injected fault");
    let err_str = err.to_string();
    assert!(
        err_str.contains("submit_fragment") || err_str.contains("submit"),
        "expected submit failure, got: {err_str}"
    );
    assert!(
        err_str.contains("debug submit fault injected"),
        "expected injected submit failure, got: {err_str}"
    );

    cluster.wait_for_be_submit_cancel_coverage(
        2,
        3,
        "reason=coordinator cancel",
        Duration::from_secs(5),
    );
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
    let metadata_dir = tempfile::tempdir_in(runtime_dir()).expect("create statistics metadata dir");
    let metadata_config = format!(
        r#"
[metadata]
provider = "sqlite"
path = "{}"
"#,
        metadata_dir.path().join("catalog.db").display()
    );
    let cluster = MultiBeClusterHarness::start_n_be(3, "", &metadata_config);
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
    conn.query_drop("INSERT INTO feh5_stats.t VALUES (1), (2), (3)")
        .expect("insert statistics rows");
    conn.query_drop("ANALYZE TABLE feh5_stats.t")
        .expect("analyze statistics table");

    let stats: Vec<(i64, i64)> = conn
        .query(
            "SELECT row_count, hll_cardinality(ndv) \
             FROM _statistics_.column_statistics \
             WHERE table_name = 'feh5_stats.t' AND column_name = 'k'",
        )
        .expect("query collected statistics");
    assert_eq!(stats, vec![(3, 3)]);

    let explain: Vec<String> = conn
        .query("EXPLAIN COSTS SELECT * FROM feh5_stats.t")
        .expect("explain with collected statistics");
    assert!(
        explain
            .iter()
            .any(|line| line.contains("ndv=3") && line.contains("stats={rows=3")),
        "EXPLAIN COSTS must consume frontend row-count and NDV statistics: {explain:?}"
    );
    assert_exact_live_backends(&mut conn, 3);
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
    let metadata_config = format!(
        r#"[metadata]
provider = "sqlite"
path = "{}"
"#,
        metadata_path.display()
    );
    let mut cluster = MultiBeClusterHarness::start_three_be_sqlite_state_store_with_extra(
        &state_store_path,
        "table-maintenance",
        &metadata_config,
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
