use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, MutexGuard, mpsc};
use std::time::{Duration, Instant};

use mysql::prelude::Queryable;
use mysql::{Conn as MysqlConn, OptsBuilder};
use tempfile::{Builder as TempFileBuilder, NamedTempFile};

static CLUSTER_MVP_TEST_LOCK: Mutex<()> = Mutex::new(());

fn lock_cluster_mvp() -> MutexGuard<'static, ()> {
    CLUSTER_MVP_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn alloc_port() -> u16 {
    std::net::TcpListener::bind(("127.0.0.1", 0))
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local addr")
        .port()
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
            .arg("standalone-server")
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
                    panic!("stdout closed before readiness marker `{marker}`; stdout={stdout:?}");
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
                panic!(
                    "timed out waiting for marker `{marker}`; stdout={stdout:?}; stderr={}",
                    self.read_stderr()
                );
            }
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
            .user(Some("root".to_string()));
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

struct ClusterHarness {
    be: ProcessGuard,
    _fe: ProcessGuard,
    fe_mysql: u16,
}

impl ClusterHarness {
    fn start(be_debug: &str, fe_extra: &str) -> Self {
        let be_http = alloc_port();
        let be_starlet = alloc_port();
        let fe_mysql = alloc_port();
        let fe_http = alloc_port();
        let fe_starlet = alloc_port();

        let be_config = write_config(
            "be",
            &format!(
                r#"
[server]
host = "127.0.0.1"
http_port = {be_http}
starlet_port = {be_starlet}

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
http_port = {fe_http}
starlet_port = {fe_starlet}

[standalone_server]
mysql_port = {fe_mysql}

[cluster]
role = "fe"
backends = ["127.0.0.1:{be_starlet}"]
{fe_extra}
"#
            ),
        );

        let mut be = ProcessGuard::spawn(be_config.path());
        be.wait_for_ready("NOVAROCKS_READY role=be");

        let mut fe = ProcessGuard::spawn(fe_config.path());
        fe.wait_for_ready("NOVAROCKS_READY mysql_port=");

        Self {
            be,
            _fe: fe,
            fe_mysql,
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

fn send_mysql_query_and_disconnect(port: u16, sql: &str) {
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
        .shutdown(Shutdown::Both)
        .expect("shutdown raw mysql client");
}

#[test]
fn cross_process_remote_dispatcher_smoke() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _lock = lock_cluster_mvp();

    let be_http = alloc_port();
    let be_starlet = alloc_port();
    let fe_mysql = alloc_port();
    let fe_http = alloc_port();
    let fe_starlet = alloc_port();

    let be_config = write_config(
        "be",
        &format!(
            r#"
[server]
host = "127.0.0.1"
http_port = {be_http}
starlet_port = {be_starlet}

[cluster]
role = "be"
"#
        ),
    );
    // Spec (PR-4): FE backends must point to be_starlet (the NovaRocksGrpc
    // service port for SubmitFragment/FetchResult on the standalone BE).
    let fe_config = write_config(
        "fe",
        &format!(
            r#"
[server]
host = "127.0.0.1"
http_port = {fe_http}
starlet_port = {fe_starlet}

[standalone_server]
mysql_port = {fe_mysql}

[cluster]
role = "fe"
backends = ["127.0.0.1:{be_starlet}"]
"#
        ),
    );

    let mut be = ProcessGuard::spawn(be_config.path());
    be.wait_for_ready("NOVAROCKS_READY role=be");

    let mut fe = ProcessGuard::spawn(fe_config.path());
    fe.wait_for_ready("NOVAROCKS_READY mysql_port=");

    // Spec (PR-4 Critical): role=fe must NOT start a local gRPC/exchange server.
    // All fragments run on BE; FE only runs MySQL + coordinator + RemoteDispatcher.
    // Assert that the FE's http_port is NOT listening.
    let fe_http_addr: std::net::SocketAddr = format!("127.0.0.1:{fe_http}")
        .parse()
        .expect("parse fe http addr");
    assert!(
        std::net::TcpStream::connect_timeout(&fe_http_addr, Duration::from_millis(200)).is_err(),
        "spec violation: role=fe must NOT bind local gRPC exchange server on http_port={fe_http}"
    );

    let mut conn = connect_mysql(fe_mysql);

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

#[test]
fn submit_half_failure_cancels_submitted() {
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
        "NOVAROCKS_CANCEL count=1 finsts=1 reason=coordinator cancel",
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
"#,
        "",
    );

    send_mysql_query_and_disconnect(cluster.fe_mysql, disconnect_blocking_query_sql());

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
        let result = conn.query::<String, _>(multi_submit_query_sql());
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
