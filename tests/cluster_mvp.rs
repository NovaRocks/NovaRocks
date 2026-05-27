use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, MutexGuard, mpsc};
use std::time::{Duration, Instant};

use mysql::prelude::Queryable;
use mysql::{Conn as MysqlConn, OptsBuilder};
use tempfile::{Builder as TempFileBuilder, NamedTempFile};

static CLUSTER_MVP_TEST_LOCK: Mutex<()> = Mutex::new(());

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

#[test]
fn cross_process_select_one_smoke() {
    let binary = Path::new(env!("CARGO_BIN_EXE_novarocks"));
    if !binary.exists() {
        return;
    }
    let _lock: MutexGuard<'static, ()> = CLUSTER_MVP_TEST_LOCK.lock().expect("cluster mvp lock");

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

    let mut conn = connect_mysql(fe_mysql);
    let rows: Vec<String> = conn.query("SELECT 1").expect("run SELECT 1");
    assert_eq!(rows, vec!["1".to_string()]);
}
