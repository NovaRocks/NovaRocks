use crate::types::RunnerConfig;
use anyhow::{Context, Result, bail};
use clap::ValueEnum;
use std::fs;
use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, mpsc};
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
pub(crate) struct CrossProcessRuntime {
    pub(crate) be_http_port: u16,
    pub(crate) be_starlet_port: u16,
    pub(crate) fe_http_port: u16,
    pub(crate) fe_starlet_port: u16,
    pub(crate) fe_mysql_port: u16,
}

pub(crate) trait ServerHandle {
    fn target_host(&self) -> Option<&str>;
    fn target_port(&self) -> Option<u16>;
}

pub(crate) fn launch_server(
    mode: ClusterMode,
    repo_root: &Path,
    runner_config: &RunnerConfig,
) -> Result<Box<dyn ServerHandle>> {
    match mode {
        ClusterMode::AllInOne => Ok(Box::new(NoopServerHandle)),
        ClusterMode::CrossProcess => Ok(Box::new(CrossProcessServerHandle::launch(
            repo_root,
            runner_config,
        )?)),
    }
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

    bail!("failed to locate standalone-server config for cross-process mode")
}

pub(crate) fn render_cross_process_config(
    base_config: &str,
    role: ClusterProcessRole,
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
                "starlet_port".to_string(),
                Value::Integer(i64::from(runtime.fe_starlet_port)),
            );
        }
        ClusterProcessRole::Be => {
            server.insert(
                "http_port".to_string(),
                Value::Integer(i64::from(runtime.be_http_port)),
            );
            server.insert(
                "starlet_port".to_string(),
                Value::Integer(i64::from(runtime.be_starlet_port)),
            );
        }
    }

    if matches!(role, ClusterProcessRole::Fe) {
        let standalone_server = table_mut(root, "standalone_server");
        standalone_server.insert(
            "mysql_port".to_string(),
            Value::Integer(i64::from(runtime.fe_mysql_port)),
        );
    }

    let cluster = table_mut(root, "cluster");
    match role {
        ClusterProcessRole::Fe => {
            cluster.insert("role".to_string(), Value::String("fe".to_string()));
            cluster.insert(
                "backends".to_string(),
                Value::Array(vec![Value::String(format!(
                    "127.0.0.1:{}",
                    runtime.be_starlet_port
                ))]),
            );
        }
        ClusterProcessRole::Be => {
            cluster.insert("role".to_string(), Value::String("be".to_string()));
            cluster.remove("backends");
        }
    }

    toml::to_string(&value).context("serialize cross-process standalone config")
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

struct CrossProcessServerHandle {
    target_host: String,
    target_port: u16,
    runtime_dir: PathBuf,
    _be_config_path: PathBuf,
    _fe_config_path: PathBuf,
    be_process: ProcessGuard,
    fe_process: ProcessGuard,
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
        self.runtime_dir
            .as_deref()
            .expect("runtime dir available")
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
    fn launch(repo_root: &Path, runner_config: &RunnerConfig) -> Result<Self> {
        let runtime_dir = RuntimeDirGuard::new(create_runtime_dir(repo_root)?);
        let ReservedRuntimePorts {
            be_http_port,
            be_starlet_port,
            fe_http_port,
            fe_starlet_port,
            fe_mysql_port,
        } = ReservedRuntimePorts::new()?;
        let runtime = CrossProcessRuntime {
            be_http_port: be_http_port.port(),
            be_starlet_port: be_starlet_port.port(),
            fe_http_port: fe_http_port.port(),
            fe_starlet_port: fe_starlet_port.port(),
            fe_mysql_port: fe_mysql_port.port(),
        };

        let novarocks_bin = discover_novarocks_binary(repo_root)?;
        let base_config_path = resolve_base_app_config_path(repo_root, runner_config)?;
        let base_config = fs::read_to_string(&base_config_path).with_context(|| {
            format!(
                "read standalone config for cross-process mode: {}",
                base_config_path.display()
            )
        })?;

        let be_config_path = runtime_dir.path().join("be.toml");
        fs::write(
            &be_config_path,
            render_cross_process_config(&base_config, ClusterProcessRole::Be, &runtime)?,
        )
        .with_context(|| format!("write {}", be_config_path.display()))?;

        let fe_config_path = runtime_dir.path().join("fe.toml");
        fs::write(
            &fe_config_path,
            render_cross_process_config(&base_config, ClusterProcessRole::Fe, &runtime)?,
        )
        .with_context(|| format!("write {}", fe_config_path.display()))?;

        let _ = be_http_port.release();
        let _ = be_starlet_port.release();
        let be_process = ProcessGuard::spawn(
            &novarocks_bin,
            "be",
            &be_config_path,
            "NOVAROCKS_READY role=be",
        )?;
        println!(
            "🚀 started cross-process BE pid={} starlet_port={} config={}",
            be_process.pid(),
            runtime.be_starlet_port,
            be_config_path.display()
        );

        let _ = fe_http_port.release();
        let _ = fe_starlet_port.release();
        let _ = fe_mysql_port.release();
        let fe_process = ProcessGuard::spawn(
            &novarocks_bin,
            "fe",
            &fe_config_path,
            "NOVAROCKS_READY mysql_port=",
        )?;
        println!(
            "🚀 started cross-process FE pid={} mysql_port={} config={}",
            fe_process.pid(),
            runtime.fe_mysql_port,
            fe_config_path.display()
        );

        Ok(Self {
            target_host: "127.0.0.1".to_string(),
            target_port: runtime.fe_mysql_port,
            runtime_dir: runtime_dir.into_path(),
            _be_config_path: be_config_path,
            _fe_config_path: fe_config_path,
            be_process,
            fe_process,
        })
    }
}

impl ServerHandle for CrossProcessServerHandle {
    fn target_host(&self) -> Option<&str> {
        Some(self.target_host.as_str())
    }

    fn target_port(&self) -> Option<u16> {
        Some(self.target_port)
    }
}

impl Drop for CrossProcessServerHandle {
    fn drop(&mut self) {
        let _ = self.fe_process.stop();
        let _ = self.be_process.stop();
        let _ = fs::remove_dir_all(&self.runtime_dir);
    }
}

struct ProcessGuard {
    child: Child,
    stdout_rx: mpsc::Receiver<String>,
    stderr_buffer: Arc<Mutex<String>>,
    _stdout_thread: thread::JoinHandle<()>,
    stderr_thread: Option<thread::JoinHandle<()>>,
}

impl ProcessGuard {
    fn spawn(binary: &Path, role: &str, config_path: &Path, ready_marker: &str) -> Result<Self> {
        let mut child = build_novarocks_command(binary, role, config_path)
            .spawn()
            .with_context(|| format!("spawn novarocks {role} from {}", binary.display()))?;

        let stdout = child.stdout.take().context("capture child stdout")?;
        let stderr = child.stderr.take();
        let (tx, rx) = mpsc::channel();
        let stdout_thread = thread::spawn(move || {
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
        let stderr_buffer = Arc::new(Mutex::new(String::new()));
        let stderr_thread = stderr.map(|stderr| {
            let stderr_buffer = Arc::clone(&stderr_buffer);
            thread::spawn(move || {
                let reader = BufReader::new(stderr);
                for line in reader.lines() {
                    let Ok(line) = line else {
                        break;
                    };
                    if let Ok(mut buffer) = stderr_buffer.lock() {
                        if !buffer.is_empty() {
                            buffer.push('\n');
                        }
                        buffer.push_str(&line);
                    }
                }
            })
        });

        let mut process = Self {
            child,
            stdout_rx: rx,
            stderr_buffer,
            _stdout_thread: stdout_thread,
            stderr_thread,
        };
        process.wait_for_ready(ready_marker)?;
        Ok(process)
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }

    fn stop(&mut self) -> Result<()> {
        if self.child.try_wait()?.is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        self.join_stderr_thread();
        Ok(())
    }

    fn join_stderr_thread(&mut self) {
        if let Some(stderr_thread) = self.stderr_thread.take() {
            let _ = stderr_thread.join();
        }
    }

    fn wait_for_ready(&mut self, marker: &str) -> Result<()> {
        let deadline = Instant::now() + startup_timeout();
        let mut stdout = Vec::new();
        loop {
            if let Some(status) = self.child.try_wait()? {
                self.join_stderr_thread();
                let stderr = self.read_stderr();
                bail!(
                    "{}",
                    format_startup_failure(
                        marker,
                        &format!(
                            "novarocks exited before readiness marker with status {status}; stdout={stdout:?}; stderr={stderr}"
                        ),
                        &stderr,
                    )
                );
            }

            match self.stdout_rx.recv_timeout(Duration::from_millis(100)) {
                Ok(line) => {
                    if line.contains(marker) {
                        return Ok(());
                    }
                    stdout.push(line);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    if self.child.try_wait()?.is_none() {
                        let _ = self.child.kill();
                        let _ = self.child.wait();
                    }
                    self.join_stderr_thread();
                    let stderr = self.read_stderr();
                    bail!(
                        "{}",
                        format_startup_failure(
                            marker,
                            &format!("stdout closed before readiness marker; stdout={stdout:?}"),
                            &stderr,
                        )
                    );
                }
            }

            if Instant::now() >= deadline {
                let _ = self.child.kill();
                let _ = self.child.wait();
                self.join_stderr_thread();
                let stderr = self.read_stderr();
                bail!(
                    "{}",
                    format_startup_failure(
                        marker,
                        &format!(
                            "timed out waiting for readiness marker; stdout={stdout:?}; stderr={stderr}"
                        ),
                        &stderr,
                    )
                );
            }
        }
    }

    fn read_stderr(&mut self) -> String {
        self.stderr_buffer
            .lock()
            .map(|buffer| buffer.clone())
            .unwrap_or_default()
    }
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

pub(crate) fn build_novarocks_command(binary: &Path, role: &str, config_path: &Path) -> Command {
    let mut command = Command::new(binary);
    command
        .arg("standalone-server")
        .arg("--role")
        .arg(role)
        .arg("--config")
        .arg(config_path)
        .env("NO_PROXY", "127.0.0.1,localhost")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

pub(crate) fn startup_timeout() -> Duration {
    startup_timeout_from_env(std::env::var("NOVAROCKS_STARTUP_TIMEOUT_SECS").ok().as_deref())
}

pub(crate) fn startup_timeout_from_env(raw: Option<&str>) -> Duration {
    let timeout_secs = raw
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .unwrap_or(120);
    Duration::from_secs(timeout_secs)
}

struct ReservedRuntimePorts {
    be_http_port: ReservedPort,
    be_starlet_port: ReservedPort,
    fe_http_port: ReservedPort,
    fe_starlet_port: ReservedPort,
    fe_mysql_port: ReservedPort,
}

impl ReservedRuntimePorts {
    fn new() -> Result<Self> {
        Ok(Self {
            be_http_port: ReservedPort::new()?,
            be_starlet_port: ReservedPort::new()?,
            fe_http_port: ReservedPort::new()?,
            fe_starlet_port: ReservedPort::new()?,
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
        let port = listener
            .local_addr()
            .context("read ephemeral port")?
            .port();
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

    #[test]
    fn process_guard_declares_drop_cleanup() {
        assert!(include_str!("cluster.rs").contains("impl Drop for ProcessGuard"));
    }

    #[test]
    fn process_guard_declares_stderr_thread_join_helper() {
        let source = include_str!("cluster.rs")
            .split("\n#[cfg(test)]")
            .next()
            .expect("source before tests");
        assert!(source.contains("fn join_stderr_thread"), "missing stderr join helper");
        assert!(
            source.contains("self.join_stderr_thread();"),
            "wait_for_ready should join stderr thread before reading stderr"
        );
    }

    #[test]
    fn process_guard_disconnected_branch_uses_startup_failure_diagnostics() {
        let source = include_str!("cluster.rs")
            .split("\n#[cfg(test)]")
            .next()
            .expect("source before tests");
        let disconnected_branch = source
            .split("Err(mpsc::RecvTimeoutError::Disconnected) => {")
            .nth(1)
            .expect("disconnected branch");
        let disconnected_branch = disconnected_branch
            .split("if Instant::now() >= deadline {")
            .next()
            .expect("disconnected branch body");

        assert!(
            disconnected_branch.contains("self.join_stderr_thread();"),
            "disconnected branch should join stderr thread"
        );
        assert!(
            disconnected_branch.contains("format_startup_failure("),
            "disconnected branch should use startup failure diagnostics"
        );
        assert!(
            disconnected_branch.contains("self.read_stderr()"),
            "disconnected branch should read stderr before formatting failure"
        );
    }

    #[test]
    fn reserved_port_blocks_rebinding_until_release() {
        let reserved = ReservedPort::new().expect("reserve port");
        let port = reserved.port();
        assert!(TcpListener::bind(("127.0.0.1", port)).is_err());

        let port = reserved.release();
        let listener = TcpListener::bind(("127.0.0.1", port)).expect("bind released port");
        drop(listener);
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
        assert!(dir.exists(), "disarmed runtime dir should remain for caller cleanup");

        fs::remove_dir_all(&dir).expect("cleanup runtime dir");
    }
}
