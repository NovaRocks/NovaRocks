use crate::types::RunnerConfig;
use anyhow::{Context, Result, bail};
use clap::ValueEnum;
use std::fs;
use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
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
}

pub(crate) fn launch_server(
    mode: ClusterMode,
    cluster_size: usize,
    repo_root: &Path,
    runner_config: &RunnerConfig,
) -> Result<Box<dyn ServerHandle>> {
    match mode {
        ClusterMode::AllInOne => Ok(Box::new(NoopServerHandle)),
        ClusterMode::CrossProcess => Ok(Box::new(CrossProcessServerHandle::launch(
            cluster_size,
            repo_root,
            runner_config,
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

    let fallback = repo_root.join("tests/sql-test-runner/conf/standalone_managed_lake.toml");
    if fallback.is_file() {
        return Ok(fallback);
    }

    bail!("failed to locate standalone-server config for cross-process mode")
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
    novarocks_bin: PathBuf,
    be_config_paths: Vec<PathBuf>,
    be_processes: Vec<ProcessGuard>,
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
    fn launch(cluster_size: usize, repo_root: &Path, runner_config: &RunnerConfig) -> Result<Self> {
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

        // Write per-BE configs.
        let mut be_config_paths: Vec<PathBuf> = Vec::with_capacity(cluster_size);
        for i in 0..cluster_size {
            let be_config_path = runtime_dir.path().join(format!("be_{i}.toml"));
            fs::write(
                &be_config_path,
                render_cross_process_config(&base_config, ClusterProcessRole::Be, i, &runtime)?,
            )
            .with_context(|| format!("write {}", be_config_path.display()))?;
            be_config_paths.push(be_config_path);
        }

        // Write FE config.
        let fe_config_path = runtime_dir.path().join("fe.toml");
        fs::write(
            &fe_config_path,
            render_cross_process_config(&base_config, ClusterProcessRole::Fe, 0, &runtime)?,
        )
        .with_context(|| format!("write {}", fe_config_path.display()))?;

        // Spawn all BEs: release each BE's ports immediately before spawning it.
        let mut be_processes: Vec<ProcessGuard> = Vec::with_capacity(cluster_size);
        for (i, (reserved_be, be_config_path)) in reserved
            .be_ports
            .into_iter()
            .zip(be_config_paths.iter())
            .enumerate()
        {
            let grpc_port = reserved_be.grpc.port();
            let _ = reserved_be.http.release();
            let _ = reserved_be.grpc.release();
            let be_process = ProcessGuard::spawn(
                &novarocks_bin,
                "be",
                be_config_path,
                "NOVAROCKS_READY role=be",
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
        let fe_process = ProcessGuard::spawn(
            &novarocks_bin,
            "fe",
            &fe_config_path,
            "NOVAROCKS_READY mysql_port=",
        )?;
        println!(
            "started cross-process FE pid={} mysql_port={} config={}",
            fe_process.pid(),
            runtime.fe_mysql_port,
            fe_config_path.display()
        );

        Ok(Self {
            target_host: "127.0.0.1".to_string(),
            target_port: runtime.fe_mysql_port,
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
        let new_process = ProcessGuard::spawn(
            &self.novarocks_bin,
            "be",
            &config_path,
            "NOVAROCKS_READY role=be",
        )
        .with_context(|| format!("restart cross-process BE[{index}]"))?;
        println!(
            "restarted cross-process BE[{index}] pid={} config={}",
            new_process.pid(),
            config_path.display()
        );
        self.be_processes[index] = new_process;
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

    fn kill_now(&mut self) -> Result<()> {
        if self.child.try_wait()?.is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
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
                    let status = self.wait_for_exit_after_stdout_disconnect()?;
                    if status.is_none() {
                        let _ = self.child.kill();
                        let _ = self.child.wait();
                    }
                    self.join_stderr_thread();
                    let stderr = self.read_stderr();
                    let status_detail = match status {
                        Some(status) => format!("; child status={status}"),
                        None => {
                            "; child was still running after stdout closed and was killed"
                                .to_string()
                        }
                    };
                    bail!(
                        "{}",
                        format_startup_failure(
                            marker,
                            &format!(
                                "stdout closed before readiness marker{status_detail}; stdout={stdout:?}; stderr={stderr}"
                            ),
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

    fn wait_for_exit_after_stdout_disconnect(&mut self) -> Result<Option<ExitStatus>> {
        let deadline = Instant::now() + Duration::from_millis(500);
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Ok(Some(status));
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
            thread::sleep(Duration::from_millis(10));
        }
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
    Duration::from_secs(timeout_secs)
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
        assert!(
            source.contains("fn join_stderr_thread"),
            "missing stderr join helper"
        );
        assert!(
            source.contains("self.join_stderr_thread();"),
            "wait_for_ready should join stderr thread before reading stderr"
        );
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
        assert!(
            disconnected_branch.contains("wait_for_exit_after_stdout_disconnect"),
            "disconnected branch should wait briefly for child exit before killing"
        );
        assert!(
            disconnected_branch.contains("child status="),
            "disconnected branch should include child exit status when available"
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

[standalone_server]
mysql_port = 9030
warehouse_uri = "s3://warehouse/sql-tests"
user = "root"

[standalone_server.object_store]
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
            fe_value["standalone_server"]["object_store"]["endpoint"].as_str(),
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
        assert_eq!(
            be_value["standalone_server"]["object_store"]["endpoint"].as_str(),
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
}
