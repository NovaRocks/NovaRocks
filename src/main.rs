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
use std::env;
use std::fs::{self, File, OpenOptions};
use std::net::{TcpStream, ToSocketAddrs};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{self, Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use novarocks::common::network;
use novarocks::novarocks_config;
use novarocks::novarocks_logging;

#[derive(Debug, PartialEq, Eq)]
struct StandaloneServerCliArgs {
    mysql_port: Option<u16>,
    config_path: Option<String>,
    role: Option<novarocks::common::app_config::ClusterRole>,
}

fn print_main_usage() {
    eprintln!("Usage: novarocks [run|start|stop|restart|standalone-server] [--config <path>]");
    eprintln!("  run       - Run in foreground (default)");
    eprintln!("  start     - Run in background as daemon");
    eprintln!("  stop      - Stop running daemon");
    eprintln!("  restart   - Restart daemon");
    eprintln!("  standalone-server - Run a local MySQL-compatible standalone server");
}

fn print_standalone_server_usage() {
    eprintln!(
        "Usage: novarocks standalone-server [--port <port>] [--config <path>] [--role <fe|be|all-in-one>]"
    );
    eprintln!("Example:");
    eprintln!("  novarocks standalone-server --port 9030 --config /etc/novarocks/novarocks.toml");
    eprintln!("  novarocks standalone-server --role be --config /etc/novarocks/novarocks.toml");
}

fn parse_standalone_server_args(
    args: &[String],
) -> Result<Option<StandaloneServerCliArgs>, String> {
    let mut idx = 0usize;
    let mut mysql_port: Option<u16> = None;
    let mut config_path: Option<String> = None;
    let mut role: Option<novarocks::common::app_config::ClusterRole> = None;

    while let Some(arg) = args.get(idx) {
        match arg.as_str() {
            "--port" => {
                idx += 1;
                let raw = args
                    .get(idx)
                    .ok_or_else(|| "missing value for --port".to_string())?;
                mysql_port = Some(
                    raw.parse::<u16>()
                        .map_err(|e| format!("invalid --port value `{raw}`: {e}"))?,
                );
                idx += 1;
            }
            "--config" | "-c" => {
                idx += 1;
                config_path = args.get(idx).cloned();
                if config_path.is_none() {
                    return Err("missing value for --config/-c".to_string());
                }
                idx += 1;
            }
            "--role" => {
                idx += 1;
                let raw = args
                    .get(idx)
                    .ok_or_else(|| "missing value for --role".to_string())?;
                role = Some(
                    parse_cluster_role(raw)
                        .map_err(|e| format!("invalid --role value `{raw}`; {e}"))?,
                );
                idx += 1;
            }
            "--help" | "-h" => return Ok(None),
            other => {
                return Err(format!(
                    "unknown standalone-server arg: {other} (try `novarocks standalone-server --help`)"
                ));
            }
        }
    }

    Ok(Some(StandaloneServerCliArgs {
        mysql_port,
        config_path,
        role,
    }))
}

fn parse_cluster_role(value: &str) -> Result<novarocks::common::app_config::ClusterRole, String> {
    match value {
        "fe" => Ok(novarocks::common::app_config::ClusterRole::Fe),
        "be" => Ok(novarocks::common::app_config::ClusterRole::Be),
        "all-in-one" => Ok(novarocks::common::app_config::ClusterRole::AllInOne),
        other => Err(format!(
            "invalid cluster role '{}'; expected one of: fe, be, all-in-one",
            other
        )),
    }
}

fn resolve_cluster_role(
    cfg: &novarocks::common::app_config::NovaRocksConfig,
    role_override: Option<novarocks::common::app_config::ClusterRole>,
) -> novarocks::common::app_config::ClusterRole {
    role_override.unwrap_or(cfg.cluster.role)
}

fn wait_for_tcp_ready(
    addr: std::net::SocketAddr,
    timeout: Duration,
    label: &str,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    let mut last_error = None;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let attempt_timeout = remaining.min(Duration::from_millis(100));
        match TcpStream::connect_timeout(&addr, attempt_timeout) {
            Ok(_) => return Ok(()),
            Err(e) => last_error = Some(e),
        }
        std::thread::sleep(remaining.min(Duration::from_millis(10)));
    }

    match last_error {
        Some(e) => Err(anyhow::anyhow!(
            "{label} at {addr} did not become ready within {}ms: {e}",
            timeout.as_millis()
        )),
        None => Err(anyhow::anyhow!(
            "{label} at {addr} did not become ready within {}ms",
            timeout.as_millis()
        )),
    }
}

/// Load config from `cli.config_path` (or use defaults when absent), resolve
/// the effective cluster role (CLI override wins over config), and validate the
/// loaded cluster section.  Returns the owned config, the resolved role, and
/// the resolved config file path so callers can thread the pre-loaded config
/// into the execution path without a second file read (I1 fix).
fn load_config_and_resolve_role(
    cli: &StandaloneServerCliArgs,
) -> anyhow::Result<(
    novarocks::common::app_config::NovaRocksConfig,
    novarocks::common::app_config::ClusterRole,
    Option<PathBuf>,
)> {
    // C2: honour NOVAROCKS_CONFIG env var and ./novarocks.toml fallback, not
    // just the explicit --config path.
    let config_path = novarocks::common::app_config::resolve_config_path(
        cli.config_path.as_deref().map(std::path::Path::new),
    );
    let cfg = match config_path.as_ref() {
        Some(p) => novarocks::common::app_config::NovaRocksConfig::load_from_file(p)
            .map_err(|e| anyhow::anyhow!("{}", e))?,
        None => novarocks::common::app_config::NovaRocksConfig::default(),
    };

    let role_override = cli.role;

    let role = resolve_cluster_role(&cfg, role_override);

    // C1: validate using the *effective* (CLI-overridden) role, not the
    // config-file role.  Cloning only the small ClusterConfig struct avoids
    // mutating the returned cfg.
    let mut effective_cluster = cfg.cluster.clone();
    effective_cluster.role = role;
    effective_cluster
        .validate()
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    Ok((cfg, role, config_path))
}

/// Returns a human-readable warning string when `--port` is supplied together
/// with `role=be`.  The BE starts a gRPC server, not a MySQL server, so the
/// MySQL port override has no effect.
fn be_role_start_warning(port_override: Option<u16>) -> Option<String> {
    port_override.map(|p| {
        format!(
            "role=be: --port {p} is ignored; the BE role starts a gRPC server, not a MySQL server"
        )
    })
}

fn dispatch_standalone_role(
    role: novarocks::common::app_config::ClusterRole,
    cfg: novarocks::common::app_config::NovaRocksConfig,
    port_override: Option<u16>,
    run_all_in_one: impl FnOnce(
        novarocks::common::app_config::NovaRocksConfig,
        Option<u16>,
    ) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    match role {
        novarocks::common::app_config::ClusterRole::AllInOne => run_all_in_one(cfg, port_override),
        novarocks::common::app_config::ClusterRole::Fe => {
            let n = cfg.cluster.backends.len();
            if n != 1 {
                return Err(anyhow::anyhow!(
                    "role=fe: expected exactly one backend, got {n} in cluster.backends"
                ));
            }
            let backend_str = cfg
                .cluster
                .backends
                .first()
                .expect("length already checked above");
            let backend_addr: std::net::SocketAddr = backend_str.parse().map_err(|e| {
                anyhow::anyhow!("role=fe: invalid backend addr '{backend_str}': {e}")
            })?;
            std::net::TcpStream::connect_timeout(&backend_addr, std::time::Duration::from_secs(5))
                .map_err(|e| {
                    anyhow::anyhow!("role=fe: cannot reach backend {backend_addr}: {e}")
                })?;
            run_all_in_one(cfg, port_override)
        }
        novarocks::common::app_config::ClusterRole::Be => {
            if let Some(warn) = be_role_start_warning(port_override) {
                eprintln!("WARN: {warn}");
            }
            let host = cfg.server.host.clone();
            let starlet_port = cfg.server.starlet_port;
            let pid = std::process::id();
            let starlet_addr: std::net::SocketAddr =
                format!("{host}:{starlet_port}").parse().map_err(|e| {
                    anyhow::anyhow!(
                        "role=be: invalid novarocks grpc addr '{host}:{starlet_port}': {e}"
                    )
                })?;
            novarocks::common::app_config::install_preloaded_config(cfg);
            // Spec (PR-4): standalone BE exposes NovaRocksGrpc
            // (SubmitFragment/FetchResult/CancelFragment/Exchange) on starlet_port.
            // FE cluster.backends must point to this port.
            novarocks::start_grpc_exchange_server(&host, starlet_port)
                .map_err(|e| {
                    anyhow::anyhow!(
                        "role=be: failed to start NovaRocksGrpc server on {host}:{starlet_port}: {e}"
                    )
                })?;
            wait_for_tcp_ready(starlet_addr, Duration::from_secs(5), "novarocks grpc")
                .map_err(|e| anyhow::anyhow!("role=be: {e}"))?;
            println!("NOVAROCKS_READY role=be starlet_port={starlet_port} pid={pid}");
            let (_tx, rx) = std::sync::mpsc::channel::<()>();
            rx.recv().ok();
            Ok(())
        }
    }
}

fn run_standalone_server_cli(cli: StandaloneServerCliArgs) -> anyhow::Result<()> {
    // I1: load_config_and_resolve_role returns the resolved path so we thread
    // it — along with the already-validated cfg — into the execution path
    // without a second file read.
    let (cfg, role, resolved_config_path) = load_config_and_resolve_role(&cli)?;

    // Spec (PR-4): role=fe must NOT start a local gRPC/exchange server.
    // Use a role-specific server entry point so the closure below routes to
    // the right variant without changing dispatch_standalone_role's signature.
    let is_fe = role == novarocks::common::app_config::ClusterRole::Fe;
    dispatch_standalone_role(role, cfg, cli.mysql_port, move |cfg, port| {
        if is_fe {
            novarocks::server::run_standalone_fe_server_with_config(cfg, resolved_config_path, port)
                .map_err(|e| anyhow::anyhow!("{}", e))
        } else {
            novarocks::server::run_standalone_server_with_config(cfg, resolved_config_path, port)
                .map_err(|e| anyhow::anyhow!("{}", e))
        }
    })
}

fn read_pid_file(pid_file: &str) -> Result<Option<u32>, String> {
    if !Path::new(pid_file).exists() {
        return Ok(None);
    }
    let pid_raw = fs::read_to_string(pid_file).map_err(|e| format!("read pid file failed: {e}"))?;
    let pid_text = pid_raw.trim();
    if pid_text.is_empty() {
        return Err("pid file is empty".to_string());
    }
    let pid = pid_text
        .parse::<u32>()
        .map_err(|e| format!("invalid pid value '{pid_text}': {e}"))?;
    Ok(Some(pid))
}

fn is_process_running(pid: u32) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn stop_process(pid: u32, grace: Duration) {
    let _ = Command::new("kill")
        .arg("-2")
        .arg(pid.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

    let deadline = Instant::now() + grace;
    while Instant::now() < deadline {
        if !is_process_running(pid) {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    if is_process_running(pid) {
        eprintln!(
            "novarocks did not stop within {}s, sending SIGKILL...",
            grace.as_secs()
        );
        let _ = Command::new("kill")
            .arg("-9")
            .arg(pid.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

fn spawn_child_reaper(mut child: Child) {
    let _ = std::thread::Builder::new()
        .name("novarocks-daemon-reaper".to_string())
        .spawn(move || {
            let _ = child.wait();
        });
}

fn health_check_host(bind_host: &str) -> String {
    match bind_host {
        "0.0.0.0" => "127.0.0.1".to_string(),
        "::" | "[::]" => "::1".to_string(),
        other => other.to_string(),
    }
}

fn heartbeat_ready(host: &str, port: u16) -> Result<(), String> {
    let addrs = (host, port)
        .to_socket_addrs()
        .map_err(|e| format!("resolve {host}:{port} failed: {e}"))?;
    for addr in addrs {
        if TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
            return Ok(());
        }
    }
    Err(format!("connect {host}:{port} failed"))
}

fn wait_for_start_ready(
    pid: u32,
    pid_file: &str,
    host: &str,
    port: u16,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    let mut last_error = String::new();
    let mut stable_since: Option<Instant> = None;
    let stable_window = Duration::from_millis(800);
    while Instant::now() < deadline {
        if !is_process_running(pid) {
            return Err(format!("process {pid} exited unexpectedly"));
        }

        let pid_ready = match read_pid_file(pid_file) {
            Ok(Some(file_pid)) if file_pid == pid => true,
            Ok(Some(file_pid)) => {
                last_error = format!("pid file points to pid={file_pid}, expect {pid}");
                false
            }
            Ok(None) => {
                last_error = "pid file not created yet".to_string();
                false
            }
            Err(e) => {
                last_error = format!("pid file not ready: {e}");
                false
            }
        };

        let heartbeat_ok = match heartbeat_ready(host, port) {
            Ok(()) => true,
            Err(e) => {
                last_error = e;
                false
            }
        };

        if pid_ready && heartbeat_ok {
            if stable_since.is_none() {
                stable_since = Some(Instant::now());
            }
            if stable_since.is_some_and(|t| t.elapsed() >= stable_window) {
                return Ok(());
            }
        } else {
            stable_since = None;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    Err(format!(
        "timeout waiting heartbeat ready on {host}:{port}, last_error={last_error}"
    ))
}

#[cfg(unix)]
fn raise_nofile_limit() {
    const TARGET_SOFT_NOFILE: libc::rlim_t = 8192;

    let mut limit: libc::rlimit = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return;
    }

    if limit.rlim_cur >= TARGET_SOFT_NOFILE {
        return;
    }

    let target = std::cmp::min(limit.rlim_max, TARGET_SOFT_NOFILE);
    if target <= limit.rlim_cur {
        return;
    }

    let updated = libc::rlimit {
        rlim_cur: target,
        rlim_max: limit.rlim_max,
    };
    if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &updated) } == 0 {
        eprintln!(
            "Raised RLIMIT_NOFILE soft limit from {} to {}",
            limit.rlim_cur, target
        );
    }
}

#[cfg(not(unix))]
fn raise_nofile_limit() {}

fn open_daemon_stdout_log() -> Result<(File, String), String> {
    let log_path = novarocks_logging::resolve_stdout_log_path();
    if let Some(parent) = log_path.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            format!(
                "create daemon log directory {} failed: {e}",
                parent.display()
            )
        })?;
    }
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .map_err(|e| format!("open daemon stdout log {} failed: {e}", log_path.display()))?;
    Ok((file, log_path.display().to_string()))
}

fn main() {
    raise_nofile_limit();

    let args: Vec<String> = env::args().collect();
    let mut idx = 1usize;
    let mode = if args.get(idx).is_some_and(|s| !s.starts_with('-')) {
        let m = args[idx].as_str();
        idx += 1;
        m
    } else {
        "run"
    };

    if mode == "standalone-server" {
        match parse_standalone_server_args(&args[idx..]) {
            Ok(Some(cli)) => {
                if let Err(err) = run_standalone_server_cli(cli) {
                    eprintln!("{err}");
                    process::exit(1);
                }
                return;
            }
            Ok(None) => {
                print_standalone_server_usage();
                process::exit(0);
            }
            Err(err) => {
                eprintln!("{err}");
                print_standalone_server_usage();
                process::exit(1);
            }
        }
    }

    let mut config_path: Option<String> = None;
    while let Some(arg) = args.get(idx) {
        match arg.as_str() {
            "--config" | "-c" => {
                idx += 1;
                config_path = args.get(idx).cloned();
                if config_path.is_none() {
                    eprintln!("missing value for --config/-c");
                    process::exit(1);
                }
                idx += 1;
            }
            "--help" | "-h" => {
                print_main_usage();
                process::exit(0);
            }
            other => {
                eprintln!("unknown arg: {other} (try --help)");
                process::exit(1);
            }
        }
    }
    let pid_file = "novarocks.pid";
    match mode {
        "start" => {
            if Path::new(pid_file).exists() {
                match read_pid_file(pid_file) {
                    Ok(Some(pid)) if is_process_running(pid) => {
                        eprintln!("novarocks already running with pid={pid}");
                        return;
                    }
                    Ok(Some(pid)) => {
                        eprintln!("found stale pid file (pid={pid}), removing");
                        let _ = fs::remove_file(pid_file);
                    }
                    Ok(None) => {
                        eprintln!("pid file exists without pid, removing");
                        let _ = fs::remove_file(pid_file);
                    }
                    Err(err) => {
                        eprintln!("invalid pid file, removing: {err}");
                        let _ = fs::remove_file(pid_file);
                    }
                }
            }

            let cfg = match config_path.as_deref() {
                Some(p) => novarocks_config::init_from_path(p).expect("load novarocks config"),
                None => {
                    novarocks_config::init_from_env_or_default().expect("load novarocks config")
                }
            };
            let (stdout, log_file) = open_daemon_stdout_log().expect("open daemon stdout log");
            let stderr = stdout.try_clone().expect("clone log file handle");
            let ready_host = health_check_host(&cfg.server.host);
            let ready_port = cfg.server.heartbeat_port;

            let mut cmd = Command::new(env::current_exe().expect("current exe"));
            cmd.arg("run");
            if let Some(p) = config_path.as_deref() {
                cmd.arg("--config").arg(p);
            }
            cmd.stdin(Stdio::null());
            #[cfg(unix)]
            unsafe {
                cmd.pre_exec(|| {
                    if libc::setsid() == -1 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
            let child = cmd
                .stdout(Stdio::from(stdout))
                .stderr(Stdio::from(stderr))
                .spawn()
                .expect("spawn child");

            let child_pid = child.id();
            spawn_child_reaper(child);
            match wait_for_start_ready(
                child_pid,
                pid_file,
                &ready_host,
                ready_port,
                Duration::from_secs(8),
            ) {
                Ok(()) => {
                    println!(
                        "Started novarocks in background (PID: {}), heartbeat ready on {}:{}",
                        child_pid, ready_host, ready_port
                    );
                }
                Err(err) => {
                    eprintln!(
                        "novarocks start health check failed: {}. Check {}",
                        err, log_file
                    );
                    stop_process(child_pid, Duration::from_secs(2));
                    process::exit(1);
                }
            }
        }
        "run" => {
            let pid = process::id();
            fs::write(pid_file, pid.to_string()).expect("write pid file");

            // Setup signal handler for graceful shutdown
            let running = Arc::new(AtomicBool::new(true));
            let running_clone = running.clone();

            ctrlc::set_handler(move || {
                println!("\nReceived interrupt signal, shutting down...");
                running_clone.store(false, Ordering::SeqCst);
            })
            .expect("Error setting Ctrl-C handler");

            let cfg = match config_path.as_deref() {
                Some(p) => novarocks_config::init_from_path(p).expect("load novarocks config"),
                None => {
                    novarocks_config::init_from_env_or_default().expect("load novarocks config")
                }
            };

            // Build logging filter from config.
            // Prefer `log_filter` (full EnvFilter expression) if present.
            // Otherwise, treat `log_level` as the level for our own crate (`novarocks`)
            // and keep a sane default (global `info`) for dependencies, so that
            // noisy system libraries do not spam debug/trace logs.
            let filter = if let Some(ref f) = cfg.log_filter {
                f.as_str()
            } else {
                match cfg.log_level.as_str() {
                    // High-verbosity levels: enable for our crate, keep deps at info.
                    "debug" => "info,novarocks=debug",
                    "trace" => "info,novarocks=trace",
                    // Other levels: apply globally.
                    other => other,
                }
            };

            novarocks_logging::init_with_level(filter);

            eprintln!("NovaRocks {}", novarocks::version::full_version());

            let page_cache_initialized = if cfg.runtime.cache.page_cache_enable {
                novarocks::cache::DataCacheManager::instance().init_page_cache(
                    novarocks::cache::DataCachePageCacheOptions {
                        capacity: cfg.runtime.cache.page_cache_capacity,
                        evict_probability: cfg.runtime.cache.page_cache_evict_probability,
                    },
                )
            } else {
                false
            };
            if page_cache_initialized {
                eprintln!(
                    "DataCache page cache initialized: capacity={}, evict_probability={}",
                    cfg.runtime.cache.page_cache_capacity,
                    cfg.runtime.cache.page_cache_evict_probability
                );
            }

            let parquet_cache_initialized =
                novarocks::formats::parquet::init_datacache_parquet_cache(
                    novarocks::formats::parquet::ParquetCacheOptions {
                        enable_metadata: cfg.runtime.cache.parquet_meta_cache_enable,
                        metadata_ttl: Duration::from_secs(
                            cfg.runtime.cache.parquet_meta_cache_ttl_seconds,
                        ),
                        enable_page: cfg.runtime.cache.parquet_page_cache_enable,
                    },
                );
            if parquet_cache_initialized {
                eprintln!(
                    "Parquet DataCache policy initialized: meta_enabled={}, meta_ttl={}s, page_enabled={}",
                    cfg.runtime.cache.parquet_meta_cache_enable,
                    cfg.runtime.cache.parquet_meta_cache_ttl_seconds,
                    cfg.runtime.cache.parquet_page_cache_enable,
                );
            }
            if (cfg.runtime.cache.parquet_meta_cache_enable
                || cfg.runtime.cache.parquet_page_cache_enable)
                && !cfg.runtime.cache.page_cache_enable
            {
                eprintln!(
                    "Parquet cache policy enabled but runtime.cache.page_cache_enable=false; parquet meta/page cache is disabled at runtime"
                );
            }

            if cfg.runtime.cache.datacache_enable {
                eprintln!(
                    "Block cache is configured but currently disabled; skip disk datacache initialization and use memory cache only"
                );
            }

            #[cfg(feature = "compat")]
            let log_level_num = match cfg.log_level.as_str() {
                "trace" | "debug" => 0,
                "info" => 0,
                "warn" => 1,
                "error" => 2,
                _ => 0,
            };

            let server = &cfg.server;
            let advertise_host =
                network::advertise_host_for_server(server).expect("resolve advertise host");

            novarocks::service::frontend_rpc::init_frontend_rpc_manager();

            // Start NovaRocks gRPC servers first to guarantee Starlet endpoint is online
            // before heartbeat reports ports to FE.
            novarocks::start_grpc_server(server.host.as_str()).expect("start grpc server");

            // Start Rust heartbeat service
            let heartbeat_cfg = novarocks::service::heartbeat_service::HeartbeatConfig {
                host: server.host.clone(),
                advertise_host: advertise_host.clone(),
                heartbeat_port: server.heartbeat_port,
                be_port: server.be_port,
                brpc_port: server.brpc_port,
                http_port: server.http_port,
                starlet_port: server.starlet_port,
            };
            novarocks::service::heartbeat_service::start_heartbeat_server(heartbeat_cfg)
                .expect("start heartbeat server");

            // Start Rust BackendService (StarRocks BE be_port)
            let backend_cfg = novarocks::service::backend_service::BackendServiceConfig {
                host: server.host.clone(),
                be_port: server.be_port,
            };
            novarocks::service::backend_service::start_backend_service(backend_cfg)
                .expect("start backend service");

            // Start C++ brpc service (for query execution) — only with compat feature.
            #[cfg(feature = "compat")]
            {
                let compat_cfg = novarocks::service::compat::CompatConfig {
                    host: server.host.as_str(),
                    heartbeat_port: server.heartbeat_port,
                    brpc_port: server.brpc_port,
                    internal_service_query_rpc_thread_num:
                        novarocks::common::config::internal_service_query_rpc_thread_num()
                            .min(u32::MAX as usize) as u32,
                    debug_exec_batch_plan_json: cfg.debug.exec_batch_plan_json,
                    log_level: log_level_num,
                };
                novarocks::service::compat::start(&compat_cfg).expect("start compat");
            }

            println!(
                "novarocksd started (bind_host={}, advertise_host={}, heartbeat_port={}, be_port={}, brpc_port={}, http_port={}, starlet_port={})",
                server.host,
                advertise_host,
                server.heartbeat_port,
                server.be_port,
                server.brpc_port,
                server.http_port,
                server.starlet_port
            );
            println!("Press Ctrl-C to stop...");

            // Keep process alive until Ctrl-C or signal
            while running.load(Ordering::SeqCst) {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }

            #[cfg(feature = "compat")]
            novarocks::service::compat::stop();
            novarocks::service::backend_service::stop_backend_service();
            novarocks::service::heartbeat_service::stop_heartbeat_server();
            novarocks::service::grpc_server::stop_grpc_server();
            novarocks::service::report_worker::stop();

            // Cleanup: remove pid file
            let _ = fs::remove_file(pid_file);
            println!("novarocksd stopped");
        }
        "stop" => match read_pid_file(pid_file) {
            Ok(Some(pid)) => {
                if is_process_running(pid) {
                    println!("Stopping novarocks (PID: {})...", pid);
                    stop_process(pid, Duration::from_secs(5));
                } else {
                    println!("Found stale pid file (PID: {}), cleaning up...", pid);
                }
                let _ = fs::remove_file(pid_file);
            }
            Ok(None) => {
                println!("No novarocks.pid file found.");
            }
            Err(err) => {
                eprintln!("failed to parse pid file: {err}");
                let _ = fs::remove_file(pid_file);
            }
        },
        "restart" => {
            // Stop first
            if Path::new(pid_file).exists() {
                match read_pid_file(pid_file) {
                    Ok(Some(pid)) if is_process_running(pid) => {
                        println!("Stopping novarocks (PID: {})...", pid);
                        stop_process(pid, Duration::from_secs(5));
                        let _ = fs::remove_file(pid_file);
                        std::thread::sleep(Duration::from_secs(1));
                    }
                    Ok(Some(pid)) => {
                        println!("Found stale pid file (PID: {}), cleaning up...", pid);
                        let _ = fs::remove_file(pid_file);
                    }
                    Ok(None) => {
                        let _ = fs::remove_file(pid_file);
                    }
                    Err(err) => {
                        eprintln!("failed to parse pid file: {err}");
                        let _ = fs::remove_file(pid_file);
                    }
                }
            }

            let cfg = match config_path.as_deref() {
                Some(p) => novarocks_config::init_from_path(p).expect("load novarocks config"),
                None => {
                    novarocks_config::init_from_env_or_default().expect("load novarocks config")
                }
            };
            let ready_host = health_check_host(&cfg.server.host);
            let ready_port = cfg.server.heartbeat_port;

            // Start again
            let (stdout, log_file) = open_daemon_stdout_log().expect("open daemon stdout log");
            let stderr = stdout.try_clone().expect("clone log file handle");

            let mut cmd = Command::new(env::current_exe().expect("current exe"));
            cmd.arg("run");
            if let Some(p) = config_path.as_deref() {
                cmd.arg("--config").arg(p);
            }
            cmd.stdin(Stdio::null());
            #[cfg(unix)]
            unsafe {
                cmd.pre_exec(|| {
                    if libc::setsid() == -1 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
            let child = cmd
                .stdout(Stdio::from(stdout))
                .stderr(Stdio::from(stderr))
                .spawn()
                .expect("spawn child");

            let child_pid = child.id();
            spawn_child_reaper(child);
            match wait_for_start_ready(
                child_pid,
                pid_file,
                &ready_host,
                ready_port,
                Duration::from_secs(8),
            ) {
                Ok(()) => {
                    println!(
                        "Restarted novarocks in background (PID: {}), heartbeat ready on {}:{}",
                        child_pid, ready_host, ready_port
                    );
                }
                Err(err) => {
                    eprintln!(
                        "novarocks restart health check failed: {}. Check {}",
                        err, log_file
                    );
                    stop_process(child_pid, Duration::from_secs(2));
                    process::exit(1);
                }
            }
        }
        _ => {
            print_main_usage();
            process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        StandaloneServerCliArgs, dispatch_standalone_role, load_config_and_resolve_role,
        parse_standalone_server_args, resolve_cluster_role, wait_for_tcp_ready,
    };

    #[test]
    fn parse_standalone_server_args_accepts_port_and_config() {
        let args = vec![
            "--port".to_string(),
            "19030".to_string(),
            "--config".to_string(),
            "novarocks.toml".to_string(),
        ];
        let parsed = parse_standalone_server_args(&args)
            .expect("parse standalone-server args")
            .expect("standalone-server args");
        assert_eq!(
            parsed,
            StandaloneServerCliArgs {
                mysql_port: Some(19030),
                config_path: Some("novarocks.toml".to_string()),
                role: None,
            }
        );
    }

    #[test]
    fn parse_standalone_server_args_accepts_empty() {
        let parsed = parse_standalone_server_args(&[])
            .expect("parse standalone-server args")
            .expect("standalone-server args");
        assert_eq!(
            parsed,
            StandaloneServerCliArgs {
                mysql_port: None,
                config_path: None,
                role: None,
            }
        );
    }

    #[test]
    fn parse_standalone_server_args_rejects_unknown_flag() {
        let args = vec!["--unknown".to_string()];
        let err = parse_standalone_server_args(&args).expect_err("unknown flag must fail");
        assert!(err.contains("unknown standalone-server arg"));
    }

    #[test]
    fn test_standalone_server_role_arg_parses_fe() {
        let args = vec![
            "--role".to_string(),
            "fe".to_string(),
            "--config".to_string(),
            "fe.toml".to_string(),
        ];
        let parsed = parse_standalone_server_args(&args)
            .expect("parse args")
            .expect("args");
        assert_eq!(
            parsed.role,
            Some(novarocks::common::app_config::ClusterRole::Fe)
        );
        assert_eq!(parsed.config_path.as_deref(), Some("fe.toml"));
    }

    #[test]
    fn test_standalone_server_role_arg_parses_all_in_one() {
        let args = vec!["--role".to_string(), "all-in-one".to_string()];
        let parsed = parse_standalone_server_args(&args)
            .expect("parse args")
            .expect("args");
        assert_eq!(
            parsed.role,
            Some(novarocks::common::app_config::ClusterRole::AllInOne)
        );
    }

    #[test]
    fn test_standalone_server_role_invalid_rejected() {
        let args = vec!["--role".to_string(), "master".to_string()];
        let err = parse_standalone_server_args(&args).expect_err("invalid role must fail");
        assert!(err.contains("invalid --role value"));
    }

    #[test]
    fn test_role_override_wins_over_config() {
        let mut cfg = novarocks::common::app_config::NovaRocksConfig::default();
        cfg.cluster.role = novarocks::common::app_config::ClusterRole::AllInOne;
        let role = resolve_cluster_role(&cfg, Some(novarocks::common::app_config::ClusterRole::Fe));
        assert_eq!(role, novarocks::common::app_config::ClusterRole::Fe);
    }

    #[test]
    fn test_dispatch_role_fe_with_no_backend_errors() {
        let mut cfg = novarocks::common::app_config::NovaRocksConfig::default();
        cfg.cluster.backends.clear();
        let err = dispatch_standalone_role(
            novarocks::common::app_config::ClusterRole::Fe,
            cfg,
            None,
            |_, _| unreachable!("all-in-one should not be called"),
        )
        .expect_err("fe with no backend should error");
        assert!(err.to_string().contains("role=fe"));
    }

    // --- PR-4 spec compliance tests ---

    /// Issue 3: be_role_start_warning emits a message that mentions both
    /// "role=be" and "--port" when a port override is supplied.
    #[test]
    fn dispatch_be_role_with_port_override_warns_message() {
        let msg = super::be_role_start_warning(Some(9030));
        assert!(msg.is_some(), "expected warning when port_override is Some");
        let s = msg.unwrap();
        assert!(s.contains("role=be"), "must mention role=be: {s}");
        assert!(s.contains("--port"), "must mention --port: {s}");
        assert!(s.contains("9030"), "must include port value: {s}");
    }

    /// Issue 3: no warning is emitted when port_override is None.
    #[test]
    fn dispatch_be_role_without_port_override_no_warning() {
        let msg = super::be_role_start_warning(None);
        assert!(
            msg.is_none(),
            "no warning expected when port_override is None"
        );
    }

    /// Issue 4: dispatch_standalone_role returns an error that includes the
    /// backend count when more than one backend is configured for role=fe.
    /// Without the exactly-one guard, the first backend would be silently
    /// accepted even though validation should reject it.
    #[test]
    fn dispatch_fe_multiple_backends_returns_error_with_count() {
        // Open a TCP listener so the first backend IS reachable.  Without the
        // guard, dispatch_standalone_role would reach run_all_in_one and hit
        // unreachable!().  With the guard it must fail before the TCP probe.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral listener");
        let addr = listener.local_addr().expect("listener addr");
        let mut cfg = novarocks::common::app_config::NovaRocksConfig::default();
        cfg.cluster.backends = vec![addr.to_string(), "127.0.0.1:19999".to_string()];
        let err = dispatch_standalone_role(
            novarocks::common::app_config::ClusterRole::Fe,
            cfg,
            None,
            |_, _| unreachable!("must not reach run_all_in_one with multiple backends"),
        )
        .expect_err("fe with multiple backends must error");
        let msg = err.to_string();
        assert!(msg.contains("role=fe"), "must mention role=fe: {msg}");
        assert!(msg.contains('2'), "must include backend count: {msg}");
    }

    #[test]
    fn test_wait_for_tcp_ready_returns_ok_for_listening_socket() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        wait_for_tcp_ready(addr, std::time::Duration::from_millis(100), "test")
            .expect("listening socket should be ready");
    }

    #[test]
    fn test_wait_for_tcp_ready_errors_for_unbound_socket() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        drop(listener);
        let err = wait_for_tcp_ready(addr, std::time::Duration::from_millis(10), "test")
            .expect_err("unbound socket should not be ready");
        assert!(err.to_string().contains("test"));
    }

    // Serialize tests that mutate process-wide state (env vars, CWD) so they
    // don't interfere when the test harness runs tests in parallel threads.
    static ENV_MUTEX: std::sync::LazyLock<std::sync::Mutex<()>> =
        std::sync::LazyLock::new(|| std::sync::Mutex::new(()));

    // --- PR-1 spec compliance gap tests ---
    // These three tests fail on the current production code and drive the fixes:
    // 1. Config file role must be used when no CLI --role is given.
    // 2. ClusterConfig::validate() must run before dispatch.
    // 3. CLI --role override must still win over the config file role.

    fn write_toml_tempfile(toml: &str) -> tempfile::NamedTempFile {
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().expect("create tempfile");
        f.write_all(toml.as_bytes())
            .expect("write toml to tempfile");
        f
    }

    #[test]
    fn test_config_file_role_fe_used_when_no_cli_override() {
        // Config declares role=fe with exactly one backend (valid). No CLI --role.
        // load_config_and_resolve_role must read the file and return ClusterRole::Fe,
        // after which dispatch_standalone_role must validate reachability and enter
        // the standalone coordinator path.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind backend probe");
        let backend_addr = listener.local_addr().expect("backend probe addr");
        let toml = format!(
            r#"
[cluster]
role = "fe"
backends = ["{backend_addr}"]
"#
        );
        let f = write_toml_tempfile(&toml);
        let cli = StandaloneServerCliArgs {
            config_path: Some(f.path().to_str().expect("utf-8 path").to_string()),
            role: None,
            mysql_port: None,
        };
        let (cfg, role, _) =
            load_config_and_resolve_role(&cli).expect("load and resolve must succeed for valid fe");
        assert_eq!(role, novarocks::common::app_config::ClusterRole::Fe);
        dispatch_standalone_role(role, cfg, None, |_, _| Ok(()))
            .expect("fe with reachable backend must enter coordinator path");
    }

    #[test]
    fn test_config_file_fe_zero_backends_fails_validation_before_dispatch() {
        // Config declares role=fe with zero backends — invalid. Startup must fail
        // with the D1 v1 validation message before any dispatch happens.
        let toml = r#"
[cluster]
role = "fe"
backends = []
"#;
        let f = write_toml_tempfile(toml);
        let cli = StandaloneServerCliArgs {
            config_path: Some(f.path().to_str().expect("utf-8 path").to_string()),
            role: None,
            mysql_port: None,
        };
        let result = load_config_and_resolve_role(&cli);
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("fe with zero backends must fail validation"),
        };
        assert!(
            err.to_string()
                .contains("D1 v1 only supports exactly one backend"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_cli_role_override_be_wins_over_config_all_in_one() {
        // Config says all-in-one (no backends — valid for both all-in-one and be).
        // CLI --role be must win: load_config_and_resolve_role returns ClusterRole::Be.
        // BE startup binds sockets and blocks, so this unit test stops at role
        // resolution; the cluster MVP smoke test covers BE startup.
        let toml = r#"
[cluster]
role = "all-in-one"
"#;
        let f = write_toml_tempfile(toml);
        let cli = StandaloneServerCliArgs {
            config_path: Some(f.path().to_str().expect("utf-8 path").to_string()),
            role: Some(novarocks::common::app_config::ClusterRole::Be),
            mysql_port: None,
        };
        let (cfg, role, _) = load_config_and_resolve_role(&cli)
            .expect("load and resolve must succeed (be with no backends is valid)");
        assert_eq!(role, novarocks::common::app_config::ClusterRole::Be);
        assert!(cfg.cluster.backends.is_empty());
    }

    // C1: validate against the *effective* (CLI-overridden) role, not the config-file role.
    #[test]
    fn test_c1_cli_role_be_rejects_backends_from_config_file() {
        // Config says role=fe with 1 backend (valid for fe).
        // CLI says --role be. Effective role is BE, which must reject backends.
        let toml = r#"
[cluster]
role = "fe"
backends = ["127.0.0.1:9070"]
"#;
        let f = write_toml_tempfile(toml);
        let cli = StandaloneServerCliArgs {
            config_path: Some(f.path().to_str().expect("utf-8").to_string()),
            role: Some(novarocks::common::app_config::ClusterRole::Be),
            mysql_port: None,
        };
        let result = load_config_and_resolve_role(&cli);
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("be with backends must fail validation"),
        };
        assert!(
            err.to_string()
                .contains("role=be must not configure [cluster].backends"),
            "unexpected error: {err}"
        );
    }

    // C2: NOVAROCKS_CONFIG env var must be honoured when no explicit --config is given.
    #[test]
    fn test_c2_novarocks_config_env_var_used_when_no_cli_config() {
        let toml = r#"
[cluster]
role = "fe"
backends = ["127.0.0.1:9070"]
"#;
        let f = write_toml_tempfile(toml);
        let path = f.path().to_str().expect("utf-8").to_string();

        let _guard = ENV_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let prev = std::env::var("NOVAROCKS_CONFIG").ok();
        // SAFETY: single-threaded thanks to ENV_MUTEX held above.
        unsafe { std::env::set_var("NOVAROCKS_CONFIG", &path) };
        let cli = StandaloneServerCliArgs {
            config_path: None,
            role: None,
            mysql_port: None,
        };
        let result = load_config_and_resolve_role(&cli);
        match prev {
            // SAFETY: single-threaded thanks to ENV_MUTEX.
            Some(v) => unsafe { std::env::set_var("NOVAROCKS_CONFIG", v) },
            None => unsafe { std::env::remove_var("NOVAROCKS_CONFIG") },
        }

        let (_, role, _) = result.expect("NOVAROCKS_CONFIG must be picked up");
        assert_eq!(role, novarocks::common::app_config::ClusterRole::Fe);
    }

    // C2: ./novarocks.toml in CWD must be discovered when no --config and no env var.
    #[test]
    fn test_c2_default_novarocks_toml_in_cwd_used() {
        let toml = r#"
[cluster]
role = "fe"
backends = ["127.0.0.1:9070"]
"#;
        let dir = tempfile::TempDir::new().expect("create tempdir");
        std::fs::write(dir.path().join("novarocks.toml"), toml).expect("write novarocks.toml");

        let _guard = ENV_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let prev_env = std::env::var("NOVAROCKS_CONFIG").ok();
        let prev_dir = std::env::current_dir().expect("current dir");
        // SAFETY: single-threaded thanks to ENV_MUTEX.
        unsafe { std::env::remove_var("NOVAROCKS_CONFIG") };
        std::env::set_current_dir(dir.path()).expect("change to tempdir");

        let cli = StandaloneServerCliArgs {
            config_path: None,
            role: None,
            mysql_port: None,
        };
        let result = load_config_and_resolve_role(&cli);

        std::env::set_current_dir(&prev_dir).expect("restore cwd");
        match prev_env {
            // SAFETY: single-threaded thanks to ENV_MUTEX.
            Some(v) => unsafe { std::env::set_var("NOVAROCKS_CONFIG", v) },
            None => unsafe { std::env::remove_var("NOVAROCKS_CONFIG") },
        }

        let (_, role, _) = result.expect("./novarocks.toml in CWD must be picked up");
        assert_eq!(role, novarocks::common::app_config::ClusterRole::Fe);
    }

    // I1: dispatch_standalone_role must pass the pre-loaded cfg to the
    // all-in-one closure, not drop it.
    #[test]
    fn test_i1_all_in_one_closure_receives_validated_config() {
        use novarocks::common::app_config::StandaloneServerConfig;
        let mut cfg = novarocks::common::app_config::NovaRocksConfig::default();
        // Plant a sentinel mysql_port in the config that can only come from the
        // pre-loaded instance — it's never the default 9030.
        cfg.standalone_server = Some(StandaloneServerConfig {
            mysql_port: 23456,
            ..StandaloneServerConfig::default()
        });
        let captured_port: std::cell::Cell<u16> = std::cell::Cell::new(0);
        dispatch_standalone_role(
            novarocks::common::app_config::ClusterRole::AllInOne,
            cfg,
            None,
            |cfg, _port| {
                // The closure must receive the sentinel config (not a freshly
                // defaulted one).
                captured_port.set(
                    cfg.standalone_server
                        .as_ref()
                        .map(|s| s.mysql_port)
                        .unwrap_or(0),
                );
                Ok(())
            },
        )
        .expect("all-in-one dispatch must succeed");
        assert_eq!(
            captured_port.get(),
            23456,
            "all-in-one runner must receive the pre-loaded cfg with the sentinel mysql_port"
        );
    }

    // I1: load_config_and_resolve_role returns the resolved config path so the
    // caller can pass it to the server without a second resolve call.
    #[test]
    fn test_i1_load_config_returns_resolved_path() {
        let toml = r#"
[cluster]
role = "all-in-one"
"#;
        let f = write_toml_tempfile(toml);
        let explicit_path = f.path().to_str().expect("utf-8").to_string();
        let cli = StandaloneServerCliArgs {
            config_path: Some(explicit_path.clone()),
            role: None,
            mysql_port: None,
        };
        let (_, _, resolved_path) = load_config_and_resolve_role(&cli).expect("load must succeed");
        assert!(
            resolved_path.is_some(),
            "resolved_path must be Some when --config was provided"
        );
        assert_eq!(
            resolved_path.unwrap().to_str().unwrap(),
            explicit_path,
            "resolved path must match the explicit --config path"
        );
    }
}
