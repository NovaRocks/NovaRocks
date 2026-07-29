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

use novarocks::novarocks_config;
use novarocks::novarocks_logging;

mod composition;

#[derive(Debug, PartialEq, Eq)]
struct StandaloneServerCliArgs {
    mysql_port: Option<u16>,
    config_path: Option<String>,
    role: Option<novarocks::common::app_config::ClusterRole>,
}

fn print_main_usage() {
    eprintln!("Usage: novarocks [run|start|stop|restart|standalone] [--config <path>]");
    eprintln!("  run       - Run in foreground (default)");
    eprintln!("  start     - Run in background as daemon");
    eprintln!("  stop      - Stop running daemon");
    eprintln!("  restart   - Restart daemon");
    eprintln!("  standalone - Run a local MySQL-compatible standalone server");
}

fn print_standalone_server_usage() {
    eprintln!(
        "Usage: novarocks standalone [--port <port>] [--config <path>] [--role <fe|be|all-in-one>]"
    );
    eprintln!("Example:");
    eprintln!("  novarocks standalone --port 9030 --config /etc/novarocks/novarocks.toml");
    eprintln!("  novarocks standalone --role be --config /etc/novarocks/novarocks.toml");
}

#[cfg(feature = "compat")]
fn validate_daemon_build(_command: &str) -> Result<(), String> {
    Ok(())
}

#[cfg(not(feature = "compat"))]
fn validate_daemon_build(command: &str) -> Result<(), String> {
    Err(format!(
        "the {command} daemon interface requires a compat build; use `novarocks standalone --role be|fe|all-in-one --config <path>` for native roles"
    ))
}

/// Build the tracing EnvFilter expression from config: prefer the explicit
/// `log_filter`, else map `log_level` (keeping deps at info for debug/trace).
fn resolve_log_filter(cfg: &novarocks::common::app_config::NovaRocksConfig) -> String {
    if let Some(ref f) = cfg.log_filter {
        f.clone()
    } else {
        match cfg.log_level.as_str() {
            "debug" => "info,novarocks=debug".to_string(),
            "trace" => "info,novarocks=trace".to_string(),
            other => other.to_string(),
        }
    }
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
                    "unknown standalone arg: {other} (try `novarocks standalone --help`)"
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
    let mut cfg = match config_path.as_ref() {
        Some(p) => novarocks::common::app_config::NovaRocksConfig::load_from_file(p)
            .map_err(|e| anyhow::anyhow!("{}", e))?,
        None => novarocks::common::app_config::NovaRocksConfig::default(),
    };

    let role_override = cli.role;

    let role = resolve_cluster_role(&cfg, role_override);

    // Persist the effective role into the owned configuration before any
    // composition root observes it. Frontend admission and topology ownership
    // must never disagree with the CLI role override.
    cfg.cluster.role = role;
    cfg.cluster
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

/// Dial every backend address in `backends` with a 3-second TCP timeout.
/// Returns `Ok(())` if all are reachable, or an `Err` whose message identifies
/// the failing backend index and address: `"failed to dial backend {idx} ({addr}): {e}"`.
#[cfg(test)]
pub(crate) fn probe_all_backends(backends: &[String]) -> Result<(), String> {
    for (idx, b) in backends.iter().enumerate() {
        let addr: std::net::SocketAddr = b
            .parse()
            .map_err(|e| format!("invalid backend addr '{}': {}", b, e))?;
        std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_secs(3))
            .map_err(|e| format!("failed to dial backend {idx} ({addr}): {e}"))?;
    }
    Ok(())
}

fn dispatch_standalone_role_with_all_in_one(
    role: novarocks::common::app_config::ClusterRole,
    cfg: novarocks::common::app_config::NovaRocksConfig,
    port_override: Option<u16>,
    run_frontend: impl FnOnce(
        novarocks::common::app_config::NovaRocksConfig,
        Option<u16>,
    ) -> anyhow::Result<()>,
    run_backend: impl FnOnce(
        novarocks::common::app_config::NovaRocksConfig,
        Option<u16>,
    ) -> anyhow::Result<()>,
    run_all_in_one: impl FnOnce(
        novarocks::common::app_config::NovaRocksConfig,
        Option<u16>,
    ) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    match role {
        novarocks::common::app_config::ClusterRole::AllInOne => run_all_in_one(cfg, port_override),
        novarocks::common::app_config::ClusterRole::Fe => run_frontend(cfg, port_override),
        novarocks::common::app_config::ClusterRole::Be => run_backend(cfg, port_override),
    }
}

fn run_standalone_be_role(
    cfg: novarocks::common::app_config::NovaRocksConfig,
    port_override: Option<u16>,
) -> anyhow::Result<()> {
    if let Some(warn) = be_role_start_warning(port_override) {
        eprintln!("WARN: {warn}");
    }
    novarocks_backend::run_backend_server(novarocks_backend::BackendServerConfig { config: cfg })
        .map_err(|error| anyhow::anyhow!("role=be: {error}"))
}

fn run_standalone_server_cli(cli: StandaloneServerCliArgs) -> anyhow::Result<()> {
    // I1: load_config_and_resolve_role returns the resolved path so we thread
    // it — along with the already-validated cfg — into the execution path
    // without a second file read.
    let (cfg, role, resolved_config_path) = load_config_and_resolve_role(&cli)?;

    // Install the global config and initialize the tracing subscriber before
    // starting the server. Without this, standalone runs with no logging
    // (init_with_level is otherwise only called on the FE-compatible run/start
    // path), so log_filter/log_level/sys_log_dir from the config are ignored.
    novarocks::common::app_config::install_preloaded_config(cfg.clone());
    novarocks_logging::init_with_level(&resolve_log_filter(&cfg));
    novarocks::server::configure_standalone_internal_rpc_transport();

    let frontend_config_path = resolved_config_path.clone();

    dispatch_standalone_role_with_all_in_one(
        role,
        cfg,
        cli.mysql_port,
        move |cfg, port| {
            novarocks_frontend::run_frontend_server(novarocks_frontend::FrontendServerConfig {
                config: cfg,
                config_path: frontend_config_path,
                port_override: port,
                grpc_endpoint: novarocks_frontend::FrontendGrpcEndpointOwnership::HostedReportOnly,
            })
            .map_err(|error| anyhow::anyhow!("{error}"))
        },
        run_standalone_be_role,
        move |cfg, port| composition::run_all_in_one(cfg, resolved_config_path, port),
    )
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

/// Builds a `SocketAddr` for the BE readiness probe, correctly handling IPv6
/// hosts by using `SocketAddr` construction via `IpAddr` rather than string
/// concatenation, which produces invalid `::1:PORT` for IPv6.
#[cfg(test)]
fn be_readiness_probe_addr(bind_host: &str, port: u16) -> Result<std::net::SocketAddr, String> {
    let probe_host = health_check_host(bind_host);
    // Strip brackets so bare IPv6 addresses can be parsed as IpAddr.
    let stripped = probe_host.trim_matches(|c| c == '[' || c == ']');
    stripped
        .parse::<std::net::IpAddr>()
        .map(|ip| std::net::SocketAddr::new(ip, port))
        .or_else(|_| {
            // Hostname fallback: use bracketed form for `format!`-style parsing.
            let bracketed = if probe_host.contains(':') && !probe_host.starts_with('[') {
                format!("[{probe_host}]:{port}")
            } else {
                format!("{probe_host}:{port}")
            };
            bracketed
                .parse::<std::net::SocketAddr>()
                .map_err(|e| format!("invalid BE readiness probe addr '{bracketed}': {e}"))
        })
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

#[cfg(feature = "compat")]
fn run_compat_application_with<E>(
    config: novarocks::common::app_config::NovaRocksConfig,
    running: Arc<AtomicBool>,
    runner: impl FnOnce(novarocks_compat::CompatServerConfig, Box<dyn FnMut() -> bool>) -> Result<(), E>,
) -> Result<(), E> {
    runner(
        novarocks_compat::CompatServerConfig { config },
        Box::new(move || !running.load(Ordering::SeqCst)),
    )
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

    if mode == "standalone" {
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
    if matches!(mode, "run" | "start" | "restart")
        && let Err(error) = validate_daemon_build(mode)
    {
        eprintln!("{error}");
        process::exit(1);
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
            novarocks_logging::init_with_level(&resolve_log_filter(&cfg));

            eprintln!("NovaRocks {}", novarocks::version::full_version());

            let page_cache_initialized = if cfg.runtime.cache.page_cache_enable {
                novarocks_fs::DataCacheManager::instance().init_page_cache(
                    novarocks_fs::DataCachePageCacheOptions {
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
                novarocks_fs::init_parquet_cache(novarocks_fs::ParquetCacheOptions {
                    enable_metadata: cfg.runtime.cache.parquet_meta_cache_enable,
                    metadata_ttl: Duration::from_secs(
                        cfg.runtime.cache.parquet_meta_cache_ttl_seconds,
                    ),
                    enable_page: cfg.runtime.cache.parquet_page_cache_enable,
                });
            if parquet_cache_initialized {
                eprintln!(
                    "Parquet physical cache policy initialized: meta_enabled={}, meta_ttl={}s, page_enabled={}",
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
            let compat_result =
                run_compat_application_with(cfg.clone(), running.clone(), |config, shutdown| {
                    novarocks_compat::run_compat_server_until_shutdown(config, shutdown)
                });

            // Cleanup: remove pid file
            let _ = fs::remove_file(pid_file);
            #[cfg(feature = "compat")]
            compat_result.expect("compatibility BE application failed");
            #[cfg(feature = "compat")]
            println!("novarocksd stopped");
            #[cfg(not(feature = "compat"))]
            unreachable!("native daemon commands are rejected before runtime initialization");
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
        StandaloneServerCliArgs, dispatch_standalone_role_with_all_in_one,
        load_config_and_resolve_role, parse_standalone_server_args, probe_all_backends,
        resolve_cluster_role,
    };

    #[cfg(not(feature = "compat"))]
    #[test]
    fn native_daemon_commands_fail_before_runtime_initialization() {
        let error = super::validate_daemon_build("run")
            .expect_err("native build must reject the daemon command");
        assert!(error.contains("requires a compat build"), "{error}");
        assert!(
            error.contains("standalone --role be|fe|all-in-one"),
            "{error}"
        );
    }

    #[cfg(feature = "compat")]
    mod compat_delegation {
        use std::cell::Cell;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        use super::super::run_compat_application_with;

        #[test]
        fn compat_run_delegates_the_loaded_config_once() {
            let mut config = novarocks::common::app_config::NovaRocksConfig::default();
            config.server.brpc_port = 18_060;
            let running = Arc::new(AtomicBool::new(true));
            let calls = Cell::new(0);

            run_compat_application_with(config, running, |received, _shutdown_requested| {
                calls.set(calls.get() + 1);
                assert_eq!(received.config.server.brpc_port, 18_060);
                Ok::<(), &'static str>(())
            })
            .expect("compat runner delegation");

            assert_eq!(calls.get(), 1);
        }

        #[test]
        fn compat_run_forwards_the_shared_shutdown_state() {
            let running = Arc::new(AtomicBool::new(true));
            let runner_running = running.clone();

            run_compat_application_with(
                novarocks::common::app_config::NovaRocksConfig::default(),
                running,
                move |_config, mut shutdown_requested| {
                    assert!(!shutdown_requested());
                    runner_running.store(false, Ordering::SeqCst);
                    assert!(shutdown_requested());
                    Ok::<(), &'static str>(())
                },
            )
            .expect("compat runner delegation");
        }

        #[test]
        fn compat_run_propagates_the_runner_error_unchanged() {
            #[derive(Debug, PartialEq, Eq)]
            struct RunnerError(&'static str);

            let error = run_compat_application_with(
                novarocks::common::app_config::NovaRocksConfig::default(),
                Arc::new(AtomicBool::new(true)),
                |_config, _shutdown_requested| Err(RunnerError("compat runner failed")),
            )
            .expect_err("runner error must propagate");

            assert_eq!(error, RunnerError("compat runner failed"));
        }
    }

    mod frontend_dispatch {
        use super::dispatch_standalone_role_with_all_in_one;

        #[test]
        fn fe_and_all_in_one_dispatch_use_distinct_composition_roots() {
            for role in [
                novarocks::common::app_config::ClusterRole::Fe,
                novarocks::common::app_config::ClusterRole::AllInOne,
            ] {
                let frontend_calls = std::cell::Cell::new(0);
                let backend_calls = std::cell::Cell::new(0);
                let all_in_one_calls = std::cell::Cell::new(0);
                dispatch_standalone_role_with_all_in_one(
                    role,
                    novarocks::common::app_config::NovaRocksConfig::default(),
                    None,
                    |_, _| {
                        frontend_calls.set(frontend_calls.get() + 1);
                        Ok(())
                    },
                    |_, _| {
                        backend_calls.set(backend_calls.get() + 1);
                        Ok(())
                    },
                    |_, _| {
                        all_in_one_calls.set(all_in_one_calls.get() + 1);
                        Ok(())
                    },
                )
                .expect("role dispatch should succeed");
                assert_eq!(
                    frontend_calls.get(),
                    (role == novarocks::common::app_config::ClusterRole::Fe) as usize
                );
                assert_eq!(backend_calls.get(), 0, "{role:?} must not invoke backend");
                assert_eq!(
                    all_in_one_calls.get(),
                    (role == novarocks::common::app_config::ClusterRole::AllInOne) as usize
                );
            }
        }

        #[test]
        fn be_dispatch_invokes_backend_runner_exactly_once() {
            let frontend_calls = std::cell::Cell::new(0);
            let backend_calls = std::cell::Cell::new(0);
            let all_in_one_calls = std::cell::Cell::new(0);

            dispatch_standalone_role_with_all_in_one(
                novarocks::common::app_config::ClusterRole::Be,
                novarocks::common::app_config::NovaRocksConfig::default(),
                None,
                |_, _| {
                    frontend_calls.set(frontend_calls.get() + 1);
                    Ok(())
                },
                |_, _| {
                    backend_calls.set(backend_calls.get() + 1);
                    Ok(())
                },
                |_, _| {
                    all_in_one_calls.set(all_in_one_calls.get() + 1);
                    Ok(())
                },
            )
            .expect("BE role dispatch should succeed");

            assert_eq!(frontend_calls.get(), 0, "BE must not invoke frontend");
            assert_eq!(backend_calls.get(), 1, "BE must invoke backend once");
            assert_eq!(all_in_one_calls.get(), 0, "BE must not invoke all-in-one");
        }
    }

    #[test]
    fn parse_standalone_server_args_accepts_port_and_config() {
        let args = vec![
            "--port".to_string(),
            "19030".to_string(),
            "--config".to_string(),
            "novarocks.toml".to_string(),
        ];
        let parsed = parse_standalone_server_args(&args)
            .expect("parse standalone args")
            .expect("standalone args");
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
            .expect("parse standalone args")
            .expect("standalone args");
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
        assert!(err.contains("unknown standalone arg"));
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
    fn test_dispatch_role_fe_with_no_backend_enters_coordinator() {
        let mut cfg = novarocks::common::app_config::NovaRocksConfig::default();
        cfg.cluster.backends.clear();
        dispatch_standalone_role_with_all_in_one(
            novarocks::common::app_config::ClusterRole::Fe,
            cfg,
            None,
            |_, _| Ok(()),
            |_, _| panic!("role=fe must not invoke the backend runner"),
            |_, _| panic!("role=fe must not invoke the all-in-one runner"),
        )
        .expect("role=fe may start without configured backends");
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

    /// D2: dispatch_standalone_role with multiple reachable backends succeeds
    /// and calls run_all_in_one (coordinator path).
    #[test]
    fn dispatch_fe_multiple_reachable_backends_enters_coordinator() {
        let l1 = std::net::TcpListener::bind("127.0.0.1:0").expect("bind listener 1");
        let l2 = std::net::TcpListener::bind("127.0.0.1:0").expect("bind listener 2");
        let addr1 = l1.local_addr().expect("listener 1 addr");
        let addr2 = l2.local_addr().expect("listener 2 addr");
        let mut cfg = novarocks::common::app_config::NovaRocksConfig::default();
        cfg.cluster.backends = vec![addr1.to_string(), addr2.to_string()];
        dispatch_standalone_role_with_all_in_one(
            novarocks::common::app_config::ClusterRole::Fe,
            cfg,
            None,
            |_, _| Ok(()),
            |_, _| panic!("role=fe must not invoke the backend runner"),
            |_, _| panic!("role=fe must not invoke the all-in-one runner"),
        )
        .expect("fe with multiple reachable backends should enter coordinator path");
        drop(l1);
        drop(l2);
    }

    /// D4: FE startup does not synchronously dial configured backends; the
    /// dynamic registry and heartbeat/query paths own liveness.
    #[test]
    fn dispatch_fe_one_unreachable_backend_still_enters_coordinator() {
        let live = std::net::TcpListener::bind("127.0.0.1:0").expect("bind live listener");
        let live_addr = live.local_addr().expect("live addr");
        let dead = std::net::TcpListener::bind("127.0.0.1:0").expect("bind dead listener");
        let dead_port = dead.local_addr().expect("dead addr").port();
        drop(dead);
        let mut cfg = novarocks::common::app_config::NovaRocksConfig::default();
        cfg.cluster.backends = vec![live_addr.to_string(), format!("127.0.0.1:{dead_port}")];
        dispatch_standalone_role_with_all_in_one(
            novarocks::common::app_config::ClusterRole::Fe,
            cfg,
            None,
            |_, _| Ok(()),
            |_, _| panic!("role=fe must not invoke the backend runner"),
            |_, _| panic!("role=fe must not invoke the all-in-one runner"),
        )
        .expect("role=fe startup should not synchronously dial backends");
        drop(live);
    }

    #[test]
    fn test_fe_startup_dials_all_backends() {
        // Keep the first backend live so probe_all_backends must successfully dial
        // it, then fail on the second (dead) one. This proves the probe walks past
        // a reachable backend rather than short-circuiting on the first entry.
        let live = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let dead = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let live_port = live.local_addr().unwrap().port();
        let dead_port = dead.local_addr().unwrap().port();
        drop(dead);
        let backends = vec![
            format!("127.0.0.1:{live_port}"),
            format!("127.0.0.1:{dead_port}"),
        ];
        let err = probe_all_backends(&backends).expect_err("second backend down should fail");
        assert!(
            err.contains("backend 1") && err.contains(&dead_port.to_string()),
            "error must name backend index 1 and the dead port: {err}"
        );
        drop(live);
    }

    #[test]
    fn test_fe_startup_reports_first_unreachable_backend() {
        let live = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let live_port = live.local_addr().unwrap().port();
        let dead = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let dead_port = dead.local_addr().unwrap().port();
        drop(dead);
        let backends = vec![
            format!("127.0.0.1:{}", live_port),
            format!("127.0.0.1:{}", dead_port),
        ];
        let err = probe_all_backends(&backends).expect_err("one down should fail");
        assert!(
            err.contains(&dead_port.to_string()),
            "error must name the failing backend: {err}"
        );
        drop(live);
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
        dispatch_standalone_role_with_all_in_one(
            role,
            cfg,
            None,
            |_, _| Ok(()),
            |_, _| panic!("role=fe must not invoke the backend runner"),
            |_, _| panic!("role=fe must not invoke the all-in-one runner"),
        )
        .expect("fe with reachable backend must enter coordinator path");
    }

    #[test]
    fn test_config_file_fe_zero_backends_allowed_before_dispatch() {
        // Config declares role=fe with zero backends. D4 allows this because
        // backend membership is managed dynamically through SQL and metadata.
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
        let (cfg, role, _) =
            load_config_and_resolve_role(&cli).expect("fe with zero backends must load");
        assert_eq!(role, novarocks::common::app_config::ClusterRole::Fe);
        assert!(cfg.cluster.backends.is_empty());
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
        dispatch_standalone_role_with_all_in_one(
            novarocks::common::app_config::ClusterRole::AllInOne,
            cfg,
            None,
            |_, _| panic!("all-in-one must not use the frontend-only runner"),
            |_, _| panic!("all-in-one must not use the backend-only runner"),
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

    // D1 PR-4: BE readiness probe must use loopback when bind host is a wildcard.
    // The probe address is built via `health_check_host(bind_host)` so that
    // `0.0.0.0` / `::` never appear in `wait_for_tcp_ready`.
    #[test]
    fn be_readiness_probe_addr_uses_loopback_for_wildcard_bind() {
        // `health_check_host` is the shared helper used by both the daemon path
        // and the BE path.  Assert its mapping is correct for every wildcard form.
        assert_eq!(
            super::health_check_host("0.0.0.0"),
            "127.0.0.1",
            "IPv4 wildcard must map to IPv4 loopback"
        );
        assert_eq!(
            super::health_check_host("::"),
            "::1",
            "IPv6 wildcard :: must map to IPv6 loopback"
        );
        assert_eq!(
            super::health_check_host("[::]"),
            "::1",
            "IPv6 wildcard [::] must map to IPv6 loopback"
        );
        assert_eq!(
            super::health_check_host("192.168.1.10"),
            "192.168.1.10",
            "non-wildcard host must pass through unchanged"
        );
    }

    // D1b PR-4: BE readiness probe address construction must produce a valid
    // SocketAddr for all bind host variants, including IPv6.
    #[test]
    fn be_readiness_probe_addr_produces_valid_socket_addr() {
        // IPv4 wildcard -> 127.0.0.1:port
        let addr = super::be_readiness_probe_addr("0.0.0.0", 9020)
            .expect("IPv4 wildcard must build valid SocketAddr");
        assert_eq!(addr.to_string(), "127.0.0.1:9020");

        // IPv6 wildcard :: -> [::1]:port
        let addr = super::be_readiness_probe_addr("::", 9020)
            .expect("IPv6 wildcard :: must build valid SocketAddr");
        assert_eq!(
            addr.ip(),
            std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST)
        );
        assert_eq!(addr.port(), 9020);

        // IPv6 wildcard [::] -> [::1]:port
        let addr = super::be_readiness_probe_addr("[::]", 9020)
            .expect("IPv6 wildcard [::] must build valid SocketAddr");
        assert_eq!(
            addr.ip(),
            std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST)
        );
        assert_eq!(addr.port(), 9020);

        // specific IPv4 host -> unchanged
        let addr = super::be_readiness_probe_addr("192.168.1.10", 9020)
            .expect("specific IPv4 host must build valid SocketAddr");
        assert_eq!(addr.to_string(), "192.168.1.10:9020");

        // specific IPv6 host -> valid SocketAddr
        let addr = super::be_readiness_probe_addr("2001:db8::1", 9020)
            .expect("specific IPv6 host must build valid SocketAddr");
        assert_eq!(addr.ip().to_string(), "2001:db8::1");
        assert_eq!(addr.port(), 9020);
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
