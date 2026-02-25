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
use std::fs::{self, File};
use std::net::{TcpStream, ToSocketAddrs};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{self, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use novarocks::novarocks_config;
use novarocks::novarocks_logging;
use std::eprintln;

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

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut idx = 1usize;
    let mode = if args.get(idx).is_some_and(|s| !s.starts_with('-')) {
        let m = args[idx].as_str();
        idx += 1;
        m
    } else {
        "run"
    };

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
                eprintln!("Usage: novarocks [run|start|stop|restart] [--config <path>]");
                eprintln!("  run      - Run in foreground (default)");
                eprintln!("  start    - Run in background as daemon");
                eprintln!("  stop     - Stop running daemon");
                eprintln!("  restart  - Restart daemon");
                process::exit(0);
            }
            other => {
                eprintln!("unknown arg: {other} (try --help)");
                process::exit(1);
            }
        }
    }
    let pid_file = "novarocks.pid";
    let log_file = "novarocks.log";

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

            let stdout = File::create(log_file).expect("create log file");
            let stderr = stdout.try_clone().expect("clone log file handle");

            let cfg = match config_path.as_deref() {
                Some(p) => novarocks_config::init_from_path(p).expect("load novarocks config"),
                None => {
                    novarocks_config::init_from_env_or_default().expect("load novarocks config")
                }
            };
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
                let options = novarocks::cache::block_cache::BlockCacheOptions {
                    capacity: cfg.runtime.cache.datacache_disk_size,
                    block_size: cfg.runtime.cache.datacache_block_size,
                    enable_checksum: cfg.runtime.cache.datacache_checksum_enable,
                    direct_io: cfg.runtime.cache.datacache_direct_io_enable,
                    io_align_unit_size: cfg.runtime.cache.datacache_io_align_unit_size,
                    ..Default::default()
                };
                novarocks::cache::block_cache::init_block_cache(
                    &cfg.runtime.cache.datacache_disk_path,
                    options,
                )
                .expect("initialize block cache");
                eprintln!(
                    "Block cache initialized: path={}, capacity={}, block_size={}, checksum={}, direct_io={}, align_unit={}",
                    cfg.runtime.cache.datacache_disk_path,
                    cfg.runtime.cache.datacache_disk_size,
                    cfg.runtime.cache.datacache_block_size,
                    cfg.runtime.cache.datacache_checksum_enable,
                    cfg.runtime.cache.datacache_direct_io_enable,
                    cfg.runtime.cache.datacache_io_align_unit_size
                );
            }

            let log_level_num = match cfg.log_level.as_str() {
                "trace" | "debug" => 0,
                "info" => 0,
                "warn" => 1,
                "error" => 2,
                _ => 0,
            };

            let server = &cfg.server;

            // Start NovaRocks gRPC servers first to guarantee Starlet endpoint is online
            // before heartbeat reports ports to FE.
            novarocks::start_grpc_server(server.host.as_str()).expect("start grpc server");

            // Start Rust heartbeat service
            let heartbeat_cfg = novarocks::service::heartbeat_service::HeartbeatConfig {
                host: server.host.clone(),
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

            // Start C++ brpc service (for query execution)
            let compat_cfg = novarocks::service::compat::CompatConfig {
                host: server.host.as_str(),
                heartbeat_port: server.heartbeat_port,
                brpc_port: server.brpc_port,
                debug_exec_batch_plan_json: cfg.debug.exec_batch_plan_json,
                log_level: log_level_num,
            };
            novarocks::service::compat::start(&compat_cfg).expect("start compat");

            println!(
                "novarocksd started (host={}, heartbeat_port={}, be_port={}, brpc_port={}, http_port={}, starlet_port={})",
                compat_cfg.host,
                compat_cfg.heartbeat_port,
                server.be_port,
                compat_cfg.brpc_port,
                server.http_port,
                server.starlet_port
            );
            println!("Press Ctrl-C to stop...");

            // Keep process alive until Ctrl-C or signal
            while running.load(Ordering::SeqCst) {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }

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
            let stdout = File::create(log_file).expect("create log file");
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
                    process::exit(1);
                }
            }
        }
        _ => {
            eprintln!("Usage: novarocks [start|stop|restart|run]");
            process::exit(1);
        }
    }
}
