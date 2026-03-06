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
use std::io::ErrorKind;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime};

use threadpool::ThreadPool;
use thrift::protocol::{
    TBinaryInputProtocolFactory, TBinaryOutputProtocolFactory, TInputProtocolFactory,
    TOutputProtocolFactory,
};
use thrift::server::TProcessor;
use thrift::transport::{
    TBufferedReadTransportFactory, TBufferedWriteTransportFactory, TIoChannel,
    TReadTransportFactory, TWriteTransportFactory,
};

use crate::runtime::backend_id as backend_id_store;
use crate::{
    heartbeat_service::{
        HeartbeatServiceSyncHandler, HeartbeatServiceSyncProcessor, TBackendInfo, THeartbeatResult,
        TMasterInfo,
    },
    service::disk_report,
    status::TStatus,
    status_code::TStatusCode,
};

/// Configuration for the heartbeat service
#[derive(Debug, Clone)]
pub struct HeartbeatConfig {
    pub host: String,
    pub heartbeat_port: u16,
    pub be_port: u16,
    pub brpc_port: u16,
    pub http_port: u16,
    pub starlet_port: u16,
}

#[derive(Default)]
struct HeartbeatServerState {
    started: bool,
    stop: Option<Arc<AtomicBool>>,
    join_handle: Option<JoinHandle<()>>,
    wake_addr: Option<String>,
}

fn heartbeat_server_state() -> &'static Mutex<HeartbeatServerState> {
    static STATE: OnceLock<Mutex<HeartbeatServerState>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(HeartbeatServerState::default()))
}

fn shutdown_probe_host(bind_host: &str) -> String {
    match bind_host {
        "" | "0.0.0.0" => "127.0.0.1".to_string(),
        "::" | "[::]" => "::1".to_string(),
        other => other.to_string(),
    }
}

#[derive(Clone)]
struct HeartbeatHandler {
    config: HeartbeatConfig,
    start_time: SystemTime,
}

impl HeartbeatHandler {
    fn new(config: HeartbeatConfig) -> Self {
        Self {
            config,
            start_time: SystemTime::now(),
        }
    }
}

impl HeartbeatServiceSyncHandler for HeartbeatHandler {
    fn handle_heartbeat(&self, master_info: TMasterInfo) -> thrift::Result<THeartbeatResult> {
        // This is the real StarRocks FE -> BE heartbeat RPC payload.
        // Log only non-sensitive fields (avoid printing token).
        tracing::debug!(
            fe_host = %master_info.network_address.hostname,
            fe_port = master_info.network_address.port,
            epoch = master_info.epoch,
            backend_id = ?master_info.backend_id,
            backend_ip = ?master_info.backend_ip,
            http_port = ?master_info.http_port,
            run_mode = ?master_info.run_mode,
            node_type = ?master_info.node_type,
            heartbeat_flags = ?master_info.heartbeat_flags,
            min_active_txn_id = ?master_info.min_active_txn_id,
            encrypted = ?master_info.encrypted,
            "Received HeartbeatService.heartbeat"
        );
        if let Some(id) = master_info.backend_id {
            // Row position uses backend_id as row_source_id; missing it breaks fetch routing.
            backend_id_store::set_backend_id(id);
        }
        let mut backend_host = master_info
            .backend_ip
            .clone()
            .unwrap_or_else(|| self.config.host.clone());
        if backend_host.trim().is_empty() || backend_host == "0.0.0.0" {
            backend_host = "127.0.0.1".to_string();
        }
        disk_report::maybe_report_disks(
            &master_info.network_address,
            backend_host,
            self.config.be_port,
            self.config.http_port,
            master_info.http_port,
        );

        let status = TStatus::new(TStatusCode::OK, None);

        let reboot_time = self
            .start_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let num_cores = thread::available_parallelism()
            .map(|n| n.get() as i32)
            .unwrap_or(1);

        let backend_info = TBackendInfo::new(
            self.config.be_port as i32,
            self.config.http_port as i32,
            Some(self.config.brpc_port as i32),
            Some(self.config.brpc_port as i32),
            Some("novarocks".to_string()),
            Some(num_cores),
            Some(self.config.starlet_port as i32),
            Some(reboot_time),
            Some(true),
            None,
            None,
        );

        tracing::debug!("Heartbeat response: reboot_time={}", reboot_time);

        Ok(THeartbeatResult::new(status, backend_info))
    }
}

/// Starts the heartbeat Thrift server in a background thread
pub fn start_heartbeat_server(config: HeartbeatConfig) -> Result<(), String> {
    let host = if config.host.is_empty() {
        "0.0.0.0".to_string()
    } else {
        config.host.clone()
    };

    let addr = format!("{}:{}", host, config.heartbeat_port);
    let addr_for_log = addr.clone();

    tracing::info!(
        "Starting heartbeat service on {} (brpc_port={}, starlet_port={})",
        addr,
        config.brpc_port,
        config.starlet_port
    );

    let listener = TcpListener::bind(&addr).map_err(|e| format!("Heartbeat bind error: {e}"))?;
    listener
        .set_nonblocking(true)
        .map_err(|e| format!("Heartbeat set_nonblocking failed: {e}"))?;

    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_thread = Arc::clone(&stop);
    let wake_addr = format!("{}:{}", shutdown_probe_host(&host), config.heartbeat_port);
    let handler = HeartbeatHandler::new(config);

    let join_handle = thread::Builder::new()
        .name("heartbeat-server".to_string())
        .spawn(move || {
            tracing::info!("Heartbeat service listening on {}", addr_for_log);
            let worker_pool = ThreadPool::with_name("Heartbeat processor".to_owned(), 4);

            while !stop_for_thread.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((s, _)) => {
                        if stop_for_thread.load(Ordering::Acquire) {
                            break;
                        }
                        let handler = handler.clone();
                        worker_pool.execute(move || handle_heartbeat_stream(s, handler));
                    }
                    Err(e) if e.kind() == ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(50));
                    }
                    Err(e) => {
                        if !stop_for_thread.load(Ordering::Acquire) {
                            tracing::error!("Heartbeat server accept error: {}", e);
                        }
                    }
                }
            }
        })
        .map_err(|e| format!("Failed to spawn heartbeat thread: {}", e))?;

    let mut state = heartbeat_server_state()
        .lock()
        .map_err(|_| "lock heartbeat service state failed".to_string())?;
    if state.started {
        return Ok(());
    }
    state.started = true;
    state.stop = Some(stop);
    state.join_handle = Some(join_handle);
    state.wake_addr = Some(wake_addr);

    Ok(())
}

fn handle_heartbeat_stream(stream: TcpStream, handler: HeartbeatHandler) {
    let channel = thrift::transport::TTcpChannel::with_stream(stream);
    let (r_chan, w_chan) = match channel.split() {
        Ok(v) => v,
        Err(e) => {
            tracing::debug!("Heartbeat split channel failed: {}", e);
            return;
        }
    };
    let r_tran = TBufferedReadTransportFactory::new().create(Box::new(r_chan));
    let mut i_prot = TBinaryInputProtocolFactory::new().create(r_tran);
    let w_tran = TBufferedWriteTransportFactory::new().create(Box::new(w_chan));
    let mut o_prot = TBinaryOutputProtocolFactory::new().create(w_tran);
    let processor = HeartbeatServiceSyncProcessor::new(handler);

    loop {
        match processor.process(&mut *i_prot, &mut *o_prot) {
            Ok(()) => {}
            Err(thrift::Error::Transport(ref te))
                if te.kind == thrift::TransportErrorKind::EndOfFile =>
            {
                break;
            }
            Err(other) => {
                tracing::debug!("Heartbeat processor error: {:?}", other);
                break;
            }
        }
    }
}

pub fn stop_heartbeat_server() {
    let (stop, wake_addr, join_handle) = {
        let mut state = match heartbeat_server_state().lock() {
            Ok(guard) => guard,
            Err(_) => return,
        };
        if !state.started {
            return;
        }
        state.started = false;
        (
            state.stop.take(),
            state.wake_addr.take(),
            state.join_handle.take(),
        )
    };

    if let Some(stop) = stop {
        stop.store(true, Ordering::Release);
    }
    if let Some(addr) = wake_addr {
        let _ = TcpStream::connect(addr);
    }
    if let Some(handle) = join_handle {
        let _ = handle.join();
    }
}

#[cfg(test)]
mod tests {
    use std::net::{TcpListener, TcpStream};
    use std::sync::{Mutex, OnceLock};
    use std::thread;
    use std::time::{Duration, Instant};

    use super::{HeartbeatConfig, start_heartbeat_server, stop_heartbeat_server};

    fn heartbeat_test_guard() -> &'static Mutex<()> {
        static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
        GUARD.get_or_init(|| Mutex::new(()))
    }

    fn pick_free_port() -> u16 {
        TcpListener::bind("127.0.0.1:0")
            .expect("bind ephemeral port")
            .local_addr()
            .expect("get local addr")
            .port()
    }

    fn wait_for_connectable(addr: &str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match TcpStream::connect(addr) {
                Ok(_) => return,
                Err(err) if Instant::now() < deadline => {
                    let _ = err;
                    thread::sleep(Duration::from_millis(25));
                }
                Err(err) => panic!("timed out waiting for {addr} to accept connections: {err}"),
            }
        }
    }

    fn wait_for_bindable(addr: &str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match TcpListener::bind(addr) {
                Ok(listener) => {
                    drop(listener);
                    return;
                }
                Err(err) if Instant::now() < deadline => {
                    let _ = err;
                    thread::sleep(Duration::from_millis(25));
                }
                Err(err) => panic!("timed out waiting for {addr} to become bindable: {err}"),
            }
        }
    }

    #[test]
    fn heartbeat_service_can_stop_and_restart_on_same_port() {
        let _guard = heartbeat_test_guard()
            .lock()
            .expect("lock heartbeat test guard");
        stop_heartbeat_server();

        let port = pick_free_port();
        let addr = format!("127.0.0.1:{port}");
        let config = HeartbeatConfig {
            host: "127.0.0.1".to_string(),
            heartbeat_port: port,
            be_port: 1,
            brpc_port: 2,
            http_port: 3,
            starlet_port: 4,
        };

        start_heartbeat_server(config.clone()).expect("start heartbeat server first time");
        wait_for_connectable(&addr);

        stop_heartbeat_server();
        wait_for_bindable(&addr);

        start_heartbeat_server(config).expect("start heartbeat server second time");
        wait_for_connectable(&addr);

        stop_heartbeat_server();
        wait_for_bindable(&addr);
    }
}
