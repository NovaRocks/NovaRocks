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
use std::thread;
use std::time::SystemTime;

use thrift::protocol::{TBinaryInputProtocolFactory, TBinaryOutputProtocolFactory};
use thrift::server::TServer;
use thrift::transport::{TBufferedReadTransportFactory, TBufferedWriteTransportFactory};

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

        // TPort is i32 type alias, not a struct
        let backend_info = TBackendInfo::new(
            self.config.be_port as i32,
            self.config.http_port as i32,
            Some(self.config.brpc_port as i32),
            Some(self.config.brpc_port as i32),
            Some("novarocks".to_string()),
            Some(num_cores),
            Some(self.config.starlet_port as i32),
            Some(reboot_time),
            Some(true), // is_set_storage_path
            None,       // mem_limit_bytes
            None,       // arrow_flight_port
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

    // Create handler and processor
    let handler = HeartbeatHandler::new(config);
    let processor = HeartbeatServiceSyncProcessor::new(handler);

    // Create server
    let mut server = TServer::new(
        TBufferedReadTransportFactory::new(),
        TBinaryInputProtocolFactory::new(),
        TBufferedWriteTransportFactory::new(),
        TBinaryOutputProtocolFactory::new(),
        processor,
        4, // num_workers
    );

    // Start server in background thread
    thread::Builder::new()
        .name("heartbeat-server".to_string())
        .spawn(move || {
            tracing::info!("Heartbeat service listening on {}", addr_for_log);
            if let Err(e) = server.listen(&addr) {
                tracing::error!("Heartbeat server error: {}", e);
            }
        })
        .map_err(|e| format!("Failed to spawn heartbeat thread: {}", e))?;

    Ok(())
}
