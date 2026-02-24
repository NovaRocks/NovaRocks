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
use std::collections::BTreeMap;
use std::ffi::CString;
use std::hash::{Hash, Hasher};
use std::net::{SocketAddr, TcpStream};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::transport::{TBufferedReadTransport, TBufferedWriteTransport, TIoChannel, TTcpChannel};

use crate::frontend_service::{FrontendServiceSyncClient, TFrontendServiceSyncClient};
use crate::novarocks_logging::{debug, warn};
use crate::{master_service, status_code, types};

#[derive(Debug, Default)]
struct ReportState {
    fe_addr: Option<types::TNetworkAddress>,
    fe_http_port: Option<i32>,
    in_flight: bool,
    reported: bool,
}

fn state() -> &'static Mutex<ReportState> {
    static STATE: OnceLock<Mutex<ReportState>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(ReportState::default()))
}

fn default_storage_path() -> String {
    if let Ok(path) = std::env::var("novarocks_STORAGE_PATH") {
        if !path.trim().is_empty() && std::path::Path::new(&path).exists() {
            return path;
        }
    }
    if let Ok(cwd) = std::env::current_dir() {
        let path = cwd.to_string_lossy().to_string();
        if std::path::Path::new(&path).exists() {
            return path;
        }
    }
    "/".to_string()
}

fn stat_capacity_bytes(path: &str) -> Option<(u64, u64)> {
    let c_path = CString::new(path).ok()?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
    if rc != 0 {
        return None;
    }
    let block_size = stat.f_frsize as u64;
    let total = stat.f_blocks as u64 * block_size;
    let available = stat.f_bavail as u64 * block_size;
    Some((total, available))
}

fn hash_path(path: &str) -> i64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    path.hash(&mut hasher);
    hasher.finish() as i64
}

fn send_report(
    fe_addr: &types::TNetworkAddress,
    backend_host: String,
    be_port: u16,
    http_port: u16,
) -> Result<(), String> {
    let addr: SocketAddr = format!("{}:{}", fe_addr.hostname, fe_addr.port)
        .parse()
        .map_err(|e| format!("invalid FE address: {e}"))?;
    let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5))
        .map_err(|e| format!("connect FE failed: {e}"))?;
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_nodelay(true);

    let channel = TTcpChannel::with_stream(stream);
    let (i_chan, o_chan) = channel
        .split()
        .map_err(|e| format!("split thrift channel failed: {e}"))?;
    let i_trans = TBufferedReadTransport::new(i_chan);
    let o_trans = TBufferedWriteTransport::new(o_chan);
    let i_prot = TBinaryInputProtocol::new(i_trans, true);
    let o_prot = TBinaryOutputProtocol::new(o_trans, true);

    let backend = types::TBackend::new(backend_host, be_port as i32, http_port as i32);

    let root_path = default_storage_path();
    let (total, available) = stat_capacity_bytes(&root_path).unwrap_or((1_u64 << 40, 1_u64 << 40));
    let used = total.saturating_sub(available);

    let disk = master_service::TDisk::new(
        root_path.clone(),
        total as i64,
        used as i64,
        true,
        Some(available as i64),
        Some(hash_path(&root_path)),
        Some(types::TStorageMedium::HDD),
    );

    let mut disks = BTreeMap::new();
    disks.insert(root_path, disk);

    let request = master_service::TReportRequest::new(
        backend,
        Some(0),
        None,
        None,
        Some(disks),
        None,
        None,
        None,
        None,
        None,
        None,
    );

    let mut client = FrontendServiceSyncClient::new(i_prot, o_prot);
    let result = client.report(request).map_err(|e| e.to_string())?;
    if result.status.status_code != status_code::TStatusCode::OK {
        return Err(format!("FE returned error: {:?}", result.status));
    }
    Ok(())
}

pub(crate) fn maybe_report_disks(
    fe_addr: &types::TNetworkAddress,
    backend_host: String,
    be_port: u16,
    http_port: u16,
    fe_http_port: Option<i32>,
) {
    let mut guard = state().lock().expect("disk report state lock");
    if let Some(port) = fe_http_port.filter(|port| *port > 0) {
        guard.fe_http_port = Some(port);
    }
    if guard
        .fe_addr
        .as_ref()
        .is_some_and(|addr| addr.hostname == fe_addr.hostname && addr.port == fe_addr.port)
    {
        if guard.reported || guard.in_flight {
            return;
        }
    } else {
        guard.fe_addr = Some(fe_addr.clone());
        guard.reported = false;
        guard.in_flight = false;
    }

    if guard.in_flight {
        return;
    }
    guard.in_flight = true;
    let fe_addr = fe_addr.clone();
    drop(guard);

    std::thread::spawn(move || {
        let result = send_report(&fe_addr, backend_host, be_port, http_port);
        let mut guard = state().lock().expect("disk report state lock");
        guard.in_flight = false;
        guard.reported = result.is_ok();
        if let Err(err) = result {
            warn!("failed to report disks to FE: {}", err);
        } else {
            debug!("reported disk info to FE");
        }
    });
}

pub(crate) fn latest_fe_addr() -> Option<types::TNetworkAddress> {
    let guard = state().lock().ok()?;
    guard.fe_addr.clone()
}
