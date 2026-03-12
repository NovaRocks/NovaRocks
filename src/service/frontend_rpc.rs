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

use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::transport::{
    ReadHalf as TReadHalf, TBufferedReadTransport, TBufferedWriteTransport, TIoChannel,
    TTcpChannel, WriteHalf as TWriteHalf,
};

use crate::common::config;
use crate::frontend_service::{FrontendServiceSyncClient, TFrontendServiceSyncClient};
use crate::novarocks_logging::{debug, info, warn};
use crate::types;

type FrontendRpcClientInner = FrontendServiceSyncClient<
    TBinaryInputProtocol<TBufferedReadTransport<TReadHalf<TTcpChannel>>>,
    TBinaryOutputProtocol<TBufferedWriteTransport<TWriteHalf<TTcpChannel>>>,
>;

const FE_RPC_HOST_BACKOFF_MS: u64 = 500;
const FE_RPC_LOW_NOFILE_WARNING: u64 = 8_192;

#[derive(Clone, Copy, Debug)]
pub(crate) struct FrontendRpcCallOptions {
    pub(crate) transport_retries: usize,
}

impl Default for FrontendRpcCallOptions {
    fn default() -> Self {
        Self {
            transport_retries: 1,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) enum FrontendRpcKind {
    SchemaMeta,
    ExecStatus,
    Control,
    SchemaQuery,
}

impl Display for FrontendRpcKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FrontendRpcKind::SchemaMeta => write!(f, "SchemaMeta"),
            FrontendRpcKind::ExecStatus => write!(f, "ExecStatus"),
            FrontendRpcKind::Control => write!(f, "Control"),
            FrontendRpcKind::SchemaQuery => write!(f, "SchemaQuery"),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct FrontendRpcError {
    message: String,
    transport: bool,
    emfile: bool,
}

impl FrontendRpcError {
    pub(crate) fn application(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            transport: false,
            emfile: false,
        }
    }

    pub(crate) fn transport(message: impl Into<String>) -> Self {
        let message = message.into();
        let emfile = is_emfile_message(&message);
        Self {
            message,
            transport: true,
            emfile,
        }
    }

    pub(crate) fn from_thrift(err: thrift::Error) -> Self {
        match err {
            thrift::Error::Transport(transport) => Self::transport(transport.to_string()),
            thrift::Error::Protocol(protocol) => Self::transport(protocol.to_string()),
            thrift::Error::Application(app) => Self::application(app.to_string()),
            thrift::Error::User(user) => {
                let message = user.to_string();
                if looks_like_transport_message(&message) {
                    Self::transport(message)
                } else {
                    Self::application(message)
                }
            }
        }
    }

    pub(crate) fn from_message_guess(message: impl Into<String>) -> Self {
        let message = message.into();
        if looks_like_transport_message(&message) {
            Self::transport(message)
        } else {
            Self::application(message)
        }
    }

    pub(crate) fn is_transport(&self) -> bool {
        self.transport
    }

    pub(crate) fn is_emfile(&self) -> bool {
        self.emfile
    }

    fn should_backoff_host(&self) -> bool {
        self.transport || looks_like_backoff_message(&self.message)
    }
}

impl Display for FrontendRpcError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for FrontendRpcError {}

#[derive(Clone, Copy, Debug)]
struct FrontendRpcSettings {
    connect_timeout: Duration,
    rpc_timeout: Duration,
    retry_interval: Duration,
    host_backoff: Duration,
    max_idle_per_host: usize,
    max_inflight_total: usize,
    max_inflight_schema: usize,
    max_inflight_exec_status: usize,
    max_inflight_control: usize,
    max_inflight_schema_query: usize,
}

impl FrontendRpcSettings {
    fn from_config() -> Self {
        Self {
            connect_timeout: Duration::from_millis(config::fe_rpc_connect_timeout_ms()),
            rpc_timeout: Duration::from_millis(config::fe_rpc_timeout_ms()),
            retry_interval: Duration::from_millis(config::fe_rpc_retry_interval_ms()),
            host_backoff: Duration::from_millis(FE_RPC_HOST_BACKOFF_MS),
            max_idle_per_host: config::fe_rpc_pool_max_idle_per_host(),
            max_inflight_total: config::fe_rpc_max_inflight_total(),
            max_inflight_schema: config::fe_rpc_max_inflight_schema(),
            max_inflight_exec_status: config::fe_rpc_max_inflight_exec_status(),
            max_inflight_control: config::fe_rpc_max_inflight_control(),
            max_inflight_schema_query: config::fe_rpc_max_inflight_schema_query(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct NoFileLimit {
    soft: Option<u64>,
    hard: Option<u64>,
}

#[derive(Default)]
struct HostClientPool {
    idle: Vec<FrontendRpcClientInner>,
    backoff_until: Option<Instant>,
}

struct PermitPool {
    max: usize,
    in_use: AtomicUsize,
    state: Mutex<usize>,
    cv: Condvar,
}

impl PermitPool {
    fn new(max: usize) -> Self {
        Self {
            max: max.max(1),
            in_use: AtomicUsize::new(0),
            state: Mutex::new(0),
            cv: Condvar::new(),
        }
    }

    fn acquire(&self) -> PermitGuard<'_> {
        let mut used = self.state.lock().expect("permit pool lock");
        while *used >= self.max {
            used = self.cv.wait(used).expect("permit pool wait");
        }
        *used += 1;
        self.in_use.fetch_add(1, Ordering::AcqRel);
        PermitGuard { pool: self }
    }

    fn current(&self) -> usize {
        self.in_use.load(Ordering::Acquire)
    }
}

struct PermitGuard<'a> {
    pool: &'a PermitPool,
}

impl Drop for PermitGuard<'_> {
    fn drop(&mut self) {
        self.pool.in_use.fetch_sub(1, Ordering::AcqRel);
        let mut used = self.pool.state.lock().expect("permit pool lock");
        *used = used.saturating_sub(1);
        self.pool.cv.notify_one();
    }
}

pub(crate) struct FrontendRpcManager {
    settings: FrontendRpcSettings,
    nofile_limit: NoFileLimit,
    host_pools: Mutex<HashMap<String, HostClientPool>>,
    total_permits: PermitPool,
    schema_meta_permits: PermitPool,
    exec_status_permits: PermitPool,
    control_permits: PermitPool,
    schema_query_permits: PermitPool,
}

impl FrontendRpcManager {
    fn new(settings: FrontendRpcSettings) -> Self {
        let nofile_limit = read_nofile_limit();
        log_nofile_limit(nofile_limit);
        Self {
            total_permits: PermitPool::new(settings.max_inflight_total),
            schema_meta_permits: PermitPool::new(settings.max_inflight_schema),
            exec_status_permits: PermitPool::new(settings.max_inflight_exec_status),
            control_permits: PermitPool::new(settings.max_inflight_control),
            schema_query_permits: PermitPool::new(settings.max_inflight_schema_query),
            settings,
            nofile_limit,
            host_pools: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn shared() -> &'static Self {
        static INSTANCE: OnceLock<FrontendRpcManager> = OnceLock::new();
        INSTANCE.get_or_init(|| FrontendRpcManager::new(FrontendRpcSettings::from_config()))
    }

    pub(crate) fn call<T, F>(
        &self,
        rpc_kind: FrontendRpcKind,
        fe_addr: &types::TNetworkAddress,
        f: F,
    ) -> Result<T, FrontendRpcError>
    where
        F: FnMut(&mut dyn TFrontendServiceSyncClient) -> Result<T, FrontendRpcError>,
    {
        self.call_with_options(rpc_kind, fe_addr, FrontendRpcCallOptions::default(), f)
    }

    pub(crate) fn call_with_options<T, F>(
        &self,
        rpc_kind: FrontendRpcKind,
        fe_addr: &types::TNetworkAddress,
        options: FrontendRpcCallOptions,
        mut f: F,
    ) -> Result<T, FrontendRpcError>
    where
        F: FnMut(&mut dyn TFrontendServiceSyncClient) -> Result<T, FrontendRpcError>,
    {
        let _total_permit = self.total_permits.acquire();
        let _kind_permit = self.kind_permit_pool(rpc_kind).acquire();

        let attempts = options.transport_retries.saturating_add(1);
        let host_key = host_key(fe_addr);
        for attempt in 1..=attempts {
            self.wait_host_backoff(rpc_kind, fe_addr, &host_key, attempt);
            let pool_idle_host = self.host_idle_len(&host_key);
            debug!(
                target: "novarocks::fe_rpc",
                event = "rpc_attempt",
                rpc_kind = %rpc_kind,
                fe_addr = %format_addr(fe_addr),
                attempt,
                timeout_ms = self.settings.rpc_timeout.as_millis() as u64,
                inflight_total = self.total_permits.current(),
                inflight_kind = self.kind_permit_pool(rpc_kind).current(),
                pool_idle_host,
                "FE RPC attempt"
            );

            let mut client = match FrontendRpcClient::checkout(self, rpc_kind, fe_addr, &host_key) {
                Ok(client) => client,
                Err(err) => {
                    self.record_transport_error(rpc_kind, fe_addr, &host_key, attempt, &err);
                    if err.is_transport() && attempt < attempts {
                        std::thread::sleep(self.settings.retry_interval);
                        continue;
                    }
                    return Err(err);
                }
            };

            match f(client.client_mut()) {
                Ok(value) => {
                    debug!(
                        target: "novarocks::fe_rpc",
                        event = "rpc_success",
                        rpc_kind = %rpc_kind,
                        fe_addr = %format_addr(fe_addr),
                        attempt,
                        timeout_ms = self.settings.rpc_timeout.as_millis() as u64,
                        inflight_total = self.total_permits.current(),
                        inflight_kind = self.kind_permit_pool(rpc_kind).current(),
                        pool_idle_host = self.host_idle_len(&host_key),
                        "FE RPC succeeded"
                    );
                    return Ok(value);
                }
                Err(err) => {
                    if err.is_transport() {
                        client.mark_broken();
                        self.record_transport_error(rpc_kind, fe_addr, &host_key, attempt, &err);
                        if attempt < attempts {
                            std::thread::sleep(self.settings.retry_interval);
                            continue;
                        }
                    } else {
                        warn!(
                            target: "novarocks::fe_rpc",
                            event = "rpc_error",
                            rpc_kind = %rpc_kind,
                            fe_addr = %format_addr(fe_addr),
                            attempt,
                            timeout_ms = self.settings.rpc_timeout.as_millis() as u64,
                            inflight_total = self.total_permits.current(),
                            inflight_kind = self.kind_permit_pool(rpc_kind).current(),
                            pool_idle_host = self.host_idle_len(&host_key),
                            error = %err,
                            "FE RPC returned application error"
                        );
                    }
                    return Err(err);
                }
            }
        }

        Err(FrontendRpcError::application(
            "FE RPC exhausted attempts without a terminal result",
        ))
    }

    fn kind_permit_pool(&self, rpc_kind: FrontendRpcKind) -> &PermitPool {
        match rpc_kind {
            FrontendRpcKind::SchemaMeta => &self.schema_meta_permits,
            FrontendRpcKind::ExecStatus => &self.exec_status_permits,
            FrontendRpcKind::Control => &self.control_permits,
            FrontendRpcKind::SchemaQuery => &self.schema_query_permits,
        }
    }

    fn take_idle_client(
        &self,
        rpc_kind: FrontendRpcKind,
        fe_addr: &types::TNetworkAddress,
        host_key: &str,
    ) -> Option<FrontendRpcClientInner> {
        let mut guard = self.host_pools.lock().expect("FE host pool lock");
        let pool = guard.get_mut(host_key)?;
        let client = pool.idle.pop()?;
        debug!(
            target: "novarocks::fe_rpc",
            event = "pool_hit",
            rpc_kind = %rpc_kind,
            fe_addr = %format_addr(fe_addr),
            inflight_total = self.total_permits.current(),
            inflight_kind = self.kind_permit_pool(rpc_kind).current(),
            pool_idle_host = pool.idle.len(),
            "Reused idle FE client"
        );
        Some(client)
    }

    fn connect_client(
        &self,
        rpc_kind: FrontendRpcKind,
        fe_addr: &types::TNetworkAddress,
    ) -> Result<FrontendRpcClientInner, FrontendRpcError> {
        let addr: SocketAddr = format_addr(fe_addr)
            .parse()
            .map_err(|e| FrontendRpcError::application(format!("invalid FE address: {e}")))?;
        let stream =
            TcpStream::connect_timeout(&addr, self.settings.connect_timeout).map_err(|e| {
                if e.raw_os_error() == Some(libc::EMFILE) {
                    FrontendRpcError::transport(format!("connect FE failed: {e}"))
                } else {
                    FrontendRpcError::transport(format!("connect FE failed: {e}"))
                }
            })?;
        let _ = stream.set_read_timeout(Some(self.settings.rpc_timeout));
        let _ = stream.set_write_timeout(Some(self.settings.rpc_timeout));
        let _ = stream.set_nodelay(true);

        let channel = TTcpChannel::with_stream(stream);
        let (i_chan, o_chan): (TReadHalf<TTcpChannel>, TWriteHalf<TTcpChannel>) =
            channel.split().map_err(|e| {
                FrontendRpcError::transport(format!("split FE thrift channel failed: {e}"))
            })?;
        let i_trans = TBufferedReadTransport::new(i_chan);
        let o_trans = TBufferedWriteTransport::new(o_chan);
        let i_prot = TBinaryInputProtocol::new(i_trans, true);
        let o_prot = TBinaryOutputProtocol::new(o_trans, true);
        debug!(
            target: "novarocks::fe_rpc",
            event = "pool_create",
            rpc_kind = %rpc_kind,
            fe_addr = %format_addr(fe_addr),
            inflight_total = self.total_permits.current(),
            inflight_kind = self.kind_permit_pool(rpc_kind).current(),
            pool_idle_host = self.host_idle_len(&host_key(fe_addr)),
            "Created new FE client"
        );
        Ok(FrontendServiceSyncClient::new(i_prot, o_prot))
    }

    fn return_client(
        &self,
        rpc_kind: FrontendRpcKind,
        fe_addr: &types::TNetworkAddress,
        host_key: &str,
        broken: bool,
        client: FrontendRpcClientInner,
    ) {
        if broken {
            debug!(
                target: "novarocks::fe_rpc",
                event = "pool_discard_broken",
                rpc_kind = %rpc_kind,
                fe_addr = %format_addr(fe_addr),
                inflight_total = self.total_permits.current(),
                inflight_kind = self.kind_permit_pool(rpc_kind).current(),
                pool_idle_host = self.host_idle_len(host_key),
                "Discarded broken FE client"
            );
            drop(client);
            return;
        }

        let mut guard = self.host_pools.lock().expect("FE host pool lock");
        let pool = guard.entry(host_key.to_string()).or_default();
        if pool.idle.len() >= self.settings.max_idle_per_host {
            debug!(
                target: "novarocks::fe_rpc",
                event = "pool_drop_overflow",
                rpc_kind = %rpc_kind,
                fe_addr = %format_addr(fe_addr),
                inflight_total = self.total_permits.current(),
                inflight_kind = self.kind_permit_pool(rpc_kind).current(),
                pool_idle_host = pool.idle.len(),
                "Dropped FE client because host idle pool is full"
            );
            drop(client);
            return;
        }
        pool.idle.push(client);
        debug!(
            target: "novarocks::fe_rpc",
            event = "pool_return",
            rpc_kind = %rpc_kind,
            fe_addr = %format_addr(fe_addr),
            inflight_total = self.total_permits.current(),
            inflight_kind = self.kind_permit_pool(rpc_kind).current(),
            pool_idle_host = pool.idle.len(),
            "Returned FE client to idle pool"
        );
    }

    fn wait_host_backoff(
        &self,
        rpc_kind: FrontendRpcKind,
        fe_addr: &types::TNetworkAddress,
        host_key: &str,
        attempt: usize,
    ) {
        loop {
            let backoff_for = {
                let guard = self.host_pools.lock().expect("FE host pool lock");
                let Some(pool) = guard.get(host_key) else {
                    return;
                };
                let Some(backoff_until) = pool.backoff_until else {
                    return;
                };
                backoff_until.saturating_duration_since(Instant::now())
            };
            if backoff_for.is_zero() {
                return;
            }
            debug!(
                target: "novarocks::fe_rpc",
                event = "pool_backoff_hit",
                rpc_kind = %rpc_kind,
                fe_addr = %format_addr(fe_addr),
                attempt,
                timeout_ms = self.settings.rpc_timeout.as_millis() as u64,
                inflight_total = self.total_permits.current(),
                inflight_kind = self.kind_permit_pool(rpc_kind).current(),
                pool_idle_host = self.host_idle_len(host_key),
                backoff_ms = backoff_for.as_millis() as u64,
                "Applied FE host backoff"
            );
            std::thread::sleep(backoff_for);
        }
    }

    fn record_transport_error(
        &self,
        rpc_kind: FrontendRpcKind,
        fe_addr: &types::TNetworkAddress,
        host_key: &str,
        attempt: usize,
        err: &FrontendRpcError,
    ) {
        if err.should_backoff_host() {
            let mut guard = self.host_pools.lock().expect("FE host pool lock");
            let pool = guard.entry(host_key.to_string()).or_default();
            pool.backoff_until = Some(Instant::now() + self.settings.host_backoff);
        }
        warn!(
            target: "novarocks::fe_rpc",
            event = "rpc_transport_error",
            rpc_kind = %rpc_kind,
            fe_addr = %format_addr(fe_addr),
            attempt,
            timeout_ms = self.settings.rpc_timeout.as_millis() as u64,
            inflight_total = self.total_permits.current(),
            inflight_kind = self.kind_permit_pool(rpc_kind).current(),
            pool_idle_host = self.host_idle_len(host_key),
            error = %err,
            "FE RPC transport error"
        );
        if err.is_emfile() {
            warn!(
                target: "novarocks::fe_rpc",
                event = "emfile_diagnostics",
                soft_nofile = self.nofile_limit.soft,
                hard_nofile = self.nofile_limit.hard,
                inflight_total = self.total_permits.current(),
                inflight_schema = self.schema_meta_permits.current(),
                inflight_exec_status = self.exec_status_permits.current(),
                inflight_control = self.control_permits.current(),
                inflight_schema_query = self.schema_query_permits.current(),
                "Encountered EMFILE while issuing FE RPC"
            );
        }
    }

    fn host_idle_len(&self, host_key: &str) -> usize {
        self.host_pools
            .lock()
            .ok()
            .and_then(|guard| guard.get(host_key).map(|pool| pool.idle.len()))
            .unwrap_or(0)
    }
}

pub fn init_frontend_rpc_manager() {
    let _ = FrontendRpcManager::shared();
}

#[cfg(test)]
pub(crate) fn test_clear_shared_host_pools() {
    if let Ok(mut guard) = FrontendRpcManager::shared().host_pools.lock() {
        guard.clear();
    }
}

struct FrontendRpcClient<'a> {
    manager: &'a FrontendRpcManager,
    rpc_kind: FrontendRpcKind,
    fe_addr: types::TNetworkAddress,
    host_key: String,
    broken: bool,
    inner: Option<FrontendRpcClientInner>,
}

impl<'a> FrontendRpcClient<'a> {
    fn checkout(
        manager: &'a FrontendRpcManager,
        rpc_kind: FrontendRpcKind,
        fe_addr: &types::TNetworkAddress,
        host_key: &str,
    ) -> Result<Self, FrontendRpcError> {
        let inner = manager
            .take_idle_client(rpc_kind, fe_addr, host_key)
            .map(Ok)
            .unwrap_or_else(|| manager.connect_client(rpc_kind, fe_addr))?;
        Ok(Self {
            manager,
            rpc_kind,
            fe_addr: fe_addr.clone(),
            host_key: host_key.to_string(),
            broken: false,
            inner: Some(inner),
        })
    }

    fn client_mut(&mut self) -> &mut dyn TFrontendServiceSyncClient {
        self.inner
            .as_mut()
            .expect("frontend rpc client is present for the request lifetime")
    }

    fn mark_broken(&mut self) {
        self.broken = true;
    }
}

impl Drop for FrontendRpcClient<'_> {
    fn drop(&mut self) {
        let Some(client) = self.inner.take() else {
            return;
        };
        self.manager.return_client(
            self.rpc_kind,
            &self.fe_addr,
            &self.host_key,
            self.broken,
            client,
        );
    }
}

fn host_key(fe_addr: &types::TNetworkAddress) -> String {
    format_addr(fe_addr)
}

fn format_addr(fe_addr: &types::TNetworkAddress) -> String {
    format!("{}:{}", fe_addr.hostname, fe_addr.port)
}

fn looks_like_transport_message(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("transport")
        || lower.contains("broken pipe")
        || lower.contains("connection reset")
        || lower.contains("connection refused")
        || lower.contains("unexpected eof")
        || lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("too many open files")
}

fn looks_like_backoff_message(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("connection reset") || lower.contains("too many open files")
}

fn is_emfile_message(message: &str) -> bool {
    message.to_ascii_lowercase().contains("too many open files")
}

fn read_nofile_limit() -> NoFileLimit {
    #[cfg(unix)]
    {
        let mut limit: libc::rlimit = unsafe { std::mem::zeroed() };
        let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) };
        if rc == 0 {
            return NoFileLimit {
                soft: Some(limit.rlim_cur),
                hard: Some(limit.rlim_max),
            };
        }
    }
    NoFileLimit::default()
}

fn log_nofile_limit(limit: NoFileLimit) {
    info!(
        target: "novarocks::fe_rpc",
        event = "startup_nofile_limit",
        soft_nofile = limit.soft,
        hard_nofile = limit.hard,
        "Recorded RLIMIT_NOFILE for FE RPC manager"
    );
    if limit
        .soft
        .is_some_and(|soft| soft < FE_RPC_LOW_NOFILE_WARNING)
    {
        warn!(
            target: "novarocks::fe_rpc",
            event = "low_nofile_warning",
            soft_nofile = limit.soft,
            hard_nofile = limit.hard,
            required_soft_nofile = FE_RPC_LOW_NOFILE_WARNING,
            "RLIMIT_NOFILE soft limit is lower than the recommended FE RPC floor"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use crate::descriptors;
    use crate::frontend_service;
    use crate::status;
    use crate::status_code;
    use crate::types;

    use super::{FrontendRpcError, FrontendRpcKind, FrontendRpcManager, FrontendRpcSettings};
    mod fe_rpc_server {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/common/fe_rpc_server.rs"
        ));
    }
    use fe_rpc_server::{FakeFeRpcServer, ServerAction, read_struct_arg, write_struct_reply};

    fn test_manager_settings() -> FrontendRpcSettings {
        FrontendRpcSettings {
            connect_timeout: Duration::from_secs(1),
            rpc_timeout: Duration::from_secs(1),
            retry_interval: Duration::from_millis(20),
            host_backoff: Duration::from_millis(50),
            max_idle_per_host: 10,
            max_inflight_total: 32,
            max_inflight_schema: 8,
            max_inflight_exec_status: 4,
            max_inflight_control: 4,
            max_inflight_schema_query: 4,
        }
    }

    fn ok_status() -> status::TStatus {
        status::TStatus::new(status_code::TStatusCode::OK, None)
    }

    fn test_tablet_schema(schema_id: i64) -> crate::agent_service::TTabletSchema {
        crate::agent_service::TTabletSchema {
            short_key_column_count: 1,
            schema_hash: 1001,
            keys_type: types::TKeysType::DUP_KEYS,
            storage_type: types::TStorageType::COLUMN,
            columns: vec![descriptors::TColumn {
                column_name: "c1".to_string(),
                column_type: Some(types::TColumnType {
                    type_: types::TPrimitiveType::BIGINT,
                    len: Some(8),
                    index_len: Some(8),
                    precision: None,
                    scale: None,
                }),
                aggregation_type: None,
                is_key: Some(true),
                is_allow_null: Some(false),
                default_value: Some("1".to_string()),
                is_bloom_filter_column: Some(false),
                define_expr: None,
                is_auto_increment: Some(false),
                col_unique_id: Some(1),
                has_bitmap_index: Some(false),
                agg_state_desc: None,
                index_len: Some(8),
                type_desc: None,
            }],
            bloom_filter_fpp: None,
            indexes: None,
            is_in_memory: Some(false),
            id: Some(schema_id),
            sort_key_idxes: None,
            sort_key_unique_ids: None,
            schema_version: Some(0),
            compression_type: Some(types::TCompressionType::LZ4_FRAME),
            compression_level: None,
        }
    }

    #[test]
    fn reuses_idle_clients_across_requests() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_server = Arc::clone(&calls);
        let server = FakeFeRpcServer::start(
            0,
            Box::new(move |method, seq, i_prot, o_prot| match method {
                "getQueryProfile" => {
                    let _req: frontend_service::TGetProfileRequest = read_struct_arg(i_prot)?;
                    calls_for_server.fetch_add(1, Ordering::AcqRel);
                    let response = frontend_service::TGetProfileResponse {
                        status: Some(ok_status()),
                        query_result: Some(vec!["profile".to_string()]),
                    };
                    write_struct_reply(o_prot, method, seq, &response)?;
                    Ok(ServerAction::Continue)
                }
                other => panic!("unexpected FE RPC method: {other}"),
            }),
        );
        let manager = FrontendRpcManager::new(test_manager_settings());
        let request = frontend_service::TGetProfileRequest::new(Some(vec!["q".to_string()]));

        let first = manager
            .call(FrontendRpcKind::SchemaQuery, server.addr(), |client| {
                client
                    .get_query_profile(request.clone())
                    .map_err(FrontendRpcError::from_thrift)
            })
            .expect("first getQueryProfile succeeds");
        assert_eq!(manager.host_idle_len(&super::host_key(server.addr())), 1);
        let second = manager
            .call(FrontendRpcKind::SchemaQuery, server.addr(), |client| {
                client
                    .get_query_profile(request.clone())
                    .map_err(FrontendRpcError::from_thrift)
            })
            .expect("second getQueryProfile succeeds");

        assert_eq!(first.query_result, Some(vec!["profile".to_string()]));
        assert_eq!(second.query_result, Some(vec!["profile".to_string()]));
        assert_eq!(calls.load(Ordering::Acquire), 2);
        assert_eq!(manager.host_idle_len(&super::host_key(server.addr())), 1);
    }

    #[test]
    fn drops_broken_client_after_transport_error() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_server = Arc::clone(&calls);
        let server = FakeFeRpcServer::start(
            1,
            Box::new(move |method, seq, i_prot, o_prot| match method {
                "getQueryProfile" => {
                    let _req: frontend_service::TGetProfileRequest = read_struct_arg(i_prot)?;
                    calls_for_server.fetch_add(1, Ordering::AcqRel);
                    let response = frontend_service::TGetProfileResponse {
                        status: Some(ok_status()),
                        query_result: Some(vec!["profile".to_string()]),
                    };
                    write_struct_reply(o_prot, method, seq, &response)?;
                    Ok(ServerAction::Continue)
                }
                other => panic!("unexpected FE RPC method: {other}"),
            }),
        );
        let manager = FrontendRpcManager::new(test_manager_settings());
        let request = frontend_service::TGetProfileRequest::new(Some(vec!["q".to_string()]));

        manager
            .call(FrontendRpcKind::SchemaQuery, server.addr(), |client| {
                client
                    .get_query_profile(request.clone())
                    .map_err(FrontendRpcError::from_thrift)
            })
            .expect("first request reconnects after transport error");
        manager
            .call(FrontendRpcKind::SchemaQuery, server.addr(), |client| {
                client
                    .get_query_profile(request.clone())
                    .map_err(FrontendRpcError::from_thrift)
            })
            .expect("second request reuses healthy client");

        assert_eq!(calls.load(Ordering::Acquire), 2);
        assert!(server.accepts() >= 2);
        assert_eq!(manager.host_idle_len(&super::host_key(server.addr())), 1);
    }

    #[test]
    fn control_permit_is_not_starved_by_schema_requests() {
        let schema_started = Arc::new(AtomicBool::new(false));
        let release_schema = Arc::new(AtomicBool::new(false));
        let control_calls = Arc::new(AtomicUsize::new(0));
        let schema_started_for_server = Arc::clone(&schema_started);
        let release_schema_for_server = Arc::clone(&release_schema);
        let control_calls_for_server = Arc::clone(&control_calls);
        let server = FakeFeRpcServer::start(
            0,
            Box::new(move |method, seq, i_prot, o_prot| match method {
                "getTableSchema" => {
                    let _req: frontend_service::TBatchGetTableSchemaRequest =
                        read_struct_arg(i_prot)?;
                    schema_started_for_server.store(true, Ordering::Release);
                    while !release_schema_for_server.load(Ordering::Acquire) {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    let response = frontend_service::TBatchGetTableSchemaResponse {
                        status: Some(ok_status()),
                        responses: Some(vec![frontend_service::TGetTableSchemaResponse {
                            status: Some(ok_status()),
                            schema: Some(test_tablet_schema(101)),
                        }]),
                    };
                    write_struct_reply(o_prot, method, seq, &response)?;
                    Ok(ServerAction::Continue)
                }
                "createPartition" => {
                    let _req: frontend_service::TCreatePartitionRequest = read_struct_arg(i_prot)?;
                    control_calls_for_server.fetch_add(1, Ordering::AcqRel);
                    let response = frontend_service::TCreatePartitionResult {
                        status: Some(ok_status()),
                        partitions: None,
                        tablets: None,
                        nodes: None,
                    };
                    write_struct_reply(o_prot, method, seq, &response)?;
                    Ok(ServerAction::Continue)
                }
                other => panic!("unexpected FE RPC method: {other}"),
            }),
        );
        let manager = Arc::new(FrontendRpcManager::new(FrontendRpcSettings {
            max_inflight_total: 2,
            max_inflight_schema: 1,
            max_inflight_exec_status: 1,
            max_inflight_control: 1,
            max_inflight_schema_query: 1,
            ..test_manager_settings()
        }));
        let schema_request = frontend_service::TBatchGetTableSchemaRequest {
            requests: Some(vec![frontend_service::TGetTableSchemaRequest {
                schema_meta: Some(crate::plan_nodes::TTableSchemaMeta {
                    db_id: Some(1),
                    table_id: Some(2),
                    schema_id: Some(3),
                }),
                source: Some(frontend_service::TTableSchemaRequestSource::SCAN),
                tablet_id: None,
                query_id: Some(types::TUniqueId::new(11, 22)),
                txn_id: None,
            }]),
        };
        let control_request = frontend_service::TCreatePartitionRequest::new(
            Some(10),
            Some(1),
            Some(2),
            Some(vec![vec!["2025-01-01".to_string()]]),
            Some(false),
        );

        let server_addr = server.addr().clone();
        let manager_for_schema = Arc::clone(&manager);
        let schema_handle = std::thread::spawn(move || {
            manager_for_schema
                .call(FrontendRpcKind::SchemaMeta, &server_addr, |client| {
                    client
                        .get_table_schema(schema_request.clone())
                        .map_err(FrontendRpcError::from_thrift)
                })
                .expect("schema request succeeds");
        });

        let start = Instant::now();
        while !schema_started.load(Ordering::Acquire) {
            assert!(start.elapsed() < Duration::from_secs(1));
            std::thread::sleep(Duration::from_millis(10));
        }

        let control_response = manager
            .call(FrontendRpcKind::Control, server.addr(), |client| {
                client
                    .create_partition(control_request.clone())
                    .map_err(FrontendRpcError::from_thrift)
            })
            .expect("control request obtains its own permit");
        assert_eq!(control_response.status, Some(ok_status()));
        assert_eq!(control_calls.load(Ordering::Acquire), 1);

        release_schema.store(true, Ordering::Release);
        schema_handle.join().expect("join schema request");
    }

    #[test]
    fn drops_idle_clients_when_host_pool_is_full() {
        let concurrent_calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(AtomicBool::new(false));
        let concurrent_calls_for_server = Arc::clone(&concurrent_calls);
        let release_for_server = Arc::clone(&release);
        let server = FakeFeRpcServer::start(
            0,
            Box::new(move |method, seq, i_prot, o_prot| match method {
                "getQueryProfile" => {
                    let _req: frontend_service::TGetProfileRequest = read_struct_arg(i_prot)?;
                    concurrent_calls_for_server.fetch_add(1, Ordering::AcqRel);
                    while !release_for_server.load(Ordering::Acquire) {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    concurrent_calls_for_server.fetch_sub(1, Ordering::AcqRel);
                    let response = frontend_service::TGetProfileResponse {
                        status: Some(ok_status()),
                        query_result: Some(vec!["profile".to_string()]),
                    };
                    write_struct_reply(o_prot, method, seq, &response)?;
                    Ok(ServerAction::Close)
                }
                other => panic!("unexpected FE RPC method: {other}"),
            }),
        );
        let manager = Arc::new(FrontendRpcManager::new(FrontendRpcSettings {
            max_idle_per_host: 1,
            max_inflight_total: 2,
            max_inflight_schema: 1,
            max_inflight_exec_status: 1,
            max_inflight_control: 1,
            max_inflight_schema_query: 2,
            ..test_manager_settings()
        }));
        let request = frontend_service::TGetProfileRequest::new(Some(vec!["q".to_string()]));

        let mut handles = Vec::new();
        for _ in 0..2 {
            let manager = Arc::clone(&manager);
            let coord = server.addr().clone();
            let request = request.clone();
            handles.push(std::thread::spawn(move || {
                manager
                    .call(FrontendRpcKind::SchemaQuery, &coord, |client| {
                        client
                            .get_query_profile(request.clone())
                            .map_err(FrontendRpcError::from_thrift)
                    })
                    .expect("concurrent getQueryProfile succeeds");
            }));
        }

        let start = Instant::now();
        while server.accepts() < 2 {
            assert!(start.elapsed() < Duration::from_secs(1));
            std::thread::sleep(Duration::from_millis(10));
        }
        release.store(true, Ordering::Release);

        for handle in handles {
            handle.join().expect("join concurrent FE RPC");
        }

        assert_eq!(concurrent_calls.load(Ordering::Acquire), 0);
        assert_eq!(manager.host_idle_len(&super::host_key(server.addr())), 1);
    }
}
