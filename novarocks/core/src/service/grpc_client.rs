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
use std::net::SocketAddr;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

#[cfg(test)]
use std::sync::mpsc::SyncSender;

use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::transport::Channel;

use crate::common::network::format_host_for_url;
use crate::common::types::UniqueId;
use crate::novarocks_logging::error;
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime::global_async_runtime::{data_block_on, data_runtime_handle};

pub use crate::proto;

/// gRPC client for NovaRocks BE-to-BE and coordinator RPCs.
///
/// Wraps the tonic async client with blocking wrappers so that PR-4's
/// `RemoteDispatcher` can drive it from a non-async context.  One
/// `NovaRocksGrpcRemoteClient` per remote BE address; callers are
/// responsible for caching instances.
pub struct NovaRocksGrpcRemoteClient {
    host: String,
    port: u16,
}

#[derive(Debug)]
pub(crate) enum QueryLifecycleRpcError {
    PreSubmission(String),
    PostSubmissionDeadlineExceeded(String),
    PostSubmissionStatus(tonic::Status),
}

impl NovaRocksGrpcRemoteClient {
    /// Create a client for `addr`.
    ///
    /// The underlying HTTP/2 channel is established lazily via the shared
    /// channel cache, so construction itself is cheap.
    pub fn new(addr: SocketAddr) -> Result<Self, String> {
        Self::new_host_port(addr.ip().to_string(), addr.port())
    }

    /// Create a client for a neutral runtime endpoint.
    ///
    /// Backend-owned data-plane transports use this generic channel helper;
    /// it does not create or look up query-scoped runtime-filter state.
    pub fn new_runtime_endpoint(endpoint: &RuntimeEndpoint) -> Result<Self, String> {
        let port = u16::try_from(endpoint.port())
            .map_err(|_| format!("invalid runtime filter endpoint port {}", endpoint.port()))?;
        Self::new_host_port(endpoint.host().to_string(), port)
    }

    pub fn new_host_port(host: String, port: u16) -> Result<Self, String> {
        // Eagerly verify the endpoint can be parsed; actual TCP setup is lazy.
        channel_endpoint(&host, port)
            .map_err(|e| format!("invalid BE endpoint {host}:{port}: {e}"))?;
        Ok(Self { host, port })
    }

    /// Connect to `addr` and return a ready client.
    ///
    /// The underlying HTTP/2 channel is established lazily via the shared
    /// channel cache, so the connect itself is cheap.
    pub fn connect_blocking(addr: SocketAddr) -> Result<Self, String> {
        Self::new(addr)
    }

    fn make_client(
        &self,
    ) -> Result<proto::novarocks::nova_rocks_grpc_client::NovaRocksGrpcClient<Channel>, String>
    {
        let host = self.host.clone();
        let port = self.port;
        let ch = data_block_on(async move { get_or_create_channel(&host, port).await })??;
        Ok(Self::client_from_channel(ch))
    }

    async fn make_async_client(
        &self,
    ) -> Result<proto::novarocks::nova_rocks_grpc_client::NovaRocksGrpcClient<Channel>, String>
    {
        let ch = get_or_create_channel(&self.host, self.port).await?;
        Ok(Self::client_from_channel(ch))
    }

    async fn make_deadline_async_client(
        &self,
        operation: &str,
        deadline_at: tokio::time::Instant,
    ) -> Result<proto::novarocks::nova_rocks_grpc_client::NovaRocksGrpcClient<Channel>, String>
    {
        let acquire = async {
            #[cfg(test)]
            await_deadline_channel_acquisition_test_hook(operation, &self.host, self.port).await;
            self.make_async_client().await
        };
        tokio::time::timeout_at(deadline_at, acquire)
            .await
            .map_err(|_| format!("{operation} deadline exceeded during channel acquisition"))?
            .map_err(|error| format!("{operation} channel acquisition failed: {error}"))
    }

    async fn make_runtime_filter_async_client(
        &self,
        operation: &str,
        deadline_at: tokio::time::Instant,
    ) -> Result<proto::novarocks::nova_rocks_grpc_client::NovaRocksGrpcClient<Channel>, String>
    {
        let acquire = async {
            #[cfg(test)]
            await_runtime_filter_channel_acquisition_test_hook(&self.host, self.port).await;
            self.make_async_client().await
        };
        tokio::time::timeout_at(deadline_at, acquire)
            .await
            .map_err(|_| {
                format!("runtime filter {operation} deadline exceeded during channel acquisition")
            })?
            .map_err(|error| {
                format!("runtime filter {operation} channel acquisition failed: {error}")
            })
    }

    fn client_from_channel(
        ch: Channel,
    ) -> proto::novarocks::nova_rocks_grpc_client::NovaRocksGrpcClient<Channel> {
        proto::novarocks::nova_rocks_grpc_client::NovaRocksGrpcClient::new(ch)
            .max_encoding_message_size(GRPC_MAX_ENCODING_BYTES)
            .max_decoding_message_size(GRPC_MAX_DECODING_BYTES)
    }

    pub(crate) async fn init_query_async(
        &self,
        request: proto::novarocks::InitQueryRequest,
        timeout: Duration,
    ) -> Result<proto::novarocks::InitQueryResponse, QueryLifecycleRpcError> {
        let deadline_at = tokio::time::Instant::now() + timeout;
        let mut client = self
            .make_deadline_async_client("init_query", deadline_at)
            .await
            .map_err(QueryLifecycleRpcError::PreSubmission)?;
        let remaining = deadline_at.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(QueryLifecycleRpcError::PreSubmission(
                "init_query deadline exceeded before unary RPC submission".to_string(),
            ));
        }
        let mut request = Request::new(request);
        request.set_timeout(remaining);
        tokio::time::timeout_at(deadline_at, client.init_query(request))
            .await
            .map_err(|_| {
                QueryLifecycleRpcError::PostSubmissionDeadlineExceeded(
                    "init_query deadline exceeded during unary RPC".to_string(),
                )
            })?
            .map(|response| response.into_inner())
            .map_err(QueryLifecycleRpcError::PostSubmissionStatus)
    }

    pub(crate) async fn stage_fragments_async(
        &self,
        request: proto::novarocks::StageFragmentsRequest,
        timeout: Duration,
    ) -> Result<proto::novarocks::StageFragmentsResponse, QueryLifecycleRpcError> {
        let deadline_at = tokio::time::Instant::now() + timeout;
        let mut client = self
            .make_deadline_async_client("stage_fragments", deadline_at)
            .await
            .map_err(QueryLifecycleRpcError::PreSubmission)?;
        let remaining = deadline_at.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(QueryLifecycleRpcError::PreSubmission(
                "stage_fragments deadline exceeded before unary RPC submission".to_string(),
            ));
        }
        let mut request = Request::new(request);
        request.set_timeout(remaining);
        tokio::time::timeout_at(deadline_at, client.stage_fragments(request))
            .await
            .map_err(|_| {
                QueryLifecycleRpcError::PostSubmissionDeadlineExceeded(
                    "stage_fragments deadline exceeded during unary RPC".to_string(),
                )
            })?
            .map(|response| response.into_inner())
            .map_err(QueryLifecycleRpcError::PostSubmissionStatus)
    }

    pub(crate) async fn start_prepared_query_async(
        &self,
        request: proto::novarocks::StartPreparedQueryRequest,
        timeout: Duration,
    ) -> Result<proto::novarocks::StartPreparedQueryResponse, QueryLifecycleRpcError> {
        let deadline_at = tokio::time::Instant::now() + timeout;
        let mut client = self
            .make_deadline_async_client("start_prepared_query", deadline_at)
            .await
            .map_err(QueryLifecycleRpcError::PreSubmission)?;
        let remaining = deadline_at.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(QueryLifecycleRpcError::PreSubmission(
                "start_prepared_query deadline exceeded before unary RPC submission".to_string(),
            ));
        }
        let mut request = Request::new(request);
        request.set_timeout(remaining);
        tokio::time::timeout_at(deadline_at, client.start_prepared_query(request))
            .await
            .map_err(|_| {
                QueryLifecycleRpcError::PostSubmissionDeadlineExceeded(
                    "start_prepared_query deadline exceeded during unary RPC".to_string(),
                )
            })?
            .map(|response| response.into_inner())
            .map_err(QueryLifecycleRpcError::PostSubmissionStatus)
    }

    pub(crate) async fn abort_query_async(
        &self,
        request: proto::novarocks::AbortQueryRequest,
        timeout: Duration,
    ) -> Result<proto::novarocks::AbortQueryResponse, QueryLifecycleRpcError> {
        let deadline_at = tokio::time::Instant::now() + timeout;
        let mut client = self
            .make_deadline_async_client("abort_query", deadline_at)
            .await
            .map_err(QueryLifecycleRpcError::PreSubmission)?;
        let remaining = deadline_at.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(QueryLifecycleRpcError::PreSubmission(
                "abort_query deadline exceeded before unary RPC submission".to_string(),
            ));
        }
        let mut request = Request::new(request);
        request.set_timeout(remaining);
        tokio::time::timeout_at(deadline_at, client.abort_query(request))
            .await
            .map_err(|_| {
                QueryLifecycleRpcError::PostSubmissionDeadlineExceeded(
                    "abort_query deadline exceeded during unary RPC".to_string(),
                )
            })?
            .map(|response| response.into_inner())
            .map_err(QueryLifecycleRpcError::PostSubmissionStatus)
    }

    pub(crate) async fn attach_query_control_async(
        &self,
        outbound: ReceiverStream<proto::novarocks::QueryControlRequest>,
        timeout: Duration,
    ) -> Result<tonic::Streaming<proto::novarocks::QueryControlResponse>, QueryLifecycleRpcError>
    {
        let deadline_at = tokio::time::Instant::now() + timeout;
        let mut client = self
            .make_deadline_async_client("query_control_attach", deadline_at)
            .await
            .map_err(QueryLifecycleRpcError::PreSubmission)?;
        let remaining = deadline_at.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(QueryLifecycleRpcError::PreSubmission(
                "query_control_attach deadline exceeded before stream RPC submission".to_string(),
            ));
        }
        tokio::time::timeout_at(
            deadline_at,
            client.query_control_stream(Request::new(outbound)),
        )
        .await
        .map_err(|_| {
            QueryLifecycleRpcError::PostSubmissionDeadlineExceeded(
                "query_control_attach deadline exceeded during stream RPC".to_string(),
            )
        })?
        .map(|response| response.into_inner())
        .map_err(QueryLifecycleRpcError::PostSubmissionStatus)
    }

    pub fn blocking_ensure_connector_execution_binding(
        &self,
        req: proto::novarocks::EnsureConnectorExecutionBindingRequest,
    ) -> Result<proto::novarocks::EnsureConnectorExecutionBindingResponse, String> {
        let mut cli = self.make_client()?;
        data_block_on(async move {
            cli.ensure_connector_execution_binding(req)
                .await
                .map(|response| response.into_inner())
                .map_err(|error| format!("ensure_connector_execution_binding rpc failed: {error}"))
        })?
    }

    pub fn blocking_retire_connector_execution_binding(
        &self,
        req: proto::novarocks::RetireConnectorExecutionBindingRequest,
    ) -> Result<proto::novarocks::RetireConnectorExecutionBindingResponse, String> {
        let mut cli = self.make_client()?;
        data_block_on(async move {
            cli.retire_connector_execution_binding(req)
                .await
                .map(|response| response.into_inner())
                .map_err(|error| format!("retire_connector_execution_binding rpc failed: {error}"))
        })?
    }

    pub fn blocking_fetch_result(
        &self,
        req: proto::novarocks::FetchResultRequest,
    ) -> Result<proto::novarocks::FetchResultResponse, String> {
        let mut cli = self.make_client()?;
        data_block_on(async move {
            cli.fetch_result(req)
                .await
                .map(|r| r.into_inner())
                .map_err(|e| format!("fetch_result rpc failed: {e}"))
        })?
    }

    #[cfg(test)]
    pub(crate) fn blocking_fetch_result_with_timeout(
        &self,
        req: proto::novarocks::FetchResultRequest,
        timeout: Duration,
    ) -> Result<proto::novarocks::FetchResultResponse, String> {
        data_block_on(async {
            let deadline_at = tokio::time::Instant::now() + timeout;
            let mut client = self
                .make_deadline_async_client("fetch_result", deadline_at)
                .await?;
            let mut request = Request::new(req);
            request.set_timeout(timeout);
            tokio::time::timeout_at(deadline_at, client.fetch_result(request))
                .await
                .map_err(|_| "fetch_result deadline exceeded".to_string())?
                .map(|response| response.into_inner())
                .map_err(|error| format!("fetch_result rpc failed: {error}"))
        })?
    }

    pub fn blocking_report_query_terminal_with_timeout(
        &self,
        req: proto::novarocks::ReportQueryTerminalRequest,
        timeout: Duration,
    ) -> Result<proto::novarocks::ReportQueryTerminalResponse, String> {
        data_block_on(async {
            let deadline_at = tokio::time::Instant::now() + timeout;
            let mut client = self
                .make_deadline_async_client("report_query_terminal", deadline_at)
                .await?;
            let remaining = deadline_at.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(
                    "report_query_terminal deadline exceeded before unary RPC submission"
                        .to_string(),
                );
            }
            let mut request = Request::new(req);
            request.set_timeout(remaining);
            tokio::time::timeout_at(deadline_at, client.report_query_terminal(request))
                .await
                .map_err(|_| {
                    "report_query_terminal deadline exceeded during unary RPC".to_string()
                })?
                .map(|response| response.into_inner())
                .map_err(|error| format!("report_query_terminal rpc failed: {error}"))
        })?
    }

    pub async fn heartbeat_async(
        &self,
        req: proto::novarocks::HeartbeatRequest,
    ) -> Result<proto::novarocks::HeartbeatResponse, String> {
        let mut cli = self.make_async_client().await?;
        let mut req = Request::new(req);
        req.set_timeout(Duration::from_secs(3));
        cli.heartbeat(req)
            .await
            .map(|r| r.into_inner())
            .map_err(|e| format!("heartbeat rpc failed: {e}"))
    }

    /// Send an already-encoded runtime-filter envelope over the neutral gRPC
    /// channel. Routing, retries, and service ownership belong to Backend.
    pub async fn transmit_runtime_filter_envelope_async(
        &self,
        request: proto::filter::RuntimeFilterEnvelope,
        deadline: Duration,
    ) -> Result<proto::filter::RuntimeFilterEnvelopeResponse, String> {
        let deadline_at = tokio::time::Instant::now() + deadline;
        let mut client = self
            .make_runtime_filter_async_client("envelope", deadline_at)
            .await?;
        let remaining = deadline_at.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(
                "runtime filter envelope deadline exceeded before unary RPC submission".to_string(),
            );
        }
        let mut request = Request::new(request);
        request.set_timeout(remaining);
        tokio::time::timeout_at(
            deadline_at,
            client.transmit_runtime_filter_envelope(request),
        )
        .await
        .map_err(|_| "runtime filter envelope deadline exceeded during unary RPC".to_string())?
        .map(|response| response.into_inner())
        .map_err(|error| format!("transmit_runtime_filter_envelope rpc failed: {error}"))
    }

    pub fn blocking_heartbeat(
        &self,
        req: proto::novarocks::HeartbeatRequest,
    ) -> Result<proto::novarocks::HeartbeatResponse, String> {
        let mut cli = self.make_client()?;
        data_block_on(async move {
            let mut req = Request::new(req);
            req.set_timeout(Duration::from_secs(3));
            cli.heartbeat(req)
                .await
                .map(|r| r.into_inner())
                .map_err(|e| format!("heartbeat rpc failed: {e}"))
        })?
    }
}

const GRPC_MAX_ENCODING_BYTES: usize = 64 * 1024 * 1024;
const GRPC_MAX_DECODING_BYTES: usize = 64 * 1024 * 1024;
const REPORT_EXEC_STATUS_RPC_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Default)]
struct ChannelCache {
    mu: Mutex<HashMap<String, Channel>>,
}

static CHANNELS: OnceLock<ChannelCache> = OnceLock::new();

#[cfg(test)]
struct DeadlineChannelAcquisitionTestHook {
    operation: String,
    endpoint: String,
    started: SyncSender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}

#[cfg(test)]
fn deadline_channel_acquisition_test_hook()
-> &'static Mutex<Option<DeadlineChannelAcquisitionTestHook>> {
    static HOOK: OnceLock<Mutex<Option<DeadlineChannelAcquisitionTestHook>>> = OnceLock::new();
    HOOK.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn set_deadline_channel_acquisition_test_hook(
    operation: &str,
    endpoint: String,
    started: SyncSender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
) {
    let mut hook = deadline_channel_acquisition_test_hook()
        .lock()
        .expect("deadline channel acquisition test hook lock");
    assert!(
        hook.is_none(),
        "deadline channel acquisition test hook already set"
    );
    *hook = Some(DeadlineChannelAcquisitionTestHook {
        operation: operation.to_string(),
        endpoint,
        started,
        release,
    });
}

#[cfg(test)]
async fn await_deadline_channel_acquisition_test_hook(operation: &str, host: &str, port: u16) {
    let endpoint = format!("{}:{port}", format_host_for_url(host));
    let hook = {
        let mut hook = deadline_channel_acquisition_test_hook()
            .lock()
            .expect("deadline channel acquisition test hook lock");
        if hook
            .as_ref()
            .is_some_and(|hook| hook.operation == operation && hook.endpoint == endpoint)
        {
            hook.take()
        } else {
            None
        }
    };
    if let Some(hook) = hook {
        hook.started
            .send(())
            .expect("deadline channel acquisition observer");
        let _ = hook.release.await;
    }
}

#[cfg(test)]
struct RuntimeFilterChannelAcquisitionTestHook {
    endpoint: String,
    started: SyncSender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}

#[cfg(test)]
fn runtime_filter_channel_acquisition_test_hook()
-> &'static Mutex<Option<RuntimeFilterChannelAcquisitionTestHook>> {
    static HOOK: OnceLock<Mutex<Option<RuntimeFilterChannelAcquisitionTestHook>>> = OnceLock::new();
    HOOK.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn set_runtime_filter_channel_acquisition_test_hook(
    endpoint: String,
    started: SyncSender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
) {
    let mut hook = runtime_filter_channel_acquisition_test_hook()
        .lock()
        .expect("runtime filter channel acquisition test hook lock");
    assert!(
        hook.is_none(),
        "runtime filter channel acquisition test hook already set"
    );
    *hook = Some(RuntimeFilterChannelAcquisitionTestHook {
        endpoint,
        started,
        release,
    });
}

#[cfg(test)]
async fn await_runtime_filter_channel_acquisition_test_hook(host: &str, port: u16) {
    let endpoint = format!("{}:{port}", format_host_for_url(host));
    let hook = {
        let mut hook = runtime_filter_channel_acquisition_test_hook()
            .lock()
            .expect("runtime filter channel acquisition test hook lock");
        if hook.as_ref().is_some_and(|hook| hook.endpoint == endpoint) {
            hook.take()
        } else {
            None
        }
    };
    if let Some(hook) = hook {
        hook.started
            .send(())
            .expect("channel acquisition test observer");
        let _ = hook.release.await;
    }
}

fn channels() -> &'static ChannelCache {
    CHANNELS.get_or_init(|| ChannelCache {
        mu: Mutex::new(HashMap::new()),
    })
}

fn channel_endpoint_uri(host: &str, port: u16) -> String {
    format!("http://{}:{port}", format_host_for_url(host))
}

fn channel_endpoint(
    host: &str,
    port: u16,
) -> Result<tonic::transport::Endpoint, tonic::transport::Error> {
    channel_endpoint_uri(host, port).parse::<tonic::transport::Endpoint>()
}

/// Return a cached channel for the given endpoint, creating one if needed.
///
/// Must be called from within an async Tokio context (inside data_block_on or
/// a spawned task), because `connect()` drives TCP+HTTP2 setup via the reactor.
/// One channel per (host, port) is sufficient — HTTP/2 multiplexes all
/// concurrent RPCs over the single connection.
async fn get_or_create_channel(host: &str, port: u16) -> Result<Channel, String> {
    let key = format!("{}:{port}", format_host_for_url(host));
    {
        let guard = channels().mu.lock().expect("channel cache lock");
        if let Some(ch) = guard.get(&key).cloned() {
            return Ok(ch);
        }
    }
    let ch = channel_endpoint(host, port)
        .map_err(|e| format!("invalid endpoint: {e}"))?
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .timeout(Duration::from_secs(600))
        .connect_timeout(Duration::from_secs(10))
        .http2_adaptive_window(true)
        .initial_stream_window_size(Some(32 * 1024 * 1024))
        .initial_connection_window_size(Some(128 * 1024 * 1024))
        .connect()
        .await
        .map_err(|e| format!("connect exchange endpoint failed: {e}"))?;
    channels()
        .mu
        .lock()
        .expect("channel cache lock")
        .insert(key, ch.clone());
    Ok(ch)
}

#[cfg(test)]
mod pr3_tests {
    use super::*;

    use std::time::Instant;

    use crate::service::grpc_server::GrpcService;

    #[test]
    fn remote_client_connect_accepts_socket_addr() {
        let addr: SocketAddr = "127.0.0.1:19030".parse().expect("valid addr");
        let client = NovaRocksGrpcRemoteClient::connect_blocking(addr)
            .expect("connect wrapper should accept SocketAddr");
        assert_eq!(client.host, "127.0.0.1");
        assert_eq!(client.port, 19030);
    }

    #[test]
    fn channel_endpoint_uri_formats_ipv4_and_ipv6_hosts() {
        assert_eq!(
            channel_endpoint_uri("127.0.0.1", 9070),
            "http://127.0.0.1:9070"
        );
        assert_eq!(channel_endpoint_uri("::1", 9070), "http://[::1]:9070");
    }

    #[test]
    fn remote_client_connect_accepts_ipv6_socket_addr() {
        let addr: SocketAddr = "[::1]:19030".parse().expect("valid ipv6 addr");
        let client = NovaRocksGrpcRemoteClient::connect_blocking(addr)
            .expect("connect wrapper should accept IPv6 SocketAddr");
        assert_eq!(client.host, "::1");
        assert_eq!(client.port, 19030);
    }
}

/// Synchronous exchange send — blocks until the server acknowledges receipt.
///
/// Each call opens a single-message gRPC stream, sends the request, and waits
/// for the server ack before returning.  This matches the delivery guarantee of
/// the brpc path and ensures `ExchangeSendTracker::on_complete` fires only
/// after the data has actually been received by the exchange registry.
pub fn send_chunks(
    dest_host: &str,
    dest_port: u16,
    finst_id: UniqueId,
    node_id: i32,
    sender_id: i32,
    be_number: i32,
    eos: bool,
    sequence: i64,
    payload: Vec<u8>,
) -> Result<(), String> {
    let host = dest_host.to_string();
    let port = dest_port;
    let req = proto::novarocks::ExchangeRequest {
        finst_id_hi: finst_id.high(),
        finst_id_lo: finst_id.low(),
        node_id,
        sender_id,
        be_number,
        eos,
        sequence,
        payload,
    };

    data_block_on(async move {
        let ch = get_or_create_channel(&host, port).await?;
        let mut cli = proto::novarocks::nova_rocks_grpc_client::NovaRocksGrpcClient::new(ch)
            .max_encoding_message_size(64 * 1024 * 1024)
            .max_decoding_message_size(64 * 1024 * 1024);

        let response = cli
            .exchange_unary(req)
            .await
            .map_err(|e| format!("exchange rpc failed: {e}"))?
            .into_inner();
        if let Some(status) = response.status.as_ref()
            && status.code != 0
        {
            return Err(if status.message.is_empty() {
                format!("exchange rpc returned status_code={}", status.code)
            } else {
                format!("exchange rpc failed: {}", status.message)
            });
        }
        Ok(())
    })?
}

pub fn lookup(
    dest_host: &str,
    dest_port: u16,
    params: proto::filter::LookupRequest,
) -> Result<proto::filter::LookupResponse, String> {
    let dest_host = dest_host.to_string();
    let port = dest_port;
    data_block_on(async move {
        let ch = get_or_create_channel(&dest_host, port)
            .await
            .map_err(|e| format!("lookup connect failed: dest={dest_host}:{port} error={e}"))?;
        let mut cli = proto::novarocks::nova_rocks_grpc_client::NovaRocksGrpcClient::new(ch)
            .max_encoding_message_size(64 * 1024 * 1024)
            .max_decoding_message_size(64 * 1024 * 1024);
        let resp = cli
            .lookup(params)
            .await
            .map_err(|e| format!("lookup request failed: dest={dest_host}:{port} error={e}"))?;
        Ok(resp.into_inner())
    })
    .map_err(|e| format!("lookup runtime execution failed: {e}"))?
}

#[cfg(test)]
mod lookup_tests {
    use super::*;
    use crate::runtime::global_async_runtime::data_block_on;
    use crate::service::grpc_server::GrpcService;

    fn spawn_lookup_server() -> std::net::SocketAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind lookup server");
        let addr = listener.local_addr().expect("lookup server local addr");
        data_block_on(async move {
            listener
                .set_nonblocking(true)
                .expect("set lookup server nonblocking");
            let listener = tokio::net::TcpListener::from_std(listener).expect("tokio listener");
            let incoming = futures::stream::unfold(listener, |listener| async {
                let item = listener.accept().await.map(|(stream, _)| stream);
                Some((item, listener))
            });
            tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(
                        proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpcServer::new(
                            GrpcService::with_fragment_execution(
                                crate::service::grpc_server::rejecting_test_native_fragment_ingress(
                                ),
                                crate::service::grpc_server::rejecting_test_query_lifecycle_ingress(
                                ),
                            ),
                        ),
                    )
                    .serve_with_incoming(incoming),
            );
        })
        .expect("spawn lookup server");

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_millis(50))
                .is_ok()
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "lookup grpc server did not become ready at {addr}"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        addr
    }

    #[test]
    fn test_lookup_uses_native_tonic_server_without_hook() {
        let addr = spawn_lookup_server();

        let response = lookup(
            "127.0.0.1",
            addr.port(),
            proto::filter::LookupRequest {
                query_id: None,
                lookup_node_id: 77,
                request_tuple_id: 1,
                request_columns: Vec::new(),
            },
        )
        .expect("lookup rpc should return native response");

        let status = response.status.expect("lookup response status");
        assert_ne!(status.code, 0);
        assert!(status.message.contains("missing query_id for lookup"));
    }
}
