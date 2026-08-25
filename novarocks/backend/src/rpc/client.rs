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

//! Backend-owned outbound RPC transport.
//!
//! This is deliberately role-private: it provides only BE-to-BE data-plane
//! calls and the BE-to-FE terminal fallback.  It shares no transport facade
//! with Frontend or Core.

use std::time::Duration;

use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_proto::{filter, novarocks as proto};
use novarocks_types::format_host_for_url;
use novarocks_types::identity::UniqueId;
use tonic::Request;
use tonic::transport::Channel;

use super::runtime::BackendDataRuntime;
use super::transport::nova_rocks_grpc_client::NovaRocksGrpcClient;

const GRPC_MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

pub(crate) struct BackendRpcClient {
    runtime: BackendDataRuntime,
    host: String,
    port: u16,
}

impl BackendRpcClient {
    pub(crate) fn new_runtime_endpoint(
        runtime: BackendDataRuntime,
        endpoint: &RuntimeEndpoint,
    ) -> Result<Self, String> {
        let port = u16::try_from(endpoint.port())
            .map_err(|_| format!("invalid runtime filter endpoint port {}", endpoint.port()))?;
        Self::new_host_port(runtime, endpoint.host().to_string(), port)
    }

    pub(crate) fn new_host_port(
        runtime: BackendDataRuntime,
        host: String,
        port: u16,
    ) -> Result<Self, String> {
        channel_endpoint(&host, port)
            .map_err(|error| format!("invalid BE endpoint {host}:{port}: {error}"))?;
        Ok(Self {
            runtime,
            host,
            port,
        })
    }

    fn make_client(&self) -> Result<NovaRocksGrpcClient<Channel>, String> {
        let host = self.host.clone();
        let port = self.port;
        let runtime = self.runtime.clone();
        let channel = runtime
            .clone()
            .block_on(async move { get_or_create_channel(&runtime, &host, port).await })?;
        Ok(client_from_channel(channel))
    }

    async fn make_deadline_async_client(
        &self,
        operation: &str,
        deadline_at: tokio::time::Instant,
    ) -> Result<NovaRocksGrpcClient<Channel>, String> {
        tokio::time::timeout_at(
            deadline_at,
            get_or_create_channel(&self.runtime, &self.host, self.port),
        )
        .await
        .map_err(|_| format!("{operation} deadline exceeded during channel acquisition"))?
        .map(client_from_channel)
        .map_err(|error| format!("{operation} channel acquisition failed: {error}"))
    }

    pub(crate) async fn transmit_runtime_filter_envelope_async(
        &self,
        request: filter::RuntimeFilterEnvelope,
        deadline: Duration,
    ) -> Result<filter::RuntimeFilterEnvelopeResponse, String> {
        let deadline_at = tokio::time::Instant::now() + deadline;
        let mut client = self
            .make_deadline_async_client("runtime filter envelope", deadline_at)
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

    pub(crate) fn blocking_report_query_terminal_with_timeout(
        &self,
        request: proto::ReportQueryTerminalRequest,
        timeout: Duration,
    ) -> Result<proto::ReportQueryTerminalResponse, String> {
        self.runtime.block_on(async {
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
            let mut request = Request::new(request);
            request.set_timeout(remaining);
            tokio::time::timeout_at(deadline_at, client.report_query_terminal(request))
                .await
                .map_err(|_| {
                    "report_query_terminal deadline exceeded during unary RPC".to_string()
                })?
                .map(|response| response.into_inner())
                .map_err(|error| format!("report_query_terminal rpc failed: {error}"))
        })
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "The frozen native boundary keeps independently validated inputs explicit."
    )]
    pub(crate) fn exchange_unary(
        &self,
        finst_id: UniqueId,
        node_id: i32,
        sender_id: i32,
        be_number: i32,
        eos: bool,
        sequence: i64,
        payload: Vec<u8>,
    ) -> Result<(), String> {
        let mut client = self.make_client()?;
        let request = proto::ExchangeRequest {
            finst_id_hi: finst_id.high(),
            finst_id_lo: finst_id.low(),
            node_id,
            sender_id,
            be_number,
            eos,
            sequence,
            payload,
        };
        self.runtime.block_on(async move {
            let response = client
                .exchange_unary(request)
                .await
                .map_err(|error| format!("exchange rpc failed: {error}"))?
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
        })
    }

    pub(crate) fn lookup(
        &self,
        request: filter::LookupRequest,
    ) -> Result<filter::LookupResponse, String> {
        let host = self.host.clone();
        let port = self.port;
        let mut client = self
            .make_client()
            .map_err(|error| format!("lookup connect failed: dest={host}:{port} error={error}"))?;
        self.runtime
            .block_on(async move {
                client
                    .lookup(request)
                    .await
                    .map(|response| response.into_inner())
                    .map_err(|error| {
                        format!("lookup request failed: dest={host}:{port} error={error}")
                    })
            })
            .map_err(|error| format!("lookup runtime execution failed: {error}"))
    }
}

fn channel_endpoint(
    host: &str,
    port: u16,
) -> Result<tonic::transport::Endpoint, tonic::transport::Error> {
    format!("http://{}:{port}", format_host_for_url(host)).parse()
}

fn client_from_channel(channel: Channel) -> NovaRocksGrpcClient<Channel> {
    NovaRocksGrpcClient::new(channel)
        .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES)
        .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
}

async fn get_or_create_channel(
    runtime: &BackendDataRuntime,
    host: &str,
    port: u16,
) -> Result<Channel, String> {
    let key = format!("{}:{port}", format_host_for_url(host));
    if let Some(channel) = runtime
        .channels()
        .lock()
        .expect("native channel cache lock")
        .get(&key)
        .cloned()
    {
        return Ok(channel);
    }
    let channel = channel_endpoint(host, port)
        .map_err(|error| format!("invalid endpoint: {error}"))?
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .timeout(Duration::from_secs(600))
        .connect_timeout(Duration::from_secs(10))
        .http2_adaptive_window(true)
        .initial_stream_window_size(Some(32 * 1024 * 1024))
        .initial_connection_window_size(Some(128 * 1024 * 1024))
        .connect()
        .await
        .map_err(|error| format!("connect exchange endpoint failed: {error}"))?;
    runtime
        .channels()
        .lock()
        .expect("native channel cache lock")
        .insert(key, channel.clone());
    Ok(channel)
}

#[cfg(test)]
mod tests {
    use super::channel_endpoint;

    #[test]
    fn channel_endpoint_formats_ipv4_and_ipv6_hosts() {
        assert_eq!(
            channel_endpoint("127.0.0.1", 9070)
                .expect("IPv4 endpoint")
                .uri()
                .to_string(),
            "http://127.0.0.1:9070/"
        );
        assert_eq!(
            channel_endpoint("::1", 9070)
                .expect("IPv6 endpoint")
                .uri()
                .to_string(),
            "http://[::1]:9070/"
        );
    }
}
