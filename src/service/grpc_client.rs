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
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use tonic::transport::Channel;
use tracing::debug;

use crate::common::types::UniqueId;
use crate::novarocks_logging::error;
use crate::runtime::global_async_runtime::{data_block_on, data_runtime_handle};

pub use crate::service::grpc_proto as proto;

#[derive(Default)]
struct ChannelCache {
    mu: Mutex<HashMap<String, Channel>>,
}

static CHANNELS: OnceLock<ChannelCache> = OnceLock::new();

fn channels() -> &'static ChannelCache {
    CHANNELS.get_or_init(|| ChannelCache {
        mu: Mutex::new(HashMap::new()),
    })
}

async fn get_channel(host: &str, port: u16) -> Result<Channel, String> {
    let key = format!("{host}:{port}");
    // Temporarily disable cache to test timeout fix
    // if let Some(ch) = channels()
    //     .mu
    //     .lock()
    //     .expect("channel cache lock")
    //     .get(&key)
    //     .cloned()
    // {
    //     debug!("get_channel: using CACHED channel for {}", key);
    //     return Ok(ch);
    // }

    debug!(
        "get_channel: creating NEW channel for {} with timeout=600s",
        key
    );
    let endpoint = format!("http://{host}:{port}")
        .parse::<tonic::transport::Endpoint>()
        .map_err(|e| format!("invalid endpoint: {e}"))?
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .timeout(Duration::from_secs(600))
        .connect_timeout(Duration::from_secs(10));

    let ch = endpoint
        .connect()
        .await
        .map_err(|e| format!("connect exchange endpoint failed: {e}"))?;

    channels()
        .mu
        .lock()
        .expect("channel cache lock")
        .insert(key.clone(), ch.clone());
    debug!("get_channel: channel created and cached for {}", key);
    Ok(ch)
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
        finst_id_hi: finst_id.hi,
        finst_id_lo: finst_id.lo,
        node_id,
        sender_id,
        be_number,
        eos,
        sequence,
        payload,
    };

    data_block_on(async move {
        let ch = get_channel(&host, port).await?;
        let mut cli =
            proto::novarocks::nova_rocks_grpc_client::NovaRocksGrpcClient::new(ch)
                .max_encoding_message_size(64 * 1024 * 1024)
                .max_decoding_message_size(64 * 1024 * 1024);

        let stream = tokio_stream::once(req);
        let resp = cli
            .exchange(stream)
            .await
            .map_err(|e| format!("exchange rpc failed: {e}"))?;
        let mut inbound = resp.into_inner();
        // Wait for the server ack to confirm delivery.
        match inbound.message().await {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("exchange ack recv failed: {e}")),
        }
    })?
}

pub fn transmit_runtime_filter(
    dest_host: &str,
    dest_port: u16,
    params: proto::starrocks::PTransmitRuntimeFilterParams,
) -> Result<(), String> {
    let dest_host = dest_host.to_string();
    let port = dest_port;
    let runtime_handle = data_runtime_handle()?;
    runtime_handle.spawn(async move {
        let ch = match get_channel(&dest_host, port).await {
            Ok(v) => v,
            Err(e) => {
                error!(
                    "runtime filter connect failed: dest={}:{} error={}",
                    dest_host, port, e
                );
                return;
            }
        };
        let mut cli = proto::novarocks::nova_rocks_grpc_client::NovaRocksGrpcClient::new(ch)
            .max_encoding_message_size(64 * 1024 * 1024)
            .max_decoding_message_size(64 * 1024 * 1024);
        match cli.transmit_runtime_filter(params).await {
            Ok(resp) => {
                if let Some(status) = resp.get_ref().status.as_ref() {
                    if status.status_code != 0 {
                        error!(
                            "runtime filter send failed: dest={}:{} code={} msgs={:?}",
                            dest_host, port, status.status_code, status.error_msgs
                        );
                    }
                }
            }
            Err(e) => {
                error!(
                    "runtime filter send failed: dest={}:{} error={}",
                    dest_host, port, e
                );
            }
        }
    });
    Ok(())
}

pub fn lookup(
    dest_host: &str,
    dest_port: u16,
    params: proto::starrocks::PLookUpRequest,
) -> Result<proto::starrocks::PLookUpResponse, String> {
    let dest_host = dest_host.to_string();
    let port = dest_port;
    data_block_on(async move {
        let ch = get_channel(&dest_host, port)
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
