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

use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tracing::debug;

use crate::common::types::UniqueId;
use crate::novarocks_logging::error;

pub mod proto {
    pub mod novarocks {
        tonic::include_proto!("novarocks");
    }
    pub mod starrocks {
        tonic::include_proto!("starrocks");
    }
}

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

#[derive(Clone)]
struct StreamConn {
    tx: tokio::sync::mpsc::Sender<proto::novarocks::ExchangeRequest>,
}

#[derive(Default)]
struct StreamCache {
    mu: Mutex<HashMap<String, StreamConn>>,
}

static STREAMS: OnceLock<StreamCache> = OnceLock::new();

fn streams() -> &'static StreamCache {
    STREAMS.get_or_init(|| StreamCache {
        mu: Mutex::new(HashMap::new()),
    })
}

fn client_runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(8)
            .build()
            .expect("build tokio runtime")
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

fn get_or_create_exchange_stream(dest_host: &str, port: u16) -> Result<StreamConn, String> {
    let key = format!("{dest_host}:{port}");
    if let Some(conn) = streams()
        .mu
        .lock()
        .expect("stream cache lock")
        .get(&key)
        .cloned()
    {
        return Ok(conn);
    }

    let (tx, rx) = tokio::sync::mpsc::channel::<proto::novarocks::ExchangeRequest>(4096);
    let dest_host = dest_host.to_string();
    let port = port;
    client_runtime().handle().spawn(async move {
        let ch = match get_channel(&dest_host, port).await {
            Ok(v) => v,
            Err(e) => {
                error!(
                    "exchange stream connect failed: dest={}:{} error={}",
                    dest_host, port, e
                );
                return;
            }
        };
        let mut cli = proto::novarocks::nova_rocks_grpc_client::NovaRocksGrpcClient::new(ch)
            .max_encoding_message_size(64 * 1024 * 1024)
            .max_decoding_message_size(64 * 1024 * 1024);

        let req_stream = ReceiverStream::new(rx);
        let resp = match cli.exchange(req_stream).await {
            Ok(v) => v,
            Err(e) => {
                error!(
                    "exchange stream open failed: dest={}:{} error={}",
                    dest_host, port, e
                );
                return;
            }
        };

        let mut inbound = resp.into_inner();
        let mut ack_count = 0u64;
        loop {
            match inbound.message().await {
                Ok(Some(_ack)) => {
                    // Drain acks to avoid backpressure on the server response stream.
                    ack_count = ack_count.saturating_add(1);
                    if ack_count % 1024 == 1 {
                        debug!(
                            "exchange ack RECV: dest={}:{} count={}",
                            dest_host, port, ack_count
                        );
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    error!(
                        "exchange stream recv failed: dest={}:{} error={}",
                        dest_host, port, e
                    );
                    break;
                }
            }
        }
    });

    let conn = StreamConn { tx };
    streams()
        .mu
        .lock()
        .expect("stream cache lock")
        .insert(key, conn.clone());
    Ok(conn)
}

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
    use crate::novarocks_logging::debug;
    let payload_size = payload.len();
    debug!(
        "send_chunks START: dest={} finst={} node_id={} sender_id={} be_number={} eos={} seq={} payload_bytes={}",
        dest_host, finst_id, node_id, sender_id, be_number, eos, sequence, payload_size
    );
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

    let conn = get_or_create_exchange_stream(dest_host, port)?;
    match conn.tx.blocking_send(req) {
        Ok(()) => Ok(()),
        Err(e) => {
            // Drop cached stream so the next send will re-create it.
            let key = format!("{dest_host}:{port}");
            streams().mu.lock().expect("stream cache lock").remove(&key);
            Err(format!("exchange stream send failed: {e}"))
        }
    }
}

pub fn transmit_runtime_filter(
    dest_host: &str,
    dest_port: u16,
    params: proto::starrocks::PTransmitRuntimeFilterParams,
) -> Result<(), String> {
    let dest_host = dest_host.to_string();
    let port = dest_port;
    client_runtime().handle().spawn(async move {
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
    client_runtime().block_on(async move {
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
}
