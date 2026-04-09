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
use std::net::{SocketAddr, TcpListener};
use std::sync::{Mutex, OnceLock};
use std::thread::JoinHandle;

use axum::routing::{get, post, put};
use tokio::sync::watch;
use tokio_stream::wrappers::ReceiverStream;
use tonic::service::Routes;
use tonic::transport::Server;

use crate::common::config::{http_port, starlet_port};
use crate::common::types::format_uuid;
use crate::connector::starrocks::starmgr;
use crate::novarocks_logging::{error, info, warn};
use crate::runtime::starlet_shard_registry;
use crate::service::internal_rpc;
use crate::service::{load_tracking_http, stream_load_http};

pub use crate::service::grpc_proto as proto;

const GRPC_MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Default)]
struct GrpcServerState {
    started: bool,
    bound_port: Option<u16>,
    shutdown_tx: Option<watch::Sender<bool>>,
    join_handle: Option<JoinHandle<()>>,
}

fn grpc_server_state() -> &'static Mutex<GrpcServerState> {
    static STATE: OnceLock<Mutex<GrpcServerState>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(GrpcServerState::default()))
}

#[derive(Default)]
pub struct GrpcService;

#[tonic::async_trait]
impl proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpc for GrpcService {
    type ExchangeStream = std::pin::Pin<
        Box<
            dyn tokio_stream::Stream<
                    Item = Result<proto::novarocks::ExchangeResponse, tonic::Status>,
                > + Send
                + 'static,
        >,
    >;

    async fn exchange(
        &self,
        request: tonic::Request<tonic::Streaming<proto::novarocks::ExchangeRequest>>,
    ) -> Result<tonic::Response<Self::ExchangeStream>, tonic::Status> {
        use crate::novarocks_logging::debug;

        let mut inbound = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel::<
            Result<proto::novarocks::ExchangeResponse, tonic::Status>,
        >(4096);

        tokio::spawn(async move {
            loop {
                let req = match inbound.message().await {
                    Ok(Some(v)) => v,
                    Ok(None) => break,
                    Err(e) => {
                        let _ = tx
                            .send(Err(tonic::Status::internal(format!(
                                "exchange recv failed: {e}"
                            ))))
                            .await;
                        break;
                    }
                };

                let params = proto::starrocks::PTransmitChunkParams {
                    finst_id: Some(proto::starrocks::PUniqueId {
                        hi: req.finst_id_hi,
                        lo: req.finst_id_lo,
                    }),
                    node_id: Some(req.node_id),
                    sender_id: Some(req.sender_id),
                    be_number: Some(req.be_number),
                    eos: Some(req.eos),
                    sequence: Some(req.sequence),
                    chunks: vec![proto::starrocks::ChunkPb {
                        data: Some(req.payload),
                        data_size: Some(0),
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                // handle_transmit_chunk includes Arrow IPC decoding which is CPU-intensive.
                // Offload to the blocking thread pool so async worker threads stay free for I/O.
                let result = match tokio::task::spawn_blocking(move || {
                    internal_rpc::handle_transmit_chunk(params)
                })
                .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = tx
                            .send(Err(tonic::Status::internal(format!(
                                "exchange handler panicked: {e}"
                            ))))
                            .await;
                        break;
                    }
                };
                if let Some(status) = result.status.as_ref() {
                    if status.status_code != 0 {
                        let _ = tx
                            .send(Err(tonic::Status::internal(status.error_msgs.join("; "))))
                            .await;
                        break;
                    }
                }

                let ack = proto::novarocks::ExchangeResponse {
                    ack_sequence: req.sequence,
                };
                debug!(
                    "exchange ack SEND: finst={} node_id={} sender_id={} be_number={} eos={} seq={}",
                    format_uuid(req.finst_id_hi, req.finst_id_lo),
                    req.node_id,
                    req.sender_id,
                    req.be_number,
                    req.eos,
                    req.sequence
                );

                if tx.send(Ok(ack)).await.is_err() {
                    break;
                }
                debug!(
                    "exchange ack SENT: finst={} node_id={} sender_id={} be_number={} eos={} seq={}",
                    format_uuid(req.finst_id_hi, req.finst_id_lo),
                    req.node_id,
                    req.sender_id,
                    req.be_number,
                    req.eos,
                    req.sequence
                );
            }
        });

        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn transmit_runtime_filter(
        &self,
        request: tonic::Request<proto::starrocks::PTransmitRuntimeFilterParams>,
    ) -> Result<tonic::Response<proto::starrocks::PTransmitRuntimeFilterResult>, tonic::Status>
    {
        Ok(tonic::Response::new(
            internal_rpc::handle_transmit_runtime_filter(request.into_inner()),
        ))
    }

    async fn lookup(
        &self,
        request: tonic::Request<proto::starrocks::PLookUpRequest>,
    ) -> Result<tonic::Response<proto::starrocks::PLookUpResponse>, tonic::Status> {
        Ok(tonic::Response::new(internal_rpc::handle_lookup(
            request.into_inner(),
        )))
    }
}

#[derive(Default)]
pub struct StarletGrpcService;

fn staros_ok_status() -> proto::staros::StarStatus {
    proto::staros::StarStatus {
        status_code: proto::staros::StatusCode::Ok as i32,
        error_msg: String::new(),
        extra_info: Vec::new(),
    }
}

fn parse_add_shard_s3_config(
    path_info: &proto::staros::FilePathInfo,
) -> Result<Option<starlet_shard_registry::S3StoreConfig>, String> {
    starmgr::parse_s3_config_from_file_path_info(path_info)
}

fn summarize_top_counts(counts: &HashMap<String, usize>, top_n: usize) -> String {
    if counts.is_empty() {
        return "-".to_string();
    }
    let mut entries = counts
        .iter()
        .map(|(key, count)| (key.clone(), *count))
        .collect::<Vec<_>>();
    entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    entries
        .into_iter()
        .take(top_n.max(1))
        .map(|(key, count)| format!("{key}:{count}"))
        .collect::<Vec<_>>()
        .join(",")
}

#[tonic::async_trait]
impl proto::staros::starlet_server::Starlet for StarletGrpcService {
    async fn add_shard(
        &self,
        request: tonic::Request<proto::staros::AddShardRequest>,
    ) -> Result<tonic::Response<proto::staros::AddShardResponse>, tonic::Status> {
        let req = request.into_inner();
        starmgr::observe_starlet_service(&req.service_id);
        let worker_id = req.worker_id;
        let shard_count = req.shard_info.len();
        let shard_infos = req.shard_info;

        // AddShard may carry very large batches. Process in background so
        // heartbeat RPCs are not blocked by shard registry updates.
        tokio::task::spawn_blocking(move || {
            let mut updates = Vec::with_capacity(shard_infos.len());
            let mut invalid_shard_id = 0usize;
            let mut missing_full_path = 0usize;
            let mut invalid_s3_config = 0usize;
            let mut s3_config_count = 0usize;
            let mut s3_endpoint_counts: HashMap<String, usize> = HashMap::new();
            let mut s3_bucket_counts: HashMap<String, usize> = HashMap::new();
            for shard in &shard_infos {
                let Ok(shard_id) = i64::try_from(shard.shard_id) else {
                    invalid_shard_id += 1;
                    continue;
                };
                let Some(path_info) = shard.file_path_info.as_ref() else {
                    missing_full_path += 1;
                    continue;
                };
                if path_info.full_path.trim().is_empty() {
                    missing_full_path += 1;
                    continue;
                }
                let s3 = match parse_add_shard_s3_config(path_info) {
                    Ok(v) => v,
                    Err(err) => {
                        invalid_s3_config += 1;
                        warn!(
                            target: "novarocks::grpc",
                            shard_id,
                            error = %err,
                            "skip invalid AddShard S3 fs_info; only full_path is cached"
                        );
                        None
                    }
                };
                if let Some(cfg) = s3.as_ref() {
                    s3_config_count = s3_config_count.saturating_add(1);
                    *s3_endpoint_counts.entry(cfg.endpoint.clone()).or_insert(0) += 1;
                    *s3_bucket_counts.entry(cfg.bucket.clone()).or_insert(0) += 1;
                }
                updates.push((
                    shard_id,
                    starlet_shard_registry::StarletShardInfo {
                        full_path: path_info.full_path.clone(),
                        s3,
                    },
                ));
            }
            let upserted = starlet_shard_registry::upsert_many_infos(updates);
            info!(
                target: "novarocks::grpc",
                worker_id,
                shard_count,
                upserted,
                invalid_shard_id,
                missing_full_path,
                invalid_s3_config,
                s3_config_count,
                s3_endpoint_summary = %summarize_top_counts(&s3_endpoint_counts, 3),
                s3_bucket_summary = %summarize_top_counts(&s3_bucket_counts, 3),
                "processed starlet AddShard"
            );
        });

        info!(
            target: "novarocks::grpc",
            worker_id,
            shard_count,
            "accepted starlet AddShard"
        );
        Ok(tonic::Response::new(proto::staros::AddShardResponse {
            status: Some(staros_ok_status()),
        }))
    }

    async fn remove_shard(
        &self,
        request: tonic::Request<proto::staros::RemoveShardRequest>,
    ) -> Result<tonic::Response<proto::staros::RemoveShardResponse>, tonic::Status> {
        let req = request.into_inner();
        starmgr::observe_starlet_service(&req.service_id);
        let tablet_ids = req
            .shard_ids
            .iter()
            .filter_map(|id| i64::try_from(*id).ok())
            .collect::<Vec<_>>();
        let removed = starlet_shard_registry::remove_many(tablet_ids);
        info!(
            target: "novarocks::grpc",
            worker_id = req.worker_id,
            service_id = req.service_id,
            shard_count = req.shard_ids.len(),
            removed,
            "received starlet RemoveShard"
        );
        Ok(tonic::Response::new(proto::staros::RemoveShardResponse {
            status: Some(staros_ok_status()),
        }))
    }

    async fn starlet_heartbeat(
        &self,
        request: tonic::Request<proto::staros::StarletHeartbeatRequest>,
    ) -> Result<tonic::Response<proto::staros::StarletHeartbeatResponse>, tonic::Status> {
        let req = request.into_inner();
        starmgr::observe_starlet_heartbeat(
            &req.star_mgr_leader,
            &req.service_id,
            req.worker_group_id,
            req.worker_id,
        );
        info!(
            target: "novarocks::grpc",
            worker_id = req.worker_id,
            worker_group_id = req.worker_group_id,
            service_id = req.service_id,
            star_mgr_leader = req.star_mgr_leader,
            "received starlet StarletHeartbeat"
        );
        Ok(tonic::Response::new(
            proto::staros::StarletHeartbeatResponse {
                status: Some(staros_ok_status()),
            },
        ))
    }

    async fn write_cache(
        &self,
        request: tonic::Request<proto::staros::WriteCacheRequest>,
    ) -> Result<tonic::Response<proto::staros::WriteCacheResponse>, tonic::Status> {
        let req = request.into_inner();
        info!(
            target: "novarocks::grpc",
            shard_id = req.shard_id,
            payload_bytes = req.data.len(),
            "received starlet WriteCache"
        );
        Ok(tonic::Response::new(proto::staros::WriteCacheResponse {
            status: Some(staros_ok_status()),
        }))
    }
}

pub fn start_grpc_server(host: &str) -> Result<(), String> {
    {
        let state = grpc_server_state()
            .lock()
            .map_err(|_| "lock grpc server state failed".to_string())?;
        if state.started {
            return Ok(());
        }
    }

    let host = host.to_string();
    let grpc_http_port = http_port();
    let grpc_starlet_port = starlet_port();
    validate_grpc_ports(grpc_http_port, grpc_starlet_port)?;
    ensure_bindable(&host, grpc_http_port, "novarocks grpc/http")?;
    ensure_bindable(&host, grpc_starlet_port, "starlet grpc")?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let join_handle = std::thread::spawn(move || {
        info!(
            target: "novarocks::grpc",
            host = %host,
            http_port = grpc_http_port,
            starlet_port = grpc_starlet_port,
            "starting grpc servers"
        );
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(8)
            .build()
            .expect("build grpc server runtime");

        rt.block_on(async move {
            let http_addr: SocketAddr = format!("{host}:{grpc_http_port}")
                .parse()
                .expect("parse grpc/http bind addr");
            let starlet_addr: SocketAddr = format!("{host}:{grpc_starlet_port}")
                .parse()
                .expect("parse starlet bind addr");
            let mut http_shutdown = shutdown_rx.clone();
            let mut starlet_shutdown = shutdown_rx.clone();

            let svc = GrpcService::default();
            let svc = proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpcServer::new(svc)
                .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
            let grpc_routes = Routes::new(svc);
            let app = grpc_routes
                .into_axum_router()
                .route(
                    "/api/:db/:table/_stream_load",
                    put(stream_load_http::handle_stream_load),
                )
                .route(
                    "/api/transaction/load",
                    put(stream_load_http::handle_transaction_load),
                )
                .route(
                    "/api/transaction/:txn_op",
                    post(stream_load_http::handle_transaction_op)
                        .put(stream_load_http::handle_transaction_op),
                )
                .route(
                    "/api/_load_tracking/:hi/:lo",
                    get(load_tracking_http::handle_load_tracking_log),
                );
            let novarocks_server = Server::builder()
                .accept_http1(true)
                .add_routes(Routes::from(app))
                .serve_with_shutdown(http_addr, async move {
                    while !*http_shutdown.borrow() {
                        if http_shutdown.changed().await.is_err() {
                            break;
                        }
                    }
                });

            let starlet = StarletGrpcService::default();
            let starlet = proto::staros::starlet_server::StarletServer::new(starlet)
                .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
            let starlet_server = Server::builder().add_service(starlet).serve_with_shutdown(
                starlet_addr,
                async move {
                    while !*starlet_shutdown.borrow() {
                        if starlet_shutdown.changed().await.is_err() {
                            break;
                        }
                    }
                },
            );

            if let Err(e) = tokio::try_join!(novarocks_server, starlet_server) {
                error!(
                    target: "novarocks::grpc",
                    error = %e,
                    http_port = grpc_http_port,
                    starlet_port = grpc_starlet_port,
                    "grpc server stopped"
                );
            }
        });
    });

    let mut state = grpc_server_state()
        .lock()
        .map_err(|_| "lock grpc server state failed".to_string())?;
    if state.started {
        return Ok(());
    }
    state.started = true;
    state.bound_port = Some(grpc_http_port);
    state.shutdown_tx = Some(shutdown_tx);
    state.join_handle = Some(join_handle);
    Ok(())
}

pub fn grpc_server_bound_port() -> Result<u16, String> {
    let state = grpc_server_state()
        .lock()
        .map_err(|_| "lock grpc server state failed".to_string())?;
    if !state.started {
        return Err("grpc server not started".to_string());
    }
    state
        .bound_port
        .ok_or_else(|| "grpc server bound port unavailable".to_string())
}

pub fn stop_grpc_server() {
    let (shutdown_tx, join_handle) = {
        let mut state = match grpc_server_state().lock() {
            Ok(guard) => guard,
            Err(_) => return,
        };
        if !state.started {
            return;
        }
        state.started = false;
        state.bound_port = None;
        (state.shutdown_tx.take(), state.join_handle.take())
    };

    if let Some(tx) = shutdown_tx {
        let _ = tx.send(true);
    }
    if let Some(handle) = join_handle {
        let _ = handle.join();
    }
}

fn validate_grpc_ports(http_port: u16, starlet_port: u16) -> Result<(), String> {
    if http_port == starlet_port {
        return Err(format!(
            "invalid config: server.http_port ({http_port}) and server.starlet_port ({starlet_port}) must be different"
        ));
    }
    Ok(())
}

fn ensure_bindable(host: &str, port: u16, role: &str) -> Result<(), String> {
    let addr: SocketAddr = format!("{host}:{port}")
        .parse()
        .map_err(|e| format!("parse {role} bind addr failed: {e}"))?;
    let listener = TcpListener::bind(addr)
        .map_err(|e| format!("failed to bind {role} listener on {addr}: {e}"))?;
    drop(listener);
    Ok(())
}

/// Start a lightweight gRPC exchange server on a specific port.
///
/// Unlike [`start_grpc_server`] this does not require global config to be
/// initialised — the caller supplies the bind address directly.  Only the
/// exchange service is started (no starlet, no HTTP routes), which is
/// sufficient for standalone multi-fragment CTE execution.
pub fn start_grpc_exchange_server(host: &str, port: u16) -> Result<(), String> {
    {
        let state = grpc_server_state()
            .lock()
            .map_err(|_| "lock grpc server state failed".to_string())?;
        if state.started {
            return Ok(());
        }
    }

    let host = host.to_string();
    ensure_bindable(&host, port, "standalone grpc/exchange")?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let join_handle = std::thread::spawn(move || {
        info!(
            target: "novarocks::grpc",
            host = %host,
            port = port,
            "starting standalone grpc exchange server"
        );
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(4)
            .build()
            .expect("build standalone grpc server runtime");

        rt.block_on(async move {
            let addr: SocketAddr = format!("{host}:{port}")
                .parse()
                .expect("parse standalone grpc bind addr");
            let mut shutdown = shutdown_rx.clone();

            let svc = GrpcService::default();
            let svc = proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpcServer::new(svc)
                .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);

            let server = Server::builder()
                .add_service(svc)
                .serve_with_shutdown(addr, async move {
                    while !*shutdown.borrow() {
                        if shutdown.changed().await.is_err() {
                            break;
                        }
                    }
                });

            if let Err(e) = server.await {
                error!(
                    target: "novarocks::grpc",
                    error = %e,
                    port = port,
                    "standalone grpc exchange server stopped"
                );
            }
        });
    });

    let mut state = grpc_server_state()
        .lock()
        .map_err(|_| "lock grpc server state failed".to_string())?;
    if state.started {
        return Ok(());
    }
    state.started = true;
    state.bound_port = Some(port);
    state.shutdown_tx = Some(shutdown_tx);
    state.join_handle = Some(join_handle);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ensure_bindable, validate_grpc_ports};
    use std::net::TcpListener;

    #[test]
    fn test_validate_grpc_ports_accept_distinct_ports() {
        assert!(validate_grpc_ports(8040, 9070).is_ok());
    }

    #[test]
    fn test_validate_grpc_ports_reject_same_port() {
        let err = validate_grpc_ports(8040, 8040).expect_err("expected same-port validation error");
        assert!(err.contains("must be different"));
    }

    #[test]
    fn test_ensure_bindable_fails_for_occupied_port() {
        let occupied = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral test port");
        let occupied_port = occupied.local_addr().expect("get local addr").port();
        let err = ensure_bindable("127.0.0.1", occupied_port, "unit-test")
            .expect_err("expected bind failure");
        assert!(err.contains("failed to bind"));
        drop(occupied);
    }
}
