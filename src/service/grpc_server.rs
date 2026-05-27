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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::task::{Context, Poll};
use std::thread::JoinHandle;

use axum::Router;
use axum::http::{HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post, put};
use tokio::net::TcpListener as TokioTcpListener;
use tokio::sync::watch;
use tokio_stream::wrappers::ReceiverStream;
use tonic::body::boxed;
use tonic::codegen::Service;
use tonic::server::NamedService;
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
static SUBMIT_FRAGMENT_CALLS: AtomicUsize = AtomicUsize::new(0);
static FETCH_RESULT_CALLS: AtomicUsize = AtomicUsize::new(0);
static CANCEL_FRAGMENT_CALLS: AtomicUsize = AtomicUsize::new(0);

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
                if let Some(status) = result.status.as_ref()
                    && status.status_code != 0
                {
                    let _ = tx
                        .send(Err(tonic::Status::internal(status.error_msgs.join("; "))))
                        .await;
                    break;
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

    async fn exchange_unary(
        &self,
        request: tonic::Request<proto::novarocks::ExchangeRequest>,
    ) -> Result<tonic::Response<proto::novarocks::ExchangeResponse>, tonic::Status> {
        let req = request.into_inner();
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
        let result =
            tokio::task::spawn_blocking(move || internal_rpc::handle_transmit_chunk(params))
                .await
                .map_err(|e| {
                    tonic::Status::internal(format!("exchange_unary handler panicked: {e}"))
                })?;
        if let Some(status) = result.status.as_ref()
            && status.status_code != 0
        {
            return Err(tonic::Status::internal(status.error_msgs.join("; ")));
        }
        Ok(tonic::Response::new(proto::novarocks::ExchangeResponse {
            ack_sequence: req.sequence,
        }))
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

    async fn submit_fragment(
        &self,
        request: tonic::Request<proto::novarocks::SubmitFragmentRequest>,
    ) -> Result<tonic::Response<proto::novarocks::SubmitFragmentResponse>, tonic::Status> {
        let call_index = SUBMIT_FRAGMENT_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
        if crate::common::config::debug_fault_inject_submit_fail_after()
            .is_some_and(|successes| call_index > successes)
        {
            return Err(tonic::Status::unavailable(format!(
                "debug submit fault injected on call {call_index}"
            )));
        }
        let bytes = request.into_inner().exec_plan_fragment_params_thrift;
        // submit_exec_plan_fragment does thrift deserialization and pipeline setup,
        // which is CPU-bound. Offload to the blocking thread pool so tonic worker
        // threads remain free for I/O.
        let result = tokio::task::spawn_blocking(move || crate::submit_exec_plan_fragment(&bytes))
            .await
            .map_err(|e| {
                tonic::Status::internal(format!("submit_fragment handler panicked: {e}"))
            })?;
        match result {
            Ok(()) => Ok(tonic::Response::new(
                proto::novarocks::SubmitFragmentResponse {
                    status_code: 0,
                    message: String::new(),
                },
            )),
            Err(e) => Ok(tonic::Response::new(
                proto::novarocks::SubmitFragmentResponse {
                    status_code: 1,
                    message: e,
                },
            )),
        }
    }

    async fn fetch_result(
        &self,
        request: tonic::Request<proto::novarocks::FetchResultRequest>,
    ) -> Result<tonic::Response<proto::novarocks::FetchResultResponse>, tonic::Status> {
        use proto::novarocks::fetch_result_response::Status as FetchStatus;

        let req = request.into_inner();
        let finst_id = match req.finst_id {
            Some(id) => crate::UniqueId {
                hi: id.hi,
                lo: id.lo,
            },
            None => {
                return Ok(tonic::Response::new(
                    proto::novarocks::FetchResultResponse {
                        status: FetchStatus::Error as i32,
                        result_batch_thrift: vec![],
                        message: "missing finst_id in FetchResultRequest".to_string(),
                    },
                ));
            }
        };
        let call_index = FETCH_RESULT_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
        if crate::common::config::debug_fault_inject_fetch_not_ready_count()
            .is_some_and(|limit| call_index <= limit)
        {
            return Ok(tonic::Response::new(
                proto::novarocks::FetchResultResponse {
                    status: FetchStatus::NotReady as i32,
                    result_batch_thrift: vec![],
                    message: String::new(),
                },
            ));
        }

        // wait_fetch uses std::sync::Condvar::wait_timeout, which blocks the OS
        // thread for up to max_wait_ms. Offload to the blocking thread pool so
        // tonic worker threads remain free for I/O.
        use crate::runtime::result_buffer::{TryFetchResult, wait_fetch};
        let max_wait_ms = req.max_wait_ms;
        let fetch_result = tokio::task::spawn_blocking(move || wait_fetch(finst_id, max_wait_ms))
            .await
            .map_err(|e| tonic::Status::internal(format!("fetch_result handler panicked: {e}")))?;
        match fetch_result {
            TryFetchResult::Ready(result) => {
                let status = if result.eos {
                    FetchStatus::Eof
                } else {
                    FetchStatus::Ready
                };
                // Thrift-binary-encode the TResultBatch for transport.
                // The receiver (PR-4 RemoteDispatcher) deserializes the same bytes.
                let batch_bytes =
                    crate::common::thrift::thrift_serialize_result_batch(&result.result_batch);
                Ok(tonic::Response::new(
                    proto::novarocks::FetchResultResponse {
                        status: status as i32,
                        result_batch_thrift: batch_bytes,
                        message: String::new(),
                    },
                ))
            }
            TryFetchResult::NotReady => Ok(tonic::Response::new(
                proto::novarocks::FetchResultResponse {
                    status: FetchStatus::NotReady as i32,
                    result_batch_thrift: vec![],
                    message: String::new(),
                },
            )),
            TryFetchResult::Error(err) => Ok(tonic::Response::new(
                proto::novarocks::FetchResultResponse {
                    status: FetchStatus::Error as i32,
                    result_batch_thrift: vec![],
                    message: err.message,
                },
            )),
        }
    }

    async fn cancel_fragment(
        &self,
        request: tonic::Request<proto::novarocks::CancelFragmentRequest>,
    ) -> Result<tonic::Response<proto::novarocks::CancelFragmentResponse>, tonic::Status> {
        let req = request.into_inner();
        for id in &req.finst_ids {
            crate::cancel(crate::UniqueId {
                hi: id.hi,
                lo: id.lo,
            });
        }
        if crate::common::config::debug_emit_cancel_marker() {
            let count = CANCEL_FRAGMENT_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
            println!(
                "NOVAROCKS_CANCEL count={} finsts={} reason={}",
                count,
                req.finst_ids.len(),
                req.reason
            );
            let _ = std::io::Write::flush(&mut std::io::stdout());
        }
        Ok(tonic::Response::new(
            proto::novarocks::CancelFragmentResponse { status_code: 0 },
        ))
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

async fn grpc_unimplemented_fallback() -> impl IntoResponse {
    (
        StatusCode::OK,
        [
            (tonic::Status::GRPC_STATUS, HeaderValue::from_static("12")),
            (
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/grpc"),
            ),
        ],
    )
}

#[derive(Clone)]
struct AxumGrpcService<S> {
    inner: S,
}

impl<S> AxumGrpcService<S> {
    fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S> Service<axum::http::Request<axum::body::Body>> for AxumGrpcService<S>
where
    S: Service<
            axum::http::Request<tonic::body::BoxBody>,
            Response = axum::http::Response<tonic::body::BoxBody>,
            Error = std::convert::Infallible,
        > + Clone,
{
    type Response = axum::http::Response<tonic::body::BoxBody>;
    type Error = std::convert::Infallible;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: axum::http::Request<axum::body::Body>) -> Self::Future {
        self.inner.call(req.map(boxed))
    }
}

fn build_novarocks_http_app(grpc_routes: Routes) -> Router {
    grpc_routes
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
        )
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
            .thread_stack_size(crate::runtime::global_async_runtime::WORKER_STACK_SIZE_BYTES)
            .build()
            .expect("build grpc server runtime");

        rt.block_on(async move {
            let (http_addr, starlet_addr) =
                grpc_server_bind_addrs(&host, grpc_http_port, grpc_starlet_port)
                    .expect("parse grpc server bind addrs");
            let mut http_shutdown = shutdown_rx.clone();
            let mut starlet_shutdown = shutdown_rx.clone();

            let svc = GrpcService;
            let svc = proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpcServer::new(svc)
                .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
            let app = build_novarocks_http_app(Routes::new(svc));
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

            let starlet = StarletGrpcService;
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

/// Parse a gRPC bind address from a host string and port.
///
/// Handles bare IPv6 addresses (`::`, `::1`), bracketed IPv6 (`[::]`, `[::1]`),
/// and IPv4/hostname strings.  Bare and bracketed IPv6 forms are parsed via
/// `IpAddr` to avoid the `:::PORT` ambiguity that arises from naive
/// `format!("{host}:{port}")` string concatenation.
/// Build both gRPC server bind addresses from a single host string and two ports.
///
/// Uses [`parse_grpc_bind_addr`] for each port so bare IPv6 addresses like `::` and
/// `::1` are handled correctly, avoiding the `:::PORT` ambiguity produced by naive
/// `format!("{host}:{port}")` string concatenation.
pub(crate) fn grpc_server_bind_addrs(
    host: &str,
    http_port: u16,
    starlet_port: u16,
) -> Result<(SocketAddr, SocketAddr), String> {
    let http_addr = parse_grpc_bind_addr(host, http_port)
        .map_err(|e| format!("parse grpc/http bind addr failed: {e}"))?;
    let starlet_addr = parse_grpc_bind_addr(host, starlet_port)
        .map_err(|e| format!("parse starlet bind addr failed: {e}"))?;
    Ok((http_addr, starlet_addr))
}

pub(crate) fn parse_grpc_bind_addr(host: &str, port: u16) -> Result<SocketAddr, String> {
    // Strip brackets from bracketed IPv6 literals, e.g. `[::1]` -> `::1`.
    let bare = if host.starts_with('[') && host.ends_with(']') {
        &host[1..host.len() - 1]
    } else {
        host
    };

    // If the bare string is a valid IP literal, build SocketAddr directly.
    if let Ok(ip) = bare.parse::<std::net::IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }

    // Fallback for hostnames: use bracketed form for any host containing `:`.
    let formatted = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    formatted
        .parse::<SocketAddr>()
        .map_err(|e| format!("parse gRPC bind addr '{formatted}' failed: {e}"))
}

fn ensure_bindable(host: &str, port: u16, role: &str) -> Result<(), String> {
    let addr = parse_grpc_bind_addr(host, port)
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
            .worker_threads(8)
            .thread_stack_size(crate::runtime::global_async_runtime::WORKER_STACK_SIZE_BYTES)
            .build()
            .expect("build standalone grpc server runtime");

        rt.block_on(async move {
            let addr = parse_grpc_bind_addr(&host, port)
                .expect("parse standalone grpc bind addr");
            let mut shutdown = shutdown_rx.clone();

            let svc = GrpcService;
            let svc = proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpcServer::new(svc)
                .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
            let grpc_path = format!(
                "/{}/*rest",
                <proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpcServer<GrpcService> as NamedService>::NAME
            );
            let grpc_service = AxumGrpcService::new(svc);
            let app = Router::new()
                .route_service(&grpc_path, grpc_service)
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
                )
                .fallback(grpc_unimplemented_fallback);
            let listener = TokioTcpListener::bind(addr)
                .await
                .expect("bind standalone grpc/http addr");

            let server = axum::serve(listener, app).with_graceful_shutdown(async move {
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
    use super::{ensure_bindable, parse_grpc_bind_addr, validate_grpc_ports};
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, TcpListener};

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

    #[test]
    fn parse_grpc_bind_addr_bare_ipv6_wildcard() {
        let addr = parse_grpc_bind_addr("::", 9070).expect("parse :: wildcard");
        assert_eq!(addr.ip(), IpAddr::V6(Ipv6Addr::UNSPECIFIED));
        assert_eq!(addr.port(), 9070);
    }

    #[test]
    fn parse_grpc_bind_addr_bracketed_ipv6_wildcard() {
        let addr = parse_grpc_bind_addr("[::]", 9070).expect("parse [::] wildcard");
        assert_eq!(addr.ip(), IpAddr::V6(Ipv6Addr::UNSPECIFIED));
        assert_eq!(addr.port(), 9070);
    }

    #[test]
    fn parse_grpc_bind_addr_bracketed_ipv6_loopback() {
        let addr = parse_grpc_bind_addr("[::1]", 9070).expect("parse [::1]");
        assert_eq!(addr.ip(), IpAddr::V6(Ipv6Addr::LOCALHOST));
        assert_eq!(addr.port(), 9070);
    }

    #[test]
    fn parse_grpc_bind_addr_bare_ipv6_loopback() {
        let addr = parse_grpc_bind_addr("::1", 9070).expect("parse ::1");
        assert_eq!(addr.ip(), IpAddr::V6(Ipv6Addr::LOCALHOST));
        assert_eq!(addr.port(), 9070);
    }

    #[test]
    fn parse_grpc_bind_addr_ipv4() {
        let addr = parse_grpc_bind_addr("127.0.0.1", 9070).expect("parse 127.0.0.1");
        assert_eq!(addr.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(addr.port(), 9070);
    }

    #[test]
    fn parse_grpc_bind_addr_ipv4_wildcard() {
        let addr = parse_grpc_bind_addr("0.0.0.0", 9070).expect("parse 0.0.0.0");
        assert_eq!(addr.ip(), IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        assert_eq!(addr.port(), 9070);
    }

    // --- PR-4 regression: grpc_server_bind_addrs must use safe addr construction ---

    #[test]
    fn grpc_server_bind_addrs_bare_ipv6_wildcard_two_ports() {
        let (http, starlet) =
            super::grpc_server_bind_addrs("::", 8040, 9070).expect("bare :: two ports");
        assert_eq!(http.ip(), IpAddr::V6(Ipv6Addr::UNSPECIFIED));
        assert_eq!(http.port(), 8040);
        assert_eq!(starlet.ip(), IpAddr::V6(Ipv6Addr::UNSPECIFIED));
        assert_eq!(starlet.port(), 9070);
    }

    #[test]
    fn grpc_server_bind_addrs_bare_ipv6_loopback_two_ports() {
        let (http, starlet) =
            super::grpc_server_bind_addrs("::1", 8040, 9070).expect("bare ::1 two ports");
        assert_eq!(http.ip(), IpAddr::V6(Ipv6Addr::LOCALHOST));
        assert_eq!(http.port(), 8040);
        assert_eq!(starlet.ip(), IpAddr::V6(Ipv6Addr::LOCALHOST));
        assert_eq!(starlet.port(), 9070);
    }

    #[test]
    fn grpc_server_bind_addrs_ipv4_two_ports() {
        let (http, starlet) =
            super::grpc_server_bind_addrs("127.0.0.1", 8040, 9070).expect("ipv4 two ports");
        assert_eq!(http.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(http.port(), 8040);
        assert_eq!(starlet.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(starlet.port(), 9070);
    }
}

#[cfg(test)]
mod pr3_tests {
    use super::GrpcService;
    use super::proto::novarocks::fetch_result_response::Status as FetchStatus;
    use super::proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpc as _;
    use super::proto::novarocks::{
        CancelFragmentRequest, FetchResultRequest, PUniqueId, SubmitFragmentRequest,
    };
    use tonic::Request;

    #[tokio::test]
    async fn submit_fragment_thrift_decode_error_returns_business_error() {
        let svc = GrpcService::default();
        let req = Request::new(SubmitFragmentRequest {
            exec_plan_fragment_params_thrift: vec![0xff, 0xff, 0xff], // illegal thrift
        });
        let resp = svc.submit_fragment(req).await.expect("RPC level success");
        let body = resp.into_inner();
        assert_ne!(
            body.status_code, 0,
            "should return business error for bad thrift"
        );
        assert!(!body.message.is_empty());
    }

    #[tokio::test]
    async fn cancel_fragment_is_idempotent() {
        let svc = GrpcService::default();
        let req = Request::new(CancelFragmentRequest {
            finst_ids: vec![PUniqueId { hi: 1, lo: 2 }],
            reason: "test".to_string(),
        });
        let resp = svc.cancel_fragment(req).await.expect("RPC success");
        assert_eq!(resp.into_inner().status_code, 0);

        let req2 = Request::new(CancelFragmentRequest {
            finst_ids: vec![PUniqueId { hi: 1, lo: 2 }],
            reason: "test-2".to_string(),
        });
        let resp2 = svc.cancel_fragment(req2).await.expect("RPC success");
        assert_eq!(resp2.into_inner().status_code, 0);
    }

    #[tokio::test]
    async fn fetch_result_missing_finst_id_returns_error_status() {
        let svc = GrpcService::default();
        let req = Request::new(FetchResultRequest {
            finst_id: None,
            max_wait_ms: 0,
        });
        let resp = svc.fetch_result(req).await.expect("RPC level success");
        let body = resp.into_inner();
        assert_eq!(
            body.status,
            FetchStatus::Error as i32,
            "missing finst_id must return ERROR status"
        );
        assert!(!body.message.is_empty(), "error message must be non-empty");
        assert!(
            body.result_batch_thrift.is_empty(),
            "payload must be empty on error"
        );
    }

    #[tokio::test]
    async fn fetch_result_empty_open_buffer_returns_not_ready_without_wait() {
        use crate::common::types::UniqueId;
        use crate::runtime::result_buffer::create_sender;

        let finst_id = UniqueId { hi: 8801, lo: 8802 };
        create_sender(finst_id);

        let svc = GrpcService::default();
        let req = Request::new(FetchResultRequest {
            finst_id: Some(PUniqueId {
                hi: finst_id.hi,
                lo: finst_id.lo,
            }),
            max_wait_ms: 0,
        });
        let resp = svc.fetch_result(req).await.expect("RPC level success");
        let body = resp.into_inner();
        assert_eq!(
            body.status,
            FetchStatus::NotReady as i32,
            "empty open buffer with max_wait_ms=0 must return NOT_READY"
        );
    }

    #[tokio::test]
    async fn fetch_result_waits_for_ready_result() {
        use crate::common::types::{FetchResult, UniqueId};
        use crate::runtime::result_buffer::{create_sender, insert};

        let finst_id = UniqueId { hi: 8803, lo: 8804 };
        create_sender(finst_id);

        // Insert a result from a background thread after 20 ms.
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            insert(
                finst_id,
                FetchResult {
                    packet_seq: 0,
                    eos: false,
                    result_batch: crate::data::TResultBatch::new(
                        vec![b"hello".to_vec()],
                        false,
                        0,
                        None,
                    ),
                },
            );
        });

        let svc = GrpcService::default();
        let req = Request::new(FetchResultRequest {
            finst_id: Some(PUniqueId {
                hi: finst_id.hi,
                lo: finst_id.lo,
            }),
            max_wait_ms: 1000,
        });
        let resp = svc.fetch_result(req).await.expect("RPC level success");
        let body = resp.into_inner();
        assert_eq!(
            body.status,
            FetchStatus::Ready as i32,
            "should return READY after delayed insert with max_wait_ms=1000"
        );
        assert!(
            !body.result_batch_thrift.is_empty(),
            "result_batch_thrift payload must be non-empty"
        );
    }

    #[tokio::test]
    async fn fetch_result_buffer_error_returns_error_status() {
        use crate::common::types::UniqueId;
        use crate::runtime::result_buffer::{close_error, create_sender};

        let finst_id = UniqueId { hi: 8807, lo: 8808 };
        create_sender(finst_id);
        close_error(finst_id, "boom".to_string());

        let svc = GrpcService::default();
        let req = Request::new(FetchResultRequest {
            finst_id: Some(PUniqueId {
                hi: finst_id.hi,
                lo: finst_id.lo,
            }),
            max_wait_ms: 0,
        });
        let resp = svc.fetch_result(req).await.expect("RPC level success");
        let body = resp.into_inner();
        assert_eq!(
            body.status,
            FetchStatus::Error as i32,
            "close_error buffer must return ERROR status"
        );
        assert_eq!(body.message, "boom", "error message must match");
        assert!(
            body.result_batch_thrift.is_empty(),
            "payload must be empty on error"
        );
    }

    #[tokio::test]
    async fn fetch_result_closed_buffer_returns_eof() {
        use crate::common::types::UniqueId;
        use crate::runtime::result_buffer::{close_ok, create_sender};

        let finst_id = UniqueId { hi: 8805, lo: 8806 };
        create_sender(finst_id);
        close_ok(finst_id);

        let svc = GrpcService::default();
        let req = Request::new(FetchResultRequest {
            finst_id: Some(PUniqueId {
                hi: finst_id.hi,
                lo: finst_id.lo,
            }),
            max_wait_ms: 0,
        });
        let resp = svc.fetch_result(req).await.expect("RPC level success");
        let body = resp.into_inner();
        assert_eq!(
            body.status,
            FetchStatus::Eof as i32,
            "closed buffer must return EOF"
        );
    }
}
