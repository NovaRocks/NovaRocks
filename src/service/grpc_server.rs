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
use std::sync::OnceLock;

use axum::routing::{post, put};
use tokio_stream::wrappers::ReceiverStream;
use tonic::service::Routes;
use tonic::transport::Server;

use crate::common::config::{http_port, starlet_port};
use crate::common::ids::SlotId;
use crate::runtime::exchange;
use crate::runtime::lookup::{decode_column_ipc, encode_column_ipc, execute_lookup_request};
use crate::runtime::query_context::{QueryId, query_context_manager, query_expire_durations};
use crate::runtime::starlet_shard_registry;
use crate::service::stream_load_http;
use crate::novarocks_logging::{error, info, warn};

pub mod proto {
    pub mod novarocks {
        tonic::include_proto!("novarocks");
    }
    pub mod starrocks {
        tonic::include_proto!("starrocks");
    }
    pub mod staros {
        tonic::include_proto!("staros");
    }
}

const GRPC_MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

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

                let payload = req.payload;
                let payload_size = payload.len();
                let decode_start = std::time::Instant::now();
                let chunks = match exchange::decode_chunks(&payload) {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = tx
                            .send(Err(tonic::Status::invalid_argument(format!(
                                "exchange decode failed: {e}"
                            ))))
                            .await;
                        break;
                    }
                };
                let decode_ns = decode_start.elapsed().as_nanos() as u128;

                exchange::push_chunks_with_stats(
                    exchange::ExchangeKey {
                        finst_id_hi: req.finst_id_hi,
                        finst_id_lo: req.finst_id_lo,
                        node_id: req.node_id,
                    },
                    req.sender_id,
                    req.be_number,
                    chunks,
                    req.eos,
                    payload_size,
                    decode_ns,
                );

                let ack = proto::novarocks::ExchangeResponse {
                    ack_sequence: req.sequence,
                };
                debug!(
                    "exchange ack SEND: finst={}:{} node_id={} sender_id={} be_number={} eos={} seq={}",
                    req.finst_id_hi,
                    req.finst_id_lo,
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
                    "exchange ack SENT: finst={}:{} node_id={} sender_id={} be_number={} eos={} seq={}",
                    req.finst_id_hi,
                    req.finst_id_lo,
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
        let params = request.get_ref();
        let Some(filter_id) = params.filter_id else {
            let mut response = proto::starrocks::PTransmitRuntimeFilterResult {
                status: Some(proto::starrocks::StatusPb {
                    status_code: 1,
                    error_msgs: vec!["missing filter_id for transmit_runtime_filter".to_string()],
                }),
                filter_id: Some(0),
            };
            if let Some(status) = response.status.as_mut() {
                status.status_code = 1;
            }
            return Ok(tonic::Response::new(response));
        };
        let mut response = proto::starrocks::PTransmitRuntimeFilterResult {
            status: Some(proto::starrocks::StatusPb {
                status_code: 0,
                error_msgs: Vec::new(),
            }),
            filter_id: Some(filter_id),
        };

        let Some(query_id) = params.query_id.as_ref() else {
            if let Some(status) = response.status.as_mut() {
                status.status_code = 1;
                status
                    .error_msgs
                    .push("missing query_id for transmit_runtime_filter".to_string());
            }
            return Ok(tonic::Response::new(response));
        };
        let query_id = QueryId {
            hi: query_id.hi,
            lo: query_id.lo,
        };

        let Some(payload) = params.data.as_ref() else {
            if let Some(status) = response.status.as_mut() {
                status.status_code = 1;
                status.error_msgs.push(format!(
                    "missing runtime filter payload: query_id={}:{} filter_id={}",
                    query_id.hi, query_id.lo, filter_id
                ));
            }
            return Ok(tonic::Response::new(response));
        };

        if payload.is_empty() {
            if let Some(status) = response.status.as_mut() {
                status.status_code = 1;
                status.error_msgs.push(format!(
                    "runtime filter payload is empty: query_id={}:{} filter_id={}",
                    query_id.hi, query_id.lo, filter_id
                ));
            }
            return Ok(tonic::Response::new(response));
        }

        let is_partial = params.is_partial.unwrap_or(false);
        if is_partial {
            let Some(worker) =
                query_context_manager().get_or_create_runtime_filter_worker(query_id)
            else {
                let (delivery_expire, query_expire) = query_expire_durations(None);
                let _ = query_context_manager().ensure_context(
                    query_id,
                    false,
                    delivery_expire,
                    query_expire,
                );
                let _ = query_context_manager().enqueue_pending_runtime_filter(
                    query_id,
                    filter_id,
                    params.build_be_number.unwrap_or(0),
                    payload.to_vec(),
                );
                return Ok(tonic::Response::new(response));
            };
            let build_be_number = params.build_be_number.unwrap_or(0);
            if let Err(err) = worker.receive_partial(filter_id, payload, build_be_number) {
                warn!(
                    "receive_partial_runtime_filter failed: query_id={}:{} filter_id={} err={}",
                    query_id.hi, query_id.lo, filter_id, err
                );
                if let Some(status) = response.status.as_mut() {
                    status.status_code = 1;
                    status.error_msgs.push(err);
                }
            }
            return Ok(tonic::Response::new(response));
        }

        let Some(hub) = query_context_manager().get_runtime_filter_hub(query_id) else {
            if let Some(status) = response.status.as_mut() {
                status.status_code = 1;
                status.error_msgs.push(format!(
                    "runtime filter hub not found: query_id={}:{}",
                    query_id.hi, query_id.lo
                ));
            }
            return Ok(tonic::Response::new(response));
        };

        if let Err(err) = hub.receive_remote_filter(filter_id, payload) {
            warn!(
                "receive_remote_filter failed: query_id={}:{} filter_id={} err={}",
                query_id.hi, query_id.lo, filter_id, err
            );
            if let Some(status) = response.status.as_mut() {
                status.status_code = 1;
                status.error_msgs.push(err);
            }
        }
        Ok(tonic::Response::new(response))
    }

    async fn lookup(
        &self,
        request: tonic::Request<proto::starrocks::PLookUpRequest>,
    ) -> Result<tonic::Response<proto::starrocks::PLookUpResponse>, tonic::Status> {
        let req = request.into_inner();
        let mut response = proto::starrocks::PLookUpResponse {
            status: Some(proto::starrocks::StatusPb {
                status_code: 0,
                error_msgs: Vec::new(),
            }),
            columns: Vec::new(),
        };

        let Some(query_id) = req.query_id.as_ref() else {
            if let Some(status) = response.status.as_mut() {
                status.status_code = 1;
                status
                    .error_msgs
                    .push("missing query_id for lookup".to_string());
            }
            return Ok(tonic::Response::new(response));
        };
        let query_id = QueryId {
            hi: query_id.hi,
            lo: query_id.lo,
        };
        let Some(tuple_id) = req.request_tuple_id else {
            if let Some(status) = response.status.as_mut() {
                status.status_code = 1;
                status
                    .error_msgs
                    .push("missing request_tuple_id for lookup".to_string());
            }
            return Ok(tonic::Response::new(response));
        };

        let mut request_columns = HashMap::new();
        for col in req.request_columns {
            let Some(slot_id) = col.slot_id else {
                if let Some(status) = response.status.as_mut() {
                    status.status_code = 1;
                    status
                        .error_msgs
                        .push("lookup request column missing slot_id".to_string());
                }
                return Ok(tonic::Response::new(response));
            };
            if col.data.as_ref().map_or(true, |data| data.is_empty()) {
                if let Some(status) = response.status.as_mut() {
                    status.status_code = 1;
                    status
                        .error_msgs
                        .push(format!("lookup request column {} missing data", slot_id));
                }
                return Ok(tonic::Response::new(response));
            }
            let slot_id = match SlotId::try_from(slot_id) {
                Ok(v) => v,
                Err(err) => {
                    if let Some(status) = response.status.as_mut() {
                        status.status_code = 1;
                        status.error_msgs.push(err);
                    }
                    return Ok(tonic::Response::new(response));
                }
            };
            let data = match col.data.as_ref() {
                Some(v) => v,
                None => {
                    if let Some(status) = response.status.as_mut() {
                        status.status_code = 1;
                        status
                            .error_msgs
                            .push(format!("lookup request column {} missing data", slot_id));
                    }
                    return Ok(tonic::Response::new(response));
                }
            };
            let array = match decode_column_ipc(data) {
                Ok(arr) => arr,
                Err(err) => {
                    if let Some(status) = response.status.as_mut() {
                        status.status_code = 1;
                        status.error_msgs.push(err);
                    }
                    return Ok(tonic::Response::new(response));
                }
            };
            request_columns.insert(slot_id, array);
        }

        match execute_lookup_request(query_id, tuple_id, request_columns) {
            Ok(columns) => {
                for (slot_id, array) in columns {
                    let data = match encode_column_ipc(&array) {
                        Ok(v) => v,
                        Err(err) => {
                            if let Some(status) = response.status.as_mut() {
                                status.status_code = 1;
                                status.error_msgs.push(err);
                            }
                            return Ok(tonic::Response::new(response));
                        }
                    };
                    response.columns.push(proto::starrocks::PColumn {
                        slot_id: Some(slot_id.as_u32() as i32),
                        data_size: Some(data.len() as i64),
                        data: Some(data),
                    });
                }
            }
            Err(err) => {
                if let Some(status) = response.status.as_mut() {
                    status.status_code = 1;
                    status.error_msgs.push(err);
                }
            }
        }
        Ok(tonic::Response::new(response))
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

fn parse_bucket_from_full_path(full_path: &str) -> Option<String> {
    let trimmed = full_path.trim();
    for scheme in ["s3://", "oss://"] {
        if let Some(rest) = trimmed.strip_prefix(scheme) {
            let bucket = rest.split('/').next().unwrap_or_default().trim();
            if !bucket.is_empty() {
                return Some(bucket.to_string());
            }
        }
    }
    None
}

fn parse_add_shard_s3_config(
    path_info: &proto::staros::FilePathInfo,
) -> Result<Option<starlet_shard_registry::S3StoreConfig>, String> {
    let Some(fs_info) = path_info.fs_info.as_ref() else {
        return Ok(None);
    };
    if fs_info.fs_type != proto::staros::FileStoreType::S3 as i32 {
        return Ok(None);
    }

    let s3 = fs_info
        .s3_fs_info
        .as_ref()
        .ok_or_else(|| "AddShard fs_type=S3 but s3_fs_info is missing".to_string())?;
    let endpoint = s3.endpoint.trim();
    if endpoint.is_empty() {
        return Err("AddShard S3 endpoint is empty".to_string());
    }

    let bucket = if s3.bucket.trim().is_empty() {
        parse_bucket_from_full_path(&path_info.full_path).ok_or_else(|| {
            "AddShard S3 bucket is empty and cannot be inferred from full_path".to_string()
        })?
    } else {
        s3.bucket.trim().to_string()
    };
    let root = s3.path_prefix.trim_matches('/').to_string();
    let credential = s3
        .credential
        .as_ref()
        .ok_or_else(|| "AddShard S3 credential is missing".to_string())?;
    let (access_key_id, access_key_secret) = match credential.credential.as_ref() {
        Some(proto::staros::aws_credential_info::Credential::SimpleCredential(simple)) => {
            let ak = simple.access_key.trim();
            let sk = simple.access_key_secret.trim();
            if ak.is_empty() || sk.is_empty() {
                return Err(
                    "AddShard S3 simple credential contains empty access key/secret".to_string(),
                );
            }
            (ak.to_string(), sk.to_string())
        }
        Some(other) => resolve_add_shard_s3_credentials_fallback(
            &path_info.full_path,
            Some(other),
            endpoint,
            &bucket,
        )?,
        None => resolve_add_shard_s3_credentials_fallback(
            &path_info.full_path,
            None,
            endpoint,
            &bucket,
        )?,
    };

    let enable_path_style_access = match s3.path_style_access {
        1 => Some(true),
        2 => Some(false),
        _ => None,
    };
    let region = s3
        .region
        .trim()
        .split_once('\0')
        .map(|(v, _)| v)
        .unwrap_or(s3.region.as_str())
        .trim();
    let region = (!region.is_empty()).then_some(region.to_string());

    Ok(Some(starlet_shard_registry::S3StoreConfig {
        endpoint: endpoint.to_string(),
        bucket,
        root,
        access_key_id,
        access_key_secret,
        region,
        enable_path_style_access,
    }))
}

fn resolve_add_shard_s3_credentials_fallback(
    full_path: &str,
    credential_mode: Option<&proto::staros::aws_credential_info::Credential>,
    endpoint: &str,
    bucket: &str,
) -> Result<(String, String), String> {
    if let Some(existing) = starlet_shard_registry::infer_s3_config_for_path(full_path)
        && !existing.access_key_id.trim().is_empty()
        && !existing.access_key_secret.trim().is_empty()
    {
        return Ok((existing.access_key_id, existing.access_key_secret));
    }

    let access_key_id = std::env::var("AWS_ACCESS_KEY_ID")
        .ok()
        .or_else(|| std::env::var("AWS_ACCESS_KEY").ok())
        .or_else(|| std::env::var("MINIO_ROOT_USER").ok())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    let access_key_secret = std::env::var("AWS_SECRET_ACCESS_KEY")
        .ok()
        .or_else(|| std::env::var("AWS_SECRET_KEY").ok())
        .or_else(|| std::env::var("MINIO_ROOT_PASSWORD").ok())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    if let (Some(ak), Some(sk)) = (access_key_id, access_key_secret) {
        return Ok((ak, sk));
    }

    let mode = match credential_mode {
        Some(proto::staros::aws_credential_info::Credential::SimpleCredential(_)) => "SIMPLE",
        Some(proto::staros::aws_credential_info::Credential::DefaultCredential(_)) => "DEFAULT",
        Some(proto::staros::aws_credential_info::Credential::ProfileCredential(_)) => {
            "INSTANCE_PROFILE"
        }
        Some(proto::staros::aws_credential_info::Credential::AssumeRoleCredential(_)) => {
            "ASSUME_ROLE"
        }
        None => "EMPTY",
    };
    Err(format!(
        "AddShard S3 credential fallback failed: mode={}, endpoint={}, bucket={}, full_path={}",
        mode, endpoint, bucket, full_path
    ))
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
        let tablet_ids = req
            .shard_ids
            .iter()
            .filter_map(|id| i64::try_from(*id).ok())
            .collect::<Vec<_>>();
        let removed = starlet_shard_registry::remove_many(tablet_ids);
        info!(
            target: "novarocks::grpc",
            worker_id = req.worker_id,
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
        info!(
            target: "novarocks::grpc",
            worker_id = req.worker_id,
            worker_group_id = req.worker_group_id,
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
    static STARTED: OnceLock<()> = OnceLock::new();
    if STARTED.get().is_some() {
        return Ok(());
    }

    let host = host.to_string();
    let grpc_http_port = http_port();
    let grpc_starlet_port = starlet_port();
    validate_grpc_ports(grpc_http_port, grpc_starlet_port)?;
    ensure_bindable(&host, grpc_http_port, "novarocks grpc/http")?;
    ensure_bindable(&host, grpc_starlet_port, "starlet grpc")?;

    STARTED
        .set(())
        .map_err(|_| "grpc server already started".to_string())?;

    std::thread::spawn(move || {
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
                );
            let novarocks_server = Server::builder()
                .accept_http1(true)
                .add_routes(Routes::from(app))
                .serve(http_addr);

            let starlet = StarletGrpcService::default();
            let starlet = proto::staros::starlet_server::StarletServer::new(starlet)
                .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
            let starlet_server = Server::builder().add_service(starlet).serve(starlet_addr);

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
    Ok(())
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
