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

use tonic::transport::{Channel, Endpoint};

use crate::runtime::global_async_runtime::data_block_on;
use crate::runtime::starlet_shard_registry::{self, S3StoreConfig, StarletShardInfo};
use crate::service::grpc_proto::staros;

const STARMGR_SERVICE_NAME: &str = "starrocks";
const DEFAULT_WORKER_GROUP_ID: u64 = 0;
const GRPC_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const GRPC_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const GRPC_MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug, Default)]
struct StarMgrState {
    leader_addr: Option<String>,
    service_id: Option<String>,
    worker_group_id: Option<u64>,
    worker_id: Option<u64>,
}

#[derive(Clone, Debug)]
struct ResolvedRouting {
    leader_addr: String,
    service_id: String,
    worker_group_id: u64,
}

#[derive(Default)]
struct ChannelCache {
    mu: Mutex<HashMap<String, Channel>>,
}

static STARMGR_STATE: OnceLock<Mutex<StarMgrState>> = OnceLock::new();
static CHANNEL_CACHE: OnceLock<ChannelCache> = OnceLock::new();

fn state() -> &'static Mutex<StarMgrState> {
    STARMGR_STATE.get_or_init(|| Mutex::new(StarMgrState::default()))
}

fn channels() -> &'static ChannelCache {
    CHANNEL_CACHE.get_or_init(|| ChannelCache {
        mu: Mutex::new(HashMap::new()),
    })
}

fn normalize_optional_text(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then_some(trimmed.to_string())
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

fn resolve_s3_credentials_fallback(
    full_path: &str,
    credential_mode: Option<&staros::aws_credential_info::Credential>,
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
        Some(staros::aws_credential_info::Credential::SimpleCredential(_)) => "SIMPLE",
        Some(staros::aws_credential_info::Credential::DefaultCredential(_)) => "DEFAULT",
        Some(staros::aws_credential_info::Credential::ProfileCredential(_)) => "INSTANCE_PROFILE",
        Some(staros::aws_credential_info::Credential::AssumeRoleCredential(_)) => "ASSUME_ROLE",
        None => "EMPTY",
    };
    Err(format!(
        "StarOS S3 credential fallback failed: mode={mode}, endpoint={endpoint}, bucket={bucket}, full_path={full_path}"
    ))
}

pub(crate) fn parse_s3_config_from_file_path_info(
    path_info: &staros::FilePathInfo,
) -> Result<Option<S3StoreConfig>, String> {
    let Some(fs_info) = path_info.fs_info.as_ref() else {
        return Ok(None);
    };
    if fs_info.fs_type != staros::FileStoreType::S3 as i32 {
        return Ok(None);
    }

    let s3 = fs_info
        .s3_fs_info
        .as_ref()
        .ok_or_else(|| "StarOS fs_type=S3 but s3_fs_info is missing".to_string())?;
    let endpoint = s3.endpoint.trim();
    if endpoint.is_empty() {
        return Err("StarOS S3 endpoint is empty".to_string());
    }

    let bucket = if s3.bucket.trim().is_empty() {
        parse_bucket_from_full_path(&path_info.full_path).ok_or_else(|| {
            "StarOS S3 bucket is empty and cannot be inferred from full_path".to_string()
        })?
    } else {
        s3.bucket.trim().to_string()
    };
    let root = s3.path_prefix.trim_matches('/').to_string();
    let credential = s3
        .credential
        .as_ref()
        .ok_or_else(|| "StarOS S3 credential is missing".to_string())?;
    let (access_key_id, access_key_secret) = match credential.credential.as_ref() {
        Some(staros::aws_credential_info::Credential::SimpleCredential(simple)) => {
            let ak = simple.access_key.trim();
            let sk = simple.access_key_secret.trim();
            if ak.is_empty() || sk.is_empty() {
                return Err(
                    "StarOS S3 simple credential contains empty access key/secret".to_string(),
                );
            }
            (ak.to_string(), sk.to_string())
        }
        Some(other) => {
            resolve_s3_credentials_fallback(&path_info.full_path, Some(other), endpoint, &bucket)?
        }
        None => resolve_s3_credentials_fallback(&path_info.full_path, None, endpoint, &bucket)?,
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

    Ok(Some(S3StoreConfig {
        endpoint: endpoint.to_string(),
        bucket,
        root,
        access_key_id,
        access_key_secret,
        region,
        enable_path_style_access,
    }))
}

fn shard_info_to_registry_info(
    shard: &staros::ShardInfo,
) -> Result<(i64, StarletShardInfo), String> {
    let tablet_id = i64::try_from(shard.shard_id).map_err(|_| {
        format!(
            "StarManager returned shard_id out of i64 range: {}",
            shard.shard_id
        )
    })?;
    let path_info = shard.file_path.as_ref().ok_or_else(|| {
        format!("StarManager GetShard missing file_path for shard_id={tablet_id}")
    })?;
    let full_path = path_info.full_path.trim();
    if full_path.is_empty() {
        return Err(format!(
            "StarManager GetShard returned empty full_path for shard_id={tablet_id}"
        ));
    }
    let s3 = parse_s3_config_from_file_path_info(path_info)?;
    Ok((
        tablet_id,
        StarletShardInfo {
            full_path: full_path.to_string(),
            s3,
        },
    ))
}

pub(crate) fn observe_starlet_service(service_id: &str) {
    let Some(service_id) = normalize_optional_text(service_id) else {
        return;
    };
    if let Ok(mut guard) = state().lock() {
        guard.service_id = Some(service_id);
    }
}

pub(crate) fn observe_starlet_heartbeat(
    star_mgr_leader: &str,
    service_id: &str,
    worker_group_id: u64,
    worker_id: u64,
) {
    if let Ok(mut guard) = state().lock() {
        if let Some(leader_addr) = normalize_optional_text(star_mgr_leader) {
            guard.leader_addr = Some(leader_addr);
        }
        if let Some(service_id) = normalize_optional_text(service_id) {
            guard.service_id = Some(service_id);
        }
        guard.worker_group_id = Some(worker_group_id);
        guard.worker_id = Some(worker_id);
    }
}

fn current_state_snapshot() -> StarMgrState {
    state()
        .lock()
        .map(|guard| guard.clone())
        .unwrap_or_default()
}

fn update_service_id_cache(service_id: String) {
    if let Ok(mut guard) = state().lock() {
        guard.service_id = Some(service_id);
    }
}

async fn get_channel(leader_addr: &str) -> Result<Channel, String> {
    if let Some(ch) = channels()
        .mu
        .lock()
        .map_err(|_| "lock StarManager channel cache failed".to_string())?
        .get(leader_addr)
        .cloned()
    {
        return Ok(ch);
    }

    let endpoint = format!("http://{leader_addr}")
        .parse::<Endpoint>()
        .map_err(|e| format!("invalid StarManager endpoint '{leader_addr}': {e}"))?
        .connect_timeout(GRPC_CONNECT_TIMEOUT)
        .timeout(GRPC_REQUEST_TIMEOUT)
        .tcp_keepalive(Some(Duration::from_secs(60)));
    let channel = endpoint
        .connect()
        .await
        .map_err(|e| format!("connect StarManager leader {leader_addr} failed: {e}"))?;
    channels()
        .mu
        .lock()
        .map_err(|_| "lock StarManager channel cache failed".to_string())?
        .insert(leader_addr.to_string(), channel.clone());
    Ok(channel)
}

fn require_ok_status(status: Option<&staros::StarStatus>, op: &str) -> Result<(), String> {
    // StarManager uses proto3 responses. Successful replies may omit the zero-value `status`
    // message entirely, so treat missing status as implicit OK instead of a protocol error.
    let Some(status) = status else {
        return Ok(());
    };
    if status.status_code == staros::StatusCode::Ok as i32 {
        return Ok(());
    }
    let code = staros::StatusCode::try_from(status.status_code)
        .map(|v| format!("{v:?}"))
        .unwrap_or_else(|_| status.status_code.to_string());
    if status.error_msg.trim().is_empty() {
        Err(format!("StarManager {op} failed with status_code={code}"))
    } else {
        Err(format!(
            "StarManager {op} failed with status_code={code}: {}",
            status.error_msg
        ))
    }
}

async fn resolve_routing() -> Result<ResolvedRouting, String> {
    let snapshot = current_state_snapshot();
    let leader_addr = snapshot.leader_addr.ok_or_else(|| {
        "StarManager leader is unknown; waiting for StarletHeartbeat to provide star_mgr_leader"
            .to_string()
    })?;

    let service_id = match snapshot.service_id {
        Some(service_id) if !service_id.trim().is_empty() => service_id,
        _ => {
            let channel = get_channel(&leader_addr).await?;
            let mut client = staros::star_manager_client::StarManagerClient::new(channel)
                .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
            let response = client
                .get_service(staros::GetServiceRequest {
                    identifier: Some(staros::get_service_request::Identifier::ServiceName(
                        STARMGR_SERVICE_NAME.to_string(),
                    )),
                })
                .await
                .map_err(|e| format!("StarManager GetService failed: {e}"))?;
            let response = response.into_inner();
            require_ok_status(response.status.as_ref(), "GetService")?;
            let service_info = response
                .service_info
                .ok_or_else(|| "StarManager GetService missing service_info".to_string())?;
            if service_info.service_id.trim().is_empty() {
                return Err("StarManager GetService returned empty service_id".to_string());
            }
            update_service_id_cache(service_info.service_id.clone());
            service_info.service_id
        }
    };

    Ok(ResolvedRouting {
        leader_addr,
        service_id,
        worker_group_id: snapshot.worker_group_id.unwrap_or(DEFAULT_WORKER_GROUP_ID),
    })
}

async fn retrieve_shard_infos_async(
    tablet_ids: &[i64],
) -> Result<HashMap<i64, StarletShardInfo>, String> {
    let routing = resolve_routing().await?;
    let shard_ids = tablet_ids
        .iter()
        .copied()
        .map(|tablet_id| {
            u64::try_from(tablet_id)
                .map_err(|_| format!("tablet_id out of u64 range for GetShard: {tablet_id}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let channel = get_channel(&routing.leader_addr).await?;
    let mut client = staros::star_manager_client::StarManagerClient::new(channel)
        .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
        .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
    let response = client
        .get_shard(staros::GetShardRequest {
            service_id: routing.service_id,
            shard_id: shard_ids,
            worker_group_id: routing.worker_group_id,
            no_wait_replica_allocation: false,
        })
        .await
        .map_err(|e| format!("StarManager GetShard failed: {e}"))?;
    let response = response.into_inner();
    require_ok_status(response.status.as_ref(), "GetShard")?;

    let mut recovered = HashMap::with_capacity(response.shard_info.len());
    for shard in &response.shard_info {
        let (tablet_id, info) = shard_info_to_registry_info(shard)?;
        recovered.insert(tablet_id, info);
    }
    Ok(recovered)
}

pub(crate) fn retrieve_shard_infos(
    tablet_ids: &[i64],
) -> Result<HashMap<i64, StarletShardInfo>, String> {
    let mut deduped = tablet_ids
        .iter()
        .copied()
        .filter(|tablet_id| *tablet_id > 0)
        .collect::<Vec<_>>();
    deduped.sort_unstable();
    deduped.dedup();
    if deduped.is_empty() {
        return Ok(HashMap::new());
    }

    let recovered = data_block_on(retrieve_shard_infos_async(&deduped))??;
    if !recovered.is_empty() {
        let _ = starlet_shard_registry::upsert_many_infos(
            recovered
                .iter()
                .map(|(tablet_id, info)| (*tablet_id, info.clone())),
        );
    }
    Ok(recovered)
}

pub(crate) fn retrieve_shard_info(tablet_id: i64) -> Result<Option<StarletShardInfo>, String> {
    let recovered = retrieve_shard_infos(&[tablet_id])?;
    Ok(recovered.get(&tablet_id).cloned())
}

#[cfg(test)]
pub(crate) fn clear_state_for_test() {
    if let Ok(mut guard) = state().lock() {
        *guard = StarMgrState::default();
    }
    if let Ok(mut guard) = channels().mu.lock() {
        guard.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::{
        clear_state_for_test, observe_starlet_heartbeat, parse_s3_config_from_file_path_info,
        require_ok_status, resolve_routing,
    };
    use crate::runtime::global_async_runtime::data_block_on;
    use crate::service::grpc_proto::staros;

    fn sample_file_path_info() -> staros::FilePathInfo {
        staros::FilePathInfo {
            fs_info: Some(staros::FileStoreInfo {
                fs_type: staros::FileStoreType::S3 as i32,
                s3_fs_info: Some(staros::S3FileStoreInfo {
                    bucket: "bucket".to_string(),
                    region: "us-east-1".to_string(),
                    endpoint: "http://127.0.0.1:9000".to_string(),
                    credential: Some(staros::AwsCredentialInfo {
                        credential: Some(
                            staros::aws_credential_info::Credential::SimpleCredential(
                                staros::AwsSimpleCredentialInfo {
                                    access_key: "ak".to_string(),
                                    access_key_secret: "sk".to_string(),
                                    encrypted: false,
                                },
                            ),
                        ),
                    }),
                    path_prefix: "lake/root".to_string(),
                    partitioned_prefix_enabled: false,
                    num_partitioned_prefix: 0,
                    path_style_access: 1,
                }),
                ..Default::default()
            }),
            full_path: "s3://bucket/lake/root/10001".to_string(),
            base_file_path_info: Vec::new(),
        }
    }

    #[test]
    fn heartbeat_updates_routing_state() {
        clear_state_for_test();
        observe_starlet_heartbeat("127.0.0.1:6091", "service-1", 7, 9);
        let routing = data_block_on(resolve_routing())
            .expect("block on resolve routing")
            .expect("resolve routing");
        assert_eq!(routing.leader_addr, "127.0.0.1:6091");
        assert_eq!(routing.service_id, "service-1");
        assert_eq!(routing.worker_group_id, 7);
    }

    #[test]
    fn parse_s3_config_from_file_path_info_reads_staros_payload() {
        let cfg = parse_s3_config_from_file_path_info(&sample_file_path_info())
            .expect("parse s3 config")
            .expect("s3 config");
        assert_eq!(cfg.endpoint, "http://127.0.0.1:9000");
        assert_eq!(cfg.bucket, "bucket");
        assert_eq!(cfg.root, "lake/root");
        assert_eq!(cfg.access_key_id, "ak");
        assert_eq!(cfg.access_key_secret, "sk");
    }

    #[test]
    fn missing_proto3_status_is_treated_as_ok() {
        require_ok_status(None, "GetShard").expect("missing status should be implicit ok");
    }
}
