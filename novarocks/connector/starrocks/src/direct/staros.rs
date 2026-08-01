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

//! Provider-private StarOS V1 outbound/read adapter.
//!
//! Planning may call only `GetShard` to freeze a tablet location. Execution may
//! call only `ListFileStore` to refresh credentials for that already-frozen
//! location. The generated protobuf DTOs do not leave this module.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use novarocks_fs::{ObjectStoreConfig, parse_object_store_path_parse_only};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorRequestContext, ConnectorSplitPlanningRequest,
};
use tokio::runtime::Handle;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Response, Status};

use super::planning::ensure_active;
use super::{StarRocksDirectLocation, StarRocksDirectLocationSource, StarRocksStorageBindingRef};

const GRPC_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const GRPC_MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;
const CANCELLATION_POLL_INTERVAL: Duration = Duration::from_millis(10);

#[allow(clippy::enum_variant_names)]
mod wire {
    tonic::include_proto!("staros");
}

/// Explicit startup-local routing for the read-only StarOS closure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StarOsV1Routing {
    leader_endpoint: Arc<str>,
    service_id: Arc<str>,
    worker_group_id: u64,
}

impl StarOsV1Routing {
    pub fn try_new(
        leader_endpoint: impl AsRef<str>,
        service_id: impl AsRef<str>,
        worker_group_id: Option<u64>,
    ) -> Result<Self, ConnectorError> {
        let leader_endpoint = normalize_leader_endpoint(leader_endpoint.as_ref())?;
        let service_id = service_id.as_ref().trim();
        if service_id.is_empty() || service_id.len() > 4 * 1024 || !service_id.is_ascii() {
            return Err(unavailable("StarOS service routing is unavailable"));
        }
        let worker_group_id = worker_group_id
            .ok_or_else(|| unavailable("StarOS worker-group routing is unavailable"))?;
        Ok(Self {
            leader_endpoint: Arc::from(leader_endpoint),
            service_id: Arc::from(service_id),
            worker_group_id,
        })
    }

    pub fn leader_endpoint(&self) -> &str {
        &self.leader_endpoint
    }

    pub fn service_id(&self) -> &str {
        &self.service_id
    }

    pub const fn worker_group_id(&self) -> u64 {
        self.worker_group_id
    }
}

/// Startup-local StarOS client. Its channel cache and Tokio handle are never
/// shared through connector declarations or split payloads.
#[derive(Clone)]
pub struct StarRocksDirectIoRuntime {
    handle: Handle,
}

impl StarRocksDirectIoRuntime {
    pub fn new(handle: Handle) -> Self {
        Self { handle }
    }

    pub(crate) fn block_on<F, T>(&self, future: F) -> Result<T, ConnectorError>
    where
        F: Future<Output = Result<T, ConnectorError>> + Send + 'static,
        T: Send + 'static,
    {
        block_on_runtime(&self.handle, future)
    }
}

#[derive(Clone)]
pub struct StarOsV1Client {
    routing: StarOsV1Routing,
    rpc: Arc<dyn StarOsV1Rpc>,
}

impl StarOsV1Client {
    pub fn new(routing: StarOsV1Routing, runtime: StarRocksDirectIoRuntime) -> Self {
        Self {
            routing,
            rpc: Arc::new(TonicStarOsV1Rpc::new(runtime.handle)),
        }
    }

    #[cfg(test)]
    fn with_rpc(routing: StarOsV1Routing, rpc: Arc<dyn StarOsV1Rpc>) -> Self {
        Self { routing, rpc }
    }

    fn get_shards(
        &self,
        tablet_ids: &[u64],
        context: &ConnectorRequestContext,
    ) -> Result<wire::GetShardResponse, ConnectorError> {
        ensure_active(context)?;
        let response = self.rpc.get_shard(&self.routing, tablet_ids, context)?;
        ensure_active(context)?;
        require_ok(response.status.as_ref(), "GetShard")?;
        Ok(response)
    }

    fn list_file_stores(
        &self,
        context: &ConnectorRequestContext,
    ) -> Result<wire::ListFileStoreResponse, ConnectorError> {
        ensure_active(context)?;
        let response = self.rpc.list_file_store(&self.routing, context)?;
        ensure_active(context)?;
        require_ok(response.status.as_ref(), "ListFileStore")?;
        Ok(response)
    }
}

/// Planning-side location source backed only by `StarManager.GetShard`.
pub struct StarOsV1LocationSource {
    client: StarOsV1Client,
}

impl StarOsV1LocationSource {
    pub fn new(client: StarOsV1Client) -> Self {
        Self { client }
    }
}

impl StarRocksDirectLocationSource for StarOsV1LocationSource {
    fn resolve_locations(
        &self,
        tablet_ids: &[i64],
        request: &ConnectorSplitPlanningRequest,
    ) -> Result<Vec<StarRocksDirectLocation>, ConnectorError> {
        ensure_active(&request.context)?;
        let requested = tablet_ids
            .iter()
            .map(|tablet_id| {
                u64::try_from(*tablet_id)
                    .map_err(|_| invalid("StarOS GetShard requires positive StarRocks tablet IDs"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if requested.is_empty()
            || requested.contains(&0)
            || requested.iter().collect::<BTreeSet<_>>().len() != requested.len()
        {
            return Err(invalid(
                "StarOS GetShard requires non-empty unique StarRocks tablet IDs",
            ));
        }

        let response = self.client.get_shards(&requested, &request.context)?;
        parse_shard_locations(&requested, response.shard_info)
    }
}

/// Execution-side resolver. It refreshes only credentials for an exact frozen
/// location and binding; it cannot change tablet, version, or object path.
pub struct StarOsV1ObjectStoreResolver {
    client: StarOsV1Client,
}

impl StarOsV1ObjectStoreResolver {
    pub fn new(client: StarOsV1Client) -> Self {
        Self { client }
    }

    pub fn resolve(
        &self,
        storage_binding: &StarRocksStorageBindingRef,
        frozen_location: &str,
        context: &ConnectorRequestContext,
    ) -> Result<ObjectStoreConfig, ConnectorError> {
        ensure_active(context)?;
        let (target_bucket, target_key) = parse_object_location(frozen_location)?;
        let response = self.client.list_file_stores(context)?;

        let mut matching = response
            .fs_infos
            .iter()
            .filter(|file_store| file_store.fs_key.trim() == storage_binding.as_str());
        let file_store = matching
            .next()
            .ok_or_else(|| unavailable("StarOS object-store binding is unavailable"))?;
        if matching.next().is_some() {
            return Err(corrupt(
                "StarOS returned duplicate object-store binding records",
            ));
        }

        parse_object_store_config(file_store, &target_bucket, &target_key)
    }
}

trait StarOsV1Rpc: Send + Sync {
    fn get_shard(
        &self,
        routing: &StarOsV1Routing,
        tablet_ids: &[u64],
        context: &ConnectorRequestContext,
    ) -> Result<wire::GetShardResponse, ConnectorError>;

    fn list_file_store(
        &self,
        routing: &StarOsV1Routing,
        context: &ConnectorRequestContext,
    ) -> Result<wire::ListFileStoreResponse, ConnectorError>;
}

struct TonicStarOsV1Rpc {
    runtime: Handle,
    channels: Arc<Mutex<HashMap<String, Channel>>>,
}

impl TonicStarOsV1Rpc {
    fn new(runtime: Handle) -> Self {
        Self {
            runtime,
            channels: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl StarOsV1Rpc for TonicStarOsV1Rpc {
    fn get_shard(
        &self,
        routing: &StarOsV1Routing,
        tablet_ids: &[u64],
        context: &ConnectorRequestContext,
    ) -> Result<wire::GetShardResponse, ConnectorError> {
        let runtime = self.runtime.clone();
        let channels = Arc::clone(&self.channels);
        let routing = routing.clone();
        let tablet_ids = tablet_ids.to_vec();
        let context = context.clone();
        block_on_runtime(&runtime, async move {
            let channel = cached_channel(&channels, routing.leader_endpoint())?;
            let mut client = wire::star_manager_client::StarManagerClient::new(channel)
                .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
            let request = request_with_deadline(
                wire::GetShardRequest {
                    service_id: routing.service_id().to_string(),
                    shard_id: tablet_ids,
                    worker_group_id: routing.worker_group_id(),
                },
                &context,
            )?;
            await_tonic(&context, "GetShard", client.get_shard(request))
                .await
                .map(Response::into_inner)
        })
    }

    fn list_file_store(
        &self,
        routing: &StarOsV1Routing,
        context: &ConnectorRequestContext,
    ) -> Result<wire::ListFileStoreResponse, ConnectorError> {
        let runtime = self.runtime.clone();
        let channels = Arc::clone(&self.channels);
        let routing = routing.clone();
        let context = context.clone();
        block_on_runtime(&runtime, async move {
            let channel = cached_channel(&channels, routing.leader_endpoint())?;
            let mut client = wire::star_manager_client::StarManagerClient::new(channel)
                .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
            let request = request_with_deadline(
                wire::ListFileStoreRequest {
                    service_id: routing.service_id().to_string(),
                    fs_type: wire::FileStoreType::S3 as i32,
                },
                &context,
            )?;
            await_tonic(&context, "ListFileStore", client.list_file_store(request))
                .await
                .map(Response::into_inner)
        })
    }
}

fn normalize_leader_endpoint(raw: &str) -> Result<String, ConnectorError> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(unavailable("StarOS leader routing is unavailable"));
    }
    let lower = raw.to_ascii_lowercase();
    if raw.contains('@')
        || raw.contains('?')
        || raw.contains('#')
        || lower.contains("token=")
        || lower.contains("signature=")
    {
        return Err(invalid(
            "StarOS leader endpoint contains credential material",
        ));
    }
    let endpoint = if raw.contains("://") {
        if !lower.starts_with("http://") && !lower.starts_with("https://") {
            return Err(unsupported("unsupported StarOS leader endpoint scheme"));
        }
        raw.to_string()
    } else {
        format!("http://{raw}")
    };
    Endpoint::from_shared(endpoint.clone())
        .map_err(|_| unavailable("StarOS leader routing is unavailable"))?;
    Ok(endpoint)
}

fn cached_channel(
    channels: &Mutex<HashMap<String, Channel>>,
    endpoint: &str,
) -> Result<Channel, ConnectorError> {
    if let Some(channel) = channels
        .lock()
        .map_err(|_| internal("StarOS channel cache lock is poisoned"))?
        .get(endpoint)
        .cloned()
    {
        return Ok(channel);
    }
    let channel = Endpoint::from_shared(endpoint.to_string())
        .map_err(|_| unavailable("StarOS leader routing is unavailable"))?
        .connect_timeout(GRPC_CONNECT_TIMEOUT)
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .connect_lazy();
    channels
        .lock()
        .map_err(|_| internal("StarOS channel cache lock is poisoned"))?
        .insert(endpoint.to_string(), channel.clone());
    Ok(channel)
}

fn block_on_runtime<F, T>(runtime: &Handle, future: F) -> Result<T, ConnectorError>
where
    F: Future<Output = Result<T, ConnectorError>> + Send + 'static,
    T: Send + 'static,
{
    if Handle::try_current().is_ok() {
        let runtime = runtime.clone();
        return std::thread::Builder::new()
            .name("staros-v1-rpc".to_string())
            .spawn(move || runtime.block_on(future))
            .map_err(|_| unavailable("StarOS I/O runtime is unavailable"))?
            .join()
            .map_err(|_| internal("StarOS I/O runtime task panicked"))?;
    }
    runtime.block_on(future)
}

fn request_with_deadline<T>(
    payload: T,
    context: &ConnectorRequestContext,
) -> Result<Request<T>, ConnectorError> {
    ensure_active(context)?;
    let timeout = context
        .deadline()
        .checked_duration_since(Instant::now())
        .ok_or_else(|| deadline("StarOS request deadline elapsed"))?;
    let mut request = Request::new(payload);
    request.set_timeout(timeout);
    Ok(request)
}

async fn await_tonic<T, F>(
    context: &ConnectorRequestContext,
    operation: &'static str,
    future: F,
) -> Result<Response<T>, ConnectorError>
where
    F: Future<Output = Result<Response<T>, Status>>,
{
    tokio::pin!(future);
    loop {
        ensure_active(context)?;
        let remaining = context
            .deadline()
            .checked_duration_since(Instant::now())
            .ok_or_else(|| deadline("StarOS request deadline elapsed"))?;
        tokio::select! {
            response = &mut future => return response.map_err(|status| map_tonic_status(operation, status)),
            _ = tokio::time::sleep(remaining.min(CANCELLATION_POLL_INTERVAL)) => {}
        }
    }
}

fn map_tonic_status(operation: &str, status: Status) -> ConnectorError {
    let kind = match status.code() {
        Code::Cancelled => ConnectorErrorKind::Cancelled,
        Code::DeadlineExceeded => ConnectorErrorKind::DeadlineExceeded,
        Code::InvalidArgument | Code::OutOfRange => ConnectorErrorKind::InvalidRequest,
        Code::NotFound => ConnectorErrorKind::NotFound,
        Code::PermissionDenied | Code::Unauthenticated => ConnectorErrorKind::PermissionDenied,
        Code::Unimplemented => ConnectorErrorKind::Unsupported,
        Code::ResourceExhausted => ConnectorErrorKind::ResourceExhausted,
        Code::Unavailable => ConnectorErrorKind::Unavailable,
        Code::DataLoss => ConnectorErrorKind::CorruptData,
        _ => ConnectorErrorKind::Internal,
    };
    ConnectorError::new(
        kind,
        format!(
            "StarOS {operation} RPC failed with status {:?}",
            status.code()
        ),
    )
}

fn require_ok(status: Option<&wire::StarStatus>, operation: &str) -> Result<(), ConnectorError> {
    // A proto3 success response may omit the all-default status submessage.
    let Some(status) = status else {
        return Ok(());
    };
    let code = wire::StatusCode::try_from(status.status_code)
        .map_err(|_| unsupported("StarOS returned an unknown status code"))?;
    if code == wire::StatusCode::Ok {
        return Ok(());
    }
    let kind = match code {
        wire::StatusCode::InvalidArgument => ConnectorErrorKind::InvalidRequest,
        wire::StatusCode::NotExist => ConnectorErrorKind::NotFound,
        wire::StatusCode::NotAllowed => ConnectorErrorKind::PermissionDenied,
        wire::StatusCode::NotImplemented => ConnectorErrorKind::Unsupported,
        wire::StatusCode::Grpc
        | wire::StatusCode::Io
        | wire::StatusCode::NotLeader
        | wire::StatusCode::ShutDown
        | wire::StatusCode::WorkerNotHealthy => ConnectorErrorKind::Unavailable,
        _ => ConnectorErrorKind::Internal,
    };
    Err(ConnectorError::new(
        kind,
        format!("StarOS {operation} failed with status {code:?}"),
    ))
}

fn parse_shard_locations(
    requested: &[u64],
    shards: Vec<wire::ShardInfo>,
) -> Result<Vec<StarRocksDirectLocation>, ConnectorError> {
    let requested = requested.iter().copied().collect::<BTreeSet<_>>();
    let mut locations = BTreeMap::new();
    for shard in shards {
        if !requested.contains(&shard.shard_id) {
            return Err(corrupt("StarOS GetShard returned an unrequested shard"));
        }
        let tablet_id = i64::try_from(shard.shard_id)
            .map_err(|_| corrupt("StarOS returned an out-of-range shard ID"))?;
        let path = shard
            .file_path
            .ok_or_else(|| corrupt("StarOS GetShard response is missing a shard path"))?;
        let full_path = path.full_path.trim();
        let (target_bucket, target_key) = parse_object_location(full_path)?;
        let file_store = path
            .fs_info
            .ok_or_else(|| corrupt("StarOS GetShard response is missing a file-store identity"))?;
        require_s3_type(file_store.fs_type)?;
        let s3 = file_store
            .s3_fs_info
            .as_ref()
            .ok_or_else(|| corrupt("StarOS S3 shard path is missing file-store metadata"))?;
        if !s3.bucket.trim().is_empty() && s3.bucket.trim() != target_bucket {
            return Err(corrupt(
                "StarOS shard path bucket disagrees with file-store metadata",
            ));
        }
        if !key_matches_prefix(&target_key, &s3.path_prefix) {
            return Err(corrupt(
                "StarOS shard path is outside its file-store prefix",
            ));
        }
        let storage_binding = StarRocksStorageBindingRef::parse(file_store.fs_key.trim())?;
        let location = StarRocksDirectLocation::try_new(
            tablet_id,
            full_path,
            storage_binding,
            file_store.fs_key.trim(),
        )?;
        if locations.insert(shard.shard_id, location).is_some() {
            return Err(corrupt("StarOS GetShard returned a duplicate shard"));
        }
    }
    if locations.len() != requested.len() {
        return Err(corrupt(
            "StarOS GetShard did not resolve every requested shard",
        ));
    }
    Ok(requested
        .into_iter()
        .map(|shard_id| locations.remove(&shard_id).expect("validated shard set"))
        .collect())
}

fn parse_object_store_config(
    file_store: &wire::FileStoreInfo,
    target_bucket: &str,
    target_key: &str,
) -> Result<ObjectStoreConfig, ConnectorError> {
    require_s3_type(file_store.fs_type)?;
    let s3 = file_store
        .s3_fs_info
        .as_ref()
        .ok_or_else(|| corrupt("StarOS S3 file-store metadata is missing"))?;
    let bucket = s3.bucket.trim();
    if bucket.is_empty() || bucket != target_bucket {
        return Err(corrupt(
            "StarOS object-store binding does not match the frozen bucket",
        ));
    }
    if !key_matches_prefix(target_key, &s3.path_prefix) {
        return Err(corrupt(
            "StarOS object-store binding does not cover the frozen path",
        ));
    }
    let endpoint = validate_object_store_endpoint(&s3.endpoint)?;
    let credential = s3
        .credential
        .as_ref()
        .ok_or_else(|| unavailable("StarOS object-store credential is unavailable"))?;
    let (access_key_id, access_key_secret) = match credential.credential.as_ref() {
        Some(wire::aws_credential_info::Credential::SimpleCredential(simple)) => {
            if simple.encrypted {
                return Err(unsupported(
                    "encrypted StarOS S3 simple credentials are unsupported",
                ));
            }
            let access_key = simple.access_key.trim();
            let secret_key = simple.access_key_secret.trim();
            if access_key.is_empty() || secret_key.is_empty() {
                return Err(unavailable("StarOS S3 simple credential is unavailable"));
            }
            (access_key.to_string(), secret_key.to_string())
        }
        Some(wire::aws_credential_info::Credential::DefaultCredential(_))
        | Some(wire::aws_credential_info::Credential::ProfileCredential(_))
        | Some(wire::aws_credential_info::Credential::AssumeRoleCredential(_)) => {
            return Err(unsupported("unsupported StarOS S3 credential mode"));
        }
        None => return Err(unavailable("StarOS object-store credential is unavailable")),
    };
    let enable_path_style_access = match s3.path_style_access {
        0 => None,
        1 => Some(true),
        2 => Some(false),
        _ => return Err(unsupported("unknown StarOS S3 path-style mode")),
    };
    let region = normalize_region(&s3.region);
    Ok(ObjectStoreConfig {
        endpoint,
        access_key_id,
        access_key_secret,
        session_token: None,
        enable_path_style_access,
        region,
        retry_max_times: None,
        retry_min_delay_ms: None,
        retry_max_delay_ms: None,
        timeout_ms: None,
        io_timeout_ms: None,
    })
}

fn require_s3_type(raw: i32) -> Result<(), ConnectorError> {
    let file_store_type = wire::FileStoreType::try_from(raw)
        .map_err(|_| unsupported("StarOS returned an unknown file-store type"))?;
    if file_store_type != wire::FileStoreType::S3 {
        return Err(unsupported(
            "StarRocks direct read supports only S3-compatible StarOS storage",
        ));
    }
    Ok(())
}

fn parse_object_location(location: &str) -> Result<(String, String), ConnectorError> {
    parse_object_store_path_parse_only(location).map_err(|error| {
        let kind = match error.kind() {
            novarocks_fs::FileErrorKind::Unsupported => ConnectorErrorKind::Unsupported,
            _ => ConnectorErrorKind::InvalidRequest,
        };
        ConnectorError::new(kind, "invalid StarRocks direct object-store location")
    })
}

fn validate_object_store_endpoint(raw: &str) -> Result<String, ConnectorError> {
    let endpoint = raw.trim();
    let lower = endpoint.to_ascii_lowercase();
    if endpoint.is_empty() {
        return Err(corrupt("StarOS S3 endpoint is missing"));
    }
    if endpoint.contains('@')
        || endpoint.contains('?')
        || endpoint.contains('#')
        || lower.contains("token=")
        || lower.contains("signature=")
    {
        return Err(invalid("StarOS S3 endpoint contains credential material"));
    }
    if endpoint.contains("://") && !lower.starts_with("http://") && !lower.starts_with("https://") {
        return Err(unsupported("unsupported StarOS S3 endpoint scheme"));
    }
    Ok(endpoint.to_string())
}

fn normalize_region(raw: &str) -> Option<String> {
    let region = raw
        .trim()
        .split_once('\0')
        .map(|(prefix, _)| prefix)
        .unwrap_or(raw)
        .trim();
    (!region.is_empty()).then(|| region.to_string())
}

fn key_matches_prefix(key: &str, prefix: &str) -> bool {
    let prefix = prefix.trim_matches('/');
    prefix.is_empty()
        || key == prefix
        || key
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

fn unsupported(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message)
}

fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message)
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

fn deadline(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::DeadlineExceeded, message)
}

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use novarocks_spi::connector::ConnectorCancellation;

    use super::*;

    #[derive(Default)]
    struct TestCancellation(AtomicBool);

    impl ConnectorCancellation for TestCancellation {
        fn is_cancelled(&self) -> bool {
            self.0.load(Ordering::SeqCst)
        }
    }

    fn context(deadline: Instant) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            deadline,
            Arc::new(TestCancellation::default()),
            1024,
            4096,
        )
        .unwrap()
    }

    fn planning_request() -> ConnectorSplitPlanningRequest {
        ConnectorSplitPlanningRequest {
            target_parallelism: NonZeroUsize::new(1).unwrap(),
            max_split_bytes: None,
            context: context(Instant::now() + Duration::from_secs(1)),
        }
    }

    fn routing() -> StarOsV1Routing {
        StarOsV1Routing::try_new("127.0.0.1:8490", "starrocks", Some(7)).unwrap()
    }

    fn ok_status() -> Option<wire::StarStatus> {
        Some(wire::StarStatus {
            status_code: wire::StatusCode::Ok as i32,
            error_msg: String::new(),
            extra_info: Vec::new(),
        })
    }

    fn s3_file_store(
        fs_key: &str,
        bucket: &str,
        prefix: &str,
        access_key: &str,
        secret_key: &str,
    ) -> wire::FileStoreInfo {
        wire::FileStoreInfo {
            fs_type: wire::FileStoreType::S3 as i32,
            fs_key: fs_key.to_string(),
            s3_fs_info: Some(wire::S3FileStoreInfo {
                bucket: bucket.to_string(),
                region: "us-east-1\0ignored".to_string(),
                endpoint: "http://object-store:9000".to_string(),
                credential: Some(wire::AwsCredentialInfo {
                    credential: Some(wire::aws_credential_info::Credential::SimpleCredential(
                        wire::AwsSimpleCredentialInfo {
                            access_key: access_key.to_string(),
                            access_key_secret: secret_key.to_string(),
                            encrypted: false,
                        },
                    )),
                }),
                path_prefix: prefix.to_string(),
                partitioned_prefix_enabled: false,
                num_partitioned_prefix: 0,
                path_style_access: 1,
            }),
            fs_name: "shared-data".to_string(),
        }
    }

    fn shard(shard_id: u64, file_store: wire::FileStoreInfo) -> wire::ShardInfo {
        wire::ShardInfo {
            shard_id,
            file_path: Some(wire::FilePathInfo {
                fs_info: Some(file_store),
                full_path: format!("s3://bucket/root/{shard_id}"),
            }),
        }
    }

    struct MockRpc {
        get_shard: wire::GetShardResponse,
        list_file_store: wire::ListFileStoreResponse,
        get_shard_calls: AtomicUsize,
        list_file_store_calls: AtomicUsize,
    }

    impl StarOsV1Rpc for MockRpc {
        fn get_shard(
            &self,
            _routing: &StarOsV1Routing,
            _tablet_ids: &[u64],
            _context: &ConnectorRequestContext,
        ) -> Result<wire::GetShardResponse, ConnectorError> {
            self.get_shard_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.get_shard.clone())
        }

        fn list_file_store(
            &self,
            _routing: &StarOsV1Routing,
            _context: &ConnectorRequestContext,
        ) -> Result<wire::ListFileStoreResponse, ConnectorError> {
            self.list_file_store_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.list_file_store.clone())
        }
    }

    fn client(
        get_shard: wire::GetShardResponse,
        fs_infos: Vec<wire::FileStoreInfo>,
    ) -> StarOsV1Client {
        StarOsV1Client::with_rpc(
            routing(),
            Arc::new(MockRpc {
                get_shard,
                list_file_store: wire::ListFileStoreResponse {
                    status: ok_status(),
                    fs_infos,
                },
                get_shard_calls: AtomicUsize::new(0),
                list_file_store_calls: AtomicUsize::new(0),
            }),
        )
    }

    #[test]
    fn routing_is_explicit_and_secret_free() {
        let routing = routing();
        assert_eq!(routing.leader_endpoint(), "http://127.0.0.1:8490");
        assert_eq!(routing.service_id(), "starrocks");
        assert_eq!(routing.worker_group_id(), 7);
        assert_eq!(
            StarOsV1Routing::try_new("", "starrocks", Some(7))
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::Unavailable
        );
        assert_eq!(
            StarOsV1Routing::try_new("http://user:secret@host", "starrocks", Some(7))
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
        assert_eq!(
            StarOsV1Routing::try_new("host:8490", "", Some(7))
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::Unavailable
        );
        assert_eq!(
            StarOsV1Routing::try_new("host:8490", "starrocks", None)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::Unavailable
        );
    }

    #[test]
    fn get_shard_freezes_location_and_binding_without_credentials() {
        let response = wire::GetShardResponse {
            status: ok_status(),
            shard_info: vec![shard(
                11,
                s3_file_store("volume-a", "bucket", "root", "AK_SECRET", "SK_SECRET"),
            )],
        };
        let source = StarOsV1LocationSource::new(client(response, Vec::new()));
        let locations = source
            .resolve_locations(&[11], &planning_request())
            .unwrap();
        assert_eq!(locations.len(), 1);
        assert_eq!(locations[0].tablet_id, 11);
        assert_eq!(locations[0].tablet_root.as_ref(), "s3://bucket/root/11");
        assert_eq!(locations[0].storage_binding.as_str(), "volume-a");
        let debug = format!("{locations:?}");
        assert!(!debug.contains("AK_SECRET"));
        assert!(!debug.contains("SK_SECRET"));
    }

    #[test]
    fn list_file_store_resolves_exact_binding_bucket_prefix_and_simple_credential() {
        let resolver = StarOsV1ObjectStoreResolver::new(client(
            wire::GetShardResponse {
                status: ok_status(),
                shard_info: Vec::new(),
            },
            vec![
                s3_file_store("other", "bucket", "root", "OTHER_AK", "OTHER_SK"),
                s3_file_store("volume-a", "bucket", "root", "ACCESS_KEY", "SECRET_KEY"),
            ],
        ));
        let binding = StarRocksStorageBindingRef::parse("volume-a").unwrap();
        let config = resolver
            .resolve(
                &binding,
                "s3://bucket/root/tablet/segment.dat",
                &context(Instant::now() + Duration::from_secs(1)),
            )
            .unwrap();
        assert_eq!(config.endpoint, "http://object-store:9000");
        assert_eq!(config.region.as_deref(), Some("us-east-1"));
        assert_eq!(config.enable_path_style_access, Some(true));
        assert_eq!(config.access_key_id, "ACCESS_KEY");
        assert_eq!(config.access_key_secret, "SECRET_KEY");
        let debug = format!("{config:?}");
        assert!(!debug.contains("ACCESS_KEY"));
        assert!(!debug.contains("SECRET_KEY"));
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn resolver_rejects_missing_binding_prefix_drift_and_unknown_modes() {
        let context = context(Instant::now() + Duration::from_secs(1));
        let binding = StarRocksStorageBindingRef::parse("volume-a").unwrap();
        let missing = StarOsV1ObjectStoreResolver::new(client(
            wire::GetShardResponse::default(),
            vec![s3_file_store("other", "bucket", "root", "AK", "SK")],
        ));
        assert_eq!(
            missing
                .resolve(&binding, "s3://bucket/root/segment", &context)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::Unavailable
        );

        let drifted = StarOsV1ObjectStoreResolver::new(client(
            wire::GetShardResponse::default(),
            vec![s3_file_store(
                "volume-a",
                "bucket",
                "other-root",
                "AK",
                "SK",
            )],
        ));
        assert_eq!(
            drifted
                .resolve(&binding, "s3://bucket/root/segment", &context)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );

        let mut unknown = s3_file_store("volume-a", "bucket", "root", "AK", "SK");
        unknown.s3_fs_info.as_mut().unwrap().path_style_access = 99;
        let unknown = StarOsV1ObjectStoreResolver::new(client(
            wire::GetShardResponse::default(),
            vec![unknown],
        ));
        assert_eq!(
            unknown
                .resolve(&binding, "s3://bucket/root/segment", &context)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::Unsupported
        );
    }

    #[test]
    fn malformed_shard_and_unknown_status_are_typed() {
        let malformed = wire::GetShardResponse {
            status: ok_status(),
            shard_info: vec![wire::ShardInfo {
                shard_id: 11,
                file_path: None,
            }],
        };
        let source = StarOsV1LocationSource::new(client(malformed, Vec::new()));
        assert_eq!(
            source
                .resolve_locations(&[11], &planning_request())
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );

        let unknown = wire::GetShardResponse {
            status: Some(wire::StarStatus {
                status_code: 999,
                error_msg: "must not escape".to_string(),
                extra_info: vec![1, 2, 3],
            }),
            shard_info: Vec::new(),
        };
        let source = StarOsV1LocationSource::new(client(unknown, Vec::new()));
        let error = source
            .resolve_locations(&[11], &planning_request())
            .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        assert!(!error.to_string().contains("must not escape"));
    }

    #[test]
    fn in_flight_deadline_is_preserved() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let context = context(Instant::now() + Duration::from_millis(5));
        let result = runtime.block_on(await_tonic(&context, "GetShard", async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok::<_, Status>(Response::new(wire::GetShardResponse::default()))
        }));
        assert_eq!(
            result.unwrap_err().kind(),
            ConnectorErrorKind::DeadlineExceeded
        );
    }
}
