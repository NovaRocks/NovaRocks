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

use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Formatter};
use std::net::IpAddr;
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use novarocks_spi::connector::{
    CredentialLeaseId, StaticCredentialReference, StorageAccessDomainId,
};
use opendal::Operator;
use opendal::layers::{ConcurrentLimitLayer, HttpClientLayer, RetryLayer, TimeoutLayer};
use url::{Host, Url};

use crate::{FileCancellation, FileError, FileErrorKind, FileReadRange, FileResult, SecretValue};

const DEFAULT_OBJECT_STORE_RETRY_MAX_TIMES: usize = 6;
const DEFAULT_OBJECT_STORE_RETRY_MIN_DELAY_MS: u64 = 100;
const DEFAULT_OBJECT_STORE_RETRY_MAX_DELAY_MS: u64 = 10_000;
const DEFAULT_OBJECT_STORE_TIMEOUT_MS: u64 = 60_000;
const DEFAULT_OBJECT_STORE_IO_TIMEOUT_MS: u64 = 60_000;
const DEFAULT_OBJECT_STORE_CONCURRENT_LIMIT: usize = 1024;
const DEFAULT_OBJECT_STORE_HTTP_CONCURRENT_LIMIT: usize = 256;
const DEFAULT_OBJECT_STORE_PROVIDER_POOL_CAPACITY: usize = 1024;
const MIN_OBJECT_STORE_PROVIDER_POOL_CAPACITY: usize = 1;
const MAX_OBJECT_STORE_PROVIDER_POOL_CAPACITY: usize = 16_384;
const DEFAULT_OBJECT_STORE_PROVIDER_POOL_IDLE_TTL: Duration = Duration::from_secs(15 * 60);
const MIN_OBJECT_STORE_PROVIDER_POOL_IDLE_TTL: Duration = Duration::from_secs(60);
const MAX_OBJECT_STORE_PROVIDER_POOL_IDLE_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const OBJECT_STORE_PROVIDER_BUILD_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FsScheme {
    Local,
    ObjectStore,
    Hdfs,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConditionalCreateOutcome {
    Created,
    AlreadyExists,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FsListEntry {
    location: String,
    size: u64,
    is_dir: bool,
}

impl FsListEntry {
    pub fn location(&self) -> &str {
        &self.location
    }

    pub const fn size(&self) -> u64 {
        self.size
    }

    pub const fn is_dir(&self) -> bool {
        self.is_dir
    }
}

pub type FsListStream = Pin<Box<dyn Stream<Item = FileResult<FsListEntry>> + Send + 'static>>;

impl FsScheme {
    pub fn is_object_store(self) -> bool {
        self == Self::ObjectStore
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FsLocation {
    original: String,
    scheme: FsScheme,
    uri_scheme: Option<String>,
    authority: Option<String>,
    path: String,
}

impl FsLocation {
    pub fn parse(raw: impl AsRef<str>) -> FileResult<Self> {
        let original = raw.as_ref().trim();
        if original.is_empty() {
            return Err(FileError::invalid("fs location is empty"));
        }

        let Some((uri_scheme, rest)) = split_uri_scheme(original) else {
            if original.contains("://") {
                return Err(FileError::unsupported(format!(
                    "unsupported fs location scheme: {original}"
                )));
            }
            return Ok(Self::local(original, None, original));
        };
        let uri_scheme = uri_scheme.to_ascii_lowercase();

        match uri_scheme.as_str() {
            "file" => Self::parse_file(original, uri_scheme, rest),
            "s3" | "s3a" | "oss" => {
                let (authority, path) =
                    parse_authority_and_path(original, rest, true, uri_scheme.as_str())?;
                Ok(Self {
                    original: original.to_string(),
                    scheme: FsScheme::ObjectStore,
                    uri_scheme: Some(uri_scheme),
                    authority,
                    path,
                })
            }
            "hdfs" => {
                let (authority, path) = parse_authority_and_path(original, rest, true, "hdfs")?;
                Ok(Self {
                    original: original.to_string(),
                    scheme: FsScheme::Hdfs,
                    uri_scheme: Some(uri_scheme),
                    authority,
                    path,
                })
            }
            _ => Err(FileError::unsupported(format!(
                "unsupported fs location scheme: {original}"
            ))),
        }
    }

    pub fn original(&self) -> &str {
        &self.original
    }

    pub fn scheme(&self) -> FsScheme {
        self.scheme
    }

    pub fn uri_scheme(&self) -> Option<&str> {
        self.uri_scheme.as_deref()
    }

    pub fn authority(&self) -> Option<&str> {
        self.authority.as_deref()
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    fn local(original: &str, uri_scheme: Option<String>, path: &str) -> Self {
        Self {
            original: original.to_string(),
            scheme: FsScheme::Local,
            uri_scheme,
            authority: None,
            path: path.to_string(),
        }
    }

    fn parse_file(original: &str, uri_scheme: String, rest: &str) -> FileResult<Self> {
        if let Some(without_prefix) = rest.strip_prefix("//") {
            if without_prefix.starts_with('/') {
                ensure_non_empty_path(original, "file", without_prefix)?;
                return Ok(Self::local(original, Some(uri_scheme), without_prefix));
            }

            let (authority, path) = without_prefix
                .split_once('/')
                .unwrap_or((without_prefix, ""));
            if !authority.is_empty() && authority != "localhost" {
                return Err(FileError::unsupported(format!(
                    "unsupported file URI host in local path: {original}"
                )));
            }
            let path = if path.is_empty() {
                ""
            } else {
                &without_prefix[authority.len()..]
            };
            ensure_non_empty_path(original, "file", path)?;
            return Ok(Self::local(original, Some(uri_scheme), path));
        }

        ensure_non_empty_path(original, "file", rest)?;
        Ok(Self::local(original, Some(uri_scheme), rest))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedFsPath {
    location: FsLocation,
    operator_relative_path: String,
}

impl ResolvedFsPath {
    pub fn new(
        location: FsLocation,
        operator_relative_path: impl Into<String>,
    ) -> FileResult<Self> {
        let operator_relative_path = operator_relative_path.into();
        if operator_relative_path.trim().is_empty() {
            return Err(FileError::invalid("operator-relative path is empty"));
        }
        Ok(Self {
            location,
            operator_relative_path,
        })
    }

    pub fn location(&self) -> &FsLocation {
        &self.location
    }

    pub fn operator_relative_path(&self) -> &str {
        &self.operator_relative_path
    }
}

#[derive(Clone)]
pub struct FsAccessHandle {
    access_domain: StorageAccessDomainId,
    scheme: FsScheme,
    operator: Operator,
    authority: Option<String>,
    root: Option<String>,
    paths: Vec<ResolvedFsPath>,
}

impl FsAccessHandle {
    pub fn new(
        access_domain: StorageAccessDomainId,
        scheme: FsScheme,
        operator: Operator,
        authority: Option<String>,
        root: Option<String>,
        paths: Vec<ResolvedFsPath>,
    ) -> Self {
        Self {
            access_domain,
            scheme,
            operator,
            authority,
            root,
            paths,
        }
    }

    pub const fn access_domain(&self) -> StorageAccessDomainId {
        self.access_domain
    }

    pub fn scheme(&self) -> FsScheme {
        self.scheme
    }

    pub fn operator(&self) -> Operator {
        self.operator.clone()
    }

    pub fn authority(&self) -> Option<&str> {
        self.authority.as_deref()
    }

    pub fn root(&self) -> Option<&str> {
        self.root.as_deref()
    }

    pub fn paths(&self) -> &[ResolvedFsPath] {
        &self.paths
    }

    pub fn bind(&self, path_index: usize, identity: FileIdentity) -> FileResult<BoundFile> {
        let path = self
            .paths
            .get(path_index)
            .ok_or_else(|| {
                FileError::invalid(format!("file path index out of bounds: {path_index}"))
            })?
            .clone();
        Ok(BoundFile {
            access: self.clone(),
            path,
            identity,
        })
    }

    /// Bind another file covered by this already-authorized access handle.
    ///
    /// The new location must remain in the same filesystem domain (local
    /// root, object-store bucket, or HDFS authority). This lets a connector
    /// open table-format side files without carrying credentials in a read
    /// request or constructing a second storage client.
    pub fn bind_location(
        &self,
        location: impl AsRef<str>,
        identity: FileIdentity,
    ) -> FileResult<BoundFile> {
        let location = FsLocation::parse(location)?;
        if location.scheme() != self.scheme {
            return Err(FileError::invalid(
                "bound file location uses a different filesystem scheme",
            ));
        }
        let operator_relative_path = match self.scheme {
            FsScheme::Local => {
                let root = self.root.as_deref().ok_or_else(|| {
                    FileError::new(FileErrorKind::Internal, "local access handle has no root")
                })?;
                let path = Path::new(location.path());
                let relative = if path.is_absolute() {
                    if root == "." {
                        return Err(FileError::new(
                            FileErrorKind::Permission,
                            format!(
                                "absolute local file {} is outside relative authorized root",
                                location.original()
                            ),
                        ));
                    }
                    path.strip_prefix(root)
                        .map(Path::to_path_buf)
                        .map_err(|_| {
                            FileError::new(
                                FileErrorKind::Permission,
                                format!(
                                    "local file {} is outside authorized root {root}",
                                    location.original()
                                ),
                            )
                        })?
                } else {
                    path.to_path_buf()
                };
                normalize_bound_relative_path(&relative, location.original())?
            }
            FsScheme::ObjectStore => {
                if location.authority() != self.authority.as_deref() {
                    return Err(FileError::new(
                        FileErrorKind::Permission,
                        "object-store file uses a different bucket",
                    ));
                }
                location.path().trim_start_matches('/').to_string()
            }
            FsScheme::Hdfs => {
                if location.authority() != self.authority.as_deref() {
                    return Err(FileError::new(
                        FileErrorKind::Permission,
                        "HDFS file uses a different authority",
                    ));
                }
                location.path().trim_start_matches('/').to_string()
            }
        };
        let path = ResolvedFsPath::new(location, operator_relative_path)?;
        Ok(BoundFile {
            access: self.clone(),
            path,
            identity,
        })
    }

    pub fn operator_relative_paths(&self) -> Vec<&str> {
        self.paths
            .iter()
            .map(ResolvedFsPath::operator_relative_path)
            .collect()
    }

    /// List entries through this already-authorized storage client.
    ///
    /// The returned stream preserves OpenDAL's paginated traversal. The
    /// caller supplies a fully qualified prefix in the same access domain;
    /// this method never parses credentials or constructs another client.
    pub async fn list_location(
        &self,
        prefix: impl AsRef<str>,
        recursive: bool,
        cancellation: &FileCancellation,
    ) -> FileResult<FsListStream> {
        cancellation.check()?;
        let location = FsLocation::parse(prefix.as_ref())?;
        let bound = self.bind_location(
            location.original(),
            FileIdentity::new(location.original(), 0, None),
        )?;
        let list_path = bound
            .operator_relative_path()
            .trim_end_matches('/')
            .to_string()
            + "/";
        let lister = self
            .operator
            .lister_with(&list_path)
            .recursive(recursive)
            .await
            .map_err(|error| map_opendal_error("list files", error))?;
        cancellation.check()?;

        let uri_scheme = location.uri_scheme().map(ToOwned::to_owned);
        let authority = location.authority().map(ToOwned::to_owned);
        let local_root = self.root.clone();
        let scheme = self.scheme;
        let cancellation = cancellation.clone();
        Ok(Box::pin(lister.map(move |entry| {
            cancellation.check()?;
            let entry = entry.map_err(|error| map_opendal_error("list files", error))?;
            let path = entry.path();
            let location = match scheme {
                FsScheme::ObjectStore | FsScheme::Hdfs => format!(
                    "{}://{}/{}",
                    uri_scheme.as_deref().unwrap_or(match scheme {
                        FsScheme::ObjectStore => "s3",
                        FsScheme::Hdfs => "hdfs",
                        FsScheme::Local => unreachable!(),
                    }),
                    authority.as_deref().unwrap_or_default(),
                    path.trim_start_matches('/')
                ),
                FsScheme::Local => {
                    let path = match local_root.as_deref() {
                        Some(root) if root != "." => Path::new(root).join(path),
                        _ => PathBuf::from(path),
                    };
                    path.to_string_lossy().into_owned()
                }
            };
            Ok(FsListEntry {
                location,
                size: entry.metadata().content_length(),
                is_dir: entry.metadata().is_dir(),
            })
        })))
    }

    /// Atomically create one authorized path without replacing an existing file.
    ///
    /// Design: ADR-0077 makes native conditional storage creation the publication
    /// fence; this method must never emulate it with an existence check plus write.
    pub async fn create_if_absent(
        &self,
        path_index: usize,
        payload: Bytes,
        cancellation: &FileCancellation,
    ) -> FileResult<ConditionalCreateOutcome> {
        cancellation.check()?;
        let path = self.paths.get(path_index).ok_or_else(|| {
            FileError::invalid(format!("file path index out of bounds: {path_index}"))
        })?;
        if !self
            .operator
            .info()
            .full_capability()
            .write_with_if_not_exists
        {
            return Err(FileError::unsupported(
                "filesystem does not support native conditional create",
            ));
        }

        let result = self
            .operator
            .write_with(path.operator_relative_path(), payload)
            .if_not_exists(true)
            .await;
        cancellation.check()?;
        match result {
            Ok(_) => Ok(ConditionalCreateOutcome::Created),
            Err(error) => {
                let error = map_conditional_create_error("conditionally create file", error);
                if error.kind() == FileErrorKind::AlreadyExists {
                    Ok(ConditionalCreateOutcome::AlreadyExists)
                } else {
                    Err(error)
                }
            }
        }
    }
}

fn normalize_bound_relative_path(path: &Path, original: &str) -> FileResult<String> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(component) => normalized.push(component),
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(FileError::new(
                    FileErrorKind::Permission,
                    format!("local file {original} escapes its authorized root"),
                ));
            }
        }
    }
    let normalized = normalized.to_string_lossy().to_string();
    if normalized.is_empty() {
        return Err(FileError::invalid(format!(
            "local file {original} does not name a file beneath its authorized root"
        )));
    }
    Ok(normalized)
}

pub fn is_object_store_location_parse_only(location: &str) -> FileResult<bool> {
    Ok(FsLocation::parse(location)?.scheme() == FsScheme::ObjectStore)
}

pub fn parse_object_store_path_parse_only(location: &str) -> FileResult<(String, String)> {
    let location = FsLocation::parse(location)?;
    if location.scheme() != FsScheme::ObjectStore {
        return Err(FileError::invalid(format!(
            "location is not an object-store URI: {}",
            location.original()
        )));
    }
    let bucket = location
        .authority()
        .ok_or_else(|| FileError::invalid("object-store location missing bucket"))?;
    Ok((
        bucket.to_string(),
        location.path().trim_start_matches('/').to_string(),
    ))
}

impl Debug for FsAccessHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FsAccessHandle")
            .field("access_domain", &self.access_domain)
            .field("scheme", &self.scheme)
            .field("authority", &self.authority)
            .field("root", &self.root)
            .field("paths", &self.paths)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub struct BoundFile {
    access: FsAccessHandle,
    path: ResolvedFsPath,
    identity: FileIdentity,
}

impl BoundFile {
    pub fn access(&self) -> &FsAccessHandle {
        &self.access
    }

    pub const fn access_domain(&self) -> StorageAccessDomainId {
        self.access.access_domain()
    }

    pub fn location(&self) -> &FsLocation {
        self.path.location()
    }

    pub fn operator_relative_path(&self) -> &str {
        self.path.operator_relative_path()
    }

    pub fn identity(&self) -> &FileIdentity {
        &self.identity
    }

    pub async fn read(
        &self,
        range: FileReadRange,
        cancellation: &FileCancellation,
    ) -> FileResult<Bytes> {
        cancellation.check()?;
        let result = match range {
            FileReadRange::WholeFile => {
                self.access
                    .operator
                    .read(self.operator_relative_path())
                    .await
            }
            FileReadRange::Bounded { offset, length } => {
                let end = offset
                    .checked_add(length)
                    .ok_or_else(|| FileError::invalid("bounded file read range overflows"))?;
                self.access
                    .operator
                    .read_with(self.operator_relative_path())
                    .range(offset..end)
                    .await
            }
        };
        cancellation.check()?;
        result
            .map(|buffer| buffer.to_bytes())
            .map_err(|error| map_opendal_error("read file", error))
    }

    pub async fn stat(&self, cancellation: &FileCancellation) -> FileResult<u64> {
        cancellation.check()?;
        let metadata = self
            .access
            .operator
            .stat(self.operator_relative_path())
            .await
            .map_err(|error| map_opendal_error("stat file", error))?;
        cancellation.check()?;
        Ok(metadata.content_length())
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct FileIdentity {
    path: String,
    file_size: u64,
    modification_time: Option<i64>,
}

impl FileIdentity {
    pub fn new(path: impl Into<String>, file_size: u64, modification_time: Option<i64>) -> Self {
        Self {
            path: path.into(),
            file_size,
            modification_time,
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    pub fn modification_time(&self) -> Option<i64> {
        self.modification_time
    }

    pub fn with_modification_time_override(mut self, modification_time: Option<i64>) -> Self {
        if modification_time.is_some() {
            self.modification_time = modification_time;
        }
        self
    }

    pub fn starrocks_cache_tail(&self) -> u32 {
        if self.modification_time.unwrap_or(0) > 0 {
            ((self.modification_time.unwrap_or(0) >> 9) & 0x0000_0000_FFFF_FFFF) as u32
        } else {
            self.file_size as u32
        }
    }
}

#[derive(Clone)]
pub struct ObjectStoreConfig {
    pub endpoint: String,
    pub access_key_id: SecretValue,
    pub access_key_secret: SecretValue,
    pub session_token: Option<SecretValue>,
    pub enable_path_style_access: Option<bool>,
    pub region: Option<String>,
    pub retry_max_times: Option<usize>,
    pub retry_min_delay_ms: Option<u64>,
    pub retry_max_delay_ms: Option<u64>,
    pub timeout_ms: Option<u64>,
    pub io_timeout_ms: Option<u64>,
}

/// Non-secret object-store configuration that participates in provider reuse.
#[derive(Clone)]
pub struct ObjectStoreEndpointConfig {
    pub endpoint: String,
    pub enable_path_style_access: Option<bool>,
    pub region: Option<String>,
    pub retry_max_times: Option<usize>,
    pub retry_min_delay_ms: Option<u64>,
    pub retry_max_delay_ms: Option<u64>,
    pub timeout_ms: Option<u64>,
    pub io_timeout_ms: Option<u64>,
}

impl Debug for ObjectStoreEndpointConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let endpoint = normalize_s3_endpoint(&self.endpoint).ok();
        f.debug_struct("ObjectStoreEndpointConfig")
            .field(
                "endpoint",
                &endpoint.as_deref().unwrap_or("<invalid-endpoint>"),
            )
            .field("enable_path_style_access", &self.enable_path_style_access)
            .field("region", &self.region)
            .field("retry_max_times", &self.retry_max_times)
            .field("retry_min_delay_ms", &self.retry_min_delay_ms)
            .field("retry_max_delay_ms", &self.retry_max_delay_ms)
            .field("timeout_ms", &self.timeout_ms)
            .field("io_timeout_ms", &self.io_timeout_ms)
            .finish()
    }
}

/// Role-local secret material used only while constructing an object-store provider.
#[derive(Clone)]
pub struct ObjectStoreSecretMaterial {
    pub access_key_id: SecretValue,
    pub access_key_secret: SecretValue,
    pub session_token: Option<SecretValue>,
}

impl Debug for ObjectStoreSecretMaterial {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreSecretMaterial")
            .field("access_key_id", &"<redacted>")
            .field("access_key_secret", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl ObjectStoreConfig {
    pub fn endpoint_config(&self) -> ObjectStoreEndpointConfig {
        ObjectStoreEndpointConfig {
            endpoint: self.endpoint.clone(),
            enable_path_style_access: self.enable_path_style_access,
            region: self.region.clone(),
            retry_max_times: self.retry_max_times,
            retry_min_delay_ms: self.retry_min_delay_ms,
            retry_max_delay_ms: self.retry_max_delay_ms,
            timeout_ms: self.timeout_ms,
            io_timeout_ms: self.io_timeout_ms,
        }
    }

    pub fn secret_material(&self) -> ObjectStoreSecretMaterial {
        ObjectStoreSecretMaterial {
            access_key_id: self.access_key_id.clone(),
            access_key_secret: self.access_key_secret.clone(),
            session_token: self.session_token.clone(),
        }
    }
}

impl Debug for ObjectStoreConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreConfig")
            .field("endpoint_config", &self.endpoint_config())
            .field("access_key_id", &"<redacted>")
            .field("access_key_secret", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectStoreProviderPoolOptions {
    pub capacity: usize,
    pub idle_ttl: Duration,
}

impl Default for ObjectStoreProviderPoolOptions {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_OBJECT_STORE_PROVIDER_POOL_CAPACITY,
            idle_ttl: DEFAULT_OBJECT_STORE_PROVIDER_POOL_IDLE_TTL,
        }
    }
}

impl ObjectStoreProviderPoolOptions {
    pub fn validate(self) -> FileResult<Self> {
        if !(MIN_OBJECT_STORE_PROVIDER_POOL_CAPACITY..=MAX_OBJECT_STORE_PROVIDER_POOL_CAPACITY)
            .contains(&self.capacity)
        {
            return Err(FileError::invalid(format!(
                "object-store provider pool capacity must be in {MIN_OBJECT_STORE_PROVIDER_POOL_CAPACITY}..={MAX_OBJECT_STORE_PROVIDER_POOL_CAPACITY}"
            )));
        }
        if !(MIN_OBJECT_STORE_PROVIDER_POOL_IDLE_TTL..=MAX_OBJECT_STORE_PROVIDER_POOL_IDLE_TTL)
            .contains(&self.idle_ttl)
        {
            return Err(FileError::invalid(
                "object-store provider pool idle TTL must be in 60s..=24h",
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ObjectStoreProviderPoolMetrics {
    pub operator_constructions: u64,
    pub operator_construction_failures: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub singleflight_waits: u64,
    pub resident_entries: usize,
    pub high_water_entries: usize,
    pub capacity_evictions: u64,
    pub idle_expirations: u64,
    pub credential_expirations: u64,
}

/// Explicit, bounded owner for credential-bound object-store operators.
///
/// Keys contain only canonical non-secret endpoint identity, access domain,
/// and credential provider identity. Secret values are never retained as map
/// keys, and construction happens outside the pool lock behind a per-key
/// single-flight reservation.
pub struct ObjectStoreProviderPool {
    state: Arc<ObjectStoreProviderPoolState>,
    janitor: Option<JoinHandle<()>>,
}

impl ObjectStoreProviderPool {
    pub fn new(options: ObjectStoreProviderPoolOptions) -> FileResult<Self> {
        Self::start(options.validate()?)
    }

    #[cfg(test)]
    fn new_for_test(capacity: usize, idle_ttl: Duration) -> FileResult<Self> {
        debug_assert!(capacity > 0);
        Self::start(ObjectStoreProviderPoolOptions { capacity, idle_ttl })
    }

    fn start(options: ObjectStoreProviderPoolOptions) -> FileResult<Self> {
        let state = Arc::new(ObjectStoreProviderPoolState {
            options,
            inner: Mutex::new(ObjectStoreProviderPoolInner::default()),
            wake: Condvar::new(),
        });
        let weak_state = Arc::downgrade(&state);
        let janitor = std::thread::Builder::new()
            .name("novarocks-object-store-provider-janitor".to_string())
            .spawn(move || run_object_store_provider_janitor(weak_state))
            .map_err(|error| {
                FileError::with_source(
                    FileErrorKind::Internal,
                    "spawn object-store provider janitor",
                    error,
                )
            })?;
        Ok(Self {
            state,
            janitor: Some(janitor),
        })
    }

    pub fn options(&self) -> ObjectStoreProviderPoolOptions {
        self.state.options
    }

    pub fn metrics_snapshot(&self) -> FileResult<ObjectStoreProviderPoolMetrics> {
        let inner = self.state.lock_inner()?;
        Ok(inner.metrics())
    }

    #[cfg(test)]
    fn acquire_with_builder<F>(
        &self,
        access_domain: StorageAccessDomainId,
        bucket: &str,
        endpoint_config: &ObjectStoreEndpointConfig,
        credential_identity: &ObjectStoreCredentialProviderIdentity,
        secret_material: &ObjectStoreSecretMaterial,
        builder: F,
    ) -> FileResult<Operator>
    where
        F: FnOnce(&ObjectStoreEndpointIdentity, &ObjectStoreSecretMaterial) -> FileResult<Operator>,
    {
        self.acquire_with_expiration(
            access_domain,
            bucket,
            endpoint_config,
            credential_identity,
            secret_material,
            None,
            builder,
        )
    }

    fn acquire_with_expiration<F>(
        &self,
        access_domain: StorageAccessDomainId,
        bucket: &str,
        endpoint_config: &ObjectStoreEndpointConfig,
        credential_identity: &ObjectStoreCredentialProviderIdentity,
        secret_material: &ObjectStoreSecretMaterial,
        credential_expires_at: Option<Instant>,
        builder: F,
    ) -> FileResult<Operator>
    where
        F: FnOnce(&ObjectStoreEndpointIdentity, &ObjectStoreSecretMaterial) -> FileResult<Operator>,
    {
        let endpoint = ObjectStoreEndpointIdentity::try_new(bucket, endpoint_config)?;
        let key = ObjectStoreProviderKey {
            endpoint: endpoint.clone(),
            access_domain,
            credential_identity: credential_identity.clone(),
        };
        let now = Instant::now();
        if credential_expires_at.is_some_and(|expires_at| expires_at <= now) {
            return Err(FileError::invalid(
                "object-store vended credential has expired",
            ));
        }
        let acquisition = {
            let mut inner = self.state.lock_inner()?;
            inner.expire_due(now);
            if let Some(operator) = inner.touch(
                &key,
                now,
                self.state.options.idle_ttl,
                credential_expires_at,
            ) {
                inner.cache_hits = inner.cache_hits.saturating_add(1);
                self.state.wake.notify_one();
                return Ok(operator);
            }
            inner.cache_misses = inner.cache_misses.saturating_add(1);
            if let Some(reservation) = inner.inflight.get(&key).cloned() {
                inner.singleflight_waits = inner.singleflight_waits.saturating_add(1);
                ProviderAcquisition::Wait(reservation)
            } else {
                if inner.inflight.len() >= self.state.options.capacity {
                    return Err(FileError::new(
                        FileErrorKind::ResourceExhausted,
                        "object-store provider construction reservations are full",
                    ));
                }
                let reservation = Arc::new(ObjectStoreBuildReservation::default());
                inner.inflight.insert(key.clone(), Arc::clone(&reservation));
                ProviderAcquisition::Build(reservation)
            }
        };

        let reservation = match acquisition {
            ProviderAcquisition::Build(reservation) => reservation,
            ProviderAcquisition::Wait(reservation) => {
                return reservation.wait(OBJECT_STORE_PROVIDER_BUILD_WAIT_TIMEOUT);
            }
        };

        let build_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            builder(&endpoint, secret_material)
        }))
        .unwrap_or_else(|_| {
            Err(FileError::new(
                FileErrorKind::Internal,
                "object-store provider construction panicked",
            ))
        });
        let result = {
            let mut inner = self.state.lock_inner()?;
            inner.inflight.remove(&key);
            match build_result {
                Ok(operator) => {
                    if inner.entries.len() >= self.state.options.capacity {
                        inner.evict_oldest();
                    }
                    inner.operator_constructions = inner.operator_constructions.saturating_add(1);
                    let insert_now = Instant::now();
                    if credential_expires_at.is_some_and(|expires_at| expires_at <= insert_now) {
                        Err(FileError::invalid(
                            "object-store vended credential expired during provider construction",
                        ))
                    } else {
                        inner.insert(
                            key,
                            operator.clone(),
                            insert_now,
                            self.state.options.idle_ttl,
                            credential_expires_at,
                        );
                        Ok(operator)
                    }
                }
                Err(error) => {
                    inner.operator_construction_failures =
                        inner.operator_construction_failures.saturating_add(1);
                    Err(error)
                }
            }
        };
        reservation.complete(&result);
        self.state.wake.notify_all();
        result
    }
}

impl Drop for ObjectStoreProviderPool {
    fn drop(&mut self) {
        if let Ok(mut inner) = self.state.inner.lock() {
            inner.shutting_down = true;
        }
        self.state.wake.notify_all();
        if let Some(janitor) = self.janitor.take() {
            let _ = janitor.join();
        }
    }
}

impl Debug for ObjectStoreProviderPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreProviderPool")
            .field("options", &self.state.options)
            .finish_non_exhaustive()
    }
}

struct ObjectStoreProviderPoolState {
    options: ObjectStoreProviderPoolOptions,
    inner: Mutex<ObjectStoreProviderPoolInner>,
    wake: Condvar,
}

impl ObjectStoreProviderPoolState {
    fn lock_inner(&self) -> FileResult<std::sync::MutexGuard<'_, ObjectStoreProviderPoolInner>> {
        self.inner
            .lock()
            .map_err(|_| FileError::new(FileErrorKind::Internal, "lock object-store provider pool"))
    }
}

fn run_object_store_provider_janitor(weak_state: Weak<ObjectStoreProviderPoolState>) {
    let Some(state) = weak_state.upgrade() else {
        return;
    };
    let Ok(mut inner) = state.inner.lock() else {
        return;
    };
    loop {
        if inner.shutting_down {
            return;
        }
        inner.expire_due(Instant::now());
        let wait = inner
            .next_expiration()
            .map(|deadline| deadline.saturating_duration_since(Instant::now()));
        inner = match wait {
            Some(wait) => match state.wake.wait_timeout(inner, wait) {
                Ok((inner, _)) => inner,
                Err(_) => return,
            },
            None => match state.wake.wait(inner) {
                Ok(inner) => inner,
                Err(_) => return,
            },
        };
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ObjectStoreEndpointIdentity {
    bucket: String,
    endpoint: String,
    use_path_style: bool,
    region: String,
    retry_max_times: usize,
    retry_min_delay_ms: u64,
    retry_max_delay_ms: u64,
    timeout_ms: u64,
    io_timeout_ms: u64,
}

impl ObjectStoreEndpointIdentity {
    fn try_new(bucket: &str, config: &ObjectStoreEndpointConfig) -> FileResult<Self> {
        let bucket = bucket.trim();
        if bucket.is_empty() {
            return Err(FileError::invalid("empty object-store bucket"));
        }
        let endpoint = normalize_s3_endpoint(&config.endpoint)?;
        Ok(Self {
            bucket: bucket.to_string(),
            use_path_style: config
                .enable_path_style_access
                .unwrap_or_else(|| !prefer_virtual_host_style(&endpoint)),
            endpoint,
            region: config
                .region
                .as_deref()
                .filter(|region| !region.is_empty())
                .unwrap_or("us-east-1")
                .to_string(),
            retry_max_times: config
                .retry_max_times
                .unwrap_or(DEFAULT_OBJECT_STORE_RETRY_MAX_TIMES),
            retry_min_delay_ms: config
                .retry_min_delay_ms
                .unwrap_or(DEFAULT_OBJECT_STORE_RETRY_MIN_DELAY_MS),
            retry_max_delay_ms: config
                .retry_max_delay_ms
                .unwrap_or(DEFAULT_OBJECT_STORE_RETRY_MAX_DELAY_MS),
            timeout_ms: config.timeout_ms.unwrap_or(DEFAULT_OBJECT_STORE_TIMEOUT_MS),
            io_timeout_ms: config
                .io_timeout_ms
                .unwrap_or(DEFAULT_OBJECT_STORE_IO_TIMEOUT_MS),
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ObjectStoreProviderKey {
    endpoint: ObjectStoreEndpointIdentity,
    access_domain: StorageAccessDomainId,
    credential_identity: ObjectStoreCredentialProviderIdentity,
}

struct ObjectStoreProviderEntry {
    operator: Operator,
    expiration_key: (Instant, u64),
    credential_expires_at: Option<Instant>,
}

#[derive(Default)]
struct ObjectStoreProviderPoolInner {
    entries: HashMap<ObjectStoreProviderKey, ObjectStoreProviderEntry>,
    expiration_index: BTreeMap<(Instant, u64), ObjectStoreProviderKey>,
    inflight: HashMap<ObjectStoreProviderKey, Arc<ObjectStoreBuildReservation>>,
    next_sequence: u64,
    operator_constructions: u64,
    operator_construction_failures: u64,
    cache_hits: u64,
    cache_misses: u64,
    singleflight_waits: u64,
    high_water_entries: usize,
    capacity_evictions: u64,
    idle_expirations: u64,
    credential_expirations: u64,
    shutting_down: bool,
}

impl ObjectStoreProviderPoolInner {
    fn touch(
        &mut self,
        key: &ObjectStoreProviderKey,
        now: Instant,
        idle_ttl: Duration,
        credential_expires_at: Option<Instant>,
    ) -> Option<Operator> {
        let entry = self.entries.remove(key)?;
        self.expiration_index.remove(&entry.expiration_key);
        let operator = entry.operator.clone();
        self.insert(
            key.clone(),
            entry.operator,
            now,
            idle_ttl,
            earlier_deadline(entry.credential_expires_at, credential_expires_at),
        );
        Some(operator)
    }

    fn insert(
        &mut self,
        key: ObjectStoreProviderKey,
        operator: Operator,
        now: Instant,
        idle_ttl: Duration,
        credential_expires_at: Option<Instant>,
    ) {
        self.next_sequence = self.next_sequence.wrapping_add(1);
        let idle_expires_at = now.checked_add(idle_ttl).unwrap_or(now);
        let expires_at = earlier_deadline(Some(idle_expires_at), credential_expires_at)
            .expect("idle expiration is always present");
        let expiration_key = (expires_at, self.next_sequence);
        self.expiration_index.insert(expiration_key, key.clone());
        self.entries.insert(
            key,
            ObjectStoreProviderEntry {
                operator,
                expiration_key,
                credential_expires_at,
            },
        );
        self.high_water_entries = self.high_water_entries.max(self.entries.len());
    }

    fn expire_due(&mut self, now: Instant) {
        loop {
            let expired = self
                .expiration_index
                .first_key_value()
                .filter(|(expiration, _)| expiration.0 <= now)
                .map(|(expiration, key)| (*expiration, key.clone()));
            let Some((expiration, key)) = expired else {
                return;
            };
            self.expiration_index.remove(&expiration);
            if let Some(entry) = self.entries.remove(&key) {
                if entry
                    .credential_expires_at
                    .is_some_and(|deadline| deadline <= now)
                {
                    self.credential_expirations = self.credential_expirations.saturating_add(1);
                } else {
                    self.idle_expirations = self.idle_expirations.saturating_add(1);
                }
            }
        }
    }

    fn next_expiration(&self) -> Option<Instant> {
        self.expiration_index
            .first_key_value()
            .map(|(expiration, _)| expiration.0)
    }

    fn evict_oldest(&mut self) {
        if let Some((_, oldest)) = self.expiration_index.pop_first() {
            self.entries.remove(&oldest);
            self.capacity_evictions = self.capacity_evictions.saturating_add(1);
        }
    }

    fn metrics(&self) -> ObjectStoreProviderPoolMetrics {
        ObjectStoreProviderPoolMetrics {
            operator_constructions: self.operator_constructions,
            operator_construction_failures: self.operator_construction_failures,
            cache_hits: self.cache_hits,
            cache_misses: self.cache_misses,
            singleflight_waits: self.singleflight_waits,
            resident_entries: self.entries.len(),
            high_water_entries: self.high_water_entries,
            capacity_evictions: self.capacity_evictions,
            idle_expirations: self.idle_expirations,
            credential_expirations: self.credential_expirations,
        }
    }
}

fn earlier_deadline(left: Option<Instant>, right: Option<Instant>) -> Option<Instant> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
        (None, None) => None,
    }
}

enum ProviderAcquisition {
    Build(Arc<ObjectStoreBuildReservation>),
    Wait(Arc<ObjectStoreBuildReservation>),
}

#[derive(Default)]
struct ObjectStoreBuildReservation {
    state: Mutex<ObjectStoreBuildReservationState>,
    ready: Condvar,
}

#[derive(Clone, Default)]
enum ObjectStoreBuildReservationState {
    #[default]
    Building,
    Complete(Result<Operator, ObjectStoreBuildFailure>),
}

#[derive(Clone)]
struct ObjectStoreBuildFailure {
    kind: FileErrorKind,
}

impl ObjectStoreBuildReservation {
    fn wait(&self, timeout: Duration) -> FileResult<Operator> {
        let state = self.state.lock().map_err(|_| {
            FileError::new(
                FileErrorKind::Internal,
                "lock object-store provider construction reservation",
            )
        })?;
        let (state, wait) = self
            .ready
            .wait_timeout_while(state, timeout, |state| {
                matches!(state, ObjectStoreBuildReservationState::Building)
            })
            .map_err(|_| {
                FileError::new(
                    FileErrorKind::Internal,
                    "wait for object-store provider construction",
                )
            })?;
        match &*state {
            ObjectStoreBuildReservationState::Complete(Ok(operator)) => Ok(operator.clone()),
            ObjectStoreBuildReservationState::Complete(Err(error)) => Err(FileError::new(
                error.kind,
                "object-store provider construction failed",
            )),
            ObjectStoreBuildReservationState::Building if wait.timed_out() => Err(
                FileError::deadline("timed out waiting for object-store provider construction"),
            ),
            ObjectStoreBuildReservationState::Building => Err(FileError::new(
                FileErrorKind::Internal,
                "object-store provider construction waiter woke without completion",
            )),
        }
    }

    fn complete(&self, result: &FileResult<Operator>) {
        if let Ok(mut state) = self.state.lock() {
            *state = ObjectStoreBuildReservationState::Complete(match result {
                Ok(operator) => Ok(operator.clone()),
                Err(error) => Err(ObjectStoreBuildFailure { kind: error.kind() }),
            });
        }
        self.ready.notify_all();
    }
}

/// Non-secret identity of the credential provider bound to one Operator.
///
/// M2 extends this enum with query-attempt lease identity and epoch; secret
/// material remains outside this type and outside all pool keys.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ObjectStoreCredentialProviderIdentity {
    Static(StaticCredentialReference),
    Vended {
        lease_id: CredentialLeaseId,
        epoch: u64,
    },
}

pub struct ObjectStoreAccessContext<'a> {
    endpoint_config: ObjectStoreEndpointConfig,
    credential_identity: ObjectStoreCredentialProviderIdentity,
    secret_material: ObjectStoreSecretMaterial,
    provider_pool: &'a ObjectStoreProviderPool,
    credential_expires_at: Option<Instant>,
}

impl<'a> ObjectStoreAccessContext<'a> {
    pub fn new(
        endpoint_config: ObjectStoreEndpointConfig,
        credential_identity: ObjectStoreCredentialProviderIdentity,
        secret_material: ObjectStoreSecretMaterial,
        provider_pool: &'a ObjectStoreProviderPool,
    ) -> Self {
        Self {
            endpoint_config,
            credential_identity,
            secret_material,
            provider_pool,
            credential_expires_at: None,
        }
    }

    /// Bounds one query-scoped credential to its local lease expiration.
    /// Pool entries retain no secret material and cannot outlive this deadline.
    pub fn with_credential_expiration(mut self, credential_expires_at: Instant) -> Self {
        self.credential_expires_at = Some(credential_expires_at);
        self
    }
}

impl Debug for ObjectStoreAccessContext<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreAccessContext")
            .field("endpoint_config", &self.endpoint_config)
            .field("credential_identity", &self.credential_identity)
            .field("secret_material", &self.secret_material)
            .field("provider_pool", &self.provider_pool)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct FsAccessResolver;

impl FsAccessResolver {
    pub fn new() -> Self {
        Self
    }

    pub fn parse_location(&self, raw: impl AsRef<str>) -> FileResult<FsLocation> {
        FsLocation::parse(raw)
    }

    pub fn parse_locations<I, S>(&self, locations: I) -> FileResult<Vec<FsLocation>>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        locations
            .into_iter()
            .map(|location| self.parse_location(location))
            .collect()
    }

    pub fn resolve_location(
        &self,
        access_domain: StorageAccessDomainId,
        location: impl AsRef<str>,
        object_store_access: Option<ObjectStoreAccessContext<'_>>,
    ) -> FileResult<FsAccessHandle> {
        self.resolve_locations(
            access_domain,
            std::iter::once(location),
            object_store_access,
        )
    }

    pub fn resolve_locations<I, S>(
        &self,
        access_domain: StorageAccessDomainId,
        locations: I,
        object_store_access: Option<ObjectStoreAccessContext<'_>>,
    ) -> FileResult<FsAccessHandle>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let locations = self.parse_locations(locations)?;
        let first = locations
            .first()
            .ok_or_else(|| FileError::invalid("fs access locations are empty"))?;
        let scheme = first.scheme();
        if locations.iter().any(|location| location.scheme() != scheme) {
            return Err(FileError::invalid(
                "mixed fs location schemes are not allowed",
            ));
        }

        match scheme {
            FsScheme::Local => resolve_local_locations(access_domain, locations),
            FsScheme::ObjectStore => {
                resolve_object_store_locations(access_domain, locations, object_store_access)
            }
            FsScheme::Hdfs => resolve_hdfs_locations(access_domain, locations),
        }
    }
}

fn resolve_local_locations(
    access_domain: StorageAccessDomainId,
    locations: Vec<FsLocation>,
) -> FileResult<FsAccessHandle> {
    let raw_paths = locations.iter().map(FsLocation::path).collect::<Vec<_>>();
    let (root, relative_paths) = normalize_local_paths(&raw_paths)?;
    let builder = opendal::services::Fs::default().root(&root);
    let operator = Operator::new(builder)
        .map_err(|error| map_opendal_error("initialize local file operator", error))?
        .finish();
    let paths = locations
        .into_iter()
        .zip(relative_paths)
        .map(|(location, relative_path)| ResolvedFsPath::new(location, relative_path))
        .collect::<FileResult<Vec<_>>>()?;
    Ok(FsAccessHandle::new(
        access_domain,
        FsScheme::Local,
        operator,
        None,
        Some(root),
        paths,
    ))
}

fn normalize_local_paths(paths: &[&str]) -> FileResult<(String, Vec<String>)> {
    if paths.is_empty() {
        return Err(FileError::invalid("file paths are empty"));
    }

    let mut absolute = None;
    let mut directories = Vec::with_capacity(paths.len());
    for raw in paths {
        let path = Path::new(raw.trim());
        let is_absolute = path.is_absolute();
        if absolute.is_some_and(|previous| previous != is_absolute) {
            return Err(FileError::invalid(
                "mixed absolute and relative local paths are not allowed",
            ));
        }
        absolute.get_or_insert(is_absolute);
        let directory = path.parent().unwrap_or(Path::new(""));
        directories.push(if directory.as_os_str().is_empty() {
            PathBuf::from(".")
        } else {
            directory.to_path_buf()
        });
    }

    let root = common_path_prefix(&directories)
        .ok_or_else(|| FileError::invalid("failed to compute common local root"))?;
    let root_string = if root.as_os_str().is_empty() {
        ".".to_string()
    } else {
        root.to_string_lossy().to_string()
    };
    let relative_paths = paths
        .iter()
        .map(|raw| {
            let path = Path::new(raw.trim());
            let relative = if root == Path::new(".") {
                path.to_path_buf()
            } else {
                path.strip_prefix(&root)
                    .map_err(|_| {
                        FileError::invalid(format!(
                            "local path {raw} does not start with root {root_string}"
                        ))
                    })?
                    .to_path_buf()
            };
            let relative = relative.to_string_lossy().to_string();
            if relative.is_empty() {
                return Err(FileError::invalid(format!(
                    "invalid local path after stripping root: {raw}"
                )));
            }
            Ok(relative)
        })
        .collect::<FileResult<Vec<_>>>()?;
    Ok((root_string, relative_paths))
}

fn common_path_prefix(paths: &[PathBuf]) -> Option<PathBuf> {
    let mut paths = paths.iter();
    let first = paths.next()?.components().collect::<Vec<_>>();
    let mut prefix_len = first.len();
    for path in paths {
        let components = path.components().collect::<Vec<_>>();
        prefix_len = prefix_len.min(components.len());
        for index in 0..prefix_len {
            if components[index] != first[index] {
                prefix_len = index;
                break;
            }
        }
    }
    if prefix_len == 0 {
        return None;
    }
    let mut root = PathBuf::new();
    for component in &first[..prefix_len] {
        root.push(component.as_os_str());
    }
    Some(root)
}

fn resolve_object_store_locations(
    access_domain: StorageAccessDomainId,
    locations: Vec<FsLocation>,
    object_store_access: Option<ObjectStoreAccessContext<'_>>,
) -> FileResult<FsAccessHandle> {
    let object_store_access = object_store_access.ok_or_else(|| {
        FileError::invalid("object-store location requires explicit access context")
    })?;
    let bucket = locations
        .first()
        .and_then(FsLocation::authority)
        .ok_or_else(|| FileError::invalid("object-store location missing bucket"))?
        .to_string();
    if locations
        .iter()
        .any(|location| location.authority() != Some(bucket.as_str()))
    {
        return Err(FileError::invalid(
            "mixed object-store buckets are not allowed",
        ));
    }
    let operator = object_store_access.provider_pool.acquire_with_expiration(
        access_domain,
        &bucket,
        &object_store_access.endpoint_config,
        &object_store_access.credential_identity,
        &object_store_access.secret_material,
        object_store_access.credential_expires_at,
        build_object_store_operator,
    )?;
    let paths = locations
        .into_iter()
        .map(|location| {
            let relative_path = location.path().trim_start_matches('/').to_string();
            ResolvedFsPath::new(location, relative_path)
        })
        .collect::<FileResult<Vec<_>>>()?;
    Ok(FsAccessHandle::new(
        access_domain,
        FsScheme::ObjectStore,
        operator,
        Some(bucket),
        None,
        paths,
    ))
}

fn build_object_store_operator(
    endpoint: &ObjectStoreEndpointIdentity,
    secrets: &ObjectStoreSecretMaterial,
) -> FileResult<Operator> {
    let mut builder = opendal::services::S3::default()
        .endpoint(&endpoint.endpoint)
        .bucket(&endpoint.bucket)
        .region(&endpoint.region)
        .access_key_id(secrets.access_key_id.expose_secret())
        .secret_access_key(secrets.access_key_secret.expose_secret());
    if !endpoint.use_path_style {
        builder = builder.enable_virtual_host_style();
    }
    if let Some(session_token) = secrets.session_token.as_ref() {
        builder = builder.session_token(session_token.expose_secret());
    }
    let mut operator = Operator::new(builder)
        .map_err(|error| map_opendal_error("initialize object store operator", error))?
        .finish();
    if is_local_endpoint(&endpoint.endpoint) {
        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .map_err(|error| {
                FileError::with_source(
                    FileErrorKind::Internal,
                    "build local object store HTTP client",
                    error,
                )
            })?;
        operator = operator.layer(HttpClientLayer::new(opendal::raw::HttpClient::with(client)));
    }
    let mut timeout = TimeoutLayer::new();
    timeout = timeout.with_timeout(Duration::from_millis(endpoint.timeout_ms));
    timeout = timeout.with_io_timeout(Duration::from_millis(endpoint.io_timeout_ms));
    operator = operator.layer(timeout);
    operator = operator.layer(
        ConcurrentLimitLayer::new(DEFAULT_OBJECT_STORE_CONCURRENT_LIMIT)
            .with_http_concurrent_limit(DEFAULT_OBJECT_STORE_HTTP_CONCURRENT_LIMIT),
    );
    operator = operator.layer(
        RetryLayer::new()
            .with_jitter()
            .with_min_delay(Duration::from_millis(endpoint.retry_min_delay_ms))
            .with_max_delay(Duration::from_millis(endpoint.retry_max_delay_ms))
            .with_max_times(endpoint.retry_max_times),
    );
    Ok(operator)
}

fn normalize_s3_endpoint(raw_endpoint: &str) -> FileResult<String> {
    let endpoint = raw_endpoint.trim();
    if endpoint.is_empty() {
        return Err(FileError::invalid("empty object-store endpoint"));
    }

    let explicit_scheme = endpoint.contains("://");
    let candidate = if explicit_scheme {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    };
    let mut url = Url::parse(&candidate)
        .map_err(|_| FileError::invalid("invalid object-store endpoint URL"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(FileError::invalid(
            "object-store endpoint scheme must be http or https",
        ));
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(FileError::invalid(
            "object-store endpoint must not contain userinfo, query, or fragment",
        ));
    }
    let host = url
        .host_str()
        .ok_or_else(|| FileError::invalid("object-store endpoint is missing a host"))?;
    if !explicit_scheme {
        let scheme = if host.eq_ignore_ascii_case("localhost") || host.parse::<IpAddr>().is_ok() {
            "http"
        } else {
            "https"
        };
        url.set_scheme(scheme)
            .map_err(|_| FileError::invalid("invalid object-store endpoint scheme"))?;
    }
    let default_port = match url.scheme() {
        "http" => 80,
        "https" => 443,
        _ => unreachable!(),
    };
    if url.port() == Some(default_port) {
        url.set_port(None)
            .map_err(|_| FileError::invalid("invalid object-store endpoint port"))?;
    }

    let normalized_path = url
        .path()
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join("/");
    if normalized_path.is_empty() {
        url.set_path("/");
    } else {
        url.set_path(&format!("/{normalized_path}"));
    }
    Ok(url.to_string().trim_end_matches('/').to_string())
}

fn endpoint_host(endpoint: &str) -> String {
    Url::parse(endpoint)
        .ok()
        .and_then(|url| url.host().map(|host| host.to_owned()))
        .map(|host| match host {
            Host::Domain(host) => host.to_ascii_lowercase(),
            Host::Ipv4(host) => host.to_string(),
            Host::Ipv6(host) => host.to_string(),
        })
        .unwrap_or_default()
}

fn is_local_endpoint(endpoint: &str) -> bool {
    let host = endpoint_host(endpoint);
    host == "localhost" || host.parse::<IpAddr>().is_ok()
}

fn prefer_virtual_host_style(endpoint: &str) -> bool {
    let host = endpoint_host(endpoint);
    [
        ".amazonaws.com",
        ".aliyuncs.com",
        ".myhuaweicloud.com",
        ".myqcloud.com",
        ".volces.com",
        ".ivolces.com",
        ".ksyuncs.com",
        "storage.googleapis.com",
    ]
    .iter()
    .any(|suffix| host.ends_with(suffix))
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct HdfsOperatorKey {
    name_node: String,
    user: Option<String>,
}

fn hdfs_operator_cache() -> &'static Mutex<HashMap<HdfsOperatorKey, Operator>> {
    static CACHE: OnceLock<Mutex<HashMap<HdfsOperatorKey, Operator>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn resolve_hdfs_locations(
    access_domain: StorageAccessDomainId,
    locations: Vec<FsLocation>,
) -> FileResult<FsAccessHandle> {
    let mut parsed = locations
        .iter()
        .map(|location| parse_hdfs_path(location.original()))
        .collect::<FileResult<Vec<_>>>()?;
    let first = parsed
        .first()
        .cloned()
        .ok_or_else(|| FileError::invalid("file paths are empty"))?;
    if parsed.iter().any(|path| path.name_node != first.name_node) {
        return Err(FileError::invalid(
            "mixed hdfs namenodes are not allowed in one access handle",
        ));
    }
    if parsed.iter().any(|path| path.user != first.user) {
        return Err(FileError::invalid(
            "mixed hdfs users are not allowed in one access handle",
        ));
    }
    let operator = build_hdfs_operator(&first.name_node, first.user.as_deref())?;
    let paths = locations
        .into_iter()
        .zip(parsed.drain(..))
        .map(|(location, path)| ResolvedFsPath::new(location, path.relative_path))
        .collect::<FileResult<Vec<_>>>()?;
    Ok(FsAccessHandle::new(
        access_domain,
        FsScheme::Hdfs,
        operator,
        Some(first.name_node.clone()),
        Some(first.name_node),
        paths,
    ))
}

#[derive(Clone)]
struct HdfsPath {
    name_node: String,
    user: Option<String>,
    relative_path: String,
}

fn parse_hdfs_path(raw: &str) -> FileResult<HdfsPath> {
    let url = Url::parse(raw).map_err(|error| {
        FileError::with_source(FileErrorKind::Invalid, "parse hdfs path", error)
    })?;
    if url.scheme() != "hdfs" {
        return Err(FileError::invalid(format!(
            "invalid hdfs path scheme: {}",
            url.scheme()
        )));
    }
    if url.password().is_some() || url.query().is_some() || url.fragment().is_some() {
        return Err(FileError::invalid(
            "hdfs path must not include password, query, or fragment",
        ));
    }
    let host = url
        .host_str()
        .ok_or_else(|| FileError::invalid("hdfs path missing host"))?;
    let authority = url
        .port()
        .map(|port| format!("{host}:{port}"))
        .unwrap_or_else(|| host.to_string());
    let relative_path = url.path().trim_start_matches('/').to_string();
    if relative_path.is_empty() {
        return Err(FileError::invalid(
            "hdfs path points to namenode root and cannot be used as file path",
        ));
    }
    Ok(HdfsPath {
        name_node: format!("hdfs://{authority}"),
        user: (!url.username().is_empty()).then(|| url.username().to_string()),
        relative_path,
    })
}

fn build_hdfs_operator(name_node: &str, user: Option<&str>) -> FileResult<Operator> {
    let key = HdfsOperatorKey {
        name_node: name_node.to_string(),
        user: user.map(str::to_string),
    };
    if let Some(operator) = hdfs_operator_cache()
        .lock()
        .map_err(|_| FileError::new(FileErrorKind::Internal, "lock hdfs operator cache"))?
        .get(&key)
        .cloned()
    {
        return Ok(operator);
    }
    let mut url = Url::parse(name_node)
        .map_err(|error| FileError::with_source(FileErrorKind::Invalid, "parse namenode", error))?;
    if let Some(user) = user {
        url.set_username(user)
            .map_err(|_| FileError::invalid("invalid hdfs user"))?;
    }
    let builder = opendal::services::HdfsNative::default()
        .name_node(url.as_str().trim_end_matches('/'))
        .root("/");
    let operator = Operator::new(builder)
        .map_err(|error| map_opendal_error("initialize hdfs operator", error))?
        .finish();
    hdfs_operator_cache()
        .lock()
        .map_err(|_| FileError::new(FileErrorKind::Internal, "lock hdfs operator cache"))?
        .insert(key, operator.clone());
    Ok(operator)
}

fn map_opendal_error(operation: &str, error: opendal::Error) -> FileError {
    let kind = match error.kind() {
        opendal::ErrorKind::NotFound => FileErrorKind::NotFound,
        opendal::ErrorKind::AlreadyExists => FileErrorKind::AlreadyExists,
        opendal::ErrorKind::PermissionDenied => FileErrorKind::Permission,
        opendal::ErrorKind::Unsupported => FileErrorKind::Unsupported,
        opendal::ErrorKind::RateLimited
        | opendal::ErrorKind::Unexpected
        | opendal::ErrorKind::ConditionNotMatch => FileErrorKind::Transient,
        _ => FileErrorKind::Internal,
    };
    FileError::with_source(kind, operation, error)
}

fn map_conditional_create_error(operation: &str, error: opendal::Error) -> FileError {
    let kind = match error.kind() {
        opendal::ErrorKind::AlreadyExists | opendal::ErrorKind::ConditionNotMatch => {
            FileErrorKind::AlreadyExists
        }
        _ => return map_opendal_error(operation, error),
    };
    FileError::with_source(kind, operation, error)
}

fn split_uri_scheme(value: &str) -> Option<(&str, &str)> {
    if let Some(rest) = value.strip_prefix("file:") {
        return Some(("file", rest));
    }

    let colon = value.find("://")?;
    let scheme = &value[..colon];
    if scheme.is_empty() || !scheme.as_bytes()[0].is_ascii_alphabetic() {
        return None;
    }
    if !scheme
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'.' | b'-'))
    {
        return None;
    }
    Some((scheme, &value[colon + 1..]))
}

fn parse_authority_and_path(
    original: &str,
    rest: &str,
    require_authority: bool,
    scheme: &str,
) -> FileResult<(Option<String>, String)> {
    let Some(without_prefix) = rest.strip_prefix("//") else {
        return Err(FileError::invalid(format!(
            "{scheme} location must use {scheme}://authority/path: {original}"
        )));
    };
    let (authority, path) = without_prefix
        .split_once('/')
        .unwrap_or((without_prefix, ""));
    if require_authority && authority.is_empty() {
        return Err(FileError::invalid(format!(
            "{scheme} location missing authority: {original}"
        )));
    }
    let path = if path.is_empty() {
        ""
    } else {
        &without_prefix[authority.len()..]
    };
    ensure_non_empty_path(original, scheme, path)?;
    Ok((
        (!authority.is_empty()).then(|| authority.to_string()),
        path.to_string(),
    ))
}

fn ensure_non_empty_path(original: &str, scheme: &str, path: &str) -> FileResult<()> {
    if path.is_empty() || path == "/" {
        return Err(FileError::invalid(format!(
            "{scheme} location missing file path: {original}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn domain(value: u8) -> StorageAccessDomainId {
        StorageAccessDomainId::from_bytes([value; 32])
    }

    fn credential_identity(generation: &str) -> ObjectStoreCredentialProviderIdentity {
        ObjectStoreCredentialProviderIdentity::Static(
            StaticCredentialReference::try_new("warehouse-data", generation).unwrap(),
        )
    }

    fn vended_identity(epoch: u64) -> ObjectStoreCredentialProviderIdentity {
        ObjectStoreCredentialProviderIdentity::Vended {
            lease_id: CredentialLeaseId::try_from_bytes([0x5A; 16]).unwrap(),
            epoch,
        }
    }

    fn endpoint_config(endpoint: &str) -> ObjectStoreEndpointConfig {
        ObjectStoreEndpointConfig {
            endpoint: endpoint.to_string(),
            enable_path_style_access: Some(true),
            region: Some("us-east-1".to_string()),
            retry_max_times: None,
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: None,
            io_timeout_ms: None,
        }
    }

    fn secret_material(secret_suffix: &str) -> ObjectStoreSecretMaterial {
        ObjectStoreSecretMaterial {
            access_key_id: SecretValue::new(format!("access-{secret_suffix}")),
            access_key_secret: SecretValue::new(format!("secret-{secret_suffix}")),
            session_token: Some(SecretValue::new(format!("token-{secret_suffix}"))),
        }
    }

    fn legacy_config(endpoint: &str, secret_suffix: &str) -> ObjectStoreConfig {
        ObjectStoreConfig {
            endpoint: endpoint.to_string(),
            access_key_id: SecretValue::new(format!("access-{secret_suffix}")),
            access_key_secret: SecretValue::new(format!("secret-{secret_suffix}")),
            session_token: Some(SecretValue::new(format!("token-{secret_suffix}"))),
            enable_path_style_access: Some(true),
            region: Some("us-east-1".to_string()),
            retry_max_times: None,
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: None,
            io_timeout_ms: None,
        }
    }

    fn memory_operator() -> FileResult<Operator> {
        Operator::new(opendal::services::Memory::default())
            .map(|builder| builder.finish())
            .map_err(|error| map_opendal_error("initialize test memory operator", error))
    }

    #[test]
    fn provider_pool_options_enforce_capacity_and_idle_ttl_bounds() {
        for capacity in [0, 16_385] {
            assert!(
                ObjectStoreProviderPool::new(ObjectStoreProviderPoolOptions {
                    capacity,
                    idle_ttl: Duration::from_secs(60),
                })
                .is_err()
            );
        }
        for idle_ttl in [Duration::from_secs(59), Duration::from_secs(86_401)] {
            assert!(
                ObjectStoreProviderPool::new(ObjectStoreProviderPoolOptions {
                    capacity: 1,
                    idle_ttl,
                })
                .is_err()
            );
        }
        assert!(ObjectStoreProviderPool::new(ObjectStoreProviderPoolOptions::default()).is_ok());
    }

    #[test]
    fn provider_pool_reuses_static_reference_without_secret_keys() {
        let pool = ObjectStoreProviderPool::new(ObjectStoreProviderPoolOptions::default()).unwrap();
        let endpoint = endpoint_config("http://localhost:9000");
        let identity = credential_identity("blue");
        pool.acquire_with_builder(
            domain(1),
            "warehouse",
            &endpoint,
            &identity,
            &secret_material("first"),
            |_, _| memory_operator(),
        )
        .unwrap();
        pool.acquire_with_builder(
            domain(1),
            "warehouse",
            &endpoint,
            &identity,
            &secret_material("different-value-same-reference"),
            |_, _| -> FileResult<Operator> {
                panic!("secret material must not participate in the provider key")
            },
        )
        .unwrap();

        let metrics = pool.metrics_snapshot().unwrap();
        assert_eq!(metrics.operator_constructions, 1);
        assert_eq!(metrics.cache_hits, 1);
        assert_eq!(metrics.cache_misses, 1);
        assert_eq!(metrics.resident_entries, 1);
        assert_eq!(metrics.high_water_entries, 1);
    }

    #[test]
    fn provider_pool_reuses_vended_epoch_without_secret_keys_but_rotates_on_epoch() {
        let pool = ObjectStoreProviderPool::new_for_test(4, Duration::from_secs(1)).unwrap();
        let endpoint = endpoint_config("http://localhost:9000");
        pool.acquire_with_builder(
            domain(1),
            "warehouse",
            &endpoint,
            &vended_identity(7),
            &secret_material("first"),
            |_, _| memory_operator(),
        )
        .unwrap();
        pool.acquire_with_builder(
            domain(1),
            "warehouse",
            &endpoint,
            &vended_identity(7),
            &secret_material("different-value-same-lease"),
            |_, _| -> FileResult<Operator> {
                panic!("vended secret material must not participate in the provider key")
            },
        )
        .unwrap();
        pool.acquire_with_builder(
            domain(1),
            "warehouse",
            &endpoint,
            &vended_identity(8),
            &secret_material("rotated-epoch"),
            |_, _| memory_operator(),
        )
        .unwrap();

        let metrics = pool.metrics_snapshot().unwrap();
        assert_eq!(metrics.operator_constructions, 2);
        assert_eq!(metrics.cache_hits, 1);
        assert_eq!(metrics.resident_entries, 2);
    }

    #[test]
    fn provider_pool_evicts_vended_provider_at_credential_expiration() {
        let pool = ObjectStoreProviderPool::new_for_test(2, Duration::from_secs(1)).unwrap();
        pool.acquire_with_expiration(
            domain(1),
            "warehouse",
            &endpoint_config("http://localhost:9000"),
            &vended_identity(7),
            &secret_material("ephemeral"),
            Some(Instant::now() + Duration::from_millis(30)),
            |_, _| memory_operator(),
        )
        .unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let metrics = pool.metrics_snapshot().unwrap();
            if metrics.resident_entries == 0 {
                assert_eq!(metrics.credential_expirations, 1);
                break;
            }
            assert!(
                Instant::now() < deadline,
                "janitor did not evict expired vended provider"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn provider_pool_capacity_evicts_the_oldest_access_domain() {
        let pool = ObjectStoreProviderPool::new(ObjectStoreProviderPoolOptions {
            capacity: 1,
            idle_ttl: Duration::from_secs(60),
        })
        .unwrap();
        let endpoint = endpoint_config("http://localhost:9000");
        let identity = credential_identity("blue");
        let secrets = secret_material("one");
        pool.acquire_with_builder(
            domain(1),
            "warehouse",
            &endpoint,
            &identity,
            &secrets,
            |_, _| memory_operator(),
        )
        .unwrap();
        pool.acquire_with_builder(
            domain(2),
            "warehouse",
            &endpoint,
            &identity,
            &secrets,
            |_, _| memory_operator(),
        )
        .unwrap();

        let metrics = pool.metrics_snapshot().unwrap();
        assert_eq!(metrics.operator_constructions, 2);
        assert_eq!(metrics.resident_entries, 1);
        assert_eq!(metrics.high_water_entries, 1);
        assert_eq!(metrics.capacity_evictions, 1);
    }

    #[test]
    fn provider_pool_janitor_actively_expires_idle_entries() {
        let pool = ObjectStoreProviderPool::new_for_test(2, Duration::from_millis(30)).unwrap();
        pool.acquire_with_builder(
            domain(1),
            "warehouse",
            &endpoint_config("http://localhost:9000"),
            &credential_identity("blue"),
            &secret_material("one"),
            |_, _| memory_operator(),
        )
        .unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let metrics = pool.metrics_snapshot().unwrap();
            if metrics.resident_entries == 0 {
                assert_eq!(metrics.idle_expirations, 1);
                break;
            }
            assert!(
                Instant::now() < deadline,
                "janitor did not evict idle entry"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn concurrent_same_key_construction_is_single_flight() {
        let pool =
            Arc::new(ObjectStoreProviderPool::new_for_test(2, Duration::from_secs(1)).unwrap());
        let barrier = Arc::new(Barrier::new(3));
        let constructions = Arc::new(AtomicUsize::new(0));
        let mut workers = Vec::new();
        for _ in 0..2 {
            let pool = Arc::clone(&pool);
            let barrier = Arc::clone(&barrier);
            let constructions = Arc::clone(&constructions);
            workers.push(std::thread::spawn(move || {
                let endpoint = endpoint_config("http://localhost:9000");
                let identity = credential_identity("blue");
                let secrets = secret_material("one");
                barrier.wait();
                pool.acquire_with_builder(
                    domain(1),
                    "warehouse",
                    &endpoint,
                    &identity,
                    &secrets,
                    |_, _| {
                        constructions.fetch_add(1, Ordering::SeqCst);
                        std::thread::sleep(Duration::from_millis(50));
                        memory_operator()
                    },
                )
                .unwrap();
            }));
        }
        barrier.wait();
        for worker in workers {
            worker.join().unwrap();
        }

        assert_eq!(constructions.load(Ordering::SeqCst), 1);
        let metrics = pool.metrics_snapshot().unwrap();
        assert_eq!(metrics.operator_constructions, 1);
        assert_eq!(metrics.singleflight_waits, 1);
        assert_eq!(metrics.resident_entries, 1);
    }

    #[test]
    fn failed_construction_does_not_evict_a_healthy_entry() {
        let pool = ObjectStoreProviderPool::new_for_test(1, Duration::from_secs(1)).unwrap();
        let endpoint = endpoint_config("http://localhost:9000");
        let identity = credential_identity("blue");
        let secrets = secret_material("one");
        pool.acquire_with_builder(
            domain(1),
            "warehouse",
            &endpoint,
            &identity,
            &secrets,
            |_, _| memory_operator(),
        )
        .unwrap();
        let error = pool
            .acquire_with_builder(
                domain(2),
                "warehouse",
                &endpoint,
                &identity,
                &secrets,
                |_, _| {
                    Err(FileError::new(
                        FileErrorKind::Invalid,
                        "injected construction failure",
                    ))
                },
            )
            .unwrap_err();
        assert_eq!(error.kind(), FileErrorKind::Invalid);
        pool.acquire_with_builder(
            domain(1),
            "warehouse",
            &endpoint,
            &identity,
            &secrets,
            |_, _| -> FileResult<Operator> { panic!("healthy entry must remain resident") },
        )
        .unwrap();

        let metrics = pool.metrics_snapshot().unwrap();
        assert_eq!(metrics.operator_constructions, 1);
        assert_eq!(metrics.operator_construction_failures, 1);
        assert_eq!(metrics.capacity_evictions, 0);
        assert_eq!(metrics.resident_entries, 1);
    }

    #[test]
    fn endpoint_identity_is_canonical_and_rejects_secret_bearing_url_parts() {
        assert_eq!(
            normalize_s3_endpoint("HTTPS://EXAMPLE.COM:443/a/../api//").unwrap(),
            "https://example.com/api"
        );
        assert_eq!(
            normalize_s3_endpoint("localhost:9000/").unwrap(),
            "http://localhost:9000"
        );

        let canary = "endpoint-secret-canary";
        for raw in [
            format!("https://user:{canary}@example.com"),
            format!("https://example.com?token={canary}"),
            format!("https://example.com#{canary}"),
        ] {
            let config = endpoint_config(&raw);
            let error = normalize_s3_endpoint(&raw).unwrap_err();
            let diagnostic = format!("{config:?} {error:?} {error}");
            assert!(!diagnostic.contains(canary));
            assert!(diagnostic.contains("<invalid-endpoint>"));
        }

        let legacy = legacy_config(
            "https://user:endpoint-secret-canary@example.com",
            "material-canary",
        );
        let debug = format!("{legacy:?}");
        assert!(!debug.contains("endpoint-secret-canary"));
        assert!(!debug.contains("material-canary"));
    }

    #[test]
    fn canonical_equivalent_endpoints_share_one_provider_key() {
        let pool = ObjectStoreProviderPool::new_for_test(2, Duration::from_secs(1)).unwrap();
        let identity = credential_identity("blue");
        let secrets = secret_material("one");
        pool.acquire_with_builder(
            domain(1),
            "warehouse",
            &endpoint_config("HTTPS://EXAMPLE.COM:443/a/../api//"),
            &identity,
            &secrets,
            |_, _| memory_operator(),
        )
        .unwrap();
        pool.acquire_with_builder(
            domain(1),
            "warehouse",
            &endpoint_config("https://example.com/api"),
            &identity,
            &secrets,
            |_, _| -> FileResult<Operator> {
                panic!("canonical equivalent endpoint must reuse the resident provider")
            },
        )
        .unwrap();

        let metrics = pool.metrics_snapshot().unwrap();
        assert_eq!(metrics.operator_constructions, 1);
        assert_eq!(metrics.cache_hits, 1);
        assert_eq!(metrics.resident_entries, 1);
    }
}
