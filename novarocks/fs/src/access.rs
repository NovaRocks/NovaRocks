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
use novarocks_spi::connector::{StaticCredentialReference, StorageAccessDomainId};
use opendal::layers::{ConcurrentLimitLayer, HttpClientLayer, RetryLayer, TimeoutLayer};
use opendal::raw::{
    Access, Layer, LayeredAccess, OpDelete, OpList, OpRead, OpStat, OpWrite, RpList, RpRead,
    RpStat, RpWrite, oio,
};
use opendal::{Buffer, Metadata, Operator};
use url::{Host, Url};

use crate::storage_authority::{StorageAuthority, StorageAuthorityId};
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
        let read = async {
            let buffer = match range {
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
            }
            .map_err(|error| map_opendal_error("read file", error))?;
            Ok::<Bytes, FileError>(buffer.to_bytes())
        };
        let result =
            match futures::future::select(Box::pin(read), Box::pin(cancellation.ended())).await {
                futures::future::Either::Left((result, _)) => result,
                futures::future::Either::Right((error, _)) => {
                    return Err(error);
                }
            };
        cancellation.check()?;
        result
    }

    /// Fill an already allocated target from one exact authorized range.
    /// Stream chunks never grow the target. Callers discard it on error.
    pub async fn read_into(
        &self,
        range: FileReadRange,
        target: &mut [u8],
        cancellation: &FileCancellation,
    ) -> FileResult<()> {
        cancellation.check()?;
        let (start, length) = match range {
            FileReadRange::WholeFile => (0, self.identity.file_size()),
            FileReadRange::Bounded { offset, length } => (offset, length),
        };
        let end = start
            .checked_add(length)
            .ok_or_else(|| FileError::invalid("file read range overflows"))?;
        if end > self.identity.file_size() {
            return Err(FileError::new(
                FileErrorKind::Corrupt,
                "file read range exceeds bound file length",
            ));
        }
        let target_len = u64::try_from(target.len())
            .map_err(|_| FileError::invalid("file read target length overflows"))?;
        if length != target_len {
            return Err(FileError::invalid(
                "file read target length differs from the requested range",
            ));
        }
        if length == 0 {
            return Ok(());
        }

        let reader = tokio::select! {
            result = self.access.operator.reader_with(self.operator_relative_path()) => {
                result.map_err(|error| map_opendal_error("open file reader", error))?
            }
            error = cancellation.ended() => return Err(error),
        };
        let mut stream = tokio::select! {
            result = reader.into_bytes_stream(start..end) => {
                result.map_err(|error| map_opendal_error("stream file range", error))?
            }
            error = cancellation.ended() => return Err(error),
        };
        let mut filled = 0usize;
        loop {
            let chunk = tokio::select! {
                result = stream.next() => result,
                error = cancellation.ended() => return Err(error),
            };
            let Some(chunk) = chunk else { break };
            let chunk = chunk.map_err(|error| map_stream_error("read file range", error))?;
            let next = filled.checked_add(chunk.len()).ok_or_else(|| {
                FileError::new(FileErrorKind::Corrupt, "file read length overflow")
            })?;
            if next > target.len() {
                return Err(FileError::new(
                    FileErrorKind::Corrupt,
                    "file range stream exceeded the fixed target length",
                ));
            }
            target[filled..next].copy_from_slice(&chunk);
            filled = next;
        }
        cancellation.check()?;
        if filled != target.len() {
            return Err(FileError::new(
                FileErrorKind::Corrupt,
                format!(
                    "file range stream ended early: expected {} bytes, received {filled}",
                    target.len()
                ),
            ));
        }
        Ok(())
    }

    pub async fn stat(&self, cancellation: &FileCancellation) -> FileResult<u64> {
        cancellation.check()?;
        let metadata = match futures::future::select(
            Box::pin(self.access.operator.stat(self.operator_relative_path())),
            Box::pin(cancellation.ended()),
        )
        .await
        {
            futures::future::Either::Left((result, _)) => {
                result.map_err(|error| map_opendal_error("stat file", error))?
            }
            futures::future::Either::Right((error, _)) => {
                return Err(error);
            }
        };
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
}

/// Explicit, bounded owner for credential-bound object-store operators.
///
/// Keys contain only canonical non-secret endpoint identity, access domain,
/// and credential provider identity. Secret values are never retained as map
/// keys, and construction happens outside the pool lock behind a per-key
/// single-flight reservation.
///
/// An entry leaves the pool for exactly two reasons: it went unused for the
/// configured idle TTL, or capacity forced the oldest one out. Credential
/// expiry is deliberately not one of them (CAD-1 D0): an authority-backed
/// operator re-reads its material from [`StorageAuthority`] for every request
/// it signs, so it outlives both the rotation and the expiry of any single
/// credential instead of being rebuilt behind them.
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

    fn acquire<F>(
        &self,
        access_domain: StorageAccessDomainId,
        bucket: &str,
        endpoint_config: &ObjectStoreEndpointConfig,
        credential_identity: &ObjectStoreCredentialProviderIdentity,
        credentials: &ObjectStoreCredentialSource,
        builder: F,
    ) -> FileResult<Operator>
    where
        F: FnOnce(
            &ObjectStoreEndpointIdentity,
            &ObjectStoreCredentialSource,
        ) -> FileResult<Operator>,
    {
        let (endpoint, key) =
            provider_key(access_domain, bucket, endpoint_config, credential_identity)?;
        match self.claim(key)? {
            ProviderAcquisition::Ready(operator) => Ok(operator),
            ProviderAcquisition::Build(claim) => claim.build(&endpoint, credentials, builder),
            ProviderAcquisition::Wait(reservation) => {
                reservation.wait(OBJECT_STORE_PROVIDER_BUILD_WAIT_TIMEOUT)
            }
        }
    }

    /// Like [`Self::acquire`], for a caller that must not block its thread.
    ///
    /// A caller that finds another caller constructing the same operator
    /// awaits that construction under its own `cancellation` and deadline;
    /// giving up ends only its own wait. The construction itself never yields:
    /// the claimer builds the client inline, so dropping the claimer's future
    /// can never leave a construction without an owner.
    async fn acquire_async<F>(
        &self,
        endpoint: ObjectStoreEndpointIdentity,
        key: ObjectStoreProviderKey,
        credentials: &ObjectStoreCredentialSource,
        cancellation: &FileCancellation,
        builder: F,
    ) -> FileResult<Operator>
    where
        F: FnOnce(
            &ObjectStoreEndpointIdentity,
            &ObjectStoreCredentialSource,
        ) -> FileResult<Operator>,
    {
        cancellation.check()?;
        match self.claim(key)? {
            ProviderAcquisition::Ready(operator) => Ok(operator),
            ProviderAcquisition::Build(claim) => claim.build(&endpoint, credentials, builder),
            ProviderAcquisition::Wait(reservation) => reservation.wait_async(cancellation).await,
        }
    }

    /// Serves `key` from the pool, joins the construction already in flight
    /// for it, or claims its construction.
    fn claim(&self, key: ObjectStoreProviderKey) -> FileResult<ProviderAcquisition> {
        let now = Instant::now();
        let mut inner = self.state.lock_inner()?;
        inner.expire_due(now);
        if let Some(operator) = inner.touch(&key, now, self.state.options.idle_ttl) {
            inner.cache_hits = inner.cache_hits.saturating_add(1);
            self.state.wake.notify_one();
            return Ok(ProviderAcquisition::Ready(operator));
        }
        inner.cache_misses = inner.cache_misses.saturating_add(1);
        if let Some(reservation) = inner.inflight.get(&key).cloned() {
            inner.singleflight_waits = inner.singleflight_waits.saturating_add(1);
            return Ok(ProviderAcquisition::Wait(reservation));
        }
        if inner.inflight.len() >= self.state.options.capacity {
            return Err(FileError::new(
                FileErrorKind::ResourceExhausted,
                "object-store provider construction reservations are full",
            ));
        }
        let reservation = Arc::new(ObjectStoreBuildReservation::default());
        inner.inflight.insert(key.clone(), Arc::clone(&reservation));
        drop(inner);
        Ok(ProviderAcquisition::Build(Box::new(BuildClaim {
            state: Arc::clone(&self.state),
            key,
            reservation,
            settled: false,
        })))
    }
}

/// The one caller that constructs the operator for a key.
///
/// However the construction ends -- published, failed, panicked, or abandoned
/// by an early return -- the claim takes the key out of the in-flight set and
/// completes its reservation, so no key stays in construction and every
/// waiter wakes with an outcome. Settling tolerates a poisoned pool lock for
/// the same reason.
struct BuildClaim {
    state: Arc<ObjectStoreProviderPoolState>,
    key: ObjectStoreProviderKey,
    reservation: Arc<ObjectStoreBuildReservation>,
    settled: bool,
}

impl BuildClaim {
    fn build<F>(
        self,
        endpoint: &ObjectStoreEndpointIdentity,
        credentials: &ObjectStoreCredentialSource,
        builder: F,
    ) -> FileResult<Operator>
    where
        F: FnOnce(
            &ObjectStoreEndpointIdentity,
            &ObjectStoreCredentialSource,
        ) -> FileResult<Operator>,
    {
        let built = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            builder(endpoint, credentials)
        }))
        .unwrap_or_else(|_| {
            Err(FileError::new(
                FileErrorKind::Internal,
                "object-store provider construction panicked",
            ))
        });
        self.settle(built)
    }

    fn settle(mut self, built: FileResult<Operator>) -> FileResult<Operator> {
        self.settled = true;
        let result = {
            let mut inner = self.state.lock_inner_for_settlement();
            inner.remove_inflight(&self.key, &self.reservation);
            match built {
                Ok(operator) => {
                    if inner.entries.len() >= self.state.options.capacity {
                        inner.evict_oldest();
                    }
                    inner.operator_constructions = inner.operator_constructions.saturating_add(1);
                    inner.insert(
                        self.key.clone(),
                        operator.clone(),
                        Instant::now(),
                        self.state.options.idle_ttl,
                    );
                    Ok(operator)
                }
                Err(error) => {
                    inner.operator_construction_failures =
                        inner.operator_construction_failures.saturating_add(1);
                    Err(error)
                }
            }
        };
        self.reservation.complete(&result);
        self.state.wake.notify_all();
        result
    }
}

impl Drop for BuildClaim {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        {
            let mut inner = self.state.lock_inner_for_settlement();
            inner.remove_inflight(&self.key, &self.reservation);
            inner.operator_construction_failures =
                inner.operator_construction_failures.saturating_add(1);
        }
        self.reservation.complete(&Err(FileError::new(
            FileErrorKind::Internal,
            "object-store provider construction was abandoned before it finished",
        )));
        self.state.wake.notify_all();
    }
}

/// The endpoint an operator is built for and the pool key that names it.
fn provider_key(
    access_domain: StorageAccessDomainId,
    bucket: &str,
    endpoint_config: &ObjectStoreEndpointConfig,
    credential_identity: &ObjectStoreCredentialProviderIdentity,
) -> FileResult<(ObjectStoreEndpointIdentity, ObjectStoreProviderKey)> {
    let endpoint = ObjectStoreEndpointIdentity::try_new(bucket, endpoint_config)?;
    let key = ObjectStoreProviderKey {
        endpoint: endpoint.clone(),
        access_domain,
        credential_identity: credential_identity.clone(),
    };
    Ok((endpoint, key))
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

    /// The pool lock for settling a construction, which must happen even
    /// after an unrelated panic poisoned it: a construction that cannot
    /// settle would leave its key in construction forever.
    fn lock_inner_for_settlement(&self) -> std::sync::MutexGuard<'_, ObjectStoreProviderPoolInner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
    shutting_down: bool,
}

impl ObjectStoreProviderPoolInner {
    fn touch(
        &mut self,
        key: &ObjectStoreProviderKey,
        now: Instant,
        idle_ttl: Duration,
    ) -> Option<Operator> {
        let entry = self.entries.remove(key)?;
        self.expiration_index.remove(&entry.expiration_key);
        let operator = entry.operator.clone();
        self.insert(key.clone(), entry.operator, now, idle_ttl);
        Some(operator)
    }

    fn insert(
        &mut self,
        key: ObjectStoreProviderKey,
        operator: Operator,
        now: Instant,
        idle_ttl: Duration,
    ) {
        self.next_sequence = self.next_sequence.wrapping_add(1);
        let expires_at = now.checked_add(idle_ttl).unwrap_or(now);
        let expiration_key = (expires_at, self.next_sequence);
        self.expiration_index.insert(expiration_key, key.clone());
        self.entries.insert(
            key,
            ObjectStoreProviderEntry {
                operator,
                expiration_key,
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
            if self.entries.remove(&key).is_some() {
                self.idle_expirations = self.idle_expirations.saturating_add(1);
            }
        }
    }

    /// Takes `key` out of construction, if `reservation` is still the one in
    /// flight for it.
    fn remove_inflight(
        &mut self,
        key: &ObjectStoreProviderKey,
        reservation: &Arc<ObjectStoreBuildReservation>,
    ) {
        if self
            .inflight
            .get(key)
            .is_some_and(|inflight| Arc::ptr_eq(inflight, reservation))
        {
            self.inflight.remove(key);
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
        }
    }
}

enum ProviderAcquisition {
    Ready(Operator),
    Build(Box<BuildClaim>),
    Wait(Arc<ObjectStoreBuildReservation>),
}

/// One construction in flight, shared by the caller that builds it and every
/// caller that waits for it. Waiters either block, bounded by a fixed timeout
/// on the coordinator's synchronous path, or await it under their own stop.
#[derive(Default)]
struct ObjectStoreBuildReservation {
    state: Mutex<ObjectStoreBuildReservationState>,
    ready: Condvar,
    completed: tokio::sync::Notify,
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
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (state, wait) = self
            .ready
            .wait_timeout_while(state, timeout, |state| {
                matches!(state, ObjectStoreBuildReservationState::Building)
            })
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match Self::outcome(&state) {
            Some(outcome) => outcome,
            None if wait.timed_out() => Err(FileError::deadline(
                "timed out waiting for object-store provider construction",
            )),
            None => Err(FileError::new(
                FileErrorKind::Internal,
                "object-store provider construction waiter woke without completion",
            )),
        }
    }

    /// Awaits the construction under the waiter's own cancellation and
    /// deadline. Registered before every check, so a completion between the
    /// check and the wait is never missed.
    async fn wait_async(&self, cancellation: &FileCancellation) -> FileResult<Operator> {
        loop {
            let completed = self.completed.notified();
            tokio::pin!(completed);
            completed.as_mut().enable();
            let outcome = Self::outcome(
                &self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            );
            if let Some(outcome) = outcome {
                return outcome;
            }
            tokio::select! {
                _ = &mut completed => {}
                error = cancellation.ended() => return Err(error),
            }
        }
    }

    fn outcome(state: &ObjectStoreBuildReservationState) -> Option<FileResult<Operator>> {
        match state {
            ObjectStoreBuildReservationState::Building => None,
            ObjectStoreBuildReservationState::Complete(Ok(operator)) => Some(Ok(operator.clone())),
            ObjectStoreBuildReservationState::Complete(Err(error)) => Some(Err(FileError::new(
                error.kind,
                "object-store provider construction failed",
            ))),
        }
    }

    fn complete(&self, result: &FileResult<Operator>) {
        *self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            ObjectStoreBuildReservationState::Complete(match result {
                Ok(operator) => Ok(operator.clone()),
                Err(error) => Err(ObjectStoreBuildFailure { kind: error.kind() }),
            });
        self.ready.notify_all();
        self.completed.notify_waiters();
    }
}

/// Non-secret identity of the credential provider bound to one Operator.
///
/// Every variant is query-independent. Nothing derived from a query attempt —
/// no lease, no epoch, no `QueryExecutionId` — may enter this type: it is the
/// credential dimension of the operator pool key, and a per-attempt dimension
/// would rebuild every storage client on every query (CAD-1 D0).
///
/// Secret material remains outside this type and outside all pool keys.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ObjectStoreCredentialProviderIdentity {
    /// Startup-composed material named by an exact static reference.
    Static(StaticCredentialReference),
    /// Material owned by a consumer-side [`StorageAuthority`]. Mint this only
    /// through [`ObjectStoreAccessContext::for_authority`], which takes the
    /// identity from the authority that will actually sign the requests.
    Vended(StorageAuthorityId),
}

/// What one object-store operator signs its requests with.
///
/// The two arms differ in *when* the material is read, not only in where it
/// came from. Static material is welded into the operator at construction and
/// is valid for as long as the binding that composed it. Authority-backed
/// material is re-read at every signing boundary, which is what lets one
/// operator outlive the rotation and the expiry of any single credential.
#[derive(Clone)]
enum ObjectStoreCredentialSource {
    Static(ObjectStoreSecretMaterial),
    Authority(Arc<StorageAuthority>),
}

impl Debug for ObjectStoreCredentialSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Static(material) => f.debug_tuple("Static").field(material).finish(),
            Self::Authority(authority) => f.debug_tuple("Authority").field(authority).finish(),
        }
    }
}

pub struct ObjectStoreAccessContext<'a> {
    endpoint_config: ObjectStoreEndpointConfig,
    credential_identity: ObjectStoreCredentialProviderIdentity,
    credentials: ObjectStoreCredentialSource,
    provider_pool: &'a ObjectStoreProviderPool,
}

impl<'a> ObjectStoreAccessContext<'a> {
    /// Bind startup-composed static material to its exact reference.
    pub fn new(
        endpoint_config: ObjectStoreEndpointConfig,
        credential_identity: ObjectStoreCredentialProviderIdentity,
        secret_material: ObjectStoreSecretMaterial,
        provider_pool: &'a ObjectStoreProviderPool,
    ) -> Self {
        Self {
            endpoint_config,
            credential_identity,
            credentials: ObjectStoreCredentialSource::Static(secret_material),
            provider_pool,
        }
    }

    /// Bind one consumer-side storage authority.
    ///
    /// The pool key comes from the authority itself, so the client that signs a
    /// request and the identity that names it in the pool can never disagree.
    pub fn for_authority(
        endpoint_config: ObjectStoreEndpointConfig,
        authority: Arc<StorageAuthority>,
        provider_pool: &'a ObjectStoreProviderPool,
    ) -> Self {
        Self {
            endpoint_config,
            credential_identity: ObjectStoreCredentialProviderIdentity::Vended(
                authority.id().clone(),
            ),
            credentials: ObjectStoreCredentialSource::Authority(authority),
            provider_pool,
        }
    }
}

impl Debug for ObjectStoreAccessContext<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreAccessContext")
            .field("endpoint_config", &self.endpoint_config)
            .field("credential_identity", &self.credential_identity)
            .field("credentials", &self.credentials)
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
        let (scheme, locations) = self.parse_one_scheme(locations)?;
        match scheme {
            FsScheme::Local => resolve_local_locations(access_domain, locations),
            FsScheme::ObjectStore => {
                resolve_object_store_locations(access_domain, locations, object_store_access)
            }
            FsScheme::Hdfs => resolve_hdfs_locations(access_domain, locations),
        }
    }

    /// Resolves like [`Self::resolve_location`] for a caller that must not
    /// block its thread; see [`Self::resolve_locations_async`].
    pub async fn resolve_location_async(
        &self,
        access_domain: StorageAccessDomainId,
        location: impl AsRef<str>,
        object_store_access: Option<ObjectStoreAccessContext<'_>>,
        cancellation: &FileCancellation,
    ) -> FileResult<FsAccessHandle> {
        self.resolve_locations_async(
            access_domain,
            std::iter::once(location),
            object_store_access,
            cancellation,
        )
        .await
    }

    /// Resolves like [`Self::resolve_locations`] for a caller that must not
    /// block its thread. Resolution only waits when another caller is
    /// constructing the same object-store client; this caller then awaits that
    /// construction under `cancellation` and its deadline instead of parking.
    pub async fn resolve_locations_async<I, S>(
        &self,
        access_domain: StorageAccessDomainId,
        locations: I,
        object_store_access: Option<ObjectStoreAccessContext<'_>>,
        cancellation: &FileCancellation,
    ) -> FileResult<FsAccessHandle>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let (scheme, locations) = self.parse_one_scheme(locations)?;
        match scheme {
            FsScheme::Local => resolve_local_locations(access_domain, locations),
            FsScheme::ObjectStore => {
                let (access, bucket) = object_store_target(&locations, object_store_access)?;
                let (endpoint, key) = provider_key(
                    access_domain,
                    &bucket,
                    &access.endpoint_config,
                    &access.credential_identity,
                )?;
                let operator = access
                    .provider_pool
                    .acquire_async(
                        endpoint,
                        key,
                        &access.credentials,
                        cancellation,
                        build_object_store_operator,
                    )
                    .await?;
                object_store_handle(access_domain, locations, bucket, operator)
            }
            FsScheme::Hdfs => resolve_hdfs_locations(access_domain, locations),
        }
    }

    fn parse_one_scheme<I, S>(&self, locations: I) -> FileResult<(FsScheme, Vec<FsLocation>)>
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
        Ok((scheme, locations))
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
    let (object_store_access, bucket) = object_store_target(&locations, object_store_access)?;
    let operator = object_store_access.provider_pool.acquire(
        access_domain,
        &bucket,
        &object_store_access.endpoint_config,
        &object_store_access.credential_identity,
        &object_store_access.credentials,
        build_object_store_operator,
    )?;
    object_store_handle(access_domain, locations, bucket, operator)
}

/// The access context and the one bucket every object-store location names.
fn object_store_target<'a>(
    locations: &[FsLocation],
    object_store_access: Option<ObjectStoreAccessContext<'a>>,
) -> FileResult<(ObjectStoreAccessContext<'a>, String)> {
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
    Ok((object_store_access, bucket))
}

fn object_store_handle(
    access_domain: StorageAccessDomainId,
    locations: Vec<FsLocation>,
    bucket: String,
    operator: Operator,
) -> FileResult<FsAccessHandle> {
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
    credentials: &ObjectStoreCredentialSource,
) -> FileResult<Operator> {
    let mut builder = opendal::services::S3::default()
        .endpoint(&endpoint.endpoint)
        .bucket(&endpoint.bucket)
        .region(&endpoint.region);
    if !endpoint.use_path_style {
        builder = builder.enable_virtual_host_style();
    }
    let denial = match credentials {
        ObjectStoreCredentialSource::Static(secrets) => {
            builder = builder
                .access_key_id(secrets.access_key_id.expose_secret())
                .secret_access_key(secrets.access_key_secret.expose_secret());
            if let Some(session_token) = secrets.session_token.as_ref() {
                builder = builder.session_token(session_token.expose_secret());
            }
            None
        }
        ObjectStoreCredentialSource::Authority(authority) => {
            // The material is never welded into the builder: the loader reads
            // it back from the authority for every request that is about to be
            // signed, which is what keeps this operator usable across a
            // credential rotation (CAD-1 D0).
            let denial = Arc::new(CredentialDenialLatch::default());
            builder = builder.customized_credential_load(Box::new(AuthorityCredentialLoad {
                authority: Arc::clone(authority),
                denial: Arc::clone(&denial),
                budget: Duration::from_millis(endpoint.timeout_ms),
            }));
            Some(denial)
        }
    };
    let mut operator = Operator::new(builder)
        .map_err(|error| map_opendal_error("initialize object store operator", error))?
        .finish();
    if let Some(denial) = denial {
        // Installed first, so it sits *under* the RetryLayer added below and
        // gets to restore a confirmed denial's meaning before the retry
        // predicate reads it.
        operator = operator.layer(CredentialDenialLayer { denial });
    }
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

/// Serves one operator's signing material out of its storage authority.
///
/// opendal calls this at every authentication boundary, so the path that
/// matters is the cache hit: [`StorageAuthority::material_for_request`] takes a
/// lock, clones the material in hand, and returns without touching the network.
/// A remote acquisition happens only when the authority itself decides one is
/// due, and it runs on the authority's own executor, never on this thread.
struct AuthorityCredentialLoad {
    authority: Arc<StorageAuthority>,
    denial: Arc<CredentialDenialLatch>,
    /// How long one credential load may take. This is the operator's own
    /// request timeout, not a second retry budget: CAD-1 D5 keeps the
    /// operation deadline and the acquisition budget separate, and this seam
    /// has no narrower deadline to offer.
    budget: Duration,
}

#[async_trait::async_trait]
impl reqsign::AwsCredentialLoad for AuthorityCredentialLoad {
    async fn load_credential(
        &self,
        _client: reqwest::Client,
    ) -> anyhow::Result<Option<reqsign::AwsCredential>> {
        let now = Instant::now();
        let deadline = now.checked_add(self.budget).unwrap_or(now);
        match self.authority.material_for_request(now, deadline).await {
            Ok(material) => Ok(Some(reqsign::AwsCredential {
                access_key_id: material.access_key_id().expose_secret().to_string(),
                secret_access_key: material.secret_access_key().expose_secret().to_string(),
                session_token: material
                    .session_token()
                    .map(|token| token.expose_secret().to_string()),
                // The authority is the only judge of validity. reqsign would
                // otherwise apply its own two-minute buffer to a wall-clock
                // expiry, which is a second, disagreeing opinion about the
                // same material.
                expires_in: None,
            })),
            Err(error) => {
                if error.kind() == FileErrorKind::Permission {
                    self.denial.record(&error);
                }
                Err(anyhow::Error::new(error))
            }
        }
    }
}

/// One operator's record that its storage authority answered with a denial.
///
/// opendal turns *every* credential-load failure into a temporary error
/// (`opendal-0.55.0/src/services/s3/core.rs`: a loader that succeeded before
/// and fails now becomes `PermissionDenied` + `set_temporary()`, and an error
/// returned by the loader becomes `Unexpected` + `set_temporary()`), and its
/// `RetryLayer` retries whatever is temporary with a fixed, non-overridable
/// predicate. That collapses the third state of CAD-1 D4: a confirmed denial
/// would be retried as if it were jitter and would then surface as transient.
///
/// The loader latches a denial here and [`CredentialDenialLayer`], installed
/// under the RetryLayer, restores its meaning before the retry predicate can
/// read it. The layer also recognizes a transient authority failure in the
/// source chain and ends only that object operation's retry cycle. A later
/// operation can still ask the open authority for material.
#[derive(Debug, Default)]
struct CredentialDenialLatch {
    denial: Mutex<Option<String>>,
}

impl CredentialDenialLatch {
    fn record(&self, error: &FileError) {
        let mut denial = self
            .denial
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if denial.is_none() {
            *denial = Some(error.to_string());
        }
    }

    fn observed(&self) -> Option<String> {
        self.denial
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Keep an authority credential-load failure inside one object operation.
    ///
    /// Confirmed denial closes the authority. A transient acquisition failure
    /// leaves the authority open for a later operation, but this operation has
    /// already consumed its bounded acquisition effort and must not restart it
    /// through OpenDAL's outer retry layer. Other temporary S3 failures retain
    /// OpenDAL's retry behavior.
    fn classify(&self, error: opendal::Error) -> opendal::Error {
        if !error.is_temporary() {
            return error;
        }
        // OpenDAL marks a failed credential loader temporary even when the
        // authority has already spent its bounded acquisition effort. An
        // object-store retry must not open another effort for this same read.
        // The next independent operation can still ask the authority again.
        let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&error);
        while let Some(current) = cause {
            if let Some(failure) = current.downcast_ref::<FileError>() {
                let kind = match failure.kind() {
                    FileErrorKind::Permission => opendal::ErrorKind::PermissionDenied,
                    FileErrorKind::Transient | FileErrorKind::DeadlineExceeded => {
                        opendal::ErrorKind::Unexpected
                    }
                    _ => break,
                };
                return opendal::Error::new(kind, failure.to_string())
                    .set_permanent()
                    .set_source(error);
            }
            cause = current.source();
        }
        match self.observed() {
            Some(detail) => opendal::Error::new(opendal::ErrorKind::PermissionDenied, detail)
                .set_permanent()
                .set_source(error),
            None => error,
        }
    }
}

/// Restores a confirmed credential denial before opendal's retry predicate
/// sees it. Must be installed under the `RetryLayer`.
#[derive(Clone)]
struct CredentialDenialLayer {
    denial: Arc<CredentialDenialLatch>,
}

impl<A: Access> Layer<A> for CredentialDenialLayer {
    type LayeredAccess = CredentialDenialAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        CredentialDenialAccessor {
            inner,
            denial: Arc::clone(&self.denial),
        }
    }
}

#[derive(Debug)]
struct CredentialDenialAccessor<A: Access> {
    inner: A,
    denial: Arc<CredentialDenialLatch>,
}

impl<A: Access> CredentialDenialAccessor<A> {
    fn wrap<T>(&self, inner: T) -> CredentialDenialStream<T> {
        CredentialDenialStream {
            inner,
            denial: Arc::clone(&self.denial),
        }
    }
}

impl<A: Access> LayeredAccess for CredentialDenialAccessor<A> {
    type Inner = A;
    type Reader = CredentialDenialStream<A::Reader>;
    type Writer = CredentialDenialStream<A::Writer>;
    type Lister = CredentialDenialStream<A::Lister>;
    type Deleter = CredentialDenialStream<A::Deleter>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> opendal::Result<(RpRead, Self::Reader)> {
        let outcome = self.inner.read(path, args).await;
        outcome
            .map(|(reply, reader)| (reply, self.wrap(reader)))
            .map_err(|error| self.denial.classify(error))
    }

    async fn write(&self, path: &str, args: OpWrite) -> opendal::Result<(RpWrite, Self::Writer)> {
        let outcome = self.inner.write(path, args).await;
        outcome
            .map(|(reply, writer)| (reply, self.wrap(writer)))
            .map_err(|error| self.denial.classify(error))
    }

    async fn stat(&self, path: &str, args: OpStat) -> opendal::Result<RpStat> {
        let outcome = self.inner.stat(path, args).await;
        outcome.map_err(|error| self.denial.classify(error))
    }

    async fn delete(&self) -> opendal::Result<(opendal::raw::RpDelete, Self::Deleter)> {
        let outcome = self.inner.delete().await;
        outcome
            .map(|(reply, deleter)| (reply, self.wrap(deleter)))
            .map_err(|error| self.denial.classify(error))
    }

    async fn list(&self, path: &str, args: OpList) -> opendal::Result<(RpList, Self::Lister)> {
        let outcome = self.inner.list(path, args).await;
        outcome
            .map(|(reply, lister)| (reply, self.wrap(lister)))
            .map_err(|error| self.denial.classify(error))
    }
}

/// Wraps one streamed opendal handle so the per-request signing failures it
/// raises are classified the same way the accessor's own calls are.
struct CredentialDenialStream<T> {
    inner: T,
    denial: Arc<CredentialDenialLatch>,
}

impl<R: oio::Read> oio::Read for CredentialDenialStream<R> {
    async fn read(&mut self) -> opendal::Result<Buffer> {
        let outcome = self.inner.read().await;
        outcome.map_err(|error| self.denial.classify(error))
    }
}

impl<W: oio::Write> oio::Write for CredentialDenialStream<W> {
    async fn write(&mut self, buffer: Buffer) -> opendal::Result<()> {
        let outcome = self.inner.write(buffer).await;
        outcome.map_err(|error| self.denial.classify(error))
    }

    async fn close(&mut self) -> opendal::Result<Metadata> {
        let outcome = self.inner.close().await;
        outcome.map_err(|error| self.denial.classify(error))
    }

    async fn abort(&mut self) -> opendal::Result<()> {
        let outcome = self.inner.abort().await;
        outcome.map_err(|error| self.denial.classify(error))
    }
}

impl<L: oio::List> oio::List for CredentialDenialStream<L> {
    async fn next(&mut self) -> opendal::Result<Option<oio::Entry>> {
        let outcome = self.inner.next().await;
        outcome.map_err(|error| self.denial.classify(error))
    }
}

impl<D: oio::Delete> oio::Delete for CredentialDenialStream<D> {
    fn delete(&mut self, path: &str, args: OpDelete) -> opendal::Result<()> {
        self.inner
            .delete(path, args)
            .map_err(|error| self.denial.classify(error))
    }

    async fn flush(&mut self) -> opendal::Result<usize> {
        let outcome = self.inner.flush().await;
        outcome.map_err(|error| self.denial.classify(error))
    }
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
    let kind = file_error_kind_for_opendal(error.kind());
    FileError::with_source(kind, operation, error)
}

fn map_stream_error(operation: &str, error: std::io::Error) -> FileError {
    let kind = error
        .get_ref()
        .and_then(|source| source.downcast_ref::<opendal::Error>())
        .map(|source| file_error_kind_for_opendal(source.kind()))
        .unwrap_or_else(|| match error.kind() {
            std::io::ErrorKind::NotFound => FileErrorKind::NotFound,
            std::io::ErrorKind::PermissionDenied => FileErrorKind::Permission,
            _ => FileErrorKind::Transient,
        });
    FileError::with_source(kind, operation, error)
}

fn file_error_kind_for_opendal(kind: opendal::ErrorKind) -> FileErrorKind {
    match kind {
        opendal::ErrorKind::NotFound => FileErrorKind::NotFound,
        opendal::ErrorKind::AlreadyExists => FileErrorKind::AlreadyExists,
        opendal::ErrorKind::PermissionDenied => FileErrorKind::Permission,
        opendal::ErrorKind::Unsupported => FileErrorKind::Unsupported,
        opendal::ErrorKind::RateLimited
        | opendal::ErrorKind::Unexpected
        | opendal::ErrorKind::ConditionNotMatch => FileErrorKind::Transient,
        _ => FileErrorKind::Internal,
    }
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

    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceId, StorageCredentialScopePrefix,
    };
    use reqsign::AwsCredentialLoad;

    use super::*;
    use crate::storage_authority::{
        AcquisitionFailure, AuthorityCapabilityPath, AuthorityMaterial, AuthorityMaterialSource,
        RefreshExecutor, RefreshPolicy,
    };

    fn domain(value: u8) -> StorageAccessDomainId {
        StorageAccessDomainId::from_bytes([value; 32])
    }

    #[test]
    fn streamed_opendal_errors_keep_their_file_classification() {
        let missing = std::io::Error::new(
            std::io::ErrorKind::NotFound,
            opendal::Error::new(opendal::ErrorKind::NotFound, "missing"),
        );
        assert_eq!(
            map_stream_error("read", missing).kind(),
            FileErrorKind::NotFound
        );
        let denied = std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            opendal::Error::new(opendal::ErrorKind::PermissionDenied, "denied"),
        );
        assert_eq!(
            map_stream_error("read", denied).kind(),
            FileErrorKind::Permission
        );
    }

    #[tokio::test]
    async fn read_into_requires_an_exact_fixed_target() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("read-into.bin");
        std::fs::write(&path, b"abcdefgh").expect("write file");
        let location = path.to_string_lossy().into_owned();
        let access = FsAccessResolver::new()
            .resolve_location(domain(33), &location, None)
            .expect("resolve local file");
        let file = access
            .bind_location(&location, FileIdentity::new(&location, 8, None))
            .expect("bind local file");
        let cancellation = FileCancellation::new();
        let mut target = [0u8; 4];
        file.read_into(
            FileReadRange::Bounded {
                offset: 2,
                length: 4,
            },
            &mut target,
            &cancellation,
        )
        .await
        .expect("exact read");
        assert_eq!(&target, b"cdef");

        let mut short_target = [0u8; 3];
        assert_eq!(
            file.read_into(
                FileReadRange::Bounded {
                    offset: 2,
                    length: 4,
                },
                &mut short_target,
                &cancellation,
            )
            .await
            .expect_err("mismatched target")
            .kind(),
            FileErrorKind::Invalid
        );

        std::fs::write(&path, b"abc").expect("shorten source after binding");
        let mut exact_target = [0u8; 8];
        assert!(
            file.read_into(FileReadRange::WholeFile, &mut exact_target, &cancellation)
                .await
                .is_err()
        );
    }

    fn credential_identity(generation: &str) -> ObjectStoreCredentialProviderIdentity {
        ObjectStoreCredentialProviderIdentity::Static(
            StaticCredentialReference::try_new("warehouse-data", generation).unwrap(),
        )
    }

    fn static_source(secret_suffix: &str) -> ObjectStoreCredentialSource {
        ObjectStoreCredentialSource::Static(secret_material(secret_suffix))
    }

    fn authority_id(scope: &str) -> StorageAuthorityId {
        StorageAuthorityId::new(
            CatalogHandle::new(
                ConnectorInstanceId::parse("lake").unwrap(),
                CatalogVersion::from_bytes([0x11; 32]),
            ),
            StorageCredentialScopePrefix::try_from_normalized(scope).unwrap(),
            AuthorityCapabilityPath::SeededWithoutRenewal,
        )
    }

    /// CAD-1 M1 shape: material arrives through `install_material`, so neither
    /// of these is ever reached. They exist because the authority's contract
    /// demands both, not because this milestone can acquire anything.
    struct NeverAcquires;

    impl AuthorityMaterialSource for NeverAcquires {
        fn acquire(&self, _deadline: Instant) -> Result<AuthorityMaterial, AcquisitionFailure> {
            Err(AcquisitionFailure::NoRenewalCapability)
        }
    }

    impl RefreshExecutor for NeverAcquires {
        fn execute(&self, _job: Box<dyn FnOnce() + Send + 'static>) {
            panic!("a seeded authority must never schedule a refresh");
        }
    }

    fn seeded_authority(scope: &str, material: Option<AuthorityMaterial>) -> Arc<StorageAuthority> {
        let authority = Arc::new(StorageAuthority::new(
            authority_id(scope),
            Arc::new(NeverAcquires),
            Arc::new(NeverAcquires),
            RefreshPolicy::default(),
        ));
        if let Some(material) = material {
            authority.install_material(material);
        }
        authority
    }

    fn vended_material(suffix: &str, not_after: Instant) -> AuthorityMaterial {
        AuthorityMaterial::new(
            SecretValue::new(format!("access-{suffix}")),
            SecretValue::new(format!("secret-{suffix}")),
            Some(SecretValue::new(format!("token-{suffix}"))),
            not_after,
        )
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
        pool.acquire(
            domain(1),
            "warehouse",
            &endpoint,
            &identity,
            &static_source("first"),
            |_, _| memory_operator(),
        )
        .unwrap();
        pool.acquire(
            domain(1),
            "warehouse",
            &endpoint,
            &identity,
            &static_source("different-value-same-reference"),
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
    fn provider_pool_reuses_one_authority_identity_across_credential_rotations() {
        let pool = ObjectStoreProviderPool::new_for_test(4, Duration::from_secs(60)).unwrap();
        let endpoint = endpoint_config("http://localhost:9000");
        let orders = ObjectStoreCredentialProviderIdentity::Vended(authority_id(
            "s3://warehouse/sales/orders/",
        ));
        let returns = ObjectStoreCredentialProviderIdentity::Vended(authority_id(
            "s3://warehouse/sales/returns/",
        ));
        let first = seeded_authority(
            "s3://warehouse/sales/orders/",
            Some(vended_material(
                "first",
                Instant::now() + Duration::from_secs(900),
            )),
        );
        // A later resolution of the same authorized scope brings rotated
        // material. CAD-1 D0: that must be a cache hit, not a rebuild.
        let rotated = seeded_authority(
            "s3://warehouse/sales/orders/",
            Some(vended_material(
                "rotated",
                Instant::now() + Duration::from_secs(1800),
            )),
        );
        let other_scope = seeded_authority("s3://warehouse/sales/returns/", None);

        pool.acquire(
            domain(1),
            "warehouse",
            &endpoint,
            &orders,
            &ObjectStoreCredentialSource::Authority(first),
            |_, _| memory_operator(),
        )
        .unwrap();
        pool.acquire(
            domain(1),
            "warehouse",
            &endpoint,
            &orders,
            &ObjectStoreCredentialSource::Authority(rotated),
            |_, _| -> FileResult<Operator> {
                panic!("rotated credential material must not rebuild the operator")
            },
        )
        .unwrap();
        pool.acquire(
            domain(1),
            "warehouse",
            &endpoint,
            &returns,
            &ObjectStoreCredentialSource::Authority(other_scope),
            |_, _| memory_operator(),
        )
        .unwrap();

        let metrics = pool.metrics_snapshot().unwrap();
        assert_eq!(metrics.operator_constructions, 2);
        assert_eq!(metrics.cache_hits, 1);
        assert_eq!(metrics.resident_entries, 2);
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
        let secrets = static_source("one");
        pool.acquire(
            domain(1),
            "warehouse",
            &endpoint,
            &identity,
            &secrets,
            |_, _| memory_operator(),
        )
        .unwrap();
        pool.acquire(
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
        pool.acquire(
            domain(1),
            "warehouse",
            &endpoint_config("http://localhost:9000"),
            &credential_identity("blue"),
            &static_source("one"),
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
                let secrets = static_source("one");
                barrier.wait();
                pool.acquire(
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

    /// Polls `condition` until it holds, failing the test after five seconds.
    async fn eventually(mut condition: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("condition within five seconds");
    }

    fn waiting_key() -> (ObjectStoreEndpointIdentity, ObjectStoreProviderKey) {
        provider_key(
            domain(1),
            "warehouse",
            &endpoint_config("http://localhost:9000"),
            &credential_identity("blue"),
        )
        .expect("provider key")
    }

    fn spawn_async_waiter(
        pool: &Arc<ObjectStoreProviderPool>,
        cancellation: FileCancellation,
    ) -> tokio::task::JoinHandle<FileResult<Operator>> {
        let pool = Arc::clone(pool);
        let (endpoint, key) = waiting_key();
        tokio::spawn(async move {
            pool.acquire_async(
                endpoint,
                key,
                &static_source("one"),
                &cancellation,
                |_, _| -> FileResult<Operator> { panic!("a waiter never constructs") },
            )
            .await
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_async_waiter_gives_up_alone_while_another_receives_the_shared_build() {
        let pool =
            Arc::new(ObjectStoreProviderPool::new_for_test(2, Duration::from_secs(60)).unwrap());
        let (entered, building) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel::<()>();
        let constructions = Arc::new(AtomicUsize::new(0));
        // The claimer constructs on its own thread and holds it open.
        let claimer = {
            let pool = Arc::clone(&pool);
            let constructions = Arc::clone(&constructions);
            std::thread::spawn(move || {
                pool.acquire(
                    domain(1),
                    "warehouse",
                    &endpoint_config("http://localhost:9000"),
                    &credential_identity("blue"),
                    &static_source("one"),
                    |_, _| {
                        constructions.fetch_add(1, Ordering::SeqCst);
                        entered.send(()).expect("entered");
                        released.recv().expect("released");
                        memory_operator()
                    },
                )
            })
        };
        building
            .recv_timeout(Duration::from_secs(5))
            .expect("the claimer is constructing");

        let stopped = FileCancellation::new();
        let gives_up = spawn_async_waiter(&pool, stopped.clone());
        let times_out = spawn_async_waiter(
            &pool,
            FileCancellation::new().with_deadline(Some(Instant::now() + Duration::from_millis(50))),
        );
        let receives = spawn_async_waiter(&pool, FileCancellation::new());
        eventually(|| pool.metrics_snapshot().unwrap().singleflight_waits == 3).await;

        stopped.cancel();
        let error = tokio::time::timeout(Duration::from_secs(1), gives_up)
            .await
            .expect("a stop ends only its own wait")
            .expect("waiter task")
            .expect_err("stopped");
        assert_eq!(error.kind(), FileErrorKind::Cancelled);
        let error = tokio::time::timeout(Duration::from_secs(1), times_out)
            .await
            .expect("a deadline ends only its own wait")
            .expect("waiter task")
            .expect_err("timed out");
        assert_eq!(error.kind(), FileErrorKind::DeadlineExceeded);

        release.send(()).expect("release");
        tokio::time::timeout(Duration::from_secs(5), receives)
            .await
            .expect("the shared construction reaches the remaining waiter")
            .expect("waiter task")
            .expect("operator");
        claimer
            .join()
            .expect("claimer thread")
            .expect("claimer operator");
        assert_eq!(constructions.load(Ordering::SeqCst), 1);
        let metrics = pool.metrics_snapshot().unwrap();
        assert_eq!(metrics.operator_constructions, 1);
        assert_eq!(metrics.resident_entries, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_abandoned_claim_wakes_its_waiters_and_frees_its_key() {
        let pool =
            Arc::new(ObjectStoreProviderPool::new_for_test(2, Duration::from_secs(60)).unwrap());
        let (_, key) = waiting_key();
        let ProviderAcquisition::Build(claim) = pool.claim(key).expect("claim") else {
            panic!("the first caller claims the construction");
        };
        let waiter = spawn_async_waiter(&pool, FileCancellation::new());
        eventually(|| pool.metrics_snapshot().unwrap().singleflight_waits == 1).await;

        drop(claim);
        let error = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("an abandoned claim still wakes its waiter")
            .expect("waiter task")
            .expect_err("abandoned");
        assert_eq!(error.kind(), FileErrorKind::Internal);

        pool.acquire(
            domain(1),
            "warehouse",
            &endpoint_config("http://localhost:9000"),
            &credential_identity("blue"),
            &static_source("one"),
            |_, _| memory_operator(),
        )
        .expect("the key is free to construct again");
        let metrics = pool.metrics_snapshot().unwrap();
        assert_eq!(metrics.operator_construction_failures, 1);
        assert_eq!(metrics.operator_constructions, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_construction_settles_even_after_the_pool_lock_was_poisoned() {
        let pool =
            Arc::new(ObjectStoreProviderPool::new_for_test(2, Duration::from_secs(60)).unwrap());
        let (endpoint, key) = waiting_key();
        let ProviderAcquisition::Build(claim) = pool.claim(key).expect("claim") else {
            panic!("the first caller claims the construction");
        };
        let waiter = spawn_async_waiter(&pool, FileCancellation::new());
        eventually(|| pool.metrics_snapshot().unwrap().singleflight_waits == 1).await;

        let state = Arc::clone(&pool.state);
        let poisoner = std::thread::spawn(move || {
            let _held = state.inner.lock().expect("pool lock");
            panic!("poison the pool lock");
        });
        assert!(poisoner.join().is_err());
        assert!(pool.state.inner.is_poisoned());

        claim
            .build(&endpoint, &static_source("one"), |_, _| memory_operator())
            .expect("the claimer still settles");
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("the waiter is not left behind a poisoned lock")
            .expect("waiter task")
            .expect("operator");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_panicking_construction_leaves_no_key_in_construction() {
        let pool =
            Arc::new(ObjectStoreProviderPool::new_for_test(2, Duration::from_secs(60)).unwrap());
        let (entered, building) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel::<()>();
        let claimer = {
            let pool = Arc::clone(&pool);
            std::thread::spawn(move || {
                pool.acquire(
                    domain(1),
                    "warehouse",
                    &endpoint_config("http://localhost:9000"),
                    &credential_identity("blue"),
                    &static_source("one"),
                    |_, _| -> FileResult<Operator> {
                        entered.send(()).expect("entered");
                        released.recv().expect("released");
                        panic!("injected construction panic")
                    },
                )
            })
        };
        building
            .recv_timeout(Duration::from_secs(5))
            .expect("the claimer is constructing");
        let waiter = spawn_async_waiter(&pool, FileCancellation::new());
        eventually(|| pool.metrics_snapshot().unwrap().singleflight_waits == 1).await;

        release.send(()).expect("release");
        let error = claimer
            .join()
            .expect("the panic is contained")
            .expect_err("constructing panicked");
        assert_eq!(error.kind(), FileErrorKind::Internal);
        let error = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("the waiter learns of the panic")
            .expect("waiter task")
            .expect_err("construction failed");
        assert_eq!(error.kind(), FileErrorKind::Internal);

        pool.acquire(
            domain(1),
            "warehouse",
            &endpoint_config("http://localhost:9000"),
            &credential_identity("blue"),
            &static_source("one"),
            |_, _| memory_operator(),
        )
        .expect("the key is not left in construction");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_and_blocking_resolution_share_one_pooled_client() {
        let pool =
            Arc::new(ObjectStoreProviderPool::new_for_test(2, Duration::from_secs(60)).unwrap());
        let resolved = {
            let pool = Arc::clone(&pool);
            // Spawned onto the runtime: the resolution future is Send.
            tokio::spawn(async move {
                FsAccessResolver::new()
                    .resolve_location_async(
                        domain(1),
                        "s3://warehouse/sales/part-0.parquet",
                        Some(ObjectStoreAccessContext::new(
                            endpoint_config("http://localhost:9000"),
                            credential_identity("blue"),
                            secret_material("one"),
                            &pool,
                        )),
                        &FileCancellation::new(),
                    )
                    .await
                    .map(|handle| handle.paths().len())
            })
        };
        assert_eq!(
            resolved
                .await
                .expect("resolution task")
                .expect("async resolution"),
            1
        );
        FsAccessResolver::new()
            .resolve_location(
                domain(1),
                "s3://warehouse/sales/part-1.parquet",
                Some(ObjectStoreAccessContext::new(
                    endpoint_config("http://localhost:9000"),
                    credential_identity("blue"),
                    secret_material("one"),
                    &pool,
                )),
            )
            .expect("blocking resolution");
        let metrics = pool.metrics_snapshot().unwrap();
        assert_eq!(metrics.operator_constructions, 1);
        assert_eq!(metrics.cache_hits, 1);
    }

    #[test]
    fn failed_construction_does_not_evict_a_healthy_entry() {
        let pool = ObjectStoreProviderPool::new_for_test(1, Duration::from_secs(1)).unwrap();
        let endpoint = endpoint_config("http://localhost:9000");
        let identity = credential_identity("blue");
        let secrets = static_source("one");
        pool.acquire(
            domain(1),
            "warehouse",
            &endpoint,
            &identity,
            &secrets,
            |_, _| memory_operator(),
        )
        .unwrap();
        let error = pool
            .acquire(
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
        pool.acquire(
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
        let secrets = static_source("one");
        pool.acquire(
            domain(1),
            "warehouse",
            &endpoint_config("HTTPS://EXAMPLE.COM:443/a/../api//"),
            &identity,
            &secrets,
            |_, _| memory_operator(),
        )
        .unwrap();
        pool.acquire(
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

    #[test]
    fn authority_credential_load_serves_every_request_from_material_in_hand() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let authority = seeded_authority(
            "s3://warehouse/sales/orders/",
            Some(vended_material(
                "live",
                Instant::now() + Duration::from_secs(900),
            )),
        );
        let loader = AuthorityCredentialLoad {
            authority: Arc::clone(&authority),
            denial: Arc::new(CredentialDenialLatch::default()),
            budget: Duration::from_secs(60),
        };

        for _ in 0..3 {
            let credential = runtime
                .block_on(loader.load_credential(reqwest::Client::new()))
                .unwrap()
                .expect("material in hand is served");
            assert_eq!(credential.access_key_id, "access-live");
            assert_eq!(credential.secret_access_key, "secret-live");
            assert_eq!(credential.session_token.as_deref(), Some("token-live"));
        }

        // Every load was answered from the material already held: no
        // acquisition was started and nothing waited on one.
        let metrics = authority.metrics();
        assert_eq!(metrics.cache_hits, 3);
        assert_eq!(metrics.blocking_waits, 0);
        assert_eq!(metrics.prefetch_started, 0);
    }

    /// An authority whose catalog this node has no route to.
    struct CatalogUnreachableSource;

    impl AuthorityMaterialSource for CatalogUnreachableSource {
        fn acquire(&self, _deadline: Instant) -> Result<AuthorityMaterial, AcquisitionFailure> {
            Err(AcquisitionFailure::CatalogUnreachable(
                "connect to catalog refused".to_string(),
            ))
        }
    }

    impl RefreshExecutor for CatalogUnreachableSource {
        fn execute(&self, job: Box<dyn FnOnce() + Send + 'static>) {
            // Never inline: the job is handed over under the authority's state
            // lock and takes that same lock to apply its outcome.
            std::thread::spawn(job);
        }
    }

    #[test]
    fn an_unreachable_catalog_reaches_the_caller_as_itself() {
        // CAD-1 D12 / acceptance 18, at the seam where the two classification
        // sources meet: the storage side turns every credential-load failure
        // into something temporary, so the catalog-side reason has to survive
        // in the text or an operator loses the only signal they get.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        // The capability, not the source, decides whether a renewal is even
        // attempted, so this authority must be one that can renew — a seeded
        // one would answer "no renewal capability" without ever asking.
        let authority = Arc::new(StorageAuthority::new(
            StorageAuthorityId::new(
                CatalogHandle::new(
                    ConnectorInstanceId::parse("lake").unwrap(),
                    CatalogVersion::from_bytes([0x11; 32]),
                ),
                StorageCredentialScopePrefix::try_from_normalized("s3://warehouse/sales/orders/")
                    .unwrap(),
                AuthorityCapabilityPath::CredentialsEndpoint {
                    principal: StaticCredentialReference::try_new("executor", "v1").unwrap(),
                    endpoint: Arc::from("http://127.0.0.1:1/v1/credentials"),
                },
            ),
            Arc::new(CatalogUnreachableSource),
            Arc::new(CatalogUnreachableSource),
            RefreshPolicy::default(),
        ));
        let endpoint = ObjectStoreEndpointIdentity::try_new(
            "warehouse",
            &endpoint_config("http://127.0.0.1:1"),
        )
        .unwrap();
        let operator = build_object_store_operator(
            &endpoint,
            &ObjectStoreCredentialSource::Authority(Arc::clone(&authority)),
        )
        .unwrap();

        let error = runtime
            .block_on(operator.stat("sales/orders/part-0.parquet"))
            .expect_err("an unreachable catalog cannot sign a request");
        let mapped = map_opendal_error("stat file", error);
        // Not a denial: nothing has said this node may not read, only that it
        // cannot ask.
        assert_eq!(mapped.kind(), FileErrorKind::Transient);
        let rendered = format!("{mapped:#}");
        assert!(
            rendered.contains("could not reach its catalog"),
            "the reachability reason must survive to the caller, got {rendered}"
        );
        assert_eq!(
            authority.metrics().blocking_waits,
            1,
            "one object-store operation must not retry a failed credential load"
        );
    }

    #[test]
    fn a_confirmed_denial_is_not_retried_and_stays_a_denial() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        // No material and no renewal capability: every credential load is a
        // confirmed denial, decided locally and without any remote request.
        let authority = seeded_authority("s3://warehouse/sales/orders/", None);
        let endpoint = ObjectStoreEndpointIdentity::try_new(
            "warehouse",
            &endpoint_config("http://127.0.0.1:1"),
        )
        .unwrap();
        let operator = build_object_store_operator(
            &endpoint,
            &ObjectStoreCredentialSource::Authority(Arc::clone(&authority)),
        )
        .unwrap();

        let error = runtime
            .block_on(operator.stat("sales/orders/part-0.parquet"))
            .expect_err("a denied authority cannot sign a request");
        assert_eq!(
            map_opendal_error("stat file", error).kind(),
            FileErrorKind::Permission
        );
        // The retry layer swallows anything opendal marked temporary. One
        // credential load means the denial reached the caller as itself
        // instead of being retried as jitter (CAD-1 D4).
        assert_eq!(authority.metrics().blocking_waits, 1);
    }

    #[test]
    fn a_credential_acquisition_failure_ends_only_the_current_object_operation() {
        let latch = CredentialDenialLatch::default();
        let credential_failure = opendal::Error::new(
            opendal::ErrorKind::Unexpected,
            "loading credential to sign http request",
        )
        .set_temporary()
        .set_source(FileError::new(
            FileErrorKind::Transient,
            "storage authority could not reach its catalog",
        ));
        let classified = latch.classify(credential_failure);
        assert_eq!(classified.kind(), opendal::ErrorKind::Unexpected);
        assert!(!classified.is_temporary());
        assert!(
            classified
                .to_string()
                .contains("could not reach its catalog")
        );

        let unrelated_s3_failure =
            opendal::Error::new(opendal::ErrorKind::Unexpected, "S3 response unavailable")
                .set_temporary();
        assert!(latch.classify(unrelated_s3_failure).is_temporary());
    }
}
