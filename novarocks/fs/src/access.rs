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
use std::future::IntoFuture;
use std::io::Write;
use std::net::IpAddr;
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use novarocks_failure_contract::SharedFailureCause;
use novarocks_spi::connector::{StaticCredentialReference, StorageAccessDomainId};
use opendal::layers::{ConcurrentLimitLayer, RetryLayer, TimeoutLayer};
use opendal::raw::{
    Layer, OpCopier, OpCopy, OpCreateDir, OpDelete, OpList, OpPresign, OpRead, OpRename, OpStat,
    OpWrite, RpCreateDir, RpPresign, RpRead, RpRename, RpStat, Service, ServiceInfo, Servicer, oio,
};
use opendal::{Buffer, HttpTransporter, Metadata, OperationContext, Operator};
use url::{Host, Url};

use crate::http_transport::Reqwest012Transport;

#[path = "s3_signing.rs"]
mod s3_signing;
use s3_signing::{S3PresignLayer, S3RequestSigner, S3SigningTransport};

use crate::list_body_limit::{
    ListBodyLimitFetch, OBJECT_STORE_LIST_BODY_LIMIT_BYTES, is_list_body_limit_exceeded,
};
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

    /// Transfer the already-owned URI without a second provider path copy.
    pub fn into_parts(self) -> (String, u64, bool) {
        (self.location, self.size, self.is_dir)
    }
}

/// Explicit bounds for a local catalog listing, never applied to generic reads.
/// Page size is an OpenDAL request parameter, not a response/XML allocation bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FsListingBound {
    page_entries: usize,
    path_bytes: usize,
    workspace_bytes: usize,
}

impl FsListingBound {
    pub fn try_new(
        page_entries: usize,
        path_bytes: usize,
        workspace_bytes: usize,
    ) -> FileResult<Self> {
        if page_entries == 0
            || page_entries > 256
            || path_bytes == 0
            || path_bytes > 65_536
            || workspace_bytes == 0
            || workspace_bytes > 32 * 1024 * 1024
        {
            return Err(FileError::invalid(
                "bounded local listing exceeds its frozen page/path/workspace profile",
            ));
        }
        Ok(Self {
            page_entries,
            path_bytes,
            workspace_bytes,
        })
    }

    /// Borrowed preflight for a host's validation copies, before parsing a URI.
    pub fn check_path(&self, path: &str) -> FileResult<()> {
        if path.len() > self.path_bytes {
            return Err(listing_path_exhausted());
        }
        // FsLocation keeps original + split pieces (<=2*input bytes), with
        // fixed String/error headers. A provider can use this before its own
        // location validation, even when it supplies a smaller workspace.
        self.check_workspace(
            path.len()
                .checked_mul(2)
                .and_then(|bytes| bytes.checked_add(256))
                .ok_or_else(listing_workspace_exhausted)?,
        )
    }

    fn check_workspace(&self, bytes: usize) -> FileResult<()> {
        if bytes > self.workspace_bytes {
            return Err(listing_workspace_exhausted());
        }
        Ok(())
    }
}

impl FsListingBound {
    fn check_path_length(&self, bytes: usize) -> FileResult<()> {
        if bytes > self.path_bytes {
            return Err(listing_path_exhausted());
        }
        Ok(())
    }
}

struct ListingUriContext {
    scheme: FsScheme,
    uri_scheme: Option<String>,
    authority: Option<String>,
    local_root: Option<String>,
    bounds: FsListingBound,
}

impl ListingUriContext {
    fn render(&self, path: &str) -> FileResult<String> {
        self.bounds.check_path(path)?;
        match self.scheme {
            FsScheme::ObjectStore | FsScheme::Hdfs => {
                let scheme =
                    self.uri_scheme
                        .as_deref()
                        .unwrap_or(if self.scheme == FsScheme::ObjectStore {
                            "s3"
                        } else {
                            "hdfs"
                        });
                let authority = self.authority.as_deref().unwrap_or_default();
                let path = path.trim_start_matches('/');
                let length = scheme
                    .len()
                    .checked_add(3)
                    .and_then(|bytes| bytes.checked_add(authority.len()))
                    .and_then(|bytes| bytes.checked_add(1))
                    .and_then(|bytes| bytes.checked_add(path.len()))
                    .ok_or_else(listing_path_exhausted)?;
                let mut uri = bounded_listing_string(length, self.bounds)?;
                uri.push_str(scheme);
                uri.push_str("://");
                uri.push_str(authority);
                uri.push('/');
                uri.push_str(path);
                Ok(uri)
            }
            FsScheme::Local => {
                // Preauthorize the worst-case join before PathBuf copies. Keep
                // std Path::join semantics, including absolute child paths.
                let root = self.local_root.as_deref().filter(|root| *root != ".");
                let length = match root {
                    Some(root) if !Path::new(path).is_absolute() => {
                        let separator = usize::from(
                            !root.is_empty() && !root.ends_with(std::path::MAIN_SEPARATOR),
                        );
                        root.len()
                            .checked_add(separator)
                            .and_then(|bytes| bytes.checked_add(path.len()))
                            .ok_or_else(listing_path_exhausted)?
                    }
                    _ => path.len(),
                };
                self.bounds.check_path_length(length)?;
                self.bounds.check_workspace(
                    length
                        .checked_mul(3)
                        .ok_or_else(listing_workspace_exhausted)?,
                )?;
                let mut joined = PathBuf::with_capacity(length);
                if !Path::new(path).is_absolute()
                    && let Some(root) = root
                {
                    joined.push(root);
                }
                joined.push(path);
                self.bounds.check_path_length(joined.capacity())?;
                let text = joined.to_str().ok_or_else(|| {
                    FileError::new(FileErrorKind::Invalid, "listing joined path is not UTF-8")
                })?;
                let mut uri = bounded_listing_string(text.len(), self.bounds)?;
                uri.push_str(text);
                Ok(uri)
            }
        }
    }
}

fn bounded_listing_string(bytes: usize, bounds: FsListingBound) -> FileResult<String> {
    bounds.check_path_length(bytes)?;
    bounds.check_workspace(
        bytes
            .checked_mul(3)
            .ok_or_else(listing_workspace_exhausted)?,
    )?;
    let mut result = String::new();
    result
        .try_reserve_exact(bytes)
        .map_err(|_| listing_workspace_exhausted())?;
    bounds.check_path_length(result.capacity())?;
    Ok(result)
}

fn listing_path_exhausted() -> FileError {
    FileError::new(
        FileErrorKind::ResourceExhausted,
        "bounded listing path bytes exceeded",
    )
}
fn listing_workspace_exhausted() -> FileError {
    FileError::new(
        FileErrorKind::ResourceExhausted,
        "bounded listing URI workspace exceeded",
    )
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
    /// Derive only a request view. The pooled operator, signer, client and
    /// authority identities remain the originals; no client/cache is minted.
    pub fn with_request_context(
        mut self,
        request: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Self {
        if request.io_admission().is_some() {
            let base = self.operator.base_context();
            let transport = HttpTransporter::new(s3_signing::GovernedRequestTransport::new(
                base.http_transport().clone(),
                request,
            ));
            let context = base.with_http_transport(transport);
            self.operator = self.operator.with_context(context);
        }
        self
    }

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
        let operator_relative_path = self.resolve_bound_relative_path(&location)?;
        let path = ResolvedFsPath::new(location, operator_relative_path)?;
        Ok(BoundFile {
            access: self.clone(),
            path,
            identity,
        })
    }

    fn resolve_bound_relative_path(&self, location: &FsLocation) -> FileResult<String> {
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
        Ok(operator_relative_path)
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

    fn preflight_bounded_location(&self, prefix: &str, bounds: FsListingBound) -> FileResult<()> {
        bounds.check_path(prefix)?;
        let root_capacity = self.root.as_ref().map_or(0, String::capacity);
        let authority_capacity = self.authority.as_ref().map_or(0, String::capacity);
        if root_capacity > bounds.path_bytes || authority_capacity > bounds.path_bytes {
            return Err(listing_path_exhausted());
        }
        // Object/capacity proof (P=prefix bytes, R/A=actual root/authority
        // capacities, L=path limit): FsLocation original + split components
        // <=2P; local relative PathBuf <=P; normalization old+new <=3P,
        // or normalized String + PathBuf <=3P. Their maximum is <=6P.
        // Subsequent list-path/context copies and bounded permission/error
        // formatting fit the 12P allowance. Root/authority context copies,
        // retained originals and error formatting fit 4(R+A). Active local
        // join + exact URI and host handoff fit 4L (handoff moves the String).
        // Fixed headers/async state use 64KiB. P,R,A,L<=64KiB makes the whole
        // transient proof <=25*64KiB<2MiB, reserved inside the catalog's
        // 32MiB workspace; it is not an independent capacity wallet.
        let prepared_bytes = prefix
            .len()
            .checked_mul(12)
            .and_then(|bytes| {
                root_capacity
                    .checked_add(authority_capacity)
                    .and_then(|roots| roots.checked_mul(4))
                    .and_then(|roots| bytes.checked_add(roots))
            })
            .and_then(|bytes| {
                bounds
                    .path_bytes
                    .checked_mul(4)
                    .and_then(|item| bytes.checked_add(item))
            })
            .and_then(|bytes| bytes.checked_add(64 * 1024))
            .ok_or_else(listing_workspace_exhausted)?;
        bounds.check_workspace(prepared_bytes)?;
        Ok(())
    }

    /// One explicitly bounded catalog metadata probe over the authorized
    /// client. It creates no BoundFile, paths-inventory clone or size cache.
    /// The same <2MiB source auxiliary covers both listing and sequential
    /// schema probes; this does not allocate a second workspace allowance.
    pub async fn stat_location_bounded(
        &self,
        prefix: impl AsRef<str>,
        cancellation: &FileCancellation,
        bounds: FsListingBound,
    ) -> FileResult<u64> {
        cancellation.check()?;
        let prefix = prefix.as_ref();
        self.preflight_bounded_location(prefix, bounds)?;
        let location = FsLocation::parse(prefix)?;
        let relative = self.resolve_bound_relative_path(&location)?;
        bounds.check_path_length(relative.capacity())?;
        // FsLocation <=2P + normalized relative <=P; normalization's old/new
        // <=3P and headers/error formatting are already in the shared 12P
        // preflight. The operator is borrowed; no identity/path-inventory copy.
        let metadata = match futures::future::select(
            Box::pin(self.operator.stat(&relative)),
            Box::pin(cancellation.ended()),
        )
        .await
        {
            futures::future::Either::Left((result, _)) => result.map_err(|error| {
                map_bounded_listing_opendal_error("stat catalog metadata", error)
            })?,
            futures::future::Either::Right((error, _)) => return Err(error),
        };
        cancellation.check()?;
        Ok(metadata.content_length())
    }

    /// Bounded local-catalog listing over this same authorized client.
    ///
    /// Checks borrowed inputs before URI/identity/relative-path growth, avoids
    /// cloning the handle's entire paths inventory, and requests finite pages.
    /// OpenDAL's received page and XML decoder remain third-party internals;
    /// this entrypoint does not claim to bound that response body.
    pub async fn list_location_bounded(
        &self,
        prefix: impl AsRef<str>,
        recursive: bool,
        cancellation: &FileCancellation,
        bounds: FsListingBound,
    ) -> FileResult<FsListStream> {
        cancellation.check()?;
        let prefix = prefix.as_ref();
        self.preflight_bounded_location(prefix, bounds)?;
        let location = FsLocation::parse(prefix)?;
        let relative = self.resolve_bound_relative_path(&location)?;
        let relative = relative.trim_end_matches('/');
        let list_len = relative
            .len()
            .checked_add(1)
            .ok_or_else(listing_path_exhausted)?;
        bounds.check_path_length(list_len)?;
        let mut list_path = bounded_listing_string(list_len, bounds)?;
        list_path.push_str(relative);
        list_path.push('/');
        let lister = match futures::future::select(
            Box::pin(
                self.operator
                    .lister_with(&list_path)
                    .recursive(recursive)
                    .limit(bounds.page_entries)
                    .into_future(),
            ),
            Box::pin(cancellation.ended()),
        )
        .await
        {
            futures::future::Either::Left((result, _)) => {
                result.map_err(|error| map_bounded_listing_opendal_error("list files", error))?
            }
            futures::future::Either::Right((error, _)) => return Err(error),
        };
        cancellation.check()?;
        let context = ListingUriContext {
            scheme: self.scheme,
            uri_scheme: location.uri_scheme().map(ToOwned::to_owned),
            authority: location.authority().map(ToOwned::to_owned),
            local_root: self.root.clone(),
            bounds,
        };
        let initial = Some((lister, context, cancellation.clone()));
        Ok(Box::pin(futures::stream::unfold(
            initial,
            |state| async move {
                let (mut lister, context, cancellation) = state?;
                if let Err(error) = cancellation.check() {
                    return Some((Err(error), None));
                }
                // A pending SDK next must register a stop/deadline wake; a
                // checkpoint alone cannot observe cancellation until it returns.
                // The losing future is dropped before releasing this state.
                let next = match futures::future::select(
                    Box::pin(lister.next()),
                    Box::pin(cancellation.ended()),
                )
                .await
                {
                    futures::future::Either::Left((result, _)) => result,
                    futures::future::Either::Right((error, _)) => return Some((Err(error), None)),
                };
                let entry = match next? {
                    Ok(entry) => entry,
                    Err(error) => {
                        return Some((
                            Err(map_bounded_listing_opendal_error("list files", error)),
                            None,
                        ));
                    }
                };
                let result = cancellation
                    .check()
                    .and_then(|()| context.render(entry.path()))
                    .map(|location| FsListEntry {
                        location,
                        size: entry.metadata().content_length(),
                        is_dir: entry.metadata().is_dir(),
                    });
                match result {
                    Ok(entry) => Some((Ok(entry), Some((lister, context, cancellation)))),
                    Err(error) => Some((Err(error), None)),
                }
            },
        )))
    }

    /// Atomically create one authorized path without replacing an existing file.
    ///
    /// Design: ADR-0077 makes native conditional storage creation the publication
    /// fence; this method must never emulate it with an existence check plus write.
    /// Local installation performs synchronous filesystem calls on the calling
    /// thread. Its caller must retain that thread until the operation returns
    /// (catalog callers use their joining runtime bridge), rather than run it
    /// directly on a Tokio worker. This layer does not schedule another task.
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
        if !self.operator.info().capability().write_with_if_not_exists {
            return Err(FileError::unsupported(
                "filesystem does not support native conditional create",
            ));
        }

        if self.scheme == FsScheme::Local {
            let root = self.root.as_deref().ok_or_else(|| {
                FileError::new(FileErrorKind::Internal, "local access handle has no root")
            })?;
            let relative = normalize_bound_relative_path(
                Path::new(path.operator_relative_path()),
                path.location().original(),
            )?;
            return install_local_file(&Path::new(root).join(relative), cancellation, |file| {
                file.write_all(&payload).map_err(|error| {
                    map_local_install_error("write conditional temporary file", error)
                })
            });
        }

        let result = self
            .operator
            .write_with(path.operator_relative_path(), payload)
            .if_not_exists(true)
            .await;
        // The completed operation owns its result. A later stop cannot erase
        // its ACK (or replace its original failure with a cancellation error).
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

/// Write and close a private file before exposing its name at the target slot.
/// No future or detached work survives this synchronous call, including stop
/// and deadline paths. The write callback is the one whole-payload writer.
fn install_local_file(
    target: &Path,
    cancellation: &FileCancellation,
    write: impl FnOnce(&mut std::fs::File) -> FileResult<()>,
) -> FileResult<ConditionalCreateOutcome> {
    cancellation.check()?;
    let parent = target
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    // Preserve the local operator's existing parent-directory creation policy.
    std::fs::create_dir_all(parent)
        .map_err(|error| map_local_install_error("create conditional file parent", error))?;
    cancellation.check()?;
    let mut temporary = tempfile::Builder::new()
        .prefix(".novarocks-create-")
        .suffix(".tmp")
        .tempfile_in(parent)
        .map_err(|error| map_local_install_error("create conditional temporary file", error))?;
    let written = cancellation
        .check()
        .and_then(|()| write(temporary.as_file_mut()));
    // into_temp_path closes the file before hard_link. We promise complete
    // runtime visibility, not persistence across power loss, so do not fsync.
    let temporary = temporary.into_temp_path();
    let result = written.and_then(|()| {
        cancellation.check()?;
        match std::fs::hard_link(&temporary, target) {
            Ok(()) => Ok(ConditionalCreateOutcome::Created),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                Ok(ConditionalCreateOutcome::AlreadyExists)
            }
            Err(error) => Err(map_local_install_error("install conditional file", error)),
        }
    });
    if let Err(error) = temporary.close() {
        // Removing the private name never retracts a successful installation.
        tracing::warn!(%error, "failed to remove conditional temporary file");
    }
    result
}

fn map_local_install_error(operation: &str, error: std::io::Error) -> FileError {
    let kind = match error.kind() {
        std::io::ErrorKind::AlreadyExists => FileErrorKind::AlreadyExists,
        std::io::ErrorKind::NotFound => FileErrorKind::NotFound,
        std::io::ErrorKind::PermissionDenied => FileErrorKind::Permission,
        _ => FileErrorKind::Transient,
    };
    let cause = authority_cause_in(&error);
    carry_authority_cause(FileError::with_source(kind, operation, error), cause)
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
#[derive(Clone, Eq, PartialEq)]
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
        // Build both transport policies once, before publishing the pool or
        // starting its janitor. Operators clone these clients, not their pools.
        let http_clients = ObjectStoreHttpClients::try_new()?;
        let state = Arc::new(ObjectStoreProviderPoolState {
            options,
            http_clients,
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

    // Invoked only by the original single-flight construction owner, after
    // claim released the pool mutex. Client selection performs no I/O.
    fn build_operator(
        &self,
        endpoint: &ObjectStoreEndpointIdentity,
        credentials: &ObjectStoreCredentialSource,
    ) -> FileResult<Operator> {
        build_object_store_operator(
            endpoint,
            credentials,
            self.state.http_clients.select(endpoint),
        )
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
        credentials: &ObjectStoreCredentialSource,
        builder: F,
    ) -> FileResult<Operator>
    where
        F: FnOnce(
            &ObjectStoreEndpointIdentity,
            &ObjectStoreCredentialSource,
        ) -> FileResult<Operator>,
    {
        let (endpoint, key) = provider_key(access_domain, bucket, endpoint_config, credentials)?;
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
        access_domain: StorageAccessDomainId,
        bucket: &str,
        endpoint_config: &ObjectStoreEndpointConfig,
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
        let (endpoint, key) = provider_key(access_domain, bucket, endpoint_config, credentials)?;
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
    credentials: &ObjectStoreCredentialSource,
) -> FileResult<(ObjectStoreEndpointIdentity, ObjectStoreProviderKey)> {
    let endpoint = ObjectStoreEndpointIdentity::try_new(bucket, endpoint_config)?;
    let key = ObjectStoreProviderKey {
        endpoint: endpoint.clone(),
        access_domain,
        credential_identity: credentials.identity(),
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

// HTTP policy belongs to the process FS pool, independently of the operator
// cache key, bucket, access domain, or credential provider identity.
struct ObjectStoreHttpClients {
    local: reqwest::Client,
    remote: reqwest::Client,
}

impl ObjectStoreHttpClients {
    fn try_new() -> FileResult<Self> {
        let build = |builder: reqwest::ClientBuilder| {
            builder
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|error| {
                    FileError::with_source(
                        FileErrorKind::Internal,
                        "build object-store pool HTTP client",
                        error,
                    )
                })
        };
        Ok(Self {
            local: build(reqwest::Client::builder().no_proxy())?,
            remote: build(reqwest::Client::builder())?,
        })
    }

    fn select(&self, endpoint: &ObjectStoreEndpointIdentity) -> reqwest::Client {
        if is_local_endpoint(&endpoint.endpoint) {
            self.local.clone()
        } else {
            self.remote.clone()
        }
    }
}

struct ObjectStoreProviderPoolState {
    options: ObjectStoreProviderPoolOptions,
    http_clients: ObjectStoreHttpClients,
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
enum ObjectStoreCredentialProviderIdentity {
    /// Startup-composed material named by an exact static reference.
    Static(StaticCredentialReference),
    /// Identity derived from the authority that will sign the request.
    Authority(StorageAuthorityId),
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
    Static {
        reference: StaticCredentialReference,
        material: ObjectStoreSecretMaterial,
    },
    Authority(Arc<StorageAuthority>),
}

impl ObjectStoreCredentialSource {
    fn identity(&self) -> ObjectStoreCredentialProviderIdentity {
        match self {
            Self::Static { reference, .. } => {
                ObjectStoreCredentialProviderIdentity::Static(reference.clone())
            }
            Self::Authority(authority) => {
                ObjectStoreCredentialProviderIdentity::Authority(authority.id().clone())
            }
        }
    }
}

impl Debug for ObjectStoreCredentialSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Static {
                reference,
                material,
            } => f
                .debug_struct("Static")
                .field("reference", reference)
                .field("material", material)
                .finish(),
            Self::Authority(authority) => f.debug_tuple("Authority").field(authority).finish(),
        }
    }
}

pub struct ObjectStoreAccessContext<'a> {
    endpoint_config: ObjectStoreEndpointConfig,
    credentials: ObjectStoreCredentialSource,
    provider_pool: &'a ObjectStoreProviderPool,
}

impl<'a> ObjectStoreAccessContext<'a> {
    /// Bind startup-composed static material to its exact reference.
    pub fn for_static(
        endpoint_config: ObjectStoreEndpointConfig,
        credential_reference: StaticCredentialReference,
        secret_material: ObjectStoreSecretMaterial,
        provider_pool: &'a ObjectStoreProviderPool,
    ) -> Self {
        Self {
            endpoint_config,
            credentials: ObjectStoreCredentialSource::Static {
                reference: credential_reference,
                material: secret_material,
            },
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
            credentials: ObjectStoreCredentialSource::Authority(authority),
            provider_pool,
        }
    }
}

impl Debug for ObjectStoreAccessContext<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreAccessContext")
            .field("endpoint_config", &self.endpoint_config)
            .field("credentials", &self.credentials)
            .field("provider_pool", &self.provider_pool)
            .finish_non_exhaustive()
    }
}

/// Cloneable view of one explicitly composed filesystem runtime epoch.
/// A configuration-only view can parse locations and resolve local/S3 access;
/// HDFS execution requires the role runtime before any cache lookup.
#[derive(Clone, Default)]
pub struct FsAccessResolver {
    hdfs: Option<Arc<HdfsRuntimeView>>,
}

// The former HDFS operator cache is owned by this original resolver view,
// rather than a process-global key which can cross role/runtime epochs.
// Operators retain only a clone of the supplied Handle, never this view.
struct HdfsRuntimeView {
    runtime: tokio::runtime::Handle,
    operators: Mutex<HashMap<HdfsOperatorKey, Operator>>,
}

impl Debug for FsAccessResolver {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FsAccessResolver")
            .field("hdfs_runtime_installed", &self.hdfs.is_some())
            .finish_non_exhaustive()
    }
}

impl FsAccessResolver {
    /// Configuration/local/S3 view. This never discovers or creates a runtime.
    pub fn new() -> Self {
        Self::default()
    }

    /// The composition root supplies its existing role runtime. Clones share
    /// this same HDFS cache epoch; changing an I/O view does not mint another.
    /// The caller retains the Runtime itself through all filesystem work.
    pub fn with_runtime(runtime: tokio::runtime::Handle) -> Self {
        Self {
            hdfs: Some(Arc::new(HdfsRuntimeView {
                runtime,
                operators: Mutex::new(HashMap::new()),
            })),
        }
    }

    pub fn parse_location(&self, raw: impl AsRef<str>) -> FileResult<FsLocation> {
        FsLocation::parse(raw)
    }

    /// Pure projection from the SAME route author used by resolve_locations.
    /// It does not create an operator, resolve DNS, acquire secrets, or dial.
    pub fn selected_dependency(
        &self,
        location: &str,
        object_store: Option<&ObjectStoreEndpointConfig>,
    ) -> FileResult<novarocks_spi::connector::SharedDependency> {
        use novarocks_spi::connector::{HdfsServiceOrigin, NetworkServiceOrigin, SharedDependency};
        let selected = self.parse_location(location)?;
        match selected.scheme() {
            FsScheme::Local => Ok(SharedDependency::LocalHost),
            FsScheme::Hdfs => {
                let original = parse_hdfs_selection(location, false)?;
                let route = Url::parse(&original.name_node)
                    .map_err(|_| FileError::invalid("invalid selected HDFS service"))?;
                HdfsServiceOrigin::from_selected_url(&route)
                    .map(SharedDependency::Hdfs)
                    .map_err(|_| {
                        FileError::invalid("selected HDFS service exceeds dependency profile")
                    })
            }
            FsScheme::ObjectStore => {
                let config = object_store.ok_or_else(|| {
                    FileError::invalid("object-store route has no original endpoint configuration")
                })?;
                let bucket = selected
                    .authority()
                    .ok_or_else(|| FileError::invalid("object-store path has no bucket"))?;
                let endpoint = ObjectStoreEndpointIdentity::try_new(bucket, config)?;
                let route = Url::parse(&endpoint.endpoint)
                    .map_err(|_| FileError::invalid("invalid selected object-store endpoint"))?;
                NetworkServiceOrigin::from_selected_url(&route)
                    .map(SharedDependency::Network)
                    .map_err(|_| {
                        FileError::invalid("selected storage service exceeds dependency profile")
                    })
            }
        }
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
            FsScheme::Hdfs => {
                resolve_hdfs_locations(access_domain, locations, self.hdfs.as_deref())
            }
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
                let operator = access
                    .provider_pool
                    .acquire_async(
                        access_domain,
                        &bucket,
                        &access.endpoint_config,
                        &access.credentials,
                        cancellation,
                        |endpoint, credentials| {
                            access.provider_pool.build_operator(endpoint, credentials)
                        },
                    )
                    .await?;
                object_store_handle(access_domain, locations, bucket, operator)
            }
            FsScheme::Hdfs => {
                resolve_hdfs_locations(access_domain, locations, self.hdfs.as_deref())
            }
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
        .map_err(|error| map_opendal_error("initialize local file operator", error))?;
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
        &object_store_access.credentials,
        |endpoint, credentials| {
            object_store_access
                .provider_pool
                .build_operator(endpoint, credentials)
        },
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
    client: reqwest::Client,
) -> FileResult<Operator> {
    // Signing belongs to the Provider transport, after it sees the actual
    // SDK-selected request URI. The SDK must not load credentials first.
    let mut builder = opendal::services::S3::default()
        .endpoint(&endpoint.endpoint)
        .bucket(&endpoint.bucket)
        .region(&endpoint.region)
        .skip_signature()
        .disable_config_load()
        .disable_ec2_metadata();
    if !endpoint.use_path_style {
        builder = builder.enable_virtual_host_style();
    }
    let denial = Arc::new(CredentialDenialLatch::default());
    let signer = Arc::new(S3RequestSigner::new(
        endpoint,
        credentials,
        Arc::clone(&denial),
    )?);
    let mut operator = Operator::new(builder)
        .map_err(|error| map_opendal_error("initialize object store operator", error))?
        .layer(S3PresignLayer::new(Arc::clone(&signer)));
    if matches!(credentials, ObjectStoreCredentialSource::Authority(_)) {
        // Preserve the original authority denial classifier below RetryLayer.
        operator = operator.layer(CredentialDenialLayer { denial });
    }
    let signed_transport = HttpTransporter::new(S3SigningTransport::new(
        Reqwest012Transport::new(client),
        signer,
    ));
    // Preserve the independent list body limit outside signing/HTTP dispatch.
    let context = operator
        .base_context()
        .with_http_transport(HttpTransporter::new(ListBodyLimitFetch::new(
            signed_transport,
            OBJECT_STORE_LIST_BODY_LIMIT_BYTES,
        )));
    operator = operator.with_context(context);
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
/// The Provider signer reads the original authority on every signing attempt.
/// [`StorageAuthority::material_for_request`] serves cached material without
/// touching the network while it remains valid.
/// A remote acquisition happens only when the authority itself decides one is
/// due, and it runs on the authority's own executor, never on this thread.
struct AuthoritySigningSource {
    authority: Arc<StorageAuthority>,
    denial: Arc<CredentialDenialLatch>,
    /// How long one credential load may take. This is the operator's own
    /// request timeout, not a second retry budget: CAD-1 D5 keeps the
    /// operation deadline and the acquisition budget separate, and this seam
    /// has no narrower deadline to offer.
    budget: Duration,
}

impl AuthoritySigningSource {
    async fn credential_for_request_in(
        &self,
        request: Option<&novarocks_spi::connector::ConnectorRequestContext>,
    ) -> anyhow::Result<reqsign::AwsCredential> {
        let now = Instant::now();
        let deadline = now.checked_add(self.budget).unwrap_or(now);
        let result = match request {
            Some(request) => {
                self.authority
                    .material_for_request_in(now, deadline, request.clone())
                    .await
            }
            None => self.authority.material_for_request(now, deadline).await,
        };
        match result {
            Ok(material) => Ok(reqsign::AwsCredential {
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
            }),
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
/// The Provider signs requests through the original authority. Its typed
/// acquisition outcomes must survive the SDK operation and stream wrappers;
/// a confirmed denial must not be retried as transient S3 transport failure.
///
/// The loader latches a denial here and [`CredentialDenialLayer`], installed
/// under the RetryLayer, restores its meaning before the retry predicate can
/// read it. The layer also recognizes a transient authority failure in the
/// source chain and ends only that object operation's retry cycle. A later
/// operation can still ask the open authority for material.
#[derive(Debug, Default)]
struct CredentialDenialLatch {
    denial: Mutex<Option<RecordedCredentialDenial>>,
}

/// Keep the original local denial, including its producer-owned fact when
/// present. Unclassified Permission errors retain the existing generic path.
#[derive(Clone, Debug)]
enum RecordedCredentialDenial {
    Authority(SharedFailureCause),
    Generic(String),
}

impl CredentialDenialLatch {
    fn record(&self, error: &FileError) {
        let mut denial = self
            .denial
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if denial.is_none() {
            *denial = Some(match error.authority_cause() {
                Some(cause) => RecordedCredentialDenial::Authority(cause.clone()),
                None => RecordedCredentialDenial::Generic(error.to_string()),
            });
        }
    }

    fn observed(&self) -> Option<RecordedCredentialDenial> {
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
            Some(RecordedCredentialDenial::Authority(cause)) => {
                let detail = cause.detail().to_owned();
                let source = FileError::with_source(
                    FileErrorKind::Permission,
                    "retained storage authority credential denial",
                    error,
                )
                .with_authority_cause(cause);
                opendal::Error::new(opendal::ErrorKind::PermissionDenied, detail)
                    .set_permanent()
                    .set_source(source)
            }
            Some(RecordedCredentialDenial::Generic(detail)) => {
                opendal::Error::new(opendal::ErrorKind::PermissionDenied, detail)
                    .set_permanent()
                    .set_source(error)
            }
            None => error,
        }
    }
}

/// Restores a confirmed credential denial before opendal's retry predicate
/// sees it. Must be installed under the `RetryLayer`.
#[derive(Clone, Debug)]
struct CredentialDenialLayer {
    denial: Arc<CredentialDenialLatch>,
}

impl Layer for CredentialDenialLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(CredentialDenialService {
            inner,
            denial: Arc::clone(&self.denial),
        })
    }
}

#[derive(Debug)]
struct CredentialDenialService {
    inner: Servicer,
    denial: Arc<CredentialDenialLatch>,
}

impl CredentialDenialService {
    fn wrap<T>(&self, inner: T) -> CredentialDenialStream<T> {
        CredentialDenialStream {
            inner,
            denial: Arc::clone(&self.denial),
        }
    }
}

impl Service for CredentialDenialService {
    type Reader = CredentialDenialStream<oio::Reader>;
    type Writer = CredentialDenialStream<oio::Writer>;
    type Lister = CredentialDenialStream<oio::Lister>;
    type Deleter = CredentialDenialStream<oio::Deleter>;
    type Copier = oio::Copier;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> opendal::Capability {
        self.inner.capability()
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> opendal::Result<RpCreateDir> {
        self.inner.create_dir(ctx, path, args).await
    }

    fn read(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRead,
    ) -> opendal::Result<Self::Reader> {
        self.inner
            .read(ctx, path, args)
            .map(|reader| self.wrap(reader))
            .map_err(|error| self.denial.classify(error))
    }

    fn write(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpWrite,
    ) -> opendal::Result<Self::Writer> {
        self.inner
            .write(ctx, path, args)
            .map(|writer| self.wrap(writer))
            .map_err(|error| self.denial.classify(error))
    }

    async fn stat(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpStat,
    ) -> opendal::Result<RpStat> {
        self.inner
            .stat(ctx, path, args)
            .await
            .map_err(|error| self.denial.classify(error))
    }

    fn delete(&self, ctx: &OperationContext) -> opendal::Result<Self::Deleter> {
        self.inner
            .delete(ctx)
            .map(|deleter| self.wrap(deleter))
            .map_err(|error| self.denial.classify(error))
    }

    fn list(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpList,
    ) -> opendal::Result<Self::Lister> {
        self.inner
            .list(ctx, path, args)
            .map(|lister| self.wrap(lister))
            .map_err(|error| self.denial.classify(error))
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        opts: OpCopier,
    ) -> opendal::Result<Self::Copier> {
        self.inner.copy(ctx, from, to, args, opts)
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> opendal::Result<RpRename> {
        self.inner.rename(ctx, from, to, args).await
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> opendal::Result<RpPresign> {
        self.inner.presign(ctx, path, args).await
    }
}

/// Classify the same signing failures at lazy reader and stream boundaries.
struct CredentialDenialStream<T> {
    inner: T,
    denial: Arc<CredentialDenialLatch>,
}

impl<R: oio::Read> oio::Read for CredentialDenialStream<R> {
    async fn open(
        &self,
        range: opendal::BytesRange,
    ) -> opendal::Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let (reply, inner) = self
            .inner
            .open(range)
            .await
            .map_err(|error| self.denial.classify(error))?;
        let stream: Box<dyn oio::ReadStreamDyn> = Box::new(CredentialDenialStream {
            inner,
            denial: Arc::clone(&self.denial),
        });
        Ok((reply, stream))
    }

    async fn read(&self, range: opendal::BytesRange) -> opendal::Result<(RpRead, Buffer)> {
        self.inner
            .read(range)
            .await
            .map_err(|error| self.denial.classify(error))
    }
}

impl<R: oio::ReadStream> oio::ReadStream for CredentialDenialStream<R> {
    async fn read(&mut self) -> opendal::Result<Buffer> {
        self.inner
            .read()
            .await
            .map_err(|error| self.denial.classify(error))
    }
}

impl<W: oio::Write> oio::Write for CredentialDenialStream<W> {
    async fn write(&mut self, buffer: Buffer) -> opendal::Result<()> {
        self.inner
            .write(buffer)
            .await
            .map_err(|error| self.denial.classify(error))
    }

    async fn close(&mut self) -> opendal::Result<Metadata> {
        self.inner
            .close()
            .await
            .map_err(|error| self.denial.classify(error))
    }

    async fn abort(&mut self) -> opendal::Result<()> {
        self.inner
            .abort()
            .await
            .map_err(|error| self.denial.classify(error))
    }
}

impl<L: oio::List> oio::List for CredentialDenialStream<L> {
    async fn next(&mut self) -> opendal::Result<Option<oio::Entry>> {
        self.inner
            .next()
            .await
            .map_err(|error| self.denial.classify(error))
    }
}

impl<D: oio::Delete> oio::Delete for CredentialDenialStream<D> {
    async fn delete<'a>(&'a mut self, path: &'a str, args: OpDelete) -> opendal::Result<()> {
        self.inner
            .delete(path, args)
            .await
            .map_err(|error| self.denial.classify(error))
    }

    async fn close(&mut self) -> opendal::Result<()> {
        self.inner
            .close()
            .await
            .map_err(|error| self.denial.classify(error))
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

fn resolve_hdfs_locations(
    access_domain: StorageAccessDomainId,
    locations: Vec<FsLocation>,
    runtime: Option<&HdfsRuntimeView>,
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
    // Parse/identity refusals stay unchanged. Missing execution custody is
    // refused before looking at any cached operator or constructing a client.
    let runtime = runtime.ok_or_else(|| {
        FileError::new(
            FileErrorKind::Internal,
            "HDFS execution requires an explicitly composed role runtime",
        )
    })?;
    let operator = build_hdfs_operator(runtime, &first.name_node, first.user.as_deref())?;
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
    parse_hdfs_selection(raw, true)
}

fn parse_hdfs_selection(raw: &str, require_file: bool) -> FileResult<HdfsPath> {
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
    if require_file && relative_path.is_empty() {
        return Err(FileError::invalid(
            "hdfs path points to namenode root and cannot be used as file path",
        ));
    }
    let user = if url.username().is_empty() {
        None
    } else {
        Some(
            percent_encoding::percent_decode_str(url.username())
                .decode_utf8()
                .map_err(|error| {
                    FileError::with_source(FileErrorKind::Invalid, "decode HDFS user", error)
                })?
                .into_owned(),
        )
    };
    Ok(HdfsPath {
        name_node: format!("hdfs://{authority}"),
        user,
        relative_path,
    })
}

fn build_hdfs_operator(
    owner: &HdfsRuntimeView,
    name_node: &str,
    user: Option<&str>,
) -> FileResult<Operator> {
    let key = HdfsOperatorKey {
        name_node: name_node.to_string(),
        user: user.map(str::to_string),
    };
    if let Some(operator) = owner
        .operators
        .lock()
        .map_err(|_| FileError::new(FileErrorKind::Internal, "lock hdfs operator cache"))?
        .get(&key)
        .cloned()
    {
        return Ok(operator);
    }
    // Client construction is outside the original cache mutex. The service
    // consumes this exact role Handle through hdfs-native's public builder.
    let builder =
        crate::hdfs_service::HdfsServiceBuilder::new(name_node, user, owner.runtime.clone());
    let operator = Operator::new(builder)
        .map_err(|error| map_opendal_error("initialize hdfs operator", error))?;
    let selected = {
        let mut operators = owner
            .operators
            .lock()
            .map_err(|_| FileError::new(FileErrorKind::Internal, "lock hdfs operator cache"))?;
        // Concurrent misses may finish together. Keep the first published
        // instance and destroy the redundant local instance outside the lock.
        operators
            .entry(key)
            .or_insert_with(|| operator.clone())
            .clone()
    };
    drop(operator);
    Ok(selected)
}

/// Recover only an already projected FS acquisition cause through the actual
/// file/signing/stream wrappers. This bound does not change ordinary error
/// classification or OpenDAL's retry predicate.
fn authority_cause_in(error: &(dyn std::error::Error + 'static)) -> Option<SharedFailureCause> {
    const MAX_SOURCE_DEPTH: usize = 16;
    let mut current = error;
    for _ in 0..MAX_SOURCE_DEPTH {
        if let Some(file) = current.downcast_ref::<FileError>() {
            if let Some(cause) = file.authority_cause() {
                return Some(cause.clone());
            }
        } else if let Some(cause) = current.downcast_ref::<SharedFailureCause>() {
            return cause
                .downcast_ref::<crate::storage_authority::failure::AuthorityFailure>()
                .map(|_| cause.clone());
        } else if let Some(io) = current.downcast_ref::<std::io::Error>() {
            // io::Error owns the OpenDAL error as its custom payload. Use that
            // payload explicitly rather than depending on its source shortcut.
            current = io.get_ref()?;
            continue;
        } else if !current.is::<opendal::Error>() {
            return None;
        }
        current = current.source()?;
    }
    None
}

fn carry_authority_cause(error: FileError, cause: Option<SharedFailureCause>) -> FileError {
    match cause {
        Some(cause) => error.with_authority_cause(cause),
        None => error,
    }
}

fn map_opendal_error(operation: &str, error: opendal::Error) -> FileError {
    let kind = classify_opendal_error(&error);
    let cause = authority_cause_in(&error);
    carry_authority_cause(FileError::with_source(kind, operation, error), cause)
}

/// Bound the diagnostic we create at the catalog listing boundary. OpenDAL
/// 0.55 exposes its kind but no borrowed message accessor; its Display builds
/// context Vecs/Strings and may invoke arbitrary source Display. Preserve the
/// classification and bounded operation, without retaining/rendering that
/// opaque source. Generic file operations retain their existing cause chain.
pub fn map_object_store_listing_error(error: opendal::Error) -> FileError {
    map_bounded_listing_opendal_error("list object-store entries", error)
}

fn map_bounded_listing_opendal_error(operation: &str, error: opendal::Error) -> FileError {
    let cause = authority_cause_in(&error);
    carry_authority_cause(
        bounded_listing_failure(classify_opendal_error(&error), operation),
        cause,
    )
}

fn bounded_listing_failure(kind: FileErrorKind, message: &str) -> FileError {
    const DIAGNOSTIC_BYTES: usize = 4 * 1024;
    let message = if message.len() <= DIAGNOSTIC_BYTES {
        message
    } else {
        "bounded catalog listing storage failure"
    };
    FileError::new(kind, message)
}

fn map_stream_error(operation: &str, error: std::io::Error) -> FileError {
    let kind = error
        .get_ref()
        .and_then(|source| source.downcast_ref::<opendal::Error>())
        .map(classify_opendal_error)
        .unwrap_or_else(|| match error.kind() {
            std::io::ErrorKind::NotFound => FileErrorKind::NotFound,
            std::io::ErrorKind::PermissionDenied => FileErrorKind::Permission,
            _ => FileErrorKind::Transient,
        });
    let cause = authority_cause_in(&error);
    carry_authority_cause(FileError::with_source(kind, operation, error), cause)
}

fn classify_opendal_error(error: &opendal::Error) -> FileErrorKind {
    if is_list_body_limit_exceeded(error) {
        FileErrorKind::ResourceExhausted
    } else {
        file_error_kind_for_opendal(error.kind())
    }
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
    // FS owns complete installation visibility; these tests use isolated local
    // directories and join every writer. They supplement conformance's public
    // API checks with pauses/failures inside the private whole-file writer.
    #[test]
    fn conditional_local_install_exposes_only_complete_payload_and_excludes_temporary_name() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("v1.metadata.json");
        let outcome = install_local_file(&target, &FileCancellation::new(), |file| {
            file.write_all(b"first-").unwrap();
            assert!(!target.exists(), "a partial write must have no target name");
            let names = std::fs::read_dir(directory.path())
                .unwrap()
                .map(|entry| entry.unwrap().file_name().into_string().unwrap())
                .collect::<Vec<_>>();
            assert_eq!(names.len(), 1);
            assert!(!names[0].starts_with('v'));
            assert!(!names[0].ends_with(".metadata.json"));
            file.write_all(b"payload").unwrap();
            Ok(())
        })
        .unwrap();
        assert_eq!(outcome, ConditionalCreateOutcome::Created);
        assert_eq!(std::fs::read(&target).unwrap(), b"first-payload");
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
        assert_eq!(
            install_local_file(&target, &FileCancellation::new(), |file| {
                file.write_all(b"replacement").unwrap();
                Ok(())
            })
            .unwrap(),
            ConditionalCreateOutcome::AlreadyExists
        );
        assert_eq!(std::fs::read(&target).unwrap(), b"first-payload");
    }

    #[test]
    fn conditional_local_install_preserves_write_failure_and_removes_partial_file() {
        use crate::storage_authority::failure::AuthorityFailureReason;
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("v1.metadata.json");
        let original = FileError::authority(
            FileErrorKind::Permission,
            AuthorityFailureReason::AcquisitionDenied,
            "original conditional write failure",
        );
        let cause = original.authority_cause().unwrap().clone();
        let error = install_local_file(&target, &FileCancellation::new(), |file| {
            file.write_all(b"partial").unwrap();
            Err(original)
        })
        .unwrap_err();
        assert_eq!(error.kind(), FileErrorKind::Permission);
        assert_eq!(error.authority_cause(), Some(&cause));
        assert_authority_fact(&error, 2);
        assert!(
            error
                .to_string()
                .contains("original conditional write failure")
        );
        assert!(!target.exists());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[test]
    fn conditional_local_install_stop_waits_for_actual_writer_exit_before_returning() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("v1.metadata.json");
        let cancellation = FileCancellation::new();
        let (entered_send, entered) = std::sync::mpsc::channel();
        let (resume, resume_receive) = std::sync::mpsc::channel();
        let (finished_send, finished) = std::sync::mpsc::channel();
        let writer_target = target.clone();
        let writer_cancellation = cancellation.clone();
        let writer = std::thread::spawn(move || {
            let result = install_local_file(&writer_target, &writer_cancellation, |file| {
                file.write_all(b"partial").unwrap();
                entered_send.send(()).unwrap();
                resume_receive.recv_timeout(Duration::from_secs(5)).unwrap();
                file.write_all(b"-complete").unwrap();
                Ok(())
            });
            finished_send.send(result).unwrap();
        });
        let entered_result = entered.recv_timeout(Duration::from_secs(5));
        cancellation.cancel();
        let target_was_absent = !target.exists();
        let pending = matches!(
            finished.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        );
        let writer_was_running = !writer.is_finished();
        // Release and join even when an earlier observation failed, so a test
        // assertion cannot abandon the writer or its private temporary file.
        let resumed = resume.send(());
        let result = finished.recv_timeout(Duration::from_secs(5));
        let joined = writer.join();
        entered_result.unwrap();
        resumed.unwrap();
        joined.unwrap();
        assert!(target_was_absent);
        assert!(pending);
        assert!(
            writer_was_running,
            "stop cannot release an in-flight synchronous writer"
        );
        let error = result.unwrap().unwrap_err();
        assert_eq!(error.kind(), FileErrorKind::Cancelled);
        assert!(!target.exists());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[test]
    fn conditional_local_install_expired_deadline_never_opens_temporary_file() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("v1.metadata.json");
        let cancellation = FileCancellation::new().with_deadline(Some(Instant::now()));
        let error = install_local_file(&target, &cancellation, |_| {
            panic!("an expired installation must never invoke the writer")
        })
        .unwrap_err();
        assert_eq!(error.kind(), FileErrorKind::DeadlineExceeded);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[derive(Clone, Debug)]
    struct ConditionalCompletion {
        cancellation: FileCancellation,
        failure: Option<opendal::ErrorKind>,
        calls: Arc<AtomicUsize>,
    }

    impl Layer for ConditionalCompletion {
        fn apply_service(&self, inner: Servicer) -> Servicer {
            Arc::new(ConditionalCompletionAccess {
                inner,
                completion: self.clone(),
            })
        }
    }

    #[derive(Debug)]
    struct ConditionalCompletionAccess {
        inner: Servicer,
        completion: ConditionalCompletion,
    }

    struct ConditionalCompletionWriter<W> {
        inner: W,
        completion: ConditionalCompletion,
    }

    impl<W: oio::Write> oio::Write for ConditionalCompletionWriter<W> {
        async fn write(&mut self, buffer: Buffer) -> opendal::Result<()> {
            self.inner.write(buffer).await
        }
        async fn close(&mut self) -> opendal::Result<Metadata> {
            let ack = self.inner.close().await?;
            self.completion.calls.fetch_add(1, Ordering::SeqCst);
            // The operation completes and stop becomes visible in the same poll.
            self.completion.cancellation.cancel();
            if let Some(kind) = self.completion.failure {
                let error = FileError::authority(
                    FileErrorKind::Permission,
                    crate::storage_authority::failure::AuthorityFailureReason::AcquisitionDenied,
                    "original conditional object failure",
                );
                return Err(opendal::Error::new(kind, "conditional write failed").set_source(error));
            }
            Ok(ack)
        }
        async fn abort(&mut self) -> opendal::Result<()> {
            self.inner.abort().await
        }
    }

    impl Service for ConditionalCompletionAccess {
        type Reader = oio::Reader;
        type Writer = ConditionalCompletionWriter<oio::Writer>;
        type Lister = oio::Lister;
        type Deleter = oio::Deleter;
        type Copier = oio::Copier;

        fn info(&self) -> ServiceInfo {
            self.inner.info()
        }

        fn capability(&self) -> opendal::Capability {
            // This layer models a condition-capable store's terminal ACK/error.
            let mut capability = self.inner.capability();
            capability.write_with_if_not_exists = true;
            capability
        }

        async fn create_dir(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpCreateDir,
        ) -> opendal::Result<RpCreateDir> {
            self.inner.create_dir(ctx, path, args).await
        }

        async fn stat(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpStat,
        ) -> opendal::Result<RpStat> {
            self.inner.stat(ctx, path, args).await
        }

        fn read(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpRead,
        ) -> opendal::Result<Self::Reader> {
            self.inner.read(ctx, path, args)
        }

        fn write(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpWrite,
        ) -> opendal::Result<Self::Writer> {
            assert!(args.if_not_exists());
            self.inner
                .write(ctx, path, args)
                .map(|inner| ConditionalCompletionWriter {
                    inner,
                    completion: self.completion.clone(),
                })
        }

        fn delete(&self, ctx: &OperationContext) -> opendal::Result<Self::Deleter> {
            self.inner.delete(ctx)
        }

        fn list(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpList,
        ) -> opendal::Result<Self::Lister> {
            self.inner.list(ctx, path, args)
        }

        fn copy(
            &self,
            ctx: &OperationContext,
            from: &str,
            to: &str,
            args: OpCopy,
            opts: OpCopier,
        ) -> opendal::Result<Self::Copier> {
            self.inner.copy(ctx, from, to, args, opts)
        }

        async fn rename(
            &self,
            ctx: &OperationContext,
            from: &str,
            to: &str,
            args: OpRename,
        ) -> opendal::Result<RpRename> {
            self.inner.rename(ctx, from, to, args).await
        }

        async fn presign(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpPresign,
        ) -> opendal::Result<RpPresign> {
            self.inner.presign(ctx, path, args).await
        }
    }

    #[tokio::test]
    async fn conditional_object_completion_keeps_ack_condition_and_typed_error_after_stop() {
        for failure in [
            None,
            Some(opendal::ErrorKind::AlreadyExists),
            Some(opendal::ErrorKind::PermissionDenied),
        ] {
            let cancellation = FileCancellation::new();
            let completion = ConditionalCompletion {
                cancellation: cancellation.clone(),
                failure,
                calls: Arc::new(AtomicUsize::new(0)),
            };
            let operator = memory_operator().unwrap().layer(completion.clone());
            let access = FsAccessHandle::new(
                domain(86),
                FsScheme::ObjectStore,
                operator,
                Some("bucket".into()),
                None,
                vec![
                    ResolvedFsPath::new(
                        FsLocation::parse("s3://bucket/v1.metadata.json").unwrap(),
                        "v1.metadata.json",
                    )
                    .unwrap(),
                ],
            );
            let result = access
                .create_if_absent(0, Bytes::from_static(b"complete-payload"), &cancellation)
                .await;
            assert_eq!(
                completion.calls.load(Ordering::SeqCst),
                1,
                "terminal operation seam was reached"
            );
            assert!(cancellation.is_cancelled());
            match failure {
                None => assert_eq!(result.unwrap(), ConditionalCreateOutcome::Created),
                Some(opendal::ErrorKind::AlreadyExists) => {
                    assert_eq!(result.unwrap(), ConditionalCreateOutcome::AlreadyExists)
                }
                _ => {
                    let error = result.unwrap_err();
                    assert_eq!(error.kind(), FileErrorKind::Permission);
                    assert_authority_fact(&error, 2);
                    assert!(
                        error
                            .to_string()
                            .contains("original conditional object failure")
                    );
                }
            }
        }
    }

    #[test]
    fn list_body_limit_error_remains_resource_exhausted_in_all_fs_mappings() {
        fn overflow() -> opendal::Error {
            opendal::Error::new(opendal::ErrorKind::Unexpected, "bounded list body")
                .set_source(crate::list_body_limit::ListBodyLimitExceeded)
        }
        assert_eq!(
            map_opendal_error("list", overflow()).kind(),
            FileErrorKind::ResourceExhausted
        );
        assert_eq!(
            map_bounded_listing_opendal_error("list", overflow()).kind(),
            FileErrorKind::ResourceExhausted
        );
        assert_eq!(
            map_stream_error("list", std::io::Error::other(overflow())).kind(),
            FileErrorKind::ResourceExhausted
        );
    }

    use std::sync::Barrier;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceId, StorageCredentialScopePrefix,
    };

    use super::*;
    use crate::storage_authority::{
        AcquisitionFailure, AuthorityCapabilityPath, AuthorityMaterial, AuthorityMaterialSource,
        RefreshExecutor, RefreshPolicy,
    };

    fn domain(value: u8) -> StorageAccessDomainId {
        StorageAccessDomainId::from_bytes([value; 32])
    }

    #[derive(Debug)]
    struct ListingPanicDisplay;
    impl std::fmt::Display for ListingPanicDisplay {
        fn fmt(&self, _formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            panic!("bounded listing must never display its opaque source")
        }
    }
    impl std::error::Error for ListingPanicDisplay {}

    #[test]
    fn bounded_listing_diagnostic_never_displays_source_or_huge_context() {
        for (kind, expected) in [
            (opendal::ErrorKind::NotFound, FileErrorKind::NotFound),
            (
                opendal::ErrorKind::PermissionDenied,
                FileErrorKind::Permission,
            ),
            (opendal::ErrorKind::Unsupported, FileErrorKind::Unsupported),
            (opendal::ErrorKind::Unexpected, FileErrorKind::Transient),
        ] {
            let error = opendal::Error::new(kind, "x".repeat(8192))
                .with_context("path", "y".repeat(8192))
                .set_source(ListingPanicDisplay);
            let mapped = map_bounded_listing_opendal_error("list files", error);
            assert_eq!(mapped.kind(), expected);
            assert!(std::error::Error::source(&mapped).is_none());
            assert!(mapped.to_string().len() < 128);
            // Both Paimon Unsupported's to_string and the generic FileError ->
            // ConnectorError conversion now see only this bounded FileError.
            assert!(
                novarocks_spi::connector::ConnectorError::from(&mapped)
                    .message()
                    .len()
                    < 128
            );
        }
    }

    #[test]
    fn bounded_listing_diagnostic_checks_borrowed_bytes_before_copy() {
        let exact = "é".repeat(2048);
        let mapped = bounded_listing_failure(FileErrorKind::Transient, &exact);
        assert!(mapped.to_string().ends_with(&exact));
        let oversized = format!("{exact}x");
        let mapped = bounded_listing_failure(FileErrorKind::Transient, &oversized);
        assert_eq!(mapped.kind(), FileErrorKind::Transient);
        assert!(mapped.to_string().len() < 128);
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

    // Explicit fixture policy for tests that deliberately bypass the pool.
    fn test_s3_http_client() -> reqwest::Client {
        reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap()
    }

    fn static_reference(generation: &str) -> StaticCredentialReference {
        StaticCredentialReference::try_new("warehouse-data", generation).unwrap()
    }

    fn static_source(secret_suffix: &str) -> ObjectStoreCredentialSource {
        ObjectStoreCredentialSource::Static {
            reference: static_reference("blue"),
            material: secret_material(secret_suffix),
        }
    }

    fn authority_id(scope: &str) -> StorageAuthorityId {
        StorageAuthorityId::new(
            CatalogHandle::new(
                ConnectorInstanceId::parse("lake").unwrap(),
                CatalogVersion::from_bytes([0x11; 32]),
            ),
            StorageCredentialScopePrefix::try_from_normalized(scope).unwrap(),
            AuthorityCapabilityPath::SeededWithoutRenewal { grant: None },
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

    #[derive(Clone, Debug, Default)]
    struct ListingRecord {
        pending_first_poll: Arc<AtomicUsize>,
        pending_next: Arc<AtomicUsize>,
        first_poll_future_drops: Arc<AtomicUsize>,
        next_future_drops: Arc<AtomicUsize>,
        stats: Arc<AtomicUsize>,
        stat_failure: Arc<AtomicUsize>,
        requests: Arc<Mutex<Vec<Option<usize>>>>,
        polls: Arc<AtomicUsize>,
        first_polls: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }
    impl Layer for ListingRecord {
        fn apply_service(&self, inner: Servicer) -> Servicer {
            Arc::new(RecordingListAccess {
                inner,
                record: self.clone(),
            })
        }
    }
    #[derive(Debug)]
    struct RecordingListAccess {
        inner: Servicer,
        record: ListingRecord,
    }
    struct RecordedLister<L> {
        inner: L,
        record: ListingRecord,
        first_poll: bool,
    }
    struct PendingListingFutureDrop(Arc<AtomicUsize>);
    impl Drop for PendingListingFutureDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl<L> Drop for RecordedLister<L> {
        fn drop(&mut self) {
            self.record.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl<L: oio::List> oio::List for RecordedLister<L> {
        async fn next(&mut self) -> opendal::Result<Option<oio::Entry>> {
            self.record.polls.fetch_add(1, Ordering::SeqCst);
            if std::mem::replace(&mut self.first_poll, false) {
                self.record.first_polls.fetch_add(1, Ordering::SeqCst);
                if self.record.pending_first_poll.load(Ordering::SeqCst) != 0 {
                    let _hold =
                        PendingListingFutureDrop(Arc::clone(&self.record.first_poll_future_drops));
                    return std::future::pending::<opendal::Result<Option<oio::Entry>>>().await;
                }
            } else if self.record.pending_next.load(Ordering::SeqCst) != 0 {
                let _hold = PendingListingFutureDrop(Arc::clone(&self.record.next_future_drops));
                return std::future::pending::<opendal::Result<Option<oio::Entry>>>().await;
            }
            self.inner.next().await
        }
    }
    impl Service for RecordingListAccess {
        type Reader = oio::Reader;
        type Writer = oio::Writer;
        type Lister = RecordedLister<oio::Lister>;
        type Deleter = oio::Deleter;
        type Copier = oio::Copier;

        fn info(&self) -> ServiceInfo {
            self.inner.info()
        }

        fn capability(&self) -> opendal::Capability {
            self.inner.capability()
        }

        async fn create_dir(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpCreateDir,
        ) -> opendal::Result<RpCreateDir> {
            self.inner.create_dir(ctx, path, args).await
        }

        fn read(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpRead,
        ) -> opendal::Result<Self::Reader> {
            self.inner.read(ctx, path, args)
        }

        fn write(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpWrite,
        ) -> opendal::Result<Self::Writer> {
            self.inner.write(ctx, path, args)
        }

        fn delete(&self, ctx: &OperationContext) -> opendal::Result<Self::Deleter> {
            self.inner.delete(ctx)
        }

        async fn stat(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpStat,
        ) -> opendal::Result<RpStat> {
            self.record.stats.fetch_add(1, Ordering::SeqCst);
            if self.record.stat_failure.load(Ordering::SeqCst) != 0 {
                return Err(opendal::Error::new(
                    opendal::ErrorKind::PermissionDenied,
                    "x".repeat(8192),
                )
                .with_context("path", "y".repeat(8192))
                .set_source(ListingPanicDisplay));
            }
            self.inner.stat(ctx, path, args).await
        }

        fn list(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpList,
        ) -> opendal::Result<Self::Lister> {
            self.record.requests.lock().unwrap().push(args.limit());
            self.inner
                .list(ctx, path, args)
                .map(|inner| RecordedLister {
                    inner,
                    record: self.record.clone(),
                    first_poll: true,
                })
        }

        fn copy(
            &self,
            ctx: &OperationContext,
            from: &str,
            to: &str,
            args: OpCopy,
            opts: OpCopier,
        ) -> opendal::Result<Self::Copier> {
            self.inner.copy(ctx, from, to, args, opts)
        }

        async fn rename(
            &self,
            ctx: &OperationContext,
            from: &str,
            to: &str,
            args: OpRename,
        ) -> opendal::Result<RpRename> {
            self.inner.rename(ctx, from, to, args).await
        }

        async fn presign(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpPresign,
        ) -> opendal::Result<RpPresign> {
            self.inner.presign(ctx, path, args).await
        }
    }

    fn listing_access(operator: Operator) -> FsAccessHandle {
        FsAccessHandle::new(
            domain(84),
            FsScheme::ObjectStore,
            operator,
            Some("bucket".to_owned()),
            None,
            Vec::new(),
        )
    }
    fn listing_bound(path_bytes: usize) -> FsListingBound {
        FsListingBound::try_new(1, path_bytes, 32 * 1024 * 1024).unwrap()
    }

    #[tokio::test]
    async fn bounded_listing_pending_first_poll_stop_and_deadline_drop_state_then_eof() {
        for deadline in [false, true] {
            let record = ListingRecord::default();
            record.pending_first_poll.store(1, Ordering::SeqCst);
            let access = listing_access(memory_operator().unwrap().layer(record.clone()));
            let cancellation = if deadline {
                FileCancellation::new()
                    .with_deadline(Some(Instant::now() + Duration::from_millis(100)))
            } else {
                FileCancellation::new()
            };
            let mut opening = Box::pin(access.list_location_bounded(
                "s3://bucket/warehouse",
                false,
                &cancellation,
                listing_bound(64),
            ));
            // OpenDAL 0.58 creates the lister synchronously. The first async
            // listing boundary is the returned lister's actual next poll.
            let mut stream = match futures::poll!(opening.as_mut()) {
                std::task::Poll::Ready(Ok(stream)) => stream,
                std::task::Poll::Ready(Err(error)) => panic!("lister creation failed: {error}"),
                std::task::Poll::Pending => panic!("synchronous SDK lister creation suspended"),
            };
            drop(opening);
            assert_eq!(*record.requests.lock().unwrap(), [Some(1)]);
            assert_eq!(record.polls.load(Ordering::SeqCst), 0);
            assert_eq!(record.drops.load(Ordering::SeqCst), 0);
            let mut next = Box::pin(stream.next());
            assert!(matches!(
                futures::poll!(next.as_mut()),
                std::task::Poll::Pending
            ));
            assert_eq!(record.polls.load(Ordering::SeqCst), 1);
            assert_eq!(record.first_polls.load(Ordering::SeqCst), 1);
            assert_eq!(record.first_poll_future_drops.load(Ordering::SeqCst), 0);
            assert_eq!(record.next_future_drops.load(Ordering::SeqCst), 0);
            assert_eq!(record.drops.load(Ordering::SeqCst), 0);
            if !deadline {
                cancellation.cancel();
            }
            let error = tokio::time::timeout(Duration::from_secs(2), next.as_mut())
                .await
                .expect("pending first SDK poll must wake on stop/deadline")
                .expect("typed terminal error")
                .unwrap_err();
            assert_eq!(
                error.kind(),
                if deadline {
                    FileErrorKind::DeadlineExceeded
                } else {
                    FileErrorKind::Cancelled
                }
            );
            drop(next);
            assert_eq!(record.first_poll_future_drops.load(Ordering::SeqCst), 1);
            assert_eq!(record.next_future_drops.load(Ordering::SeqCst), 0);
            assert_eq!(
                record.drops.load(Ordering::SeqCst),
                1,
                "the constructed lister exits with the terminal error"
            );
            assert!(stream.next().await.is_none());
            assert_eq!(record.polls.load(Ordering::SeqCst), 1);
            assert_eq!(record.first_polls.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn bounded_listing_pending_next_stop_and_deadline_drop_state_then_eof() {
        for deadline in [false, true] {
            let record = ListingRecord::default();
            record.pending_next.store(1, Ordering::SeqCst);
            let operator = memory_operator().unwrap().layer(record.clone());
            operator.write("warehouse/a", "a").await.unwrap();
            operator.write("warehouse/b", "b").await.unwrap();
            let access = listing_access(operator);
            let cancellation = if deadline {
                FileCancellation::new()
                    .with_deadline(Some(Instant::now() + Duration::from_millis(100)))
            } else {
                FileCancellation::new()
            };
            let mut stream = access
                .list_location_bounded(
                    "s3://bucket/warehouse",
                    false,
                    &cancellation,
                    listing_bound(64),
                )
                .await
                .unwrap();
            assert_eq!(*record.requests.lock().unwrap(), [Some(1)]);
            assert_eq!(record.polls.load(Ordering::SeqCst), 0);
            let first = stream.next().await.unwrap().unwrap();
            assert_eq!(first.location, "s3://bucket/warehouse/a");
            assert!(!first.is_dir);
            assert_eq!(record.polls.load(Ordering::SeqCst), 1);
            assert_eq!(record.first_polls.load(Ordering::SeqCst), 1);
            assert_eq!(record.first_poll_future_drops.load(Ordering::SeqCst), 0);
            assert_eq!(record.next_future_drops.load(Ordering::SeqCst), 0);
            assert_eq!(record.drops.load(Ordering::SeqCst), 0);
            let mut next = Box::pin(stream.next());
            assert!(matches!(
                futures::poll!(next.as_mut()),
                std::task::Poll::Pending
            ));
            assert_eq!(
                record.polls.load(Ordering::SeqCst),
                2,
                "the SDK continuation actually started after a real entry"
            );
            assert_eq!(record.first_polls.load(Ordering::SeqCst), 1);
            assert_eq!(record.next_future_drops.load(Ordering::SeqCst), 0);
            if !deadline {
                cancellation.cancel();
            }
            let error = tokio::time::timeout(Duration::from_secs(2), next.as_mut())
                .await
                .expect("pending SDK next must wake on stop/deadline")
                .expect("typed terminal error")
                .unwrap_err();
            assert_eq!(
                error.kind(),
                if deadline {
                    FileErrorKind::DeadlineExceeded
                } else {
                    FileErrorKind::Cancelled
                }
            );
            drop(next);
            assert_eq!(record.first_poll_future_drops.load(Ordering::SeqCst), 0);
            assert_eq!(record.next_future_drops.load(Ordering::SeqCst), 1);
            assert_eq!(
                record.drops.load(Ordering::SeqCst),
                1,
                "lister/context state exits with the terminal error"
            );
            assert!(stream.next().await.is_none());
            assert_eq!(
                record.polls.load(Ordering::SeqCst),
                2,
                "EOF must not poll the dropped lister"
            );
            assert_eq!(record.first_polls.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn bounded_stat_ignores_unrelated_inventory_and_keeps_no_bound_file() {
        let record = ListingRecord::default();
        let operator = memory_operator().unwrap().layer(record.clone());
        operator
            .write("warehouse/schema-0", "schema")
            .await
            .unwrap();
        let mut access = listing_access(operator);
        // These owner-held paths are outside the one bounded probe. Copying
        // this inventory would exceed its explicit 1MiB temporary allowance.
        for _ in 0..4 {
            access.paths.push(ResolvedFsPath {
                location: FsLocation::parse("s3://bucket/warehouse").unwrap(),
                operator_relative_path: "x".repeat(512 * 1024),
            });
        }
        let paths_ptr = access.paths.as_ptr();
        let size = access
            .stat_location_bounded(
                "s3://bucket/warehouse/schema-0",
                &FileCancellation::new(),
                FsListingBound::try_new(1, 64, 1024 * 1024).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(size, 6);
        assert_eq!(access.paths.as_ptr(), paths_ptr);
        assert_eq!(record.stats.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn bounded_stat_long_schema_and_liveness_refuse_before_io() {
        let record = ListingRecord::default();
        let access = listing_access(memory_operator().unwrap().layer(record.clone()));
        let long = format!("s3://bucket/warehouse/{}/schema-0", "x".repeat(128));
        let expired =
            FileCancellation::new().with_deadline(Some(Instant::now() - Duration::from_secs(1)));
        let cancelled = FileCancellation::new();
        cancelled.cancel();
        for (path, cancellation, kind) in [
            (
                long.as_str(),
                FileCancellation::new(),
                FileErrorKind::ResourceExhausted,
            ),
            (
                "s3://bucket/warehouse/schema-0",
                expired,
                FileErrorKind::DeadlineExceeded,
            ),
            (
                "s3://bucket/warehouse/schema-0",
                cancelled,
                FileErrorKind::Cancelled,
            ),
        ] {
            let error = access
                .stat_location_bounded(path, &cancellation, listing_bound(64))
                .await
                .unwrap_err();
            assert_eq!(error.kind(), kind);
        }
        assert_eq!(record.stats.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn bounded_stat_not_found_and_opaque_source_keep_typed_bounded_diagnostics() {
        let record = ListingRecord::default();
        let access = listing_access(memory_operator().unwrap().layer(record.clone()));
        let path = "s3://bucket/warehouse/schema-0";
        let error = access
            .stat_location_bounded(path, &FileCancellation::new(), listing_bound(64))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), FileErrorKind::NotFound);
        assert!(std::error::Error::source(&error).is_none());
        record.stat_failure.store(1, Ordering::SeqCst);
        let error = access
            .stat_location_bounded(path, &FileCancellation::new(), listing_bound(64))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), FileErrorKind::Permission);
        assert!(
            novarocks_spi::connector::ConnectorError::from(&error)
                .message()
                .len()
                < 128
        );
    }

    #[test]
    fn bounded_listing_uri_matches_old_bytes_and_exact_capacity() {
        for (scheme, uri_scheme, authority, root, path) in [
            (
                FsScheme::ObjectStore,
                Some("s3a"),
                Some("bucket"),
                None,
                "/warehouse/表/",
            ),
            (
                FsScheme::Hdfs,
                Some("hdfs"),
                Some("host:9000"),
                None,
                "warehouse/t",
            ),
            (FsScheme::Local, None, None, Some("/tmp/root/"), "table/"),
            (FsScheme::Local, None, None, Some("."), "table/"),
            (
                FsScheme::Local,
                None,
                None,
                Some("/tmp/root"),
                "/absolute/table",
            ),
        ] {
            let old = if scheme == FsScheme::Local {
                match root {
                    Some(root) if root != "." => Path::new(root).join(path),
                    _ => PathBuf::from(path),
                }
                .to_string_lossy()
                .into_owned()
            } else {
                format!(
                    "{}://{}/{}",
                    uri_scheme.unwrap(),
                    authority.unwrap(),
                    path.trim_start_matches('/')
                )
            };
            let context = ListingUriContext {
                scheme,
                uri_scheme: uri_scheme.map(str::to_owned),
                authority: authority.map(str::to_owned),
                local_root: root.map(str::to_owned),
                bounds: listing_bound(old.len()),
            };
            let actual = context.render(path).unwrap();
            assert_eq!(actual, old);
            assert_eq!(actual.capacity(), actual.len());
        }
    }

    #[test]
    fn listing_entry_transfers_the_same_backing_without_copy() {
        let mut location = String::with_capacity(128);
        location.push_str("s3://bucket/warehouse/a");
        let pointer = location.as_ptr();
        let capacity = location.capacity();
        let (moved, size, is_dir) = FsListEntry {
            location,
            size: 7,
            is_dir: false,
        }
        .into_parts();
        assert_eq!(moved.as_ptr(), pointer);
        assert_eq!(moved.capacity(), capacity);
        assert_eq!((size, is_dir), (7, false));
    }

    #[test]
    fn bounded_listing_uri_refuses_composed_size_before_copy() {
        let context = ListingUriContext {
            scheme: FsScheme::ObjectStore,
            uri_scheme: Some("s3".to_owned()),
            authority: Some("bucket".to_owned()),
            local_root: None,
            bounds: listing_bound(16),
        };
        assert_eq!(
            context.render("warehouse/name").unwrap_err().kind(),
            FileErrorKind::ResourceExhausted
        );
        assert_eq!(
            bounded_listing_string(usize::MAX, listing_bound(64))
                .unwrap_err()
                .kind(),
            FileErrorKind::ResourceExhausted
        );
    }

    #[tokio::test]
    async fn bounded_listing_requests_pages_and_keeps_complete_generic_semantics() {
        let record = ListingRecord::default();
        let operator = memory_operator().unwrap().layer(record.clone());
        operator.write("warehouse/a", "a").await.unwrap();
        operator.write("warehouse/b", "b").await.unwrap();
        let access = listing_access(operator);
        let cancellation = FileCancellation::new();
        let bounded = access
            .list_location_bounded(
                "s3://bucket/warehouse",
                false,
                &cancellation,
                listing_bound(64),
            )
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<FileResult<Vec<_>>>()
            .unwrap();
        let generic = access
            .list_location("s3://bucket/warehouse", false, &cancellation)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<FileResult<Vec<_>>>()
            .unwrap();
        assert_eq!(bounded, generic);
        assert_eq!(bounded.len(), 2);
        assert_eq!(*record.requests.lock().unwrap(), [Some(1), None]);
    }

    #[tokio::test]
    async fn bounded_listing_rejects_borrowed_prefix_and_capacity_before_open() {
        let record = ListingRecord::default();
        let mut access = listing_access(memory_operator().unwrap().layer(record.clone()));
        let cancellation = FileCancellation::new();
        for (prefix, bounds) in [
            ("s3://bucket/warehouse-too-long", listing_bound(16)),
            (
                "s3://bucket/warehouse",
                FsListingBound::try_new(1, 64, 64).unwrap(),
            ),
        ] {
            let error = match access
                .list_location_bounded(prefix, false, &cancellation, bounds)
                .await
            {
                Ok(_) => panic!("oversized source accepted"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), FileErrorKind::ResourceExhausted);
        }
        let mut root = String::with_capacity(1024);
        root.push_str("root");
        access.root = Some(root);
        let error = match access
            .list_location_bounded(
                "s3://bucket/warehouse",
                false,
                &cancellation,
                listing_bound(64),
            )
            .await
        {
            Ok(_) => panic!("spare capacity accepted"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), FileErrorKind::ResourceExhausted);
        assert!(record.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn bounded_listing_stops_and_drops_after_streamed_uri_refusal() {
        let record = ListingRecord::default();
        let operator = memory_operator().unwrap().layer(record.clone());
        operator
            .write("warehouse/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "a")
            .await
            .unwrap();
        operator
            .write("warehouse/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "b")
            .await
            .unwrap();
        let access = listing_access(operator);
        let mut stream = access
            .list_location_bounded(
                "s3://bucket/warehouse",
                false,
                &FileCancellation::new(),
                listing_bound(32),
            )
            .await
            .unwrap();
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().kind(),
            FileErrorKind::ResourceExhausted
        );
        let polls = record.polls.load(Ordering::SeqCst);
        assert!(stream.next().await.is_none());
        assert_eq!(record.polls.load(Ordering::SeqCst), polls);
        assert_eq!(record.drops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn bounded_listing_preserves_authority_and_cancel_refusals() {
        let record = ListingRecord::default();
        let access = listing_access(memory_operator().unwrap().layer(record.clone()));
        let cancellation = FileCancellation::new();
        let error = match access
            .list_location_bounded(
                "s3://other/warehouse",
                false,
                &cancellation,
                listing_bound(64),
            )
            .await
        {
            Ok(_) => panic!("other authority accepted"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), FileErrorKind::Permission);
        let mut stream = access
            .list_location_bounded(
                "s3://bucket/warehouse",
                false,
                &cancellation,
                listing_bound(64),
            )
            .await
            .unwrap();
        cancellation.cancel();
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().kind(),
            FileErrorKind::Cancelled
        );
        assert!(stream.next().await.is_none());
        assert_eq!(record.polls.load(Ordering::SeqCst), 0);
    }

    fn memory_operator() -> FileResult<Operator> {
        Operator::new(opendal::services::Memory::default())
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
        pool.acquire(
            domain(1),
            "warehouse",
            &endpoint,
            &static_source("first"),
            |_, _| memory_operator(),
        )
        .unwrap();
        pool.acquire(
            domain(1),
            "warehouse",
            &endpoint,
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
        let first = seeded_authority(
            "s3://warehouse/sales/orders/",
            Some(vended_material(
                "first",
                Instant::now() + Duration::from_secs(900),
            )),
        );
        let other_scope = seeded_authority("s3://warehouse/sales/returns/", None);

        pool.acquire(
            domain(1),
            "warehouse",
            &endpoint,
            &ObjectStoreCredentialSource::Authority(Arc::clone(&first)),
            |_, source| {
                assert!(
                    matches!(source, ObjectStoreCredentialSource::Authority(owner)
                    if Arc::ptr_eq(owner, &first))
                );
                memory_operator()
            },
        )
        .unwrap();
        // Rotate the original authority, not another Arc with
        // the same identity. The resident signer must keep this exact owner.
        first.install_material(vended_material(
            "rotated",
            Instant::now() + Duration::from_secs(1800),
        ));
        pool.acquire(
            domain(1),
            "warehouse",
            &endpoint,
            &ObjectStoreCredentialSource::Authority(Arc::clone(&first)),
            |_, _| -> FileResult<Operator> {
                panic!("rotated credential material must not rebuild the operator")
            },
        )
        .unwrap();
        pool.acquire(
            domain(1),
            "warehouse",
            &endpoint,
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
    fn provider_pool_keeps_static_and_authority_sources_separate_in_both_orders() {
        for authority_first in [false, true] {
            let pool = ObjectStoreProviderPool::new_for_test(8, Duration::from_secs(60)).unwrap();
            let endpoint = endpoint_config("http://localhost:9000");
            let authority = seeded_authority("s3://warehouse/sales/orders/", None);
            let static_context = ObjectStoreAccessContext::for_static(
                endpoint.clone(),
                static_reference("blue"),
                secret_material("static"),
                &pool,
            );
            let authority_context = ObjectStoreAccessContext::for_authority(
                endpoint.clone(),
                Arc::clone(&authority),
                &pool,
            );
            let acquire = |context: &ObjectStoreAccessContext<'_>| {
                context.provider_pool.acquire(
                    domain(1),
                    "warehouse",
                    &context.endpoint_config,
                    &context.credentials,
                    |_, _| memory_operator(),
                )
            };
            let (static_operator, authority_operator) = if authority_first {
                let authority_operator = acquire(&authority_context).unwrap();
                let static_operator = acquire(&static_context).unwrap();
                (static_operator, authority_operator)
            } else {
                let static_operator = acquire(&static_context).unwrap();
                let authority_operator = acquire(&authority_context).unwrap();
                (static_operator, authority_operator)
            };
            assert!(
                !Arc::ptr_eq(
                    static_operator.base_service(),
                    authority_operator.base_service()
                ),
                "a static context cannot hit an authority-backed operator or poison its key"
            );

            let repeated_static = ObjectStoreAccessContext::for_static(
                endpoint.clone(),
                static_reference("blue"),
                secret_material("same-reference-ignored-material"),
                &pool,
            );
            let repeated_authority = ObjectStoreAccessContext::for_authority(
                endpoint.clone(),
                Arc::clone(&authority),
                &pool,
            );
            let static_hit = acquire(&repeated_static).unwrap();
            let authority_hit = acquire(&repeated_authority).unwrap();
            assert!(Arc::ptr_eq(
                static_operator.base_service(),
                static_hit.base_service()
            ));
            assert!(Arc::ptr_eq(
                authority_operator.base_service(),
                authority_hit.base_service()
            ));

            let next_generation = ObjectStoreAccessContext::for_static(
                endpoint.clone(),
                static_reference("green"),
                secret_material("static"),
                &pool,
            );
            let next_operator = acquire(&next_generation).unwrap();
            assert!(!Arc::ptr_eq(
                static_operator.base_service(),
                next_operator.base_service()
            ));
            let other_domain = pool
                .acquire(
                    domain(2),
                    "warehouse",
                    &static_context.endpoint_config,
                    &static_context.credentials,
                    |_, _| memory_operator(),
                )
                .unwrap();
            assert!(!Arc::ptr_eq(
                static_operator.base_service(),
                other_domain.base_service()
            ));
            let metrics = pool.metrics_snapshot().unwrap();
            assert_eq!(metrics.operator_constructions, 4);
            assert_eq!(metrics.cache_hits, 2);
            assert_eq!(metrics.cache_misses, 4);
            assert_eq!(metrics.resident_entries, 4);
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
        let secrets = static_source("one");
        pool.acquire(domain(1), "warehouse", &endpoint, &secrets, |_, _| {
            memory_operator()
        })
        .unwrap();
        pool.acquire(domain(2), "warehouse", &endpoint, &secrets, |_, _| {
            memory_operator()
        })
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
                let secrets = static_source("one");
                barrier.wait();
                pool.acquire(domain(1), "warehouse", &endpoint, &secrets, |_, _| {
                    constructions.fetch_add(1, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(50));
                    memory_operator()
                })
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
            &static_source("one"),
        )
        .expect("provider key")
    }

    fn spawn_async_waiter(
        pool: &Arc<ObjectStoreProviderPool>,
        cancellation: FileCancellation,
    ) -> tokio::task::JoinHandle<FileResult<Operator>> {
        let pool = Arc::clone(pool);
        tokio::spawn(async move {
            pool.acquire_async(
                domain(1),
                "warehouse",
                &endpoint_config("http://localhost:9000"),
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
                        Some(ObjectStoreAccessContext::for_static(
                            endpoint_config("http://localhost:9000"),
                            static_reference("blue"),
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
                Some(ObjectStoreAccessContext::for_static(
                    endpoint_config("http://localhost:9000"),
                    static_reference("blue"),
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
        let secrets = static_source("one");
        pool.acquire(domain(1), "warehouse", &endpoint, &secrets, |_, _| {
            memory_operator()
        })
        .unwrap();
        let error = pool
            .acquire(domain(2), "warehouse", &endpoint, &secrets, |_, _| {
                Err(FileError::new(
                    FileErrorKind::Invalid,
                    "injected construction failure",
                ))
            })
            .unwrap_err();
        assert_eq!(error.kind(), FileErrorKind::Invalid);
        pool.acquire(
            domain(1),
            "warehouse",
            &endpoint,
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
        let secrets = static_source("one");
        pool.acquire(
            domain(1),
            "warehouse",
            &endpoint_config("HTTPS://EXAMPLE.COM:443/a/../api//"),
            &secrets,
            |_, _| memory_operator(),
        )
        .unwrap();
        pool.acquire(
            domain(1),
            "warehouse",
            &endpoint_config("https://example.com/api"),
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
        let loader = AuthoritySigningSource {
            authority: Arc::clone(&authority),
            denial: Arc::new(CredentialDenialLatch::default()),
            budget: Duration::from_secs(60),
        };

        for _ in 0..3 {
            let credential = runtime
                .block_on(loader.credential_for_request_in(None))
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
                    routing: novarocks_spi::connector::CredentialRoutingContext::try_new(
                        "http://127.0.0.1:1/v1/credentials",
                        [],
                    )
                    .unwrap(),
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
            test_s3_http_client(),
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
        // No material and no renewal capability: this local permission
        // failure is distinct from a provider-confirmed denial.
        let authority = seeded_authority("s3://warehouse/sales/orders/", None);
        let endpoint = ObjectStoreEndpointIdentity::try_new(
            "warehouse",
            &endpoint_config("http://127.0.0.1:1"),
        )
        .unwrap();
        let operator = build_object_store_operator(
            &endpoint,
            &ObjectStoreCredentialSource::Authority(Arc::clone(&authority)),
            test_s3_http_client(),
        )
        .unwrap();

        let error = runtime
            .block_on(operator.stat("sales/orders/part-0.parquet"))
            .expect_err("a denied authority cannot sign a request");
        let mapped = map_opendal_error("stat file", error);
        assert_eq!(mapped.kind(), FileErrorKind::Permission);
        assert_authority_fact(&mapped, 4);
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

    fn assert_authority_fact(error: &FileError, code: u16) {
        use novarocks_failure_contract::{FailureClass, FailureEvidence, FailureProjection};
        let fact = novarocks_spi::connector::ConnectorError::from(error).failure_fact();
        assert_eq!(fact.class(), FailureClass::Dependency);
        assert_eq!(fact.evidence(), FailureEvidence::Originating);
        assert_eq!((fact.reason().domain(), fact.reason().code()), (23, code));
    }

    struct FixedAuthorityFailure {
        failure: AcquisitionFailure,
        calls: AtomicUsize,
        refreshes: Mutex<Vec<JoinHandle<()>>>,
    }

    impl FixedAuthorityFailure {
        /// The test thread observes actual refresh exit before asserting the
        /// operation result. Do not join from Drop: a refresh owns the source.
        fn join_refreshes(&self) -> std::thread::Result<()> {
            let handles = std::mem::take(
                &mut *self
                    .refreshes
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            );
            let mut first_panic = None;
            for handle in handles {
                if let Err(panic) = handle.join()
                    && first_panic.is_none()
                {
                    first_panic = Some(panic);
                }
            }
            match first_panic {
                Some(panic) => Err(panic),
                None => Ok(()),
            }
        }
    }

    impl AuthorityMaterialSource for FixedAuthorityFailure {
        fn acquire(&self, _deadline: Instant) -> Result<AuthorityMaterial, AcquisitionFailure> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Err(self.failure.clone())
        }
    }

    impl RefreshExecutor for FixedAuthorityFailure {
        fn execute(&self, job: Box<dyn FnOnce() + Send + 'static>) {
            let handle = std::thread::spawn(job);
            self.refreshes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(handle);
        }
    }

    fn failing_authority_operator(
        failure: AcquisitionFailure,
    ) -> (Operator, Arc<StorageAuthority>, Arc<FixedAuthorityFailure>) {
        let capability = if matches!(&failure, AcquisitionFailure::NoRenewalCapability) {
            AuthorityCapabilityPath::SeededWithoutRenewal { grant: None }
        } else {
            AuthorityCapabilityPath::CredentialsEndpoint {
                principal: StaticCredentialReference::try_new("executor", "v1").unwrap(),
                routing: novarocks_spi::connector::CredentialRoutingContext::try_new(
                    "http://127.0.0.1:1/v1/credentials",
                    [],
                )
                .unwrap(),
            }
        };
        let source = Arc::new(FixedAuthorityFailure {
            failure,
            calls: AtomicUsize::new(0),
            refreshes: Mutex::new(Vec::new()),
        });
        let authority = Arc::new(StorageAuthority::new(
            StorageAuthorityId::new(
                CatalogHandle::new(
                    ConnectorInstanceId::parse("lake").unwrap(),
                    CatalogVersion::from_bytes([0x11; 32]),
                ),
                StorageCredentialScopePrefix::try_from_normalized("s3://warehouse/sales/orders/")
                    .unwrap(),
                capability,
            ),
            source.clone(),
            source.clone(),
            RefreshPolicy::default(),
        ));
        let endpoint = ObjectStoreEndpointIdentity::try_new(
            "warehouse",
            &endpoint_config("http://127.0.0.1:1"),
        )
        .unwrap();
        let operator = build_object_store_operator(
            &endpoint,
            &ObjectStoreCredentialSource::Authority(authority.clone()),
            test_s3_http_client(),
        )
        .unwrap();
        (operator, authority, source)
    }

    #[test]
    fn four_acquisition_causes_cross_the_signer_and_each_file_error_bridge() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        // These are independent producer inputs. The FS does not own the
        // provider's HTTP-to-AcquisitionFailure classification. Each input
        // crosses the real authority, reqsign loader and OpenDAL operation.
        for bridge in 0..4 {
            for (failure, code, kind) in [
                (
                    AcquisitionFailure::Transient("provider timeout PRIVATE".into()),
                    1,
                    FileErrorKind::Transient,
                ),
                (
                    AcquisitionFailure::Denied("provider confirmed denial PRIVATE".into()),
                    2,
                    FileErrorKind::Permission,
                ),
                (
                    AcquisitionFailure::CatalogUnreachable(
                        "catalog connect refused PRIVATE".into(),
                    ),
                    3,
                    FileErrorKind::Transient,
                ),
                (
                    AcquisitionFailure::NoRenewalCapability,
                    4,
                    FileErrorKind::Permission,
                ),
            ] {
                let (operator, authority, source) = failing_authority_operator(failure);
                let result = runtime.block_on(operator.stat("sales/orders/part-0.parquet"));
                source
                    .join_refreshes()
                    .expect("join acquisition refresh owner");
                let error = result.expect_err("the producer refused signing material");
                assert!(!error.is_temporary());
                let original = authority_cause_in(&error).expect("original acquisition cause");
                let mapped = match bridge {
                    0 => map_opendal_error("stat file", error),
                    1 => map_stream_error("stream file", std::io::Error::from(error)),
                    2 => map_bounded_listing_opendal_error("list files", error),
                    3 => map_conditional_create_error("conditionally create file", error),
                    _ => unreachable!(),
                };
                assert_eq!(mapped.kind(), kind);
                assert_eq!(mapped.authority_cause(), Some(&original));
                assert_authority_fact(&mapped, code);
                assert_eq!(authority.metrics().blocking_waits, 1);
                assert_eq!(source.calls.load(Ordering::Relaxed), usize::from(code != 4));
            }
        }
    }

    #[test]
    fn denial_latch_shares_the_original_cause_and_preserves_generic_permission() {
        use crate::storage_authority::failure::AuthorityFailureReason;
        for (reason, code) in [
            (AuthorityFailureReason::AcquisitionDenied, 2),
            (AuthorityFailureReason::NoRenewalCapability, 4),
        ] {
            let latch = CredentialDenialLatch::default();
            let denial = FileError::authority(FileErrorKind::Permission, reason, "original denial");
            let original = denial.authority_cause().unwrap().clone();
            latch.record(&denial);
            latch.record(&FileError::new(
                FileErrorKind::Permission,
                "later unrelated denial",
            ));
            let restored = latch.classify(
                opendal::Error::new(opendal::ErrorKind::Unexpected, "loader lost its source")
                    .set_temporary(),
            );
            assert!(!restored.is_temporary());
            let mapped = map_opendal_error("stat file", restored);
            assert_eq!(mapped.kind(), FileErrorKind::Permission);
            assert_eq!(mapped.authority_cause(), Some(&original));
            assert_authority_fact(&mapped, code);
        }

        let latch = CredentialDenialLatch::default();
        latch.record(&FileError::new(
            FileErrorKind::Permission,
            "ordinary permission",
        ));
        let restored = latch.classify(
            opendal::Error::new(opendal::ErrorKind::Unexpected, "ordinary loader failure")
                .set_temporary(),
        );
        assert!(!restored.is_temporary());
        let mapped = map_opendal_error("stat file", restored);
        assert_eq!(mapped.kind(), FileErrorKind::Permission);
        assert!(mapped.authority_cause().is_none());
        assert!(mapped.to_string().contains("ordinary permission"));
    }

    #[test]
    fn bounded_listing_retains_the_cause_without_displaying_opaque_sources() {
        use crate::storage_authority::failure::AuthorityFailureReason;
        let original = FileError::authority(
            FileErrorKind::Permission,
            AuthorityFailureReason::AcquisitionDenied,
            "known local denial",
        )
        .authority_cause()
        .unwrap()
        .clone();
        let opaque = FileError::with_source(
            FileErrorKind::Permission,
            "x".repeat(8192),
            ListingPanicDisplay,
        )
        .with_authority_cause(original.clone());
        let error = opendal::Error::new(opendal::ErrorKind::PermissionDenied, "x".repeat(8192))
            .with_context("path", "y".repeat(8192))
            .set_source(opaque);
        let mapped = map_bounded_listing_opendal_error("list files", error);
        assert_eq!(mapped.authority_cause(), Some(&original));
        assert_authority_fact(&mapped, 2);
        assert!(mapped.to_string().len() < 128);
    }

    #[test]
    fn authority_extraction_is_bounded_and_does_not_reclassify_waiter_deadlines() {
        use crate::storage_authority::failure::AuthorityFailureReason;
        let source = FileError::authority(
            FileErrorKind::Transient,
            AuthorityFailureReason::CatalogUnreachable,
            "catalog refused",
        );
        let mut error =
            opendal::Error::new(opendal::ErrorKind::Unexpected, "signing").set_source(source);
        // Fifteen OpenDAL nodes and the FileError fit the sixteen-node bound.
        for _ in 1..15 {
            error =
                opendal::Error::new(opendal::ErrorKind::Unexpected, "wrapped").set_source(error);
        }
        assert!(authority_cause_in(&error).is_some());
        let over_bound =
            opendal::Error::new(opendal::ErrorKind::Unexpected, "wrapped").set_source(error);
        assert!(authority_cause_in(&over_bound).is_none());
        let mapped = map_opendal_error("stat file", over_bound);
        assert_eq!(mapped.kind(), FileErrorKind::Transient);
        assert!(mapped.authority_cause().is_none());

        let deadline = FileError::deadline("only this waiter ran out of time");
        let error = CredentialDenialLatch::default().classify(
            opendal::Error::new(opendal::ErrorKind::Unexpected, "signing")
                .set_temporary()
                .set_source(deadline),
        );
        assert!(!error.is_temporary());
        let mapped = map_opendal_error("stat file", error);
        assert!(mapped.authority_cause().is_none());
    }

    #[test]
    fn unknown_source_wrappers_do_not_invent_an_authority_fact() {
        use crate::storage_authority::failure::AuthorityFailureReason;
        #[derive(Debug)]
        struct UnknownWrapper(FileError);
        impl std::fmt::Display for UnknownWrapper {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("unknown wrapper")
            }
        }
        impl std::error::Error for UnknownWrapper {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }
        let error = opendal::Error::new(opendal::ErrorKind::PermissionDenied, "opaque").set_source(
            UnknownWrapper(FileError::authority(
                FileErrorKind::Permission,
                AuthorityFailureReason::AcquisitionDenied,
                "known only behind an unsupported wrapper",
            )),
        );
        let mapped = map_bounded_listing_opendal_error("list files", error);
        assert_eq!(mapped.kind(), FileErrorKind::Permission);
        assert!(mapped.authority_cause().is_none());
    }

    // One finite server owner multiplexes every accepted socket, so an
    // accidental per-operator client produces a count failure, not a hang
    // waiting for a second connection behind the first keepalive socket.
    struct S3KeepaliveObservation {
        accepted: usize,
        targets: Vec<String>,
    }

    struct S3KeepaliveFixture {
        address: std::net::SocketAddr,
        stop: Arc<std::sync::atomic::AtomicBool>,
        server: Option<JoinHandle<std::io::Result<S3KeepaliveObservation>>>,
    }

    impl S3KeepaliveFixture {
        fn start() -> Self {
            use std::io::{Error, ErrorKind, Read, Write};
            use std::sync::atomic::{AtomicBool, Ordering};
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            listener.set_nonblocking(true).unwrap();
            let stop = Arc::new(AtomicBool::new(false));
            let server_stop = Arc::clone(&stop);
            let server = std::thread::spawn(move || {
                struct Connection {
                    stream: std::net::TcpStream,
                    request: Vec<u8>,
                    response_offset: Option<usize>,
                }
                const RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nETag: \"fixture\"\r\nLast-Modified: Mon, 01 Jan 2024 00:00:00 GMT\r\nConnection: keep-alive\r\n\r\n";
                let deadline = Instant::now() + Duration::from_secs(20);
                let mut connections = Vec::<Connection>::new();
                let mut accepted = 0;
                let mut targets = Vec::new();
                while !server_stop.load(Ordering::Acquire) {
                    if Instant::now() >= deadline {
                        return Err(Error::new(
                            ErrorKind::TimedOut,
                            "keepalive fixture deadline",
                        ));
                    }
                    loop {
                        match listener.accept() {
                            Ok((stream, _)) => {
                                if accepted == 8 {
                                    return Err(Error::other("too many fixture connections"));
                                }
                                stream.set_nonblocking(true)?;
                                accepted += 1;
                                connections.push(Connection {
                                    stream,
                                    request: Vec::new(),
                                    response_offset: None,
                                });
                            }
                            Err(error) if error.kind() == ErrorKind::WouldBlock => break,
                            Err(error) => return Err(error),
                        }
                    }
                    let mut index = 0;
                    while index < connections.len() {
                        let connection = &mut connections[index];
                        let mut closed = false;
                        if connection.response_offset.is_none() {
                            let mut bytes = [0; 4096];
                            match connection.stream.read(&mut bytes) {
                                Ok(0) => closed = true,
                                Ok(count) => {
                                    connection.request.extend_from_slice(&bytes[..count]);
                                    if connection.request.len() > 8192 {
                                        return Err(Error::other("oversized fixture request"));
                                    }
                                    if connection.request.windows(4).any(|w| w == b"\r\n\r\n") {
                                        let line = connection
                                            .request
                                            .split(|b| *b == b'\n')
                                            .next()
                                            .unwrap();
                                        let line = std::str::from_utf8(line).map_err(|error| {
                                            Error::new(ErrorKind::InvalidData, error)
                                        })?;
                                        if !line.starts_with("HEAD ") || targets.len() == 8 {
                                            return Err(Error::other("unexpected fixture request"));
                                        }
                                        targets.push(line.trim_end().to_string());
                                        connection.request.clear();
                                        connection.response_offset = Some(0);
                                    }
                                }
                                Err(error) if error.kind() == ErrorKind::WouldBlock => {}
                                Err(error) => return Err(error),
                            }
                        }
                        if let Some(offset) = connection.response_offset {
                            match connection.stream.write(&RESPONSE[offset..]) {
                                Ok(0) => {
                                    return Err(Error::new(
                                        ErrorKind::WriteZero,
                                        "fixture response",
                                    ));
                                }
                                Ok(count) => {
                                    connection.response_offset = if offset + count == RESPONSE.len()
                                    {
                                        None
                                    } else {
                                        Some(offset + count)
                                    };
                                }
                                Err(error) if error.kind() == ErrorKind::WouldBlock => {}
                                Err(error) => return Err(error),
                            }
                        }
                        if closed {
                            connections.swap_remove(index);
                        } else {
                            index += 1;
                        }
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
                Ok(S3KeepaliveObservation { accepted, targets })
            });
            Self {
                address,
                stop,
                server: Some(server),
            }
        }

        fn finish(mut self) -> std::io::Result<S3KeepaliveObservation> {
            self.stop.store(true, std::sync::atomic::Ordering::Release);
            self.server
                .take()
                .unwrap()
                .join()
                .map_err(|_| std::io::Error::other("keepalive fixture panicked"))?
        }
    }

    impl Drop for S3KeepaliveFixture {
        fn drop(&mut self) {
            self.stop.store(true, std::sync::atomic::Ordering::Release);
            if let Some(server) = self.server.take() {
                // Also runs after a failed client assertion, without a second panic.
                let _ = server.join();
            }
        }
    }

    #[test]
    fn pool_http_client_reuses_one_connection_across_bucket_and_credential_operators() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let server = S3KeepaliveFixture::start();
        let pool = ObjectStoreProviderPool::new_for_test(8, Duration::from_secs(60)).unwrap();
        let mut config = endpoint_config(&format!("http://{}", server.address));
        config.retry_max_times = Some(0);
        config.timeout_ms = Some(5_000);
        config.io_timeout_ms = Some(5_000);
        let acquire = |bucket: &str, generation: &str| {
            pool.acquire(
                domain(77),
                bucket,
                &config,
                &ObjectStoreCredentialSource::Static {
                    reference: static_reference(generation),
                    material: secret_material(generation),
                },
                |endpoint, credentials| pool.build_operator(endpoint, credentials),
            )
            .unwrap()
        };
        let one = acquire("one", "first");
        let two = acquire("two", "first");
        let alias = acquire("one", "second");
        runtime.block_on(async {
            one.stat("object").await.unwrap();
            two.stat("object").await.unwrap();
        });
        let constructions = pool.metrics_snapshot().unwrap().operator_constructions;
        // An external operator alias retains the client after the original
        // pool owner exits. Pool destruction is not an HTTP-exit proof.
        drop(pool);
        runtime.block_on(alias.stat("object")).unwrap();
        drop((one, two, alias));
        let S3KeepaliveObservation { accepted, targets } = server.finish().unwrap();
        assert_eq!(constructions, 3);
        assert_eq!(
            targets,
            [
                "HEAD /one/object HTTP/1.1",
                "HEAD /two/object HTTP/1.1",
                "HEAD /one/object HTTP/1.1",
            ]
        );
        assert_eq!(
            accepted, 1,
            "bucket and credential aliases share the pool client"
        );
    }

    #[test]
    fn real_s3_http_failures_keep_their_generic_connector_facts() {
        use novarocks_failure_contract::FailureProjection;
        use std::io::{Read, Write};
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        for (status, kind, code) in [
            ("403 Forbidden", FileErrorKind::Permission, 3),
            ("503 Service Unavailable", FileErrorKind::Transient, 8),
        ] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            listener.set_nonblocking(true).unwrap();
            let server = std::thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(5);
                let (mut stream, _) = loop {
                    match listener.accept() {
                        Ok(accepted) => break accepted,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            assert!(Instant::now() < deadline, "S3 request did not arrive");
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        Err(error) => panic!("accept S3 request: {error}"),
                    }
                };
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut request = [0; 8192];
                let bytes = stream.read(&mut request).unwrap();
                assert!(request[..bytes].starts_with(b"HEAD "));
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .unwrap();
            });
            let authority = seeded_authority(
                "s3://warehouse/sales/orders/",
                Some(vended_material(
                    "live",
                    Instant::now() + Duration::from_secs(900),
                )),
            );
            let mut config = endpoint_config(&format!("http://{address}"));
            config.retry_max_times = Some(0);
            config.timeout_ms = Some(5_000);
            config.io_timeout_ms = Some(5_000);
            let endpoint = ObjectStoreEndpointIdentity::try_new("warehouse", &config).unwrap();
            let operator = build_object_store_operator(
                &endpoint,
                &ObjectStoreCredentialSource::Authority(authority.clone()),
                test_s3_http_client(),
            )
            .unwrap();
            let result = runtime.block_on(operator.stat("sales/orders/part-0.parquet"));
            server.join().expect("join S3 response owner");
            let mapped =
                map_opendal_error("stat file", result.expect_err("S3 refused the request"));
            assert_eq!(mapped.kind(), kind);
            assert!(mapped.authority_cause().is_none());
            let fact = novarocks_spi::connector::ConnectorError::from(&mapped).failure_fact();
            assert_eq!((fact.reason().domain(), fact.reason().code()), (4, code));
            assert_eq!(authority.metrics().blocking_waits, 0);
        }
    }
}

#[cfg(test)]
mod hdfs_user_tests {
    use super::*;

    #[test]
    fn decoded_user_is_the_exact_comparison_and_cache_identity() {
        let a = parse_hdfs_path("hdfs://%61lice@warehouse-ha/data/a").unwrap();
        let b = parse_hdfs_path("hdfs://alice@warehouse-ha/data/b").unwrap();
        assert_eq!(a.name_node, "hdfs://warehouse-ha");
        assert_eq!(a.user.as_deref(), Some("alice"));
        assert_eq!(a.user, b.user);
        assert_eq!(
            parse_hdfs_path("hdfs://%2561lice@warehouse-ha/file")
                .unwrap()
                .user
                .as_deref(),
            Some("%61lice")
        );
        assert_eq!(
            HdfsOperatorKey {
                name_node: a.name_node,
                user: a.user
            },
            HdfsOperatorKey {
                name_node: b.name_node,
                user: b.user
            },
        );
        assert_eq!(
            parse_hdfs_path("hdfs://%C3%A9@nn:8020/file")
                .unwrap()
                .user
                .as_deref(),
            Some("é")
        );
        for (encoded, expected) in [
            ("alice%3Ateam", "alice:team"),
            ("alice%40realm", "alice@realm"),
        ] {
            let parsed = parse_hdfs_path(&format!("hdfs://{encoded}@nn:8020/file")).unwrap();
            assert_eq!(parsed.name_node, "hdfs://nn:8020");
            assert_eq!(parsed.user.as_deref(), Some(expected));
        }
    }

    #[test]
    fn invalid_user_utf8_and_original_forbidden_uri_components_fail_before_build() {
        for raw in [
            "hdfs://%FF@nn:8020/file",
            "hdfs://alice:password@nn:8020/file",
            "hdfs://alice@nn:8020/file?query",
            "hdfs://alice@nn:8020/file#fragment",
        ] {
            let Err(error) = parse_hdfs_path(raw) else {
                panic!("invalid HDFS URI was accepted")
            };
            assert_eq!(error.kind(), FileErrorKind::Invalid);
        }
        let locations = vec![
            FsLocation::parse("hdfs://alice@nn:8020/a").unwrap(),
            FsLocation::parse("hdfs://bob@nn:8020/b").unwrap(),
        ];
        let result =
            resolve_hdfs_locations(StorageAccessDomainId::from_bytes([0; 32]), locations, None);
        let Err(error) = result else {
            panic!("mixed effective users were accepted")
        };
        assert_eq!(error.kind(), FileErrorKind::Invalid);
    }
    #[test]
    fn hdfs_cache_is_shared_only_by_the_same_runtime_view_and_unready_cannot_hit() {
        let first_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let second_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let first = FsAccessResolver::with_runtime(first_runtime.handle().clone());
        let first_clone = first.clone();
        let second = FsAccessResolver::with_runtime(second_runtime.handle().clone());
        let configuration_only = FsAccessResolver::new();
        let key = HdfsOperatorKey {
            name_node: "hdfs://namenode.invalid:8020".into(),
            user: Some("alice".into()),
        };
        // Populate the original cache with real storage instances. Memory
        // makes the cache selection oracle independent of DNS/RPC or Hadoop
        // machine configuration; production misses use the actual SDK client.
        let first_operator = Operator::new(opendal::services::Memory::default()).unwrap();
        let second_operator = Operator::new(opendal::services::Memory::default()).unwrap();
        first_runtime
            .block_on(first_operator.write("data", Bytes::from_static(b"first")))
            .unwrap();
        second_runtime
            .block_on(second_operator.write("data", Bytes::from_static(b"second")))
            .unwrap();
        first
            .hdfs
            .as_ref()
            .unwrap()
            .operators
            .lock()
            .unwrap()
            .insert(key.clone(), first_operator);
        second
            .hdfs
            .as_ref()
            .unwrap()
            .operators
            .lock()
            .unwrap()
            .insert(key, second_operator);
        let domain = StorageAccessDomainId::from_bytes([19; 32]);
        let location = "hdfs://alice@namenode.invalid:8020/data";
        assert!(configuration_only.parse_location(location).is_ok());
        let unready_sync = configuration_only
            .resolve_location(domain, location, None)
            .unwrap_err();
        let unready_async = first_runtime
            .block_on(configuration_only.resolve_location_async(
                domain,
                location,
                None,
                &FileCancellation::new(),
            ))
            .unwrap_err();
        let first_file = first.resolve_location(domain, location, None).unwrap();
        let clone_file = first_clone
            .resolve_location(domain, location, None)
            .unwrap();
        let second_file = second_runtime
            .block_on(second.resolve_location_async(
                domain,
                location,
                None,
                &FileCancellation::new(),
            ))
            .unwrap();
        let first_data = first_runtime
            .block_on(first_file.operator().read("data"))
            .unwrap()
            .to_bytes();
        let clone_data = first_runtime
            .block_on(clone_file.operator().read("data"))
            .unwrap()
            .to_bytes();
        let second_data = second_runtime
            .block_on(second_file.operator().read("data"))
            .unwrap()
            .to_bytes();
        // All raw outputs and runtime views exit before their original owners.
        drop((first_file, clone_file, second_file));
        drop((first, first_clone, second, configuration_only));
        drop((first_runtime, second_runtime));
        assert_eq!(unready_sync.kind(), FileErrorKind::Internal);
        assert_eq!(unready_async.kind(), FileErrorKind::Internal);
        assert_eq!(&first_data[..], b"first");
        assert_eq!(&clone_data[..], b"first");
        assert_eq!(&second_data[..], b"second");
    }
}
