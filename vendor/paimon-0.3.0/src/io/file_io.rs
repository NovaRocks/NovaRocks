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

use crate::error::*;
use std::collections::HashMap;
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::SystemTime;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use opendal::raw::normalize_root;
use opendal::Operator;
use snafu::ResultExt;
use tokio_util::compat::FuturesAsyncWriteCompatExt;
use url::Url;

use super::cache::{CachedFileReader, LocalCache};
use super::{ReadControl, ReadOnlyFileIO, ReadReservation, ReadRetention, RetainedRead, Storage};

#[derive(Clone)]
pub struct FileIO {
    backend: FileIOBackend,
    cache: Option<Arc<LocalCache>>,
    control: Option<Arc<dyn ReadControl>>,
}

#[derive(Clone)]
enum FileIOBackend {
    Native(Arc<Storage>),
    ReadOnly(Arc<dyn ReadOnlyFileIO>),
}

impl std::fmt::Debug for FileIO {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FileIO")
            .field(
                "backend",
                &match &self.backend {
                    FileIOBackend::Native(_) => "native",
                    FileIOBackend::ReadOnly(_) => "host-read-only",
                },
            )
            .field("cache_enabled", &self.cache.is_some())
            .field("control_bound", &self.control.is_some())
            .finish()
    }
}

impl FileIO {
    /// Build from a host-authorized read-only backend without constructing an
    /// SDK storage client or local cache.
    pub fn from_read_only(backend: Arc<dyn ReadOnlyFileIO>, control: Arc<dyn ReadControl>) -> Self {
        Self {
            backend: FileIOBackend::ReadOnly(backend),
            cache: None,
            control: Some(control),
        }
    }

    pub fn is_read_only(&self) -> bool {
        matches!(&self.backend, FileIOBackend::ReadOnly(_))
    }

    /// Return the request-local read control installed by the embedding host.
    ///
    /// SDK readers use this same control for decoded Arrow batches and merge
    /// workspaces so the FileIO and reader layers cannot establish independent
    /// memory or cancellation authorities.
    pub(crate) fn read_control(&self) -> Option<Arc<dyn ReadControl>> {
        self.control.clone()
    }

    #[cfg(test)]
    pub(crate) fn has_local_cache(&self) -> bool {
        self.cache.is_some()
    }

    /// Try to infer file io scheme from path.
    ///
    /// The input HashMap is paimon-java's [`Options`](https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/options/Options.java#L60)
    pub fn from_url(path: &str) -> crate::Result<FileIOBuilder> {
        let url = Url::parse(path).map_err(|_| Error::ConfigInvalid {
            message: format!("Invalid URL: {path}"),
        })?;

        Ok(FileIOBuilder::new(url.scheme()))
    }

    /// Try to infer file io scheme from path. See [`FileIO`] for supported schemes.
    ///
    /// - If it's a valid url, for example `s3://bucket/a`, url scheme will be used, and the rest of the url will be ignored.
    /// - If it's not a valid url, will try to detect if it's a file path.
    ///
    /// Otherwise will return parsing error.
    pub fn from_path(path: impl AsRef<str>) -> crate::Result<FileIOBuilder> {
        let path = path.as_ref();
        let url = if looks_like_windows_drive_path(path) {
            Url::from_file_path(path).map_err(|_| Error::ConfigInvalid {
                message: format!("Input {path} is neither a valid url nor path"),
            })?
        } else {
            Url::parse(path)
                .map_err(|_| Error::ConfigInvalid {
                    message: format!("Invalid URL: {path}"),
                })
                .or_else(|_| {
                    Url::from_file_path(path).map_err(|_| Error::ConfigInvalid {
                        message: format!("Input {path} is neither a valid url nor path"),
                    })
                })?
        };
        Ok(FileIOBuilder::new(url.scheme()))
    }

    /// Create a new input file to read data.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L76>
    pub fn new_input(&self, path: &str) -> crate::Result<InputFile> {
        if let FileIOBackend::ReadOnly(backend) = &self.backend {
            self.check_active()?;
            return Ok(InputFile {
                source: InputFileSource::ReadOnly {
                    backend: backend.clone(),
                    control: self.control.clone().expect("read-only control"),
                },
                path: path.to_string(),
                cache_path: String::new(),
                cache: None,
            });
        }
        let FileIOBackend::Native(storage) = &self.backend else {
            unreachable!()
        };
        let (op, relative_path) = storage.create(path)?;
        let cache_path = cache_object_path(&op, relative_path.as_ref());
        Ok(InputFile {
            source: InputFileSource::Native {
                op,
                relative_path: relative_path.into_owned(),
            },
            path: path.to_string(),
            cache_path,
            cache: self
                .cache
                .as_ref()
                .filter(|cache| cache.is_cacheable(path))
                .cloned(),
        })
    }

    /// Create a new output file to write data.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L87>
    pub fn new_output(&self, path: &str) -> Result<OutputFile> {
        let FileIOBackend::Native(storage) = &self.backend else {
            return Err(read_only_write_error("create output file"));
        };
        let (op, relative_path) = storage.create(path)?;
        let cache_path = cache_object_path(&op, relative_path.as_ref());
        Ok(OutputFile {
            op,
            path: path.to_string(),
            relative_path: relative_path.into_owned(),
            cache_path,
            cache: self
                .cache
                .as_ref()
                .filter(|cache| cache.is_cacheable(path))
                .cloned(),
        })
    }

    /// Return a file status object that represents the path.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L97>
    pub async fn get_status(&self, path: &str) -> Result<FileStatus> {
        if let FileIOBackend::ReadOnly(backend) = &self.backend {
            self.check_active()?;
            let status = backend.stat(path).await?;
            self.checkpoint()?;
            return Ok(status);
        }
        let FileIOBackend::Native(storage) = &self.backend else {
            unreachable!()
        };
        let (op, relative_path) = storage.create(path)?;
        let meta = op
            .stat(relative_path.as_ref())
            .await
            .context(IoUnexpectedSnafu {
                message: format!("Failed to get file status for '{path}'"),
            })?;

        Ok(FileStatus {
            size: meta.content_length(),
            is_dir: meta.is_dir(),
            last_modified: meta
                .last_modified()
                .map(|v| DateTime::<Utc>::from(SystemTime::from(v))),
            path: path.to_string(),
        })
    }

    /// List the statuses of the files/directories in the given path if the path is a directory.
    ///
    /// References: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L105>
    ///
    /// FIXME: how to handle large dir? Better to return a stream instead?
    pub async fn list_status(&self, path: &str) -> Result<Vec<FileStatus>> {
        Ok(self.list_status_retained(path).await?.into_value())
    }

    /// Controlled listing whose entry reservations travel with the returned
    /// vector. Native and legacy callers may continue using [`Self::list_status`].
    pub async fn list_status_retained(&self, path: &str) -> Result<RetainedRead<Vec<FileStatus>>> {
        if let FileIOBackend::ReadOnly(backend) = &self.backend {
            return self.collect_read_only_listing(backend, path, false).await;
        }
        let FileIOBackend::Native(storage) = &self.backend else {
            unreachable!()
        };
        let (op, relative_path) = storage.create(path)?;
        // `relative_path` is a byte-suffix of `path` for object stores and POSIX
        // local paths, so this recovers the scheme/root prefix. For a Windows
        // local path the relative form only swaps `\`->`/` (length-preserving),
        // so this is `""` and entries are reported in opendal's normalized
        // `/C:/...` form — which still round-trips back through `create`.
        let base_path = &path[..path.len() - relative_path.len()];
        // Opendal list() expects directory path to end with `/`.
        // use normalize_root to make sure it end with `/`.
        let list_path = normalize_root(relative_path.as_ref());

        let entries = op.list_with(&list_path).await.context(IoUnexpectedSnafu {
            message: format!("Failed to list files in '{path}'"),
        })?;

        let mut statuses = Vec::new();
        let list_path_normalized = list_path.trim_start_matches('/');
        for entry in entries {
            let entry_path = entry.path();
            if entry_path.trim_start_matches('/') == list_path_normalized {
                continue;
            }
            let meta = entry.metadata();
            statuses.push(FileStatus {
                size: meta.content_length(),
                is_dir: meta.is_dir(),
                path: status_path(base_path, entry_path),
                last_modified: meta
                    .last_modified()
                    .map(|v| DateTime::<Utc>::from(SystemTime::from(v))),
            });
        }

        Ok(RetainedRead::unretained(statuses))
    }

    /// List all files recursively under the given directory path.
    pub async fn list_status_recursive(&self, path: &str) -> Result<Vec<FileStatus>> {
        Ok(self
            .list_status_recursive_retained(path)
            .await?
            .into_value())
    }

    /// Recursive counterpart of [`Self::list_status_retained`].
    pub async fn list_status_recursive_retained(
        &self,
        path: &str,
    ) -> Result<RetainedRead<Vec<FileStatus>>> {
        if let FileIOBackend::ReadOnly(backend) = &self.backend {
            return self.collect_read_only_listing(backend, path, true).await;
        }
        let FileIOBackend::Native(storage) = &self.backend else {
            unreachable!()
        };
        let (op, relative_path) = storage.create(path)?;
        // See `list_status`: `relative_path` is a byte-suffix of `path` except
        // for Windows local paths, where it only swaps separators (same length).
        let base_path = &path[..path.len() - relative_path.len()];
        let list_path = normalize_root(relative_path.as_ref());

        let entries =
            op.list_with(&list_path)
                .recursive(true)
                .await
                .context(IoUnexpectedSnafu {
                    message: format!("Failed to list files recursively in '{path}'"),
                })?;

        let mut statuses = Vec::new();
        let list_path_normalized = list_path.trim_start_matches('/');
        for entry in entries {
            let entry_path = entry.path();
            if entry_path.trim_start_matches('/') == list_path_normalized {
                continue;
            }
            let meta = entry.metadata();
            if meta.is_dir() {
                continue;
            }
            statuses.push(FileStatus {
                size: meta.content_length(),
                is_dir: false,
                path: status_path(base_path, entry_path),
                last_modified: meta
                    .last_modified()
                    .map(|v| DateTime::<Utc>::from(SystemTime::from(v))),
            });
        }

        Ok(RetainedRead::unretained(statuses))
    }

    /// Check if exists.
    ///
    /// References: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L128>
    pub async fn exists(&self, path: &str) -> Result<bool> {
        if let FileIOBackend::ReadOnly(backend) = &self.backend {
            self.check_active()?;
            let exists = backend.exists(path).await?;
            self.checkpoint()?;
            return Ok(exists);
        }
        let FileIOBackend::Native(storage) = &self.backend else {
            unreachable!()
        };
        let (op, relative_path) = storage.create(path)?;

        op.exists(relative_path.as_ref())
            .await
            .context(IoUnexpectedSnafu {
                message: format!("Failed to check existence of '{path}'"),
            })
    }

    /// Check if a directory exists.
    pub async fn exists_dir(&self, path: &str) -> Result<bool> {
        if matches!(&self.backend, FileIOBackend::ReadOnly(_)) {
            return self.exists(path).await;
        }
        let FileIOBackend::Native(storage) = &self.backend else {
            unreachable!()
        };
        let (op, relative_path) = storage.create(path)?;
        let dir_path = normalize_root(relative_path.as_ref());

        op.exists(&dir_path).await.context(IoUnexpectedSnafu {
            message: format!("Failed to check existence of directory '{path}'"),
        })
    }

    /// Delete a file.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L139>
    pub async fn delete_file(&self, path: &str) -> Result<()> {
        let FileIOBackend::Native(storage) = &self.backend else {
            return Err(read_only_write_error("delete file"));
        };
        let (op, relative_path) = storage.create(path)?;
        let cache_path = cache_object_path(&op, relative_path.as_ref());

        op.delete(relative_path.as_ref())
            .await
            .context(IoUnexpectedSnafu {
                message: format!("Failed to delete file '{path}'"),
            })?;
        if let Some(cache) = self.cache.as_ref().filter(|cache| cache.is_cacheable(path)) {
            cache.invalidate_path(&cache_path).await;
        }

        Ok(())
    }

    /// Delete a dir recursively.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L139>
    pub async fn delete_dir(&self, path: &str) -> Result<()> {
        let FileIOBackend::Native(storage) = &self.backend else {
            return Err(read_only_write_error("delete directory"));
        };
        let (op, relative_path) = storage.create(path)?;
        let cache_path = cache_object_path(&op, relative_path.as_ref());

        op.delete_with(relative_path.as_ref())
            .recursive(true)
            .await
            .context(IoUnexpectedSnafu {
                message: format!("Failed to delete directory '{path}'"),
            })?;
        if let Some(cache) = &self.cache {
            cache.invalidate_prefix(&cache_path).await;
        }

        Ok(())
    }

    /// Make the given file and all non-existent parents into directories.
    ///
    /// Has the semantics of Unix 'mkdir -p'. Existence of the directory hierarchy is not an error.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L150>
    pub async fn mkdirs(&self, path: &str) -> Result<()> {
        let FileIOBackend::Native(storage) = &self.backend else {
            return Err(read_only_write_error("create directory"));
        };
        let (op, relative_path) = storage.create(path)?;
        // Opendal create_dir expects the path to end with `/` to indicate a directory.
        let dir_path = normalize_root(relative_path.as_ref());
        op.create_dir(&dir_path).await.context(IoUnexpectedSnafu {
            message: format!("Failed to create directory '{path}'"),
        })?;

        Ok(())
    }

    /// Copy a file from src to dst.
    ///
    /// Overwrites dst if it already exists.
    pub async fn copy_file(&self, src: &str, dst: &str) -> Result<()> {
        if self.is_read_only() {
            return Err(read_only_write_error("copy file"));
        }
        let input = self.new_input(src)?;
        let bytes = input.read().await?;
        let output = self.new_output(dst)?;
        output.write(bytes).await?;
        Ok(())
    }

    /// Renames the file/directory src to dst.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L159>
    pub async fn rename(&self, src: &str, dst: &str) -> Result<()> {
        let FileIOBackend::Native(storage) = &self.backend else {
            return Err(read_only_write_error("rename file or directory"));
        };
        let (op_src, relative_path_src) = storage.create(src)?;
        let (op_dst, relative_path_dst) = storage.create(dst)?;
        let cache_path_src = cache_object_path(&op_src, relative_path_src.as_ref());
        let cache_path_dst = cache_object_path(&op_dst, relative_path_dst.as_ref());

        op_src
            .rename(relative_path_src.as_ref(), relative_path_dst.as_ref())
            .await
            .context(IoUnexpectedSnafu {
                message: format!("Failed to rename '{src}' to '{dst}'"),
            })?;
        if let Some(cache) = &self.cache {
            cache.invalidate_prefix(&cache_path_src).await;
            cache.invalidate_prefix(&cache_path_dst).await;
        }

        Ok(())
    }

    fn check_active(&self) -> crate::Result<()> {
        if let Some(control) = &self.control {
            control.check_active()?;
        }
        Ok(())
    }

    fn checkpoint(&self) -> crate::Result<()> {
        if let Some(control) = &self.control {
            control.checkpoint()?;
        }
        Ok(())
    }

    async fn collect_read_only_listing(
        &self,
        backend: &Arc<dyn ReadOnlyFileIO>,
        path: &str,
        recursive: bool,
    ) -> crate::Result<RetainedRead<Vec<FileStatus>>> {
        self.check_active()?;
        let mut stream = backend.list(path, recursive).await?;
        let mut statuses = Vec::new();
        let mut retention = ReadRetention::default();
        while let Some(status) = stream.next().await {
            self.checkpoint()?;
            let status = status?;
            let retained = (std::mem::size_of::<FileStatus>() + status.path.len()) as u64;
            if let Some(control) = &self.control {
                retention.push(control.try_reserve(retained.max(1))?);
            }
            statuses.push(status);
        }
        self.checkpoint()?;
        Ok(RetainedRead::new(statuses, retention))
    }
}

fn read_only_write_error(operation: &str) -> Error {
    Error::IoUnsupported {
        message: format!("{operation} is disabled for host-injected read-only FileIO"),
    }
}

fn status_path(base_path: &str, entry_path: &str) -> String {
    if base_path.ends_with('/') || entry_path.starts_with('/') {
        format!("{base_path}{entry_path}")
    } else {
        format!("{base_path}/{entry_path}")
    }
}

fn cache_object_path(op: &Operator, relative_path: &str) -> String {
    let info = op.info();
    format!(
        "{}\0{}\0{}\0{}",
        info.scheme(),
        info.name(),
        info.root(),
        relative_path.trim_start_matches('/')
    )
}

/// Whether `path` begins with a Windows drive specifier such as `C:\` or `C:/`.
pub(crate) fn looks_like_windows_drive_path(path: &str) -> bool {
    let bytes = path.as_bytes();
    bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'\\' | b'/')
}

#[derive(Debug)]
pub struct FileIOBuilder {
    scheme_str: Option<String>,
    props: HashMap<String, String>,
    cache: Option<Arc<LocalCache>>,
}

impl FileIOBuilder {
    pub fn new(scheme_str: impl ToString) -> Self {
        Self {
            scheme_str: Some(scheme_str.to_string()),
            props: HashMap::default(),
            cache: None,
        }
    }

    pub(crate) fn into_parts(self) -> (String, HashMap<String, String>) {
        (self.scheme_str.unwrap_or_default(), self.props)
    }

    pub fn with_prop(mut self, key: impl ToString, value: impl ToString) -> Self {
        self.props.insert(key.to_string(), value.to_string());
        self
    }

    pub fn with_props(
        mut self,
        args: impl IntoIterator<Item = (impl ToString, impl ToString)>,
    ) -> Self {
        self.props
            .extend(args.into_iter().map(|e| (e.0.to_string(), e.1.to_string())));
        self
    }

    pub(crate) fn with_local_cache(mut self, cache: Arc<LocalCache>) -> Self {
        self.cache = Some(cache);
        self
    }

    pub fn build(self) -> crate::Result<FileIO> {
        let cache = self.cache.clone();
        let storage = Storage::build(self)?;
        Ok(FileIO {
            backend: FileIOBackend::Native(Arc::new(storage)),
            cache,
            control: None,
        })
    }
}

#[async_trait::async_trait]
pub trait FileRead: Send + Sync + Unpin + 'static {
    async fn read(&self, range: Range<u64>) -> crate::Result<Bytes>;

    /// Return the request-local resource authority carried by this reader.
    ///
    /// Format adapters use this hook when they must allocate a buffer in
    /// addition to the bytes retained by `read`, for example when a Parquet
    /// range spans multiple fetched chunks.
    fn read_control(&self) -> Option<Arc<dyn ReadControl>> {
        None
    }
}

#[async_trait::async_trait]
impl FileRead for opendal::Reader {
    async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
        Ok(opendal::Reader::read(self, range).await?.to_bytes())
    }
}

enum InputFileReader {
    Direct(opendal::Reader),
    Cached(CachedFileReader),
    ReadOnly {
        backend: Arc<dyn ReadOnlyFileIO>,
        control: Arc<dyn ReadControl>,
        path: String,
    },
}

struct ReservedBytes {
    bytes: Bytes,
    _reservation: Box<dyn ReadReservation>,
}

impl AsRef<[u8]> for ReservedBytes {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

pub(crate) fn retain_bytes(bytes: Bytes, reservation: Box<dyn ReadReservation>) -> Bytes {
    Bytes::from_owner(ReservedBytes {
        bytes,
        _reservation: reservation,
    })
}

#[async_trait::async_trait]
impl FileRead for InputFileReader {
    async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
        match self {
            Self::Direct(reader) => FileRead::read(reader, range).await,
            Self::Cached(reader) => FileRead::read(reader, range).await,
            Self::ReadOnly {
                backend,
                control,
                path,
            } => {
                control.check_active()?;
                let requested =
                    range
                        .end
                        .checked_sub(range.start)
                        .ok_or_else(|| Error::ConfigInvalid {
                            message: "read range end precedes start".to_string(),
                        })?;
                let reservation = control.try_reserve(requested.max(1))?;
                let bytes = backend.read(path, range).await?;
                if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > requested {
                    return Err(Error::DataInvalid {
                        message: format!(
                            "host read-only FileIO returned {} bytes for a {requested}-byte range",
                            bytes.len()
                        ),
                        source: None,
                    });
                }
                control.checkpoint()?;
                Ok(retain_bytes(bytes, reservation))
            }
        }
    }

    fn read_control(&self) -> Option<Arc<dyn ReadControl>> {
        match self {
            Self::ReadOnly { control, .. } => Some(control.clone()),
            Self::Direct(_) | Self::Cached(_) => None,
        }
    }
}

#[async_trait::async_trait]
pub trait FileWrite: Send + Unpin + 'static {
    async fn write(&mut self, bs: Bytes) -> crate::Result<()>;

    async fn close(&mut self) -> crate::Result<()>;
}

#[async_trait::async_trait]
impl FileWrite for opendal::Writer {
    async fn write(&mut self, bs: Bytes) -> crate::Result<()> {
        Ok(opendal::Writer::write(self, bs).await?)
    }

    async fn close(&mut self) -> crate::Result<()> {
        opendal::Writer::close(self).await?;
        Ok(())
    }
}

struct CacheInvalidatingWriter {
    delegate: Box<dyn FileWrite>,
    cache: Arc<LocalCache>,
    path: String,
}

#[async_trait::async_trait]
impl FileWrite for CacheInvalidatingWriter {
    async fn write(&mut self, bs: Bytes) -> crate::Result<()> {
        self.delegate.write(bs).await
    }

    async fn close(&mut self) -> crate::Result<()> {
        self.delegate.close().await?;
        self.cache.invalidate_path(&self.path).await;
        Ok(())
    }
}

/// Async streaming writer trait for format-level writers (e.g. parquet).
pub trait AsyncFileWrite: tokio::io::AsyncWrite + Unpin + Send {}

impl<T: tokio::io::AsyncWrite + Unpin + Send> AsyncFileWrite for T {}

struct CacheInvalidatingAsyncWriter {
    delegate: Box<dyn AsyncFileWrite>,
    cache: Arc<LocalCache>,
    path: String,
    delegate_shutdown: bool,
    invalidation: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl tokio::io::AsyncWrite for CacheInvalidatingAsyncWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut *self.delegate).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut *self.delegate).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        if !self.delegate_shutdown {
            match Pin::new(&mut *self.delegate).poll_shutdown(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => {
                    self.delegate_shutdown = true;
                    let cache = self.cache.clone();
                    let path = self.path.clone();
                    self.invalidation =
                        Some(Box::pin(async move { cache.invalidate_path(&path).await }));
                }
            }
        }

        if let Some(invalidation) = &mut self.invalidation {
            match invalidation.as_mut().poll(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(()) => self.invalidation = None,
            }
        }
        Poll::Ready(Ok(()))
    }
}

#[derive(Clone, Debug)]
pub struct FileStatus {
    pub size: u64,
    pub is_dir: bool,
    pub path: String,
    pub last_modified: Option<DateTime<Utc>>,
}

#[derive(Debug)]
pub struct InputFile {
    source: InputFileSource,
    path: String,
    cache_path: String,
    cache: Option<Arc<LocalCache>>,
}

#[derive(Debug)]
enum InputFileSource {
    Native {
        op: Operator,
        relative_path: String,
    },
    ReadOnly {
        backend: Arc<dyn ReadOnlyFileIO>,
        control: Arc<dyn ReadControl>,
    },
}

impl InputFile {
    pub fn location(&self) -> &str {
        &self.path
    }

    pub async fn exists(&self) -> crate::Result<bool> {
        match &self.source {
            InputFileSource::Native { op, relative_path } => Ok(op.exists(relative_path).await?),
            InputFileSource::ReadOnly { backend, control } => {
                control.check_active()?;
                let exists = backend.exists(&self.path).await?;
                control.checkpoint()?;
                Ok(exists)
            }
        }
    }

    pub async fn metadata(&self) -> crate::Result<FileStatus> {
        if let InputFileSource::ReadOnly { backend, control } = &self.source {
            control.check_active()?;
            let status = backend.stat(&self.path).await?;
            control.checkpoint()?;
            return Ok(status);
        }
        let InputFileSource::Native { op, relative_path } = &self.source else {
            unreachable!()
        };
        let meta = op.stat(relative_path).await?;

        Ok(FileStatus {
            size: meta.content_length(),
            is_dir: meta.is_dir(),
            path: self.path.clone(),
            last_modified: meta
                .last_modified()
                .map(|v| DateTime::<Utc>::from(SystemTime::from(v))),
        })
    }

    pub async fn read(&self) -> crate::Result<Bytes> {
        let Some(cache) = &self.cache else {
            return match &self.source {
                InputFileSource::Native { op, relative_path } => {
                    Ok(op.read(relative_path).await?.to_bytes())
                }
                InputFileSource::ReadOnly { backend, control } => {
                    control.check_active()?;
                    let status = backend.stat(&self.path).await?;
                    let reservation = control.try_reserve(status.size.max(1))?;
                    let bytes = backend.read(&self.path, 0..status.size).await?;
                    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > status.size {
                        return Err(Error::DataInvalid {
                            message: format!(
                                "host read-only FileIO returned {} bytes for a {}-byte file",
                                bytes.len(),
                                status.size
                            ),
                            source: None,
                        });
                    }
                    control.checkpoint()?;
                    Ok(retain_bytes(bytes, reservation))
                }
            };
        };
        let read_token = cache.read_token(&self.cache_path);
        let size = if let Some(size) = cache.file_size(&self.cache_path, &read_token).await {
            size
        } else {
            let InputFileSource::Native { op, relative_path } = &self.source else {
                unreachable!()
            };
            let size = op.stat(relative_path).await?.content_length();
            cache
                .put_file_size(&self.cache_path, size, &read_token)
                .await;
            size
        };
        let InputFileSource::Native { op, relative_path } = &self.source else {
            unreachable!()
        };
        let delegate = Arc::new(op.reader(relative_path).await?);
        CachedFileReader::new_with_token(
            delegate,
            &self.cache_path,
            size,
            cache.clone(),
            read_token,
        )
        .read_full()
        .await
    }

    pub async fn reader(&self) -> crate::Result<impl FileRead> {
        if let InputFileSource::ReadOnly { backend, control } = &self.source {
            control.check_active()?;
            return Ok(InputFileReader::ReadOnly {
                backend: backend.clone(),
                control: control.clone(),
                path: self.path.clone(),
            });
        }
        let InputFileSource::Native { op, relative_path } = &self.source else {
            unreachable!()
        };
        let reader = op.reader(relative_path).await?;
        let Some(cache) = &self.cache else {
            return Ok(InputFileReader::Direct(reader));
        };
        let read_token = cache.read_token(&self.cache_path);
        let size = if let Some(size) = cache.file_size(&self.cache_path, &read_token).await {
            size
        } else {
            let size = op.stat(relative_path).await?.content_length();
            cache
                .put_file_size(&self.cache_path, size, &read_token)
                .await;
            size
        };
        Ok(InputFileReader::Cached(CachedFileReader::new_with_token(
            Arc::new(reader),
            &self.cache_path,
            size,
            cache.clone(),
            read_token,
        )))
    }
}

#[derive(Debug, Clone)]
pub struct OutputFile {
    op: Operator,
    path: String,
    /// The opendal-relative path (see [`FileIO::new_output`]); not necessarily a
    /// suffix of `path`, since local paths are separator-normalized.
    relative_path: String,
    cache_path: String,
    cache: Option<Arc<LocalCache>>,
}

impl OutputFile {
    pub fn location(&self) -> &str {
        &self.path
    }

    pub async fn exists(&self) -> crate::Result<bool> {
        Ok(self.op.exists(&self.relative_path).await?)
    }

    pub fn to_input_file(self) -> InputFile {
        let cache = self.cache.filter(|cache| cache.is_cacheable(&self.path));
        InputFile {
            source: InputFileSource::Native {
                op: self.op,
                relative_path: self.relative_path,
            },
            path: self.path,
            cache_path: self.cache_path,
            cache,
        }
    }

    pub async fn write(&self, bs: Bytes) -> crate::Result<()> {
        let mut writer = self.writer().await?;
        writer.write(bs).await?;
        writer.close().await
    }

    pub async fn writer(&self) -> crate::Result<Box<dyn FileWrite>> {
        let writer: Box<dyn FileWrite> = Box::new(self.opendal_writer().await?);
        let Some(cache) = &self.cache else {
            return Ok(writer);
        };
        Ok(Box::new(CacheInvalidatingWriter {
            delegate: writer,
            cache: cache.clone(),
            path: self.cache_path.clone(),
        }))
    }

    /// Get an async streaming writer for format-level writes (e.g. parquet).
    pub(crate) async fn async_writer(&self) -> crate::Result<Box<dyn AsyncFileWrite>> {
        let writer: Box<dyn AsyncFileWrite> = Box::new(
            self.opendal_writer()
                .await?
                .into_futures_async_write()
                .compat_write(),
        );
        let Some(cache) = &self.cache else {
            return Ok(writer);
        };
        Ok(Box::new(CacheInvalidatingAsyncWriter {
            delegate: writer,
            cache: cache.clone(),
            path: self.cache_path.clone(),
            delegate_shutdown: false,
            invalidation: None,
        }))
    }

    async fn opendal_writer(&self) -> crate::Result<opendal::Writer> {
        Ok(self.op.writer(&self.relative_path).await?)
    }
}

#[cfg(test)]
mod file_action_test {
    use std::collections::{BTreeSet, HashMap, HashSet};
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tempfile::tempdir;

    use super::*;
    use bytes::Bytes;

    #[derive(Debug)]
    struct ObservedControl {
        current: Arc<AtomicU64>,
        limit: u64,
    }

    impl ReadControl for ObservedControl {
        fn check_active(&self) -> crate::Result<()> {
            Ok(())
        }

        fn checkpoint(&self) -> crate::Result<()> {
            Ok(())
        }

        fn try_reserve(&self, bytes: u64) -> crate::Result<Box<dyn ReadReservation>> {
            let mut current = self.current.load(Ordering::Acquire);
            loop {
                let Some(next) = current.checked_add(bytes) else {
                    return Err(crate::Error::UnexpectedError {
                        message: "test listing reservation overflowed".to_string(),
                        source: None,
                    });
                };
                if next > self.limit {
                    return Err(crate::Error::UnexpectedError {
                        message: "test listing reservation exceeded its limit".to_string(),
                        source: None,
                    });
                }
                match self.current.compare_exchange_weak(
                    current,
                    next,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        return Ok(Box::new(ObservedReservation {
                            current: Arc::clone(&self.current),
                            bytes,
                        }));
                    }
                    Err(actual) => current = actual,
                }
            }
        }
    }

    #[derive(Debug)]
    struct ObservedReservation {
        current: Arc<AtomicU64>,
        bytes: u64,
    }

    impl ReadReservation for ObservedReservation {
        fn bytes(&self) -> u64 {
            self.bytes
        }
    }

    impl Drop for ObservedReservation {
        fn drop(&mut self) {
            self.current.fetch_sub(self.bytes, Ordering::AcqRel);
        }
    }

    #[derive(Debug, Default)]
    pub(super) struct FixedReadOnlyFileIo {
        pub(super) listings: HashMap<String, Vec<FileStatus>>,
        pub(super) existing: HashSet<String>,
    }

    #[async_trait::async_trait]
    impl ReadOnlyFileIO for FixedReadOnlyFileIo {
        async fn stat(&self, path: &str) -> crate::Result<FileStatus> {
            if self.existing.contains(path) {
                Ok(FileStatus {
                    size: 0,
                    is_dir: path.ends_with('/'),
                    path: path.to_string(),
                    last_modified: None,
                })
            } else {
                Err(crate::Error::IoUnsupported {
                    message: format!("test path does not exist: {path}"),
                })
            }
        }

        async fn exists(&self, path: &str) -> crate::Result<bool> {
            Ok(self.existing.contains(path))
        }

        async fn read(&self, path: &str, _range: Range<u64>) -> crate::Result<Bytes> {
            Err(crate::Error::IoUnsupported {
                message: format!("test backend cannot read {path}"),
            })
        }

        async fn list(
            &self,
            path: &str,
            _recursive: bool,
        ) -> crate::Result<crate::io::FileStatusStream> {
            let statuses = self.listings.get(path).cloned().unwrap_or_default();
            Ok(Box::pin(futures::stream::iter(
                statuses.into_iter().map(Ok),
            )))
        }
    }

    pub(super) fn status(path: &str, is_dir: bool) -> FileStatus {
        FileStatus {
            size: 0,
            is_dir,
            path: path.to_string(),
            last_modified: None,
        }
    }

    pub(super) fn controlled_file_io(
        backend: FixedReadOnlyFileIo,
        limit: u64,
    ) -> (FileIO, Arc<AtomicU64>) {
        let current = Arc::new(AtomicU64::new(0));
        let control = ObservedControl {
            current: Arc::clone(&current),
            limit,
        };
        (
            FileIO::from_read_only(Arc::new(backend), Arc::new(control)),
            current,
        )
    }

    fn setup_memory_file_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    fn setup_fs_file_io() -> FileIO {
        FileIOBuilder::new("file").build().unwrap()
    }

    fn local_file_path(path: &std::path::Path) -> String {
        let normalized = path.to_string_lossy().replace('\\', "/");
        if normalized.starts_with('/') {
            format!("file:{normalized}")
        } else {
            format!("file:/{normalized}")
        }
    }

    async fn common_test_get_status(file_io: &FileIO, path: &str) {
        let output = file_io.new_output(path).unwrap();
        let mut writer = output.writer().await.unwrap();
        writer.write(Bytes::from("hello world")).await.unwrap();
        writer.close().await.unwrap();

        let status = file_io.get_status(path).await.unwrap();
        assert_eq!(status.size, 11);

        file_io.delete_file(path).await.unwrap();
    }

    async fn common_test_exists(file_io: &FileIO, path: &str) {
        let output = file_io.new_output(path).unwrap();
        let mut writer = output.writer().await.unwrap();
        writer.write(Bytes::from("hello world")).await.unwrap();
        writer.close().await.unwrap();

        let exists = file_io.exists(path).await.unwrap();
        assert!(exists);

        file_io.delete_file(path).await.unwrap();
    }

    async fn common_test_delete_file(file_io: &FileIO, path: &str) {
        let output = file_io.new_output(path).unwrap();
        let mut writer = output.writer().await.unwrap();
        writer.write(Bytes::from("hello world")).await.unwrap();
        writer.close().await.unwrap();

        file_io.delete_file(path).await.unwrap();

        let exists = file_io.exists(path).await.unwrap();
        assert!(!exists);
    }

    async fn common_test_mkdirs(file_io: &FileIO, dir_path: &str) {
        file_io.mkdirs(dir_path).await.unwrap();

        let exists = file_io.exists(dir_path).await.unwrap();
        assert!(exists);

        let _ = fs::remove_dir_all(dir_path.strip_prefix("file:/").unwrap());
    }

    async fn common_test_rename(file_io: &FileIO, src: &str, dst: &str) {
        let output = file_io.new_output(src).unwrap();
        let mut writer = output.writer().await.unwrap();
        writer.write(Bytes::from("hello world")).await.unwrap();
        writer.close().await.unwrap();

        file_io.rename(src, dst).await.unwrap();

        let exists_old = file_io.exists(src).await.unwrap();
        let exists_new = file_io.exists(dst).await.unwrap();
        assert!(!exists_old);
        assert!(exists_new);

        file_io.delete_file(dst).await.unwrap();
    }

    async fn common_test_list_status_paths(file_io: &FileIO, dir_path: &str) {
        if let Some(local_dir) = dir_path.strip_prefix("file:/") {
            let _ = fs::remove_dir_all(local_dir);
        }

        file_io.mkdirs(dir_path).await.unwrap();

        let file_a = format!("{dir_path}a.txt");
        let file_b = format!("{dir_path}b.txt");
        for file in [&file_a, &file_b] {
            file_io
                .new_output(file)
                .unwrap()
                .write(Bytes::from("test data"))
                .await
                .unwrap();
        }

        let statuses = file_io.list_status(dir_path).await.unwrap();
        assert_eq!(statuses.len(), 2);

        let expected_paths: BTreeSet<String> =
            [file_a.clone(), file_b.clone()].into_iter().collect();
        let actual_paths: BTreeSet<String> =
            statuses.iter().map(|status| status.path.clone()).collect();
        assert_eq!(
            actual_paths, expected_paths,
            "list_status should return exact entry paths"
        );

        file_io.delete_dir(dir_path).await.unwrap();
    }

    #[tokio::test]
    async fn test_delete_file_memory() {
        let file_io = setup_memory_file_io();
        common_test_delete_file(&file_io, "memory:/test_file_delete_mem").await;
    }

    #[tokio::test]
    async fn test_empty_path_should_return_error_for_exists_fs() {
        let file_io = setup_fs_file_io();
        let result = file_io.exists("").await;
        assert!(matches!(result, Err(Error::ConfigInvalid { .. })));
    }

    #[tokio::test]
    async fn test_empty_path_should_return_error_for_exists_memory() {
        let file_io = setup_memory_file_io();
        let result = file_io.exists("").await;
        assert!(matches!(result, Err(Error::ConfigInvalid { .. })));
    }

    #[tokio::test]
    async fn test_exists_dir_memory() {
        let file_io = setup_memory_file_io();

        file_io.mkdirs("memory:/empty").await.unwrap();
        assert!(file_io.exists_dir("memory:/empty").await.unwrap());

        file_io
            .new_output("memory:/markerless/child")
            .unwrap()
            .write(Bytes::from("data"))
            .await
            .unwrap();
        assert!(file_io.exists_dir("memory:/markerless").await.unwrap());

        assert!(!file_io.exists_dir("memory:/missing").await.unwrap());
    }

    #[tokio::test]
    async fn test_memory_operator_reuse_across_file_io_calls() {
        let file_io = setup_memory_file_io();
        let path = "memory:/tmp/reuse_case";
        let dir = "memory:/tmp/";

        file_io
            .new_output(path)
            .unwrap()
            .write(Bytes::from("data"))
            .await
            .unwrap();

        assert!(file_io.exists(path).await.unwrap());
        assert_eq!(file_io.get_status(path).await.unwrap().size, 4);
        assert!(file_io
            .list_status(dir)
            .await
            .unwrap()
            .iter()
            .any(|status| status.path == path));

        file_io.delete_dir(dir).await.unwrap();
    }

    #[tokio::test]
    async fn test_memory_operator_not_shared_between_file_io_instances() {
        let file_io_1 = setup_memory_file_io();
        let file_io_2 = setup_memory_file_io();
        let path = "memory:/tmp/reuse_isolation_case";

        file_io_1
            .new_output(path)
            .unwrap()
            .write(Bytes::from("data"))
            .await
            .unwrap();

        assert!(file_io_1.exists(path).await.unwrap());
        assert!(!file_io_2.exists(path).await.unwrap());
    }

    #[tokio::test]
    async fn test_get_status_fs() {
        let file_io = setup_fs_file_io();
        common_test_get_status(&file_io, "file:/tmp/test_file_get_status_fs").await;
    }

    #[tokio::test]
    async fn test_exists_fs() {
        let file_io = setup_fs_file_io();
        common_test_exists(&file_io, "file:/tmp/test_file_exists_fs").await;
    }

    #[tokio::test]
    async fn test_delete_file_fs() {
        let file_io = setup_fs_file_io();
        common_test_delete_file(&file_io, "file:/tmp/test_file_delete_fs").await;
    }

    #[tokio::test]
    async fn test_mkdirs_fs() {
        let file_io = setup_fs_file_io();
        common_test_mkdirs(&file_io, "file:/tmp/test_fs_dir/").await;
    }

    #[tokio::test]
    async fn test_rename_fs() {
        let file_io = setup_fs_file_io();
        common_test_rename(
            &file_io,
            "file:/tmp/test_file_fs_z",
            "file:/tmp/new_test_file_fs_o",
        )
        .await;
    }

    #[tokio::test]
    async fn test_list_status_fs_should_return_entry_paths() {
        let file_io = setup_fs_file_io();
        common_test_list_status_paths(&file_io, "file:/tmp/test_list_status_paths_fs/").await;
    }

    #[test]
    fn test_from_path_detects_local_fs_path() {
        let dir = tempdir().unwrap();
        let file_io = FileIO::from_path(dir.path().to_string_lossy())
            .unwrap()
            .build()
            .unwrap();
        let path = local_file_path(&dir.path().join("from_path_detects_local_fs_path.txt"));

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            file_io
                .new_output(&path)
                .unwrap()
                .write(Bytes::from("data"))
                .await
                .unwrap();
            assert!(file_io.exists(&path).await.unwrap());
        });
    }
}

#[cfg(all(
    test,
    any(
        feature = "storage-cos",
        feature = "storage-obs",
        feature = "storage-gcs",
        feature = "storage-azdls"
    )
))]
mod object_storage_path_test {
    use super::*;

    fn assert_relative_paths(file_io: &FileIO, path: &str, expected_relative_path: &str) {
        let input = file_io.new_input(path).unwrap();
        assert_eq!(input.location(), path);
        let InputFileSource::Native { relative_path, .. } = &input.source else {
            panic!("storage feature must build a native input")
        };
        assert_eq!(relative_path, expected_relative_path);

        let output = file_io.new_output(path).unwrap();
        assert_eq!(output.location(), path);
        assert_eq!(output.relative_path, expected_relative_path);

        let FileIOBackend::Native(storage) = &file_io.backend else {
            panic!("storage feature must build a native backend")
        };
        let (_op, relative_path) = storage.create(path).unwrap();
        assert_eq!(relative_path.as_ref(), expected_relative_path);

        let base_path = &path[..path.len() - relative_path.len()];
        assert_eq!(format!("{base_path}{relative_path}"), path);
    }

    #[cfg(feature = "storage-azdls")]
    #[test]
    fn test_azdls_root_status_path_without_trailing_slash() {
        assert_eq!(
            status_path(
                "abfs://filesystem@account.dfs.core.windows.net",
                "warehouse/"
            ),
            "abfs://filesystem@account.dfs.core.windows.net/warehouse/"
        );
        assert_eq!(
            status_path(
                "abfs://filesystem@account.dfs.core.windows.net/",
                "warehouse/"
            ),
            "abfs://filesystem@account.dfs.core.windows.net/warehouse/"
        );
    }

    #[cfg(feature = "storage-cos")]
    #[test]
    fn test_cos_file_io_relative_paths_and_scheme_aliases() {
        for scheme in ["cosn", "cos"] {
            let path = format!("{scheme}://bucket/warehouse/table/data.parquet");
            let dir_path = format!("{scheme}://bucket/warehouse/table/");
            let file_io = FileIO::from_path(&path)
                .unwrap()
                .with_props([
                    ("fs.cosn.endpoint", "https://cos.ap-shanghai.myqcloud.com"),
                    ("fs.cosn.userinfo.secretId", "secret-id"),
                    ("fs.cosn.userinfo.secretKey", "secret-key"),
                    ("fs.cosn.disable-config-load", "true"),
                ])
                .build()
                .unwrap();

            assert_relative_paths(&file_io, &path, "warehouse/table/data.parquet");
            assert_relative_paths(&file_io, &dir_path, "warehouse/table/");
        }
    }

    #[cfg(feature = "storage-obs")]
    #[test]
    fn test_obs_file_io_relative_paths() {
        let file_io = FileIO::from_path("obs://bucket/warehouse")
            .unwrap()
            .with_props([
                (
                    "fs.obs.endpoint",
                    "https://obs.cn-north-4.myhuaweicloud.com",
                ),
                ("fs.obs.access.key", "access-key"),
                ("fs.obs.secret.key", "secret-key"),
            ])
            .build()
            .unwrap();

        assert_relative_paths(
            &file_io,
            "obs://bucket/warehouse/table/data.parquet",
            "warehouse/table/data.parquet",
        );
        assert_relative_paths(
            &file_io,
            "obs://bucket/warehouse/table/",
            "warehouse/table/",
        );
    }

    #[cfg(feature = "storage-gcs")]
    #[test]
    fn test_gcs_file_io_relative_paths_and_scheme_aliases() {
        for scheme in ["gs", "gcs"] {
            let path = format!("{scheme}://bucket/warehouse/table/data.parquet");
            let dir_path = format!("{scheme}://bucket/warehouse/table/");
            let file_io = FileIO::from_path(&path)
                .unwrap()
                .with_props([
                    ("gcs.allow-anonymous", "true"),
                    ("gcs.disable-config-load", "true"),
                    ("gcs.disable-vm-metadata", "true"),
                ])
                .build()
                .unwrap();

            assert_relative_paths(&file_io, &path, "warehouse/table/data.parquet");
            assert_relative_paths(&file_io, &dir_path, "warehouse/table/");
        }
    }

    #[cfg(feature = "storage-azdls")]
    #[test]
    fn test_azdls_file_io_relative_paths_and_scheme_aliases() {
        for scheme in ["abfs", "abfss"] {
            let path = format!(
                "{scheme}://filesystem@account.dfs.core.windows.net/warehouse/data.parquet"
            );
            let dir_path = format!("{scheme}://filesystem@account.dfs.core.windows.net/warehouse/");
            let file_io = FileIO::from_path(&path)
                .unwrap()
                .with_prop("azure.account-key", "account-key")
                .build()
                .unwrap();

            assert_relative_paths(&file_io, &path, "warehouse/data.parquet");
            assert_relative_paths(&file_io, &dir_path, "warehouse/");
        }
    }
}

#[cfg(test)]
mod input_output_test {
    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use super::file_action_test::{controlled_file_io, status, FixedReadOnlyFileIo};
    use super::*;
    use crate::common::{CatalogOptions, Options};
    use crate::io::cache::{LocalCache, LocalCacheConfig};
    use bytes::Bytes;

    fn setup_memory_file_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    fn setup_fs_file_io() -> FileIO {
        FileIOBuilder::new("file").build().unwrap()
    }

    fn setup_cached_fs_file_io(cache_directory: &std::path::Path) -> FileIO {
        let mut options = Options::new();
        options.set(CatalogOptions::LOCAL_CACHE_ENABLED, "true");
        options.set(
            CatalogOptions::LOCAL_CACHE_DIR,
            cache_directory.to_string_lossy(),
        );
        options.set(CatalogOptions::LOCAL_CACHE_BLOCK_SIZE, "4");
        let cache = Arc::new(
            LocalCache::new(LocalCacheConfig::from_options(&options).unwrap().unwrap()).unwrap(),
        );
        FileIOBuilder::new("file")
            .with_local_cache(cache)
            .build()
            .unwrap()
    }

    async fn common_test_output_file_write_and_read(file_io: &FileIO, path: &str) {
        let output = file_io.new_output(path).unwrap();
        let mut writer = output.writer().await.unwrap();
        writer.write(Bytes::from("hello world")).await.unwrap();
        writer.close().await.unwrap();

        let input = output.to_input_file();
        let content = input.read().await.unwrap();

        assert_eq!(&content[..], b"hello world");

        file_io.delete_file(path).await.unwrap();
    }

    async fn common_test_output_file_exists(file_io: &FileIO, path: &str) {
        let output = file_io.new_output(path).unwrap();
        let mut writer = output.writer().await.unwrap();
        writer.write(Bytes::from("hello world")).await.unwrap();
        writer.close().await.unwrap();

        let exists = output.exists().await.unwrap();
        assert!(exists);

        file_io.delete_file(path).await.unwrap();
    }

    async fn common_test_input_file_metadata(file_io: &FileIO, path: &str) {
        let output = file_io.new_output(path).unwrap();
        let mut writer = output.writer().await.unwrap();
        writer.write(Bytes::from("hello world")).await.unwrap();
        writer.close().await.unwrap();

        let input = output.to_input_file();
        let metadata = input.metadata().await.unwrap();

        assert_eq!(metadata.size, 11);

        file_io.delete_file(path).await.unwrap();
    }

    async fn common_test_input_file_partial_read(file_io: &FileIO, path: &str) {
        let output = file_io.new_output(path).unwrap();
        let mut writer = output.writer().await.unwrap();
        writer.write(Bytes::from("hello world")).await.unwrap();
        writer.close().await.unwrap();

        let input = output.to_input_file();
        let reader = input.reader().await.unwrap();
        let partial_content = reader.read(0..5).await.unwrap(); // read "hello"

        assert_eq!(&partial_content[..], b"hello");

        file_io.delete_file(path).await.unwrap();
    }

    #[tokio::test]
    async fn test_output_file_write_and_read_memory() {
        let file_io = setup_memory_file_io();
        common_test_output_file_write_and_read(&file_io, "memory:/test_file_rw_mem").await;
    }

    #[tokio::test]
    async fn test_output_file_exists_memory() {
        let file_io = setup_memory_file_io();
        common_test_output_file_exists(&file_io, "memory:/test_file_exist_mem").await;
    }

    #[tokio::test]
    async fn test_input_file_metadata_memory() {
        let file_io = setup_memory_file_io();
        common_test_input_file_metadata(&file_io, "memory:/test_file_meta_mem").await;
    }

    #[tokio::test]
    async fn test_input_file_partial_read_memory() {
        let file_io = setup_memory_file_io();
        common_test_input_file_partial_read(&file_io, "memory:/test_file_part_read_mem").await;
    }

    #[tokio::test]
    async fn test_output_file_write_and_read_fs() {
        let file_io = setup_fs_file_io();
        common_test_output_file_write_and_read(&file_io, "file:/tmp/test_file_fs_rw").await;
    }

    #[tokio::test]
    async fn test_output_file_exists_fs() {
        let file_io = setup_fs_file_io();
        common_test_output_file_exists(&file_io, "file:/tmp/test_file_exists").await;
    }

    #[tokio::test]
    async fn test_input_file_metadata_fs() {
        let file_io = setup_fs_file_io();
        common_test_input_file_metadata(&file_io, "file:/tmp/test_file_meta").await;
    }

    #[tokio::test]
    async fn test_input_file_partial_read_fs() {
        let file_io = setup_fs_file_io();
        common_test_input_file_partial_read(&file_io, "file:/tmp/test_file_read_fs").await;
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_file_io_local_cache_serves_full_read_after_source_disappears() {
        let source_directory = tempfile::tempdir().unwrap();
        let cache_directory = tempfile::tempdir().unwrap();
        let source_path = source_directory.path().join("snapshot-1");
        std::fs::write(&source_path, b"cached metadata").unwrap();
        let location = format!("file:{}", source_path.display());
        let file_io = setup_cached_fs_file_io(cache_directory.path());

        assert_eq!(
            file_io.new_input(&location).unwrap().read().await.unwrap(),
            Bytes::from_static(b"cached metadata")
        );
        std::fs::remove_file(&source_path).unwrap();
        assert_eq!(
            file_io.new_input(&location).unwrap().read().await.unwrap(),
            Bytes::from_static(b"cached metadata")
        );
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_file_io_local_cache_serves_range_after_source_disappears() {
        let source_directory = tempfile::tempdir().unwrap();
        let cache_directory = tempfile::tempdir().unwrap();
        let source_path = source_directory.path().join("snapshot-1");
        std::fs::write(&source_path, b"cached metadata").unwrap();
        let location = format!("file:{}", source_path.display());
        let file_io = setup_cached_fs_file_io(cache_directory.path());

        let reader = file_io
            .new_input(&location)
            .unwrap()
            .reader()
            .await
            .unwrap();
        assert_eq!(
            reader.read(1..7).await.unwrap(),
            Bytes::from_static(b"ached ")
        );
        std::fs::remove_file(&source_path).unwrap();
        let reader = file_io
            .new_input(&location)
            .unwrap()
            .reader()
            .await
            .unwrap();
        assert_eq!(
            reader.read(1..7).await.unwrap(),
            Bytes::from_static(b"ached ")
        );
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_file_io_local_cache_invalidates_after_successful_write() {
        let source_directory = tempfile::tempdir().unwrap();
        let cache_directory = tempfile::tempdir().unwrap();
        let source_path = source_directory.path().join("snapshot-1");
        std::fs::write(&source_path, b"old metadata").unwrap();
        let location = format!("file:{}", source_path.display());
        let file_io = setup_cached_fs_file_io(cache_directory.path());

        assert_eq!(
            file_io.new_input(&location).unwrap().read().await.unwrap(),
            Bytes::from_static(b"old metadata")
        );
        file_io
            .new_output(&location)
            .unwrap()
            .write(Bytes::from_static(b"new metadata"))
            .await
            .unwrap();
        assert_eq!(
            file_io.new_input(&location).unwrap().read().await.unwrap(),
            Bytes::from_static(b"new metadata")
        );
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_file_io_local_cache_invalidates_equivalent_local_path_alias() {
        let source_directory = tempfile::tempdir().unwrap();
        let cache_directory = tempfile::tempdir().unwrap();
        let source_path = source_directory.path().join("snapshot-1");
        std::fs::write(&source_path, b"old metadata").unwrap();
        let file_location = format!("file:{}", source_path.display());
        let absolute_location = source_path.to_string_lossy();
        let file_io = setup_cached_fs_file_io(cache_directory.path());

        assert_eq!(
            file_io
                .new_input(&file_location)
                .unwrap()
                .read()
                .await
                .unwrap(),
            Bytes::from_static(b"old metadata")
        );
        file_io
            .new_output(absolute_location.as_ref())
            .unwrap()
            .write(Bytes::from_static(b"new metadata"))
            .await
            .unwrap();

        assert_eq!(
            file_io
                .new_input(&file_location)
                .unwrap()
                .read()
                .await
                .unwrap(),
            Bytes::from_static(b"new metadata")
        );
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_file_io_local_cache_invalidates_after_delete() {
        let source_directory = tempfile::tempdir().unwrap();
        let cache_directory = tempfile::tempdir().unwrap();
        let source_path = source_directory.path().join("snapshot-1");
        std::fs::write(&source_path, b"old metadata").unwrap();
        let location = format!("file:{}", source_path.display());
        let file_io = setup_cached_fs_file_io(cache_directory.path());

        assert_eq!(
            file_io.new_input(&location).unwrap().read().await.unwrap(),
            Bytes::from_static(b"old metadata")
        );
        file_io.delete_file(&location).await.unwrap();
        std::fs::write(&source_path, b"new metadata").unwrap();
        assert_eq!(
            file_io.new_input(&location).unwrap().read().await.unwrap(),
            Bytes::from_static(b"new metadata")
        );
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_file_io_local_cache_invalidates_after_delete_directory() {
        let source_directory = tempfile::tempdir().unwrap();
        let cache_directory = tempfile::tempdir().unwrap();
        let snapshot_directory = source_directory.path().join("snapshot");
        std::fs::create_dir(&snapshot_directory).unwrap();
        let source_path = snapshot_directory.join("snapshot-1");
        std::fs::write(&source_path, b"old metadata").unwrap();
        let location = format!("file:{}", source_path.display());
        let directory_location = format!("file:{}", snapshot_directory.display());
        let file_io = setup_cached_fs_file_io(cache_directory.path());

        assert_eq!(
            file_io.new_input(&location).unwrap().read().await.unwrap(),
            Bytes::from_static(b"old metadata")
        );
        file_io.delete_dir(&directory_location).await.unwrap();
        std::fs::create_dir(&snapshot_directory).unwrap();
        std::fs::write(&source_path, b"new metadata").unwrap();
        assert_eq!(
            file_io.new_input(&location).unwrap().read().await.unwrap(),
            Bytes::from_static(b"new metadata")
        );
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_file_io_local_cache_invalidates_copy_target() {
        let source_directory = tempfile::tempdir().unwrap();
        let cache_directory = tempfile::tempdir().unwrap();
        let source_path = source_directory.path().join("snapshot-1");
        let target_path = source_directory.path().join("snapshot-2");
        std::fs::write(&source_path, b"source value").unwrap();
        std::fs::write(&target_path, b"stale target").unwrap();
        let source_location = format!("file:{}", source_path.display());
        let target_location = format!("file:{}", target_path.display());
        let file_io = setup_cached_fs_file_io(cache_directory.path());

        assert_eq!(
            file_io
                .new_input(&target_location)
                .unwrap()
                .read()
                .await
                .unwrap(),
            Bytes::from_static(b"stale target")
        );
        file_io
            .copy_file(&source_location, &target_location)
            .await
            .unwrap();

        assert_eq!(
            file_io
                .new_input(&target_location)
                .unwrap()
                .read()
                .await
                .unwrap(),
            Bytes::from_static(b"source value")
        );
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_file_io_local_cache_invalidates_source_and_target_after_rename() {
        let source_directory = tempfile::tempdir().unwrap();
        let cache_directory = tempfile::tempdir().unwrap();
        let source_path = source_directory.path().join("snapshot-1");
        let target_path = source_directory.path().join("snapshot-2");
        std::fs::write(&source_path, b"source value").unwrap();
        std::fs::write(&target_path, b"target value").unwrap();
        let source_location = format!("file:{}", source_path.display());
        let target_location = format!("file:{}", target_path.display());
        let file_io = setup_cached_fs_file_io(cache_directory.path());

        assert_eq!(
            file_io
                .new_input(&source_location)
                .unwrap()
                .read()
                .await
                .unwrap(),
            Bytes::from_static(b"source value")
        );
        assert_eq!(
            file_io
                .new_input(&target_location)
                .unwrap()
                .read()
                .await
                .unwrap(),
            Bytes::from_static(b"target value")
        );
        file_io
            .rename(&source_location, &target_location)
            .await
            .unwrap();
        std::fs::write(&source_path, b"new source!!").unwrap();

        assert_eq!(
            file_io
                .new_input(&target_location)
                .unwrap()
                .read()
                .await
                .unwrap(),
            Bytes::from_static(b"source value")
        );
        assert_eq!(
            file_io
                .new_input(&source_location)
                .unwrap()
                .read()
                .await
                .unwrap(),
            Bytes::from_static(b"new source!!")
        );
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_file_io_local_cache_invalidates_directories_after_rename() {
        let source_directory = tempfile::tempdir().unwrap();
        let cache_directory = tempfile::tempdir().unwrap();
        let old_directory = source_directory.path().join("old");
        let new_directory = source_directory.path().join("new");
        std::fs::create_dir(&old_directory).unwrap();
        std::fs::create_dir(&new_directory).unwrap();
        let old_snapshot = old_directory.join("snapshot-1");
        let new_snapshot = new_directory.join("snapshot-1");
        std::fs::write(&old_snapshot, b"old directory").unwrap();
        std::fs::write(&new_snapshot, b"new directory").unwrap();
        let old_directory_location = format!("file:{}", old_directory.display());
        let new_directory_location = format!("file:{}", new_directory.display());
        let old_snapshot_location = format!("file:{}", old_snapshot.display());
        let new_snapshot_location = format!("file:{}", new_snapshot.display());
        let file_io = setup_cached_fs_file_io(cache_directory.path());

        assert_eq!(
            file_io
                .new_input(&old_snapshot_location)
                .unwrap()
                .read()
                .await
                .unwrap(),
            Bytes::from_static(b"old directory")
        );
        assert_eq!(
            file_io
                .new_input(&new_snapshot_location)
                .unwrap()
                .read()
                .await
                .unwrap(),
            Bytes::from_static(b"new directory")
        );
        std::fs::remove_dir_all(&new_directory).unwrap();
        file_io
            .rename(&old_directory_location, &new_directory_location)
            .await
            .unwrap();
        std::fs::create_dir(&old_directory).unwrap();
        std::fs::write(&old_snapshot, b"replacement!!").unwrap();

        assert_eq!(
            file_io
                .new_input(&new_snapshot_location)
                .unwrap()
                .read()
                .await
                .unwrap(),
            Bytes::from_static(b"old directory")
        );
        assert_eq!(
            file_io
                .new_input(&old_snapshot_location)
                .unwrap()
                .read()
                .await
                .unwrap(),
            Bytes::from_static(b"replacement!!")
        );
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_file_io_local_cache_invalidates_after_streaming_write_shutdown() {
        use tokio::io::AsyncWriteExt;

        let source_directory = tempfile::tempdir().unwrap();
        let cache_directory = tempfile::tempdir().unwrap();
        let source_path = source_directory.path().join("snapshot-1");
        std::fs::write(&source_path, b"old metadata").unwrap();
        let location = format!("file:{}", source_path.display());
        let file_io = setup_cached_fs_file_io(cache_directory.path());

        assert_eq!(
            file_io.new_input(&location).unwrap().read().await.unwrap(),
            Bytes::from_static(b"old metadata")
        );
        let mut writer = file_io
            .new_output(&location)
            .unwrap()
            .async_writer()
            .await
            .unwrap();
        writer.write_all(b"new metadata").await.unwrap();
        writer.shutdown().await.unwrap();

        assert_eq!(
            file_io.new_input(&location).unwrap().read().await.unwrap(),
            Bytes::from_static(b"new metadata")
        );
    }

    #[tokio::test]
    async fn retained_read_only_listing_holds_charge_until_drop_and_legacy_releases() {
        let path = "s3://bucket/warehouse";
        let backend = FixedReadOnlyFileIo {
            listings: HashMap::from([(
                path.to_string(),
                vec![status("s3://bucket/warehouse/db.db/", true)],
            )]),
            ..Default::default()
        };
        let (file_io, current) = controlled_file_io(backend, u64::MAX);

        let listing = file_io.list_status_retained(path).await.unwrap();
        assert_eq!(listing.value().len(), 1);
        assert!(current.load(Ordering::Acquire) > 0);
        drop(listing);
        assert_eq!(current.load(Ordering::Acquire), 0);

        assert_eq!(file_io.list_status(path).await.unwrap().len(), 1);
        assert_eq!(current.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn failed_read_only_listing_releases_already_retained_entries() {
        let path = "s3://bucket/warehouse";
        let first = status("s3://bucket/warehouse/first.db/", true);
        let first_bytes = (std::mem::size_of::<FileStatus>() + first.path.len()) as u64;
        let backend = FixedReadOnlyFileIo {
            listings: HashMap::from([(
                path.to_string(),
                vec![first, status("s3://bucket/warehouse/second.db/", true)],
            )]),
            ..Default::default()
        };
        let (file_io, current) = controlled_file_io(backend, first_bytes);

        assert!(file_io.list_status_retained(path).await.is_err());
        assert_eq!(current.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn schema_and_snapshot_discovery_use_retained_listing_paths() {
        let table = "s3://bucket/warehouse/db.db/table";
        let schema_dir = format!("{table}/schema");
        let snapshot_dir = format!("{table}/snapshot");
        let backend = FixedReadOnlyFileIo {
            listings: HashMap::from([
                (
                    schema_dir.clone(),
                    vec![
                        status(&format!("{schema_dir}/schema-3"), false),
                        status(&format!("{schema_dir}/schema-1"), false),
                    ],
                ),
                (
                    snapshot_dir.clone(),
                    vec![
                        status(&format!("{snapshot_dir}/snapshot-8"), false),
                        status(&format!("{snapshot_dir}/snapshot-2"), false),
                    ],
                ),
            ]),
            ..Default::default()
        };
        let (file_io, current) = controlled_file_io(backend, u64::MAX);

        let schemas = crate::table::SchemaManager::new(file_io.clone(), table.to_string())
            .list_all_ids()
            .await
            .unwrap();
        assert_eq!(schemas, vec![1, 3]);
        assert_eq!(current.load(Ordering::Acquire), 0);

        let snapshots = crate::table::SnapshotManager::new(file_io, table.to_string())
            .list_all_ids()
            .await
            .unwrap();
        assert_eq!(snapshots, vec![2, 8]);
        assert_eq!(current.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn schema_and_snapshot_discovery_release_partial_listings_on_budget_error() {
        for directory in ["schema", "snapshot"] {
            let table = format!("s3://bucket/warehouse/db.db/{directory}_table");
            let listing_path = format!("{table}/{directory}");
            let prefix = if directory == "schema" {
                "schema"
            } else {
                "snapshot"
            };
            let first = status(&format!("{listing_path}/{prefix}-1"), false);
            let first_bytes = (std::mem::size_of::<FileStatus>() + first.path.len()) as u64;
            let backend = FixedReadOnlyFileIo {
                listings: HashMap::from([(
                    listing_path.clone(),
                    vec![first, status(&format!("{listing_path}/{prefix}-2"), false)],
                )]),
                ..Default::default()
            };
            let (file_io, current) = controlled_file_io(backend, first_bytes);

            let result = if directory == "schema" {
                crate::table::SchemaManager::new(file_io, table)
                    .list_all_ids()
                    .await
            } else {
                crate::table::SnapshotManager::new(file_io, table)
                    .list_all_ids()
                    .await
            };
            assert!(result.is_err());
            assert_eq!(current.load(Ordering::Acquire), 0);
        }
    }

    #[tokio::test]
    async fn filesystem_catalog_retains_database_and_table_listings_for_adoption() {
        let warehouse = "s3://bucket/warehouse";
        let database_path = format!("{warehouse}/db.db");
        let table_path = format!("{database_path}/table");
        let backend = FixedReadOnlyFileIo {
            listings: HashMap::from([
                (
                    warehouse.to_string(),
                    vec![status(&format!("{database_path}/"), true)],
                ),
                (
                    database_path.clone(),
                    vec![status(&format!("{table_path}/"), true)],
                ),
            ]),
            existing: HashSet::from([
                database_path.clone(),
                format!("{table_path}/schema/schema-0"),
            ]),
        };
        let (file_io, current) = controlled_file_io(backend, u64::MAX);
        let mut options = crate::Options::new();
        options.set(crate::CatalogOptions::WAREHOUSE, warehouse);
        let catalog = crate::FileSystemCatalog::with_file_io(options, file_io).unwrap();

        let databases = catalog.list_databases_retained().await.unwrap();
        assert_eq!(databases.value(), &["db".to_string()]);
        assert!(current.load(Ordering::Acquire) > 0);
        drop(databases);
        assert_eq!(current.load(Ordering::Acquire), 0);

        let tables = catalog.list_tables_retained("db").await.unwrap();
        assert_eq!(tables.value(), &["table".to_string()]);
        assert!(current.load(Ordering::Acquire) > 0);
        drop(tables);
        assert_eq!(current.load(Ordering::Acquire), 0);
    }
}
