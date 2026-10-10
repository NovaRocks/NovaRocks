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

use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use crate::iceberg::io::{
    FileIO, FileIOBuilder, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage,
    StorageConfig, StorageFactory,
};
use crate::iceberg::{Error, ErrorKind, Result};
use crate::opendal::Operator;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use serde::{Deserialize, Serialize};

use novarocks_fs::{FsAccessHandle, FsAccessResolver, FsScheme};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorListingBound, ConnectorOperationControl,
};

use crate::access_binding::IcebergReadBinding;

/// The output capability belongs to one logical commit operation, never to the process.
#[async_trait]
pub(crate) trait IcebergWriteAdmission: Send + Sync + std::fmt::Debug {
    fn check_active(&self) -> Result<()>;
    fn check_output_active(&self, object: &crate::commit::model::ObjectIdentity) -> Result<()>;
    fn cancellation(&self) -> novarocks_fs::FileCancellation;
    fn runtime(&self) -> crate::resources::IcebergCatalogRuntime;
    fn admit(self: Arc<Self>, path: &str) -> Result<WritePermit>;
    fn mark_written(&self, object: &crate::commit::model::ObjectIdentity) -> Result<()>;
    fn record_exit_failure(
        &self,
        object: &crate::commit::model::ObjectIdentity,
        failure: StreamExitFailure,
    );
    async fn io_permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit>;
}

#[derive(Clone, Debug)]
pub(crate) enum StreamExitFailure {
    Unconfirmed,
    AbortFailed(String),
    BridgeFailed(String),
}
impl std::fmt::Display for StreamExitFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unconfirmed => {
                f.write_str("Multipart writer exited without confirmed close or abort")
            }
            Self::AbortFailed(reason) => write!(f, "Multipart abort failed: {reason}"),
            Self::BridgeFailed(reason) => write!(f, "Multipart abort bridge failed: {reason}"),
        }
    }
}

pub(crate) struct WritePermit {
    admission: Arc<dyn IcebergWriteAdmission>,
    object: crate::commit::model::ObjectIdentity,
    io: Option<tokio::sync::OwnedSemaphorePermit>,
    stream_started: std::sync::atomic::AtomicBool,
    exit_confirmed: std::sync::atomic::AtomicBool,
}

impl WritePermit {
    pub(crate) fn new(
        admission: Arc<dyn IcebergWriteAdmission>,
        object: crate::commit::model::ObjectIdentity,
    ) -> Self {
        Self {
            admission,
            object,
            io: None,
            stream_started: std::sync::atomic::AtomicBool::new(false),
            exit_confirmed: std::sync::atomic::AtomicBool::new(false),
        }
    }
    async fn acquire(mut self) -> Result<Self> {
        self.io = Some(self.admission.io_permit().await?);
        self.admission.check_output_active(&self.object)?;
        Ok(self)
    }
    fn check_active(&self) -> Result<()> {
        self.admission.check_active()
    }
    fn complete(&self) -> Result<()> {
        self.confirm_exit();
        self.admission.mark_written(&self.object)
    }
    fn confirm_exit(&self) {
        self.exit_confirmed
            .store(true, std::sync::atomic::Ordering::Release);
    }
}
impl Drop for WritePermit {
    fn drop(&mut self) {
        if self
            .stream_started
            .load(std::sync::atomic::Ordering::Acquire)
            && !self
                .exit_confirmed
                .load(std::sync::atomic::Ordering::Acquire)
        {
            // Publish an unconfirmed exit before the I/O permit field is released, including
            // panic or runtime-bridge failure. Cleanup cannot race this into a false Complete.
            self.admission
                .record_exit_failure(&self.object, StreamExitFailure::Unconfirmed);
        }
    }
}

async fn optional_io_permit(
    admission: Option<&Arc<dyn IcebergWriteAdmission>>,
) -> Result<Option<tokio::sync::OwnedSemaphorePermit>> {
    match admission {
        Some(admission) => admission.io_permit().await.map(Some),
        None => Ok(None),
    }
}

pub(crate) const HADOOP_LISTING_WORKSPACE_BYTES: usize = 32 * 1024 * 1024;

pub(crate) fn listing_refusal() -> Error {
    Error::new(
        ErrorKind::Unexpected,
        "Hadoop catalog listing exceeds its bounded workspace",
    )
    .with_source(ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "Hadoop catalog listing exceeds its bounded workspace",
    ))
}

pub(crate) fn check_listing_path_size(bytes: usize) -> Result<()> {
    if bytes > ConnectorListingBound::V1.name_bytes {
        return Err(listing_refusal());
    }
    Ok(())
}

/// Reserve only after accounting for both the old and requested new backing.
/// Unstable sorting needs no heap scratch and preserves the sorted/deduped
/// values, including equal names.
pub(crate) fn grow_listing_vec<T>(
    values: &mut Vec<T>,
    other_bytes: usize,
    workspace_bytes: usize,
    entries_limit: usize,
) -> Result<()> {
    if values.len() >= entries_limit {
        return Err(Error::new(
            ErrorKind::Unexpected,
            "Hadoop catalog listing exceeds its entries bound",
        )
        .with_source(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "Hadoop catalog listing exceeds its entries bound",
        )));
    }
    let old = values.capacity();
    let requested = if values.len() == old {
        old.checked_mul(2)
            .map(|capacity| capacity.max(4).min(entries_limit))
            .ok_or_else(listing_refusal)?
    } else {
        old
    };
    let peak_slots = if requested > old {
        old.checked_add(requested).ok_or_else(listing_refusal)?
    } else {
        old
    };
    peak_slots
        .checked_mul(std::mem::size_of::<T>())
        .and_then(|slots| slots.checked_add(other_bytes))
        .filter(|peak| *peak <= workspace_bytes)
        .ok_or_else(listing_refusal)?;
    if requested > old {
        // reserve_exact's argument is additional to len, not capacity.
        values
            .try_reserve_exact(requested - values.len())
            .map_err(|_| listing_refusal())?;
    }
    Ok(())
}

struct DirectoryListing {
    bound: ConnectorListingBound,
    workspace_bytes: usize,
    name_bytes: usize,
    directories: Vec<String>,
}

impl DirectoryListing {
    fn new(bound: ConnectorListingBound, workspace_bytes: usize) -> Result<Self> {
        bound.validate().map_err(|error| {
            Error::new(
                ErrorKind::DataInvalid,
                "Invalid Hadoop catalog listing bound",
            )
            .with_source(error)
        })?;
        if workspace_bytes > HADOOP_LISTING_WORKSPACE_BYTES {
            return Err(listing_refusal());
        }
        Ok(Self {
            bound,
            workspace_bytes,
            name_bytes: 0,
            directories: Vec::new(),
        })
    }

    fn push(&mut self, name: &str) -> Result<()> {
        // Deduplicate before admitting a copy, as the previous complete
        // directory listing did before its caller checked the listing bound.
        // The sorted Vec needs no auxiliary hash/tree allocations.
        let index = match self
            .directories
            .binary_search_by(|directory| directory.as_str().cmp(name))
        {
            Ok(_) => return Ok(()),
            Err(index) => index,
        };
        let name_bytes = self
            .name_bytes
            .checked_add(name.len())
            .filter(|bytes| *bytes <= self.bound.total_name_bytes)
            .ok_or_else(listing_refusal)?;
        if name.len() > self.bound.name_bytes {
            return Err(listing_refusal());
        }
        grow_listing_vec(
            &mut self.directories,
            name_bytes,
            self.workspace_bytes,
            self.bound.entries,
        )?;
        self.directories.insert(index, name.to_string());
        self.name_bytes = name_bytes;
        Ok(())
    }

    fn finish(self) -> Vec<String> {
        self.directories
    }
}

#[derive(Clone, Debug)]
pub struct IcebergFsAccess {
    handle: FsAccessHandle,
}

impl IcebergFsAccess {
    fn new(handle: FsAccessHandle) -> Self {
        Self { handle }
    }

    pub fn handle(&self) -> &FsAccessHandle {
        &self.handle
    }

    pub fn operator(&self) -> Operator {
        self.handle.operator()
    }

    pub fn single_relative_path(&self) -> std::result::Result<&str, String> {
        match self.handle.paths() {
            [path] => Ok(path.operator_relative_path()),
            [] => Err("fs access handle has no resolved paths".to_string()),
            paths => Err(format!(
                "fs access handle expected one resolved path, found {}",
                paths.len()
            )),
        }
    }

    pub fn supports_conditional_create(&self) -> bool {
        self.operator()
            .info()
            .full_capability()
            .write_with_if_not_exists
    }

    pub async fn ensure_parent_directory(&self) -> std::result::Result<(), String> {
        let relative_path = self.single_relative_path()?.to_string();
        IcebergFsStorage::ensure_parent_dir(&self.operator(), &relative_path)
            .await
            .map_err(|error| error.to_string())
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct IcebergFileSystemFactory {
    #[serde(skip, default)]
    binding: Option<IcebergReadBinding>,
    #[serde(skip, default)]
    listing: Option<(ConnectorListingBound, usize)>,
    #[serde(skip, default)]
    admission: Option<Arc<dyn IcebergWriteAdmission>>,
}

impl IcebergFileSystemFactory {
    pub fn new(binding: IcebergReadBinding) -> Self {
        Self {
            binding: Some(binding),
            listing: None,
            admission: None,
        }
    }
}

#[typetag::serde]
impl StorageFactory for IcebergFileSystemFactory {
    fn build(&self, _config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        let binding = self.binding.clone().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Iceberg filesystem factory has no admitted storage capability",
            )
        })?;
        Ok(Arc::new(IcebergFsStorage {
            binding: Some(binding),
            listing: self.listing,
            admission: self.admission.clone(),
        }))
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct IcebergFsStorage {
    #[serde(skip, default)]
    binding: Option<IcebergReadBinding>,
    #[serde(skip, default)]
    listing: Option<(ConnectorListingBound, usize)>,
    #[serde(skip, default)]
    admission: Option<Arc<dyn IcebergWriteAdmission>>,
}

impl IcebergFsStorage {
    pub fn new(binding: IcebergReadBinding) -> Self {
        Self {
            binding: Some(binding),
            listing: None,
            admission: None,
        }
    }

    fn read_control(&self) -> Option<Arc<dyn ConnectorOperationControl>> {
        self.binding
            .as_ref()
            .and_then(IcebergReadBinding::operation_control)
    }

    fn check_read_active(&self, operation: &str) -> Result<()> {
        if let Some(admission) = &self.admission {
            admission.check_active()?;
        }
        check_read_active(self.read_control().as_ref(), operation)
    }

    fn resolve_path(&self, _operation: &str, path: &str) -> Result<(IcebergFsAccess, String)> {
        let binding = self.binding.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Iceberg filesystem storage has no admitted storage capability",
            )
        })?;
        let access = IcebergFsAccess::new(binding.resolve_access(path).map_err(|error| {
            Error::new(ErrorKind::DataInvalid, "fs storage path resolution failed")
                .with_source(error)
        })?);
        let relative_path = access.single_relative_path().map_err(|error| {
            Error::new(
                ErrorKind::DataInvalid,
                "fs relative storage path resolution failed",
            )
            .with_source(anyhow::Error::msg(error))
        })?;
        Ok((access.clone(), relative_path.to_string()))
    }

    async fn ensure_parent_dir(op: &Operator, path: &str) -> Result<()> {
        let Some(parent) = Path::new(path).parent() else {
            return Ok(());
        };
        let mut parent = parent.to_string_lossy().replace('\\', "/");
        if parent.is_empty() || parent == "." {
            return Ok(());
        }
        if !parent.ends_with('/') {
            parent.push('/');
        }
        op.create_dir(&parent).await.map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("create fs parent directory {parent}: {e}"),
            )
        })
    }

    fn storage_arc(&self) -> Arc<dyn Storage> {
        Arc::new(self.clone())
    }
}

#[typetag::serde]
#[async_trait]
impl Storage for IcebergFsStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        self.check_read_active("exists")?;
        let _io = optional_io_permit(self.admission.as_ref()).await?;
        let (access, relative_path) = self.resolve_path("exists", path)?;
        let result = access.operator().exists(&relative_path).await;
        self.check_read_active("exists")?;
        result.map_err(|error| {
            Error::new(ErrorKind::Unexpected, "fs existence probe failed").with_source(error)
        })
    }

    async fn list_directories(&self, path: &str) -> Result<Vec<String>> {
        self.check_read_active("list_directories")?;
        let _io = optional_io_permit(self.admission.as_ref()).await?;
        let (bound, workspace_bytes) = self
            .listing
            .unwrap_or((ConnectorListingBound::V1, HADOOP_LISTING_WORKSPACE_BYTES));
        let mut directories = DirectoryListing::new(bound, workspace_bytes)?;
        check_listing_path_size(path.len())?;
        let (access, mut relative_path) = self.resolve_path("list_directories", path)?;
        if !relative_path.ends_with('/') {
            relative_path.push('/');
        }
        // The public page limit bounds the requested page size; it does not
        // prove a hard bound on the remote response body or SDK decoding.
        let result = access
            .operator()
            .lister_with(&relative_path)
            .limit(bound.page_entries)
            .await;
        self.check_read_active("list_directories")?;
        let mut entries = result.map_err(|error| {
            // Preserve typed List overflow through the Iceberg error source
            // without retaining or rendering opaque SDK diagnostics.
            Error::new(
                ErrorKind::Unexpected,
                "fs directory traversal could not start",
            )
            .with_source(novarocks_spi::connector::ConnectorError::from(
                novarocks_fs::map_object_store_listing_error(error),
            ))
        })?;
        let prefix = relative_path.trim_start_matches('/');
        while let Some(entry) = entries.next().await {
            self.check_read_active("list_directories")?;
            let entry = entry.map_err(|error| {
                Error::new(ErrorKind::Unexpected, "fs directory traversal failed").with_source(
                    novarocks_spi::connector::ConnectorError::from(
                        novarocks_fs::map_object_store_listing_error(error),
                    ),
                )
            })?;
            let listed = entry.path().trim_start_matches('/');
            if let Some(relative) = listed.strip_prefix(prefix) {
                let relative = relative.trim_start_matches('/');
                if let Some(directory) = relative.strip_suffix('/') {
                    if !directory.is_empty() && !directory.contains('/') {
                        directories.push(directory)?;
                    }
                }
            }
        }
        self.check_read_active("list_directories")?;
        Ok(directories.finish())
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        self.check_read_active("metadata")?;
        let _io = optional_io_permit(self.admission.as_ref()).await?;
        let (access, relative_path) = self.resolve_path("metadata", path)?;
        let result = access.operator().stat(&relative_path).await;
        self.check_read_active("metadata")?;
        let meta = result.map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("fs metadata({path}) through {relative_path}: {e}"),
            )
        })?;
        Ok(FileMetadata {
            size: meta.content_length(),
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        self.check_read_active("read")?;
        let _io = optional_io_permit(self.admission.as_ref()).await?;
        let (access, relative_path) = self.resolve_path("read", path)?;
        let result = access.operator().read(&relative_path).await;
        self.check_read_active("read")?;
        let data = result.map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("fs read({path}) through {relative_path}: {e}"),
            )
        })?;
        Ok(data.to_bytes())
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        self.check_read_active("reader")?;
        let _io = optional_io_permit(self.admission.as_ref()).await?;
        let (access, relative_path) = self.resolve_path("reader", path)?;
        self.check_read_active("reader")?;
        Ok(Box::new(IcebergFsFileRead {
            access,
            relative_path,
            control: self.read_control(),
            admission: self.admission.clone(),
        }))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let permit = match &self.admission {
            Some(admission) => Some(admission.clone().admit(path)?.acquire().await?),
            None => None,
        };
        let (access, relative_path) = self.resolve_path("write", path)?;
        let op = access.operator();
        if let Some(permit) = &permit {
            permit.check_active()?;
        }
        Self::ensure_parent_dir(&op, &relative_path).await?;
        if let Some(permit) = &permit {
            permit.check_active()?;
        }
        // A dispatched single PUT is awaited to actual exit. Cancellation cannot
        // drop its future and pretend that its external write stopped.
        op.write(&relative_path, bs).await.map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("fs write({path}) through {relative_path}: {e}"),
            )
        })?;
        if let Some(permit) = &permit {
            permit.complete()?;
            permit.check_active()?;
        }
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let permit = match &self.admission {
            Some(admission) => Some(admission.clone().admit(path)?.acquire().await?),
            None => None,
        };
        let (access, relative_path) = self.resolve_path("writer", path)?;
        let op = access.operator();
        if let Some(permit) = &permit {
            permit.check_active()?;
        }
        Self::ensure_parent_dir(&op, &relative_path).await?;
        if let Some(permit) = &permit {
            permit.check_active()?;
        }
        let mut writer = op.writer(&relative_path).await.map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("fs writer({path}) through {relative_path}: {e}"),
            )
        })?;
        if let Some(permit) = &permit {
            permit
                .stream_started
                .store(true, std::sync::atomic::Ordering::Release);
            if let Err(error) = permit.check_active() {
                return Err(abort_after_failure(&mut writer, error, Some(permit)).await);
            }
        }
        Ok(Box::new(IcebergFsFileWrite {
            writer: Some(writer),
            permit,
        }))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let (access, relative_path) = self.resolve_path("delete", path)?;
        access.operator().delete(&relative_path).await.map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("fs delete({path}) through {relative_path}: {e}"),
            )
        })
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let (access, relative_path) = self.resolve_path("delete_prefix", path)?;
        access
            .operator()
            .remove_all(&relative_path)
            .await
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("fs delete_prefix({path}) through {relative_path}: {e}"),
                )
            })
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(self.storage_arc(), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(self.storage_arc(), path.to_string()))
    }
}

struct IcebergFsFileRead {
    access: IcebergFsAccess,
    relative_path: String,
    control: Option<Arc<dyn ConnectorOperationControl>>,
    admission: Option<Arc<dyn IcebergWriteAdmission>>,
}

impl std::fmt::Debug for IcebergFsFileRead {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergFsFileRead")
            .field("access", &self.access)
            .field("relative_path", &self.relative_path)
            .finish_non_exhaustive()
    }
}

fn check_read_active(
    control: Option<&Arc<dyn ConnectorOperationControl>>,
    operation: &str,
) -> Result<()> {
    if let Some(control) = control {
        control.check_active().map_err(|error| {
            Error::new(ErrorKind::Unexpected, format!("fs {operation} stopped")).with_source(error)
        })?;
    }
    Ok(())
}

#[async_trait]
impl FileRead for IcebergFsFileRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        if let Some(admission) = &self.admission {
            admission.check_active()?;
        }
        check_read_active(self.control.as_ref(), "range read")?;
        if range.end < range.start {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("invalid fs read range {}..{}", range.start, range.end),
            ));
        }

        let _io = optional_io_permit(self.admission.as_ref()).await?;
        let operator = self.access.operator();
        let relative_path = self.relative_path.clone();
        let result = operator
            .read_with(&relative_path)
            .range(range.clone())
            .await;
        if let Some(admission) = &self.admission {
            admission.check_active()?;
        }
        check_read_active(self.control.as_ref(), "range read")?;
        result.map(|buffer| buffer.to_bytes()).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "fs range read({relative_path} {}..{}): {e}",
                    range.start, range.end
                ),
            )
        })
    }
}

struct IcebergFsFileWrite {
    writer: Option<crate::opendal::Writer>,
    permit: Option<WritePermit>,
}

impl std::fmt::Debug for IcebergFsFileWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergFsFileWrite")
            .field("open", &self.writer.is_some())
            .finish()
    }
}

async fn abort_after_failure(
    writer: &mut crate::opendal::Writer,
    error: Error,
    permit: Option<&WritePermit>,
) -> Error {
    match writer.abort().await {
        Ok(()) => {
            if let Some(permit) = permit {
                permit.confirm_exit();
            }
            error
        }
        Err(abort) => {
            if let Some(permit) = permit {
                permit.admission.record_exit_failure(
                    &permit.object,
                    StreamExitFailure::AbortFailed(abort.to_string()),
                );
            }
            Error::new(
                ErrorKind::Unexpected,
                format!("fs write failed: {error}; multipart abort failed: {abort}"),
            )
        }
    }
}

impl Drop for IcebergFsFileWrite {
    fn drop(&mut self) {
        // FileWrite has no public abort method. An abandoned governed writer must
        // still release its multipart upload before its operation's permit exits.
        if let (Some(mut writer), Some(permit)) = (self.writer.take(), self.permit.take()) {
            let runtime = permit.admission.runtime();
            let admission = permit.admission.clone();
            let object = permit.object.clone();
            let result = runtime.block_on(async move {
                let result = writer.abort().await;
                match &result {
                    Ok(()) => permit.confirm_exit(),
                    Err(error) => permit.admission.record_exit_failure(
                        &permit.object,
                        StreamExitFailure::AbortFailed(error.to_string()),
                    ),
                }
                drop(permit);
                result
            });
            let failure = match result {
                Ok(Ok(())) => None,
                Ok(Err(_)) => None, // recorded before releasing the permit
                Err(error) => Some(StreamExitFailure::BridgeFailed(error)),
            };
            if let Some(failure) = failure {
                admission.record_exit_failure(&object, failure);
            }
        }
    }
}

/// RetryWrapper moves the writer into the pending future. Never drop that future
/// on cancellation: wait for its actual exit so the owner can abort the next segment.
async fn await_stream_step<T>(
    permit: Option<&WritePermit>,
    future: impl std::future::Future<Output = Result<T>>,
) -> Result<(T, Option<Error>)> {
    let Some(permit) = permit else {
        return future.await.map(|value| (value, None));
    };
    permit.check_active()?;
    let cancellation = permit.admission.cancellation();
    tokio::pin!(future);
    tokio::select! {
        biased;
        error = cancellation.ended() => {
            let value = future.await?;
            Ok((value, Some(crate::commit::operation::stopped_error(error))))
        },
        result = &mut future => result.map(|value| (value, permit.check_active().err())),
    }
}

#[async_trait]
impl FileWrite for IcebergFsFileWrite {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        if self.permit.is_none() {
            return self
                .writer
                .as_mut()
                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "write to closed fs file"))?
                .write(bs)
                .await
                .map_err(|e| Error::new(ErrorKind::Unexpected, format!("fs write: {e}")));
        }
        let mut writer = self
            .writer
            .take()
            .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "write to closed fs file"))?;
        let result = await_stream_step(self.permit.as_ref(), async {
            writer.write(bs).await.map_err(|e| {
                Error::new(ErrorKind::Unexpected, "fs stream write failed").with_source(e)
            })
        })
        .await;
        match result {
            Ok(((), None)) => {
                self.writer = Some(writer);
                Ok(())
            }
            Ok(((), Some(error))) | Err(error) => {
                let error = abort_after_failure(&mut writer, error, self.permit.as_ref()).await;
                self.permit.take();
                Err(error)
            }
        }
    }

    async fn close(&mut self) -> Result<()> {
        if self.permit.is_none() {
            let mut writer = self
                .writer
                .take()
                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "fs file already closed"))?;
            writer
                .close()
                .await
                .map_err(|e| Error::new(ErrorKind::Unexpected, format!("fs close: {e}")))?;
            return Ok(());
        }
        let mut writer = self
            .writer
            .take()
            .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "fs file already closed"))?;
        let result = await_stream_step(self.permit.as_ref(), async {
            writer.close().await.map_err(|e| {
                Error::new(ErrorKind::Unexpected, "fs stream close failed").with_source(e)
            })
        })
        .await;
        let result = match result {
            Ok((_metadata, stopped)) => {
                // Close has already completed externally. There is no multipart upload
                // left to abort; retain its object in the ledger for known-uncommitted cleanup.
                self.permit.as_ref().map_or(Ok(()), WritePermit::complete)?;
                stopped.map_or(Ok(()), Err)
            }
            Err(error) => Err(abort_after_failure(&mut writer, error, self.permit.as_ref()).await),
        };
        self.permit.take();
        result
    }
}

pub(crate) fn build_admitted_file_io_for_location(
    location: &str,
    binding: IcebergReadBinding,
    admission: Arc<dyn IcebergWriteAdmission>,
) -> FileIO {
    let _ = location;
    FileIOBuilder::new(Arc::new(IcebergFileSystemFactory {
        binding: Some(binding),
        listing: None,
        admission: Some(admission),
    }))
    .build()
}

pub fn build_file_io_for_location(location: &str, binding: IcebergReadBinding) -> FileIO {
    // FileIO construction is lazy: the SDK asks the storage to resolve paths
    // only when actual IO starts, so this helper stores credentials here and
    // leaves location validation to the per-operation FsAccessResolver call.
    let _ = location;
    FileIOBuilder::new(Arc::new(IcebergFileSystemFactory::new(binding))).build()
}

/// A request-local factory with the caller's exact listing and remaining
/// workspace bounds. Only this provider can construct its owned directory Vec.
pub(crate) fn build_bounded_file_io_for_location(
    location: &str,
    binding: IcebergReadBinding,
    bound: ConnectorListingBound,
    workspace_bytes: usize,
) -> Result<FileIO> {
    let _ = DirectoryListing::new(bound, workspace_bytes)?;
    check_listing_path_size(location.len())?;
    let factory = IcebergFileSystemFactory {
        binding: Some(binding),
        listing: Some((bound, workspace_bytes)),
        admission: None,
    };
    Ok(FileIOBuilder::new(Arc::new(factory)).build())
}

pub fn build_storage_factory_for_location(
    location: &str,
    binding: IcebergReadBinding,
) -> Arc<dyn StorageFactory> {
    // StorageFactory construction is lazy for the same reason FileIO is: keep
    // credentials here and resolve concrete operators per IO call.
    let _ = location;
    Arc::new(IcebergFileSystemFactory::new(binding))
}

pub fn resolve_access_for_location(
    location: &str,
    binding: &IcebergReadBinding,
) -> std::result::Result<IcebergFsAccess, String> {
    resolve_access_for_locations(std::iter::once(location), binding)
}

pub fn resolve_access_for_locations<I, S>(
    locations: I,
    binding: &IcebergReadBinding,
) -> std::result::Result<IcebergFsAccess, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let handle = binding
        .resolve_access_for_locations(locations)
        .map_err(|error| error.to_string())?;
    Ok(IcebergFsAccess::new(handle))
}

pub fn format_resolved_location(
    handle: &FsAccessHandle,
    operator_relative_path: &str,
) -> std::result::Result<String, String> {
    let path = operator_relative_path.trim_start_matches('/');
    let location = handle
        .paths()
        .first()
        .map(|path| path.location())
        .ok_or_else(|| "fs access handle has no resolved paths".to_string())?;

    match location.scheme() {
        FsScheme::Local => {
            let root = handle
                .root()
                .ok_or_else(|| "local fs access handle missing root".to_string())?;
            let full_path = Path::new(root).join(path);
            Ok(format!("file://{}", full_path.display()))
        }
        FsScheme::ObjectStore => {
            let scheme = location.uri_scheme().unwrap_or("s3");
            let bucket = handle
                .authority()
                .or_else(|| location.authority())
                .ok_or_else(|| "object-store fs access handle missing bucket".to_string())?;
            Ok(format!("{scheme}://{bucket}/{path}"))
        }
        FsScheme::Hdfs => {
            let scheme = location.uri_scheme().unwrap_or("hdfs");
            let authority = handle
                .authority()
                .or_else(|| location.authority())
                .ok_or_else(|| "hdfs fs access handle missing authority".to_string())?;
            Ok(format!("{scheme}://{authority}/{path}"))
        }
    }
}

pub fn reader_factory_for_table_location(
    location: &str,
    binding: &IcebergReadBinding,
) -> std::result::Result<FsAccessHandle, String> {
    binding
        .resolve_access(location)
        .map_err(|error| error.to_string())
}

pub fn normalize_hdfs_path_parse_only(path: &str) -> std::result::Result<String, String> {
    let location = FsAccessResolver::new()
        .parse_location(path)
        .map_err(|e| format!("parse hdfs location {path}: {e}"))?;
    if location.scheme() != FsScheme::Hdfs {
        return Err(format!("expected hdfs location: {path}"));
    }
    Ok(location.path().trim_start_matches('/').to_string())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use crate::iceberg::io::Storage;
    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};
    use novarocks_spi::connector::{
        ConnectorError, ConnectorErrorKind, ConnectorRequestContext,
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };

    use super::{
        DirectoryListing, IcebergFsStorage, build_bounded_file_io_for_location,
        build_file_io_for_location, format_resolved_location, resolve_access_for_location,
        resolve_access_for_locations,
    };

    #[derive(Debug)]
    struct ControlledAdmission {
        cancellation: novarocks_fs::FileCancellation,
        semaphore: Arc<tokio::sync::Semaphore>,
        runtime: crate::resources::IcebergCatalogRuntime,
        completed: std::sync::atomic::AtomicUsize,
        exit_failures: std::sync::Mutex<Vec<super::StreamExitFailure>>,
    }
    #[async_trait::async_trait]
    impl super::IcebergWriteAdmission for ControlledAdmission {
        fn check_active(&self) -> crate::iceberg::Result<()> {
            self.cancellation
                .check()
                .map_err(crate::commit::operation::stopped_error)
        }
        fn check_output_active(
            &self,
            _: &crate::commit::model::ObjectIdentity,
        ) -> crate::iceberg::Result<()> {
            self.check_active()
        }
        fn cancellation(&self) -> novarocks_fs::FileCancellation {
            self.cancellation.clone()
        }
        fn runtime(&self) -> crate::resources::IcebergCatalogRuntime {
            self.runtime.clone()
        }
        fn admit(self: Arc<Self>, path: &str) -> crate::iceberg::Result<super::WritePermit> {
            Ok(super::WritePermit::new(
                self,
                crate::commit::model::ObjectIdentity::new(path)?,
            ))
        }
        fn mark_written(
            &self,
            _: &crate::commit::model::ObjectIdentity,
        ) -> crate::iceberg::Result<()> {
            self.completed
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        fn record_exit_failure(
            &self,
            _: &crate::commit::model::ObjectIdentity,
            failure: super::StreamExitFailure,
        ) {
            let mut failures = self.exit_failures.lock().unwrap();
            if failures.is_empty() || !matches!(failure, super::StreamExitFailure::Unconfirmed) {
                failures.push(failure);
            }
        }
        async fn io_permit(&self) -> crate::iceberg::Result<tokio::sync::OwnedSemaphorePermit> {
            Ok(self.semaphore.clone().acquire_owned().await.unwrap())
        }
    }

    #[derive(Debug, Default)]
    struct WriterEvents {
        started: tokio::sync::Notify,
        resume: tokio::sync::Notify,
        log: std::sync::Mutex<Vec<&'static str>>,
        fail_abort: std::sync::atomic::AtomicBool,
    }
    #[derive(Debug, Clone)]
    struct ControlledWriterService {
        events: Arc<WriterEvents>,
        delay_close: bool,
        fail_write: bool,
    }
    impl crate::opendal::raw::Access for ControlledWriterService {
        type Reader = crate::opendal::raw::oio::Reader;
        type Writer = crate::opendal::raw::oio::Writer;
        type Lister = crate::opendal::raw::oio::Lister;
        type Deleter = crate::opendal::raw::oio::Deleter;
        fn info(&self) -> Arc<crate::opendal::raw::AccessorInfo> {
            let info = crate::opendal::raw::AccessorInfo::default();
            info.set_scheme("mock")
                .set_native_capability(crate::opendal::Capability {
                    write: true,
                    write_can_multi: true,
                    ..Default::default()
                });
            Arc::new(info)
        }
        async fn write(
            &self,
            _: &str,
            _: crate::opendal::raw::OpWrite,
        ) -> crate::opendal::Result<(crate::opendal::raw::RpWrite, Self::Writer)> {
            Ok((
                crate::opendal::raw::RpWrite::new(),
                Box::new(ControlledWriter(self.clone())),
            ))
        }
    }
    #[derive(Debug)]
    struct ControlledWriter(ControlledWriterService);
    impl crate::opendal::raw::oio::Write for ControlledWriter {
        async fn write(&mut self, _: crate::opendal::Buffer) -> crate::opendal::Result<()> {
            self.0.events.log.lock().unwrap().push("write-start");
            if self.0.fail_write {
                return Err(crate::opendal::Error::new(
                    crate::opendal::ErrorKind::Unexpected,
                    "injected write failure",
                ));
            }
            if !self.0.delay_close {
                self.0.events.started.notify_one();
                self.0.events.resume.notified().await;
            }
            self.0.events.log.lock().unwrap().push("write-exit");
            Ok(())
        }
        async fn close(&mut self) -> crate::opendal::Result<crate::opendal::Metadata> {
            self.0.events.log.lock().unwrap().push("close-start");
            if self.0.delay_close {
                self.0.events.started.notify_one();
                self.0.events.resume.notified().await;
            }
            self.0.events.log.lock().unwrap().push("close-exit");
            Ok(crate::opendal::Metadata::new(
                crate::opendal::EntryMode::FILE,
            ))
        }
        async fn abort(&mut self) -> crate::opendal::Result<()> {
            self.0.events.log.lock().unwrap().push("abort-exit");
            if self
                .0
                .events
                .fail_abort
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(crate::opendal::Error::new(
                    crate::opendal::ErrorKind::Unexpected,
                    "injected abort failure",
                ));
            }
            Ok(())
        }
    }

    async fn governed_mock_writer(
        delay_close: bool,
        fail_write: bool,
    ) -> (
        super::IcebergFsFileWrite,
        Arc<ControlledAdmission>,
        Arc<WriterEvents>,
    ) {
        use super::IcebergWriteAdmission;
        let events = Arc::new(WriterEvents::default());
        let service = ControlledWriterService {
            events: events.clone(),
            delay_close,
            fail_write,
        };
        let accessor: crate::opendal::raw::Accessor = Arc::new(service);
        // Exercise the exact SDK wrapper which moves inner ownership into the pending future.
        let operator = crate::opendal::Operator::from_inner(accessor)
            .layer(crate::opendal::layers::RetryLayer::new().with_max_times(0));
        let admission = Arc::new(ControlledAdmission {
            cancellation: novarocks_fs::FileCancellation::new(),
            semaphore: Arc::new(tokio::sync::Semaphore::new(1)),
            runtime: crate::resources::IcebergCatalogRuntime::new(tokio::runtime::Handle::current()),
            completed: std::sync::atomic::AtomicUsize::new(0),
            exit_failures: std::sync::Mutex::new(Vec::new()),
        });
        let permit = admission
            .clone()
            .admit("mock.puffin")
            .unwrap()
            .acquire()
            .await
            .unwrap();
        let writer = operator.writer("mock.puffin").await.unwrap();
        permit
            .stream_started
            .store(true, std::sync::atomic::Ordering::Release);
        (
            super::IcebergFsFileWrite {
                writer: Some(writer),
                permit: Some(permit),
            },
            admission,
            events,
        )
    }

    #[tokio::test]
    async fn cancelled_stream_waits_for_retry_writer_exit_then_aborts_before_permit_release() {
        use crate::iceberg::io::FileWrite;
        let (mut writer, admission, events) = governed_mock_writer(false, false).await;
        let task = tokio::spawn(async move { writer.write(Bytes::from_static(b"part")).await });
        events.started.notified().await;
        admission.cancellation.cancel();
        tokio::task::yield_now().await;
        assert!(
            !task.is_finished(),
            "issued write must actually exit before abort"
        );
        assert_eq!(admission.semaphore.available_permits(), 0);
        assert_eq!(*events.log.lock().unwrap(), ["write-start"]);
        events.resume.notify_one();
        assert!(task.await.unwrap().is_err());
        assert_eq!(
            *events.log.lock().unwrap(),
            ["write-start", "write-exit", "abort-exit"]
        );
        assert_eq!(admission.semaphore.available_permits(), 1);
    }

    #[tokio::test]
    async fn failed_stream_aborts_with_restored_retry_owner() {
        use crate::iceberg::io::FileWrite;
        let (mut writer, admission, events) = governed_mock_writer(false, true).await;
        assert!(writer.write(Bytes::from_static(b"part")).await.is_err());
        assert_eq!(*events.log.lock().unwrap(), ["write-start", "abort-exit"]);
        assert_eq!(admission.semaphore.available_permits(), 1);
        assert!(writer.write(Bytes::from_static(b"part")).await.is_err());
        assert_eq!(
            events.log.lock().unwrap().len(),
            2,
            "no segment follows abort"
        );
    }

    #[tokio::test]
    async fn cancelled_close_waits_for_actual_close_and_retains_completed_object() {
        use crate::iceberg::io::FileWrite;
        let (mut writer, admission, events) = governed_mock_writer(true, false).await;
        let task = tokio::spawn(async move { writer.close().await });
        events.started.notified().await;
        admission.cancellation.cancel();
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        assert_eq!(admission.semaphore.available_permits(), 0);
        events.resume.notify_one();
        assert!(task.await.unwrap().is_err());
        assert_eq!(*events.log.lock().unwrap(), ["close-start", "close-exit"]);
        assert_eq!(
            admission
                .completed
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(admission.semaphore.available_permits(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abandoned_governed_writer_waits_for_abort_on_injected_runtime() {
        let (writer, admission, events) = governed_mock_writer(false, false).await;
        drop(writer);
        assert_eq!(*events.log.lock().unwrap(), ["abort-exit"]);
        assert_eq!(admission.semaphore.available_permits(), 1);
    }

    #[tokio::test]
    async fn failed_stream_retains_abort_failure_diagnostic() {
        use crate::iceberg::io::FileWrite;
        let (mut writer, admission, events) = governed_mock_writer(false, true).await;
        events
            .fail_abort
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(writer.write(Bytes::from_static(b"part")).await.is_err());
        assert_eq!(admission.semaphore.available_permits(), 1);
        assert_eq!(admission.exit_failures.lock().unwrap().len(), 1);
        assert!(
            admission.exit_failures.lock().unwrap()[0]
                .to_string()
                .contains("injected abort failure")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abandoned_writer_retains_abort_failure_instead_of_claiming_cleanup() {
        let (writer, admission, events) = governed_mock_writer(false, false).await;
        events
            .fail_abort
            .store(true, std::sync::atomic::Ordering::SeqCst);
        drop(writer);
        assert_eq!(*events.log.lock().unwrap(), ["abort-exit"]);
        assert_eq!(admission.semaphore.available_permits(), 1);
        assert_eq!(admission.exit_failures.lock().unwrap().len(), 1);
        assert!(
            admission.exit_failures.lock().unwrap()[0]
                .to_string()
                .contains("Multipart abort failed")
        );
    }

    #[test]
    fn directory_listing_checks_names_and_old_new_backing_before_growth() {
        let bound = novarocks_spi::connector::ConnectorListingBound {
            entries: 8,
            name_bytes: 4,
            total_name_bytes: 8,
            ..novarocks_spi::connector::ConnectorListingBound::V1
        };
        let mut listing =
            DirectoryListing::new(bound, 4 * std::mem::size_of::<String>() + 4).expect("listing");
        for name in ["d", "b", "c", "a"] {
            listing.push(name).expect("exact first backing boundary");
        }
        let capacity = listing.directories.capacity();
        let pointer = listing.directories.as_ptr();
        assert_eq!(
            stopped_kind(&listing.push("e").expect_err("old/new backing peak")),
            ConnectorErrorKind::ResourceExhausted
        );
        assert_eq!(listing.directories.capacity(), capacity);
        assert_eq!(listing.directories.as_ptr(), pointer);
        assert_eq!(listing.name_bytes, 4);
        assert!(listing.push("large").is_err());
        // An already-retained name does not spend another entry or payload.
        listing.push("a").expect("duplicate at exact boundary");
        assert_eq!(listing.finish(), ["a", "b", "c", "d"]);
    }

    #[tokio::test]
    async fn bounded_directory_stream_preserves_filtering_and_refuses_whole_listing() {
        let directory = tempfile::tempdir().expect("directory");
        for name in ["z", "a", ".hidden"] {
            std::fs::create_dir(directory.path().join(name)).expect("child directory");
        }
        std::fs::write(directory.path().join("ordinary-file"), b"file").expect("file");
        std::fs::create_dir(directory.path().join("a/nested")).expect("nested directory");
        let location = format!("file://{}/", directory.path().display());
        let bound = novarocks_spi::connector::ConnectorListingBound {
            entries: 3,
            page_entries: 1,
            ..novarocks_spi::connector::ConnectorListingBound::V1
        };
        let binding = local_test_binding(None, tokio::runtime::Handle::current());
        let complete = build_bounded_file_io_for_location(&location, binding.clone(), bound, 4096)
            .expect("bounded FileIO");
        assert_eq!(
            complete
                .list_directories(&location)
                .await
                .expect("complete enumeration"),
            [".hidden", "a", "z"]
        );
        let refused = build_bounded_file_io_for_location(
            &location,
            binding,
            novarocks_spi::connector::ConnectorListingBound {
                entries: 2,
                ..bound
            },
            4096,
        )
        .expect("bounded FileIO");
        assert_eq!(
            stopped_kind(
                &refused
                    .list_directories(&location)
                    .await
                    .expect_err("whole listing refusal")
            ),
            ConnectorErrorKind::ResourceExhausted
        );
    }

    fn local_test_binding(
        object_store_config: Option<novarocks_fs::ObjectStoreConfig>,
        runtime: tokio::runtime::Handle,
    ) -> crate::access_binding::IcebergReadBinding {
        crate::access_binding::IcebergReadBinding::new(
            object_store_config,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime)),
        )
    }

    fn request(
        deadline: Instant,
        cancellation: Arc<novarocks_spi::connector::ConnectorStopOwner>,
    ) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            deadline,
            cancellation.view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("request")
    }

    fn stopped_kind(error: &crate::iceberg::Error) -> ConnectorErrorKind {
        std::error::Error::source(error)
            .and_then(|source| source.downcast_ref::<ConnectorError>())
            .expect("typed operation control error")
            .kind()
    }

    #[tokio::test]
    async fn sdk_file_io_uses_only_its_request_control_for_reads() {
        let directory = tempfile::tempdir().expect("directory");
        let file = directory.path().join("metadata.json");
        std::fs::write(&file, b"manifest").expect("file");
        let location = format!("file://{}", file.display());
        let folder = format!("file://{}/", directory.path().display());
        let binding = local_test_binding(None, tokio::runtime::Handle::current());
        let cancelled = Arc::new(novarocks_spi::connector::ConnectorStopOwner::new());
        let first = build_file_io_for_location(
            &location,
            binding.for_request(request(
                Instant::now() + Duration::from_secs(30),
                cancelled.clone(),
            )),
        );
        let input = first.new_input(&location).expect("first input");
        assert!(input.exists().await.expect("exists"));
        assert_eq!(input.metadata().await.expect("metadata").size, 8);
        assert_eq!(
            input.read().await.expect("full read"),
            Bytes::from_static(b"manifest")
        );
        let reader = input.reader().await.expect("range reader");
        assert_eq!(
            reader.read(0..4).await.expect("range read"),
            Bytes::from_static(b"mani")
        );
        let first_storage = IcebergFsStorage::new(binding.for_request(request(
            Instant::now() + Duration::from_secs(30),
            cancelled.clone(),
        )));
        first_storage.list_directories(&folder).await.expect("list");

        cancelled.request_stop();
        assert_eq!(
            stopped_kind(&input.exists().await.expect_err("cancelled exists")),
            ConnectorErrorKind::Cancelled
        );
        assert_eq!(
            stopped_kind(&input.metadata().await.err().expect("cancelled metadata")),
            ConnectorErrorKind::Cancelled
        );
        assert_eq!(
            stopped_kind(&input.read().await.expect_err("cancelled full read")),
            ConnectorErrorKind::Cancelled
        );
        assert_eq!(
            stopped_kind(&reader.read(0..4).await.expect_err("cancelled range")),
            ConnectorErrorKind::Cancelled
        );
        assert_eq!(
            stopped_kind(
                &first_storage
                    .list_directories(&folder)
                    .await
                    .expect_err("cancelled list")
            ),
            ConnectorErrorKind::Cancelled
        );

        let second = build_file_io_for_location(
            &location,
            binding.for_request(request(
                Instant::now() + Duration::from_secs(30),
                Arc::new(novarocks_spi::connector::ConnectorStopOwner::new()),
            )),
        );
        assert_eq!(
            second
                .new_input(&location)
                .expect("second input")
                .read()
                .await
                .expect("second read"),
            Bytes::from_static(b"manifest")
        );
    }

    #[tokio::test]
    async fn sdk_file_io_deadline_is_distinct_from_request_cancellation() {
        let directory = tempfile::tempdir().expect("directory");
        let file = directory.path().join("metadata.json");
        std::fs::write(&file, b"manifest").expect("file");
        let location = format!("file://{}", file.display());
        let file_io = build_file_io_for_location(
            &location,
            local_test_binding(None, tokio::runtime::Handle::current()).for_request(request(
                Instant::now() - Duration::from_millis(1),
                Arc::new(novarocks_spi::connector::ConnectorStopOwner::new()),
            )),
        );
        let input = file_io.new_input(&location).expect("input");
        assert_eq!(
            stopped_kind(&input.read().await.expect_err("expired read")),
            ConnectorErrorKind::DeadlineExceeded
        );
    }

    fn test_object_store_config() -> novarocks_fs::ObjectStoreConfig {
        novarocks_fs::ObjectStoreConfig {
            endpoint: "http://localhost:9000".to_string(),
            access_key_id: novarocks_fs::SecretValue::new("ak"),
            access_key_secret: novarocks_fs::SecretValue::new("sk"),
            session_token: None,
            enable_path_style_access: Some(true),
            region: Some("us-east-1".to_string()),
            retry_max_times: None,
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: None,
            io_timeout_ms: None,
        }
    }

    #[tokio::test]
    async fn file_io_round_trips_local_file_location() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file_path = dir.path().join("metadata.json");
        let location = format!("file://{}", file_path.display());

        let file_io = build_file_io_for_location(
            &location,
            local_test_binding(None, tokio::runtime::Handle::current()),
        );
        let output = file_io.new_output(&location).expect("output file");
        output
            .write(Bytes::from_static(b"iceberg metadata"))
            .await
            .expect("write");

        let input = file_io.new_input(&location).expect("input file");
        assert!(input.exists().await.expect("exists"));
        assert_eq!(input.read().await.expect("read"), "iceberg metadata");
    }

    #[tokio::test]
    async fn file_io_writes_nested_local_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file_path = dir.path().join("metadata").join("00000.json");
        let location = format!("file://{}", file_path.display());

        let file_io = build_file_io_for_location(
            &location,
            local_test_binding(None, tokio::runtime::Handle::current()),
        );
        file_io
            .new_output(&location)
            .expect("output file")
            .write(Bytes::from_static(b"nested metadata"))
            .await
            .expect("nested write");

        assert_eq!(
            file_io
                .new_input(&location)
                .expect("input file")
                .read()
                .await
                .expect("nested read"),
            "nested metadata"
        );
    }

    #[tokio::test]
    async fn output_file_writer_writes_and_closes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file_path = dir.path().join("writer").join("00000.json");
        let location = format!("file://{}", file_path.display());

        let file_io = build_file_io_for_location(
            &location,
            local_test_binding(None, tokio::runtime::Handle::current()),
        );
        let mut writer = file_io
            .new_output(&location)
            .expect("output file")
            .writer()
            .await
            .expect("writer");
        writer
            .write(Bytes::from_static(b"writer payload"))
            .await
            .expect("writer write");
        writer.close().await.expect("writer close");

        assert_eq!(
            file_io
                .new_input(&location)
                .expect("input file")
                .read()
                .await
                .expect("read"),
            "writer payload"
        );
    }

    #[tokio::test]
    async fn iceberg_input_range_read_succeeds() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file_path = dir.path().join("data.bin");
        std::fs::write(&file_path, b"0123456789").expect("write data");
        let location = format!("file://{}", file_path.display());

        let file_io = build_file_io_for_location(
            &location,
            local_test_binding(None, tokio::runtime::Handle::current()),
        );
        let reader = file_io
            .new_input(&location)
            .expect("input file")
            .reader()
            .await
            .expect("reader");

        assert_eq!(reader.read(3..7).await.expect("range read"), "3456");
    }

    #[tokio::test]
    async fn iceberg_input_file_rejects_invalid_range() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file_path = dir.path().join("data.bin");
        std::fs::write(&file_path, b"0123456789").expect("write data");
        let location = format!("file://{}", file_path.display());

        let file_io = build_file_io_for_location(
            &location,
            local_test_binding(None, tokio::runtime::Handle::current()),
        );
        let reader = file_io
            .new_input(&location)
            .expect("input file")
            .reader()
            .await
            .expect("reader");

        let invalid_start = 7;
        let invalid_end = 3;
        let err = reader
            .read(invalid_start..invalid_end)
            .await
            .expect_err("invalid range should fail");

        assert!(
            err.to_string().contains("invalid fs read range 7..3"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn s3_file_io_without_credentials_fails_on_first_io() {
        let location = "s3://bucket/table/metadata.json";
        let file_io = build_file_io_for_location(
            location,
            local_test_binding(None, tokio::runtime::Handle::current()),
        );
        let input = file_io.new_input(location).expect("input file");

        let err = input
            .exists()
            .await
            .expect_err("first object-store IO should fail");

        assert!(
            err.to_string()
                .contains("fs storage path resolution failed"),
            "{err}"
        );
        assert!(
            err.to_string()
                .contains("object-store location has no admitted exact credential binding"),
            "{err}"
        );
    }

    #[test]
    fn format_resolved_location_preserves_object_store_uri_scheme() {
        let cfg = test_object_store_config();

        for (location, expected) in [
            (
                "s3a://bucket/warehouse/table",
                "s3a://bucket/warehouse/table/data/a.parquet",
            ),
            (
                "oss://bucket/warehouse/table",
                "oss://bucket/warehouse/table/data/a.parquet",
            ),
        ] {
            let runtime = tokio::runtime::Runtime::new().expect("runtime");
            let binding = local_test_binding(Some(cfg.clone()), runtime.handle().clone());
            let access =
                resolve_access_for_location(location, &binding).expect("resolve object store");
            let formatted =
                format_resolved_location(access.handle(), "warehouse/table/data/a.parquet")
                    .expect("format location");

            assert_eq!(formatted, expected);
        }
    }

    #[test]
    fn resolves_multiple_local_locations_to_one_access_handle() {
        let dir = tempfile::tempdir().expect("tempdir");
        let first = dir.path().join("a.bin");
        let second = dir.path().join("b.bin");
        std::fs::write(&first, b"a").expect("first");
        std::fs::write(&second, b"b").expect("second");
        let locations = [
            format!("file://{}", first.display()),
            format!("file://{}", second.display()),
        ];

        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let binding = local_test_binding(None, runtime.handle().clone());
        let access = resolve_access_for_locations(locations.iter().map(String::as_str), &binding)
            .expect("access");

        assert_eq!(
            access.handle().operator_relative_paths(),
            vec!["a.bin", "b.bin"]
        );
    }

    #[test]
    fn object_store_without_credentials_returns_resolver_error() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let binding = local_test_binding(None, runtime.handle().clone());
        let err = resolve_access_for_location("s3://bucket/table/metadata.json", &binding)
            .expect_err("missing object-store config should fail");

        assert!(
            err.ends_with("object-store location has no admitted exact credential binding"),
            "unexpected resolver error: {err}"
        );
    }
}
