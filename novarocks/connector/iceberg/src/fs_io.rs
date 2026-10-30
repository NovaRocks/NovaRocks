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

#[cfg(test)]
tokio::task_local! {
    pub(crate) static HADOOP_TEST_LIST_CALLS: std::cell::Cell<usize>;
}

#[cfg(test)]
fn note_hadoop_test_list() {
    let _ = HADOOP_TEST_LIST_CALLS.try_with(|calls| calls.set(calls.get() + 1));
}

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
        self.operator().info().capability().write_with_if_not_exists
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
    admission: Option<Arc<dyn IcebergWriteAdmission>>,
}

impl IcebergFileSystemFactory {
    pub fn new(binding: IcebergReadBinding) -> Self {
        Self {
            binding: Some(binding),
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
            listing: None,
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

    async fn resolve_path(
        &self,
        _operation: &str,
        path: &str,
    ) -> Result<(IcebergFsAccess, String)> {
        let binding = self.binding.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Iceberg filesystem storage has no admitted storage capability",
            )
        })?;
        let access = IcebergFsAccess::new(
            binding
                .resolve_access_for_request_async(path)
                .await
                .map_err(|error| {
                    Error::new(ErrorKind::DataInvalid, "fs storage path resolution failed")
                        .with_source(error)
                })?,
        );
        let relative_path = access.single_relative_path().map_err(|error| {
            Error::new(
                ErrorKind::DataInvalid,
                "fs relative storage path resolution failed",
            )
            .with_source(anyhow::Error::msg(error))
        })?;
        Ok((access.clone(), relative_path.to_string()))
    }

    async fn non_http_phase(
        &self,
        path: &str,
    ) -> Result<Option<novarocks_spi::connector::ConnectorIoPermit>> {
        match self.binding.as_ref() {
            Some(binding) => binding.admit_non_http_io(path).await.map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    "original filesystem I/O admission refused",
                )
                .with_source(e)
            }),
            None => Ok(None),
        }
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

    pub(crate) async fn list_directories(&self, path: &str) -> Result<Vec<String>> {
        self.check_read_active("list_directories")?;
        let _io = optional_io_permit(self.admission.as_ref()).await?;
        let (bound, workspace_bytes) = self
            .listing
            .unwrap_or((ConnectorListingBound::V1, HADOOP_LISTING_WORKSPACE_BYTES));
        let mut directories = DirectoryListing::new(bound, workspace_bytes)?;
        check_listing_path_size(path.len())?;
        let _phase = self.non_http_phase(path).await?;
        let (access, mut relative_path) = self.resolve_path("list_directories", path).await?;
        if !relative_path.ends_with('/') {
            relative_path.push('/');
        }
        // The public page limit bounds the requested page size; it does not
        // prove a hard bound on the remote response body or SDK decoding.
        #[cfg(test)]
        note_hadoop_test_list();
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
                if let Some(directory) = relative.strip_suffix('/')
                    && !directory.is_empty()
                    && !directory.contains('/')
                {
                    directories.push(directory)?;
                }
            }
        }
        self.check_read_active("list_directories")?;
        Ok(directories.finish())
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
        let _phase = self.non_http_phase(path).await?;
        let (access, relative_path) = self.resolve_path("exists", path).await?;
        let result = access.operator().exists(&relative_path).await;
        self.check_read_active("exists")?;
        result.map_err(|error| {
            Error::new(ErrorKind::Unexpected, "fs existence probe failed").with_source(error)
        })
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        self.check_read_active("metadata")?;
        let _io = optional_io_permit(self.admission.as_ref()).await?;
        let _phase = self.non_http_phase(path).await?;
        let (access, relative_path) = self.resolve_path("metadata", path).await?;
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
        let _phase = self.non_http_phase(path).await?;
        let (access, relative_path) = self.resolve_path("read", path).await?;
        let result = access.operator().read(&relative_path).await;
        self.check_read_active("read")?;
        let data = result.map_err(|error| {
            Error::new(ErrorKind::DataInvalid, "fs metadata read failed").with_source(
                ConnectorError::from(novarocks_fs::map_object_store_listing_error(error)),
            )
        })?;
        Ok(data.to_bytes())
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        self.check_read_active("reader")?;
        let _io = optional_io_permit(self.admission.as_ref()).await?;
        let (access, relative_path) = self.resolve_path("reader", path).await?;
        self.check_read_active("reader")?;
        Ok(Box::new(IcebergFsFileRead {
            access,
            relative_path,
            control: self.read_control(),
            admission: self.admission.clone(),
            binding: self.binding.clone(),
            location: path.to_owned(),
        }))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let permit = match &self.admission {
            Some(admission) => Some(admission.clone().admit(path)?.acquire().await?),
            None => None,
        };
        let _phase = self.non_http_phase(path).await?;
        let (access, relative_path) = self.resolve_path("write", path).await?;
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
        let phase = match self.binding.as_ref() {
            Some(binding) => binding.admit_owned_stream(path).await.map_err(|error| {
                Error::new(
                    ErrorKind::Unexpected,
                    "original writer I/O admission refused",
                )
                .with_source(error)
            })?,
            None => None,
        };
        let (access, relative_path) = self.resolve_path("writer", path).await?;
        let access = match (
            self.binding
                .as_ref()
                .and_then(IcebergReadBinding::request_context),
            phase.as_ref(),
        ) {
            (Some(request), Some(phase)) => IcebergFsAccess::new(
                access
                    .handle
                    .with_request_context(request.clone().with_io_weak_phase(phase.weak_phase())),
            ),
            _ => access,
        };
        let op = access.operator();
        if let Some(permit) = &permit {
            permit.check_active()?;
        }
        Self::ensure_parent_dir(&op, &relative_path).await?;
        if let Some(permit) = &permit {
            permit.check_active()?;
        }
        let stream = phase
            .as_ref()
            .map(novarocks_spi::connector::ConnectorIoPermit::begin_stream)
            .transpose()
            .map_err(|error| {
                Error::new(ErrorKind::Unexpected, "original stream custody refused")
                    .with_source(error)
            })?;
        let mut writer = match op.writer(&relative_path).await {
            Ok(writer) => writer,
            Err(error) => {
                if let Some(stream) = stream {
                    stream.closed();
                }
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("fs writer({path}) through {relative_path}: {error}"),
                ));
            }
        };
        if let Some(permit) = &permit {
            permit
                .stream_started
                .store(true, std::sync::atomic::Ordering::Release);
            if let Err(error) = permit.check_active() {
                let error = abort_after_failure(&mut writer, error, Some(permit)).await;
                drop(writer);
                if let Some(stream) = stream {
                    stream.closed();
                }
                return Err(error);
            }
        }
        Ok(Box::new(IcebergFsFileWrite {
            writer: Some(writer),
            permit,
            _phase: phase,
            stream,
            local_closed: false,
        }))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let _phase = self.non_http_phase(path).await?;
        let (access, relative_path) = self.resolve_path("delete", path).await?;
        access.operator().delete(&relative_path).await.map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("fs delete({path}) through {relative_path}: {e}"),
            )
        })
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let _phase = self.non_http_phase(path).await?;
        let (access, relative_path) = self.resolve_path("delete_prefix", path).await?;
        access
            .operator()
            .delete_with(&relative_path)
            .recursive(true)
            .await
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("fs delete_prefix({path}) through {relative_path}: {e}"),
                )
            })
    }

    async fn delete_stream(
        &self,
        mut paths: futures::stream::BoxStream<'static, String>,
    ) -> Result<()> {
        while let Some(path) = paths.next().await {
            self.delete(&path).await?;
        }
        Ok(())
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(self.storage_arc(), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(self.storage_arc(), path.to_string()))
    }
}

struct IcebergFsFileRead {
    binding: Option<IcebergReadBinding>,
    location: String,
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
        let _phase = match self.binding.as_ref() {
            Some(binding) => binding
                .admit_non_http_io(&self.location)
                .await
                .map_err(|e| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "original range I/O admission refused",
                    )
                    .with_source(e)
                })?,
            None => None,
        };
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
    _phase: Option<novarocks_spi::connector::ConnectorIoPermit>,
    stream: Option<novarocks_spi::connector::ConnectorIoStreamCustody>,
    local_closed: bool,
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
        if self.stream.is_some() {
            if let Some(writer) = self.writer.take() {
                let permit = self.permit.take();
                let mut cleanup: Option<novarocks_spi::connector::ConnectorIoCleanupFuture> =
                    Some(Box::pin(governed_writer_abort(
                        writer,
                        permit,
                        self._phase.as_ref().unwrap().weak_phase(),
                    )));
                let stream = self.stream.take().unwrap();
                if let Err((stream, error)) = stream.install_cleanup_in(&mut cleanup) {
                    // Whole concrete refusal retains all original custody in
                    // the same Source's original panic packet, never strings.
                    std::panic::panic_any(GovernedCleanupRefusal {
                        _cleanup: cleanup,
                        _stream: stream,
                        _error: error,
                        _phase: self._phase.take(),
                    });
                }
                self._phase.take();
                return;
            }
            if self.local_closed {
                self.stream.take().unwrap().closed();
            }
            // Otherwise the unique disposition Drop marks the original cell
            // unproven; None alone is never an actual-exit receipt.
            return;
        }
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
                drop(writer);
                self.local_closed = true;
                self.permit.take();
                if let Some(stream) = self.stream.take() {
                    stream.closed();
                }
                self._phase.take();
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
            let result = writer
                .close()
                .await
                .map_err(|e| Error::new(ErrorKind::Unexpected, format!("fs close: {e}")));
            drop(writer);
            self.local_closed = true;
            if let Some(stream) = self.stream.take() {
                stream.closed();
            }
            self._phase.take();
            return result.map(|_metadata| ());
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
        drop(writer);
        self.local_closed = true;
        self.permit.take();
        if let Some(stream) = self.stream.take() {
            stream.closed();
        }
        self._phase.take();
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

/// Write one small finalization document under the caller's admitted scope.
/// Each new storage operation checks the same grant; an issued PUT is retained
/// until its actual exit even if that grant expires while it is in flight.
pub(crate) async fn write_finalization_document(
    location: &str,
    binding: IcebergReadBinding,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    bytes: Bytes,
) -> Result<()> {
    finalization_active(context)?;
    let storage = IcebergFsStorage::new(binding);
    let _phase = storage.non_http_phase(location).await?;
    let (access, relative) = storage
        .resolve_path("write finalization document", location)
        .await?;
    write_finalization_document_to_operator(access.operator(), &relative, context, bytes).await
}

fn finalization_active(context: &novarocks_spi::connector::ConnectorRequestContext) -> Result<()> {
    context.check_active().map_err(|error| {
        Error::new(
            ErrorKind::Unexpected,
            "Finalization storage request stopped",
        )
        .with_source(error)
    })
}

async fn write_finalization_document_to_operator(
    operator: Operator,
    relative: &str,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    bytes: Bytes,
) -> Result<()> {
    finalization_active(context)?;
    IcebergFsStorage::ensure_parent_dir(&operator, relative).await?;
    finalization_active(context)?;
    operator.write(relative, bytes).await.map_err(|error| {
        Error::new(ErrorKind::Unexpected, "Finalization document PUT failed").with_source(error)
    })?;
    finalization_active(context)
}

/// Enumerate child directories directly through the provider-owned capability.
pub(crate) async fn list_directories(
    location: &str,
    binding: IcebergReadBinding,
    bound: ConnectorListingBound,
    workspace_bytes: usize,
) -> Result<Vec<String>> {
    let storage = IcebergFsStorage {
        binding: Some(binding),
        listing: Some((bound, workspace_bytes)),
        admission: None,
    };
    storage.list_directories(location).await
}

/// Reads may stop at the original request's absolute deadline without leaving
/// writes or cleanup work in flight. Preserve the request's typed stop fact.
pub(crate) async fn catalog_read<T>(
    binding: &IcebergReadBinding,
    future: impl std::future::Future<Output = T>,
) -> Result<T> {
    let cancellation = binding
        .request_context()
        .map(novarocks_fs::FileCancellation::from_connector_request)
        .unwrap_or_default();
    cancellation.check().map_err(|error| {
        Error::new(ErrorKind::Unexpected, "Hadoop catalog read stopped")
            .with_source(ConnectorError::from(error))
    })?;
    match futures::future::select(Box::pin(future), Box::pin(cancellation.ended())).await {
        futures::future::Either::Left((value, _)) => Ok(value),
        futures::future::Either::Right((error, _)) => Err(Error::new(
            ErrorKind::Unexpected,
            "Hadoop catalog read stopped",
        )
        .with_source(ConnectorError::from(error))),
    }
}

/// Visit one bounded, nonrecursive stream. Every raw entry spends an entry,
/// including directories and unrelated files; an early stop is not an inventory.
pub(crate) async fn visit_direct_files<F>(
    location: &str,
    binding: IcebergReadBinding,
    bound: ConnectorListingBound,
    mut visit: F,
) -> Result<bool>
where
    F: FnMut(&str) -> Result<bool>,
{
    let storage = IcebergFsStorage::new(binding.clone());
    storage.check_read_active("list version files")?;
    bound.validate().map_err(|error| {
        Error::new(ErrorKind::DataInvalid, "Invalid Hadoop listing bound").with_source(error)
    })?;
    check_listing_path_size(location.len())?;
    let _phase = storage.non_http_phase(location).await?;
    let (access, mut relative) = storage.resolve_path("list version files", location).await?;
    if !relative.ends_with('/') {
        relative.push('/');
    }
    let operator = access.operator();
    #[cfg(test)]
    note_hadoop_test_list();
    let result = catalog_read(&binding, async {
        operator
            .lister_with(&relative)
            .limit(bound.page_entries)
            .await
    })
    .await?;
    storage.check_read_active("list version files")?;
    let mut entries = result.map_err(listing_storage_error)?;
    let prefix = relative.trim_start_matches('/');
    let mut count = 0usize;
    while let Some(entry) = catalog_read(&binding, entries.next()).await? {
        storage.check_read_active("list version files")?;
        let entry = entry.map_err(listing_storage_error)?;
        let path = entry.path().trim_start_matches('/');
        check_listing_path_size(path.len())?;
        let name = path.strip_prefix(prefix).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Hadoop listing escaped its metadata root",
            )
        })?;
        // OpenDAL's local lister emits the requested directory itself before
        // its children. It is not a raw child entry and cannot spend their bound.
        if name.is_empty() {
            continue;
        }
        count += 1;
        if count > bound.entries {
            return Err(listing_refusal());
        }
        if entry.metadata().is_file() && !name.contains('/') && !visit(name)? {
            return Ok(false);
        }
    }
    storage.check_read_active("list version files")?;
    Ok(true)
}

fn listing_storage_error(error: crate::opendal::Error) -> Error {
    Error::new(ErrorKind::Unexpected, "Hadoop filesystem traversal failed").with_source(
        ConnectorError::from(novarocks_fs::map_object_store_listing_error(error)),
    )
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

    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};
    use novarocks_spi::connector::{
        ConnectorError, ConnectorErrorKind, ConnectorRequestContext,
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };

    use super::{
        DirectoryListing, IcebergFsStorage, build_file_io_for_location, format_resolved_location,
        resolve_access_for_location, resolve_access_for_locations,
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
    impl crate::opendal::raw::Service for ControlledWriterService {
        type Reader = ();
        type Writer = crate::opendal::raw::oio::Writer;
        type Lister = ();
        type Deleter = ();
        type Copier = ();
        fn info(&self) -> crate::opendal::raw::ServiceInfo {
            crate::opendal::raw::ServiceInfo::with_scheme("mock")
        }
        fn capability(&self) -> crate::opendal::Capability {
            crate::opendal::Capability {
                write: true,
                write_can_multi: true,
                ..Default::default()
            }
        }
        async fn create_dir(
            &self,
            ctx: &crate::opendal::OperationContext,
            path: &str,
            args: crate::opendal::raw::OpCreateDir,
        ) -> crate::opendal::Result<crate::opendal::raw::RpCreateDir> {
            crate::opendal::raw::Service::create_dir(&(), ctx, path, args).await
        }
        async fn stat(
            &self,
            ctx: &crate::opendal::OperationContext,
            path: &str,
            args: crate::opendal::raw::OpStat,
        ) -> crate::opendal::Result<crate::opendal::raw::RpStat> {
            crate::opendal::raw::Service::stat(&(), ctx, path, args).await
        }
        fn read(
            &self,
            ctx: &crate::opendal::OperationContext,
            path: &str,
            args: crate::opendal::raw::OpRead,
        ) -> crate::opendal::Result<Self::Reader> {
            crate::opendal::raw::Service::read(&(), ctx, path, args)
        }
        fn write(
            &self,
            ctx: &crate::opendal::OperationContext,
            path: &str,
            args: crate::opendal::raw::OpWrite,
        ) -> crate::opendal::Result<Self::Writer> {
            let _ = (ctx, path, args);
            Ok(Box::new(ControlledWriter(self.clone())))
        }
        fn delete(
            &self,
            ctx: &crate::opendal::OperationContext,
        ) -> crate::opendal::Result<Self::Deleter> {
            crate::opendal::raw::Service::delete(&(), ctx)
        }
        fn list(
            &self,
            ctx: &crate::opendal::OperationContext,
            path: &str,
            args: crate::opendal::raw::OpList,
        ) -> crate::opendal::Result<Self::Lister> {
            crate::opendal::raw::Service::list(&(), ctx, path, args)
        }
        fn copy(
            &self,
            ctx: &crate::opendal::OperationContext,
            from: &str,
            to: &str,
            args: crate::opendal::raw::OpCopy,
            opts: crate::opendal::raw::OpCopier,
        ) -> crate::opendal::Result<Self::Copier> {
            crate::opendal::raw::Service::copy(&(), ctx, from, to, args, opts)
        }
        async fn rename(
            &self,
            ctx: &crate::opendal::OperationContext,
            from: &str,
            to: &str,
            args: crate::opendal::raw::OpRename,
        ) -> crate::opendal::Result<crate::opendal::raw::RpRename> {
            crate::opendal::raw::Service::rename(&(), ctx, from, to, args).await
        }
        async fn presign(
            &self,
            ctx: &crate::opendal::OperationContext,
            path: &str,
            args: crate::opendal::raw::OpPresign,
        ) -> crate::opendal::Result<crate::opendal::raw::RpPresign> {
            crate::opendal::raw::Service::presign(&(), ctx, path, args).await
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
        let service: crate::opendal::raw::Servicer = Arc::new(service);
        // Exercise the exact SDK wrapper which moves inner ownership into the pending future.
        let operator = crate::opendal::Operator::from_parts(
            crate::opendal::OperationContext::default(),
            service,
        )
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
                _phase: None,
                stream: None,
                local_closed: false,
            },
            admission,
            events,
        )
    }

    #[tokio::test]
    async fn finalization_document_waits_for_actual_put_exit_and_never_extends_the_grant() {
        use novarocks_spi::connector::{ConnectorRequestContext, ConnectorStopOwner};
        let original = ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(5),
            ConnectorStopOwner::new().view(),
            64,
            128,
        )
        .unwrap();
        let grant = crate::catalog::publication::CatalogEvidencePolicy {
            finalization: std::time::Duration::from_millis(20),
            ..Default::default()
        }
        .begin_finalization()
        .unwrap();
        let context = grant.for_io(&original).unwrap();
        let events = Arc::new(WriterEvents::default());
        let service: crate::opendal::raw::Servicer = Arc::new(ControlledWriterService {
            events: events.clone(),
            delay_close: true,
            fail_write: false,
        });
        let operator = crate::opendal::Operator::from_parts(
            crate::opendal::OperationContext::default(),
            service,
        )
        .layer(crate::opendal::layers::RetryLayer::new().with_max_times(0));
        let used_operator = operator.clone();
        let used_context = context.clone();
        let task = tokio::spawn(async move {
            super::write_finalization_document_to_operator(
                used_operator,
                "provenance.json",
                &used_context,
                Bytes::from_static(b"owned provenance"),
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), events.started.notified())
            .await
            .expect("issued PUT must enter its controlled close before the readiness bound");
        tokio::time::sleep_until((grant.deadline() + std::time::Duration::from_millis(5)).into())
            .await;
        assert!(
            !task.is_finished(),
            "issued PUT/close still owns its actual work after deadline"
        );
        assert_eq!(
            *events.log.lock().unwrap(),
            ["write-start", "write-exit", "close-start"]
        );
        events.resume.notify_one();
        assert!(task.await.unwrap().is_err());
        assert_eq!(
            *events.log.lock().unwrap(),
            ["write-start", "write-exit", "close-start", "close-exit"]
        );
        assert!(
            super::write_finalization_document_to_operator(
                operator,
                "again.json",
                &context,
                Bytes::new()
            )
            .await
            .is_err()
        );
        assert_eq!(
            events.log.lock().unwrap().len(),
            4,
            "expired grant starts no second PUT"
        );
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

    struct AbortErrorReceiver {
        original: std::sync::Mutex<Option<Box<dyn std::any::Any + Send>>>,
    }
    impl novarocks_spi::connector::ConnectorIoRequestSource for AbortErrorReceiver {
        fn exit_waker(&self) -> std::task::Waker {
            std::task::Waker::noop().clone()
        }
        fn reserve_stream(&self) -> Result<(), ConnectorError> {
            Ok(())
        }
        fn stream_closed(&self) {}
        fn install_stream_cleanup_in(
            &self,
            _: &mut Option<novarocks_spi::connector::ConnectorIoCleanupFuture>,
        ) -> Result<(), ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "test does not dispatch cleanup",
            ))
        }
        fn stream_custody_lost(&self) {}
        fn record_cleanup_failure(
            &self,
            cause: Box<dyn std::any::Any + Send>,
        ) -> Result<(), Box<dyn std::any::Any + Send>> {
            let mut original = self.original.lock().unwrap();
            if original.is_some() {
                return Err(cause);
            }
            *original = Some(cause);
            Ok(())
        }
        fn attach_original_progress(
            &self,
            _: Arc<dyn novarocks_spi::connector::ConnectorIoProgressSource>,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }
        fn poll_admitted(
            &self,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), ConnectorError>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_include_dependency(
            &self,
            _: &novarocks_spi::connector::SharedDependency,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), ConnectorError>> {
            std::task::Poll::Ready(Ok(()))
        }
    }
    #[tokio::test]
    async fn two_real_abort_errors_exit_and_deliver_their_original_errors_once() {
        let source = Arc::new(AbortErrorReceiver {
            original: std::sync::Mutex::new(None),
        });
        for _ in 0..2 {
            let (mut wrapper, admission, events) = governed_mock_writer(false, false).await;
            events
                .fail_abort
                .store(true, std::sync::atomic::Ordering::SeqCst);
            let writer = wrapper.writer.take().unwrap();
            let permit = wrapper.permit.take();
            drop(wrapper);
            let request =
                novarocks_spi::connector::ConnectorIoRequest::from_original(Arc::clone(&source)
                    as Arc<dyn novarocks_spi::connector::ConnectorIoRequestSource>);
            let phase = request.admitted().await.unwrap();
            let result = super::governed_writer_abort(writer, permit, phase.weak_phase()).await;
            assert!(result.is_err());
            assert_eq!(*events.log.lock().unwrap(), ["abort-exit"]);
            assert_eq!(
                admission.semaphore.available_permits(),
                1,
                "actual helper returns after the original physical permit exits"
            );
            assert_eq!(admission.exit_failures.lock().unwrap().len(), 1);
            let original = source
                .original
                .lock()
                .unwrap()
                .take()
                .expect("received before writer Drop");
            assert_eq!(
                original
                    .downcast_ref::<crate::opendal::Error>()
                    .unwrap()
                    .kind(),
                crate::opendal::ErrorKind::Unexpected
            );
            drop(original);
            drop(phase);
            assert!(source.original.lock().unwrap().is_none());
        }
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
        let complete = IcebergFsStorage {
            binding: Some(binding.clone()),
            listing: Some((bound, 4096)),
            admission: None,
        };
        assert_eq!(
            complete
                .list_directories(&location)
                .await
                .expect("complete enumeration"),
            [".hidden", "a", "z"]
        );
        let refused = IcebergFsStorage {
            binding: Some(binding),
            listing: Some((
                novarocks_spi::connector::ConnectorListingBound {
                    entries: 2,
                    ..bound
                },
                4096,
            )),
            admission: None,
        };
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

    #[tokio::test]
    async fn direct_version_stream_refuses_partial_inventory_and_preserves_stop_fact() {
        use novarocks_failure_contract::FailureProjection;
        let directory = tempfile::tempdir().expect("directory");
        for name in ["v1.metadata.json", "v2.metadata.json"] {
            std::fs::write(
                directory.path().join(name),
                b"JSON is not needed for listing",
            )
            .expect("file");
        }
        let stop = Arc::new(novarocks_spi::connector::ConnectorStopOwner::new());
        let request = request(Instant::now() + Duration::from_secs(30), stop.clone());
        let binding = local_test_binding(None, tokio::runtime::Handle::current())
            .for_request(request.clone());
        let mut visited = 0;
        let error = super::visit_direct_files(
            directory.path().to_str().expect("directory path"),
            binding,
            novarocks_spi::connector::ConnectorListingBound::V1,
            |_| {
                visited += 1;
                stop.request_stop();
                Ok(true)
            },
        )
        .await
        .expect_err("partial stream must fail");
        assert_eq!(visited, 1);
        let actual = std::error::Error::source(&error)
            .and_then(|source| source.downcast_ref::<ConnectorError>())
            .expect("typed connector stop");
        assert_eq!(actual.kind(), ConnectorErrorKind::Cancelled);
        assert_eq!(
            actual.failure_fact(),
            ConnectorError::from(
                novarocks_fs::FileCancellation::from_connector_request(&request)
                    .check()
                    .expect_err("same file stop")
            )
            .failure_fact()
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

#[cfg(test)]
pub(crate) mod manifest_stream_tests {
    //! Protect the SDK manifest/list streaming boundary and real operation ownership.
    //! Expected allocation/admission/cleanup ordering comes from the 0.9 behavior
    //! and ADR-0171, not from a replacement ledger. The only injected behavior is
    //! splitting a real FileWrite and holding its second segment. Existing governed
    //! MockWrite tests remain the canonical protection for an issued I/O step's exit.

    use super::*;
    use crate::commit::model::{
        AddedContent, ArtifactClass, ArtifactKind, ArtifactWriteState, ArtifactWriter,
        AttemptArtifacts, AttemptToken, CleanupRemainingReason, CleanupScope, IcebergCleanupReport,
        ObjectIdentity,
    };
    use crate::commit::operation::{
        CleanupBudget, IcebergCommitAttempt, IcebergCommitOperation, OperationLimits,
    };
    use crate::commit::overwrite::preparer_tests::Fixture;
    use crate::iceberg::spec::{
        FormatVersion, Manifest, ManifestContentType, ManifestList, Struct,
    };
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    #[derive(Debug, Default)]
    struct HalfWriteGate {
        first_half_written: tokio::sync::Notify,
        resume: tokio::sync::Notify,
        bytes_written: AtomicUsize,
        write_exited: AtomicBool,
    }

    #[derive(Debug)]
    struct ManifestStreamProbe {
        operation: IcebergCommitOperation,
        original: FileIO,
        gate: Option<Arc<HalfWriteGate>>,
        atomic: Option<AtomicManifestWriterBoundary>,
        writers: AtomicUsize,
        whole_writes: AtomicUsize,
        allocated_before_output: Mutex<Vec<ObjectIdentity>>,
    }

    #[derive(Debug)]
    pub(crate) struct AtomicManifestWriterBoundary {
        operator: Operator,
        root: std::path::PathBuf,
        temporary: std::path::PathBuf,
    }

    impl AtomicManifestWriterBoundary {
        pub(crate) fn new(root: &Path) -> Self {
            let root = root.to_path_buf();
            let temporary = root.join(".atomic");
            std::fs::create_dir_all(&temporary).unwrap();
            let operator = Operator::new(
                crate::opendal::services::Fs::default()
                    .root(root.to_str().unwrap())
                    .atomic_write_dir(temporary.to_str().unwrap()),
            )
            .unwrap();
            Self {
                operator,
                root,
                temporary,
            }
        }

        pub(crate) async fn writer(
            &self,
            operation: &IcebergCommitOperation,
            path: &str,
        ) -> Result<Box<dyn FileWrite>> {
            let target = url::Url::parse(path).unwrap().to_file_path().unwrap();
            let relative = target.strip_prefix(&self.root).unwrap().to_string_lossy();
            let permit = operation
                .write_admission_for_test()
                .admit(path)?
                .acquire()
                .await?;
            permit.check_active()?;
            IcebergFsStorage::ensure_parent_dir(&self.operator, &relative).await?;
            permit.check_active()?;
            // For this fresh owned path, append selects FS's serial writer
            // while still creating an atomic temporary file. The default
            // positional writer caches its first segment until a later write.
            assert!(!target.exists(), "test artifact must be a fresh owned path");
            let mut writer = self
                .operator
                .writer_with(&relative)
                .append(true)
                .await
                .map_err(|error| {
                    Error::new(ErrorKind::Unexpected, "open atomic artifact test writer")
                        .with_source(error)
                })?;
            permit.stream_started.store(true, Ordering::Release);
            if let Err(error) = permit.check_active() {
                return Err(abort_after_failure(&mut writer, error, Some(&permit)).await);
            }
            Ok(Box::new(IcebergFsFileWrite {
                writer: Some(writer),
                permit: Some(permit),
                _phase: None,
                stream: None,
                local_closed: false,
            }))
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct ManifestProbeStorage {
        #[serde(skip)]
        probe: Option<Arc<ManifestStreamProbe>>,
    }

    impl ManifestProbeStorage {
        fn probe(&self) -> &ManifestStreamProbe {
            self.probe.as_deref().expect("initialized manifest probe")
        }
    }

    #[async_trait]
    #[typetag::serde]
    impl Storage for ManifestProbeStorage {
        async fn exists(&self, path: &str) -> Result<bool> {
            self.probe().original.new_input(path)?.exists().await
        }
        async fn metadata(&self, path: &str) -> Result<FileMetadata> {
            self.probe().original.new_input(path)?.metadata().await
        }
        async fn read(&self, path: &str) -> Result<Bytes> {
            self.probe().original.new_input(path)?.read().await
        }
        async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
            self.probe().original.new_input(path)?.reader().await
        }
        async fn write(&self, path: &str, bytes: Bytes) -> Result<()> {
            self.probe().whole_writes.fetch_add(1, Ordering::SeqCst);
            self.probe().original.new_output(path)?.write(bytes).await
        }
        async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
            let probe = self.probe();
            let inner: Box<dyn FileWrite> = if let Some(atomic) = &probe.atomic {
                // Only the cancelled fixture substitutes the resolver's default
                // local operator. Use a real private atomic FS backend (default
                // FS abort is Unsupported), preserving the real operation's
                // admission, WritePermit and IcebergFsFileWrite lifecycle.
                atomic.writer(&probe.operation, path).await?
            } else {
                // Normal manifest and list outputs exercise the complete original
                // IcebergFsStorage path, including resolution and writer opening.
                probe.original.new_output(path)?.writer().await?
            };
            probe.writers.fetch_add(1, Ordering::SeqCst);
            assert!(
                probe
                    .operation
                    .artifacts()?
                    .iter()
                    .any(|record| record.object.path() == path
                        && record.write_state == ArtifactWriteState::Writing)
            );
            Ok(Box::new(ManifestProbeWriter {
                inner,
                gate: probe.gate.clone(),
            }))
        }
        async fn delete(&self, path: &str) -> Result<()> {
            self.probe().original.delete(path).await
        }
        async fn delete_prefix(&self, path: &str) -> Result<()> {
            self.probe().original.delete_prefix(path).await
        }
        async fn delete_stream(
            &self,
            mut paths: futures::stream::BoxStream<'static, String>,
        ) -> Result<()> {
            while let Some(path) = paths.next().await {
                self.delete(&path).await?;
            }
            Ok(())
        }
        fn new_input(&self, path: &str) -> Result<InputFile> {
            Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
        }
        fn new_output(&self, path: &str) -> Result<OutputFile> {
            let probe = self.probe();
            let record = probe
                .operation
                .artifacts()?
                .into_iter()
                .find(|record| record.object.path() == path)
                .expect("the real operation must register an output before SDK construction");
            assert_eq!(record.write_state, ArtifactWriteState::Allocated);
            probe
                .allocated_before_output
                .lock()
                .unwrap()
                .push(record.object);
            Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct ManifestProbeFactory {
        #[serde(skip)]
        probe: Option<Arc<ManifestStreamProbe>>,
    }

    #[typetag::serde]
    impl StorageFactory for ManifestProbeFactory {
        fn build(&self, _: &StorageConfig) -> Result<Arc<dyn Storage>> {
            Ok(Arc::new(ManifestProbeStorage {
                probe: self.probe.clone(),
            }))
        }
    }

    struct ManifestProbeWriter {
        inner: Box<dyn FileWrite>,
        gate: Option<Arc<HalfWriteGate>>,
    }

    #[async_trait]
    impl FileWrite for ManifestProbeWriter {
        async fn write(&mut self, bytes: Bytes) -> Result<()> {
            let Some(gate) = self.gate.take() else {
                return self.inner.write(bytes).await;
            };
            let split = bytes.len() / 2;
            assert!(split > 0, "a manifest must have an actual first segment");
            self.inner.write(bytes.slice(..split)).await?;
            gate.bytes_written.store(split, Ordering::SeqCst);
            gate.first_half_written.notify_one();
            gate.resume.notified().await;
            let result = self.inner.write(bytes.slice(split..)).await;
            // A cancellation error is published only after the real writer
            // has observed stop and awaited its abort/permit exit.
            gate.write_exited.store(true, Ordering::SeqCst);
            result
        }
        async fn close(&mut self) -> Result<()> {
            self.inner.close().await
        }
    }

    struct ManifestProbeArtifacts {
        attempt: IcebergCommitAttempt,
        file_io: FileIO,
    }

    impl ArtifactWriter for ManifestProbeArtifacts {
        fn file_io(&self) -> &FileIO {
            &self.file_io
        }
        fn attempt_token(&self) -> AttemptToken {
            self.attempt.attempt_token()
        }
        fn check_active(&self) -> Result<()> {
            self.attempt.check_active()
        }
        fn allocate(&self, class: ArtifactClass, kind: ArtifactKind) -> Result<ObjectIdentity> {
            self.attempt.allocate(class, kind)
        }
        fn snapshot_artifacts(&self, references: &[ObjectIdentity]) -> Result<AttemptArtifacts> {
            self.attempt.snapshot_artifacts(references)
        }
    }

    fn probe_artifacts(
        fixture: &Fixture,
        gate: Option<Arc<HalfWriteGate>>,
    ) -> (ManifestProbeArtifacts, Arc<ManifestStreamProbe>) {
        let attempt = fixture.operation.begin_attempt().unwrap();
        let atomic = gate
            .as_ref()
            .map(|_| AtomicManifestWriterBoundary::new(fixture.directory.path()));
        let probe = Arc::new(ManifestStreamProbe {
            operation: fixture.operation.clone(),
            original: attempt.file_io().clone(),
            gate,
            atomic,
            writers: AtomicUsize::new(0),
            whole_writes: AtomicUsize::new(0),
            allocated_before_output: Mutex::new(Vec::new()),
        });
        let file_io = FileIOBuilder::new(Arc::new(ManifestProbeFactory {
            probe: Some(probe.clone()),
        }))
        .build();
        (ManifestProbeArtifacts { attempt, file_io }, probe)
    }

    fn limits() -> OperationLimits {
        OperationLimits {
            io_requests: 1,
            cleanup: CleanupBudget {
                time: Duration::from_millis(100),
                objects: 1,
            },
        }
    }

    async fn write_one_manifest(
        artifacts: &dyn ArtifactWriter,
        metadata: &crate::iceberg::spec::TableMetadata,
    ) -> Result<crate::iceberg::spec::ManifestFile> {
        let file = crate::commit::overwrite::preparer_tests::data(
            &format!("{}/data/one.parquet", metadata.location()),
            1,
            Struct::empty(),
        );
        crate::commit::staging::write_manifest(
            artifacts,
            metadata.format_version(),
            7,
            metadata.current_schema().clone(),
            metadata.default_partition_spec().as_ref().clone(),
            ManifestContentType::Data,
            [crate::commit::staging::ManifestEntryWrite::Added(
                AddedContent::new_logical_data(file, metadata.default_partition_spec_id())?,
            )],
        )
        .await
    }

    #[tokio::test]
    async fn sdk_manifest_streams_register_before_admission_and_obey_cleanup_object_bound() {
        let fixture = Fixture::new_with_limits(limits());
        let metadata = fixture.metadata(FormatVersion::V3);
        let (artifacts, probe) = probe_artifacts(&fixture, None);
        let unregistered = format!(
            "file://{}/unregistered.avro",
            fixture.directory.path().display()
        );
        assert!(
            artifacts
                .attempt
                .file_io()
                .new_output(&unregistered)
                .unwrap()
                .writer()
                .await
                .is_err()
        );
        assert!(!fixture.directory.path().join("unregistered.avro").exists());

        let manifest = write_one_manifest(&artifacts, &metadata).await.unwrap();
        let bytes = artifacts
            .file_io()
            .new_input(&manifest.manifest_path)
            .unwrap()
            .read()
            .await
            .unwrap();
        assert_eq!(Manifest::parse_avro(&bytes).unwrap().entries().len(), 1);
        let list = crate::commit::staging::write_manifest_list(
            &artifacts,
            &metadata,
            7,
            None,
            [manifest.clone()],
        )
        .await
        .unwrap();
        let bytes = artifacts
            .file_io()
            .new_input(list.object.path())
            .unwrap()
            .read()
            .await
            .unwrap();
        let parsed = ManifestList::parse_with_version(&bytes, FormatVersion::V3).unwrap();
        assert_eq!(parsed.entries().len(), 1);
        assert_eq!(parsed.entries()[0].manifest_path, manifest.manifest_path);
        assert_eq!(probe.writers.load(Ordering::SeqCst), 2);
        assert_eq!(probe.whole_writes.load(Ordering::SeqCst), 0);
        assert_eq!(probe.allocated_before_output.lock().unwrap().len(), 2);
        let records = fixture.operation.artifacts().unwrap();
        assert_eq!(records.len(), 2);
        assert!(
            records
                .iter()
                .all(|record| record.write_state == ArtifactWriteState::Written)
        );

        let report = fixture
            .operation
            .cleanup(CleanupScope::EntireOperation)
            .await;
        let IcebergCleanupReport::Partial {
            deleted: 1,
            remaining,
        } = report
        else {
            panic!("cleanup must obey the one-object bound: {report:?}")
        };
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].reason, CleanupRemainingReason::BudgetExhausted);
        assert_eq!(fixture.operation.artifacts().unwrap().len(), 1);
        assert!(matches!(
            fixture
                .operation
                .cleanup(CleanupScope::EntireOperation)
                .await,
            IcebergCleanupReport::Complete { deleted: 1 }
        ));
        assert!(fixture.operation.artifacts().unwrap().is_empty());
        for record in records {
            assert!(!Path::new(record.object.path().strip_prefix("file://").unwrap()).exists());
        }
    }

    #[tokio::test]
    async fn sdk_manifest_half_write_cancellation_retains_owner_until_exit_and_cleanup() {
        let fixture = Fixture::new_with_limits(limits());
        let metadata = fixture.metadata(FormatVersion::V3);
        let gate = Arc::new(HalfWriteGate::default());
        let (artifacts, probe) = probe_artifacts(&fixture, Some(gate.clone()));
        let task = tokio::spawn(async move { write_one_manifest(&artifacts, &metadata).await });
        tokio::time::timeout(Duration::from_secs(1), gate.first_half_written.notified())
            .await
            .unwrap();
        let expected_partial_bytes = gate.bytes_written.load(Ordering::SeqCst);
        assert!(expected_partial_bytes > 0);
        // Observe bytes in the private atomic temporary file before cancellation,
        // so this case cannot pass by holding only an in-memory SDK buffer.
        let temporary = &probe.atomic.as_ref().unwrap().temporary;
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let entries = std::fs::read_dir(temporary)
                    .unwrap()
                    .collect::<std::io::Result<Vec<_>>>()
                    .unwrap();
                if entries.len() == 1
                    && std::fs::metadata(entries[0].path()).unwrap().len()
                        == expected_partial_bytes as u64
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        fixture.cancel();
        assert!(!task.is_finished());
        let records = fixture.operation.artifacts().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].write_state, ArtifactWriteState::Writing);
        let report = tokio::time::timeout(
            Duration::from_secs(1),
            fixture.operation.cleanup(CleanupScope::EntireOperation),
        )
        .await
        .unwrap();
        let IcebergCleanupReport::Partial {
            deleted: 0,
            remaining,
        } = report
        else {
            panic!("pending stream cannot prove cleanup: {report:?}")
        };
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].object, records[0].object);
        assert_eq!(remaining[0].reason, CleanupRemainingReason::BudgetExhausted);
        assert_eq!(fixture.operation.artifacts().unwrap(), records);
        assert!(!task.is_finished());
        assert!(!gate.write_exited.load(Ordering::SeqCst));
        gate.resume.notify_one();
        let write_error = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .expect_err("cancelled manifest write must fail");
        assert!(gate.write_exited.load(Ordering::SeqCst));
        assert!(
            std::fs::read_dir(temporary).unwrap().next().is_none(),
            "real abort must remove the atomic temporary file"
        );
        assert_eq!(probe.writers.load(Ordering::SeqCst), 1);
        assert_eq!(probe.whole_writes.load(Ordering::SeqCst), 0);
        let final_cleanup = fixture
            .operation
            .cleanup(CleanupScope::EntireOperation)
            .await;
        assert!(
            matches!(final_cleanup, IcebergCleanupReport::Complete { deleted: 1 }),
            "final cleanup: {final_cleanup:?}; ledger: {:?}; write error: {write_error:?}",
            fixture.operation.artifacts().unwrap()
        );
        assert!(fixture.operation.artifacts().unwrap().is_empty());
        assert!(!Path::new(records[0].object.path().strip_prefix("file://").unwrap()).exists());
    }
}

// The panic packet owns these originals until its receiving owner disposes it.
struct GovernedCleanupRefusal {
    _cleanup: Option<novarocks_spi::connector::ConnectorIoCleanupFuture>,
    _stream: novarocks_spi::connector::ConnectorIoStreamCustody,
    _error: ConnectorError,
    _phase: Option<novarocks_spi::connector::ConnectorIoPermit>,
}
async fn governed_writer_abort(
    mut writer: crate::opendal::Writer,
    permit: Option<WritePermit>,
    phase: novarocks_spi::connector::ConnectorIoWeakPhase,
) -> std::result::Result<(), ConnectorError> {
    let result = match writer.abort().await {
        Ok(()) => Ok(()),
        Err(cause) => {
            if let Err(cause) = phase.record_cleanup_failure(Box::new(cause)) {
                std::panic::panic_any(GovernedAbortErrorRefusal {
                    _cause: cause,
                    _writer: writer,
                    _permit: permit,
                    _phase: phase,
                });
            }
            Err(ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                "original multipart abort failed",
            ))
        }
    };
    if let Some(permit) = permit.as_ref() {
        match &result {
            Ok(()) => permit.confirm_exit(),
            Err(error) => permit.admission.record_exit_failure(
                &permit.object,
                StreamExitFailure::AbortFailed(error.to_string()),
            ),
        }
    }
    // Result is retained by the same original cell before F destruction.
    drop(writer);
    drop(permit);
    result.map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            format!("original multipart abort failed: {error}"),
        )
    })
}
pub(crate) fn governed_writer_backing_bytes() -> std::result::Result<usize, ConnectorError> {
    fn future_size<A, B, C, F: std::future::Future>(_: fn(A, B, C) -> F) -> usize {
        std::mem::size_of::<F>()
    }
    std::mem::size_of::<IcebergFsFileWrite>()
        .checked_add(future_size(governed_writer_abort))
        .and_then(|n| n.checked_add(std::mem::size_of::<GovernedCleanupRefusal>()))
        .and_then(|n| {
            n.checked_add(
                std::mem::size_of::<GovernedAbortErrorRefusal>()
                    + std::mem::size_of::<crate::opendal::Error>(),
            )
        })
        .ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "governed writer backing overflow",
            )
        })
}

// This original failure also owns the writer and its pending exit responsibility.
struct GovernedAbortErrorRefusal {
    _cause: Box<dyn std::any::Any + Send>,
    _writer: crate::opendal::Writer,
    _permit: Option<WritePermit>,
    _phase: novarocks_spi::connector::ConnectorIoWeakPhase,
}
