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
//! Core-private raw file execution helpers.
//!
//! These types are deliberately not part of the connector SPI or
//! `novarocks-fs`. They describe only protocol-owned raw file access; table
//! format correctness belongs to the connector that owns its opaque split.

use crate::cache::ExternalDataCacheRangeOptions;
use crate::novarocks_logging::debug;
use bytes::Bytes;
use novarocks_execution::runtime::profile::RuntimeProfile;
use novarocks_fs::{
    BoundFile, FileBytesFuture, FileCancellation, FileError, FileErrorKind, FileFormat,
    FileIdentity, FileIoRuntime, FileProjection, FileReadBudget, FileReadContext, FileReadRange,
    FileReadRequest, FileResult, FileTask, FileTaskFuture, FileTaskSpawner, FileU64Future,
    FsAccessHandle, FsAccessResolver, FsScheme, PhysicalPruning,
};
use std::num::NonZeroUsize;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct FileScanRange {
    pub path: String,
    pub file_len: u64,
    pub offset: u64,
    pub length: u64,
    pub scan_range_id: i32,
    pub external_datacache: Option<ExternalDataCacheRangeOptions>,
}

#[derive(Clone)]
pub struct FileScanContext {
    pub ranges: Vec<FileScanRange>,
    pub access: FsAccessHandle,
    pub scheme: FsScheme,
    pub root: Option<String>,
}

impl FileScanContext {
    /// Build a scan context for the given ranges.
    ///
    /// `oss_config` must be `Some` when the paths use the `oss://` / `s3://` scheme; it is
    /// unused for local and HDFS paths.  Callers are responsible for resolving the config from
    /// whatever source is appropriate (e.g. `THdfsScanNode.cloud_configuration` for Iceberg
    /// external tables, or the shard registry for native lake tablets).
    pub fn build(
        ranges: Vec<FileScanRange>,
        _profile: Option<RuntimeProfile>,
        oss_config: Option<&novarocks_fs::ObjectStoreConfig>,
    ) -> Result<Self, String> {
        let paths = ranges.iter().map(|r| r.path.as_str()).collect::<Vec<_>>();
        let handle = FsAccessResolver::new()
            .resolve_locations(paths, oss_config)
            .map_err(|error| error.to_string())?;

        match handle.scheme() {
            FsScheme::Local => {
                let root = handle.root().unwrap_or(".");
                debug!("file scan (local): {} ranges root={}", ranges.len(), root);
            }
            FsScheme::ObjectStore => {
                debug!("file scan (object-store): {} ranges", ranges.len());
            }
            FsScheme::Hdfs => {
                let root = handle.root().unwrap_or("<unknown>");
                debug!(
                    "file scan (hdfs): {} ranges namenode={}",
                    ranges.len(),
                    root
                );
            }
        }

        Ok(Self {
            ranges,
            access: handle.clone(),
            scheme: handle.scheme(),
            root: handle.root().map(str::to_string),
        })
    }
}

#[derive(Clone, Default)]
pub(crate) struct CoreFileIoRuntime;

impl FileIoRuntime for CoreFileIoRuntime {
    fn block_on_bytes(&self, future: FileBytesFuture) -> FileResult<Bytes> {
        crate::runtime::global_async_runtime::data_block_on(future)
            .map_err(|error| FileError::new(FileErrorKind::Internal, error))?
    }

    fn block_on_u64(&self, future: FileU64Future) -> FileResult<u64> {
        crate::runtime::global_async_runtime::data_block_on(future)
            .map_err(|error| FileError::new(FileErrorKind::Internal, error))?
    }
}

#[derive(Clone)]
pub(crate) struct CoreFileTaskSpawner {
    handle: tokio::runtime::Handle,
}

impl CoreFileTaskSpawner {
    pub(crate) fn try_new() -> Result<Self, String> {
        Ok(Self {
            handle: crate::runtime::global_async_runtime::data_runtime_handle()?,
        })
    }
}

impl FileTaskSpawner for CoreFileTaskSpawner {
    fn spawn(&self, task: FileTaskFuture) -> FileResult<FileTask> {
        Ok(FileTask::new(self.handle.spawn(task)))
    }
}

pub(crate) fn foundation_read_context(
    cancellation: FileCancellation,
    deadline: Option<std::time::Instant>,
) -> Result<FileReadContext, String> {
    Ok(FileReadContext {
        cancellation,
        deadline,
        runtime: Arc::new(CoreFileIoRuntime),
        task_spawner: Arc::new(CoreFileTaskSpawner::try_new()?),
    })
}

pub(crate) fn bind_foundation_file(
    range: &FileScanRange,
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
    context: &FileReadContext,
) -> FileResult<(BoundFile, FileReadRange)> {
    context.check_active()?;
    let access =
        novarocks_fs::FsAccessResolver::new().resolve_location(&range.path, object_store_config)?;
    let provisional = access.bind(
        0,
        FileIdentity::new(
            &range.path,
            range.file_len,
            range
                .external_datacache
                .as_ref()
                .and_then(|options| options.modification_time),
        ),
    )?;
    let file_size = if range.file_len > 0 {
        range.file_len
    } else {
        let file = provisional.clone();
        let cancellation = context.cancellation.clone();
        context
            .runtime
            .block_on_u64(Box::pin(async move { file.stat(&cancellation).await }))?
    };
    if range.offset > file_size {
        return Err(FileError::invalid(format!(
            "file split offset {} exceeds file length {file_size}",
            range.offset
        )));
    }
    let bounded_length = if range.length == 0 {
        file_size.saturating_sub(range.offset)
    } else {
        range.length.min(file_size.saturating_sub(range.offset))
    };
    let read_range = if range.offset == 0 && bounded_length == file_size {
        FileReadRange::WholeFile
    } else {
        FileReadRange::bounded(range.offset, bounded_length)?
    };
    let file = access.bind(
        0,
        FileIdentity::new(
            &range.path,
            file_size,
            range
                .external_datacache
                .as_ref()
                .and_then(|options| options.modification_time),
        ),
    )?;
    Ok((file, read_range))
}

pub(crate) fn read_foundation_parquet_batches(
    access: &FsAccessHandle,
    path: &str,
    file_size: Option<u64>,
    projection: FileProjection,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    read_foundation_parquet_file_batches(access, path, file_size, projection)
        .map(|batches| batches.into_iter().map(|batch| batch.batch).collect())
}

pub(crate) fn read_foundation_parquet_file_batches(
    access: &FsAccessHandle,
    path: &str,
    file_size: Option<u64>,
    projection: FileProjection,
) -> Result<Vec<novarocks_fs::FileBatch>, String> {
    let context = foundation_read_context(FileCancellation::new(), None)?;
    let provisional_size = file_size.unwrap_or(0);
    let provisional = access
        .bind_location(path, FileIdentity::new(path, provisional_size, None))
        .map_err(|error| error.to_string())?;
    let file_size = match file_size {
        Some(size) if size > 0 => size,
        _ => {
            let file = provisional.clone();
            let cancellation = context.cancellation.clone();
            context
                .runtime
                .block_on_u64(Box::pin(async move { file.stat(&cancellation).await }))
                .map_err(|error| error.to_string())?
        }
    };
    let file = access
        .bind_location(path, FileIdentity::new(path, file_size, None))
        .map_err(|error| error.to_string())?;
    let mut reader = novarocks_fs::open_file_reader(FileReadRequest {
        file,
        format: FileFormat::Parquet,
        range: FileReadRange::WholeFile,
        projection,
        budget: FileReadBudget {
            max_rows: NonZeroUsize::new(4096).expect("foundation batch size is nonzero"),
            max_bytes: NonZeroUsize::new(64 * 1024 * 1024)
                .expect("foundation byte budget is nonzero"),
        },
        predicates: Vec::new(),
        pruning: PhysicalPruning::default(),
        cache: None,
        context,
    })
    .map_err(|error| error.to_string())?;
    let mut batches = Vec::new();
    while let Some(batch) = reader.next_batch().map_err(|error| error.to_string())? {
        batches.push(batch);
    }
    reader.close().map_err(|error| error.to_string())?;
    Ok(batches)
}

pub(crate) fn read_foundation_bytes(
    access: &FsAccessHandle,
    path: &str,
    file_size: Option<u64>,
    range: FileReadRange,
) -> Result<Bytes, String> {
    let context = foundation_read_context(FileCancellation::new(), None)?;
    let file = access
        .bind_location(
            path,
            FileIdentity::new(path, file_size.unwrap_or_default(), None),
        )
        .map_err(|error| error.to_string())?;
    let cancellation = context.cancellation.clone();
    context
        .runtime
        .block_on_bytes(Box::pin(
            async move { file.read(range, &cancellation).await },
        ))
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_fs::FsScheme;

    fn range(path: &str) -> FileScanRange {
        FileScanRange {
            path: path.to_string(),
            file_len: 1,
            offset: 0,
            length: 1,
            scan_range_id: 0,
            external_datacache: None,
        }
    }

    #[test]
    fn build_local_scan_context_uses_resolver_relative_paths() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join("a.parquet");
        std::fs::write(&file, b"data").expect("write fixture");

        let ctx = FileScanContext::build(vec![range(file.to_string_lossy().as_ref())], None, None)
            .expect("build scan context");

        assert_eq!(ctx.ranges.len(), 1);
        assert_eq!(ctx.ranges[0].path, file.to_string_lossy());
        assert_eq!(ctx.scheme, FsScheme::Local);
    }

    #[test]
    fn build_object_store_context_requires_credentials_only_config() {
        let err = match FileScanContext::build(
            vec![range("s3://bucket-a/warehouse/t/a.parquet")],
            None,
            None,
        ) {
            Ok(_) => panic!("object-store scan requires credentials"),
            Err(err) => err,
        };

        assert!(
            err.contains("object-store location requires object store config"),
            "{err}"
        );
    }
}
