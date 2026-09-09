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
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use novarocks_fs::{
    FileCancellation, FileError, FileErrorKind, FileIdentity, FileReadRange, FileResult,
    FsAccessHandle, FsLocation,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use paimon::io::{FileStatus, FileStatusStream, ReadOnlyFileIO};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaimonListedEntry {
    pub path: String,
    pub size: u64,
    pub is_dir: bool,
}

pub type PaimonListingStream =
    Pin<Box<dyn Stream<Item = FileResult<PaimonListedEntry>> + Send + 'static>>;

/// Minimal authorized listing capability required from novarocks-fs.
///
/// The main integration implements this over one already-resolved access
/// handle. It must paginate and must never manufacture credentials.
#[async_trait::async_trait]
pub trait PaimonAuthorizedListing: std::fmt::Debug + Send + Sync {
    async fn list(
        &self,
        access: &FsAccessHandle,
        prefix: &str,
        recursive: bool,
        cancellation: &FileCancellation,
    ) -> FileResult<PaimonListingStream>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PaimonFsAuthorizedListing;

#[async_trait::async_trait]
impl PaimonAuthorizedListing for PaimonFsAuthorizedListing {
    async fn list(
        &self,
        access: &FsAccessHandle,
        prefix: &str,
        recursive: bool,
        cancellation: &FileCancellation,
    ) -> FileResult<PaimonListingStream> {
        let stream = access
            .list_location(prefix, recursive, cancellation)
            .await?;
        Ok(Box::pin(stream.map(|entry| {
            entry.map(|entry| PaimonListedEntry {
                path: entry.location().to_string(),
                size: entry.size(),
                is_dir: entry.is_dir(),
            })
        })))
    }
}

#[derive(Clone)]
pub struct PaimonHostFileIo {
    access: FsAccessHandle,
    warehouse: FsLocation,
    cancellation: FileCancellation,
    listing: Arc<dyn PaimonAuthorizedListing>,
}

impl PaimonHostFileIo {
    pub fn try_new(
        access: FsAccessHandle,
        warehouse: impl AsRef<str>,
        cancellation: FileCancellation,
        listing: Arc<dyn PaimonAuthorizedListing>,
    ) -> FileResult<Self> {
        let warehouse = FsLocation::parse(warehouse)?;
        if warehouse.scheme() != access.scheme() || warehouse.authority() != access.authority() {
            return Err(FileError::new(
                FileErrorKind::Permission,
                "Paimon warehouse is outside the authorized filesystem domain",
            ));
        }
        Ok(Self {
            access,
            warehouse,
            cancellation,
            listing,
        })
    }

    fn validate_location(&self, path: &str) -> FileResult<FsLocation> {
        if path.as_bytes().contains(&b'\\') {
            return Err(FileError::invalid(
                "Paimon object path contains a backslash alias",
            ));
        }
        let location = FsLocation::parse(path)?;
        if location.scheme() != self.warehouse.scheme()
            || location.authority() != self.warehouse.authority()
        {
            return Err(FileError::new(
                FileErrorKind::Permission,
                "Paimon object is outside the authorized filesystem domain",
            ));
        }
        let base = self.warehouse.path().trim_matches('/');
        let candidate = location.path().trim_matches('/');
        let inside_warehouse = base.is_empty()
            || candidate == base
            || candidate
                .strip_prefix(base)
                .is_some_and(|suffix| suffix.starts_with('/'));
        if candidate.split('/').any(|part| part == "." || part == "..") || !inside_warehouse {
            return Err(FileError::new(
                FileErrorKind::Permission,
                "Paimon object escapes the authorized warehouse",
            ));
        }
        Ok(location)
    }

    async fn stat_size(&self, path: &str) -> FileResult<u64> {
        self.validate_location(path)?;
        let probe = self
            .access
            .bind_location(path, FileIdentity::new(path, 0, None))?;
        probe.stat(&self.cancellation).await
    }
}

impl std::fmt::Debug for PaimonHostFileIo {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PaimonHostFileIo")
            .field("access", &self.access)
            .field("warehouse", &self.warehouse.original())
            .field("cancellation", &self.cancellation)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl ReadOnlyFileIO for PaimonHostFileIo {
    async fn stat(&self, path: &str) -> paimon::Result<FileStatus> {
        let size = self.stat_size(path).await.map_err(map_file_error)?;
        Ok(FileStatus {
            size,
            is_dir: false,
            path: path.to_string(),
            last_modified: None,
        })
    }

    async fn exists(&self, path: &str) -> paimon::Result<bool> {
        match self.stat_size(path).await {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == FileErrorKind::NotFound => Ok(false),
            Err(error) => Err(map_file_error(error)),
        }
    }

    async fn read(&self, path: &str, range: Range<u64>) -> paimon::Result<Bytes> {
        let size = self.stat_size(path).await.map_err(map_file_error)?;
        if range.start > range.end || range.end > size {
            return Err(paimon::Error::ConfigInvalid {
                message: "Paimon host read range is outside the frozen object".to_string(),
            });
        }
        let file = self
            .access
            .bind_location(path, FileIdentity::new(path, size, None))
            .map_err(map_file_error)?;
        file.read(
            FileReadRange::Bounded {
                offset: range.start,
                length: range.end - range.start,
            },
            &self.cancellation,
        )
        .await
        .map_err(map_file_error)
    }

    async fn list(&self, path: &str, recursive: bool) -> paimon::Result<FileStatusStream> {
        self.validate_location(path).map_err(map_file_error)?;
        self.cancellation.check().map_err(map_file_error)?;
        let stream = self
            .listing
            .list(&self.access, path, recursive, &self.cancellation)
            .await
            .map_err(map_file_error)?;
        Ok(Box::pin(stream.map(|entry| {
            entry
                .map(|entry| FileStatus {
                    size: entry.size,
                    is_dir: entry.is_dir,
                    path: entry.path,
                    last_modified: None,
                })
                .map_err(map_file_error)
        })))
    }
}

fn map_file_error(error: FileError) -> paimon::Error {
    match error.kind() {
        FileErrorKind::Unsupported => paimon::Error::IoUnsupported {
            message: error.to_string(),
        },
        FileErrorKind::Invalid => paimon::Error::ConfigInvalid {
            message: error.to_string(),
        },
        _ => paimon::Error::UnexpectedError {
            message: "host-authorized Paimon file operation failed".to_string(),
            source: Some(Box::new(error)),
        },
    }
}

pub(crate) fn connector_error_from_file_error(error: &FileError) -> ConnectorError {
    let kind = match error.kind() {
        FileErrorKind::Invalid | FileErrorKind::AlreadyExists => ConnectorErrorKind::InvalidRequest,
        FileErrorKind::Unsupported => ConnectorErrorKind::Unsupported,
        FileErrorKind::NotFound => ConnectorErrorKind::NotFound,
        FileErrorKind::Permission => ConnectorErrorKind::PermissionDenied,
        FileErrorKind::Corrupt => ConnectorErrorKind::CorruptData,
        FileErrorKind::ResourceExhausted => ConnectorErrorKind::ResourceExhausted,
        FileErrorKind::Transient => ConnectorErrorKind::Unavailable,
        FileErrorKind::DeadlineExceeded => ConnectorErrorKind::DeadlineExceeded,
        FileErrorKind::Cancelled => ConnectorErrorKind::Cancelled,
        FileErrorKind::Internal => ConnectorErrorKind::Internal,
    };
    ConnectorError::new(kind, error.to_string())
}

#[cfg(test)]
mod tests {
    use novarocks_fs::FileError;
    use novarocks_spi::connector::ConnectorErrorKind;

    use super::{connector_error_from_file_error, map_file_error};

    #[test]
    fn preserves_host_cancellation_and_deadline_classification() {
        assert_eq!(
            connector_error_from_file_error(&FileError::cancelled("cancelled")).kind(),
            ConnectorErrorKind::Cancelled
        );
        assert_eq!(
            connector_error_from_file_error(&FileError::deadline("deadline")).kind(),
            ConnectorErrorKind::DeadlineExceeded
        );
    }

    #[test]
    fn preserves_host_permission_through_sdk_error_mapping() {
        let metadata = crate::catalog::map_sdk_error(map_file_error(FileError::new(
            novarocks_fs::FileErrorKind::Permission,
            "credential scope rejected",
        )));
        let reader = crate::reader::map_paimon_error(map_file_error(FileError::new(
            novarocks_fs::FileErrorKind::Permission,
            "credential scope rejected",
        )));

        assert_eq!(metadata.kind(), ConnectorErrorKind::PermissionDenied);
        assert_eq!(reader.kind(), ConnectorErrorKind::PermissionDenied);
    }
}
