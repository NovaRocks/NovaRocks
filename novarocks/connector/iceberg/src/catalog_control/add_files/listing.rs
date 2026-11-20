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

//! Discover one external source under the original listing and byte authors.

use std::alloc::Layout;
use std::path::Path;
use std::sync::Arc;

use futures::StreamExt;
use novarocks_failure_contract::{FailureClass, connector_reason};
use novarocks_fs::{FsAccessHandle, FsLocation, FsScheme};
use novarocks_spi::connector::{
    ConnectorAddFilesRecursion, ConnectorError, ConnectorErrorKind, ConnectorListingPageSize,
    ConnectorOperationControl, ConnectorPayloadRetentionGuard, ConnectorRequestContext,
};
use novarocks_type_contract::owned_resources::layout::arc_layout;
use opendal::{ErrorKind as StorageErrorKind, Metadata, Operator};

use crate::access_binding::IcebergReadBinding;
use crate::catalog::listing_admission::ListingAdmission;
use crate::commit::model::ObjectIdentity;
use crate::typed_read::split::validate_data_file_path;

use super::{procedure_error, validate_registration_location};

pub(crate) struct ListedCandidates {
    access: FsAccessHandle,
    files: CandidateFiles,
}

impl ListedCandidates {
    pub(crate) fn into_parts(self) -> (FsAccessHandle, CandidateFiles) {
        (self.access, self.files)
    }
}

/// The complete ordered list has one capacity receipt. A consuming iterator
/// owns that same original allocation until it exits.
#[derive(Default)]
pub(crate) struct CandidateFiles {
    files: Vec<CandidateFile>,
    skipped_hidden: usize,
    retention: Option<ConnectorPayloadRetentionGuard>,
}

impl CandidateFiles {
    pub(crate) fn len(&self) -> usize {
        self.files.len()
    }

    pub(crate) fn skipped_hidden_count(&self) -> usize {
        self.skipped_hidden
    }

    pub(crate) fn iter(&self) -> std::slice::Iter<'_, CandidateFile> {
        self.files.iter()
    }

    fn push(
        &mut self,
        file: CandidateFile,
        byte_author: &ConnectorPayloadRetentionGuard,
    ) -> Result<(), ConnectorError> {
        if self.files.len() == self.files.capacity() {
            let capacity = self
                .files
                .capacity()
                .checked_mul(2)
                .ok_or_else(|| capacity_error("candidate vector count overflow"))?
                .max(1);
            let layout = Layout::array::<CandidateFile>(capacity).map_err(capacity_error)?;
            let next = byte_author.reserve_owned_bytes(layout.size())?;
            self.files
                .try_reserve_exact(capacity - self.files.len())
                .map_err(capacity_error)?;
            // The previous allocation is accounted for until the real growth exits.
            self.retention = Some(next);
        }
        self.files.push(file);
        Ok(())
    }
}

pub(crate) struct CandidateFileIter {
    inner: std::vec::IntoIter<CandidateFile>,
    _retention: Option<ConnectorPayloadRetentionGuard>,
}

impl Iterator for CandidateFileIter {
    type Item = CandidateFile;

    fn next(&mut self) -> Option<CandidateFile> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl ExactSizeIterator for CandidateFileIter {}

impl IntoIterator for CandidateFiles {
    type Item = CandidateFile;
    type IntoIter = CandidateFileIter;

    fn into_iter(self) -> Self::IntoIter {
        CandidateFileIter {
            inner: self.files.into_iter(),
            _retention: self.retention,
        }
    }
}

#[derive(Clone, Copy)]
enum SourceEntry {
    File,
    ForbiddenNestedFile,
    UnrepresentableKey,
    NonRegularFile,
}

pub(crate) struct CandidateFile {
    identity: ObjectIdentity,
    size: u64,
    entry: SourceEntry,
    path_retention: ConnectorPayloadRetentionGuard,
}

impl CandidateFile {
    fn new(
        location: &str,
        size: u64,
        entry: SourceEntry,
        byte_author: &ConnectorPayloadRetentionGuard,
    ) -> Result<Self, ConnectorError> {
        let path = Layout::array::<u8>(location.len()).map_err(capacity_error)?;
        let path = arc_layout(path).map_err(capacity_error)?;
        let path_retention = byte_author.reserve_owned_bytes(path.size())?;
        let identity = ObjectIdentity::from_shared_path(Arc::from(location)).map_err(|error| {
            ConnectorError::new(ConnectorErrorKind::Internal, error.to_string())
        })?;
        Ok(Self {
            identity,
            size,
            entry,
            path_retention,
        })
    }

    pub(crate) fn path(&self) -> &str {
        self.identity.path()
    }

    pub(crate) fn identity(&self) -> &ObjectIdentity {
        &self.identity
    }

    pub(crate) fn size(&self) -> u64 {
        self.size
    }

    /// Call in sorted order immediately before opening this candidate. A
    /// later nested path must not preempt an earlier bad footer.
    pub(crate) fn validate(&self) -> Result<(), ConnectorError> {
        let path = self.path();
        validate_registration_location(path)?;
        match self.entry {
            SourceEntry::File => {}
            SourceEntry::ForbiddenNestedFile => {
                return Err(unsupported(format!(
                    "Recursive directory must not exist when recursive_directory argument is 'fail': {path}"
                )));
            }
            SourceEntry::UnrepresentableKey => {
                return Err(unsupported(
                    "ADD FILES cannot preserve this object's exact path in the Iceberg reader",
                ));
            }
            SourceEntry::NonRegularFile => {
                return Err(unsupported("ADD FILES source entry is not a regular file"));
            }
        }
        validate_data_file_path(path).map_err(|error| unsupported(error.message()))
    }

    pub(crate) fn into_parts(self) -> (ObjectIdentity, u64, ConnectorPayloadRetentionGuard) {
        (self.identity, self.size, self.path_retention)
    }
}

pub(crate) async fn resolve_and_list(
    location: &str,
    mode: ConnectorAddFilesRecursion,
    binding: &IcebergReadBinding,
    admission: &ListingAdmission,
    byte_author: &ConnectorPayloadRetentionGuard,
) -> Result<ListedCandidates, ConnectorError> {
    validate_registration_location(location)?;
    let context = binding.request_context().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "ADD FILES listing requires its original request context",
        )
    })?;
    let (access, mut files) = admission
        .run_wait_for_exit(context, async {
            let parsed = FsLocation::parse(location).map_err(ConnectorError::from)?;
            let local = local_uri(&parsed, byte_author)?;
            let source = local.as_ref().map_or_else(
                || parsed.original().trim_end_matches('/'),
                |path| path.text.as_str(),
            );
            if parsed.scheme() != FsScheme::Local {
                let key = parsed.path().trim_end_matches('/');
                let key = key.strip_prefix('/').unwrap_or(key);
                if location.trim_end() != location || !preserves_key(key) {
                    return Err(unsupported(
                        "ADD FILES cannot preserve the exact source location",
                    ));
                }
            }
            let _io = binding.admit_non_http_io(source).await?;
            let access = binding.resolve_access_for_request_async(source).await?;
            let relative = access
                .paths()
                .first()
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::Internal,
                        "ADD FILES source access has no path",
                    )
                })?
                .operator_relative_path()
                .trim_end_matches('/');
            let operator = access.operator();
            let files = if access.scheme().is_object_store() {
                // A successful first item proves a directory even when it is
                // hidden or a marker. The same cursor produces all later items.
                let (present, files) =
                    list_directory(&operator, source, relative, mode, context, byte_author).await?;
                if present {
                    files
                } else {
                    let metadata = stat(&operator, relative, context).await?;
                    single_file(metadata, location, source, byte_author)?
                }
            } else {
                let metadata = stat(&operator, relative, context).await?;
                if metadata.as_ref().is_some_and(Metadata::is_dir) {
                    list_directory(&operator, source, relative, mode, context, byte_author)
                        .await?
                        .1
                } else {
                    single_file(metadata, location, source, byte_author)?
                }
            };
            Ok((access, files))
        })
        .await?;

    files
        .files
        .sort_unstable_by(|left, right| left.identity.cmp(&right.identity));
    Ok(ListedCandidates { access, files })
}

async fn stat(
    operator: &Operator,
    relative: &str,
    context: &ConnectorRequestContext,
) -> Result<Option<Metadata>, ConnectorError> {
    context.check_active()?;
    let result = operator.stat(relative).await;
    context.check_active()?;
    match result {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == StorageErrorKind::NotFound => Ok(None),
        Err(error) => Err(storage_error(error)),
    }
}

fn single_file(
    metadata: Option<Metadata>,
    original: &str,
    source: &str,
    byte_author: &ConnectorPayloadRetentionGuard,
) -> Result<CandidateFiles, ConnectorError> {
    let metadata = metadata.filter(Metadata::is_file).ok_or_else(|| {
        procedure_error(
            ConnectorErrorKind::NotFound,
            FailureClass::Dependency,
            connector_reason::LOCATION_NOT_FOUND,
            format!("Location not found: {original}"),
        )
    })?;
    let candidate = CandidateFile::new(
        source,
        metadata.content_length(),
        SourceEntry::File,
        byte_author,
    )?;
    let mut files = CandidateFiles::default();
    files.push(candidate, byte_author)?;
    Ok(files)
}

async fn list_directory(
    operator: &Operator,
    source: &str,
    relative: &str,
    mode: ConnectorAddFilesRecursion,
    context: &ConnectorRequestContext,
    byte_author: &ConnectorPayloadRetentionGuard,
) -> Result<(bool, CandidateFiles), ConnectorError> {
    let prefix = PaidText::new(&[relative, "/"], byte_author)?;
    context.check_active()?;
    let mut lister = operator
        .lister_with(&prefix.text)
        .recursive(true)
        .limit(ConnectorListingPageSize::DEFAULT.get())
        .await
        .map_err(storage_error)?;
    let mut files = CandidateFiles::default();
    let mut present = false;
    loop {
        context.check_active()?;
        let entry = lister.next().await;
        context.check_active()?;
        let Some(entry) = entry else {
            break;
        };
        let entry = entry.map_err(storage_error)?;
        present = true;
        let metadata = entry.metadata();
        let path = entry.path();
        if metadata.is_dir() || path.ends_with('/') {
            continue;
        }
        let suffix = path.strip_prefix(&prefix.text).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "ADD FILES listing returned a file outside the requested prefix",
            )
        })?;
        if suffix.is_empty() {
            continue;
        }
        if suffix.split('/').any(|part| part.starts_with(['_', '.'])) {
            files.skipped_hidden = files.skipped_hidden.saturating_add(1);
            continue;
        }
        let nested = suffix.contains('/');
        if nested && mode == ConnectorAddFilesRecursion::Skip {
            continue;
        }
        let shape = if nested && mode == ConnectorAddFilesRecursion::Fail {
            SourceEntry::ForbiddenNestedFile
        } else if !preserves_key(path) {
            SourceEntry::UnrepresentableKey
        } else if !metadata.is_file() {
            SourceEntry::NonRegularFile
        } else {
            SourceEntry::File
        };
        let location = PaidText::new(&[source, "/", suffix], byte_author)?;
        let file = CandidateFile::new(
            &location.text,
            metadata.content_length(),
            shape,
            byte_author,
        )?;
        drop(location);
        files.push(file, byte_author)?;
    }
    Ok((present, files))
}

/// OpenDAL normalizes repeated separators and edge whitespace on reads. A
/// listed key that would then address another object has no exact scan path.
fn preserves_key(key: &str) -> bool {
    !key.starts_with('/') && !key.contains("//") && key.trim() == key
}

fn local_uri(
    location: &FsLocation,
    byte_author: &ConnectorPayloadRetentionGuard,
) -> Result<Option<PaidText>, ConnectorError> {
    if location.scheme() != FsScheme::Local {
        return Ok(None);
    }
    let absolute = std::path::absolute(Path::new(location.path())).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            format!("Resolve local ADD FILES path: {error}"),
        )
    })?;
    let path = absolute
        .to_str()
        .ok_or_else(|| unsupported("ADD FILES local path is not UTF-8"))?;
    let path = path.trim_end_matches('/');
    PaidText::new(&["file://", path], byte_author).map(Some)
}

struct PaidText {
    text: String,
    _retention: ConnectorPayloadRetentionGuard,
}

impl PaidText {
    fn new(
        parts: &[&str],
        byte_author: &ConnectorPayloadRetentionGuard,
    ) -> Result<Self, ConnectorError> {
        let capacity = parts.iter().try_fold(0usize, |bytes, part| {
            bytes
                .checked_add(part.len())
                .ok_or_else(|| capacity_error("path length overflow"))
        })?;
        let retention = byte_author.reserve_owned_bytes(capacity)?;
        let mut text = String::new();
        text.try_reserve_exact(capacity).map_err(capacity_error)?;
        for part in parts {
            text.push_str(part);
        }
        Ok(Self {
            text,
            _retention: retention,
        })
    }
}

fn storage_error(error: opendal::Error) -> ConnectorError {
    ConnectorError::from(novarocks_fs::map_object_store_listing_error(error))
}

fn unsupported(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message).with_public_failure_detail()
}

fn capacity_error(error: impl std::fmt::Display) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        format!("ADD FILES candidate backing: {error}"),
    )
}
