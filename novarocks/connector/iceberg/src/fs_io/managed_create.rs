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

use bytes::Bytes;
use futures::StreamExt;
use novarocks_fs::{FileCancellation, FsAccessHandle, FsScheme};
use novarocks_spi::connector::write_stack::ConnectorWriterPhysicalContext;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorIoCleanupFuture, ConnectorIoPermit,
    ConnectorIoStreamCustody, ConnectorOperationControl, ConnectorRequestContext,
};
use sha2::{Digest, Sha256};

use crate::access_binding::IcebergReadBinding;
use crate::commit::data_writer::{CreateClaim, ObjectContent, ObjectOwner, StagedPathJournal};
use crate::commit::write_stack::domain::{IcebergCreationReceipt, IcebergCreationScope};
use crate::delete_file::IcebergFileFormat;
use crate::iceberg::io::{FileMetadata, FileWrite};
use crate::iceberg::{Error, ErrorKind, Result};

const CREATE_ID: &str = "novarocks-create-id";
const CREATE_IDENTITY: &str = "novarocks-create-identity";

#[derive(Clone, Debug)]
pub(super) struct ManagedWriteObjects {
    pub(super) scope: IcebergCreationScope,
    pub(super) physical: ConnectorWriterPhysicalContext,
    pub(super) format: IcebergFileFormat,
    pub(super) journal: StagedPathJournal,
}

pub(super) fn supports_managed_create(access: &FsAccessHandle) -> bool {
    match access.scheme() {
        FsScheme::Local => cfg!(unix),
        FsScheme::ObjectStore => {
            let op = access.operator();
            let cap = op.info().capability();
            cap.write_with_if_not_exists
                && cap.write_with_user_metadata
                && cap.stat
                && cap.read
                && (cap.read_with_version || cap.read_with_if_match)
        }
        FsScheme::Hdfs => false,
    }
}

impl ManagedWriteObjects {
    pub(super) async fn open(
        &self,
        binding: &IcebergReadBinding,
        path: &str,
    ) -> Result<Box<dyn FileWrite>> {
        let original = binding.request_context().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "managed Iceberg writer has no request context",
            )
        })?;
        original.check_active().map_err(iceberg_error)?;
        let phase = binding
            .admit_owned_stream(path)
            .await
            .map_err(iceberg_error)?;
        let access = binding
            .resolve_access_for_request_async(path)
            .await
            .map_err(iceberg_error)?;
        if !supports_managed_create(&access) {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "storage lacks conditional streaming creation and exact observation",
            ));
        }
        let access = match &phase {
            Some(phase) => {
                access.with_request_context(original.clone().with_io_weak_phase(phase.weak_phase()))
            }
            None => access,
        };
        let identity = self
            .scope
            .identity(self.physical, path, self.format)
            .map_err(iceberg_error)?;
        let claim = self.journal.begin(path, identity).map_err(journal_error)?;
        let cancellation = FileCancellation::from_connector_request(original);
        let stream = phase
            .as_ref()
            .map(ConnectorIoPermit::begin_stream)
            .transpose()
            .map_err(iceberg_error)?;
        let opened = WriteStream::open(&access, claim, &cancellation).await;
        let writer = match opened {
            Ok(writer) => writer,
            Err(error) => {
                if let Some(stream) = stream {
                    stream.closed();
                }
                return Err(error);
            }
        };
        Ok(Box::new(ManagedFileWrite {
            writer: Some(writer),
            binding: binding.clone(),
            original: original.clone(),
            cancellation,
            journal: self.journal.clone(),
            path: path.to_owned(),
            physical: self.physical,
            claim,
            phase,
            stream,
            size: 0,
            digest: Sha256::new(),
        }))
    }
}

enum WriteStream {
    Object {
        writer: crate::opendal::Writer,
        uploads: novarocks_fs::S3UploadWitness,
    },
    #[cfg(unix)]
    Local(novarocks_fs::ConditionalLocalFile),
}

impl WriteStream {
    async fn open(
        access: &FsAccessHandle,
        claim: CreateClaim,
        cancellation: &FileCancellation,
    ) -> Result<Self> {
        #[cfg(unix)]
        if access.scheme() == FsScheme::Local {
            return access
                .open_conditional_local_file(0, cancellation)
                .await
                .map(Self::Local)
                .map_err(|error| storage_error("open conditional local file", error));
        }
        cancellation
            .check()
            .map_err(|error| storage_error("open conditional object", error))?;
        let path = relative_path(access)?;
        let (operator, uploads) = access
            .conditional_object_writer_operator()
            .map_err(|error| storage_error("bind conditional object upload", error))?;
        let writer = operator
            .writer_with(path)
            .if_not_exists(true)
            .user_metadata(claim_metadata(claim))
            .await
            .map_err(|error| storage_error("open conditional object", error))?;
        Ok(Self::Object { writer, uploads })
    }

    async fn write(&mut self, bytes: Bytes, cancellation: &FileCancellation) -> Result<()> {
        match self {
            Self::Object { writer, .. } => writer
                .write(bytes)
                .await
                .map_err(|error| storage_error("write conditional object", error)),
            #[cfg(unix)]
            Self::Local(writer) => writer
                .write(&bytes, cancellation)
                .await
                .map_err(|error| storage_error("write conditional local file", error)),
        }
    }

    async fn abort(self, cleanup: ConnectorRequestContext) -> Result<()> {
        match self {
            Self::Object {
                mut writer,
                uploads,
            } => {
                uploads.install_abort_context(cleanup);
                let outcome = writer.abort().await;
                match uploads.check_exit() {
                    Ok(()) => Ok(()),
                    Err(unproven) => Err(storage_error(
                        "abort conditional object upload",
                        outcome.err().unwrap_or(unproven),
                    )),
                }
            }
            #[cfg(unix)]
            Self::Local(writer) => writer
                .abort()
                .await
                .map_err(|error| storage_error("remove private local output", error)),
        }
    }
}

struct ManagedFileWrite {
    writer: Option<WriteStream>,
    binding: IcebergReadBinding,
    original: ConnectorRequestContext,
    cancellation: FileCancellation,
    journal: StagedPathJournal,
    path: String,
    physical: ConnectorWriterPhysicalContext,
    claim: CreateClaim,
    phase: Option<ConnectorIoPermit>,
    stream: Option<ConnectorIoStreamCustody>,
    size: u64,
    digest: Sha256,
}

impl std::fmt::Debug for ManagedFileWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagedFileWrite")
            .field("path", &self.path)
            .field("size", &self.size)
            .finish_non_exhaustive()
    }
}

impl ManagedFileWrite {
    fn complete_stream(&mut self) {
        if let Some(stream) = self.stream.take() {
            stream.closed();
        }
        self.phase.take();
    }

    fn record(&self, owner: ObjectOwner, content: ObjectContent) -> Result<()> {
        self.journal
            .record_result(&self.path, self.claim, owner, content)
            .map_err(journal_error)
    }

    fn record_stream_exit_failure(&self, cause: Error) -> Error {
        let original = self.journal.observe_stream_exit(&self.path).failed(cause);
        storage_error("conditional output stream exit remains unproven", original)
    }

    fn check_stream_exit(&self) -> Result<()> {
        match self.journal.stream_exit_failure(&self.path) {
            Some(original) => Err(storage_error(
                "conditional output stream exit remains unproven",
                original,
            )),
            None => Ok(()),
        }
    }

    fn confirm_content(&self, receipt: IcebergCreationReceipt, reported: u64) -> Result<()> {
        let complete = reported == 0 || reported == receipt.size();
        self.record(
            ObjectOwner::Ours,
            if complete {
                ObjectContent::VerifiedComplete(receipt)
            } else {
                ObjectContent::Invalid
            },
        )?;
        if !complete {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "conditional object reports a different size",
            ));
        }
        Ok(())
    }

    #[cfg(unix)]
    async fn recover_local(
        &self,
        writer: &novarocks_fs::ConditionalLocalFile,
        receipt: IcebergCreationReceipt,
        close: novarocks_fs::FileResult<novarocks_fs::ConditionalCreateOutcome>,
    ) -> Result<()> {
        let primary = match close {
            Ok(_) => Error::new(ErrorKind::DataInvalid, "conditional local target exists"),
            Err(error) => storage_error("close conditional local file", error),
        };
        let cleanup = self.journal.cleanup_request(&self.original);
        let observed = async {
            cleanup.check_active().map_err(iceberg_error)?;
            writer
                .observe_target()
                .await
                .map_err(|error| storage_error("observe local installation", error))
        }
        .await;
        match observed {
            Ok(Some(true)) => self.record(ObjectOwner::Ours, ObjectContent::Open)?,
            Ok(Some(false)) => {
                self.record(ObjectOwner::Foreign, ObjectContent::Unproven)?;
                return Err(primary);
            }
            Ok(None) | Err(_) => {
                self.record(ObjectOwner::Unproven, ObjectContent::Unproven)?;
                return Err(
                    primary.with_context("creation_observation", "local installation is unproven")
                );
            }
        }
        let content = match verify_local_file(writer, &cleanup, receipt).await {
            Ok(true) => ObjectContent::VerifiedComplete(receipt),
            Ok(false) => ObjectContent::Invalid,
            Err(error) => {
                self.record(ObjectOwner::Ours, ObjectContent::Unproven)?;
                return Err(primary.with_context("creation_content", error.to_string()));
            }
        };
        self.record(ObjectOwner::Ours, content)?;
        if content != ObjectContent::VerifiedComplete(receipt) {
            return Err(primary.with_context(
                "creation_content",
                "installed file differs from the closed writer",
            ));
        }
        Ok(())
    }

    async fn recover_object(&self, receipt: IcebergCreationReceipt, primary: Error) -> Result<()> {
        let cleanup = self.journal.cleanup_request(&self.original);
        let binding = self.binding.for_request(cleanup.clone());
        let access = binding.resolve_access_for_request_async(&self.path).await;
        let (op, path, stat) = match observe_marker(access, &cleanup).await {
            Ok(value) => value,
            Err(error) => {
                self.record(ObjectOwner::Unproven, ObjectContent::Unproven)?;
                return Err(primary.with_context("creation_observation", error.to_string()));
            }
        };
        if !matches_marker(&stat, self.claim) {
            self.record(ObjectOwner::Foreign, ObjectContent::Unproven)?;
            return Err(primary.with_context("creation_observation", "different creation marker"));
        }
        self.record(ObjectOwner::Ours, ObjectContent::Open)?;
        let content = match verify_object(&op, &path, &stat, &cleanup, receipt).await {
            Ok(true) => ObjectContent::VerifiedComplete(receipt),
            Ok(false) => ObjectContent::Invalid,
            Err(error) => {
                self.record(ObjectOwner::Ours, ObjectContent::Unproven)?;
                return Err(primary.with_context("creation_content", error.to_string()));
            }
        };
        self.record(ObjectOwner::Ours, content)?;
        if content != ObjectContent::VerifiedComplete(receipt) {
            return Err(primary.with_context(
                "creation_content",
                "stored bytes differ from the closed writer",
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl FileWrite for ManagedFileWrite {
    async fn write(&mut self, bytes: Bytes) -> Result<()> {
        self.check_stream_exit()?;
        self.original.check_active().map_err(iceberg_error)?;
        let size = self.size.checked_add(bytes.len() as u64).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "conditional output size overflows u64",
            )
        })?;
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "conditional output is closed"))?;
        writer.write(bytes.clone(), &self.cancellation).await?;
        self.digest.update(bytes);
        self.size = size;
        self.original.check_active().map_err(iceberg_error)
    }

    async fn close(&mut self) -> Result<FileMetadata> {
        self.check_stream_exit()?;
        self.original.check_active().map_err(iceberg_error)?;
        let receipt = IcebergCreationReceipt::try_new(
            self.physical,
            self.claim.id,
            self.claim.identity,
            self.size,
            self.digest.clone().finalize().into(),
        )
        .map_err(iceberg_error)?;
        let writer = self
            .writer
            .take()
            .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "conditional output is closed"))?;
        match writer {
            WriteStream::Object {
                mut writer,
                uploads,
            } => match writer.close().await {
                Ok(metadata) => {
                    drop(writer);
                    let exit = uploads.check_exit().map_err(|error| {
                        self.record_stream_exit_failure(storage_error(
                            "close conditional object upload",
                            error,
                        ))
                    });
                    self.complete_stream();
                    self.confirm_content(receipt, metadata.content_length())?;
                    exit?;
                }
                Err(error) => {
                    let primary = storage_error("close conditional object", error);
                    let abort = WriteStream::Object { writer, uploads }
                        .abort(self.journal.cleanup_request(&self.original))
                        .await
                        .map_err(|cause| self.record_stream_exit_failure(cause));
                    if abort.is_ok() {
                        self.complete_stream();
                    }
                    let recovered = self.recover_object(receipt, primary).await;
                    // Readback cannot certify the exit of a failed abort.
                    abort?;
                    recovered?;
                }
            },
            #[cfg(unix)]
            WriteStream::Local(mut writer) => {
                let result = writer.close_and_install(&self.cancellation).await;
                let attributed = match result {
                    Ok(novarocks_fs::ConditionalCreateOutcome::Created) => {
                        self.confirm_content(receipt, receipt.size())
                    }
                    other if writer.install_dispatched() => {
                        self.recover_local(&writer, receipt, other).await
                    }
                    Err(error) => Err(storage_error("close conditional local file", error)),
                    _ => Err(Error::new(
                        ErrorKind::Unexpected,
                        "local creation was not dispatched",
                    )),
                };
                let cleanup = writer
                    .abort()
                    .await
                    .map_err(|cause| storage_error("remove private local output", cause))
                    .map_err(|cause| self.record_stream_exit_failure(cause));
                if cleanup.is_ok() {
                    self.complete_stream();
                }
                attributed?;
                cleanup?;
            }
        }
        self.original.check_active().map_err(iceberg_error)?;
        Ok(FileMetadata {
            size: receipt.size(),
        })
    }
}

impl Drop for ManagedFileWrite {
    fn drop(&mut self) {
        let Some(writer) = self.writer.take() else {
            return;
        };
        let exit = self.journal.observe_stream_exit(&self.path);
        let cleanup_context = self.journal.cleanup_request(&self.original);
        let mut cleanup: Option<ConnectorIoCleanupFuture> = Some(Box::pin(async move {
            writer.abort(cleanup_context).await.map_err(|cause| {
                let original = exit.failed(cause);
                ConnectorError::new(ConnectorErrorKind::Unavailable, original.to_string())
            })
        }));
        if let Some(stream) = self.stream.take() {
            if let Err((stream, error)) = stream.install_cleanup_in(&mut cleanup) {
                std::panic::panic_any(super::GovernedCleanupRefusal {
                    _cleanup: cleanup,
                    _stream: stream,
                    _error: error,
                    _phase: self.phase.take(),
                });
            }
        } else {
            self.journal.retain_stream_cleanup(cleanup.take().unwrap());
        }
    }
}

fn relative_path(access: &FsAccessHandle) -> Result<&str> {
    match access.paths() {
        [path] => Ok(path.operator_relative_path()),
        _ => Err(Error::new(
            ErrorKind::DataInvalid,
            "managed creation requires one exact storage path",
        )),
    }
}

fn claim_metadata(claim: CreateClaim) -> [(String, String); 2] {
    [
        (
            CREATE_ID.into(),
            uuid::Uuid::from_bytes(claim.id).to_string(),
        ),
        (
            CREATE_IDENTITY.into(),
            format!("{:x}", sha2::digest::Output::<Sha256>::from(claim.identity)),
        ),
    ]
}

fn matches_marker(metadata: &crate::opendal::Metadata, claim: CreateClaim) -> bool {
    metadata.user_metadata().is_some_and(|actual| {
        claim_metadata(claim)
            .iter()
            .all(|(key, value)| actual.get(key) == Some(value))
    })
}

async fn observe_marker(
    access: std::result::Result<FsAccessHandle, ConnectorError>,
    context: &ConnectorRequestContext,
) -> Result<(crate::opendal::Operator, String, crate::opendal::Metadata)> {
    let access = access.map_err(iceberg_error)?;
    context.check_active().map_err(iceberg_error)?;
    let op = access.operator();
    let path = relative_path(&access)?.to_owned();
    let stat = op
        .stat(&path)
        .await
        .map_err(|error| storage_error("observe conditional object", error))?;
    Ok((op, path, stat))
}

async fn verify_object(
    op: &crate::opendal::Operator,
    path: &str,
    metadata: &crate::opendal::Metadata,
    context: &ConnectorRequestContext,
    receipt: IcebergCreationReceipt,
) -> Result<bool> {
    if metadata.content_length() != receipt.size() {
        return Ok(false);
    }
    context.check_active().map_err(iceberg_error)?;
    let mut reader = op.reader_with(path);
    let cap = op.info().capability();
    if let Some(version) = metadata.version().filter(|_| cap.read_with_version) {
        reader = reader.version(version);
    } else if let Some(etag) = metadata.etag().filter(|_| cap.read_with_if_match) {
        reader = reader.if_match(etag);
    } else {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "conditional object observation cannot pin its content",
        ));
    }
    let reader = reader
        .await
        .map_err(|error| storage_error("open exact conditional object", error))?;
    let mut stream = reader
        .into_bytes_stream(..)
        .await
        .map_err(|error| storage_error("read exact conditional object", error))?;
    let mut observed = Sha256::new();
    let mut size = 0u64;
    while let Some(bytes) = stream.next().await {
        let bytes = bytes.map_err(|error| storage_error("read exact conditional object", error))?;
        let Some(next) = size
            .checked_add(bytes.len() as u64)
            .filter(|n| *n <= receipt.size())
        else {
            return Ok(false);
        };
        size = next;
        observed.update(bytes);
        context.check_active().map_err(iceberg_error)?;
    }
    Ok(size == receipt.size() && <[u8; 32]>::from(observed.finalize()) == receipt.content_sha256())
}

#[cfg(unix)]
async fn verify_local_file(
    writer: &novarocks_fs::ConditionalLocalFile,
    context: &ConnectorRequestContext,
    receipt: IcebergCreationReceipt,
) -> Result<bool> {
    use tokio::io::AsyncReadExt;

    let mut file = writer
        .read_private_file()
        .await
        .map_err(|error| storage_error("open installed local inode", error))?;
    let mut buffer = vec![0u8; 16 * 1024];
    let mut observed = Sha256::new();
    let mut size = 0u64;
    loop {
        context.check_active().map_err(iceberg_error)?;
        let count = file
            .read(&mut buffer)
            .await
            .map_err(|error| storage_error("read installed local inode", error))?;
        if count == 0 {
            break;
        }
        let Some(next) = size
            .checked_add(count as u64)
            .filter(|n| *n <= receipt.size())
        else {
            return Ok(false);
        };
        size = next;
        observed.update(&buffer[..count]);
    }
    Ok(size == receipt.size() && <[u8; 32]>::from(observed.finalize()) == receipt.content_sha256())
}

fn iceberg_error(error: ConnectorError) -> Error {
    storage_error("managed Iceberg output", error)
}

fn journal_error(error: String) -> Error {
    Error::new(ErrorKind::Unexpected, error)
}

fn storage_error(
    label: &'static str,
    error: impl std::error::Error + Send + Sync + 'static,
) -> Error {
    Error::new(ErrorKind::Unexpected, label).with_source(error)
}

#[cfg(all(test, unix))]
mod tests;
