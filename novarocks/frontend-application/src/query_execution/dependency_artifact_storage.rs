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

//! Synchronous private artifact storage under the existing statement authority.
//! This is storage, not kernel coverage, a source certificate, or formal MEM completion.
use novarocks_query_application::session_control::StatementToken;
use novarocks_workload_control::{
    AllocationCharge, LocalResourceAuthority, ResourceClass, WorkError, WorkScope,
};
use serde::Deserialize;
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::{
    alloc::{Layout, alloc, dealloc},
    fmt::{self, Write as _},
    fs::{File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    ptr::NonNull,
    sync::Arc,
};

/// Explicit host input. Neither omission nor an artifact byte ceiling grants stock.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CaptureOutputDescriptor {
    pub version: u32,
    pub run_namespace: String,
    pub output_root: PathBuf,
    pub max_statement_bytes: u64,
}
impl CaptureOutputDescriptor {
    pub(crate) fn validate(&self) -> Result<(), CaptureStorageError> {
        if self.version != 1 {
            return Err(CaptureStorageError::Descriptor(
                "unsupported capture descriptor version",
            ));
        }
        if self.run_namespace.is_empty()
            || !self
                .run_namespace
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(CaptureStorageError::Descriptor(
                "capture run namespace is not a nonempty path component",
            ));
        }
        if !self.output_root.is_absolute() || self.output_root.to_str().is_none() {
            return Err(CaptureStorageError::Descriptor(
                "capture output root must be absolute UTF-8",
            ));
        }
        if self.max_statement_bytes == 0 || self.max_statement_bytes > isize::MAX as u64 {
            return Err(CaptureStorageError::Descriptor(
                "capture statement byte ceiling is not representable",
            ));
        }
        // The runner creates its private directory before launch; no implicit parent tree.
        let metadata =
            std::fs::symlink_metadata(&self.output_root).map_err(CaptureStorageError::Io)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(CaptureStorageError::Descriptor(
                "capture output root is not a private real directory",
            ));
        }
        #[cfg(unix)]
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(CaptureStorageError::Descriptor(
                "capture output root permits other users",
            ));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) enum CaptureStorageError {
    Descriptor(&'static str),
    Work(WorkError),
    Allocation { bytes: usize },
    InvalidLayout,
    StatementLimit { requested: u64, ceiling: u64 },
    Io(io::Error),
    Closed,
}
impl fmt::Display for CaptureStorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Descriptor(message) => f.write_str(message),
            Self::Work(error) => error.fmt(f),
            Self::Allocation { bytes } => {
                write!(f, "capture allocation request failed: {bytes} bytes")
            }
            Self::InvalidLayout => f.write_str("capture allocation layout is not representable"),
            Self::StatementLimit { requested, ceiling } => write!(
                f,
                "capture artifact request {requested} exceeds statement ceiling {ceiling}"
            ),
            Self::Io(error) => error.fmt(f),
            Self::Closed => f.write_str("capture artifact is closed"),
        }
    }
}
impl std::error::Error for CaptureStorageError {}
impl From<WorkError> for CaptureStorageError {
    fn from(error: WorkError) -> Self {
        Self::Work(error)
    }
}

/// Exactly one global allocation request, admitted before allocation and charged
/// only after success. No allocator fallback, guessed amount, or parallel wallet.
pub(crate) struct AdmittedCaptureBytes {
    data: NonNull<u8>,
    layout: Layout,
    initialized: usize,
    _charge: AllocationCharge,
}
// SAFETY: The allocation is uniquely owned; slices require a borrow of this owner.
unsafe impl Send for AdmittedCaptureBytes {}
impl AdmittedCaptureBytes {
    pub(crate) fn try_new(
        authority: &LocalResourceAuthority,
        scope: &WorkScope,
        bytes: usize,
    ) -> Result<Self, CaptureStorageError> {
        let layout = Layout::array::<u8>(bytes).map_err(|_| CaptureStorageError::InvalidLayout)?;
        if bytes == 0 {
            return Err(CaptureStorageError::InvalidLayout);
        }
        let mut reservation = authority.reserve(scope, bytes as u64, ResourceClass::Data)?;
        // SAFETY: The checked nonzero layout is the real global allocator request.
        let data = NonNull::new(unsafe { alloc(layout) })
            .ok_or(CaptureStorageError::Allocation { bytes })?;
        let charge = match reservation.charge(bytes as u64) {
            Ok(charge) => charge,
            Err(error) => {
                // SAFETY: This exact allocation has not escaped; the original layout is retained.
                unsafe { dealloc(data.as_ptr(), layout) };
                return Err(error.into());
            }
        };
        Ok(Self {
            data,
            layout,
            initialized: 0,
            _charge: charge,
        })
    }
    pub(crate) fn as_slice(&self) -> &[u8] {
        // SAFETY: Only the prefix initialized by push/write_zeroed is exposed.
        unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.initialized) }
    }
    pub(crate) fn write_zeroed(&mut self) -> &mut [u8] {
        // SAFETY: Unique ownership permits initialization of the complete checked layout.
        unsafe { self.data.as_ptr().write_bytes(0, self.layout.size()) };
        self.initialized = self.layout.size();
        // SAFETY: The complete allocation is now initialized and uniquely borrowed.
        unsafe { std::slice::from_raw_parts_mut(self.data.as_ptr(), self.initialized) }
    }
    fn append(&mut self, bytes: &[u8]) -> fmt::Result {
        let end = self
            .initialized
            .checked_add(bytes.len())
            .ok_or(fmt::Error)?;
        if end > self.layout.size() {
            return Err(fmt::Error);
        }
        // SAFETY: Source cannot alias this private allocation; bounds were checked above.
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.data.as_ptr().add(self.initialized),
                bytes.len(),
            )
        };
        self.initialized = end;
        Ok(())
    }
}
impl fmt::Write for AdmittedCaptureBytes {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.append(text.as_bytes())
    }
}
impl Drop for AdmittedCaptureBytes {
    fn drop(&mut self) {
        // SAFETY: The exact live allocation is uniquely owned and freed once, before its charge.
        unsafe { dealloc(self.data.as_ptr(), self.layout) };
        // _charge drops after backing deallocation, preserving release order.
    }
}

/// Immutable process composition input. Contains no statement registry or observer cache.
pub(crate) struct SqlDependencyArtifactFactory {
    descriptor: Arc<CaptureOutputDescriptor>,
    authority: LocalResourceAuthority,
}
impl SqlDependencyArtifactFactory {
    pub(crate) fn try_new(
        descriptor: CaptureOutputDescriptor,
        authority: LocalResourceAuthority,
    ) -> Result<Self, CaptureStorageError> {
        descriptor.validate()?;
        Ok(Self {
            descriptor: Arc::new(descriptor),
            authority,
        })
    }
    /// Called with the actual admitted token and actual original statement scope.
    /// The caller supplies the digest computed from the actual split statement bytes.
    /// Storage does not invent another token or a synthetic source identity.
    pub(crate) fn begin(
        &self,
        token: StatementToken,
        original_sql_sha256: &[u8; 32],
        scope: &WorkScope,
    ) -> Result<CaptureArtifact, CaptureStorageError> {
        let root = self
            .descriptor
            .output_root
            .to_str()
            .expect("checked output root");
        // Maximum decimal widths are representation facts, not a byte budget/grant.
        let path_bytes = root
            .len()
            .checked_add(self.descriptor.run_namespace.len())
            .and_then(|n| n.checked_add(1 + 1 + 20 + 1 + 20 + 4))
            .ok_or(CaptureStorageError::InvalidLayout)?;
        let mut path = AdmittedCaptureBytes::try_new(&self.authority, scope, path_bytes)?;
        write!(
            &mut path,
            "{root}/{}-{}-{}.cap",
            self.descriptor.run_namespace,
            token.session().key().get(),
            token.generation()
        )
        .map_err(|_| CaptureStorageError::InvalidLayout)?;
        let path = Path::new(std::str::from_utf8(path.as_slice()).expect("checked path inputs"));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let file = options.open(path).map_err(CaptureStorageError::Io)?;
        let mut artifact = CaptureArtifact {
            file: Some(file),
            authority: self.authority.clone(),
            scope: scope.clone(),
            max_statement_bytes: self.descriptor.max_statement_bytes,
            written: 0,
            failed: false,
        };
        // Private envelope only; FVT/CV/binding bodies remain original encoder DTOs.
        let mut header = [0u8; 64];
        header[..8].copy_from_slice(b"NRDEPS02");
        header[8..16].copy_from_slice(&token.session().key().get().to_le_bytes());
        header[20..28].copy_from_slice(&token.generation().to_le_bytes());
        header[28..60].copy_from_slice(original_sql_sha256);
        header[60..64].copy_from_slice(&2u32.to_le_bytes());
        artifact.write_bytes(&header)?;
        Ok(artifact)
    }
}

/// Synchronous owner: no queue, detached drain, borrowed source retention, or retry.
/// Each record uses one admitted output backing while its original DTO is still live.
/// Callers separately retain original codec scratch Reservations through DTO Drop.
pub(crate) struct CaptureArtifact {
    file: Option<File>,
    authority: LocalResourceAuthority,
    scope: WorkScope,
    max_statement_bytes: u64,
    written: u64,
    failed: bool,
}
impl CaptureArtifact {
    fn fail<T>(&mut self, error: CaptureStorageError) -> Result<T, CaptureStorageError> {
        // Return the original error without cloning potentially allocated I/O text.
        // Storage status alone never replaces an SQL/control/kernel first cause.
        self.failed = true;
        Err(error)
    }
    fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), CaptureStorageError> {
        let total = self
            .written
            .checked_add(bytes.len() as u64)
            .ok_or(CaptureStorageError::InvalidLayout)?;
        if total > self.max_statement_bytes {
            return self.fail(CaptureStorageError::StatementLimit {
                requested: total,
                ceiling: self.max_statement_bytes,
            });
        }
        let Some(file) = self.file.as_mut() else {
            return Err(CaptureStorageError::Closed);
        };
        if self.failed {
            return Err(CaptureStorageError::Closed);
        }
        if let Err(error) = file.write_all(bytes) {
            return self.fail(CaptureStorageError::Io(error));
        }
        self.written = total;
        Ok(())
    }
    /// Encode only with the original component's actual encoder into this slice.
    /// The occurrence tag is an artifact record address, not a Physical/constant ID.
    pub(crate) fn write_record<E>(
        &mut self,
        tag: u8,
        occurrence: u64,
        payload_bytes: usize,
        encode: impl FnOnce(&mut [u8]) -> Result<(), E>,
    ) -> Result<(), CaptureRecordError<E>> {
        if self.file.is_none() || self.failed {
            return Err(CaptureRecordError::Storage(CaptureStorageError::Closed));
        }
        let result = self.write_record_inner(tag, occurrence, payload_bytes, encode);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn write_record_inner<E>(
        &mut self,
        tag: u8,
        occurrence: u64,
        payload_bytes: usize,
        encode: impl FnOnce(&mut [u8]) -> Result<(), E>,
    ) -> Result<(), CaptureRecordError<E>> {
        let bytes = payload_bytes
            .checked_add(17)
            .ok_or(CaptureRecordError::Storage(
                CaptureStorageError::InvalidLayout,
            ))?;
        let total = self
            .written
            .checked_add(bytes as u64)
            .ok_or(CaptureRecordError::Storage(
                CaptureStorageError::InvalidLayout,
            ))?;
        if total > self.max_statement_bytes {
            return self
                .fail(CaptureStorageError::StatementLimit {
                    requested: total,
                    ceiling: self.max_statement_bytes,
                })
                .map_err(CaptureRecordError::Storage);
        }
        let mut output = AdmittedCaptureBytes::try_new(&self.authority, &self.scope, bytes)
            .map_err(CaptureRecordError::Storage)?;
        let slice = output.write_zeroed();
        slice[0] = tag;
        slice[1..9].copy_from_slice(&occurrence.to_le_bytes());
        slice[9..17].copy_from_slice(&(payload_bytes as u64).to_le_bytes());
        if let Err(error) = encode(&mut slice[17..]) {
            return Err(CaptureRecordError::Encoder(error));
        }
        self.write_bytes(output.as_slice())
            .map_err(CaptureRecordError::Storage)
    }
    /// This is only durable storage completion, never complete dependency coverage.
    /// Missing/failed source records remain incomplete in the runner's semantic ledger.
    pub(crate) fn finish_storage(&mut self) -> Result<(), CaptureStorageError> {
        if self.file.is_none() || self.failed {
            return Err(CaptureStorageError::Closed);
        }
        // Durably flush payload before publishing the storage ack. A lost ack is
        // incomplete evidence; a valid ack has no later fallible observation footer.
        if let Err(error) = self.file.as_mut().expect("open file").sync_all() {
            return self.fail(CaptureStorageError::Io(error));
        }
        self.write_bytes(b"NRFLUSH1")?;
        self.file.take();
        Ok(())
    }
    /// Loan the actual storage owner's pair. A record writer cannot invent
    /// another authority/scope or substitute its artifact ceiling for stock.
    pub(crate) fn resource_loan(&self) -> (&LocalResourceAuthority, &WorkScope) {
        (&self.authority, &self.scope)
    }
    pub(crate) fn bytes_written(&self) -> u64 {
        self.written
    }
}
#[derive(Debug)]
pub(crate) enum CaptureRecordError<E> {
    Storage(CaptureStorageError),
    Encoder(E),
}
