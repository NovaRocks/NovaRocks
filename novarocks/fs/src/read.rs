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

use std::fmt::{Debug, Formatter};
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::UInt64Array;
use arrow::record_batch::RecordBatch;
use bytes::{Bytes, BytesMut};
use novarocks_spi::connector::StorageAccessDomainId;

use crate::{
    BoundFile, DataCacheContext, FileCancellation, FileError, FileErrorKind, FileIdentity,
    FileIoRuntime, FileResult, FileTaskSpawner, PhysicalPruning, ScanPredicate,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileFormat {
    Parquet,
    Orc,
}

/// Whole-file probe limit shared by demand and speculative Parquet reads.
pub const SMALL_FILE_PROBE_MAX_BYTES: u64 = 64 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileReadRange {
    WholeFile,
    Bounded { offset: u64, length: u64 },
}

impl FileReadRange {
    pub fn bounded(offset: u64, length: u64) -> FileResult<Self> {
        if length == 0 {
            return Err(crate::FileError::invalid(
                "bounded file read range length must be greater than zero",
            ));
        }
        offset
            .checked_add(length)
            .ok_or_else(|| crate::FileError::invalid("bounded file read range overflows"))?;
        Ok(Self::Bounded { offset, length })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FileProjection {
    All,
    RootNames(Vec<String>),
    RootIndices(Vec<usize>),
    FieldIds(Vec<i32>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileReadBudget {
    pub max_rows: NonZeroUsize,
    pub max_bytes: NonZeroUsize,
}

#[derive(Clone)]
pub struct FileReadContext {
    pub cancellation: FileCancellation,
    pub deadline: Option<Instant>,
    pub runtime: Arc<dyn FileIoRuntime>,
    pub task_spawner: Arc<dyn FileTaskSpawner>,
    /// The shared scan I/O an execution-attempt read goes through, bound to
    /// the source the read is for. Without it a read goes straight to the
    /// object store.
    pub range: Option<crate::FileRangeBinding>,
}

/// One immutable, contiguous backing prepared for an exact authorized file.
/// The retained capacity belongs to the original allocation, even when a
/// decoder later borrows only a slice of its bytes.
#[derive(Clone)]
pub struct PreparedFileInput {
    access_domain: StorageAccessDomainId,
    identity: FileIdentity,
    offset: u64,
    end: u64,
    bytes: Bytes,
    retained_backing_capacity: usize,
}

impl Debug for PreparedFileInput {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedFileInput")
            .field("access_domain", &self.access_domain)
            .field("identity", &self.identity)
            .field("range", &self.range())
            .field("retained_backing_capacity", &self.retained_backing_capacity)
            .finish_non_exhaustive()
    }
}

impl PreparedFileInput {
    pub fn new(file: &BoundFile, offset: u64, backing: BytesMut) -> FileResult<Self> {
        let capacity = backing.capacity();
        Self::from_completed(file, offset, backing.freeze(), capacity)
    }

    pub(crate) fn from_completed(
        file: &BoundFile,
        offset: u64,
        bytes: Bytes,
        retained_backing_capacity: usize,
    ) -> FileResult<Self> {
        if bytes.is_empty() {
            return Err(FileError::invalid("prepared file input must be nonempty"));
        }
        if retained_backing_capacity < bytes.len() {
            return Err(FileError::invalid(
                "prepared file input capacity is smaller than its bytes",
            ));
        }
        let length = u64::try_from(bytes.len()).map_err(|_| {
            FileError::new(
                FileErrorKind::ResourceExhausted,
                "prepared file input length exceeds address space",
            )
        })?;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| FileError::invalid("prepared file input range overflows"))?;
        if end > file.identity().file_size() {
            return Err(FileError::new(
                FileErrorKind::Corrupt,
                "prepared file input exceeds bound file length",
            ));
        }
        Ok(Self {
            access_domain: file.access_domain(),
            identity: file.identity().clone(),
            offset,
            end,
            bytes,
            retained_backing_capacity,
        })
    }

    pub fn access_domain(&self) -> StorageAccessDomainId {
        self.access_domain
    }

    pub fn identity(&self) -> &FileIdentity {
        &self.identity
    }

    pub fn range(&self) -> Range<u64> {
        self.offset..self.end
    }

    pub fn retained_backing_capacity(&self) -> usize {
        self.retained_backing_capacity
    }

    pub(crate) fn validate_for(&self, file: &BoundFile) -> FileResult<()> {
        if self.access_domain != file.access_domain() || self.identity != *file.identity() {
            return Err(FileError::invalid(
                "prepared file input authorization domain or file identity mismatch",
            ));
        }
        Ok(())
    }

    pub(crate) fn bytes(&self) -> &Bytes {
        &self.bytes
    }
}

impl FileReadContext {
    /// The read's stop bounded by its deadline: what every wait of the read
    /// observes.
    pub fn bounded_cancellation(&self) -> FileCancellation {
        self.cancellation.clone().with_deadline(self.deadline)
    }

    pub fn check_active(&self) -> FileResult<()> {
        self.cancellation.check()?;
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(crate::FileError::deadline(
                "file operation deadline exceeded",
            ));
        }
        Ok(())
    }
}

impl Debug for FileReadContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileReadContext")
            .field("cancellation", &self.cancellation)
            .field("deadline", &self.deadline)
            .field("range", &self.range)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub struct FileReadRequest {
    pub file: BoundFile,
    pub format: FileFormat,
    pub range: FileReadRange,
    pub projection: FileProjection,
    pub budget: FileReadBudget,
    pub predicates: Vec<ScanPredicate>,
    pub pruning: PhysicalPruning,
    pub options: FileReaderOptions,
    pub cache: Option<DataCacheContext>,
    pub prepared_input: Option<PreparedFileInput>,
    pub context: FileReadContext,
}

/// Reader-open policy that remains local to the neutral filesystem boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileReaderOptions {
    pub enable_parquet_reader_page_index: bool,
    pub coalesce_reads: bool,
    pub coalesce_max_bytes: u64,
    pub coalesce_max_gap: u64,
}

impl Default for FileReaderOptions {
    fn default() -> Self {
        Self {
            enable_parquet_reader_page_index: false,
            coalesce_reads: true,
            coalesce_max_bytes: 8 * 1024 * 1024,
            coalesce_max_gap: 1024 * 1024,
        }
    }
}

#[derive(Debug)]
pub struct FileBatch {
    pub batch: RecordBatch,
    pub physical_row_positions: Option<UInt64Array>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FileMetricsSnapshot {
    pub bytes_read: u64,
    pub read_requests: u64,
    pub rows_decoded: u64,
    pub batches_delivered: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub io_time_ns: u64,
    pub decode_time_ns: u64,
    pub row_groups_read: u64,
    pub row_groups_pruned: u64,
    pub delayed_materialization_ranges: u64,
    pub page_index_attempts: u64,
    pub page_index_fallbacks: u64,
    pub page_index_rows_considered: u64,
    pub page_index_rows_pruned: u64,
    pub partial_prefetch_copy_bytes: u64,
}

pub trait FileBatchReader: Send {
    fn next_batch(&mut self) -> FileResult<Option<FileBatch>>;

    fn close(&mut self) -> FileResult<()>;

    fn metrics_snapshot(&self) -> FileMetricsSnapshot;
}
