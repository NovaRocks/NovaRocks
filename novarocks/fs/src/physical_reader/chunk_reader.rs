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

use std::future::Future;
use std::io::{self, Read};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use orc_rust::reader::ChunkReader as OrcChunkReader;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::reader::{ChunkReader as ParquetChunkReader, Length};

use crate::{
    BoundFile, DataCacheContext, DataCacheManager, DataCachePageKey, FileError,
    FileMetricsSnapshot, FileRangeOperation, FileReadContext, FileReadRange, FileResult,
    PreparedFileInput, SMALL_FILE_PROBE_MAX_BYTES,
};

const STREAM_CHUNK_SIZE: usize = 1024 * 1024;
const RANGE_SEGMENT_BYTES: usize = 8 * 1024 * 1024;

#[derive(Default)]
pub(crate) struct ReaderMetrics {
    bytes_read: AtomicU64,
    read_requests: AtomicU64,
    rows_decoded: AtomicU64,
    batches_delivered: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    io_time_ns: AtomicU64,
    decode_time_ns: AtomicU64,
    row_groups_read: AtomicU64,
    row_groups_pruned: AtomicU64,
    delayed_materialization_ranges: AtomicU64,
    page_index_attempts: AtomicU64,
    page_index_fallbacks: AtomicU64,
    page_index_rows_considered: AtomicU64,
    page_index_rows_pruned: AtomicU64,
    partial_prefetch_copy_bytes: AtomicU64,
}

impl ReaderMetrics {
    pub(crate) fn snapshot(&self) -> FileMetricsSnapshot {
        FileMetricsSnapshot {
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            read_requests: self.read_requests.load(Ordering::Relaxed),
            rows_decoded: self.rows_decoded.load(Ordering::Relaxed),
            batches_delivered: self.batches_delivered.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            io_time_ns: self.io_time_ns.load(Ordering::Relaxed),
            decode_time_ns: self.decode_time_ns.load(Ordering::Relaxed),
            row_groups_read: self.row_groups_read.load(Ordering::Relaxed),
            row_groups_pruned: self.row_groups_pruned.load(Ordering::Relaxed),
            delayed_materialization_ranges: self
                .delayed_materialization_ranges
                .load(Ordering::Relaxed),
            page_index_attempts: self.page_index_attempts.load(Ordering::Relaxed),
            page_index_fallbacks: self.page_index_fallbacks.load(Ordering::Relaxed),
            page_index_rows_considered: self.page_index_rows_considered.load(Ordering::Relaxed),
            page_index_rows_pruned: self.page_index_rows_pruned.load(Ordering::Relaxed),
            partial_prefetch_copy_bytes: self.partial_prefetch_copy_bytes.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn record_decode(&self, rows: usize, elapsed_ns: u128) {
        self.rows_decoded.fetch_add(rows as u64, Ordering::Relaxed);
        self.decode_time_ns
            .fetch_add(clamp_u128(elapsed_ns), Ordering::Relaxed);
    }

    pub(crate) fn record_delivery(&self) {
        self.batches_delivered.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_row_group_selection(&self, total: usize, selected: usize) {
        self.row_groups_read
            .fetch_add(selected as u64, Ordering::Relaxed);
        self.row_groups_pruned
            .fetch_add(total.saturating_sub(selected) as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_page_index(&self, fallback: bool, rows_considered: u64, rows_pruned: u64) {
        saturating_add(&self.page_index_attempts, 1);
        if fallback {
            saturating_add(&self.page_index_fallbacks, 1);
        }
        saturating_add(&self.page_index_rows_considered, rows_considered);
        saturating_add(&self.page_index_rows_pruned, rows_pruned);
    }
}

fn saturating_add(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(value))
    });
}

/// The whole of a small file, fetched once for every reader of one
/// inspection. Awaited readers share the single fetch in flight.
pub(crate) type SmallFileBuffer = Arc<tokio::sync::OnceCell<Bytes>>;

#[derive(Clone)]
pub(crate) struct BoundChunkReader {
    file: BoundFile,
    context: FileReadContext,
    cache: Option<DataCacheContext>,
    range_cache_enabled: bool,
    metrics: Arc<ReaderMetrics>,
    small_file: SmallFileBuffer,
    prepared_input: Option<PreparedFileInput>,
}

/// Where one read is served from, decided without any I/O.
enum ReadTier {
    Ready(Bytes),
    SmallFile { start: usize, end: usize },
    Remote,
}

impl BoundChunkReader {
    pub(crate) fn file_size(&self) -> u64 {
        self.file.identity().file_size()
    }

    pub(crate) fn new(
        file: BoundFile,
        context: FileReadContext,
        cache: Option<DataCacheContext>,
        range_cache_enabled: bool,
        metrics: Arc<ReaderMetrics>,
    ) -> Self {
        Self {
            file,
            context,
            cache,
            range_cache_enabled,
            metrics,
            small_file: SmallFileBuffer::default(),
            prepared_input: None,
        }
    }

    pub(crate) fn with_prepared_input(
        mut self,
        input: Option<PreparedFileInput>,
    ) -> FileResult<Self> {
        if let Some(input) = &input {
            input.validate_for(&self.file)?;
        }
        self.prepared_input = input;
        Ok(self)
    }

    pub(crate) fn with_small_file_buffer(mut self, buffer: SmallFileBuffer) -> Self {
        self.small_file = buffer;
        self
    }

    pub(crate) fn small_file_buffer(&self) -> SmallFileBuffer {
        Arc::clone(&self.small_file)
    }

    /// How many independent ranges of one decoder request are worth fetching
    /// at once: the source's window in the shared range service, or one at a
    /// time for a read outside it.
    pub(crate) fn range_concurrency(&self) -> usize {
        self.context
            .range
            .as_ref()
            .map_or(1, |range| range.service().source_window().max(1))
    }

    pub(crate) async fn read_bytes_async(&self, start: u64, length: usize) -> FileResult<Bytes> {
        self.read_bytes_impl_async(start, length, true).await
    }

    /// Awaited [`Self::read_backing_bytes`].
    pub(crate) async fn read_backing_bytes_async(
        &self,
        start: u64,
        length: usize,
    ) -> FileResult<Bytes> {
        self.read_bytes_impl_async(start, length, false).await
    }

    pub(crate) fn read_bytes(&self, start: u64, length: usize) -> FileResult<Bytes> {
        self.read_bytes_impl(start, length, true)
    }

    /// A shared backing may be sliced for several decoder requests. Its
    /// merged shape must not populate independent exact-range cache entries.
    pub(crate) fn read_backing_bytes(&self, start: u64, length: usize) -> FileResult<Bytes> {
        self.read_bytes_impl(start, length, false)
    }

    fn read_bytes_impl(
        &self,
        start: u64,
        length: usize,
        populate_cache: bool,
    ) -> FileResult<Bytes> {
        match self.plan_read(start, length)? {
            ReadTier::Ready(bytes) => Ok(bytes),
            ReadTier::SmallFile { start, end } => {
                let whole = match self.small_file.get() {
                    Some(whole) => whole.clone(),
                    None => {
                        let whole = self.fetch_bytes(0, self.small_file_length()?)?;
                        let _ = self.small_file.set(whole);
                        self.small_file
                            .get()
                            .expect("small-file probe initialized")
                            .clone()
                    }
                };
                self.context.check_active()?;
                Ok(whole.slice(start..end))
            }
            ReadTier::Remote => {
                let bytes = self.fetch_bytes(start, length)?;
                self.populate_cache(start, length, &bytes, populate_cache);
                Ok(bytes)
            }
        }
    }

    async fn read_bytes_impl_async(
        &self,
        start: u64,
        length: usize,
        populate_cache: bool,
    ) -> FileResult<Bytes> {
        match self.plan_read(start, length)? {
            ReadTier::Ready(bytes) => Ok(bytes),
            ReadTier::SmallFile { start, end } => {
                let whole_length = self.small_file_length()?;
                let whole = self
                    .small_file
                    .get_or_try_init(|| self.fetch_bytes_async(0, whole_length))
                    .await?
                    .clone();
                self.context.check_active()?;
                Ok(whole.slice(start..end))
            }
            ReadTier::Remote => {
                let bytes = self.fetch_bytes_async(start, length).await?;
                self.populate_cache(start, length, &bytes, populate_cache);
                Ok(bytes)
            }
        }
    }

    /// Validates one read and decides, without I/O, where it is served from:
    /// the prepared input, the page cache, the small-file probe, or storage.
    fn plan_read(&self, start: u64, length: usize) -> FileResult<ReadTier> {
        self.context.check_active()?;
        let length_u64 = u64::try_from(length)
            .map_err(|_| FileError::invalid("file read length overflows u64"))?;
        let end = start
            .checked_add(length_u64)
            .ok_or_else(|| FileError::invalid("file read range overflows"))?;
        if end > self.file.identity().file_size() {
            return Err(FileError::new(
                crate::FileErrorKind::Corrupt,
                format!(
                    "file read range [{start}, {end}) exceeds file length {}",
                    self.file.identity().file_size()
                ),
            ));
        }

        if let Some(input) = &self.prepared_input {
            let prepared = input.range();
            if start >= prepared.start && end <= prepared.end {
                self.context.check_active()?;
                let from = (start - prepared.start) as usize;
                return Ok(ReadTier::Ready(input.bytes().slice(from..from + length)));
            }
        }

        if self.file_size() <= SMALL_FILE_PROBE_MAX_BYTES
            && !self
                .cache
                .as_ref()
                .is_some_and(crate::DataCacheContext::datacache_requested)
        {
            let start = usize::try_from(start).map_err(|_| {
                FileError::new(
                    crate::FileErrorKind::ResourceExhausted,
                    "small-file offset is too large",
                )
            })?;
            let end = start.checked_add(length).ok_or_else(|| {
                FileError::new(crate::FileErrorKind::Corrupt, "small-file slice overflows")
            })?;
            return Ok(ReadTier::SmallFile { start, end });
        }

        let cache_key = self.cache_key(start, length);
        let cache_read_enabled = self
            .cache
            .as_ref()
            .is_some_and(|cache| cache.io_options().enable_datacache);
        if cache_read_enabled
            && let Some(key) = cache_key.as_ref()
            && let Some(cache) = DataCacheManager::instance().page_cache()
            && let Some(bytes) = cache.lookup_bytes(key)
        {
            self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(ReadTier::Ready(bytes));
        }
        if cache_read_enabled && cache_key.is_some() {
            self.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
        }
        Ok(ReadTier::Remote)
    }

    fn small_file_length(&self) -> FileResult<usize> {
        usize::try_from(self.file_size()).map_err(|_| {
            FileError::new(
                crate::FileErrorKind::ResourceExhausted,
                "small file is too large",
            )
        })
    }

    fn populate_cache(&self, start: u64, length: usize, bytes: &Bytes, populate_cache: bool) {
        let cache_population_enabled = self
            .cache
            .as_ref()
            .is_some_and(|cache| cache.io_options().enable_populate_datacache);
        if populate_cache
            && cache_population_enabled
            && let Some(key) = self.cache_key(start, length)
            && let Some(cache) = DataCacheManager::instance().page_cache()
        {
            let _ = cache.insert_bytes(key, bytes.clone(), bytes.len(), Some(100));
        }
    }

    fn fetch_bytes(&self, start: u64, length: usize) -> FileResult<Bytes> {
        let began = Instant::now();
        let bytes = self
            .context
            .runtime
            .block_on_bytes(Box::pin(self.fetch_future(start, length)?))?;
        self.finish_fetch(began)?;
        Ok(bytes)
    }

    async fn fetch_bytes_async(&self, start: u64, length: usize) -> FileResult<Bytes> {
        let began = Instant::now();
        let bytes = self.fetch_future(start, length)?.await?;
        self.finish_fetch(began)?;
        Ok(bytes)
    }

    fn finish_fetch(&self, began: Instant) -> FileResult<()> {
        self.context.check_active()?;
        self.metrics
            .io_time_ns
            .fetch_add(clamp_u128(began.elapsed().as_nanos()), Ordering::Relaxed);
        Ok(())
    }

    /// One storage read of `[start, start + length)`: through the source's
    /// range service when it has one, otherwise as a direct segmented read.
    /// Both wait for the read's physical exit before they return its bytes.
    fn fetch_future(
        &self,
        start: u64,
        length: usize,
    ) -> FileResult<impl Future<Output = FileResult<Bytes>> + Send + 'static> {
        let file = self.file.clone();
        let cancellation = self.context.bounded_cancellation();
        let range = FileReadRange::Bounded {
            offset: start,
            length: u64::try_from(length)
                .map_err(|_| FileError::invalid("file read length overflows u64"))?,
        };
        let spawner = Arc::clone(&self.context.task_spawner);
        let range_binding = self.context.range.clone();
        let present = self.prepared_input.clone();
        let metrics = Arc::clone(&self.metrics);
        Ok(async move {
            if let Some(binding) = range_binding {
                let mut request = binding
                    .start_wait_with_present(file, range, cancellation, present)
                    .await?;
                let fetched_bytes = request.missing_bytes();
                let partial_copy_bytes = request.partial_copy_bytes();
                metrics
                    .partial_prefetch_copy_bytes
                    .fetch_add(partial_copy_bytes as u64, Ordering::Relaxed);
                let result = request.result_ready().await;
                let drained = request.drained().await;
                return match result {
                    Err(error) => Err(error),
                    Ok(bytes) => {
                        drained?;
                        if fetched_bytes != 0 {
                            metrics.read_requests.fetch_add(1, Ordering::Relaxed);
                        }
                        metrics
                            .bytes_read
                            .fetch_add(fetched_bytes as u64, Ordering::Relaxed);
                        Ok(bytes)
                    }
                };
            }
            if present.is_some() {
                return Err(FileError::invalid(
                    "partial prepared input requires a range service",
                ));
            }
            let mut operation = FileRangeOperation::start_segmented_into(
                file,
                range,
                BytesMut::zeroed(length),
                RANGE_SEGMENT_BYTES,
                cancellation,
                &spawner,
            )?;
            let result = operation.result_ready().await;
            let drained = operation.drained().await;
            match result {
                Err(error) => Err(error),
                Ok(bytes) => {
                    drained?;
                    metrics.read_requests.fetch_add(1, Ordering::Relaxed);
                    metrics
                        .bytes_read
                        .fetch_add(length as u64, Ordering::Relaxed);
                    Ok(bytes)
                }
            }
        })
    }

    fn cache_key(&self, start: u64, length: usize) -> Option<DataCachePageKey> {
        let cache = self.cache.as_ref()?;
        if !self.range_cache_enabled || !cache.datacache_requested() {
            return None;
        }
        let identity = self.file.identity();
        let key = format!(
            "{}\0{}\0{}\0{}\0{}",
            identity.path(),
            identity.file_size(),
            identity.modification_time().unwrap_or_default(),
            start,
            length
        );
        Some(DataCachePageKey::new(
            self.file.access_domain(),
            "physical-file-range",
            key.into_bytes(),
        ))
    }
}

impl Length for BoundChunkReader {
    fn len(&self) -> u64 {
        self.file.identity().file_size()
    }
}

impl ParquetChunkReader for BoundChunkReader {
    type T = BoundRead;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        if start > self.file.identity().file_size() {
            return Err(ParquetError::EOF(format!(
                "read offset {start} exceeds file length {}",
                self.file.identity().file_size()
            )));
        }
        Ok(BoundRead::new(self.clone(), start))
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        self.read_bytes(start, length).map_err(to_parquet_error)
    }
}

impl OrcChunkReader for BoundChunkReader {
    type T = BoundRead;

    fn len(&self) -> u64 {
        self.file.identity().file_size()
    }

    fn get_read(&self, start: u64) -> io::Result<Self::T> {
        if start > self.file.identity().file_size() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "ORC read offset exceeds file length",
            ));
        }
        Ok(BoundRead::new(self.clone(), start))
    }

    fn get_bytes(&self, start: u64, length: u64) -> io::Result<Bytes> {
        let length = usize::try_from(length)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "ORC range too large"))?;
        self.read_bytes(start, length).map_err(to_io_error)
    }
}

pub(crate) struct BoundRead {
    reader: BoundChunkReader,
    position: u64,
    buffer: Bytes,
    buffer_start: u64,
}

impl BoundRead {
    fn new(reader: BoundChunkReader, position: u64) -> Self {
        Self {
            reader,
            position,
            buffer: Bytes::new(),
            buffer_start: position,
        }
    }
}

impl Read for BoundRead {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() || self.position >= self.reader.file.identity().file_size() {
            return Ok(0);
        }
        let buffer_end = self.buffer_start.saturating_add(self.buffer.len() as u64);
        if self.position < self.buffer_start || self.position >= buffer_end {
            let remaining = self
                .reader
                .file
                .identity()
                .file_size()
                .saturating_sub(self.position);
            let fetch = remaining.min(STREAM_CHUNK_SIZE as u64) as usize;
            self.buffer = self
                .reader
                .read_bytes(self.position, fetch)
                .map_err(to_io_error)?;
            self.buffer_start = self.position;
        }
        let offset = (self.position - self.buffer_start) as usize;
        let available = &self.buffer[offset..];
        let count = available.len().min(output.len());
        output[..count].copy_from_slice(&available[..count]);
        self.position += count as u64;
        Ok(count)
    }
}

fn to_parquet_error(error: FileError) -> ParquetError {
    match error.kind() {
        crate::FileErrorKind::Corrupt | crate::FileErrorKind::NotFound => {
            ParquetError::EOF(error.to_string())
        }
        _ => ParquetError::General(error.to_string()),
    }
}

fn to_io_error(error: FileError) -> io::Error {
    let kind = match error.kind() {
        crate::FileErrorKind::NotFound => io::ErrorKind::NotFound,
        crate::FileErrorKind::Permission => io::ErrorKind::PermissionDenied,
        crate::FileErrorKind::Corrupt => io::ErrorKind::UnexpectedEof,
        // `Read::read_exact` retries `Interrupted` forever. Cancellation is
        // terminal for this reader and must propagate through format decoders.
        crate::FileErrorKind::Cancelled => io::ErrorKind::Other,
        crate::FileErrorKind::DeadlineExceeded => io::ErrorKind::TimedOut,
        _ => io::ErrorKind::Other,
    };
    io::Error::new(kind, error)
}

fn clamp_u128(value: u128) -> u64 {
    value.min(u64::MAX as u128) as u64
}
