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
use std::io::{self, Read};
use std::sync::Arc;

use bytes::Bytes;
use orc_rust::reader::ChunkReader as OrcChunkReader;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::reader::{ChunkReader, Length};

use crate::cache::cache_input_stream::CacheInputStream;
use crate::cache::{BlockCache, CacheBlockRead, DataCacheContext, DataCacheMetricsRecorder};
use crate::common::file_identity::FileIdentity;
use crate::fs::opendal::OpendalRangeReader;
use crate::metrics;
use crate::runtime::profile::{CounterRef, RuntimeProfile, clamp_u128_to_i64};

#[derive(Clone, Debug)]
pub(crate) struct CacheIoCounters {
    cache_read_requests: CounterRef,
    bytes_read_from_cache: CounterRef,
    datacache_read_counter: CounterRef,
    datacache_read_bytes: CounterRef,
    datacache_read_timer: CounterRef,
    datacache_write_counter: CounterRef,
    datacache_write_bytes: CounterRef,
    datacache_write_timer: CounterRef,
}

impl CacheIoCounters {
    fn new(profile: &RuntimeProfile) -> Self {
        Self {
            cache_read_requests: profile.add_counter("CacheReadRequests", metrics::TUnit::UNIT),
            bytes_read_from_cache: profile.add_counter("BytesReadFromCache", metrics::TUnit::BYTES),
            datacache_read_counter: profile
                .add_counter("DataCacheReadCounter", metrics::TUnit::UNIT),
            datacache_read_bytes: profile.add_counter("DataCacheReadBytes", metrics::TUnit::BYTES),
            datacache_read_timer: profile
                .add_counter("DataCacheReadTimer", metrics::TUnit::TIME_NS),
            datacache_write_counter: profile
                .add_counter("DataCacheWriteCounter", metrics::TUnit::UNIT),
            datacache_write_bytes: profile
                .add_counter("DataCacheWriteBytes", metrics::TUnit::BYTES),
            datacache_write_timer: profile
                .add_counter("DataCacheWriteTimer", metrics::TUnit::TIME_NS),
        }
    }
}

impl DataCacheMetricsRecorder for CacheIoCounters {
    fn record_cache_hit(&self, length: usize) {
        self.cache_read_requests.add(1);
        self.bytes_read_from_cache
            .add(clamp_u128_to_i64(length as u128));
    }

    fn record_datacache_read(&self, length: usize, io_ns: u128) {
        self.datacache_read_counter.add(1);
        self.datacache_read_bytes
            .add(clamp_u128_to_i64(length as u128));
        self.datacache_read_timer.add(clamp_u128_to_i64(io_ns));
    }

    fn record_datacache_write(&self, length: usize, io_ns: u128) {
        self.datacache_write_counter.add(1);
        self.datacache_write_bytes
            .add(clamp_u128_to_i64(length as u128));
        self.datacache_write_timer.add(clamp_u128_to_i64(io_ns));
    }
}

#[derive(Clone, Debug)]
pub struct CachedRangeReader {
    inner: OpendalRangeReader,
    counters: Option<CacheIoCounters>,
    datacache: Option<DataCacheContext>,
    block_cache: Option<Arc<BlockCache>>,
    enable_datacache: bool,
    enable_populate_datacache: bool,
    enable_async_datacache: bool,
}

impl CachedRangeReader {
    pub fn new(inner: OpendalRangeReader, datacache: Option<DataCacheContext>) -> Self {
        let (block_cache, enable_datacache, enable_populate_datacache, enable_async_datacache) =
            if let Some(ctx) = datacache.as_ref() {
                let io = ctx.io_options();
                (
                    ctx.block_cache(),
                    io.enable_datacache,
                    io.enable_populate_datacache,
                    io.enable_datacache_async_populate_mode,
                )
            } else {
                (None, false, false, false)
            };
        Self {
            counters: inner.profile().as_ref().map(CacheIoCounters::new),
            inner,
            datacache,
            block_cache,
            enable_datacache,
            enable_populate_datacache,
            enable_async_datacache,
        }
    }

    pub fn file_identity(&self) -> &FileIdentity {
        self.inner.file_identity()
    }

    pub fn datacache(&self) -> Option<&DataCacheContext> {
        self.datacache.as_ref()
    }

    pub fn file_len(&self) -> u64 {
        self.inner.len()
    }

    pub fn open_read(&self, start: u64) -> io::Result<CachedRead> {
        if start > self.file_len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "expected to read at offset {}, while file has length {}",
                    start,
                    self.file_len()
                ),
            ));
        }
        Ok(CachedRead::new(self.clone(), start))
    }

    pub fn read_bytes(&self, start: u64, length: usize) -> io::Result<Bytes> {
        let end = start
            .checked_add(length as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "range overflow"))?;
        if start > self.file_len() || end > self.file_len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "expected to read {} bytes at offset {}, while file has length {}",
                    length,
                    start,
                    self.file_len()
                ),
            ));
        }
        if !self.enable_datacache {
            return self.inner.read_remote_bytes(start, length);
        }
        let Some(cache_stream) = self.cache_stream() else {
            return self.inner.read_remote_bytes(start, length);
        };

        let block_size = cache_stream.block_size();
        let start_block = start / block_size;
        let end_block = (end.saturating_sub(1)) / block_size;
        let mut out = Vec::with_capacity(length);
        for block_id in start_block..=end_block {
            let block_start = block_id * block_size;
            let block_end = std::cmp::min(block_start + block_size, self.file_len());
            let read = self.read_cached_block(&cache_stream, block_id, block_start, block_end)?;
            let slice_start = if block_id == start_block {
                (start - block_start) as usize
            } else {
                0
            };
            let slice_end = if block_id == end_block {
                (end - block_start) as usize
            } else {
                read.bytes.len()
            };
            out.extend_from_slice(&read.bytes[slice_start..slice_end]);
        }
        Ok(Bytes::from(out))
    }

    pub fn into_inner(self) -> OpendalRangeReader {
        self.inner
    }

    pub(crate) fn counters(&self) -> Option<CacheIoCounters> {
        self.counters.clone()
    }

    fn cache_stream(&self) -> Option<CacheInputStream> {
        let cache = self.block_cache.as_ref()?;
        Some(CacheInputStream::new(
            Arc::clone(cache),
            self.file_identity().clone(),
            self.enable_datacache,
            self.enable_populate_datacache,
            self.enable_async_datacache,
        ))
    }

    fn read_cached_block(
        &self,
        cache_stream: &CacheInputStream,
        block_id: u64,
        block_start: u64,
        block_end: u64,
    ) -> io::Result<CacheBlockRead> {
        let block_len = (block_end - block_start) as usize;
        let read = cache_stream.read_block(block_id, block_len, || {
            self.inner.read_remote_range(block_start, block_end)
        })?;
        if let Some(datacache) = self.datacache.as_ref() {
            datacache.record_block_cache_result(self.counters.as_ref(), block_len, &read);
        }
        Ok(read)
    }

    fn get_bytes_for_parquet(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        self.read_bytes(start, length).map_err(|e| match e.kind() {
            io::ErrorKind::UnexpectedEof => ParquetError::EOF(e.to_string()),
            _ => ParquetError::General(e.to_string()),
        })
    }
}

impl Length for CachedRangeReader {
    fn len(&self) -> u64 {
        self.file_len()
    }
}

impl ChunkReader for CachedRangeReader {
    type T = CachedRead;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        self.open_read(start).map_err(|e| match e.kind() {
            io::ErrorKind::UnexpectedEof => ParquetError::EOF(e.to_string()),
            _ => ParquetError::General(e.to_string()),
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        self.get_bytes_for_parquet(start, length)
    }
}

impl OrcChunkReader for CachedRangeReader {
    type T = CachedRead;

    fn len(&self) -> u64 {
        self.file_len()
    }

    fn get_read(&self, offset_from_start: u64) -> io::Result<Self::T> {
        self.open_read(offset_from_start)
    }
}

pub struct CachedRead {
    reader: CachedRangeReader,
    pos: u64,
    buf: Bytes,
    buf_pos: usize,
}

impl CachedRead {
    fn new(reader: CachedRangeReader, start: u64) -> Self {
        Self {
            reader,
            pos: start,
            buf: Bytes::new(),
            buf_pos: 0,
        }
    }

    fn refill(&mut self) -> io::Result<()> {
        if self.pos >= self.reader.file_len() {
            self.buf = Bytes::new();
            self.buf_pos = 0;
            return Ok(());
        }

        if self.reader.enable_datacache {
            if let Some(cache_stream) = self.reader.cache_stream() {
                let block_size = cache_stream.block_size();
                let block_id = self.pos / block_size;
                let block_start = block_id * block_size;
                let block_end = std::cmp::min(block_start + block_size, self.reader.file_len());
                let read = self.reader.read_cached_block(
                    &cache_stream,
                    block_id,
                    block_start,
                    block_end,
                )?;
                self.buf = read.bytes;
                self.buf_pos = (self.pos - block_start) as usize;
                return Ok(());
            }
        }

        let remaining = self.reader.file_len() - self.pos;
        let fetch_len = std::cmp::min(self.reader.inner.block_size() as u64, remaining) as usize;
        self.buf = self.reader.inner.read_remote_bytes(self.pos, fetch_len)?;
        self.buf_pos = 0;
        Ok(())
    }
}

impl Read for CachedRead {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.pos >= self.reader.file_len() {
            return Ok(0);
        }
        if self.buf_pos >= self.buf.len() {
            self.refill()?;
            if self.buf.is_empty() {
                return Ok(0);
            }
        }

        let available = &self.buf[self.buf_pos..];
        let to_copy = std::cmp::min(available.len(), out.len());
        out[..to_copy].copy_from_slice(&available[..to_copy]);
        self.buf_pos += to_copy;
        self.pos += to_copy as u64;
        Ok(to_copy)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Once;

    use crate::cache::block_cache::{BlockCacheOptions, get_block_cache, init_block_cache};
    use crate::cache::{CacheOptions, DataCacheManager};
    use crate::fs::opendal::OpendalRangeReaderFactory;
    use crate::fs::opendal::build_fs_operator;
    use crate::metrics;
    use crate::runtime::profile::RuntimeProfile;

    use super::CachedRangeReader;

    static BLOCK_CACHE_INIT: Once = Once::new();

    fn ensure_block_cache() {
        BLOCK_CACHE_INIT.call_once(|| {
            if get_block_cache().is_some() {
                return;
            }
            let root = std::env::temp_dir().join(format!(
                "novarocks-cached-reader-block-cache-{}",
                std::process::id()
            ));
            fs::create_dir_all(&root).expect("create block cache test dir");
            init_block_cache(
                root.to_str().expect("block cache test path"),
                BlockCacheOptions {
                    capacity: 1024 * 1024,
                    block_size: 4,
                    enable_checksum: false,
                    ..Default::default()
                },
            )
            .expect("init block cache");
        });
        assert!(get_block_cache().is_some());
    }

    fn test_cache_options() -> CacheOptions {
        CacheOptions {
            enable_scan_datacache: true,
            enable_populate_datacache: true,
            enable_datacache_async_populate_mode: false,
            enable_datacache_io_adaptor: true,
            enable_cache_select: false,
            datacache_evict_probability: 100,
            datacache_priority: 0,
            datacache_ttl_seconds: 0,
            datacache_sharing_work_period: None,
        }
    }

    #[test]
    fn read_bytes_uses_block_cache_after_first_fetch() {
        ensure_block_cache();
        let temp_dir = tempfile::tempdir().expect("tempdir");
        fs::write(temp_dir.path().join("sample.bin"), b"abcdefgh").expect("write fixture");

        let profile = RuntimeProfile::new("cached_reader_test");
        let op = build_fs_operator(temp_dir.path().to_str().expect("temp dir path"))
            .expect("build fs operator");
        let factory = OpendalRangeReaderFactory::from_operator(op)
            .expect("reader factory")
            .with_profile(Some(profile.clone()));
        let reader = factory
            .open_with_len("sample.bin", Some(8))
            .expect("open with len");
        let ctx = DataCacheManager::instance().external_context(test_cache_options());
        let cached = CachedRangeReader::new(reader, Some(ctx));

        let first = cached.read_bytes(0, 4).expect("first read");
        let read_requests = profile.add_counter("ReadRequests", metrics::TUnit::UNIT);
        assert_eq!(first.as_ref(), b"abcd");
        assert_eq!(read_requests.value(), 1);

        let second = cached.read_bytes(0, 4).expect("second read");
        assert_eq!(second.as_ref(), b"abcd");
        assert_eq!(read_requests.value(), 1);
    }

    #[test]
    fn read_bytes_can_cross_multiple_cache_blocks() {
        ensure_block_cache();
        let temp_dir = tempfile::tempdir().expect("tempdir");
        fs::write(temp_dir.path().join("sample.bin"), b"abcdefghij").expect("write fixture");

        let op = build_fs_operator(temp_dir.path().to_str().expect("temp dir path"))
            .expect("build fs operator");
        let factory = OpendalRangeReaderFactory::from_operator(op).expect("reader factory");
        let reader = factory
            .open_with_len("sample.bin", Some(10))
            .expect("open with len");
        let ctx = DataCacheManager::instance().external_context(test_cache_options());
        let cached = CachedRangeReader::new(reader, Some(ctx));

        let bytes = cached.read_bytes(2, 6).expect("cross block read");
        assert_eq!(bytes.as_ref(), b"cdefgh");
    }
}
