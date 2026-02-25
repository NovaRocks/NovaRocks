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
use crate::cache::cache_input_stream::CacheInputStream;
use crate::cache::{DataCacheContext, DataCacheMetricsRecorder};
use crate::formats::parquet::{
    ParquetProbe, ParquetReadCachePolicy, parquet_page_cache_get, parquet_page_cache_put,
    probe_parquet_bytes,
};
use crate::metrics;
use crate::novarocks_logging::debug;
use crate::runtime::profile::{CounterRef, RuntimeProfile, clamp_u128_to_i64};
use anyhow::{Context, Result};
use bytes::Bytes;
use futures::TryStreamExt;
use opendal::Operator;
use orc_rust::reader::ChunkReader as OrcChunkReader;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::reader::{ChunkReader, Length};
use std::future::IntoFuture;
use std::io::{self, Read};
use std::sync::Arc;
use std::time::Instant;
use tokio::runtime::Runtime;

pub const DEFAULT_OPENDAL_READ_SIZE: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug)]
struct OpendalIoCounters {
    read_requests: CounterRef,
    bytes_read: CounterRef,
    io_timer: CounterRef,
    io_task_exec_time: CounterRef,
    scan_time: CounterRef,
    cache_read_requests: CounterRef,
    bytes_read_from_cache: CounterRef,
    datacache_read_counter: CounterRef,
    datacache_read_bytes: CounterRef,
    datacache_read_timer: CounterRef,
    datacache_write_counter: CounterRef,
    datacache_write_bytes: CounterRef,
    datacache_write_timer: CounterRef,
}

impl OpendalIoCounters {
    fn new(profile: &RuntimeProfile) -> Self {
        Self {
            read_requests: profile.add_counter("ReadRequests", metrics::TUnit::UNIT),
            bytes_read: profile.add_counter("BytesRead", metrics::TUnit::BYTES),
            io_timer: profile.add_counter("IOTimer", metrics::TUnit::TIME_NS),
            io_task_exec_time: profile.add_counter("IOTaskExecTime", metrics::TUnit::TIME_NS),
            scan_time: profile.add_counter("ScanTime", metrics::TUnit::TIME_NS),
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

    fn record_cache_hit(&self, length: usize) {
        self.cache_read_requests.add(1);
        self.bytes_read_from_cache
            .add(clamp_u128_to_i64(length as u128));
    }

    fn record_remote_read(&self, length: usize, io_ns: u128) {
        self.read_requests.add(1);
        self.bytes_read.add(clamp_u128_to_i64(length as u128));
        let io_ns = clamp_u128_to_i64(io_ns);
        self.io_timer.add(io_ns);
        self.io_task_exec_time.add(io_ns);
        self.scan_time.add(io_ns);
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

impl DataCacheMetricsRecorder for OpendalIoCounters {
    fn record_cache_hit(&self, length: usize) {
        OpendalIoCounters::record_cache_hit(self, length);
    }

    fn record_datacache_read(&self, length: usize, io_ns: u128) {
        OpendalIoCounters::record_datacache_read(self, length, io_ns);
    }

    fn record_datacache_write(&self, length: usize, io_ns: u128) {
        OpendalIoCounters::record_datacache_write(self, length, io_ns);
    }
}

#[derive(Clone, Debug)]
pub struct OpendalRangeReaderFactory {
    op: Operator,
    rt: Arc<Runtime>,
    block_size: usize,
    datacache: Option<DataCacheContext>,
    parquet_cache_policy: Option<ParquetReadCachePolicy>,
    profile: Option<RuntimeProfile>,
}

impl OpendalRangeReaderFactory {
    pub fn from_operator(op: Operator) -> Result<Self> {
        let rt = Runtime::new().context("init tokio runtime for opendal range reader")?;
        Ok(Self {
            op,
            rt: Arc::new(rt),
            block_size: DEFAULT_OPENDAL_READ_SIZE,
            datacache: None,
            parquet_cache_policy: None,
            profile: None,
        })
    }

    pub fn with_block_size(mut self, block_size: usize) -> Self {
        if block_size > 0 {
            self.block_size = block_size;
        }
        self
    }

    pub fn with_profile(mut self, profile: Option<RuntimeProfile>) -> Self {
        self.profile = profile;
        self
    }

    pub fn with_datacache_context(mut self, datacache: DataCacheContext) -> Self {
        self.datacache = Some(datacache);
        self
    }

    pub fn with_parquet_cache_policy(
        mut self,
        parquet_cache_policy: Option<ParquetReadCachePolicy>,
    ) -> Self {
        self.parquet_cache_policy = parquet_cache_policy;
        self
    }

    pub fn open(&self, path: &str) -> Result<OpendalRangeReader> {
        let reader = OpendalRangeReader::try_new(self.op.clone(), Arc::clone(&self.rt), path)?;
        Ok(reader
            .with_block_size(self.block_size)
            .with_datacache_context(self.datacache.clone())
            .with_parquet_cache_policy(self.parquet_cache_policy.clone())
            .with_profile(self.profile.clone()))
    }

    pub fn open_with_len(&self, path: &str, len: Option<u64>) -> Result<OpendalRangeReader> {
        let reader = if let Some(len) = len.filter(|v| *v > 0) {
            OpendalRangeReader::from_len(self.op.clone(), Arc::clone(&self.rt), path, len)
        } else {
            OpendalRangeReader::try_new(self.op.clone(), Arc::clone(&self.rt), path)?
        };
        Ok(reader
            .with_block_size(self.block_size)
            .with_datacache_context(self.datacache.clone())
            .with_parquet_cache_policy(self.parquet_cache_policy.clone())
            .with_profile(self.profile.clone()))
    }
}

#[derive(Clone, Debug)]
pub struct OpendalRangeReader {
    op: Operator,
    rt: Arc<Runtime>,
    path: String,
    len: u64,
    block_size: usize,
    modification_time: Option<i64>,
    datacache: Option<DataCacheContext>,
    parquet_cache_policy: Option<ParquetReadCachePolicy>,
    profile: Option<RuntimeProfile>,
    counters: Option<OpendalIoCounters>,
}

impl OpendalRangeReader {
    pub fn try_new(op: Operator, rt: Arc<Runtime>, path: impl Into<String>) -> Result<Self> {
        let path = path.into();
        let meta = rt
            .block_on(op.stat(&path))
            .with_context(|| format!("opendal stat: {path}"))?;
        let len = meta.content_length();
        let mtime = meta
            .last_modified()
            .map(|dt| dt.into_inner().as_second())
            .map(|ts| ts as i64);
        Ok(Self {
            op,
            rt,
            path,
            len,
            block_size: DEFAULT_OPENDAL_READ_SIZE,
            modification_time: mtime,
            datacache: None,
            parquet_cache_policy: None,
            profile: None,
            counters: None,
        })
    }

    /// Get file metadata (for cache key)
    pub fn get_file_meta(&self) -> Result<(u64, Option<i64>), String> {
        Ok((self.len, self.modification_time))
    }

    pub fn from_len(op: Operator, rt: Arc<Runtime>, path: impl Into<String>, len: u64) -> Self {
        // Try to get modification time, but don't fail if unavailable
        let path_str = path.into();
        let mtime = rt.block_on(op.stat(&path_str)).ok().and_then(|meta| {
            meta.last_modified()
                .map(|dt| dt.into_inner().as_second())
                .map(|ts| ts as i64)
        });
        Self {
            op,
            rt,
            path: path_str,
            len,
            block_size: DEFAULT_OPENDAL_READ_SIZE,
            modification_time: mtime,
            datacache: None,
            parquet_cache_policy: None,
            profile: None,
            counters: None,
        }
    }

    pub fn with_profile(mut self, profile: Option<RuntimeProfile>) -> Self {
        self.counters = profile.as_ref().map(OpendalIoCounters::new);
        self.profile = profile;
        self
    }

    pub fn with_datacache_context(mut self, datacache: Option<DataCacheContext>) -> Self {
        self.datacache = datacache;
        self
    }

    pub fn with_parquet_cache_policy(
        mut self,
        parquet_cache_policy: Option<ParquetReadCachePolicy>,
    ) -> Self {
        self.parquet_cache_policy = parquet_cache_policy;
        self
    }

    pub fn with_modification_time_override(mut self, modification_time: Option<i64>) -> Self {
        if modification_time.is_some() {
            self.modification_time = modification_time;
        }
        self
    }

    pub fn with_block_size(mut self, block_size: usize) -> Self {
        if block_size > 0 {
            self.block_size = block_size;
        }
        self
    }

    fn datacache_io_flags(&self) -> (bool, bool, bool) {
        if let Some(ctx) = self.datacache.as_ref() {
            let io = ctx.io_options();
            (
                io.enable_datacache,
                io.enable_populate_datacache,
                io.enable_datacache_async_populate_mode,
            )
        } else {
            (false, false, false)
        }
    }

    fn block_cache_for_read(&self) -> Option<Arc<crate::cache::BlockCache>> {
        self.datacache.as_ref().and_then(|ctx| ctx.block_cache())
    }

    fn parquet_page_cache_enabled(&self) -> bool {
        self.parquet_cache_policy
            .as_ref()
            .is_some_and(|p| p.enable_pagecache)
    }

    fn page_cache_evict_probability(&self) -> Option<u32> {
        self.parquet_cache_policy
            .as_ref()
            .and_then(|p| p.page_cache_evict_probability)
    }

    fn should_cache_parquet_page_read(&self, length: usize) -> bool {
        self.parquet_cache_policy
            .as_ref()
            .is_some_and(|p| p.should_cache_page_read(length))
    }
}

pub struct OpendalRead {
    op: Operator,
    rt: Arc<Runtime>,
    path: String,
    pos: u64,
    len: u64,
    block_size: usize,
    modification_time: Option<i64>,
    block_cache: Option<std::sync::Arc<crate::cache::block_cache::BlockCache>>,
    datacache: Option<DataCacheContext>,
    enable_datacache: bool,
    enable_populate_datacache: bool,
    enable_async_datacache: bool,
    buf: Bytes,
    buf_pos: usize,
    counters: Option<OpendalIoCounters>,
}

impl OpendalRead {
    fn new(
        op: Operator,
        rt: Arc<Runtime>,
        path: String,
        start: u64,
        len: u64,
        block_size: usize,
        modification_time: Option<i64>,
        block_cache: Option<std::sync::Arc<crate::cache::block_cache::BlockCache>>,
        datacache: Option<DataCacheContext>,
        enable_datacache: bool,
        enable_populate_datacache: bool,
        enable_async_datacache: bool,
        counters: Option<OpendalIoCounters>,
    ) -> Self {
        Self {
            op,
            rt,
            path,
            pos: start,
            len,
            block_size,
            modification_time,
            block_cache,
            datacache,
            enable_datacache,
            enable_populate_datacache,
            enable_async_datacache,
            buf: Bytes::new(),
            buf_pos: 0,
            counters,
        }
    }

    fn refill(&mut self) -> io::Result<()> {
        if self.pos >= self.len {
            self.buf = Bytes::new();
            self.buf_pos = 0;
            return Ok(());
        }

        if self.enable_datacache {
            if let Some(cache) = self.block_cache.as_ref() {
                let cache_stream = CacheInputStream::new(
                    Arc::clone(cache),
                    self.path.clone(),
                    self.modification_time,
                    self.len,
                    self.enable_datacache,
                    self.enable_populate_datacache,
                    self.enable_async_datacache,
                );
                let block_size = cache_stream.block_size();
                let block_id = self.pos / block_size;
                let block_start = block_id * block_size;
                let block_end = std::cmp::min(block_start + block_size, self.len);
                let block_len = (block_end - block_start) as usize;
                let op = self.op.clone();
                let rt = Arc::clone(&self.rt);
                let path = self.path.clone();
                let counters = self.counters.clone();
                let read = cache_stream.read_block(block_id, block_len, || {
                    let io_start = Instant::now();
                    let data = rt
                        .block_on(
                            op.read_with(&path)
                                .range(block_start..block_end)
                                .into_future(),
                        )
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                    let io_ns = io_start.elapsed().as_nanos();
                    if let Some(counters) = counters.as_ref() {
                        counters.record_remote_read(block_len, io_ns);
                    }
                    Ok(data.to_bytes())
                })?;
                if let Some(datacache) = self.datacache.as_ref() {
                    datacache.record_block_cache_result(self.counters.as_ref(), block_len, &read);
                }
                self.buf = read.bytes;
                self.buf_pos = (self.pos - block_start) as usize;
                return Ok(());
            }
        }

        let remaining = self.len - self.pos;
        let fetch_len = std::cmp::min(self.block_size as u64, remaining) as usize;
        let end = self.pos + fetch_len as u64;
        let range = self.pos..end;
        let io_start = Instant::now();
        let data = self
            .rt
            .block_on(self.op.read_with(&self.path).range(range).into_future())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let io_ns = io_start.elapsed().as_nanos();
        if let Some(counters) = self.counters.as_ref() {
            counters.record_remote_read(fetch_len, io_ns);
        }
        self.buf = data.to_bytes();
        self.buf_pos = 0;
        Ok(())
    }
}

impl Read for OpendalRead {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.pos >= self.len {
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

impl Length for OpendalRangeReader {
    fn len(&self) -> u64 {
        self.len
    }
}

impl ChunkReader for OpendalRangeReader {
    type T = OpendalRead;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        if start > self.len {
            return Err(ParquetError::EOF(format!(
                "Expected to read at offset {start}, while file has length {}",
                self.len
            )));
        }
        let (enable_datacache, enable_populate_datacache, enable_async_datacache) =
            self.datacache_io_flags();
        let block_cache = self.block_cache_for_read();
        Ok(OpendalRead::new(
            self.op.clone(),
            Arc::clone(&self.rt),
            self.path.clone(),
            start,
            self.len,
            self.block_size,
            self.modification_time,
            block_cache,
            self.datacache.clone(),
            enable_datacache,
            enable_populate_datacache,
            enable_async_datacache,
            self.counters.clone(),
        ))
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        let end = start
            .checked_add(length as u64)
            .ok_or_else(|| ParquetError::General("range overflow".to_string()))?;
        if start > self.len || end > self.len {
            return Err(ParquetError::EOF(format!(
                "Expected to read {} bytes at offset {}, while file has length {}",
                length, start, self.len
            )));
        }
        let (enable_datacache, enable_populate_datacache, enable_async_datacache) =
            self.datacache_io_flags();

        // Try to get from page cache
        if let Some(cached_data) = parquet_page_cache_get(
            self.parquet_page_cache_enabled(),
            &self.path,
            self.modification_time,
            self.len,
            start,
            length,
        ) {
            debug!(
                "parquet page cache HIT: path={}, offset={}, length={}",
                self.path, start, length
            );
            if let Some(datacache) = self.datacache.as_ref() {
                datacache.record_page_cache_hit(self.counters.as_ref(), length);
            }
            return Ok(cached_data);
        }

        // Cache miss, read from opendal
        let bytes = if enable_datacache {
            if let Some(cache) = self.block_cache_for_read() {
                let cache_stream = CacheInputStream::new(
                    cache,
                    self.path.clone(),
                    self.modification_time,
                    self.len,
                    enable_datacache,
                    enable_populate_datacache,
                    enable_async_datacache,
                );
                let block_size = cache_stream.block_size();
                let start_block = start / block_size;
                let end_block = (end.saturating_sub(1)) / block_size;
                let mut out = Vec::with_capacity(length);
                for block_id in start_block..=end_block {
                    let block_start = block_id * block_size;
                    let block_end = std::cmp::min(block_start + block_size, self.len);
                    let block_len = (block_end - block_start) as usize;
                    let counters = self.counters.clone();
                    let op = self.op.clone();
                    let rt = Arc::clone(&self.rt);
                    let path = self.path.clone();
                    let read = cache_stream
                        .read_block(block_id, block_len, || {
                            let io_start = Instant::now();
                            let data = rt
                                .block_on(
                                    op.read_with(&path)
                                        .range(block_start..block_end)
                                        .into_future(),
                                )
                                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                            let io_ns = io_start.elapsed().as_nanos();
                            if let Some(counters) = counters.as_ref() {
                                counters.record_remote_read(block_len, io_ns);
                            }
                            Ok(data.to_bytes())
                        })
                        .map_err(|e| ParquetError::General(e.to_string()))?;
                    if let Some(datacache) = self.datacache.as_ref() {
                        datacache.record_block_cache_result(
                            self.counters.as_ref(),
                            block_len,
                            &read,
                        );
                    }
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
                Bytes::from(out)
            } else {
                read_remote_range(self, start, end, length)?
            }
        } else {
            read_remote_range(self, start, end, length)?
        };

        // Insert into cache if enabled
        if self.should_cache_parquet_page_read(length) {
            parquet_page_cache_put(
                self.parquet_page_cache_enabled(),
                &self.path,
                self.modification_time,
                self.len,
                start,
                length,
                bytes.clone(),
                self.page_cache_evict_probability(),
            );
            debug!(
                "parquet page cached: path={}, offset={}, length={}",
                self.path, start, length
            );
        }

        Ok(bytes)
    }
}

fn read_remote_range(
    reader: &OpendalRangeReader,
    start: u64,
    end: u64,
    length: usize,
) -> ParquetResult<Bytes> {
    let io_start = Instant::now();
    let data = reader
        .rt
        .block_on(
            reader
                .op
                .read_with(&reader.path)
                .range(start..end)
                .into_future(),
        )
        .map_err(|e| ParquetError::General(e.to_string()))?;
    let io_ns = io_start.elapsed().as_nanos();
    if let Some(counters) = reader.counters.as_ref() {
        counters.record_remote_read(length, io_ns);
    }
    Ok(data.to_bytes())
}

impl OrcChunkReader for OpendalRangeReader {
    type T = OpendalRead;

    fn len(&self) -> u64 {
        self.len
    }

    fn get_read(&self, offset_from_start: u64) -> io::Result<Self::T> {
        if offset_from_start > self.len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "expected to read at offset {}, while file has length {}",
                    offset_from_start, self.len
                ),
            ));
        }
        let (enable_datacache, enable_populate_datacache, enable_async_datacache) =
            self.datacache_io_flags();
        let block_cache = self.block_cache_for_read();
        Ok(OpendalRead::new(
            self.op.clone(),
            Arc::clone(&self.rt),
            self.path.clone(),
            offset_from_start,
            self.len,
            self.block_size,
            self.modification_time,
            block_cache,
            self.datacache.clone(),
            enable_datacache,
            enable_populate_datacache,
            enable_async_datacache,
            self.counters.clone(),
        ))
    }
}

pub fn build_fs_operator(root: &str) -> Result<Operator> {
    let builder = opendal::services::Fs::default().root(root);
    let op = Operator::new(builder)
        .context("init opendal fs operator")?
        .finish();
    Ok(op)
}

pub async fn list_parquet_files(
    op: &Operator,
    prefix: &str,
    max_files: usize,
) -> Result<Vec<String>> {
    let mut out = Vec::new();

    let mut list_prefix = prefix.trim().to_string();
    if !list_prefix.is_empty() && !list_prefix.ends_with('/') {
        list_prefix.push('/');
    }

    let mut lister = op
        .lister_with(&list_prefix)
        .recursive(true)
        .await
        .context("opendal lister")?;
    while let Some(entry) = lister.try_next().await.context("opendal list next")? {
        let path = entry.path().to_string();
        if path.ends_with(".parquet") {
            out.push(path);
            if out.len() >= max_files {
                break;
            }
        }
    }

    Ok(out)
}

pub async fn probe_parquet(op: &Operator, path: &str) -> Result<ParquetProbe> {
    let data = op
        .read(path)
        .await
        .with_context(|| format!("opendal read: {path}"))?;
    let bytes = data.to_bytes();
    probe_parquet_bytes(path, bytes)
}
