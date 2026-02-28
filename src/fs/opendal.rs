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
use crate::common::file_identity::FileIdentity;
use crate::metrics;
use crate::runtime::global_async_runtime::data_runtime;
use crate::runtime::profile::{CounterRef, RuntimeProfile, clamp_u128_to_i64};
use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use opendal::Operator;
use std::future::IntoFuture;
use std::io::{self, Read};
use std::sync::Arc;
use std::time::Instant;
use tokio::runtime::Runtime;

pub const DEFAULT_OPENDAL_READ_SIZE: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct OpendalRemoteIoCounters {
    read_requests: CounterRef,
    bytes_read: CounterRef,
    io_timer: CounterRef,
    io_task_exec_time: CounterRef,
    scan_time: CounterRef,
}

impl OpendalRemoteIoCounters {
    fn new(profile: &RuntimeProfile) -> Self {
        Self {
            read_requests: profile.add_counter("ReadRequests", metrics::TUnit::UNIT),
            bytes_read: profile.add_counter("BytesRead", metrics::TUnit::BYTES),
            io_timer: profile.add_counter("IOTimer", metrics::TUnit::TIME_NS),
            io_task_exec_time: profile.add_counter("IOTaskExecTime", metrics::TUnit::TIME_NS),
            scan_time: profile.add_counter("ScanTime", metrics::TUnit::TIME_NS),
        }
    }

    pub(crate) fn record_remote_read(&self, length: usize, io_ns: u128) {
        self.read_requests.add(1);
        self.bytes_read.add(clamp_u128_to_i64(length as u128));
        let io_ns = clamp_u128_to_i64(io_ns);
        self.io_timer.add(io_ns);
        self.io_task_exec_time.add(io_ns);
        self.scan_time.add(io_ns);
    }
}

#[derive(Clone, Debug)]
pub struct OpendalRangeReaderFactory {
    op: Operator,
    rt: Arc<Runtime>,
    block_size: usize,
    profile: Option<RuntimeProfile>,
}

impl OpendalRangeReaderFactory {
    pub fn from_operator(op: Operator) -> Result<Self> {
        let rt = Arc::clone(
            data_runtime()
                .map_err(|e| anyhow!("{e}"))
                .context("init shared tokio runtime for opendal range reader")?,
        );
        Ok(Self {
            op,
            rt,
            block_size: DEFAULT_OPENDAL_READ_SIZE,
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

    pub fn open(&self, path: &str) -> Result<OpendalRangeReader> {
        let reader = OpendalRangeReader::try_new(self.op.clone(), Arc::clone(&self.rt), path)?;
        Ok(reader
            .with_block_size(self.block_size)
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
            .with_profile(self.profile.clone()))
    }
}

#[derive(Clone, Debug)]
pub struct OpendalRangeReader {
    op: Operator,
    rt: Arc<Runtime>,
    identity: FileIdentity,
    block_size: usize,
    profile: Option<RuntimeProfile>,
    counters: Option<OpendalRemoteIoCounters>,
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
            identity: FileIdentity::new(path, len, mtime),
            block_size: DEFAULT_OPENDAL_READ_SIZE,
            profile: None,
            counters: None,
        })
    }

    pub fn from_len(op: Operator, rt: Arc<Runtime>, path: impl Into<String>, len: u64) -> Self {
        Self {
            op,
            rt,
            identity: FileIdentity::new(path, len, None),
            block_size: DEFAULT_OPENDAL_READ_SIZE,
            profile: None,
            counters: None,
        }
    }

    pub fn with_profile(mut self, profile: Option<RuntimeProfile>) -> Self {
        self.counters = profile.as_ref().map(OpendalRemoteIoCounters::new);
        self.profile = profile;
        self
    }

    pub fn with_modification_time_override(mut self, modification_time: Option<i64>) -> Self {
        self.identity = self
            .identity
            .clone()
            .with_modification_time_override(modification_time);
        self
    }

    pub fn with_block_size(mut self, block_size: usize) -> Self {
        if block_size > 0 {
            self.block_size = block_size;
        }
        self
    }

    pub fn file_identity(&self) -> &FileIdentity {
        &self.identity
    }

    pub fn get_read(&self, start: u64) -> io::Result<OpendalRead> {
        if start > self.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "expected to read at offset {}, while file has length {}",
                    start,
                    self.len()
                ),
            ));
        }
        Ok(OpendalRead::new(self.clone(), start))
    }

    pub(crate) fn block_size(&self) -> usize {
        self.block_size
    }

    pub(crate) fn len(&self) -> u64 {
        self.identity.file_size()
    }

    pub(crate) fn profile(&self) -> Option<RuntimeProfile> {
        self.profile.clone()
    }

    pub(crate) fn read_remote_bytes(&self, start: u64, length: usize) -> io::Result<Bytes> {
        let end = start
            .checked_add(length as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "range overflow"))?;
        if start > self.len() || end > self.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "expected to read {} bytes at offset {}, while file has length {}",
                    length,
                    start,
                    self.len()
                ),
            ));
        }
        self.read_remote_range(start, end)
    }

    pub(crate) fn read_remote_range(&self, start: u64, end: u64) -> io::Result<Bytes> {
        let length = usize::try_from(end.saturating_sub(start)).unwrap_or(usize::MAX);
        if length == 0 {
            return Ok(Bytes::new());
        }
        let io_start = Instant::now();
        let data = self
            .rt
            .block_on(
                self.op
                    .read_with(self.identity.path())
                    .range(start..end)
                    .into_future(),
            )
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!(
                        "opendal read failed: path={} range={}..{} len={} err={}",
                        self.identity.path(),
                        start,
                        end,
                        length,
                        e
                    ),
                )
            })?;
        let io_ns = io_start.elapsed().as_nanos();
        if let Some(counters) = self.counters.as_ref() {
            counters.record_remote_read(length, io_ns);
        }
        Ok(data.to_bytes())
    }
}

pub struct OpendalRead {
    reader: OpendalRangeReader,
    pos: u64,
    buf: Bytes,
    buf_pos: usize,
}

impl OpendalRead {
    fn new(reader: OpendalRangeReader, start: u64) -> Self {
        Self {
            reader,
            pos: start,
            buf: Bytes::new(),
            buf_pos: 0,
        }
    }

    fn refill(&mut self) -> io::Result<()> {
        if self.pos >= self.reader.len() {
            self.buf = Bytes::new();
            self.buf_pos = 0;
            return Ok(());
        }
        let remaining = self.reader.len() - self.pos;
        let fetch_len = std::cmp::min(self.reader.block_size() as u64, remaining) as usize;
        self.buf = self.reader.read_remote_bytes(self.pos, fetch_len)?;
        self.buf_pos = 0;
        Ok(())
    }
}

impl Read for OpendalRead {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.pos >= self.reader.len() {
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

pub fn build_fs_operator(root: &str) -> Result<Operator> {
    let builder = opendal::services::Fs::default().root(root);
    let op = Operator::new(builder)
        .context("init opendal fs operator")?
        .finish();
    Ok(op)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Read;

    use super::{OpendalRangeReaderFactory, build_fs_operator};

    #[test]
    fn open_with_len_keeps_mtime_empty_until_override() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        fs::write(temp_dir.path().join("sample.bin"), b"hello world").expect("write fixture");

        let op = build_fs_operator(temp_dir.path().to_str().expect("temp dir path"))
            .expect("build fs operator");
        let factory = OpendalRangeReaderFactory::from_operator(op).expect("reader factory");
        let reader = factory
            .open_with_len("sample.bin", Some(11))
            .expect("open with len");
        assert_eq!(reader.file_identity().modification_time(), None);

        let zero_override = reader.clone().with_modification_time_override(Some(0));
        assert_eq!(zero_override.file_identity().modification_time(), Some(0));

        let positive_override = reader.with_modification_time_override(Some(123));
        assert_eq!(
            positive_override.file_identity().modification_time(),
            Some(123)
        );
    }

    #[test]
    fn get_read_uses_raw_remote_reads() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        fs::write(temp_dir.path().join("sample.bin"), b"abcdefghij").expect("write fixture");

        let op = build_fs_operator(temp_dir.path().to_str().expect("temp dir path"))
            .expect("build fs operator");
        let factory = OpendalRangeReaderFactory::from_operator(op).expect("reader factory");
        let reader = factory
            .open_with_len("sample.bin", Some(10))
            .expect("open with len");
        let mut read = reader.get_read(2).expect("get read");

        let mut buf = Vec::new();
        read.read_to_end(&mut buf).expect("read remaining bytes");
        assert_eq!(buf, b"cdefghij");
    }
}
