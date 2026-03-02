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
use crate::novarocks_logging::debug;
use crate::runtime::global_async_runtime::data_runtime;
use crate::runtime::profile::{CounterRef, RuntimeProfile, clamp_u128_to_i64};
use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use opendal::Operator;
use std::future::IntoFuture;
use std::io::{self, Read};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;
use tokio::runtime::Runtime;

pub const DEFAULT_OPENDAL_READ_SIZE: usize = 8 * 1024 * 1024;
const REMOTE_READ_CONCURRENCY_LOG_INTERVAL_MS: u64 = 30_000;

#[derive(Debug)]
struct RemoteReadConcurrencyWindow {
    window_start: Instant,
    started: u64,
    succeeded: u64,
    failed: u64,
    requested_bytes_total: u64,
    requested_bytes_succeeded: u64,
    io_ns: u128,
    peak: usize,
}

impl RemoteReadConcurrencyWindow {
    fn new(now: Instant) -> Self {
        Self {
            window_start: now,
            started: 0,
            succeeded: 0,
            failed: 0,
            requested_bytes_total: 0,
            requested_bytes_succeeded: 0,
            io_ns: 0,
            peak: 0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct RemoteReadConcurrencySummary {
    started: u64,
    succeeded: u64,
    failed: u64,
    requested_bytes_total: u64,
    requested_bytes_succeeded: u64,
    avg_latency_ms: u64,
    peak_in_flight: usize,
    current_in_flight: usize,
    global_peak_in_flight: usize,
    window_ms: u64,
}

#[derive(Debug)]
struct RemoteReadConcurrencyTracker {
    in_flight: AtomicUsize,
    global_peak: AtomicUsize,
    window: Mutex<RemoteReadConcurrencyWindow>,
}

impl RemoteReadConcurrencyTracker {
    fn new() -> Self {
        Self {
            in_flight: AtomicUsize::new(0),
            global_peak: AtomicUsize::new(0),
            window: Mutex::new(RemoteReadConcurrencyWindow::new(Instant::now())),
        }
    }

    fn on_start(&self) -> usize {
        let current = self.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        update_peak_atomic(&self.global_peak, current);
        if let Ok(mut window) = self.window.lock() {
            window.started = window.started.saturating_add(1);
            if current > window.peak {
                window.peak = current;
            }
        }
        current
    }

    fn on_finish(
        &self,
        requested_len: usize,
        io_ns: u128,
        success: bool,
    ) -> Option<RemoteReadConcurrencySummary> {
        let current = self
            .in_flight
            .fetch_sub(1, Ordering::Relaxed)
            .saturating_sub(1);
        let now = Instant::now();
        let mut window = self.window.lock().ok()?;
        window.requested_bytes_total = window
            .requested_bytes_total
            .saturating_add(u64::try_from(requested_len).unwrap_or(u64::MAX));
        if success {
            window.succeeded = window.succeeded.saturating_add(1);
            window.requested_bytes_succeeded = window
                .requested_bytes_succeeded
                .saturating_add(u64::try_from(requested_len).unwrap_or(u64::MAX));
        } else {
            window.failed = window.failed.saturating_add(1);
        }
        window.io_ns = window.io_ns.saturating_add(io_ns);
        let elapsed_ms =
            u64::try_from(now.duration_since(window.window_start).as_millis()).unwrap_or(u64::MAX);
        if elapsed_ms < REMOTE_READ_CONCURRENCY_LOG_INTERVAL_MS {
            return None;
        }
        let finished = window.succeeded.saturating_add(window.failed);
        let avg_latency_ms = if finished > 0 {
            let avg_ns = window.io_ns / u128::from(finished);
            u64::try_from(avg_ns / 1_000_000).unwrap_or(u64::MAX)
        } else {
            0
        };
        let summary = RemoteReadConcurrencySummary {
            started: window.started,
            succeeded: window.succeeded,
            failed: window.failed,
            requested_bytes_total: window.requested_bytes_total,
            requested_bytes_succeeded: window.requested_bytes_succeeded,
            avg_latency_ms,
            peak_in_flight: window.peak,
            current_in_flight: current,
            global_peak_in_flight: self.global_peak.load(Ordering::Relaxed),
            window_ms: elapsed_ms,
        };
        *window = RemoteReadConcurrencyWindow::new(now);
        Some(summary)
    }
}

fn update_peak_atomic(peak: &AtomicUsize, current: usize) {
    let mut observed = peak.load(Ordering::Relaxed);
    while current > observed {
        match peak.compare_exchange_weak(observed, current, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(actual) => observed = actual,
        }
    }
}

fn remote_read_concurrency_tracker() -> &'static RemoteReadConcurrencyTracker {
    static TRACKER: OnceLock<RemoteReadConcurrencyTracker> = OnceLock::new();
    TRACKER.get_or_init(RemoteReadConcurrencyTracker::new)
}

#[derive(Clone, Debug)]
pub(crate) struct OpendalRemoteIoCounters {
    read_requests: CounterRef,
    bytes_read: CounterRef,
    io_timer: CounterRef,
    io_task_exec_time: CounterRef,
    scan_time: CounterRef,
    remote_read_in_flight_peak: CounterRef,
    observed_remote_read_peak: Arc<AtomicI64>,
}

impl OpendalRemoteIoCounters {
    fn new(profile: &RuntimeProfile) -> Self {
        Self {
            read_requests: profile.add_counter("ReadRequests", metrics::TUnit::UNIT),
            bytes_read: profile.add_counter("BytesRead", metrics::TUnit::BYTES),
            io_timer: profile.add_counter("IOTimer", metrics::TUnit::TIME_NS),
            io_task_exec_time: profile.add_counter("IOTaskExecTime", metrics::TUnit::TIME_NS),
            scan_time: profile.add_counter("ScanTime", metrics::TUnit::TIME_NS),
            remote_read_in_flight_peak: profile
                .add_counter("RemoteReadInFlightPeak", metrics::TUnit::UNIT),
            observed_remote_read_peak: Arc::new(AtomicI64::new(0)),
        }
    }

    pub(crate) fn record_remote_in_flight_peak(&self, in_flight: usize) {
        let target = i64::try_from(in_flight).unwrap_or(i64::MAX);
        let mut observed = self.observed_remote_read_peak.load(Ordering::Relaxed);
        while target > observed {
            match self.observed_remote_read_peak.compare_exchange_weak(
                observed,
                target,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.remote_read_in_flight_peak.add(target - observed);
                    break;
                }
                Err(actual) => observed = actual,
            }
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
        let tracker = remote_read_concurrency_tracker();
        let in_flight = tracker.on_start();
        if let Some(counters) = self.counters.as_ref() {
            counters.record_remote_in_flight_peak(in_flight);
        }
        let io_start = Instant::now();
        let data_result = self.rt.block_on(
            self.op
                .read_with(self.identity.path())
                .range(start..end)
                .into_future(),
        );
        let io_ns = io_start.elapsed().as_nanos();
        if let Some(summary) = tracker.on_finish(length, io_ns, data_result.is_ok()) {
            debug!(
                "opendal read concurrency summary: window_ms={} started={} succeeded={} failed={} requested_bytes_total={} requested_bytes_succeeded={} avg_latency_ms={} peak_in_flight={} current_in_flight={} global_peak_in_flight={}",
                summary.window_ms,
                summary.started,
                summary.succeeded,
                summary.failed,
                summary.requested_bytes_total,
                summary.requested_bytes_succeeded,
                summary.avg_latency_ms,
                summary.peak_in_flight,
                summary.current_in_flight,
                summary.global_peak_in_flight
            );
        }
        let data = data_result.map_err(|e| {
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
