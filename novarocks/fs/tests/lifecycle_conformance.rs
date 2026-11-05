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

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use novarocks_fs::{
    FileBytesFuture, FileCancellation, FileErrorKind, FileFormat, FileIoFuture, FileIoJob,
    FileIoRuntime, FileIoSpawner, FileProjection, FileResult, FileU64Future, open_file_reader,
};

use common::Fixture;

fn open_error(request: novarocks_fs::FileReadRequest) -> novarocks_fs::FileError {
    match open_file_reader(request) {
        Ok(_) => panic!("expected file reader open to fail"),
        Err(error) => error,
    }
}

struct ControlledIo {
    runtime: tokio::runtime::Runtime,
    cancellation: FileCancellation,
    armed: AtomicBool,
    delay_ms: AtomicU64,
    calls: AtomicU64,
}

impl ControlledIo {
    fn new(cancellation: FileCancellation) -> Arc<Self> {
        Arc::new(Self {
            runtime: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("test runtime"),
            cancellation,
            armed: AtomicBool::new(false),
            delay_ms: AtomicU64::new(0),
            calls: AtomicU64::new(0),
        })
    }

    fn arm_cancel(&self) {
        self.armed.store(true, Ordering::Release);
    }

    fn set_delay(&self, delay: Duration) {
        self.delay_ms
            .store(delay.as_millis() as u64, Ordering::Release);
    }
}

impl FileIoRuntime for ControlledIo {
    fn block_on_bytes(&self, future: FileBytesFuture) -> FileResult<bytes::Bytes> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let delay = self.delay_ms.load(Ordering::Acquire);
        if delay > 0 {
            std::thread::sleep(Duration::from_millis(delay));
        }
        if self.armed.load(Ordering::Acquire) {
            self.cancellation.cancel();
        }
        self.runtime.block_on(future)
    }

    fn block_on_u64(&self, future: FileU64Future) -> FileResult<u64> {
        self.runtime.block_on(future)
    }
}

impl FileIoSpawner for ControlledIo {
    fn spawn(&self, task: FileIoFuture) -> FileResult<FileIoJob> {
        Ok(FileIoJob::new(self.runtime.spawn(task)))
    }

    fn spawn_detached_blocking(&self, job: Box<dyn FnOnce() + Send + 'static>) {
        self.runtime.spawn_blocking(job);
    }
}

fn install_controlled_io(request: &mut novarocks_fs::FileReadRequest) -> Arc<ControlledIo> {
    let io = ControlledIo::new(request.context.cancellation.clone());
    let runtime: Arc<dyn FileIoRuntime> = io.clone();
    let io_spawner: Arc<dyn FileIoSpawner> = io.clone();
    request.context.runtime = runtime;
    request.context.io_spawner = io_spawner;
    io
}

#[test]
fn cancelled_before_open_returns_cancelled() {
    let fixture = Fixture::parquet();
    let request = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    request.context.cancellation.cancel();
    assert_eq!(open_error(request).kind(), FileErrorKind::Cancelled);
}

#[test]
fn cancellation_during_metadata_io_returns_cancelled() {
    let fixture = Fixture::parquet();
    let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    let io = install_controlled_io(&mut request);
    io.arm_cancel();
    assert_eq!(open_error(request).kind(), FileErrorKind::Cancelled);
    assert!(io.calls.load(Ordering::Relaxed) > 0);
}

#[test]
fn cancellation_after_open_before_buffered_decode_returns_cancelled() {
    let fixture = Fixture::parquet();
    let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    let io = install_controlled_io(&mut request);
    let mut reader = open_file_reader(request).expect("open reader");
    // The Parquet session may already hold the next run's input at open.
    // Cancellation must still win before a buffered batch is decoded.
    io.cancellation.cancel();
    assert_eq!(
        reader
            .next_batch()
            .expect_err("cancelled before buffered decode")
            .kind(),
        FileErrorKind::Cancelled
    );
}

#[test]
fn deadline_is_checked_after_remote_io() {
    let fixture = Fixture::parquet();
    let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    request.context.deadline = Some(Instant::now() + Duration::from_millis(5));
    let io = install_controlled_io(&mut request);
    io.set_delay(Duration::from_millis(20));
    assert_eq!(open_error(request).kind(), FileErrorKind::DeadlineExceeded);
}

#[test]
fn close_is_idempotent() {
    let fixture = Fixture::parquet();
    let mut reader = open_file_reader(fixture.request(
        FileFormat::Parquet,
        FileProjection::All,
        1024,
        1024 * 1024,
    ))
    .expect("open reader");
    reader.close().expect("first close");
    reader.close().expect("second close");
    assert!(reader.next_batch().expect("terminal read").is_none());
}

#[test]
fn terminal_reader_starts_no_new_io() {
    let fixture = Fixture::parquet();
    let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    let io = install_controlled_io(&mut request);
    let mut reader = open_file_reader(request).expect("open reader");
    reader.close().expect("close");
    let calls = io.calls.load(Ordering::Relaxed);
    assert!(reader.next_batch().expect("terminal read").is_none());
    assert!(
        reader
            .next_batch()
            .expect("repeated terminal read")
            .is_none()
    );
    assert_eq!(io.calls.load(Ordering::Relaxed), calls);
}
