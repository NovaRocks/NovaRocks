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
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use bytes::Bytes;
use tokio::runtime::{Handle, RuntimeFlavor};
use tokio::task::JoinHandle;

use crate::{FileError, FileResult};

#[derive(Clone, Default)]
pub struct FileCancellation {
    cancelled: Arc<AtomicBool>,
    connector_cancellation: Option<Arc<dyn novarocks_spi::connector::ConnectorCancellation>>,
    deadline: Option<Instant>,
}

impl FileCancellation {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bind file operations to the cancellation and deadline of one admitted
    /// connector request without spawning a polling task or retaining the
    /// request's resource ledger.
    pub fn from_connector_request(
        request: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            connector_cancellation: Some(Arc::clone(request.cancellation())),
            deadline: Some(request.deadline()),
        }
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
            || self
                .connector_cancellation
                .as_ref()
                .is_some_and(|cancellation| cancellation.is_cancelled())
            || self
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
    }

    pub fn check(&self) -> FileResult<()> {
        if self.cancelled.load(Ordering::Acquire)
            || self
                .connector_cancellation
                .as_ref()
                .is_some_and(|cancellation| cancellation.is_cancelled())
        {
            Err(FileError::cancelled("file operation cancelled"))
        } else if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            Err(FileError::deadline("file operation deadline elapsed"))
        } else {
            Ok(())
        }
    }
}

impl std::fmt::Debug for FileCancellation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileCancellation")
            .field("cancelled", &self.is_cancelled())
            .field("connector_bound", &self.connector_cancellation.is_some())
            .field("deadline", &self.deadline)
            .finish()
    }
}

pub type FileTaskFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
pub type FileBytesFuture = Pin<Box<dyn Future<Output = FileResult<Bytes>> + Send + 'static>>;
pub type FileU64Future = Pin<Box<dyn Future<Output = FileResult<u64>> + Send + 'static>>;

pub trait FileIoRuntime: Send + Sync {
    fn block_on_bytes(&self, future: FileBytesFuture) -> FileResult<Bytes>;
    fn block_on_u64(&self, future: FileU64Future) -> FileResult<u64>;
}

pub trait FileTaskSpawner: Send + Sync {
    fn spawn(&self, task: FileTaskFuture) -> FileResult<FileTask>;
}

pub struct FileTask {
    join: Option<JoinHandle<()>>,
}

impl FileTask {
    pub fn new(join: JoinHandle<()>) -> Self {
        Self { join: Some(join) }
    }

    pub fn abort(&mut self) {
        if let Some(join) = self.join.take() {
            join.abort();
        }
    }

    pub fn is_finished(&self) -> bool {
        self.join.as_ref().is_none_or(JoinHandle::is_finished)
    }
}

impl Drop for FileTask {
    fn drop(&mut self) {
        self.abort();
    }
}

#[derive(Clone)]
pub struct TokioFileIoRuntime {
    handle: Handle,
}

impl TokioFileIoRuntime {
    pub fn new(handle: Handle) -> Self {
        Self { handle }
    }
}

impl FileIoRuntime for TokioFileIoRuntime {
    fn block_on_bytes(&self, future: FileBytesFuture) -> FileResult<Bytes> {
        block_on(&self.handle, future)?
    }

    fn block_on_u64(&self, future: FileU64Future) -> FileResult<u64> {
        block_on(&self.handle, future)?
    }
}

/// File readers are synchronous pipeline callbacks, but their object-store
/// operations are asynchronous. Production data runtimes are multi-threaded;
/// yield the worker before driving the explicitly injected handle so a reader
/// reached from that runtime never nests `Handle::block_on` and panics.
fn block_on<T>(handle: &Handle, future: impl Future<Output = T>) -> FileResult<T> {
    match Handle::try_current() {
        Ok(current) if current.runtime_flavor() == RuntimeFlavor::CurrentThread => {
            Err(FileError::new(
                crate::FileErrorKind::Internal,
                "filesystem I/O cannot synchronously bridge from a current-thread Tokio runtime",
            ))
        }
        Ok(_) => Ok(tokio::task::block_in_place(|| handle.block_on(future))),
        Err(_) => Ok(handle.block_on(future)),
    }
}

#[derive(Clone)]
pub struct TokioFileTaskSpawner {
    handle: Handle,
}

impl TokioFileTaskSpawner {
    pub fn new(handle: Handle) -> Self {
        Self { handle }
    }
}

impl FileTaskSpawner for TokioFileTaskSpawner {
    fn spawn(&self, task: FileTaskFuture) -> FileResult<FileTask> {
        Ok(FileTask::new(self.handle.spawn(task)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use super::*;
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorRequestContext, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };

    struct ToggleCancellation(AtomicBool);

    impl ConnectorCancellation for ToggleCancellation {
        fn is_cancelled(&self) -> bool {
            self.0.load(Ordering::Acquire)
        }
    }

    #[test]
    fn connector_request_cancellation_and_deadline_reach_file_operations() {
        let upstream = Arc::new(ToggleCancellation(AtomicBool::new(false)));
        let request = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            upstream.clone(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("request");
        let cancellation = FileCancellation::from_connector_request(&request);
        cancellation.check().expect("active request");

        upstream.0.store(true, Ordering::Release);
        assert_eq!(
            cancellation.check().expect_err("cancelled request").kind(),
            crate::FileErrorKind::Cancelled
        );

        let expired = ConnectorRequestContext::try_new(
            Instant::now() - Duration::from_millis(1),
            Arc::new(ToggleCancellation(AtomicBool::new(false))),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("expired request");
        assert_eq!(
            FileCancellation::from_connector_request(&expired)
                .check()
                .expect_err("expired request")
                .kind(),
            crate::FileErrorKind::DeadlineExceeded
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn file_io_runtime_bridges_from_its_runtime_worker() {
        let runtime = Arc::new(TokioFileIoRuntime::new(Handle::current()));

        assert_eq!(
            runtime
                .block_on_bytes(Box::pin(async { Ok(Bytes::from_static(b"bytes")) }))
                .expect("bytes bridge"),
            Bytes::from_static(b"bytes")
        );
        assert_eq!(
            runtime
                .block_on_u64(Box::pin(async { Ok(7_u64) }))
                .expect("u64 bridge"),
            7
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn file_io_runtime_rejects_a_current_thread_runtime_without_panicking() {
        let runtime = TokioFileIoRuntime::new(Handle::current());

        let error = runtime
            .block_on_u64(Box::pin(async { Ok(7_u64) }))
            .expect_err("current-thread bridge must fail closed");
        assert_eq!(error.kind(), crate::FileErrorKind::Internal);
    }
}
