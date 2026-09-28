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
use std::time::Instant;

use bytes::Bytes;
use tokio::runtime::{Handle, RuntimeFlavor};
use tokio::task::JoinHandle;

use crate::{FileError, FileResult};

#[derive(Clone, Default)]
pub struct FileCancellation {
    local_stop: novarocks_spi::connector::ConnectorStopOwner,
    connector_stop: Option<novarocks_spi::connector::ConnectorStopView>,
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
            local_stop: novarocks_spi::connector::ConnectorStopOwner::new(),
            connector_stop: Some(request.stop().clone()),
            deadline: Some(request.deadline()),
        }
    }

    pub fn cancel(&self) {
        self.local_stop.request_stop();
    }

    /// One operation gets an independently stoppable child of its source.
    /// Stopping the source still reaches every child.
    pub fn child(&self) -> Self {
        Self {
            local_stop: self.local_stop.child(),
            connector_stop: self.connector_stop.clone(),
            deadline: self.deadline,
        }
    }

    /// Keep the earlier absolute deadline when a file context adds its own.
    pub fn with_deadline(mut self, deadline: Option<Instant>) -> Self {
        self.deadline = match (self.deadline, deadline) {
            (Some(existing), Some(additional)) => Some(existing.min(additional)),
            (existing, additional) => existing.or(additional),
        };
        self
    }

    pub fn is_cancelled(&self) -> bool {
        self.local_stop.is_stopped()
            || self
                .connector_stop
                .as_ref()
                .is_some_and(novarocks_spi::connector::ConnectorStopView::is_stopped)
            || self
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
    }

    pub fn check(&self) -> FileResult<()> {
        if self.local_stop.is_stopped()
            || self
                .connector_stop
                .as_ref()
                .is_some_and(novarocks_spi::connector::ConnectorStopView::is_stopped)
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

    /// Wake on a local stop or on the admitted connector attempt's stop.
    pub async fn stopped(&self) {
        let local = self.local_stop.view();
        if let Some(connector) = &self.connector_stop {
            let _ =
                futures::future::select(Box::pin(local.stopped()), Box::pin(connector.stopped()))
                    .await;
        } else {
            local.stopped().await;
        }
    }

    /// Wait for a stop or the original absolute deadline. The typed error
    /// still comes from `check`, so a concurrent stop keeps cancel precedence.
    pub async fn ended(&self) -> FileError {
        let deadline_elapsed = if let Some(deadline) = self.deadline {
            matches!(
                futures::future::select(
                    Box::pin(self.stopped()),
                    Box::pin(tokio::time::sleep_until(tokio::time::Instant::from_std(
                        deadline,
                    ))),
                )
                .await,
                futures::future::Either::Right(_)
            )
        } else {
            self.stopped().await;
            false
        };
        self.check().err().unwrap_or_else(|| {
            if deadline_elapsed {
                FileError::deadline("file operation deadline elapsed")
            } else {
                FileError::cancelled("file operation cancelled")
            }
        })
    }
}

impl std::fmt::Debug for FileCancellation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileCancellation")
            .field("cancelled", &self.is_cancelled())
            .field("stop_bound", &self.connector_stop.is_some())
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

    /// Run one synchronous job somewhere other than the caller's thread.
    ///
    /// Credential refreshes use this. Filesystem reads are driven synchronously
    /// from scan threads, and vended credentials across a cluster commonly
    /// expire together, so a refresh that borrowed its caller's thread could
    /// park a whole scan pool at once (CAD-1 D3). There is no default: an
    /// implementation that quietly ran the job inline would reintroduce exactly
    /// that failure while still compiling.
    fn spawn_detached_blocking(&self, job: Box<dyn FnOnce() + Send + 'static>);
}

pub struct FileTask {
    join: Option<JoinHandle<()>>,
}

impl FileTask {
    pub fn new(join: JoinHandle<()>) -> Self {
        Self { join: Some(join) }
    }

    pub fn abort(&mut self) {
        if let Some(join) = self.join.as_ref() {
            join.abort();
        }
    }

    /// Wait for the spawned work to exit. A requested abort is not itself an
    /// exit receipt, so the handle stays in the task through this await: a
    /// wait that is dropped leaves the task owning, and aborting on drop, its
    /// work, and a later wait still observes the same exit.
    pub async fn drain(&mut self) -> FileResult<()> {
        let Some(join) = self.join.as_mut() else {
            return Ok(());
        };
        let outcome = join.await;
        self.join = None;
        match outcome {
            Ok(()) => Ok(()),
            Err(error) if error.is_cancelled() => {
                Err(FileError::cancelled("file task was cancelled before drain"))
            }
            Err(error) => Err(FileError::with_source(
                crate::FileErrorKind::Internal,
                "file task failed before drain",
                error,
            )),
        }
    }

    pub async fn abort_and_drain(&mut self) -> FileResult<()> {
        self.abort();
        match self.drain().await {
            Err(error) if error.kind() == crate::FileErrorKind::Cancelled => Ok(()),
            result => result,
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

    fn spawn_detached_blocking(&self, job: Box<dyn FnOnce() + Send + 'static>) {
        // `spawn_blocking` rather than `spawn`: an acquisition is a synchronous
        // provider call, so an async worker would move the stall, not remove it.
        self.handle.spawn_blocking(move || job());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use super::*;
    use novarocks_spi::connector::{
        ConnectorRequestContext, ConnectorStopOwner, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };

    #[test]
    fn connector_request_cancellation_and_deadline_reach_file_operations() {
        let upstream = ConnectorStopOwner::new();
        let request = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            upstream.view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("request");
        let cancellation = FileCancellation::from_connector_request(&request);
        cancellation.check().expect("active request");

        upstream.request_stop();
        assert_eq!(
            cancellation.check().expect_err("cancelled request").kind(),
            crate::FileErrorKind::Cancelled
        );

        let expired = ConnectorRequestContext::try_new(
            Instant::now() - Duration::from_millis(1),
            ConnectorStopOwner::new().view(),
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

    #[test]
    fn file_context_deadline_cannot_extend_the_admitted_deadline() {
        let admitted = Instant::now() + Duration::from_secs(30);
        let request = ConnectorRequestContext::try_new(
            admitted,
            ConnectorStopOwner::new().view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("request");
        let file = FileCancellation::from_connector_request(&request);
        assert_eq!(
            file.clone()
                .with_deadline(Some(admitted + Duration::from_secs(30)))
                .deadline,
            Some(admitted)
        );
        let earlier = Instant::now() - Duration::from_millis(1);
        assert_eq!(
            file.with_deadline(Some(earlier))
                .check()
                .expect_err("file context expired")
                .kind(),
            crate::FileErrorKind::DeadlineExceeded
        );
    }

    #[tokio::test]
    async fn admitted_connector_stop_wakes_file_waiter() {
        let owner = novarocks_spi::connector::ConnectorStopOwner::new();
        let request = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            owner.view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("request");
        let cancellation = FileCancellation::from_connector_request(&request);
        let waiter = cancellation.stopped();
        owner.request_stop();
        waiter.await;
        assert_eq!(
            cancellation
                .check()
                .expect_err("stopped file request")
                .kind(),
            crate::FileErrorKind::Cancelled
        );
    }

    #[tokio::test]
    async fn original_deadline_wakes_file_waiter_without_a_stop_request() {
        let request = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_millis(10),
            ConnectorStopOwner::new().view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("request");
        let cancellation = FileCancellation::from_connector_request(&request);
        assert_eq!(
            cancellation.ended().await.kind(),
            crate::FileErrorKind::DeadlineExceeded
        );
    }

    #[tokio::test]
    async fn local_file_stop_wakes_waiter_without_stopping_connector_parent() {
        let owner = novarocks_spi::connector::ConnectorStopOwner::new();
        let request = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            owner.view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("request");
        let cancellation = FileCancellation::from_connector_request(&request);
        let waiter = cancellation.stopped();
        cancellation.cancel();
        waiter.await;
        assert!(!owner.is_stopped());
    }

    #[test]
    fn stopping_one_file_operation_preserves_its_source_and_sibling() {
        let source = FileCancellation::new();
        let first = source.child();
        let second = source.child();
        first.cancel();
        assert!(first.is_cancelled());
        assert!(!source.is_cancelled());
        assert!(!second.is_cancelled());
        source.cancel();
        assert!(second.is_cancelled());
    }

    #[tokio::test]
    async fn abort_and_drain_waits_for_the_started_task_to_exit() {
        struct ExitFlag(Arc<AtomicBool>);
        impl Drop for ExitFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let exited = Arc::new(AtomicBool::new(false));
        let (started, receiver) = tokio::sync::oneshot::channel();
        let flag = Arc::clone(&exited);
        let mut task = FileTask::new(tokio::spawn(async move {
            let _flag = ExitFlag(flag);
            let _ = started.send(());
            futures::future::pending::<()>().await;
        }));
        receiver.await.expect("task started");
        task.abort_and_drain().await.expect("task drained");
        assert!(exited.load(Ordering::Acquire));
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
