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

//! Backend-owned filesystem I/O runtime and actual-work drain.

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use novarocks_fs::{
    FileBytesFuture, FileError, FileErrorKind, FileIoRuntime, FileRangeService, FileResult,
    FileTask, FileTaskFuture, FileTaskSpawner, FileU64Future, TokioFileIoRuntime,
    TokioFileTaskSpawner,
};
use tokio::runtime::{Handle, Runtime, RuntimeFlavor};
use tokio::sync::Notify;

use crate::app_config::RuntimeConfig;

#[derive(Default)]
struct FlightState {
    inner: Mutex<FlightInner>,
    changed: Notify,
    stop: novarocks_spi::connector::ConnectorStopOwner,
}

#[derive(Default)]
struct FlightInner {
    closed: bool,
    active: usize,
}

impl FlightState {
    fn enter(self: &Arc<Self>) -> FileResult<FlightGuard> {
        let mut inner = self.inner.lock().expect("scan I/O flight lock");
        if inner.closed {
            return Err(FileError::cancelled("backend scan I/O admission is closed"));
        }
        inner.active += 1;
        Ok(FlightGuard(Arc::clone(self)))
    }

    fn close(&self) {
        self.inner.lock().expect("scan I/O flight lock").closed = true;
        self.stop.request_stop();
    }

    async fn drain(&self) {
        loop {
            let notified = self.changed.notified();
            if self.inner.lock().expect("scan I/O flight lock").active == 0 {
                return;
            }
            notified.await;
        }
    }
}

struct FlightGuard(Arc<FlightState>);

impl Drop for FlightGuard {
    fn drop(&mut self) {
        let mut inner = self.0.inner.lock().expect("scan I/O flight lock");
        inner.active -= 1;
        if inner.active == 0 {
            self.0.changed.notify_one();
        }
    }
}

struct TrackedFileIoRuntime {
    inner: TokioFileIoRuntime,
    handle: Handle,
    flight: Arc<FlightState>,
}

impl FileIoRuntime for TrackedFileIoRuntime {
    fn block_on_bytes(&self, future: FileBytesFuture) -> FileResult<bytes::Bytes> {
        ensure_blocking_bridge_available()?;
        let flight = self.flight.enter()?;
        let stop = self.flight.stop.view();
        let task = self.handle.spawn(async move {
            let _flight = flight;
            tokio::select! {
                value = future => value,
                _ = stop.stopped() => Err(FileError::cancelled("backend scan I/O is stopping")),
            }
        });
        self.inner
            .block_on_bytes(Box::pin(async move { map_scan_join(task.await) }))
    }

    fn block_on_u64(&self, future: FileU64Future) -> FileResult<u64> {
        ensure_blocking_bridge_available()?;
        let flight = self.flight.enter()?;
        let stop = self.flight.stop.view();
        let task = self.handle.spawn(async move {
            let _flight = flight;
            tokio::select! {
                value = future => value,
                _ = stop.stopped() => Err(FileError::cancelled("backend scan I/O is stopping")),
            }
        });
        self.inner
            .block_on_u64(Box::pin(async move { map_scan_join(task.await) }))
    }
}

fn ensure_blocking_bridge_available() -> FileResult<()> {
    if Handle::try_current()
        .is_ok_and(|current| current.runtime_flavor() == RuntimeFlavor::CurrentThread)
    {
        return Err(FileError::new(
            FileErrorKind::Internal,
            "filesystem I/O cannot synchronously bridge from a current-thread Tokio runtime",
        ));
    }
    Ok(())
}

fn map_scan_join<T>(result: Result<FileResult<T>, tokio::task::JoinError>) -> FileResult<T> {
    match result {
        Ok(result) => result,
        Err(error) if error.is_cancelled() => {
            Err(FileError::cancelled("backend scan I/O task was cancelled"))
        }
        Err(error) => Err(FileError::with_source(
            FileErrorKind::Internal,
            "backend scan I/O task failed",
            error,
        )),
    }
}

struct TrackedFileTaskSpawner {
    inner: TokioFileTaskSpawner,
    flight: Arc<FlightState>,
}

impl FileTaskSpawner for TrackedFileTaskSpawner {
    fn spawn(&self, task: FileTaskFuture) -> FileResult<FileTask> {
        let flight = self.flight.enter()?;
        let stop = self.flight.stop.view();
        self.inner.spawn(Box::pin(async move {
            let _flight = flight;
            tokio::select! {
                _ = task => {},
                _ = stop.stopped() => {},
            }
        }))
    }

    fn spawn_detached_blocking(&self, job: Box<dyn FnOnce() + Send + 'static>) {
        let Ok(flight) = self.flight.enter() else {
            tracing::warn!("discarded credential refresh after backend scan I/O admission closed");
            return;
        };
        self.inner.spawn_detached_blocking(Box::new(move || {
            let _flight = flight;
            job();
        }));
    }
}

/// Backend scan filesystem services injected into the Iceberg read binding.
#[derive(Clone)]
pub struct ScanIoServices {
    file_runtime: Arc<dyn FileIoRuntime>,
    file_task_spawner: Arc<dyn FileTaskSpawner>,
    range_service: Arc<FileRangeService>,
    flight: Arc<FlightState>,
    handle: tokio::runtime::Handle,
}

impl ScanIoServices {
    /// The scan I/O runtime itself: typed scan streams are polled and closed
    /// inside its context.
    pub fn runtime_handle(&self) -> tokio::runtime::Handle {
        self.handle.clone()
    }

    pub fn file_runtime(&self) -> Arc<dyn FileIoRuntime> {
        Arc::clone(&self.file_runtime)
    }

    pub fn file_task_spawner(&self) -> Arc<dyn FileTaskSpawner> {
        Arc::clone(&self.file_task_spawner)
    }

    pub fn range_service(&self) -> Arc<FileRangeService> {
        Arc::clone(&self.range_service)
    }

    pub fn close_admission(&self) {
        self.range_service.close_admission();
        self.flight.close();
    }
}

/// Owned only by the BE process composition root, through host drain.
pub struct ScanIoRuntime {
    runtime: Runtime,
    services: ScanIoServices,
    flight: Arc<FlightState>,
}

impl ScanIoRuntime {
    pub fn start(config: &RuntimeConfig) -> anyhow::Result<Self> {
        anyhow::ensure!(
            config.scan_io_max_blocking_threads > 0,
            "runtime.scan_io_max_blocking_threads must be nonzero"
        );
        anyhow::ensure!(
            config.scan_range_source_window <= config.scan_range_process_window,
            "runtime scan range source window exceeds process window"
        );
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(config.actual_scan_io_threads())
            .max_blocking_threads(config.scan_io_max_blocking_threads)
            .thread_name("novarocks-scan-io")
            .thread_stack_size(novarocks_types::WORKER_STACK_SIZE_BYTES)
            .build()
            .map_err(|error| anyhow::anyhow!("build backend scan I/O runtime: {error}"))?;
        let handle = runtime.handle().clone();
        let flight = Arc::new(FlightState::default());
        let file_task_spawner: Arc<dyn FileTaskSpawner> = Arc::new(TrackedFileTaskSpawner {
            inner: TokioFileTaskSpawner::new(handle.clone()),
            flight: Arc::clone(&flight),
        });
        let range_service = FileRangeService::new(
            NonZeroUsize::new(config.scan_range_process_window)
                .ok_or_else(|| anyhow::anyhow!("scan range process window must be nonzero"))?,
            NonZeroUsize::new(config.scan_range_source_window)
                .ok_or_else(|| anyhow::anyhow!("scan range source window must be nonzero"))?,
            NonZeroUsize::new(config.scan_range_queue_capacity)
                .ok_or_else(|| anyhow::anyhow!("scan range queue capacity must be nonzero"))?,
            Arc::clone(&file_task_spawner),
            handle.clone(),
        );
        Ok(Self {
            runtime,
            services: ScanIoServices {
                file_runtime: Arc::new(TrackedFileIoRuntime {
                    inner: TokioFileIoRuntime::new(handle.clone()),
                    handle: handle.clone(),
                    flight: Arc::clone(&flight),
                }),
                file_task_spawner,
                range_service,
                flight: Arc::clone(&flight),
                handle: handle.clone(),
            },
            flight,
        })
    }

    pub fn services(&self) -> ScanIoServices {
        self.services.clone()
    }

    /// Call after backend driver shutdown has stopped every admitted scan.
    /// A runtime drop is performed only after all composed filesystem work
    /// has returned or observed its cancellation.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        self.services.range_service.close_admission();
        self.flight.close();
        let range_drain = self.services.range_service.drain().await;
        self.flight.drain().await;
        let Self {
            runtime, services, ..
        } = self;
        drop(services);
        tokio::task::spawn_blocking(move || drop(runtime))
            .await
            .map_err(|error| anyhow::anyhow!("join backend scan I/O runtime shutdown: {error}"))?;
        range_drain.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn file_work_runs_on_scan_runtime_and_shutdown_waits_for_exit() {
        let runtime = ScanIoRuntime::start(&RuntimeConfig::default()).expect("scan runtime");
        let services = runtime.services();
        let observed = std::thread::spawn({
            let file_runtime = services.file_runtime();
            move || {
                file_runtime
                    .block_on_bytes(Box::pin(async {
                        assert!(
                            std::thread::current()
                                .name()
                                .is_some_and(|name| name.starts_with("novarocks-scan-io"))
                        );
                        Ok(bytes::Bytes::from_static(b"scan"))
                    }))
                    .expect("scan I/O")
            }
        })
        .join()
        .expect("reader thread");
        assert_eq!(&observed[..], b"scan");

        let (started, started_rx) = tokio::sync::oneshot::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        services
            .file_task_spawner()
            .spawn_detached_blocking(Box::new(move || {
                started.send(()).expect("notify blocking start");
                release_rx.recv().expect("release blocking refresh");
            }));
        started_rx.await.expect("blocking refresh started");

        let task = services
            .file_task_spawner()
            .spawn(Box::pin(async move {
                std::future::pending::<()>().await;
            }))
            .expect("spawn scan task");
        // Even a caller that forgets its FileTask cannot keep shutdown alive:
        // the service owns the stop signal and the tracked task exit.
        std::mem::forget(task);
        let stopped = runtime.flight.stop.view();
        let shutdown = tokio::spawn(runtime.shutdown());
        stopped.stopped().await;
        assert!(!shutdown.is_finished());
        let refused = services
            .file_task_spawner()
            .spawn(Box::pin(async {}))
            .err()
            .expect("closed scan I/O rejects new work");
        assert_eq!(refused.kind(), novarocks_fs::FileErrorKind::Cancelled);
        release.send(()).expect("release blocking refresh");
        shutdown.await.expect("join shutdown").expect("shutdown");
    }

    #[tokio::test]
    async fn current_thread_bridge_rejects_without_dispatching_scan_work() {
        let runtime = ScanIoRuntime::start(&RuntimeConfig::default()).expect("scan runtime");
        let error = runtime
            .services()
            .file_runtime()
            .block_on_bytes(Box::pin(async {
                panic!("rejected work must not be polled");
            }))
            .expect_err("current-thread synchronous bridge must fail");
        assert_eq!(error.kind(), FileErrorKind::Internal);
        assert_eq!(runtime.flight.inner.lock().expect("flight lock").active, 0);
        runtime.shutdown().await.expect("drain scan runtime");
    }
}
