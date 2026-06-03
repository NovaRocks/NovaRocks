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
//! Execution-service resource boundaries (IW-1).
//!
//! Responsibilities:
//! - Defines explicit execution-service classes so write-path I/O does not
//!   share the query `data_runtime` directly.
//! - In this branch only `sink_io` is a real dedicated tokio runtime; the rest
//!   alias `data_runtime` via the same `IoExecutor` handle type so a later
//!   cutover requires no call-site changes.
//!
//! Key exported interfaces:
//! - Types: `ExecutorKind`, `IoExecutor`, `IoExecutorMetrics`, `ExecutionServices`.
//! - Functions: `execution_services`.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use tokio::runtime::{Handle, Runtime};
use tokio::task::JoinHandle;

use crate::common::config::{sink_io_max_blocking_threads, sink_io_worker_threads};
use crate::novarocks_logging::info;
use crate::runtime::global_async_runtime::{WORKER_STACK_SIZE_BYTES, data_runtime_handle};

const SINK_IO_THREAD_NAME: &str = "novarocks-sink-io";

/// Identifies an execution-service class for metrics/logging.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutorKind {
    ScanIo,
    SinkIo,
    MetadataIo,
    Commit,
}

impl ExecutorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ExecutorKind::ScanIo => "scan_io",
            ExecutorKind::SinkIo => "sink_io",
            ExecutorKind::MetadataIo => "metadata_io",
            ExecutorKind::Commit => "commit",
        }
    }
}

/// Per-service counters. `queue_len = submitted - started`, `running = started - completed`.
#[derive(Debug, Default)]
pub struct IoExecutorMetrics {
    submitted: AtomicU64,
    started: AtomicU64,
    completed: AtomicU64,
    errors: AtomicU64,
    wait_time_ns_total: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IoExecutorMetricsSnapshot {
    pub submitted: u64,
    pub started: u64,
    pub completed: u64,
    pub errors: u64,
    pub wait_time_ns_total: u64,
}

impl IoExecutorMetrics {
    pub fn snapshot(&self) -> IoExecutorMetricsSnapshot {
        IoExecutorMetricsSnapshot {
            submitted: self.submitted.load(Ordering::Relaxed),
            started: self.started.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            wait_time_ns_total: self.wait_time_ns_total.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone)]
enum Spawner {
    /// Owns a dedicated runtime (sink_io).
    Owned(Arc<Runtime>),
    /// Borrows another runtime's handle (alias to data_runtime).
    Borrowed(Handle),
}

/// A uniform handle to one execution service. Cloneable; cheap to pass around.
#[derive(Clone)]
pub struct IoExecutor {
    kind: ExecutorKind,
    spawner: Spawner,
    metrics: Arc<IoExecutorMetrics>,
}

impl IoExecutor {
    fn new(kind: ExecutorKind, spawner: Spawner) -> Self {
        Self {
            kind,
            spawner,
            metrics: Arc::new(IoExecutorMetrics::default()),
        }
    }

    pub fn kind(&self) -> ExecutorKind {
        self.kind
    }

    pub fn metrics(&self) -> &Arc<IoExecutorMetrics> {
        &self.metrics
    }

    fn spawn_on<F>(&self, fut: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        match &self.spawner {
            Spawner::Owned(rt) => rt.spawn(fut),
            Spawner::Borrowed(handle) => handle.spawn(fut),
        }
    }

    /// Submit a future and account submit/start/complete + wait time.
    pub fn spawn<F>(&self, fut: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let metrics = Arc::clone(&self.metrics);
        metrics.submitted.fetch_add(1, Ordering::Relaxed);
        let enqueued = Instant::now();
        self.spawn_on(async move {
            metrics
                .wait_time_ns_total
                .fetch_add(enqueued.elapsed().as_nanos() as u64, Ordering::Relaxed);
            metrics.started.fetch_add(1, Ordering::Relaxed);
            let out = fut.await;
            metrics.completed.fetch_add(1, Ordering::Relaxed);
            out
        })
    }

    /// Clone this executor onto a fresh, isolated metrics counter while reusing
    /// the same underlying spawner. Test-only: lets accounting assertions be
    /// deterministic regardless of other tasks sharing the global service.
    #[cfg(test)]
    fn with_isolated_metrics(&self) -> Self {
        Self {
            kind: self.kind,
            spawner: self.spawner.clone(),
            metrics: Arc::new(IoExecutorMetrics::default()),
        }
    }

    /// Submit a fallible future; additionally counts `errors` on `Err`.
    pub fn spawn_fallible<F, T, E>(&self, fut: F) -> JoinHandle<Result<T, E>>
    where
        F: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        let metrics = Arc::clone(&self.metrics);
        metrics.submitted.fetch_add(1, Ordering::Relaxed);
        let enqueued = Instant::now();
        self.spawn_on(async move {
            metrics
                .wait_time_ns_total
                .fetch_add(enqueued.elapsed().as_nanos() as u64, Ordering::Relaxed);
            metrics.started.fetch_add(1, Ordering::Relaxed);
            let out = fut.await;
            if out.is_err() {
                metrics.errors.fetch_add(1, Ordering::Relaxed);
            }
            metrics.completed.fetch_add(1, Ordering::Relaxed);
            out
        })
    }
}

/// Process-global execution services. Mirrors `data_runtime()` / `global_driver_executor()`.
pub struct ExecutionServices {
    scan_io: IoExecutor,
    sink_io: IoExecutor,
    metadata_io: IoExecutor,
    commit: IoExecutor,
}

impl ExecutionServices {
    pub fn scan_io(&self) -> &IoExecutor {
        &self.scan_io
    }
    pub fn sink_io(&self) -> &IoExecutor {
        &self.sink_io
    }
    pub fn metadata_io(&self) -> &IoExecutor {
        &self.metadata_io
    }
    pub fn commit(&self) -> &IoExecutor {
        &self.commit
    }
}

static EXECUTION_SERVICES: OnceLock<Result<ExecutionServices, String>> = OnceLock::new();

fn build_sink_io_runtime() -> Result<Arc<Runtime>, String> {
    let worker_threads = sink_io_worker_threads().max(1);
    let max_blocking_threads = sink_io_max_blocking_threads().max(1);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(worker_threads)
        .max_blocking_threads(max_blocking_threads)
        .thread_name(SINK_IO_THREAD_NAME)
        .thread_stack_size(WORKER_STACK_SIZE_BYTES)
        .build()
        .map_err(|e| format!("init sink_io tokio runtime failed: {e}"))?;
    info!(
        worker_threads,
        max_blocking_threads,
        thread_name = SINK_IO_THREAD_NAME,
        "sink_io execution service initialized"
    );
    Ok(Arc::new(runtime))
}

fn build_services() -> Result<ExecutionServices, String> {
    // Real dedicated runtime for sink I/O.
    let sink_rt = build_sink_io_runtime()?;
    let sink_io = IoExecutor::new(ExecutorKind::SinkIo, Spawner::Owned(sink_rt));
    // The rest alias data_runtime for now (same handle type → zero-churn cutover later).
    let data_handle = data_runtime_handle()?;
    let scan_io = IoExecutor::new(ExecutorKind::ScanIo, Spawner::Borrowed(data_handle.clone()));
    let metadata_io = IoExecutor::new(
        ExecutorKind::MetadataIo,
        Spawner::Borrowed(data_handle.clone()),
    );
    let commit = IoExecutor::new(ExecutorKind::Commit, Spawner::Borrowed(data_handle));
    Ok(ExecutionServices {
        scan_io,
        sink_io,
        metadata_io,
        commit,
    })
}

/// Access the process-global execution services.
pub fn execution_services() -> Result<&'static ExecutionServices, String> {
    match EXECUTION_SERVICES.get_or_init(build_services) {
        Ok(services) => Ok(services),
        Err(err) => Err(err.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn services_singleton_is_stable() {
        let a = execution_services().expect("services");
        let b = execution_services().expect("services");
        assert!(std::ptr::eq(a, b));
    }

    #[test]
    fn sink_io_runs_on_dedicated_runtime() {
        let services = execution_services().expect("services");
        let handle = services.sink_io().spawn(async {
            std::thread::current()
                .name()
                .map(|s| s.to_string())
                .unwrap_or_default()
        });
        let name = futures::executor::block_on(handle).expect("join");
        assert!(
            name.contains("novarocks-sink-io"),
            "sink_io task ran on unexpected thread: {name}"
        );
    }

    #[test]
    fn metadata_io_aliases_data_runtime() {
        let services = execution_services().expect("services");
        let handle = services.metadata_io().spawn(async {
            std::thread::current()
                .name()
                .map(|s| s.to_string())
                .unwrap_or_default()
        });
        let name = futures::executor::block_on(handle).expect("join");
        assert!(
            name.contains("novarocks-data-runtime"),
            "metadata_io should alias data_runtime, ran on: {name}"
        );
    }

    #[test]
    fn spawn_accounts_submit_start_complete() {
        // Use an isolated metrics counter so parallel tests sharing the global
        // sink_io service cannot inflate these exact-delta assertions.
        let executor = execution_services()
            .expect("services")
            .sink_io()
            .with_isolated_metrics();
        let before = executor.metrics().snapshot();
        let handle = executor.spawn(async { 42_u32 });
        let value = futures::executor::block_on(handle).expect("join");
        assert_eq!(value, 42);
        let after = executor.metrics().snapshot();
        assert_eq!(after.submitted, before.submitted + 1);
        assert_eq!(after.started, before.started + 1);
        assert_eq!(after.completed, before.completed + 1);
    }

    #[test]
    fn spawn_fallible_counts_errors() {
        // Isolated metrics: see `spawn_accounts_submit_start_complete`.
        let executor = execution_services()
            .expect("services")
            .sink_io()
            .with_isolated_metrics();
        let before = executor.metrics().snapshot();
        let handle = executor.spawn_fallible(async { Err::<(), String>("boom".to_string()) });
        let out = futures::executor::block_on(handle).expect("join");
        assert!(out.is_err());
        let after = executor.metrics().snapshot();
        assert_eq!(after.errors, before.errors + 1);
    }
}
