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
//! - Every service is explicitly owned by an `ExecutionRuntime`; no lookup
//!   reaches an application configuration singleton or a shared global runtime.
//!
//! Key exported interfaces:
//! - Types: `ExecutorKind`, `IoExecutor`, `IoExecutorMetrics`, `ExecutionServices`.
//! - Types: `ExecutorKind`, `IoExecutor`, `IoExecutorMetrics`, `ExecutionServices`.

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

use crate::runtime::execution_runtime::ExecutionRuntimeConfig;

const SINK_IO_THREAD_NAME: &str = "novarocks-sink-io";
const SHARED_IO_THREAD_NAME: &str = "novarocks-execution-io";
const WORKER_STACK_SIZE_BYTES: usize = 16 * 1024 * 1024;

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

/// I/O services owned by one explicit execution runtime.
pub struct ExecutionServices {
    scan_io: IoExecutor,
    sink_io: IoExecutor,
    metadata_io: IoExecutor,
    commit: IoExecutor,
}

impl fmt::Debug for ExecutionServices {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutionServices")
            .finish_non_exhaustive()
    }
}

impl ExecutionServices {
    pub fn new(config: &ExecutionRuntimeConfig) -> Result<Self, String> {
        let sink_rt = build_runtime(
            SINK_IO_THREAD_NAME,
            config.sink_io_worker_threads,
            config.sink_io_max_blocking_threads,
        )?;
        let shared_io_rt = build_runtime(
            SHARED_IO_THREAD_NAME,
            config.scan_threads,
            config.scan_queue_capacity,
        )?;
        Ok(Self {
            scan_io: IoExecutor::new(
                ExecutorKind::ScanIo,
                Spawner::Owned(Arc::clone(&shared_io_rt)),
            ),
            sink_io: IoExecutor::new(ExecutorKind::SinkIo, Spawner::Owned(sink_rt)),
            metadata_io: IoExecutor::new(
                ExecutorKind::MetadataIo,
                Spawner::Owned(Arc::clone(&shared_io_rt)),
            ),
            commit: IoExecutor::new(ExecutorKind::Commit, Spawner::Owned(shared_io_rt)),
        })
    }

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

fn build_runtime(
    thread_name: &'static str,
    worker_threads: usize,
    max_blocking_threads: usize,
) -> Result<Arc<Runtime>, String> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(worker_threads)
        .max_blocking_threads(max_blocking_threads)
        .thread_name(thread_name)
        .thread_stack_size(WORKER_STACK_SIZE_BYTES)
        .build()
        .map_err(|e| format!("init execution I/O runtime {thread_name} failed: {e}"))?;
    Ok(Arc::new(runtime))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::execution_runtime::ExecutionRuntimeConfig;

    fn services() -> ExecutionServices {
        ExecutionServices::new(&ExecutionRuntimeConfig {
            driver_threads: 1,
            scan_threads: 1,
            scan_queue_capacity: 1,
            spill_io_threads: 1,
            spill_io_queue_capacity: 1,
            spill_storage: crate::runtime::execution_runtime::ExecutionSpillStorageConfig::default(
            ),
            exchange_io_threads: 1,
            exchange_io_max_inflight_bytes: 1,
            exchange_max_transmit_batched_bytes: 1,
            operator_buffer_chunks: 1,
            local_exchange_buffer_mem_limit_per_driver: 1,
            local_exchange_max_buffered_rows: 1,
            connector_io_tasks_per_scan_operator: 1,
            scan_submit_fail_max: 1,
            scan_submit_fail_timeout_ms: 1,
            runtime_filter_scan_wait_time_ms_override: None,
            runtime_filter_wait_timeout_ms_override: None,
            sink_io_worker_threads: 1,
            sink_io_max_blocking_threads: 1,
        })
        .expect("execution services")
    }

    #[test]
    fn sink_io_runs_on_dedicated_runtime() {
        let services = services();
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
    fn metadata_io_runs_on_runtime_owned_shared_io() {
        let services = services();
        let handle = services.metadata_io().spawn(async {
            std::thread::current()
                .name()
                .map(|s| s.to_string())
                .unwrap_or_default()
        });
        let name = futures::executor::block_on(handle).expect("join");
        assert!(
            name.contains(SHARED_IO_THREAD_NAME),
            "metadata_io ran on unexpected runtime: {name}"
        );
    }

    #[test]
    fn spawn_accounts_submit_start_complete() {
        let executor = services().sink_io().with_isolated_metrics();
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
        let executor = services().sink_io().with_isolated_metrics();
        let before = executor.metrics().snapshot();
        let handle = executor.spawn_fallible(async { Err::<(), String>("boom".to_string()) });
        let out = futures::executor::block_on(handle).expect("join");
        assert!(out.is_err());
        let after = executor.metrics().snapshot();
        assert_eq!(after.errors, before.errors + 1);
    }
}
