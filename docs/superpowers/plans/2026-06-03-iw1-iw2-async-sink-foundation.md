# IW-1 + IW-2 Async Sink Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the two foundations of the Iceberg distributed write pipeline — (IW-1) explicit execution-service boundaries so write-path I/O stops sharing the query `data_runtime`, and (IW-2) an async sink operator contract so sinks enqueue work instead of blocking the driver thread.

**Architecture:** A process-global `ExecutionServices` singleton exposes a uniform `IoExecutor` handle for five service classes; only `sink_io` becomes a real dedicated tokio runtime in this branch, the rest alias `data_runtime` (same type → zero-churn cutover later). A generic `AsyncSinkOperator<B>` wraps a tiny `AsyncSinkBackend` trait and carries the full pipeline contract (bounded queue, background drain on `sink_io`, backpressure via `need_input`/`OutputFull`, async finish via `pending_finish`/`PendingFinish`, error propagation via `RuntimeErrorState`). The driver, builder, and existing sinks are untouched; the cooperative-scheduling primitives (`DriverState::PendingFinish`, `Observable`, `blocked_driver_poller`) already exist and are reused as-is.

**Tech Stack:** Rust, tokio (`rt-multi-thread`, `sync`), `async-trait`, arrow `RecordBatch`/`Chunk`, serde config. Spec: `docs/superpowers/specs/2026-06-03-iw1-iw2-async-sink-foundation-design.md`.

**Conventions (from CLAUDE.md):**
- Code comments / logs / errors / commit messages: **English**.
- Commit messages: **no `Co-Authored-By` trailer**.
- Build for correctness iteration: `cargo build` (dev). Tests: `cargo test`.
- Scope `cargo fmt` to touched files only (`main` has pre-existing unformatted `src/sql/**` — never run a bare repo-wide `cargo fmt`).

---

## File Structure

**IW-1 (new + modified):**
- Create `src/runtime/execution_services.rs` — `ExecutionServices`, `IoExecutor`, `Spawner`, `ExecutorKind`, `IoExecutorMetrics`, `execution_services()`.
- Modify `src/runtime/mod.rs` — register `pub mod execution_services;`.
- Modify `src/common/app_config.rs` — add `ExecutionServicesConfig` nested under `RuntimeConfig`.
- Modify `src/common/config.rs` — add accessor fns `sink_io_worker_threads()`, `sink_io_max_blocking_threads()`, `async_sink_queue_capacity()`.
- Modify `src/runtime/runtime_state.rs` — add `RuntimeState::sink_io_executor()`.

**IW-2 (new + modified):**
- Create `src/exec/pipeline/async_sink.rs` — `AsyncSinkBackend`, `AsyncSinkOperator<B>`, `SinkShared`, `TestAsyncSink` (test-only), all unit + driver-level tests.
- Modify `src/exec/pipeline/mod.rs` — register `pub mod async_sink;`.

Each task below produces a self-contained, committable change.

---

## Phase A — IW-1: Execution resource boundaries

### Task A1: Config — `ExecutionServicesConfig`

**Files:**
- Modify: `src/common/app_config.rs` (add nested struct + field + defaults + Default impl)
- Modify: `src/common/config.rs` (add accessor fns)

- [ ] **Step 1: Add the config struct + defaults to `app_config.rs`**

Add this block immediately after the `PathRewriteConfig` struct (currently ends at `src/common/app_config.rs:961`):

```rust
/// Execution-service resource boundaries (IW-1).
///
/// These knobs size the dedicated `sink_io` runtime and the async-sink queue.
/// Defaults add only a few (mostly idle) threads and do not change all-in-one
/// behavior. `metadata_io` / `commit` / `scan_io` currently alias `data_runtime`
/// and therefore have no size knobs yet.
#[derive(Clone, Deserialize)]
pub struct ExecutionServicesConfig {
    /// Worker threads for the dedicated sink I/O runtime. 0 = min(4, cores).
    #[serde(default = "default_sink_io_worker_threads")]
    pub sink_io_worker_threads: usize,
    /// Max blocking threads for the dedicated sink I/O runtime.
    #[serde(default = "default_sink_io_max_blocking_threads")]
    pub sink_io_max_blocking_threads: usize,
    /// Bounded queue capacity (chunks) for `AsyncSinkOperator` backpressure.
    #[serde(default = "default_async_sink_queue_capacity")]
    pub async_sink_queue_capacity: usize,
}

fn default_sink_io_worker_threads() -> usize {
    0
}

fn default_sink_io_max_blocking_threads() -> usize {
    16
}

fn default_async_sink_queue_capacity() -> usize {
    8
}

impl Default for ExecutionServicesConfig {
    fn default() -> Self {
        Self {
            sink_io_worker_threads: default_sink_io_worker_threads(),
            sink_io_max_blocking_threads: default_sink_io_max_blocking_threads(),
            async_sink_queue_capacity: default_async_sink_queue_capacity(),
        }
    }
}

impl ExecutionServicesConfig {
    /// Resolve sink I/O worker threads; 0 means min(4, cores).
    pub fn actual_sink_io_worker_threads(&self) -> usize {
        if self.sink_io_worker_threads > 0 {
            self.sink_io_worker_threads
        } else {
            let cores = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1);
            cores.min(4).max(1)
        }
    }
}
```

- [ ] **Step 2: Add the field to `RuntimeConfig` struct + its `Default` impl**

In `RuntimeConfig` (struct ends at `src/common/app_config.rs:587`), add after the `path_rewrite` field:

```rust
    #[serde(default)]
    pub execution_services: ExecutionServicesConfig,
```

In the manual `Default for RuntimeConfig` impl, add after `path_rewrite: PathRewriteConfig::default(),` (currently `src/common/app_config.rs:934`):

```rust
            execution_services: ExecutionServicesConfig::default(),
```

- [ ] **Step 3: Add accessor fns to `config.rs`**

Add after `data_runtime_max_blocking_threads()` (currently ends at `src/common/config.rs:286`):

```rust
pub(crate) fn sink_io_worker_threads() -> usize {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.execution_services.actual_sink_io_worker_threads())
        .unwrap_or_else(|| {
            let cores = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1);
            cores.min(4).max(1)
        })
}

pub(crate) fn sink_io_max_blocking_threads() -> usize {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.execution_services.sink_io_max_blocking_threads.max(1))
        .unwrap_or(16)
}

pub(crate) fn async_sink_queue_capacity() -> usize {
    novarocks_app_config()
        .ok()
        .map(|c| c.runtime.execution_services.async_sink_queue_capacity.max(1))
        .unwrap_or(8)
}
```

- [ ] **Step 4: Add a unit test for config defaults**

Append to the existing `#[cfg(test)] mod tests` in `app_config.rs` (search for `mod tests` in that file; if none, create one at end of file with `use super::*;`):

```rust
    #[test]
    fn execution_services_defaults_are_sane() {
        let cfg = RuntimeConfig::default();
        assert_eq!(cfg.execution_services.sink_io_max_blocking_threads, 16);
        assert_eq!(cfg.execution_services.async_sink_queue_capacity, 8);
        // 0 means "derive from cores"; resolved value must be >= 1.
        assert!(cfg.execution_services.actual_sink_io_worker_threads() >= 1);
        assert!(cfg.execution_services.actual_sink_io_worker_threads() <= 4);
    }
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test --lib execution_services_defaults_are_sane`
Expected: PASS. (If it does not compile, fix the field/struct names to match Steps 1–2 exactly.)

- [ ] **Step 6: Commit**

```bash
git add src/common/app_config.rs src/common/config.rs
git commit -m "feat(runtime): add ExecutionServicesConfig for IW-1 resource boundaries"
```

---

### Task A2: `ExecutionServices` singleton + `IoExecutor`

**Files:**
- Create: `src/runtime/execution_services.rs`
- Modify: `src/runtime/mod.rs`
- Test: inline `#[cfg(test)]` in `src/runtime/execution_services.rs`

- [ ] **Step 1: Register the module**

In `src/runtime/mod.rs`, add (keep alphabetical with the other `pub mod` lines if the file is ordered):

```rust
pub mod execution_services;
```

- [ ] **Step 2: Write the new module with the failing tests first**

Create `src/runtime/execution_services.rs` with the full content below. (Tests at the bottom will fail to compile until the impl in Step 3 — that is the intended "red" state; we write both in one file but commit only after green, per the next steps.)

```rust
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
    let metadata_io =
        IoExecutor::new(ExecutorKind::MetadataIo, Spawner::Borrowed(data_handle.clone()));
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
        let services = execution_services().expect("services");
        let before = services.sink_io().metrics().snapshot();
        let handle = services.sink_io().spawn(async { 42_u32 });
        let value = futures::executor::block_on(handle).expect("join");
        assert_eq!(value, 42);
        let after = services.sink_io().metrics().snapshot();
        assert_eq!(after.submitted, before.submitted + 1);
        assert_eq!(after.started, before.started + 1);
        assert_eq!(after.completed, before.completed + 1);
    }

    #[test]
    fn spawn_fallible_counts_errors() {
        let services = execution_services().expect("services");
        let before = services.sink_io().metrics().snapshot();
        let handle = services
            .sink_io()
            .spawn_fallible(async { Err::<(), String>("boom".to_string()) });
        let out = futures::executor::block_on(handle).expect("join");
        assert!(out.is_err());
        let after = services.sink_io().metrics().snapshot();
        assert_eq!(after.errors, before.errors + 1);
    }
}
```

> Note: `data_runtime_handle()` and `WORKER_STACK_SIZE_BYTES` are public in `src/runtime/global_async_runtime.rs:35,65`. If `data_runtime_handle` is not `pub`, change it from `pub fn` — it is already `pub`.

- [ ] **Step 3: Run the tests to verify they pass**

Run: `cargo test --lib runtime::execution_services`
Expected: 5 tests PASS. Common fixes if red:
- If `info!` import path differs, match `global_async_runtime.rs` which uses `use crate::novarocks_logging::info;`.
- `futures::executor::block_on` requires the `futures` crate (already a dep at `Cargo.toml:32`).

- [ ] **Step 4: Verify the build is clean**

Run: `cargo build`
Expected: builds. Run `cargo clippy --lib -- -D warnings 2>&1 | head -40` and fix any warnings in the new file only.

- [ ] **Step 5: Commit**

```bash
git add src/runtime/execution_services.rs src/runtime/mod.rs
git commit -m "feat(runtime): add ExecutionServices singleton with dedicated sink_io runtime (IW-1)"
```

---

### Task A3: Wire `RuntimeState::sink_io_executor()`

**Files:**
- Modify: `src/runtime/runtime_state.rs`
- Test: inline `#[cfg(test)]` in `src/runtime/runtime_state.rs`

- [ ] **Step 1: Add the accessor method**

In `src/runtime/runtime_state.rs`, inside `impl RuntimeState` (e.g. right after the `error_state()` method at line 354), add:

```rust
    /// Handle to the dedicated sink I/O execution service (IW-1).
    ///
    /// Operators reach this via `&RuntimeState` in `bind_runtime_state` /
    /// `push_chunk` so they never grab the shared `data_runtime` directly.
    pub fn sink_io_executor(
        &self,
    ) -> Result<crate::runtime::execution_services::IoExecutor, String> {
        crate::runtime::execution_services::execution_services()
            .map(|services| services.sink_io().clone())
    }
```

- [ ] **Step 2: Add a test that a sink_io task runs off the data_runtime**

Append a `#[cfg(test)] mod tests` to `src/runtime/runtime_state.rs` (file currently has none):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sink_io_executor_from_default_state_runs_on_sink_runtime() {
        let state = RuntimeState::default();
        let exec = state.sink_io_executor().expect("sink_io executor");
        let handle = exec.spawn(async {
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
}
```

- [ ] **Step 3: Run the test**

Run: `cargo test --lib runtime_state::tests::sink_io_executor_from_default_state_runs_on_sink_runtime`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/runtime/runtime_state.rs
git commit -m "feat(runtime): expose RuntimeState::sink_io_executor for async sinks (IW-1)"
```

---

## Phase B — IW-2: Async sink operator contract

### Task B1: `AsyncSinkBackend` + `AsyncSinkOperator<B>` push/drain/backpressure

**Files:**
- Create: `src/exec/pipeline/async_sink.rs`
- Modify: `src/exec/pipeline/mod.rs`
- Test: inline `#[cfg(test)]` in `src/exec/pipeline/async_sink.rs`

- [ ] **Step 1: Register the module**

In `src/exec/pipeline/mod.rs`, add:

```rust
pub mod async_sink;
```

- [ ] **Step 2: Write the module — backend trait, shared state, operator skeleton (push/drain/backpressure), `TestAsyncSink`, and the first test**

Create `src/exec/pipeline/async_sink.rs`:

```rust
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
//! Async sink operator contract (IW-2).
//!
//! Responsibilities:
//! - Lets a sink enqueue chunks to a bounded queue and drain them on the
//!   dedicated `sink_io` execution service (IW-1) instead of doing blocking
//!   I/O on the driver thread.
//! - Carries the full pipeline contract on the existing `ProcessorOperator`
//!   methods: backpressure via `need_input`/`OutputFull`, async finish via
//!   `pending_finish`/`PendingFinish`, and error propagation via
//!   `RuntimeErrorState`.
//!
//! Concrete sinks implement only `AsyncSinkBackend`; `AsyncSinkOperator<B>`
//! wraps them and is the single place the contract is implemented.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::exec::chunk::Chunk;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::execution_services::IoExecutor;
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};

/// Minimal async backend implemented by concrete sinks. The wrapper drives it.
#[async_trait::async_trait]
pub trait AsyncSinkBackend: Send + 'static {
    /// Result handed to the caller after a clean finish (e.g. staged files, stats).
    type Output: Send + 'static;

    /// Write one chunk. Runs on the `sink_io` runtime; may do real I/O.
    async fn write_chunk(&mut self, chunk: Chunk) -> Result<(), String>;

    /// Finalize after all chunks are drained. Runs on the `sink_io` runtime.
    async fn finish(&mut self) -> Result<Self::Output, String>;
}

/// State shared between the driver-side operator and the background drain task.
struct SinkShared<O> {
    /// Fires when the queue drains (backpressure relief) or finish completes.
    observable: Arc<Observable>,
    /// Chunks enqueued but not yet drained (need_input watermark + metrics).
    queued: AtomicUsize,
    /// Background drain + finish fully done.
    finished: AtomicBool,
    /// Background hit an error.
    errored: AtomicBool,
    /// Output produced by a clean finish.
    result: std::sync::Mutex<Option<O>>,
}

impl<O> SinkShared<O> {
    fn new() -> Self {
        Self {
            observable: Arc::new(Observable::new()),
            queued: AtomicUsize::new(0),
            finished: AtomicBool::new(false),
            errored: AtomicBool::new(false),
            result: std::sync::Mutex::new(None),
        }
    }

    /// Wake any driver parked on this sink's observable.
    fn wake(&self) {
        self.observable.defer_notify().arm();
    }
}

/// Generic async sink operator. Implements the full pipeline contract; concrete
/// sinks only implement [`AsyncSinkBackend`].
pub struct AsyncSinkOperator<B: AsyncSinkBackend> {
    name: String,
    capacity: usize,
    // Pre-bind state (moved into the background task at bind_runtime_state):
    backend: Option<B>,
    rx: Option<mpsc::Receiver<Chunk>>,
    // Live state:
    sender: Option<mpsc::Sender<Chunk>>,
    shared: Arc<SinkShared<B::Output>>,
    join: Option<JoinHandle<()>>,
    finishing: bool,
}

impl<B: AsyncSinkBackend> AsyncSinkOperator<B> {
    pub fn new(name: impl Into<String>, backend: B, capacity: usize) -> Self {
        let capacity = capacity.max(1);
        let (tx, rx) = mpsc::channel(capacity);
        Self {
            name: name.into(),
            capacity,
            backend: Some(backend),
            rx: Some(rx),
            sender: Some(tx),
            shared: Arc::new(SinkShared::new()),
            join: None,
            finishing: false,
        }
    }

    /// Take the finish output (available once `is_finished()` is true after a
    /// clean finish). Returns None if errored or not finished.
    pub fn take_output(&self) -> Option<B::Output> {
        self.shared.result.lock().expect("sink result lock").take()
    }
}

/// Background drain loop: pull chunks, write them, then finish. Reports errors
/// through `RuntimeErrorState` and never blocks the driver thread.
async fn drain_loop<B: AsyncSinkBackend>(
    mut backend: B,
    mut rx: mpsc::Receiver<Chunk>,
    shared: Arc<SinkShared<B::Output>>,
    error_state: Arc<RuntimeErrorState>,
) -> Result<(), String> {
    loop {
        match rx.recv().await {
            Some(chunk) => match backend.write_chunk(chunk).await {
                Ok(()) => {
                    shared.queued.fetch_sub(1, Ordering::AcqRel);
                    shared.wake(); // queue has room → wake a backpressured driver
                }
                Err(e) => {
                    error_state.set_error(e.clone());
                    shared.errored.store(true, Ordering::Release);
                    shared.finished.store(true, Ordering::Release);
                    shared.wake();
                    return Err(e);
                }
            },
            None => break, // sender dropped (set_finishing / cancel)
        }
    }
    let result = backend.finish().await;
    match result {
        Ok(out) => {
            *shared.result.lock().expect("sink result lock") = Some(out);
        }
        Err(e) => {
            error_state.set_error(e.clone());
            shared.errored.store(true, Ordering::Release);
            shared.finished.store(true, Ordering::Release);
            shared.wake();
            return Err(e);
        }
    }
    shared.finished.store(true, Ordering::Release);
    shared.wake();
    Ok(())
}

impl<B: AsyncSinkBackend> Operator for AsyncSinkOperator<B> {
    fn name(&self) -> &str {
        &self.name
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> Result<(), String> {
        let sink_io: IoExecutor = state.sink_io_executor()?;
        let error_state = state.error_state();
        let backend = self
            .backend
            .take()
            .ok_or_else(|| "async sink backend already bound".to_string())?;
        let rx = self
            .rx
            .take()
            .ok_or_else(|| "async sink receiver already bound".to_string())?;
        let shared = Arc::clone(&self.shared);
        let join = sink_io.spawn(async move {
            let _ = drain_loop(backend, rx, shared, error_state).await;
        });
        self.join = Some(join);
        Ok(())
    }

    fn is_finished(&self) -> bool {
        self.shared.finished.load(Ordering::Acquire)
    }

    fn pending_finish(&self) -> bool {
        self.finishing && !self.shared.finished.load(Ordering::Acquire)
    }

    fn cancel(&mut self) {
        self.sender = None; // close the channel
        if let Some(join) = self.join.take() {
            join.abort();
        }
        self.shared.errored.store(true, Ordering::Release);
        self.shared.finished.store(true, Ordering::Release);
        self.shared.wake();
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
}

impl<B: AsyncSinkBackend> ProcessorOperator for AsyncSinkOperator<B> {
    fn need_input(&self) -> bool {
        !self.finishing
            && !self.shared.errored.load(Ordering::Acquire)
            && self.shared.queued.load(Ordering::Acquire) < self.capacity
    }

    fn has_output(&self) -> bool {
        false
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        let Some(sender) = self.sender.as_ref() else {
            return Err("async sink push after finishing/cancel".to_string());
        };
        self.shared.queued.fetch_add(1, Ordering::AcqRel);
        match sender.try_send(chunk) {
            Ok(()) => Ok(()),
            Err(e) => {
                // need_input gates this; a Full/closed here is a contract bug.
                self.shared.queued.fetch_sub(1, Ordering::AcqRel);
                Err(format!("async sink enqueue failed: {e}"))
            }
        }
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        self.finishing = true;
        self.sender = None; // drop sender → background sees recv()==None → finish()
        Ok(())
    }

    fn sink_observable(&self) -> Option<Arc<Observable>> {
        Some(Arc::clone(&self.shared.observable))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::common::ids::SlotId;
    use crate::exec::chunk::ChunkSchema;
    use crate::runtime::runtime_state::RuntimeState;

    fn make_chunk(rows: usize) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Int32, true)]));
        let data: Vec<i32> = (0..rows as i32).collect();
        let array = Arc::new(Int32Array::from(data)) as _;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId::new(1)])
                .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    /// Test backend: records rows seen; optional per-chunk gate and fail point.
    struct TestAsyncSink {
        rows: Arc<AtomicUsize>,
        chunks: Arc<AtomicUsize>,
        // When set, write_chunk waits until released (simulates slow I/O / backpressure).
        gate: Arc<tokio::sync::Semaphore>,
        // When Some(n), the n-th (0-based) write_chunk fails.
        fail_at: Option<usize>,
        // Delay applied inside finish() to exercise pending_finish.
        finish_delay: Duration,
    }

    impl TestAsyncSink {
        fn new(gate_permits: usize) -> (Self, Arc<AtomicUsize>, Arc<AtomicUsize>) {
            let rows = Arc::new(AtomicUsize::new(0));
            let chunks = Arc::new(AtomicUsize::new(0));
            let sink = Self {
                rows: Arc::clone(&rows),
                chunks: Arc::clone(&chunks),
                gate: Arc::new(tokio::sync::Semaphore::new(gate_permits)),
                fail_at: None,
                finish_delay: Duration::ZERO,
            };
            (sink, rows, chunks)
        }
    }

    #[async_trait::async_trait]
    impl AsyncSinkBackend for TestAsyncSink {
        type Output = usize;

        async fn write_chunk(&mut self, chunk: Chunk) -> Result<(), String> {
            let permit = self.gate.acquire().await.expect("gate");
            permit.forget();
            let idx = self.chunks.fetch_add(1, Ordering::AcqRel);
            if self.fail_at == Some(idx) {
                return Err(format!("forced failure at chunk {idx}"));
            }
            self.rows.fetch_add(chunk.len(), Ordering::AcqRel);
            Ok(())
        }

        async fn finish(&mut self) -> Result<usize, String> {
            if !self.finish_delay.is_zero() {
                tokio::time::sleep(self.finish_delay).await;
            }
            Ok(self.rows.load(Ordering::Acquire))
        }
    }

    fn poll_until<F: Fn() -> bool>(pred: F, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if pred() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        pred()
    }

    #[test]
    fn drains_all_chunks_with_backpressure() {
        let state = RuntimeState::default();
        // gate starts open (large permit count) so writes flow.
        let (backend, rows, chunks) = TestAsyncSink::new(1_000);
        let mut op = AsyncSinkOperator::new("test_async_sink", backend, 2);
        op.bind_runtime_state(&state).expect("bind");

        // Push 5 chunks of 3 rows each, respecting need_input backpressure.
        let mut pushed = 0;
        let deadline = Instant::now() + Duration::from_secs(5);
        while pushed < 5 {
            if op.need_input() {
                op.push_chunk(&state, make_chunk(3)).expect("push");
                pushed += 1;
            } else {
                assert!(Instant::now() < deadline, "stuck on backpressure");
                std::thread::sleep(Duration::from_millis(2));
            }
        }
        op.set_finishing(&state).expect("finish");

        assert!(
            poll_until(|| op.is_finished(), Duration::from_secs(5)),
            "sink did not finish"
        );
        assert_eq!(chunks.load(Ordering::Acquire), 5);
        assert_eq!(rows.load(Ordering::Acquire), 15);
        assert_eq!(op.take_output(), Some(15));
    }
}
```

- [ ] **Step 3: Run the test to verify it passes**

Run: `cargo test --lib async_sink::tests::drains_all_chunks_with_backpressure`
Expected: PASS. Likely fixes if red:
- If `ChunkSchema::try_ref_from_schema_and_slot_ids` path differs, copy the exact import used in `src/exec/operators/assert_num_rows_processor.rs:263`.
- If `tokio::sync::Semaphore` is unavailable, the `sync` feature is enabled (`Cargo.toml:57`) so it should resolve.
- Remove the `_AsyncMutex` shim + `use tokio::sync::Mutex as AsyncMutex;` if clippy flags it as unused (it is a placeholder; drop both lines).

- [ ] **Step 4: Commit**

```bash
git add src/exec/pipeline/async_sink.rs src/exec/pipeline/mod.rs
git commit -m "feat(pipeline): AsyncSinkBackend + AsyncSinkOperator push/drain/backpressure (IW-2)"
```

---

### Task B2: Backpressure-parks + async-finish behavior tests

These assert the two contract guarantees that need explicit timing: (a) `need_input()` actually goes false when the queue is full and recovers after drain, and (b) `pending_finish()` is true while `finish()` is in flight and clears afterward.

**Files:**
- Modify: `src/exec/pipeline/async_sink.rs` (tests only)

- [ ] **Step 1: Add a backpressure-parks test**

Add to the `tests` module in `src/exec/pipeline/async_sink.rs`. This uses a gate that starts CLOSED so the background task cannot drain, forcing `need_input` false after `capacity` pushes:

```rust
    #[test]
    fn need_input_goes_false_when_queue_full_then_recovers() {
        let state = RuntimeState::default();
        // gate starts closed (0 permits): background blocks on the first write.
        let (backend, _rows, _chunks) = TestAsyncSink::new(0);
        let gate = Arc::clone(&backend.gate);
        let mut op = AsyncSinkOperator::new("bp_sink", backend, 2);
        op.bind_runtime_state(&state).expect("bind");

        // Fill the queue: capacity=2, plus 1 in-flight pulled by the bg task.
        // Push until need_input() reports full.
        let mut pushed = 0;
        while op.need_input() && pushed < 8 {
            op.push_chunk(&state, make_chunk(1)).expect("push");
            pushed += 1;
        }
        assert!(!op.need_input(), "sink should report backpressure when full");

        // Release the gate; background drains; need_input must recover.
        gate.add_permits(100);
        assert!(
            poll_until(|| op.need_input(), Duration::from_secs(5)),
            "need_input did not recover after drain"
        );

        op.set_finishing(&state).expect("finish");
        assert!(
            poll_until(|| op.is_finished(), Duration::from_secs(5)),
            "sink did not finish"
        );
    }
```

- [ ] **Step 2: Add an async-finish / pending_finish test**

```rust
    #[test]
    fn pending_finish_true_while_finishing_then_clears() {
        let state = RuntimeState::default();
        let (mut backend, _rows, _chunks) = TestAsyncSink::new(1_000);
        backend.finish_delay = Duration::from_millis(200);
        let mut op = AsyncSinkOperator::new("finish_sink", backend, 4);
        op.bind_runtime_state(&state).expect("bind");

        op.push_chunk(&state, make_chunk(2)).expect("push");
        op.set_finishing(&state).expect("finish");

        // While finish() sleeps, pending_finish must be true and is_finished false.
        assert!(
            poll_until(|| op.pending_finish(), Duration::from_secs(1)),
            "expected pending_finish during async finish"
        );
        assert!(!op.is_finished(), "must not be finished mid-finish");

        // After finish completes, pending_finish clears and is_finished is true.
        assert!(
            poll_until(|| op.is_finished(), Duration::from_secs(5)),
            "sink did not finish"
        );
        assert!(!op.pending_finish(), "pending_finish must clear after finish");
        assert_eq!(op.take_output(), Some(2));
    }
```

- [ ] **Step 3: Run both tests**

Run: `cargo test --lib async_sink::tests::need_input_goes_false_when_queue_full_then_recovers async_sink::tests::pending_finish_true_while_finishing_then_clears`
Expected: both PASS.

> If `need_input_goes_false...` flakes because the bg task pulls one extra chunk before blocking (mpsc buffers `capacity` + the in-flight recv), the loop bound `pushed < 8` plus the `!need_input()` assert still holds: we only require that pushing eventually saturates. If it never saturates, that is a real bug (queued counter not tracking) — fix the counter, not the test.

- [ ] **Step 4: Commit**

```bash
git add src/exec/pipeline/async_sink.rs
git commit -m "test(pipeline): async sink backpressure-parks + pending_finish timing (IW-2)"
```

---

### Task B3: Background-error propagation test

**Files:**
- Modify: `src/exec/pipeline/async_sink.rs` (tests only)

- [ ] **Step 1: Add the error-propagation test**

```rust
    #[test]
    fn background_failure_sets_query_error_and_does_not_hang() {
        let state = RuntimeState::default();
        let (mut backend, _rows, _chunks) = TestAsyncSink::new(1_000);
        backend.fail_at = Some(1); // second chunk fails
        let mut op = AsyncSinkOperator::new("err_sink", backend, 4);
        op.bind_runtime_state(&state).expect("bind");

        // Push a few chunks; one of them triggers the failure in the bg task.
        for _ in 0..3 {
            if op.need_input() {
                let _ = op.push_chunk(&state, make_chunk(1));
            }
            std::thread::sleep(Duration::from_millis(5));
        }

        // The error must surface through the runtime error channel within bounded time.
        assert!(
            poll_until(|| state.error().is_some(), Duration::from_secs(5)),
            "background failure did not set runtime error"
        );
        // And the operator must converge (no hang): errored ⇒ finished, need_input false.
        assert!(
            poll_until(|| op.is_finished(), Duration::from_secs(5)),
            "errored sink did not converge to finished"
        );
        assert!(!op.need_input(), "errored sink must stop accepting input");
        assert!(
            state.error().unwrap().contains("forced failure"),
            "unexpected error text"
        );
    }
```

- [ ] **Step 2: Run the test**

Run: `cargo test --lib async_sink::tests::background_failure_sets_query_error_and_does_not_hang`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/exec/pipeline/async_sink.rs
git commit -m "test(pipeline): async sink background error propagates to query (IW-2)"
```

---

### Task B4: Cancel test

**Files:**
- Modify: `src/exec/pipeline/async_sink.rs` (tests only)

- [ ] **Step 1: Add the cancel test**

```rust
    #[test]
    fn cancel_mid_flight_aborts_without_hang() {
        let state = RuntimeState::default();
        // gate closed: bg task is parked inside write_chunk when we cancel.
        let (backend, _rows, _chunks) = TestAsyncSink::new(0);
        let mut op = AsyncSinkOperator::new("cancel_sink", backend, 4);
        op.bind_runtime_state(&state).expect("bind");

        op.push_chunk(&state, make_chunk(1)).expect("push");
        op.cancel();

        // Cancel is non-blocking and converges the operator.
        assert!(
            poll_until(|| op.is_finished(), Duration::from_secs(5)),
            "cancel did not converge sink"
        );
        assert!(!op.need_input(), "canceled sink must not accept input");
    }
```

- [ ] **Step 2: Run the test**

Run: `cargo test --lib async_sink::tests::cancel_mid_flight_aborts_without_hang`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/exec/pipeline/async_sink.rs
git commit -m "test(pipeline): async sink cancel aborts background without hang (IW-2)"
```

---

### Task B5: Driver-level integration test (DriverState transitions)

Proves the operator drives a real `PipelineDriver` into `Blocked(OutputFull)` and `PendingFinish`, then `Finished` — the IW-2 acceptance that explicitly names `DriverState`. Reuses the exact scaffolding from `src/exec/pipeline/blocked_driver_poller.rs:256-305`.

**Files:**
- Modify: `src/exec/pipeline/async_sink.rs` (tests only)

- [ ] **Step 1: Add a `TestSource` that emits N chunks then EOS, and the driver test**

Add to the `tests` module:

```rust
    use crate::exec::pipeline::driver::{DriverState, PipelineDriver};
    use crate::exec::pipeline::operator::BlockedReason;

    /// Source operator that emits `remaining` chunks then finishes.
    struct TestSource {
        remaining: usize,
        finished: bool,
    }

    impl TestSource {
        fn new(n: usize) -> Self {
            Self {
                remaining: n,
                finished: false,
            }
        }
    }

    impl Operator for TestSource {
        fn name(&self) -> &str {
            "test_source"
        }
        fn is_finished(&self) -> bool {
            self.finished
        }
        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }
        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for TestSource {
        fn need_input(&self) -> bool {
            false
        }
        fn has_output(&self) -> bool {
            self.remaining > 0
        }
        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
            Err("source does not accept input".to_string())
        }
        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            if self.remaining == 0 {
                self.finished = true;
                return Ok(None);
            }
            self.remaining -= 1;
            if self.remaining == 0 {
                self.finished = true;
            }
            Ok(Some(make_chunk(1)))
        }
        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            self.finished = true;
            Ok(())
        }
    }

    #[test]
    fn driver_parks_on_output_full_and_pending_finish_then_finishes() {
        let runtime_state = Arc::new(RuntimeState::default());

        // gate starts closed so the sink saturates and the driver must block.
        let (mut backend, _rows, _chunks) = TestAsyncSink::new(0);
        backend.finish_delay = Duration::from_millis(150);
        let gate = Arc::clone(&backend.gate);
        let mut sink = AsyncSinkOperator::new("driver_sink", backend, 2);
        // NOTE: a directly-constructed PipelineDriver does NOT call
        // bind_runtime_state on its operators (only the pipeline builder does,
        // pipeline.rs:116). Bind the sink here — with the SAME RuntimeState the
        // driver uses — so its background drain task spawns. Without this the
        // queue never drains and the driver would hang on OutputFull forever.
        sink.bind_runtime_state(&runtime_state).expect("bind sink");

        let driver_state = Arc::clone(&runtime_state);
        let mut driver = PipelineDriver::new(
            1,
            vec![Box::new(TestSource::new(6)), Box::new(sink)],
            None,
            Vec::new(),
            driver_state,
            None,
        );

        // Drive until the sink reports OutputFull (queue saturated, gate closed).
        let mut saw_output_full = false;
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            let st = driver.process(Duration::from_millis(10));
            if matches!(st, DriverState::Blocked(BlockedReason::OutputFull)) {
                saw_output_full = true;
                break;
            }
            if matches!(st, DriverState::Failed(_) | DriverState::Finished) {
                break;
            }
        }
        assert!(saw_output_full, "driver never parked on OutputFull");

        // Release the gate; keep driving. We must observe PendingFinish then Finished.
        gate.add_permits(1_000);
        let mut saw_pending_finish = false;
        let mut finished = false;
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            let st = driver.process(Duration::from_millis(10));
            match st {
                DriverState::PendingFinish => saw_pending_finish = true,
                DriverState::Finished => {
                    finished = true;
                    break;
                }
                DriverState::Failed(e) => panic!("driver failed: {e}"),
                _ => {}
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        assert!(saw_pending_finish, "driver never entered PendingFinish");
        assert!(finished, "driver did not reach Finished");
    }
```

- [ ] **Step 2: Run the test**

Run: `cargo test --lib async_sink::tests::driver_parks_on_output_full_and_pending_finish_then_finishes`
Expected: PASS. Likely fixes if red:
- Match `PipelineDriver::new`'s exact argument list against `src/exec/pipeline/blocked_driver_poller.rs:272-279` (driver_id, `Vec<Box<dyn ProcessorOperator>>`, `None`, `Vec::new()`, `Arc<RuntimeState>`, `None`). If the 3rd/4th/6th argument types differ, copy them verbatim from that test.
- If `DriverState` / `BlockedReason` are not `pub` from those modules, they are: `DriverState` at `src/exec/pipeline/driver.rs:64` and `BlockedReason` at `src/exec/pipeline/operator.rs:45`.
- If the driver never reaches `OutputFull` because the source's `pull_chunk`/`has_output` contract differs from what the driver expects, inspect `drive_dataflow`/`drive_set_finishing` in `driver.rs` and align `TestSource` (it must report `has_output()==true` while chunks remain and `is_finished()==true` after the last pull).

- [ ] **Step 3: Commit**

```bash
git add src/exec/pipeline/async_sink.rs
git commit -m "test(pipeline): driver parks on OutputFull/PendingFinish via async sink (IW-2)"
```

---

### Task B6: Full-suite regression + lint gate

**Files:** none (verification only)

- [ ] **Step 1: Build the whole crate**

Run: `cargo build`
Expected: clean build.

- [ ] **Step 2: Run the pipeline + runtime test modules**

Run: `cargo test --lib exec::pipeline:: runtime::execution_services runtime_state::`
Expected: all PASS, including the pre-existing `blocked_driver_poller` and pipeline tests (proves existing synchronous sinks are unaffected).

- [ ] **Step 3: Lint the touched files only**

Run:
```bash
cargo clippy --lib -- -D warnings 2>&1 | grep -E "execution_services|async_sink|runtime_state|app_config|config\.rs" || echo "no clippy issues in touched files"
cargo fmt -- src/runtime/execution_services.rs src/exec/pipeline/async_sink.rs src/runtime/runtime_state.rs src/common/app_config.rs src/common/config.rs src/runtime/mod.rs src/exec/pipeline/mod.rs
git diff --stat
```
Expected: no clippy issues in touched files; `cargo fmt` only reformats the listed files (do NOT run a bare repo-wide `cargo fmt`). If `git diff` shows changes outside the listed files, `git checkout --` them.

- [ ] **Step 4: Commit any formatting**

```bash
git add -p
git commit -m "style(iceberg-write): fmt touched IW-1/IW-2 files" || echo "nothing to format"
```

---

## Self-Review (completed during planning)

**Spec coverage:**
- IW-1 "5 service classes distinguishable" → `ExecutorKind` + `ExecutionServices` fields (A2). ✓
- IW-1 "sink_io real, others alias" → `Spawner::Owned` vs `Spawner::Borrowed` (A2). ✓
- IW-1 "defaults don't change all-in-one" → `sink_io_worker_threads` default min(4,cores), no semantic change (A1). ✓
- IW-1 "per-service metrics: queue/running/wait/errors" → `IoExecutorMetrics` + tests (A2). ✓
- IW-1 "reachable via RuntimeState" → `RuntimeState::sink_io_executor()` (A3). ✓
- IW-2 "push enqueue, finish async drain, PendingFinish" → `AsyncSinkOperator` contract (B1/B2). ✓
- IW-2 "need_input backpressure" → B2 test. ✓
- IW-2 "background error → query error, no lost error" → B3. ✓
- IW-2 "don't break synchronous sinks" → B6 runs existing pipeline tests. ✓
- IW-2 "general contract, not Iceberg-specific" → generic `AsyncSinkBackend<Output>` + `TestAsyncSink` (B1). ✓
- IW-2 acceptance naming `DriverState::PendingFinish` → driver-level test (B5). ✓

**Type consistency:** `IoExecutor` (Clone), `IoExecutorMetrics::snapshot()`, `execution_services()`, `RuntimeState::sink_io_executor() -> Result<IoExecutor,String>`, `AsyncSinkBackend::Output`, `AsyncSinkOperator::{new,take_output}`, `SinkShared` fields, `make_chunk`, `poll_until` are referenced with identical names/signatures across all tasks.

**Placeholder scan:** every code step contains complete code; "likely fixes if red" notes are debugging aids, not deferred work. The only intentional placeholder is the `_AsyncMutex` shim, which B1 Step 3 instructs to remove if clippy flags it.

**Decisions deferred to implementation (low-risk):** exact `PipelineDriver::new` arg types (copy from the existing test). Does not change the contract.

---

## Contract correction (discovered during B5)

B5's driver-level test revealed that the B1 sink contract did not surface `DriverState::PendingFinish` to the driver — the IW-2 acceptance requires it. The driver enters `PendingFinish` only when the sink reports `is_finished()==true` **and** `pending_finish()==true` in the same tick (`driver.rs:443` checks `self.is_finished()` = the sink's `is_finished()`; `finish_with_state` at `driver.rs:687-696` upgrades `Finished`→`PendingFinish` when `has_pending_finish()`). The original B1 design coupled both to `shared.finished`, making them mutually exclusive, so the driver only ever yielded via `Blocked(OutputFull)` during the async tail and jumped straight to `Finished`.

**Fix (StarRocks-style semantics):**
- `is_finished()` = `self.finishing || self.shared.finished` — "no more input needed" (true once `set_finishing` ran, or on background completion/error/cancel). This is what lets the driver decide to finish.
- `pending_finish()` = `self.finishing && !self.shared.finished` — "async drain/finish still running" (unchanged). True *together with* `is_finished()` during the async tail → driver enters `PendingFinish`; clears when the background completes → driver reaches `Finished`.

**Consequence for tests:** a test's notion of "fully complete" is now `is_finished() && !pending_finish()` (not `is_finished()` alone). B1 (`drains_all_chunks_with_backpressure`) and B2 (`need_input_goes_false...`, `pending_finish_true_while_finishing_then_clears`) poll/assert lines are updated accordingly; the mid-finish `assert!(!op.is_finished())` in B2 becomes `assert!(op.take_output().is_none())`. B3 (error) and B4 (cancel) are unaffected (their `is_finished()` becomes true via `shared.finished`). The driver fully finishes (and closes) only when `is_finished() && !has_pending_finish()`, i.e. after `shared.finished` — so output/commit is still gated on real background completion.
