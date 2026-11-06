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
//! - Gives write-path I/O a dedicated runtime so it does not share the query
//!   `data_runtime` directly.
//! - Every service is explicitly owned by an `ExecutionRuntime`; no lookup
//!   reaches an application configuration singleton or a shared global runtime.
//!
//! Key exported interfaces:
//! - Types: `IoExecutor`, `IoExecutorMetrics`, `ExecutionServices`.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use novarocks_types::WORKER_STACK_SIZE_BYTES;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

use crate::runtime::execution_runtime::ExecutionRuntimeConfig;
use crate::runtime::task::ExecutionFailure;
use crate::runtime::task::io::{TaskIoError, TaskIoErrorKind, TaskIoOperation};
use crate::runtime::task_control::TaskExecutionControl;

const SINK_IO_THREAD_NAME: &str = "novarocks-sink-io";

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

struct OwnedRuntime {
    runtime: Option<Runtime>,
}

impl OwnedRuntime {
    fn runtime(&self) -> &Runtime {
        self.runtime
            .as_ref()
            .expect("owned execution I/O runtime is available while referenced")
    }
}

impl Drop for OwnedRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            // `Runtime`'s normal Drop blocks, which Tokio forbids from an
            // asynchronous task. The application owns this runtime, but
            // fragment/test teardown can release its final Arc on either side
            // of that boundary, so shutdown must be non-blocking here.
            runtime.shutdown_background();
        }
    }
}

#[derive(Clone)]
enum Spawner {
    /// Owns a dedicated runtime (sink_io).
    Owned(Arc<OwnedRuntime>),
}

/// A service gate rejection, not a query resource quota or an exit receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IoSpawnRefused;
impl fmt::Display for IoSpawnRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("sink I/O admission is closed")
    }
}
impl std::error::Error for IoSpawnRefused {}
impl novarocks_failure_contract::FailureProjection for IoSpawnRefused {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(&TaskIoError::new(
            TaskIoOperation::Lookup,
            TaskIoErrorKind::Unavailable,
            self.to_string(),
        ))
    }
}
impl From<IoSpawnRefused> for ExecutionFailure {
    fn from(error: IoSpawnRefused) -> Self {
        Self::from_domain(error)
    }
}

/// Waiting stopped at the existing shutdown deadline. The original live actor
/// count remains unchanged, and its actual futures still own their exits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IoShutdownTimeout {
    pub timeout: Duration,
    pub live_actors: usize,
}
impl fmt::Display for IoShutdownTimeout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "sink I/O shutdown did not observe {} actor exits within {:?}",
            self.live_actors, self.timeout
        )
    }
}
impl std::error::Error for IoShutdownTimeout {}
impl novarocks_failure_contract::FailureProjection for IoShutdownTimeout {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(&TaskIoError::new(
            TaskIoOperation::Lookup,
            TaskIoErrorKind::Timeout,
            self.to_string(),
        ))
    }
}
impl From<IoShutdownTimeout> for ExecutionFailure {
    fn from(error: IoShutdownTimeout) -> Self {
        Self::from_domain(error)
    }
}

struct IoLifecycleState {
    open: bool,
    live: usize,
}
struct IoLifecycle {
    state: Mutex<IoLifecycleState>,
    changed: Condvar,
}
impl IoLifecycle {
    fn new() -> Self {
        Self {
            state: Mutex::new(IoLifecycleState {
                open: true,
                live: 0,
            }),
            changed: Condvar::new(),
        }
    }
    fn admit(&self) -> Result<(), IoSpawnRefused> {
        let mut state = self.state.lock().expect("sink I/O lifecycle lock");
        if !state.open {
            return Err(IoSpawnRefused);
        }
        state.live = state
            .live
            .checked_add(1)
            .expect("actual sink I/O actor count fits usize");
        Ok(())
    }
    fn exited(&self) {
        let mut state = self.state.lock().expect("sink I/O lifecycle lock");
        state.live = state
            .live
            .checked_sub(1)
            .expect("one actual sink I/O exit per actor");
        self.changed.notify_all();
    }
}

/// Only this future's actual destruction returns its service shutdown count.
/// It retains the neutral lifecycle state, never the runtime or an IoExecutor.
struct LiveFuture<F> {
    inner: Option<Pin<Box<F>>>,
    lifecycle: Arc<IoLifecycle>,
    metrics: Arc<IoExecutorMetrics>,
    enqueued: Instant,
    started: bool,
    active: bool,
    destroying: bool,
}
impl<F> LiveFuture<F> {
    fn destroy_and_exit(&mut self) {
        self.destroying = true;
        drop(self.inner.take());
        self.destroying = false;
        self.lifecycle.exited();
        self.active = false;
    }
}
impl<F: Future> Future for LiveFuture<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        assert!(
            this.active && !this.destroying,
            "sink I/O work polled after exit or failed destruction"
        );
        if !this.started {
            this.metrics
                .wait_time_ns_total
                .fetch_add(this.enqueued.elapsed().as_nanos() as u64, Ordering::Relaxed);
            this.metrics.started.fetch_add(1, Ordering::Relaxed);
            this.started = true;
        }
        match this
            .inner
            .as_mut()
            .expect("live sink I/O future")
            .as_mut()
            .poll(cx)
        {
            Poll::Pending => Poll::Pending,
            Poll::Ready(output) => {
                this.destroy_and_exit();
                this.metrics.completed.fetch_add(1, Ordering::Relaxed);
                Poll::Ready(output)
            }
        }
    }
}
impl<F> Drop for LiveFuture<F> {
    fn drop(&mut self) {
        if self.active && !self.destroying {
            self.destroy_and_exit();
        }
        // A destructor panic leaves live unchanged. No independent RAII guard
        // may publish an exit while Rust unwinds a failed inner destruction.
    }
}

/// A uniform handle to one execution service. Cloneable; cheap to pass around.
#[derive(Clone)]
pub struct IoExecutor {
    spawner: Spawner,
    metrics: Arc<IoExecutorMetrics>,
    lifecycle: Arc<IoLifecycle>,
}

impl IoExecutor {
    fn new(spawner: Spawner) -> Self {
        Self {
            spawner,
            metrics: Arc::new(IoExecutorMetrics::default()),
            lifecycle: Arc::new(IoLifecycle::new()),
        }
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
            Spawner::Owned(rt) => rt.runtime().spawn(fut),
        }
    }

    /// Submit one Task-owned actor to this service. The Task owner joins its
    /// actor; this process-level count is only the shutdown wait inventory.
    pub fn spawn<F>(
        &self,
        _owner: &TaskExecutionControl,
        fut: F,
    ) -> Result<JoinHandle<F::Output>, IoSpawnRefused>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.lifecycle.admit()?;
        self.metrics.submitted.fetch_add(1, Ordering::Relaxed);
        Ok(self.spawn_on(LiveFuture {
            inner: Some(Box::pin(fut)),
            lifecycle: self.lifecycle.clone(),
            metrics: self.metrics.clone(),
            enqueued: Instant::now(),
            started: false,
            active: true,
            destroying: false,
        }))
    }

    pub(crate) fn close_admission(&self) {
        let mut state = self
            .lifecycle
            .state
            .lock()
            .expect("sink I/O lifecycle lock");
        state.open = false;
    }
    pub(crate) fn wait_live_exit(&self, timeout: Duration) -> Result<(), IoShutdownTimeout> {
        let deadline = Instant::now() + timeout;
        let mut state = self
            .lifecycle
            .state
            .lock()
            .expect("sink I/O lifecycle lock");
        while state.live != 0 {
            let now = Instant::now();
            if now >= deadline {
                return Err(IoShutdownTimeout {
                    timeout,
                    live_actors: state.live,
                });
            }
            let (next, _) = self
                .lifecycle
                .changed
                .wait_timeout(state, deadline.saturating_duration_since(now))
                .expect("sink I/O exit wait");
            state = next;
        }
        Ok(())
    }

    /// Clone this executor onto a fresh, isolated metrics counter while reusing
    /// the same underlying spawner. Test-only: lets accounting assertions be
    /// deterministic regardless of other tasks sharing the global service.
    #[cfg(test)]
    fn with_isolated_metrics(&self) -> Self {
        Self {
            spawner: self.spawner.clone(),
            metrics: Arc::new(IoExecutorMetrics::default()),
            lifecycle: self.lifecycle.clone(),
        }
    }

    /// Submit a fallible test actor through the same Task-qualified entry.
    #[cfg(test)]
    fn spawn_fallible<F, T, E>(
        &self,
        owner: &TaskExecutionControl,
        fut: F,
    ) -> Result<JoinHandle<Result<T, E>>, IoSpawnRefused>
    where
        F: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        let metrics = self.metrics.clone();
        self.spawn(owner, async move {
            let out = fut.await;
            if out.is_err() {
                metrics.errors.fetch_add(1, Ordering::Relaxed);
            }
            out
        })
    }
}

/// I/O services owned by one explicit execution runtime.
pub struct ExecutionServices {
    sink_io: IoExecutor,
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
        Ok(Self {
            sink_io: IoExecutor::new(Spawner::Owned(sink_rt)),
        })
    }

    pub fn sink_io(&self) -> &IoExecutor {
        &self.sink_io
    }
}

fn build_runtime(
    thread_name: &'static str,
    worker_threads: usize,
    max_blocking_threads: usize,
) -> Result<Arc<OwnedRuntime>, String> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(worker_threads)
        .max_blocking_threads(max_blocking_threads)
        .thread_name(thread_name)
        .thread_stack_size(WORKER_STACK_SIZE_BYTES)
        .build()
        .map_err(|e| format!("init execution I/O runtime {thread_name} failed: {e}"))?;
    Ok(Arc::new(OwnedRuntime {
        runtime: Some(runtime),
    }))
}

/// Unit tests may exercise a deliberately local error/health domain. Their
/// service submission still uses the explicit development-only Task token;
/// this adapter is absent from ordinary dev and release library targets.
#[cfg(test)]
pub(crate) fn test_io_control() -> TaskExecutionControl {
    crate::runtime::task_control::test_support::Owner::new()
        .view(novarocks_execution_contract::TaskFailurePhase::Execution)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::execution_runtime::ExecutionRuntimeConfig;

    fn services() -> ExecutionServices {
        ExecutionServices::new(&ExecutionRuntimeConfig {
            driver_threads: 1,
            exchange_io_max_inflight_bytes: 1,
            exchange_max_transmit_batched_bytes: 1,
            operator_buffer_chunks: 1,
            local_exchange_buffer_mem_limit_per_driver: 1,
            local_exchange_max_buffered_rows: 1,
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
        let handle = services
            .sink_io()
            .spawn(&test_io_control(), async {
                std::thread::current()
                    .name()
                    .map(|s| s.to_string())
                    .unwrap_or_default()
            })
            .unwrap();
        let name = futures::executor::block_on(handle).expect("join");
        assert!(
            name.contains("novarocks-sink-io"),
            "sink_io task ran on unexpected thread: {name}"
        );
    }

    #[test]
    fn spawn_accounts_submit_start_complete() {
        let executor = services().sink_io().with_isolated_metrics();
        let before = executor.metrics().snapshot();
        let handle = executor
            .spawn(&test_io_control(), async { 42_u32 })
            .unwrap();
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
        let handle = executor
            .spawn_fallible(&test_io_control(), async {
                Err::<(), String>("boom".to_string())
            })
            .unwrap();
        let out = futures::executor::block_on(handle).expect("join");
        assert!(out.is_err());
        let after = executor.metrics().snapshot();
        assert_eq!(after.errors, before.errors + 1);
    }

    #[test]
    fn services_can_drop_from_an_async_context() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        runtime.block_on(async {
            let services = services();
            let handle = services
                .sink_io()
                .spawn(&test_io_control(), async { 7_u8 })
                .unwrap();
            assert_eq!(handle.await.expect("join"), 7);
            drop(services);
        });
    }

    #[test]
    fn closing_admission_refuses_actors_without_changing_original_metrics_or_live() {
        let services = services();
        let executor = services.sink_io();
        executor.close_admission();
        let before = executor.metrics().snapshot();
        let result = executor.spawn(&test_io_control(), async {});
        assert!(matches!(result, Err(IoSpawnRefused)));
        assert_eq!(executor.metrics().snapshot(), before);
        assert_eq!(executor.lifecycle.state.lock().unwrap().live, 0);
        assert!(executor.wait_live_exit(Duration::ZERO).is_ok());
    }

    #[test]
    fn sink_io_close_waits_for_actual_actor_and_capture_destruction() {
        struct Capture {
            entered: Option<std::sync::mpsc::Sender<()>>,
            release: std::sync::mpsc::Receiver<()>,
            destroyed: Arc<std::sync::atomic::AtomicBool>,
        }
        impl Drop for Capture {
            fn drop(&mut self) {
                self.entered.take().unwrap().send(()).unwrap();
                self.release.recv().unwrap();
                self.destroyed.store(true, Ordering::Release);
            }
        }
        struct ReadyAfterCapture(Capture);
        impl Future for ReadyAfterCapture {
            type Output = ();
            fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
                let _original = &self.0;
                Poll::Ready(())
            }
        }
        let services = services();
        let executor = services.sink_io();
        let (entered, at_drop) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        let destroyed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let handle = executor
            .spawn(
                &test_io_control(),
                ReadyAfterCapture(Capture {
                    entered: Some(entered),
                    release: released,
                    destroyed: destroyed.clone(),
                }),
            )
            .unwrap();
        at_drop.recv_timeout(Duration::from_secs(5)).unwrap();
        executor.close_admission();
        assert_eq!(
            executor
                .wait_live_exit(Duration::ZERO)
                .unwrap_err()
                .live_actors,
            1
        );
        assert!(!destroyed.load(Ordering::Acquire));
        release.send(()).unwrap();
        executor.wait_live_exit(Duration::from_secs(5)).unwrap();
        assert!(destroyed.load(Ordering::Acquire));
        futures::executor::block_on(handle).unwrap();
    }

    #[test]
    fn sink_io_deadline_stops_waiting_without_refunding_a_live_actor() {
        let services = services();
        let executor = services.sink_io();
        let (release, released) = tokio::sync::oneshot::channel::<()>();
        let handle = executor
            .spawn(&test_io_control(), async {
                let _ = released.await;
            })
            .unwrap();
        executor.close_admission();
        let failure = executor.wait_live_exit(Duration::ZERO).unwrap_err();
        assert_eq!(
            failure,
            IoShutdownTimeout {
                timeout: Duration::ZERO,
                live_actors: 1
            }
        );
        assert_eq!(executor.lifecycle.state.lock().unwrap().live, 1);
        assert!(executor.spawn(&test_io_control(), async {}).is_err());
        release.send(()).unwrap();
        futures::executor::block_on(handle).unwrap();
        executor.wait_live_exit(Duration::ZERO).unwrap();
        assert_eq!(executor.lifecycle.state.lock().unwrap().live, 0);
    }

    struct ExitProbe {
        lifecycle: Arc<IoLifecycle>,
        destroyed: Arc<std::sync::atomic::AtomicBool>,
        panic: bool,
    }
    impl Drop for ExitProbe {
        fn drop(&mut self) {
            assert_eq!(
                self.lifecycle.state.lock().unwrap().live,
                1,
                "the original actor is still counted while its captures destruct"
            );
            self.destroyed.store(true, Ordering::Release);
            assert!(!self.panic, "injected sink I/O capture destructor panic");
        }
    }
    struct ProbeFuture {
        capture: ExitProbe,
        pending: bool,
        entered: Option<std::sync::mpsc::Sender<()>>,
    }
    impl Future for ProbeFuture {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            let _original = &self.capture;
            if let Some(entered) = self.entered.take() {
                entered.send(()).unwrap();
            }
            if self.pending {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        }
    }
    fn unqueued_probe(
        lifecycle: &Arc<IoLifecycle>,
        future: ProbeFuture,
    ) -> LiveFuture<ProbeFuture> {
        lifecycle.admit().unwrap();
        LiveFuture {
            inner: Some(Box::pin(future)),
            lifecycle: lifecycle.clone(),
            metrics: Arc::new(IoExecutorMetrics::default()),
            enqueued: Instant::now(),
            started: false,
            active: true,
            destroying: false,
        }
    }

    #[test]
    fn queued_and_polled_wrapper_drop_destroy_captures_before_returning_live() {
        for polled in [false, true] {
            let lifecycle = Arc::new(IoLifecycle::new());
            let destroyed = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let mut future = unqueued_probe(
                &lifecycle,
                ProbeFuture {
                    capture: ExitProbe {
                        lifecycle: lifecycle.clone(),
                        destroyed: destroyed.clone(),
                        panic: false,
                    },
                    pending: true,
                    entered: None,
                },
            );
            if polled {
                assert!(
                    Pin::new(&mut future)
                        .poll(&mut Context::from_waker(std::task::Waker::noop()))
                        .is_pending()
                );
            }
            drop(future);
            assert!(destroyed.load(Ordering::Acquire));
            assert_eq!(lifecycle.state.lock().unwrap().live, 0);
        }
    }

    #[test]
    fn tokio_abort_returns_live_only_after_the_actor_capture_actually_drops() {
        let services = services();
        let executor = services.sink_io();
        let destroyed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (entered, started) = std::sync::mpsc::channel();
        let handle = executor
            .spawn(
                &test_io_control(),
                ProbeFuture {
                    capture: ExitProbe {
                        lifecycle: executor.lifecycle.clone(),
                        destroyed: destroyed.clone(),
                        panic: false,
                    },
                    pending: true,
                    entered: Some(entered),
                },
            )
            .unwrap();
        started.recv_timeout(Duration::from_secs(5)).unwrap();
        handle.abort();
        assert!(
            futures::executor::block_on(handle)
                .unwrap_err()
                .is_cancelled()
        );
        assert!(destroyed.load(Ordering::Acquire));
        assert_eq!(executor.lifecycle.state.lock().unwrap().live, 0);
    }

    #[test]
    fn explicit_capture_destructor_panic_keeps_original_live_unresolved() {
        for ready in [false, true] {
            let lifecycle = Arc::new(IoLifecycle::new());
            let destroyed = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let mut future = unqueued_probe(
                &lifecycle,
                ProbeFuture {
                    capture: ExitProbe {
                        lifecycle: lifecycle.clone(),
                        destroyed: destroyed.clone(),
                        panic: true,
                    },
                    pending: !ready,
                    entered: None,
                },
            );
            if ready {
                assert!(
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        Pin::new(&mut future)
                            .poll(&mut Context::from_waker(std::task::Waker::noop()))
                    }))
                    .is_err()
                );
                drop(future);
            } else {
                assert!(
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(future)))
                        .is_err()
                );
            }
            assert!(destroyed.load(Ordering::Acquire));
            assert_eq!(
                lifecycle.state.lock().unwrap().live,
                1,
                "an explicit destructor panic cannot publish an actor exit"
            );
        }
    }

    #[test]
    fn execution_runtime_shutdown_seals_its_original_sink_service_and_all_clones() {
        let runtime = crate::runtime::execution_runtime::test_execution_runtime();
        let executor = runtime.services().sink_io().clone();
        runtime.shutdown_sink_io().unwrap();
        assert!(matches!(
            executor.spawn(&test_io_control(), async {}),
            Err(IoSpawnRefused)
        ));
        runtime.shutdown_sink_io().unwrap();
        assert_eq!(executor.lifecycle.state.lock().unwrap().live, 0);
    }
}
