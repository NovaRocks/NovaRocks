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

//! Current-process MV target readiness and publication ownership.

use futures::FutureExt;
use std::{
    collections::BTreeMap,
    fmt,
    future::Future,
    mem::ManuallyDrop,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::{
    runtime::Handle,
    sync::{Notify, mpsc},
    task::JoinHandle,
};

/// One event in either of the two original process-wide worker slots.
pub type MvBackgroundFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
pub type MvRefreshBackgroundTask =
    Box<dyn FnMut(MvBackgroundStop, mpsc::Sender<()>) -> MvBackgroundFuture + Send + 'static>;
pub type MvMaintenanceBackgroundTask =
    Box<dyn FnMut(MvBackgroundStop) -> MvBackgroundFuture + Send + 'static>;

pub struct MvBackgroundTasks {
    refresh: MvRefreshBackgroundTask,
    maintenance: MvMaintenanceBackgroundTask,
}
impl MvBackgroundTasks {
    pub fn new(refresh: MvRefreshBackgroundTask, maintenance: MvMaintenanceBackgroundTask) -> Self {
        Self {
            refresh,
            maintenance,
        }
    }
}

/// Receives the same original failure without taking its event or join.
/// The receiver retains the cause before invoking notification callbacks.
/// Production composition connects this to the existing process supervisor.
pub trait MvBackgroundHealthPort: Send + Sync + 'static {
    fn record_failure(&self, cause: Arc<dyn std::error::Error + Send + Sync>);
}

/// One of the two original worker failure cells. Neither reference holds the
/// event, runtime, reporter or client, so process health has no backedge.
struct OriginalWorkerFailure {
    detail: &'static str,
    raw: Mutex<Option<Box<dyn std::any::Any + Send>>>,
    // A failed notification never replaces the original event panic. There is
    // one notification attempt, outside every event and failure lock.
    notification_failure: Mutex<Option<Box<dyn std::any::Any + Send>>>,
}
impl OriginalWorkerFailure {
    fn new(detail: &'static str) -> Arc<Self> {
        Arc::new(Self {
            detail,
            raw: Mutex::new(None),
            notification_failure: Mutex::new(None),
        })
    }
    fn is_failed(&self) -> bool {
        self.raw.lock().unwrap_or_else(|p| p.into_inner()).is_some()
    }
    fn receive(self: &Arc<Self>, raw: Box<dyn std::any::Any + Send>) {
        let mut original = self.raw.lock().unwrap_or_else(|p| p.into_inner());
        assert!(original.is_none(), "same original worker fails once");
        *original = Some(raw);
    }
    fn notify(self: &Arc<Self>, health: &dyn MvBackgroundHealthPort) {
        let cause: Arc<dyn std::error::Error + Send + Sync> = self.clone();
        if let Err(raw) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            health.record_failure(cause);
        })) {
            let mut failed = self
                .notification_failure
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            assert!(failed.is_none(), "one original health notification");
            *failed = Some(raw);
        }
    }
}
impl fmt::Debug for OriginalWorkerFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OriginalWorkerFailure")
            .field("detail", &self.detail)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for OriginalWorkerFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.detail)
    }
}
impl std::error::Error for OriginalWorkerFailure {}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct TestBackgroundHealth {
    causes: Mutex<Vec<Arc<dyn std::error::Error + Send + Sync>>>,
    changed: Notify,
}
#[cfg(test)]
impl MvBackgroundHealthPort for TestBackgroundHealth {
    fn record_failure(&self, cause: Arc<dyn std::error::Error + Send + Sync>) {
        self.causes.lock().unwrap().push(cause);
        self.changed.notify_one();
    }
}
#[cfg(test)]
pub(crate) fn test_health_port() -> Arc<dyn MvBackgroundHealthPort> {
    Arc::new(TestBackgroundHealth::default())
}

/// Stop observation, never an actual-exit proof for an event.
#[derive(Clone, Default)]
pub struct MvBackgroundStop {
    requested: Arc<AtomicBool>,
    notification: Arc<Notify>,
}
impl MvBackgroundStop {
    pub fn is_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }
    pub async fn cancelled(&self) {
        loop {
            let notified = self.notification.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_requested() {
                return;
            }
            notified.await;
        }
    }
    fn request_stop(&self) {
        self.requested.store(true, Ordering::Release);
        self.notification.notify_waiters();
    }
}

enum EventFactory {
    Refresh(MvRefreshBackgroundTask),
    Maintenance(MvMaintenanceBackgroundTask),
}
struct OriginalEvent {
    // A callback whose destructor unwound is not a valid callback again.
    // Keep its original header, but never inspect or destroy that field twice.
    factory: ManuallyDrop<EventFactory>,
    retirement_started: bool,
    future: Option<OriginalEventFuture>,
}
impl Drop for OriginalEvent {
    fn drop(&mut self) {
        if !self.retirement_started {
            self.retirement_started = true;
            // SAFETY: ordinary owner disposition, or successful retirement,
            // destroys this callback once. Poisoned retirement already tried.
            unsafe { ManuallyDrop::drop(&mut self.factory) };
        }
    }
}
/// The same poisoned receiving cell remains if its destructor panics.
struct OriginalEventFuture {
    future: ManuallyDrop<MvBackgroundFuture>,
    drop_started: bool,
}
impl OriginalEventFuture {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        assert!(!self.drop_started, "a poisoned event is never polled again");
        self.future.as_mut().poll(cx)
    }
    fn retire(&mut self) {
        self.drop_started = true;
        // SAFETY: this is the sole explicit destruction, after Ready. If it
        // unwinds, the original slot keeps the poisoned cell and never
        // tries to destroy or poll it again.
        unsafe { ManuallyDrop::drop(&mut self.future) };
    }
}
impl Drop for OriginalEventFuture {
    fn drop(&mut self) {
        if !self.drop_started {
            self.drop_started = true;
            // SAFETY: only an explicit owner disposition reaches this path;
            // normal event retirement has already set drop_started.
            unsafe { ManuallyDrop::drop(&mut self.future) };
        }
    }
}
// Keep the same callback header in place while its loan travels between the
// event task and retirement. Once destruction starts, retirement_started forbids
// invoking or destroying the callback again, including after a destructor panic.
type OriginalEventSlot = Arc<Mutex<Option<Box<OriginalEvent>>>>;
struct EventLoan {
    source: OriginalEventSlot,
    event: Option<Box<OriginalEvent>>,
}
impl EventLoan {
    fn take(source: &OriginalEventSlot) -> Self {
        let event = source.lock().unwrap_or_else(|p| p.into_inner()).take();
        assert!(event.is_some(), "one original worker event borrower");
        Self {
            source: Arc::clone(source),
            event,
        }
    }
    async fn run(&mut self, stop: MvBackgroundStop, wakeup: mpsc::Sender<()>) -> bool {
        let event = self.event.as_mut().unwrap();
        assert!(
            !event.retirement_started,
            "a retired callback is never invoked"
        );
        assert!(event.future.is_none(), "a failed event is not restarted");
        let future = match &mut *event.factory {
            EventFactory::Refresh(factory) => factory(stop.clone(), wakeup),
            EventFactory::Maintenance(factory) => factory(stop.clone()),
        };
        // Installed in the SAME original packet before its first foreign poll.
        event.future = Some(OriginalEventFuture {
            future: ManuallyDrop::new(future),
            drop_started: false,
        });
        let mut stop_observer = Box::pin(stop.cancelled());
        let mut stop_seen = false;
        std::future::poll_fn(|cx| {
            // Wake the same event on process stop without dropping it. Its
            // original Source requests cancellation and drives actual close.
            if !stop_seen && stop_observer.as_mut().poll(cx).is_ready() {
                stop_seen = true;
            }
            event.future.as_mut().unwrap().poll(cx)
        })
        .await;
        event.future.as_mut().unwrap().retire();
        event.future = None;
        // This is the observation before the last foreign poll, not a new
        // read after Ready/Drop that could skip this Source's stopped event.
        stop_seen
    }
}
impl Drop for EventLoan {
    fn drop(&mut self) {
        let mut target = self.source.lock().unwrap_or_else(|p| p.into_inner());
        assert!(target.is_none(), "same event loan returns once");
        *target = self.event.take();
    }
}

/// Original process owner of exactly two serial asynchronous event tasks.
pub struct MvBackgroundRuntime {
    refresh_worker: Option<JoinHandle<()>>,
    maintenance_wakeup_tx: mpsc::Sender<()>,
    maintenance_worker: Option<JoinHandle<()>>,
    stop: MvBackgroundStop,
    refresh_task: Option<OriginalEventSlot>,
    maintenance_task: Option<OriginalEventSlot>,
    refresh_failure: Arc<OriginalWorkerFailure>,
    maintenance_failure: Arc<OriginalWorkerFailure>,
    health: Option<Arc<dyn MvBackgroundHealthPort>>,
}
impl MvBackgroundRuntime {
    /// Compose already-owned tasks, also used by shutdown fixtures.
    pub fn new(
        stop: MvBackgroundStop,
        refresh_worker: JoinHandle<()>,
        maintenance_wakeup_tx: mpsc::Sender<()>,
        maintenance_worker: JoinHandle<()>,
    ) -> Self {
        Self {
            refresh_worker: Some(refresh_worker),
            maintenance_wakeup_tx,
            maintenance_worker: Some(maintenance_worker),
            stop,
            refresh_task: None,
            maintenance_task: None,
            refresh_failure: OriginalWorkerFailure::new("MV refresh worker failed"),
            maintenance_failure: OriginalWorkerFailure::new("MV maintenance worker failed"),
            health: None,
        }
    }
    pub fn start(
        runtime: &Handle,
        health: Arc<dyn MvBackgroundHealthPort>,
        refresh_interval: Duration,
        maintenance_interval: Duration,
        tasks: MvBackgroundTasks,
    ) -> Result<Self, String> {
        // Preserve the original coalescing wake channel, not a job queue.
        let (maintenance_wakeup_tx, mut maintenance_wakeup_rx) = mpsc::channel(1);
        let stop = MvBackgroundStop::default();
        let refresh_task = Arc::new(Mutex::new(Some(Box::new(OriginalEvent {
            factory: ManuallyDrop::new(EventFactory::Refresh(tasks.refresh)),
            retirement_started: false,
            future: None,
        }))));
        let maintenance_task = Arc::new(Mutex::new(Some(Box::new(OriginalEvent {
            factory: ManuallyDrop::new(EventFactory::Maintenance(tasks.maintenance)),
            retirement_started: false,
            future: None,
        }))));
        let refresh_failure = OriginalWorkerFailure::new("MV refresh worker failed");
        let maintenance_failure = OriginalWorkerFailure::new("MV maintenance worker failed");
        let refresh_worker = {
            let source = Arc::clone(&refresh_task);
            let stop = stop.clone();
            let wakeup = maintenance_wakeup_tx.clone();
            let failure = refresh_failure.clone();
            let health = health.clone();
            runtime.spawn(async move {
                loop {
                    let mut event = EventLoan::take(&source);
                    // Stop never drops an in-flight event. Its original Source
                    // consumes stop and continues actual receipts/close.
                    let outcome = std::panic::AssertUnwindSafe(event.run(stop.clone(), wakeup.clone()))
                        .catch_unwind().await;
                    // Restore the SAME original event before notifying a
                    // foreign health observer. No task exit is fabricated.
                    drop(event);
                    let stopped = match outcome {
                        Ok(stopped) => stopped,
                        Err(raw) => {
                            failure.receive(raw);
                            failure.notify(health.as_ref());
                            return;
                        }
                    };
                    if stopped {
                        return;
                    }
                    // Stop first seen after Ready needs the same original
                    // Source's terminal event, including its prior tails.
                    if stop.is_requested() {
                        continue;
                    }
                    tokio::select! {
                        biased;
                        _ = stop.cancelled() => {},
                        _ = tokio::time::sleep(refresh_interval.max(Duration::from_millis(1))) => {},
                    }
                }
            })
        };
        let maintenance_worker = {
            let source = Arc::clone(&maintenance_task);
            let stop = stop.clone();
            let wakeup = maintenance_wakeup_tx.clone();
            let failure = maintenance_failure.clone();
            let health = health.clone();
            runtime.spawn(async move {
                loop {
                    let mut event = EventLoan::take(&source);
                    let outcome = std::panic::AssertUnwindSafe(event.run(stop.clone(), wakeup.clone()))
                        .catch_unwind().await;
                    // Restore the SAME original event before notifying a
                    // foreign health observer. No task exit is fabricated.
                    drop(event);
                    let stopped = match outcome {
                        Ok(stopped) => stopped,
                        Err(raw) => {
                            failure.receive(raw);
                            failure.notify(health.as_ref());
                            return;
                        }
                    };
                    if stopped {
                        return;
                    }
                    // Stop first seen after Ready needs the same original
                    // Source's terminal event, including its prior tails.
                    if stop.is_requested() {
                        continue;
                    }
                    tokio::select! {
                        biased;
                        _ = stop.cancelled() => {},
                        _ = maintenance_wakeup_rx.recv() => {},
                        _ = tokio::time::sleep(maintenance_interval.max(Duration::from_millis(1))) => {},
                    }
                }
            })
        };
        Ok(Self {
            refresh_worker: Some(refresh_worker),
            maintenance_worker: Some(maintenance_worker),
            maintenance_wakeup_tx,
            stop,
            refresh_task: Some(refresh_task),
            maintenance_task: Some(maintenance_task),
            refresh_failure,
            maintenance_failure,
            health: Some(health),
        })
    }
    pub fn maintenance_wakeup(&self) -> mpsc::Sender<()> {
        self.maintenance_wakeup_tx.clone()
    }
    pub fn request_stop(&self) {
        self.stop.request_stop();
    }
    pub async fn stop_and_join_until(&mut self, deadline: Instant) -> Result<(), String> {
        self.request_stop();
        let refresh = wait_and_join(
            &mut self.refresh_worker,
            &self.refresh_failure,
            self.health.as_deref(),
            deadline,
            "MV refresh worker did not stop before the shared shutdown deadline",
            "MV refresh worker panicked during shutdown",
        )
        .await;
        let maintenance = wait_and_join(
            &mut self.maintenance_worker,
            &self.maintenance_failure,
            self.health.as_deref(),
            deadline,
            "MV maintenance worker did not stop before the shared shutdown deadline",
            "MV maintenance worker panicked during shutdown",
        )
        .await;
        match (refresh, maintenance) {
            (Err(primary), _) => return Err(primary),
            (Ok(()), Err(secondary)) => return Err(secondary),
            (Ok(()), Ok(())) => {}
        }
        let refresh = retire_callback(
            &self.refresh_task,
            &self.refresh_failure,
            self.health.as_deref(),
            "MV refresh callback panicked during retirement",
        );
        let maintenance = retire_callback(
            &self.maintenance_task,
            &self.maintenance_failure,
            self.health.as_deref(),
            "MV maintenance callback panicked during retirement",
        );
        // Both task joins and both callback retirements are attempted. A
        // primary failure must not skip the other original owner.
        refresh.and(maintenance)
    }
    #[cfg(test)]
    fn has_refresh_worker(&self) -> bool {
        self.refresh_worker.is_some()
    }
    #[cfg(test)]
    fn has_maintenance_worker(&self) -> bool {
        self.maintenance_worker.is_some()
    }
}
fn retire_callback(
    source: &Option<OriginalEventSlot>,
    original_failure: &Arc<OriginalWorkerFailure>,
    health: Option<&dyn MvBackgroundHealthPort>,
    panic_error: &'static str,
) -> Result<(), String> {
    let Some(source) = source else { return Ok(()) };
    let event = source.lock().unwrap_or_else(|p| p.into_inner()).take();
    let Some(event) = event else { return Ok(()) };
    let mut loan = EventLoan {
        source: Arc::clone(source),
        event: Some(event),
    };
    let event = loan.event.as_mut().unwrap();
    assert!(
        event.future.is_none(),
        "both original event joins completed"
    );
    assert!(
        !event.retirement_started,
        "poisoned retirement keeps its failure"
    );
    event.retirement_started = true;
    // The header is already poisoned before any foreign destructor runs.
    // SAFETY: this is the only callback destruction attempt. An unwind leaves
    // its bytes in ManuallyDrop, never converted back to a usable value.
    let retired = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        ManuallyDrop::drop(&mut event.factory);
    }));
    match retired {
        Ok(()) => {
            // No foreign callback remains. Restore an empty original slot.
            drop(loan.event.take());
            Ok(())
        }
        Err(cause) => {
            original_failure.receive(cause);
            // Restore only the poisoned header before notification, never a
            // live callback or a fictitious actual-exit fact.
            drop(loan);
            if let Some(health) = health {
                original_failure.notify(health);
            }
            Err(panic_error.to_string())
        }
    }
}

async fn wait_and_join(
    worker: &mut Option<JoinHandle<()>>,
    original_failure: &Arc<OriginalWorkerFailure>,
    health: Option<&dyn MvBackgroundHealthPort>,
    deadline: Instant,
    deadline_error: &'static str,
    panic_error: &'static str,
) -> Result<(), String> {
    let Some(handle) = worker.as_mut() else {
        return if original_failure.is_failed() {
            Err(panic_error.to_string())
        } else {
            Ok(())
        };
    };
    let joined =
        match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), handle).await {
            Err(_) => return Err(deadline_error.to_string()),
            Ok(joined) => joined,
        };
    // Await borrowed the original JH. Cancellation/timeout never takes it.
    worker.take();
    match joined {
        // A caught event panic has a normal Tokio footer, but the SAME
        // original failure remains authoritative after joining it.
        Ok(()) if original_failure.is_failed() => Err(panic_error.to_string()),
        Ok(()) => Ok(()),
        Err(error) => {
            let primary: Box<dyn std::any::Any + Send> = if error.is_panic() {
                error.into_panic()
            } else {
                Box::new(error)
            };
            original_failure.receive(primary);
            if let Some(health) = health {
                original_failure.notify(health);
            }
            Err(panic_error.to_string())
        }
    }
}

#[derive(Default)]
pub struct MvBackgroundRuntimeOwner {
    lifecycle: Mutex<BackgroundLifecycle>,
}

#[derive(Default)]
enum BackgroundLifecycle {
    #[default]
    Idle,
    Starting,
    // The same runtime is temporarily loaned outside the mutex for actual
    // joins. Cancellation restores it; nobody may install a second runtime.
    Stopping,
    Running(MvBackgroundRuntime),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvBackgroundRuntimeLifecycleError {
    AlreadyStarting,
    AlreadyRunning,
    StartupInterrupted,
    OwnerChangedDuringShutdown,
}

impl fmt::Display for MvBackgroundRuntimeLifecycleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyStarting => "MV background runtime is already starting",
            Self::AlreadyRunning => "MV background runtime was bound more than once",
            Self::StartupInterrupted => "MV background runtime is still starting during shutdown",
            Self::OwnerChangedDuringShutdown => {
                "MV background runtime owner changed during shutdown"
            }
        })
    }
}

impl std::error::Error for MvBackgroundRuntimeLifecycleError {}

impl MvBackgroundRuntimeOwner {
    pub fn begin_start(
        &self,
    ) -> Result<MvBackgroundRuntimeStart<'_>, MvBackgroundRuntimeLifecycleError> {
        let mut lifecycle = lock_lifecycle(&self.lifecycle)?;
        match &*lifecycle {
            BackgroundLifecycle::Idle => *lifecycle = BackgroundLifecycle::Starting,
            BackgroundLifecycle::Starting => {
                return Err(MvBackgroundRuntimeLifecycleError::AlreadyStarting);
            }
            BackgroundLifecycle::Running(_) | BackgroundLifecycle::Stopping => {
                return Err(MvBackgroundRuntimeLifecycleError::AlreadyRunning);
            }
        }
        Ok(MvBackgroundRuntimeStart {
            owner: self,
            completed: false,
        })
    }

    pub async fn shutdown_until(&self, deadline: Instant) -> Result<(), String> {
        let runtime = {
            let mut lifecycle = self
                .lifecycle
                .lock()
                .map_err(|error| format!("lock MV background runtime lifecycle: {error}"))?;
            match std::mem::replace(&mut *lifecycle, BackgroundLifecycle::Stopping) {
                BackgroundLifecycle::Idle => {
                    *lifecycle = BackgroundLifecycle::Idle;
                    return Ok(());
                }
                BackgroundLifecycle::Starting => {
                    *lifecycle = BackgroundLifecycle::Starting;
                    return Err(MvBackgroundRuntimeLifecycleError::StartupInterrupted.to_string());
                }
                BackgroundLifecycle::Stopping => {
                    return Err(
                        MvBackgroundRuntimeLifecycleError::OwnerChangedDuringShutdown.to_string(),
                    );
                }
                BackgroundLifecycle::Running(runtime) => runtime,
            }
        };
        // Constructed before the first join await; its Drop is a pure move
        // back to this owner, never a detached-worker cleanup approximation.
        let mut original = BackgroundRuntimeLoan {
            owner: self,
            runtime: Some(runtime),
        };
        let result = original
            .runtime
            .as_mut()
            .unwrap()
            .stop_and_join_until(deadline)
            .await;
        if result.is_ok() {
            original.finish();
        }
        result
    }

    pub fn request_stop_for_process_exit(&self) {
        let stop = {
            let lifecycle = self.lifecycle.lock().unwrap_or_else(|p| p.into_inner());
            match &*lifecycle {
                BackgroundLifecycle::Running(runtime) => Some(runtime.stop.clone()),
                _ => None,
            }
        };
        // A foreign wake may panic; the lifecycle owner is already unlocked.
        if let Some(stop) = stop {
            stop.request_stop();
        }
    }
}

struct BackgroundRuntimeLoan<'a> {
    owner: &'a MvBackgroundRuntimeOwner,
    runtime: Option<MvBackgroundRuntime>,
}
impl BackgroundRuntimeLoan<'_> {
    fn finish(&mut self) {
        {
            let mut lifecycle = self
                .owner
                .lifecycle
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            assert!(
                matches!(*lifecycle, BackgroundLifecycle::Stopping),
                "same original runtime shutdown"
            );
            *lifecycle = BackgroundLifecycle::Idle;
        }
        // Actual joins and callback/source destruction have completed. All
        // remaining owner fields are destroyed outside the lifecycle mutex.
        drop(self.runtime.take());
    }
}
impl Drop for BackgroundRuntimeLoan<'_> {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            let mut lifecycle = self
                .owner
                .lifecycle
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            assert!(
                matches!(*lifecycle, BackgroundLifecycle::Stopping),
                "same original runtime loan"
            );
            *lifecycle = BackgroundLifecycle::Running(runtime);
        }
    }
}

pub struct MvBackgroundRuntimeStart<'a> {
    owner: &'a MvBackgroundRuntimeOwner,
    completed: bool,
}

impl MvBackgroundRuntimeStart<'_> {
    pub fn install(
        mut self,
        runtime: MvBackgroundRuntime,
    ) -> Result<(), MvBackgroundRuntimeLifecycleError> {
        let mut lifecycle = lock_lifecycle(&self.owner.lifecycle)?;
        if !matches!(*lifecycle, BackgroundLifecycle::Starting) {
            return Err(MvBackgroundRuntimeLifecycleError::OwnerChangedDuringShutdown);
        }
        *lifecycle = BackgroundLifecycle::Running(runtime);
        self.completed = true;
        Ok(())
    }
}

impl Drop for MvBackgroundRuntimeStart<'_> {
    fn drop(&mut self) {
        if !self.completed
            && let Ok(mut lifecycle) = self.owner.lifecycle.lock()
            && matches!(*lifecycle, BackgroundLifecycle::Starting)
        {
            *lifecycle = BackgroundLifecycle::Idle;
        }
    }
}

fn lock_lifecycle(
    lifecycle: &Mutex<BackgroundLifecycle>,
) -> Result<std::sync::MutexGuard<'_, BackgroundLifecycle>, MvBackgroundRuntimeLifecycleError> {
    lifecycle
        .lock()
        .map_err(|_| MvBackgroundRuntimeLifecycleError::OwnerChangedDuringShutdown)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TargetReadiness {
    Unobserved,
    Ready,
    /// The projection is a sound query candidate, but this process may not
    /// manage the target until its readmission completes. Reading an MV is not
    /// a management operation, so this is deliberately distinct from
    /// `Unavailable`, where the projection itself is in doubt.
    ReadOnly(String),
    Unavailable(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeAttempt<P> {
    pub publication_id: P,
}

pub struct ProcessRuntime<T, P> {
    inner: Mutex<BTreeMap<T, RuntimeEntry<P>>>,
}

impl<T, P> Default for ProcessRuntime<T, P> {
    fn default() -> Self {
        Self {
            inner: Mutex::new(BTreeMap::new()),
        }
    }
}

/// Ordering is process-local and shared by every logical target spelling owner.
/// It is not a durable fence or evidence that a remote effect has completed.
#[derive(Default, Debug)]
pub(crate) struct ProjectionOrder {
    pub generation: u64,
    pub installed: Option<crate::repository::MvProjectionVersion>,
    /// The generation of the observation currently in flight, if any.
    ///
    /// A reservation clears `installed` before it reads the provider, because
    /// what the process knows about the target stops being current the moment
    /// a new observation starts. It then releases this cell for the duration
    /// of that read, so a reader arriving in between would otherwise conclude
    /// that no successful observation exists -- which is true only in the same
    /// sense that a letter in the post has not arrived. This says which it is,
    /// so a reader can wait for the observation it would otherwise misreport.
    pub pending: Option<u64>,
    /// Woken when a reservation settles, so waiters do not poll.
    pub settled: std::sync::Arc<tokio::sync::Notify>,
}

impl ProjectionOrder {
    /// Release an in-flight observation, if this generation still owns it.
    ///
    /// A superseded reservation must not clear the marker: the generation that
    /// replaced it owns the in-flight state, and its waiters are waiting for
    /// that one.
    pub fn settle(&mut self, generation: u64) {
        if self.pending == Some(generation) {
            self.pending = None;
            self.settled.notify_waiters();
        }
    }

    /// Discard an in-flight observation because a new ordered event replaced
    /// it. Its own completion will find itself superseded and settle nothing,
    /// so the marker is released here instead.
    pub fn supersede(&mut self) {
        if self.pending.take().is_some() {
            self.settled.notify_waiters();
        }
    }
}

impl ProjectionOrder {
    pub fn advance(&mut self) -> Result<u64, crate::repository::MvRepositoryError> {
        self.generation = self.generation.checked_add(1).ok_or_else(|| {
            crate::repository::MvRepositoryError::new(
                crate::repository::MvRepositoryErrorKind::Unavailable,
                "MV projection generation exhausted",
            )
        })?;
        Ok(self.generation)
    }
}

#[derive(Clone, Debug)]
struct RuntimeEntry<P> {
    projection_order: Arc<tokio::sync::Mutex<ProjectionOrder>>,
    readiness: TargetReadiness,
    active: Option<RuntimeAttempt<P>>,
}

impl<P> Default for RuntimeEntry<P> {
    fn default() -> Self {
        Self {
            projection_order: Arc::new(tokio::sync::Mutex::new(ProjectionOrder::default())),
            readiness: TargetReadiness::Unobserved,
            active: None,
        }
    }
}

impl<T, P> ProcessRuntime<T, P>
where
    T: Clone + Ord,
    P: Copy + Eq,
{
    pub(crate) fn projection_order(&self, target: T) -> Arc<tokio::sync::Mutex<ProjectionOrder>> {
        Arc::clone(
            &self
                .inner
                .lock()
                .expect("MV application runtime lock poisoned")
                .entry(target)
                .or_default()
                .projection_order,
        )
    }

    pub(crate) fn projection_targets(&self) -> Vec<T> {
        self.inner
            .lock()
            .expect("MV application runtime lock poisoned")
            .keys()
            .cloned()
            .collect()
    }

    pub fn readiness(&self, target: &T) -> TargetReadiness {
        self.inner
            .lock()
            .expect("MV application runtime lock poisoned")
            .get(target)
            .map(|entry| entry.readiness.clone())
            .unwrap_or(TargetReadiness::Unobserved)
    }

    /// Inspect a readiness value while its runtime owner still lends it.
    /// A bounded listing can refuse a large reason before cloning it.
    pub(crate) fn with_readiness<R>(
        &self,
        target: &T,
        inspect: impl FnOnce(&TargetReadiness) -> R,
    ) -> R {
        let entries = self
            .inner
            .lock()
            .expect("MV application runtime lock poisoned");
        inspect(
            entries
                .get(target)
                .map(|entry| &entry.readiness)
                .unwrap_or(&TargetReadiness::Unobserved),
        )
    }

    pub(crate) fn set_unavailable(&self, target: T, reason: String) {
        self.inner
            .lock()
            .expect("MV application runtime lock poisoned")
            .entry(target)
            .or_default()
            .readiness = TargetReadiness::Unavailable(reason);
    }

    pub(crate) fn set_read_only(&self, target: T, reason: String) {
        self.inner
            .lock()
            .expect("MV application runtime lock poisoned")
            .entry(target)
            .or_default()
            .readiness = TargetReadiness::ReadOnly(reason);
    }

    pub(crate) fn set_ready(&self, target: T) {
        self.inner
            .lock()
            .expect("MV application runtime lock poisoned")
            .entry(target)
            .or_default()
            .readiness = TargetReadiness::Ready;
    }

    pub fn begin(&self, target: T, publication_id: P) -> bool {
        let mut entries = self
            .inner
            .lock()
            .expect("MV application runtime lock poisoned");
        let entry = entries.entry(target).or_default();
        if entry.active.is_some() {
            return false;
        }
        entry.active = Some(RuntimeAttempt { publication_id });
        true
    }

    pub fn finish(&self, target: &T, publication_id: P) {
        let mut entries = self
            .inner
            .lock()
            .expect("MV application runtime lock poisoned");
        if let Some(entry) = entries.get_mut(target)
            && entry
                .active
                .as_ref()
                .is_some_and(|active| active.publication_id == publication_id)
        {
            entry.active = None;
        }
    }

    pub fn has_active_publications(&self) -> bool {
        self.inner
            .lock()
            .expect("MV application runtime lock poisoned")
            .values()
            .any(|entry| entry.active.is_some())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };

    use super::{MvBackgroundRuntime, MvBackgroundRuntimeOwner, ProcessRuntime, TargetReadiness};

    #[test]
    fn runtime_tracks_one_publication_and_readiness_per_target() {
        let runtime = ProcessRuntime::<String, u64>::default();
        let target = "ice.analytics.orders_mv".to_string();
        assert!(runtime.begin(target.clone(), 7));
        assert!(!runtime.begin(target.clone(), 8));
        runtime.set_unavailable(target.clone(), "accelerator projection failed".into());
        assert!(matches!(
            runtime.readiness(&target),
            TargetReadiness::Unavailable(_)
        ));
        runtime.finish(&target, 7);
        assert!(!runtime.has_active_publications());
        runtime.set_ready(target.clone());
        assert_eq!(runtime.readiness(&target), TargetReadiness::Ready);
    }

    fn empty_tasks() -> super::MvBackgroundTasks {
        super::MvBackgroundTasks::new(
            Box::new(|_, _| Box::pin(async {})),
            Box::new(|_| Box::pin(async {})),
        )
    }
    fn from_workers(
        refresh: tokio::task::JoinHandle<()>,
        maintenance: tokio::task::JoinHandle<()>,
    ) -> MvBackgroundRuntime {
        let (wakeup, _receiver) = tokio::sync::mpsc::channel(1);
        MvBackgroundRuntime::new(
            super::MvBackgroundStop::default(),
            refresh,
            wakeup,
            maintenance,
        )
    }

    struct OriginalStopWait {
        stop: super::MvBackgroundStop,
        entered: Option<tokio::sync::oneshot::Sender<()>>,
        dropped: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl std::future::Future for OriginalStopWait {
        type Output = ();
        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<()> {
            if let Some(entered) = self.entered.take() {
                let _ = entered.send(());
            }
            // The event Source observes state but deliberately owns no stop
            // waiter. The original Runtime must wake this same in-flight F.
            if self.stop.is_requested() {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        }
    }
    impl Drop for OriginalStopWait {
        fn drop(&mut self) {
            self.dropped
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    #[tokio::test(flavor = "current_thread")]
    async fn stop_wakes_both_original_pending_events_and_observes_their_actual_drop() {
        let (refresh_entered, refresh_started) = tokio::sync::oneshot::channel();
        let (maintenance_entered, maintenance_started) = tokio::sync::oneshot::channel();
        let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let refresh_dropped = dropped.clone();
        let maintenance_dropped = dropped.clone();
        let mut refresh_entered = Some(refresh_entered);
        let mut maintenance_entered = Some(maintenance_entered);
        let health = Arc::new(super::TestBackgroundHealth::default());
        let mut runtime = MvBackgroundRuntime::start(
            &tokio::runtime::Handle::current(),
            health.clone(),
            Duration::from_secs(3600),
            Duration::from_secs(3600),
            super::MvBackgroundTasks::new(
                Box::new(move |stop, _| {
                    Box::pin(OriginalStopWait {
                        stop,
                        entered: refresh_entered.take(),
                        dropped: refresh_dropped.clone(),
                    })
                }),
                Box::new(move |stop| {
                    Box::pin(OriginalStopWait {
                        stop,
                        entered: maintenance_entered.take(),
                        dropped: maintenance_dropped.clone(),
                    })
                }),
            ),
        )
        .unwrap();
        refresh_started.await.unwrap();
        maintenance_started.await.unwrap();
        assert_eq!(dropped.load(std::sync::atomic::Ordering::SeqCst), 0);
        runtime
            .stop_and_join_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(dropped.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert!(!runtime.has_refresh_worker());
        assert!(!runtime.has_maintenance_worker());
        assert!(health.causes.lock().unwrap().is_empty());
    }
    #[tokio::test(flavor = "current_thread")]
    async fn stop_during_cadence_runs_the_same_original_source_terminal_event_before_join() {
        let turns = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let source_turns = turns.clone();
        let (entered, started) = tokio::sync::oneshot::channel();
        let mut entered = Some(entered);
        let mut runtime = MvBackgroundRuntime::start(
            &tokio::runtime::Handle::current(),
            crate::process_runtime::test_health_port(),
            Duration::from_secs(3600),
            Duration::from_secs(3600),
            super::MvBackgroundTasks::new(
                Box::new(move |stop, _| {
                    let turn = source_turns.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if turn == 0 {
                        entered.take().unwrap().send(()).unwrap();
                        assert!(!stop.is_requested());
                    } else {
                        assert!(stop.is_requested());
                    }
                    Box::pin(async {})
                }),
                Box::new(|_| Box::pin(async {})),
            ),
        )
        .unwrap();
        started.await.unwrap();
        assert_eq!(turns.load(std::sync::atomic::Ordering::SeqCst), 1);
        runtime
            .stop_and_join_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(turns.load(std::sync::atomic::Ordering::SeqCst), 2);
    }
    #[tokio::test(flavor = "current_thread")]
    async fn stop_after_event_ready_still_drains_the_same_original_terminal_event() {
        use std::future::Future;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct StopAfterReady {
            stop: super::MvBackgroundStop,
            ready: bool,
        }
        impl Future for StopAfterReady {
            type Output = ();
            fn poll(
                mut self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<()> {
                assert!(!self.stop.is_requested());
                self.ready = true;
                std::task::Poll::Ready(())
            }
        }
        impl Drop for StopAfterReady {
            fn drop(&mut self) {
                assert!(self.ready);
                // Exactly after the event's final poll, before Runtime sees
                // its Ready result. No extra test poll supplies this stop.
                self.stop.request_stop();
            }
        }

        let turns = Arc::new(AtomicUsize::new(0));
        let source_turns = turns.clone();
        let exits = Arc::new(AtomicUsize::new(0));
        let source_exits = exits.clone();
        let (entered, seen) = tokio::sync::oneshot::channel();
        let (release, closed) = tokio::sync::oneshot::channel();
        let mut terminal = Some((entered, closed));
        let mut runtime = MvBackgroundRuntime::start(
            &tokio::runtime::Handle::current(),
            crate::process_runtime::test_health_port(),
            Duration::from_secs(3600),
            Duration::from_secs(3600),
            super::MvBackgroundTasks::new(
                Box::new(move |stop, _| -> super::MvBackgroundFuture {
                    if source_turns.fetch_add(1, Ordering::SeqCst) == 0 {
                        return Box::pin(StopAfterReady { stop, ready: false });
                    }
                    assert!(stop.is_requested());
                    let (entered, closed) = terminal.take().expect("one stopped final event");
                    let exits = source_exits.clone();
                    Box::pin(async move {
                        entered.send(()).unwrap();
                        closed.await.unwrap();
                        exits.fetch_add(1, Ordering::SeqCst);
                    })
                }),
                Box::new(|_| Box::pin(async {})),
            ),
        )
        .unwrap();
        let entered = tokio::time::timeout(Duration::from_secs(1), seen)
            .await
            .is_ok();
        let mut wait =
            Box::pin(runtime.stop_and_join_until(Instant::now() + Duration::from_secs(1)));
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        let pending = wait.as_mut().poll(&mut cx).is_pending();
        drop(wait);
        let not_exited = exits.load(Ordering::SeqCst) == 0;
        let released = release.send(()).is_ok();
        let joined = runtime
            .stop_and_join_until(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(entered && pending && not_exited && released && joined.is_ok());
        assert_eq!(turns.load(Ordering::SeqCst), 2);
        assert_eq!(exits.load(Ordering::SeqCst), 1);
        assert!(!runtime.has_refresh_worker());
        assert!(!runtime.has_maintenance_worker());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn callback_drop_panic_keeps_the_same_poisoned_header_and_raw_cause() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct PanicOnDrop {
            cause: Arc<u64>,
            drops: Arc<AtomicUsize>,
        }
        impl Drop for PanicOnDrop {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
                std::panic::resume_unwind(Box::new(self.cause.clone()));
            }
        }
        struct DropProbe(Arc<AtomicUsize>);
        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let cause = Arc::new(41_u64);
        let drops = Arc::new(AtomicUsize::new(0));
        let secondary_drops = Arc::new(AtomicUsize::new(0));
        let primary = PanicOnDrop {
            cause: cause.clone(),
            drops: drops.clone(),
        };
        let secondary = DropProbe(secondary_drops.clone());
        let health = Arc::new(super::TestBackgroundHealth::default());
        let mut runtime = MvBackgroundRuntime::start(
            &tokio::runtime::Handle::current(),
            health.clone(),
            Duration::from_secs(3600),
            Duration::from_secs(3600),
            super::MvBackgroundTasks::new(
                Box::new(move |_, _| {
                    let _original = &primary;
                    Box::pin(async {})
                }),
                Box::new(move |_| {
                    let _original = &secondary;
                    Box::pin(async {})
                }),
            ),
        )
        .unwrap();
        let header = runtime.refresh_task.as_ref().unwrap().clone();
        let first = runtime
            .stop_and_join_until(Instant::now() + Duration::from_secs(1))
            .await;
        let repeat = runtime
            .stop_and_join_until(Instant::now() + Duration::from_secs(1))
            .await;
        let raw_same = runtime
            .refresh_failure
            .raw
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|raw| raw.downcast_ref::<Arc<u64>>())
            .is_some_and(|raw| Arc::ptr_eq(raw, &cause));
        let poisoned = header
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|event| event.retirement_started && event.future.is_none());
        assert!(first.is_err() && repeat.is_err() && raw_same && poisoned);
        assert_eq!(health.causes.lock().unwrap().len(), 1);
        assert!(!runtime.has_refresh_worker());
        assert!(!runtime.has_maintenance_worker());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(secondary_drops.load(Ordering::SeqCst), 1);
        drop(runtime);
        drop(header);
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "half-dropped callback is never destroyed twice"
        );
    }

    #[tokio::test]
    async fn product_runtime_retains_blocked_worker_join_for_retry() {
        let (release, receiver) = tokio::sync::oneshot::channel();
        let mut runtime = from_workers(
            tokio::spawn(async move {
                let _ = receiver.await;
            }),
            tokio::spawn(async {}),
        );
        let identity = runtime.refresh_worker.as_ref().unwrap().id();
        let error = runtime
            .stop_and_join_until(Instant::now() + Duration::from_millis(10))
            .await
            .expect_err("blocked MV worker must respect the shared deadline");
        assert!(error.contains("shared shutdown deadline"));
        assert_eq!(runtime.refresh_worker.as_ref().unwrap().id(), identity);
        release.send(()).unwrap();
        runtime
            .stop_and_join_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("the retained MV worker join remains retryable");
        assert!(!runtime.has_refresh_worker());
        assert!(!runtime.has_maintenance_worker());
    }
    #[tokio::test]
    async fn owner_exclusively_installs_and_joins_product_runtime() {
        let owner = MvBackgroundRuntimeOwner::default();
        let start = owner.begin_start().expect("first start reserves the owner");
        assert!(owner.begin_start().is_err());
        let runtime = MvBackgroundRuntime::start(
            &tokio::runtime::Handle::current(),
            crate::process_runtime::test_health_port(),
            Duration::from_secs(60),
            Duration::from_secs(60),
            empty_tasks(),
        )
        .unwrap();
        start.install(runtime).expect("reservation installs once");
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("product owner joins both workers");
    }
    #[tokio::test]
    async fn product_runtime_wakes_maintenance_without_a_poll_sleep() {
        let (refresh_seen, mut refresh_receive) = tokio::sync::mpsc::unbounded_channel();
        let (maintenance_seen, mut maintenance_receive) = tokio::sync::mpsc::unbounded_channel();
        let mut runtime = MvBackgroundRuntime::start(
            &tokio::runtime::Handle::current(),
            crate::process_runtime::test_health_port(),
            Duration::from_secs(60),
            Duration::from_secs(60),
            super::MvBackgroundTasks::new(
                Box::new(move |_, _| {
                    refresh_seen.send(()).unwrap();
                    Box::pin(async {})
                }),
                Box::new(move |_| {
                    maintenance_seen.send(()).unwrap();
                    Box::pin(async {})
                }),
            ),
        )
        .unwrap();
        tokio::time::timeout(Duration::from_secs(1), refresh_receive.recv())
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), maintenance_receive.recv())
            .await
            .unwrap()
            .unwrap();
        runtime.maintenance_wakeup().send(()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), maintenance_receive.recv())
            .await
            .unwrap()
            .unwrap();
        runtime
            .stop_and_join_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("woken workers observe the product stop signal");
    }
    async fn observes_original_panic_before_stop(factory_panics: bool) {
        use std::sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        };
        struct Probe(Arc<AtomicUsize>);
        impl Drop for Probe {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        type OriginalEventSlot = Mutex<Option<Box<super::OriginalEvent>>>;
        struct InspectHealth {
            expected: Arc<u64>,
            event: Mutex<Option<std::sync::Weak<OriginalEventSlot>>>,
            received: Mutex<Vec<(bool, bool)>>,
            changed: tokio::sync::Notify,
        }
        impl super::MvBackgroundHealthPort for InspectHealth {
            fn record_failure(&self, cause: Arc<dyn std::error::Error + Send + Sync>) {
                let original = cause
                    .downcast_ref::<super::OriginalWorkerFailure>()
                    .unwrap();
                let same = original
                    .raw
                    .lock()
                    .unwrap()
                    .as_ref()
                    .and_then(|raw| raw.downcast_ref::<Arc<u64>>())
                    .is_some_and(|raw| Arc::ptr_eq(raw, &self.expected));
                // Observation happens inside the actual notification callback,
                // before the worker returns its Tokio footer.
                let event = self
                    .event
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .upgrade()
                    .unwrap();
                let restored = event.try_lock().unwrap().is_some();
                self.received.lock().unwrap().push((same, restored));
                self.changed.notify_one();
            }
        }
        let original = Arc::new(771_u64);
        let health = Arc::new(InspectHealth {
            expected: original.clone(),
            event: Mutex::new(None),
            received: Mutex::new(Vec::new()),
            changed: tokio::sync::Notify::new(),
        });
        let drops = Arc::new(AtomicUsize::new(0));
        let probe = Probe(drops.clone());
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let mut runtime = MvBackgroundRuntime::start(
            &tokio::runtime::Handle::current(),
            health.clone(),
            Duration::from_secs(60),
            Duration::from_secs(60),
            super::MvBackgroundTasks::new(
                Box::new(move |_, _| {
                    let _probe = &probe;
                    counted.fetch_add(1, Ordering::SeqCst);
                    let original = original.clone();
                    if factory_panics {
                        std::panic::resume_unwind(Box::new(original));
                    }
                    Box::pin(async move {
                        std::panic::resume_unwind(Box::new(original));
                    })
                }),
                Box::new(|_| Box::pin(async {})),
            ),
        )
        .unwrap();
        *health.event.lock().unwrap() =
            Some(Arc::downgrade(runtime.refresh_task.as_ref().unwrap()));
        tokio::time::timeout(Duration::from_secs(1), health.changed.notified())
            .await
            .unwrap();
        let before_stop = health.received.lock().unwrap().as_slice() == [(true, true)]
            && !runtime.stop.is_requested()
            && runtime.has_refresh_worker()
            && runtime.has_maintenance_worker()
            && calls.load(Ordering::SeqCst) == 1
            && drops.load(Ordering::SeqCst) == 0;
        let first = runtime
            .stop_and_join_until(Instant::now() + Duration::from_secs(1))
            .await;
        let repeat = runtime
            .stop_and_join_until(Instant::now() + Duration::from_secs(1))
            .await;
        let joined = !runtime.has_refresh_worker() && !runtime.has_maintenance_worker();
        let held = drops.load(Ordering::SeqCst) == 0;
        drop(runtime);
        assert!(before_stop && first.is_err() && repeat.is_err() && joined && held);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(health.received.lock().unwrap().as_slice(), [(true, true)]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn factory_panic_reports_the_restored_original_before_stop() {
        observes_original_panic_before_stop(true).await;
    }
    #[tokio::test(flavor = "current_thread")]
    async fn event_poll_panic_reports_the_restored_original_before_stop() {
        observes_original_panic_before_stop(false).await;
    }

    #[tokio::test]
    async fn failed_original_worker_keeps_callback_and_raw_cause_after_actual_join() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        struct DropProbe(Arc<AtomicUsize>);
        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let source_drops = Arc::new(AtomicUsize::new(0));
        let cause_drops = Arc::new(AtomicUsize::new(0));
        let source = Arc::new(DropProbe(Arc::clone(&source_drops)));
        let source_view = Arc::downgrade(&source);
        let mut cause = Some(DropProbe(Arc::clone(&cause_drops)));
        let (entered, receive) = tokio::sync::oneshot::channel();
        let mut entered = Some(entered);
        let mut runtime = MvBackgroundRuntime::start(
            &tokio::runtime::Handle::current(),
            crate::process_runtime::test_health_port(),
            Duration::from_secs(60),
            Duration::from_secs(60),
            super::MvBackgroundTasks::new(
                Box::new(|_, _| Box::pin(async {})),
                Box::new(move |_| {
                    let source = Arc::clone(&source);
                    let entered = entered.take().unwrap();
                    let cause = cause.take().unwrap();
                    Box::pin(async move {
                        let _source = source;
                        entered.send(()).unwrap();
                        std::panic::panic_any(cause);
                    })
                }),
            ),
        )
        .unwrap();
        let entered = tokio::time::timeout(Duration::from_secs(1), receive)
            .await
            .is_ok();
        let exit = runtime
            .stop_and_join_until(Instant::now() + Duration::from_secs(1))
            .await;
        let first_cause = runtime
            .maintenance_failure
            .raw
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|p| p.is::<DropProbe>());
        let repeat = runtime
            .stop_and_join_until(Instant::now() + Duration::from_secs(1))
            .await;
        let success = entered
            && exit.is_err()
            && repeat.is_err()
            && first_cause
            && !runtime.has_maintenance_worker()
            && source_view.upgrade().is_some()
            && source_drops.load(Ordering::SeqCst) == 0
            && cause_drops.load(Ordering::SeqCst) == 0;
        if !success {
            std::panic::panic_any(runtime);
        }
        drop(runtime);
        assert_eq!(source_drops.load(Ordering::SeqCst), 1);
        assert_eq!(cause_drops.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn refresh_panic_does_not_skip_the_finished_maintenance_join() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        struct Cause(Arc<AtomicUsize>);
        impl Drop for Cause {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let cause_drops = Arc::new(AtomicUsize::new(0));
        let callback_drops = Arc::new(AtomicUsize::new(0));
        let source = Arc::new(Cause(Arc::clone(&callback_drops)));
        let source_view = Arc::downgrade(&source);
        let mut primary = Some(Cause(Arc::clone(&cause_drops)));
        let (finished, receive) = tokio::sync::oneshot::channel();
        let mut finished = Some(finished);
        let mut runtime = MvBackgroundRuntime::start(
            &tokio::runtime::Handle::current(),
            crate::process_runtime::test_health_port(),
            Duration::from_secs(60),
            Duration::from_secs(60),
            super::MvBackgroundTasks::new(
                Box::new(move |_, _| {
                    let primary = primary.take().unwrap();
                    Box::pin(async move { std::panic::panic_any(primary) })
                }),
                Box::new(move |_| {
                    let _source = &source;
                    if let Some(finished) = finished.take() {
                        finished.send(()).unwrap();
                    }
                    Box::pin(async {})
                }),
            ),
        )
        .unwrap();
        let entered = tokio::time::timeout(Duration::from_secs(1), receive).await;
        let exit = runtime
            .stop_and_join_until(Instant::now() + Duration::from_secs(1))
            .await;
        let both_joined = !runtime.has_refresh_worker() && !runtime.has_maintenance_worker();
        let cause_retained = runtime
            .refresh_failure
            .raw
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|cause| cause.is::<Cause>());
        let repeat = runtime
            .stop_and_join_until(Instant::now() + Duration::from_secs(1))
            .await;
        if !(entered.is_ok()
            && exit.is_err()
            && repeat.is_err()
            && both_joined
            && cause_retained
            && source_view.upgrade().is_some()
            && cause_drops.load(Ordering::SeqCst) == 0
            && callback_drops.load(Ordering::SeqCst) == 0)
        {
            std::panic::panic_any(runtime);
        }
        drop(runtime);
        assert_eq!(cause_drops.load(Ordering::SeqCst), 1);
        assert_eq!(callback_drops.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn cancelled_shutdown_restores_the_same_two_joins_and_callback() {
        use std::future::Future;
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        struct Source(Arc<AtomicUsize>);
        impl Drop for Source {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let drops = Arc::new(AtomicUsize::new(0));
        let source = Arc::new(Source(Arc::clone(&drops)));
        let source_view = Arc::downgrade(&source);
        let (refresh_entered, refresh_seen) = tokio::sync::oneshot::channel();
        let (maintenance_entered, maintenance_seen) = tokio::sync::oneshot::channel();
        let (release_refresh, refresh_release) = tokio::sync::oneshot::channel();
        let (release_maintenance, maintenance_release) = tokio::sync::oneshot::channel();
        let mut refresh = Some((refresh_entered, refresh_release));
        let mut maintenance = Some((maintenance_entered, maintenance_release));
        let runtime = MvBackgroundRuntime::start(
            &tokio::runtime::Handle::current(),
            crate::process_runtime::test_health_port(),
            Duration::from_secs(60),
            Duration::from_secs(60),
            super::MvBackgroundTasks::new(
                Box::new(move |_, _| {
                    let (entered, release) = refresh.take().unwrap();
                    Box::pin(async move {
                        entered.send(()).unwrap();
                        let _ = release.await;
                    })
                }),
                Box::new(move |_| {
                    let source = Arc::clone(&source);
                    let (entered, release) = maintenance.take().unwrap();
                    Box::pin(async move {
                        let _source = source;
                        entered.send(()).unwrap();
                        let _ = release.await;
                    })
                }),
            ),
        )
        .unwrap();
        let identities = (
            runtime.refresh_worker.as_ref().unwrap().id(),
            runtime.maintenance_worker.as_ref().unwrap().id(),
        );
        let owner = MvBackgroundRuntimeOwner::default();
        owner.begin_start().unwrap().install(runtime).unwrap();
        let entered = tokio::time::timeout(Duration::from_secs(1), async {
            refresh_seen.await.unwrap();
            maintenance_seen.await.unwrap();
        })
        .await
        .is_ok();
        let mut wait = Box::pin(owner.shutdown_until(Instant::now() + Duration::from_secs(1)));
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        let pending = wait.as_mut().poll(&mut cx).is_pending();
        drop(wait);
        let restored = {
            let lifecycle = owner.lifecycle.lock().unwrap();
            match &*lifecycle {
                super::BackgroundLifecycle::Running(runtime) => {
                    runtime.refresh_worker.as_ref().map(|h| h.id()) == Some(identities.0)
                        && runtime.maintenance_worker.as_ref().map(|h| h.id()) == Some(identities.1)
                        && runtime.maintenance_task.is_some()
                }
                _ => false,
            }
        };
        let retained = source_view.upgrade().is_some() && drops.load(Ordering::SeqCst) == 0;
        let refresh_released = release_refresh.send(()).is_ok();
        let maintenance_released = release_maintenance.send(()).is_ok();
        let joined = owner
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await;
        let closed = matches!(
            *owner.lifecycle.lock().unwrap(),
            super::BackgroundLifecycle::Idle
        );
        if !(entered
            && pending
            && restored
            && retained
            && refresh_released
            && maintenance_released
            && joined.is_ok()
            && closed
            && drops.load(Ordering::SeqCst) == 1)
        {
            std::panic::panic_any(owner);
        }
    }
}
