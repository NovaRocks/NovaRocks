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

//! Speculative Iceberg data input. Metadata decisions stay on the scan CPU;
//! the shared range service remains the only owner of physical read dispatch.

use std::sync::{Arc, Mutex};

use futures::FutureExt;
use novarocks_fs::{
    BoundFile, FileCancellation, FileRangeBinding, FileRangeClass, FileRangeControl,
    FileRangeStart, FileReadContext, FileReadRange, FileTask, PreparedFileInput,
};
use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::read_stack::{
    ConnectorPreparationControl, ConnectorPreparationProgress,
};
use tokio::sync::Notify;

use crate::file_reader::map_file_error;

pub(super) struct PlannedInput {
    pub file: BoundFile,
    pub range: FileReadRange,
}

type Planner =
    dyn Fn(FileReadContext) -> Result<Option<PlannedInput>, ConnectorError> + Send + Sync;

enum Phase {
    New,
    Planned(Option<PlannedInput>),
    Reading,
    Ready,
    Failed,
    Stopped,
}

struct State {
    generation: u64,
    phase: Phase,
    paused: bool,
    active_jobs: usize,
    /// The service allocates exactly this requested capacity before dispatch.
    reserved_bytes: u64,
    ready_input: Option<PreparedFileInput>,
    present_input: Option<PreparedFileInput>,
    failure: Option<ConnectorError>,
    cancellation: Option<FileCancellation>,
    request_control: Option<FileRangeControl>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            generation: 0,
            phase: Phase::New,
            paused: false,
            active_jobs: 0,
            reserved_bytes: 0,
            ready_input: None,
            present_input: None,
            failure: None,
            cancellation: None,
            request_control: None,
        }
    }
}

struct Shared {
    state: Mutex<State>,
    watcher: Mutex<Option<FileTask>>,
    changed: Notify,
}

#[derive(Default)]
struct GroupState {
    paused: bool,
    stopped: bool,
    reclaim_epoch: u64,
    held_input: Option<PreparedFileInput>,
    controls: Vec<Arc<dyn ConnectorPreparationControl>>,
}

/// Stable control for the lifetime of one active source. A row-group candidate
/// can retire while its physical request is still draining; replacing the
/// candidate never makes those bytes disappear from the source's B ledger.
pub(super) struct SuccessorPreparationGroup {
    state: Mutex<GroupState>,
}

impl SuccessorPreparationGroup {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(GroupState::default()),
        }
    }

    pub fn add(&self, control: Arc<dyn ConnectorPreparationControl>) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.controls.retain(|existing| !existing.is_drained());
        if state.stopped {
            control.request_stop();
        } else if state.paused {
            control.request_pause();
        }
        state.controls.push(control);
    }

    pub fn reclaim_epoch(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .reclaim_epoch
    }

    pub fn hold_input(&self, input: PreparedFileInput) {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .held_input = Some(input);
    }

    pub fn take_input(&self) -> Option<PreparedFileInput> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .held_input
            .take()
    }

    pub fn input(&self) -> Option<PreparedFileInput> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .held_input
            .clone()
    }

    pub fn undrained_candidate_count(&self) -> usize {
        self.controls()
            .iter()
            .filter(|control| !control.is_drained())
            .count()
    }

    fn controls(&self) -> Vec<Arc<dyn ConnectorPreparationControl>> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .controls
            .clone()
    }
}

impl ConnectorPreparationControl for SuccessorPreparationGroup {
    fn request_pause(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.paused = true;
        for control in &state.controls {
            control.request_pause();
        }
    }

    fn request_resume(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.paused = false;
        for control in &state.controls {
            control.request_resume();
        }
    }

    fn request_reclaim(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.reclaim_epoch = state.reclaim_epoch.wrapping_add(1);
        state.held_input = None;
        for control in &state.controls {
            control.request_reclaim();
        }
    }

    fn request_stop(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.stopped = true;
        state.held_input = None;
        for control in &state.controls {
            control.request_stop();
        }
    }

    fn retained_input_bytes(&self) -> u64 {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let held = state
            .held_input
            .as_ref()
            .map_or(0, |input| input.retained_backing_capacity() as u64);
        state.controls.iter().fold(held, |sum, control| {
            sum.saturating_add(control.retained_input_bytes())
        })
    }

    fn is_drained(&self) -> bool {
        self.controls().iter().all(|control| control.is_drained())
    }

    fn wait_drained(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let controls = self.controls();
            futures::future::join_all(controls.iter().map(|control| control.wait_drained())).await;
        })
    }
}

impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn finish_job(&self, generation: u64, observed_failure: Option<ConnectorError>) -> bool {
        let mut state = self.lock();
        state.active_jobs = state.active_jobs.saturating_sub(1);
        let current = state.generation == generation && !matches!(state.phase, Phase::Stopped);
        if !current {
            state.reserved_bytes = 0;
            // A reclaim cancels this generation, but a non-cancellation I/O
            // verdict can already have won the race. Preserve that verdict
            // for the ordered successor instead of retrying it silently.
            if !matches!(state.phase, Phase::Stopped)
                && let Some(error) = observed_failure
                && error.kind() != novarocks_spi::connector::ConnectorErrorKind::Cancelled
            {
                state.failure.get_or_insert(error);
                state.phase = Phase::Failed;
            }
        }
        if current {
            state.request_control = None;
        }
        self.changed.notify_waiters();
        current
    }
}

impl ConnectorPreparationControl for Shared {
    fn request_pause(&self) {
        let mut state = self.lock();
        state.paused = true;
        if let Some(control) = &state.request_control {
            control.request_pause();
        }
    }

    fn request_resume(&self) {
        let mut state = self.lock();
        state.paused = false;
        if let Some(control) = &state.request_control {
            control.request_resume();
        }
        self.changed.notify_waiters();
    }

    fn request_reclaim(&self) {
        let mut state = self.lock();
        state.generation = state.generation.wrapping_add(1);
        if let Some(cancellation) = state.cancellation.take() {
            cancellation.cancel();
        }
        state.request_control = None;
        state.ready_input = None;
        state.present_input = None;
        // A real preparation error belongs to the ordered successor. A long
        // pause can discard bytes, but cannot erase that typed verdict before
        // the successor is promoted (or discarded by query completion).
        if !matches!(state.phase, Phase::Failed) {
            state.phase = Phase::New;
        }
        if state.active_jobs == 0 {
            state.reserved_bytes = 0;
        }
        self.changed.notify_waiters();
    }

    fn request_stop(&self) {
        let mut state = self.lock();
        state.generation = state.generation.wrapping_add(1);
        if let Some(cancellation) = state.cancellation.take() {
            cancellation.cancel();
        }
        state.request_control = None;
        state.ready_input = None;
        state.present_input = None;
        state.failure = None;
        state.phase = Phase::Stopped;
        if state.active_jobs == 0 {
            state.reserved_bytes = 0;
        }
        self.changed.notify_waiters();
    }

    fn retained_input_bytes(&self) -> u64 {
        let state = self.lock();
        state
            .reserved_bytes
            .saturating_add(
                state
                    .present_input
                    .as_ref()
                    .map_or(0, |input| input.retained_backing_capacity() as u64),
            )
            .saturating_add(
                state
                    .ready_input
                    .as_ref()
                    .map_or(0, |input| input.retained_backing_capacity() as u64),
            )
    }

    fn is_drained(&self) -> bool {
        let state = self.lock();
        state.active_jobs == 0 && state.reserved_bytes == 0
    }

    fn wait_drained(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            loop {
                let notified = self.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.is_drained() {
                    return;
                }
                notified.await;
            }
        })
    }
}

/// One bounded candidate. Reclaim discards its input without affecting the
/// active source and allows the same immutable planner to re-arm later.
pub(super) struct PreparedRangeCandidate {
    context: FileReadContext,
    range: FileRangeBinding,
    planner: Arc<Planner>,
    allow_partial: bool,
    shared: Arc<Shared>,
}

impl PreparedRangeCandidate {
    pub fn new(context: FileReadContext, planner: Arc<Planner>) -> Option<Self> {
        Self::new_with_present(context, planner, None, true)
    }

    pub fn new_with_present(
        context: FileReadContext,
        planner: Arc<Planner>,
        present_input: Option<PreparedFileInput>,
        allow_partial: bool,
    ) -> Option<Self> {
        let mut state = State::default();
        state.present_input = present_input;
        Some(Self {
            range: context.range.clone()?,
            context,
            planner,
            allow_partial,
            shared: Arc::new(Shared {
                state: Mutex::new(state),
                watcher: Mutex::new(None),
                changed: Notify::new(),
            }),
        })
    }

    pub fn control(&self) -> Arc<dyn ConnectorPreparationControl> {
        Arc::clone(&self.shared) as Arc<dyn ConnectorPreparationControl>
    }

    pub fn advance(
        &mut self,
        remaining_input_bytes: u64,
    ) -> Result<ConnectorPreparationProgress, ConnectorError> {
        let mut state = self.shared.lock();
        if state.paused || state.active_jobs > 0 && matches!(state.phase, Phase::New) {
            return Ok(ConnectorPreparationProgress::Pending);
        }
        match &state.phase {
            Phase::New => {
                let mut context = self.context.clone();
                context.cancellation = self.context.cancellation.child();
                match (self.planner)(context) {
                    Ok(planned) => state.phase = Phase::Planned(planned),
                    Err(error) => {
                        state.failure = Some(error);
                        state.phase = Phase::Failed;
                    }
                }
                Ok(ConnectorPreparationProgress::Pending)
            }
            Phase::Reading => Ok(ConnectorPreparationProgress::Pending),
            Phase::Ready | Phase::Failed => Ok(ConnectorPreparationProgress::Ready),
            Phase::Stopped => Ok(ConnectorPreparationProgress::Deferred),
            Phase::Planned(planned) => {
                let Some(planned) = planned.as_ref() else {
                    state.phase = Phase::Ready;
                    return Ok(ConnectorPreparationProgress::Ready);
                };
                let (offset, available) = match planned.range {
                    FileReadRange::Bounded { offset, length } => (offset, length),
                    FileReadRange::WholeFile => (0, planned.file.identity().file_size()),
                };
                let length = if !self.allow_partial {
                    if available > remaining_input_bytes {
                        return Ok(ConnectorPreparationProgress::Deferred);
                    }
                    available
                } else {
                    available.min(8 * 1024 * 1024).min(remaining_input_bytes)
                };
                if length == 0 {
                    return Ok(ConnectorPreparationProgress::Deferred);
                }
                let cancellation = self.context.cancellation.child();
                let range = FileReadRange::bounded(offset, length).map_err(map_file_error)?;
                let file = planned.file.clone();
                let present_bytes = state
                    .present_input
                    .as_ref()
                    .map_or(0, |input| input.retained_backing_capacity() as u64);
                let retained_after_start = length.checked_add(present_bytes).ok_or_else(|| {
                    ConnectorError::new(
                        novarocks_spi::connector::ConnectorErrorKind::Internal,
                        "prepared range capacity overflow",
                    )
                })?;
                // Publish the new target reservation before FS allocates it.
                // The optional old tail remains separately accounted until
                // `try_start_with_present` finishes its bounded copy.
                state.reserved_bytes = length;
                let start = self
                    .range
                    .try_start_with_present(
                        FileRangeClass::Prefetch,
                        file,
                        range,
                        cancellation.clone(),
                        state.present_input.clone(),
                    )
                    .map_err(map_file_error);
                let start = match start {
                    Ok(start) => start,
                    Err(error) => {
                        state.reserved_bytes = 0;
                        state.failure = Some(error);
                        state.phase = Phase::Failed;
                        return Ok(ConnectorPreparationProgress::Ready);
                    }
                };
                let FileRangeStart::Started(mut request) = start else {
                    state.reserved_bytes = 0;
                    return Ok(ConnectorPreparationProgress::Deferred);
                };
                let generation = state.generation;
                state.cancellation = Some(cancellation);
                state.request_control = Some(request.control());
                state.present_input = None;
                // The request still owns the old present backing while its
                // final target is in flight, even after this state releases it.
                state.reserved_bytes = retained_after_start;
                state.active_jobs += 1;
                state.phase = Phase::Reading;
                drop(state);
                let weak = Arc::downgrade(&self.shared);
                let spawn = self.context.task_spawner.spawn(Box::pin(async move {
                    let result = std::panic::AssertUnwindSafe(request.prepared_input_ready())
                        .catch_unwind()
                        .await;
                    if result.is_err() {
                        request.request_stop();
                    }
                    let drain = std::panic::AssertUnwindSafe(request.drained())
                        .catch_unwind()
                        .await;
                    let result = match result {
                        Ok(result) => result.map_err(map_file_error),
                        Err(_) => Err(ConnectorError::new(
                            novarocks_spi::connector::ConnectorErrorKind::Internal,
                            "prepared range result task panicked",
                        )),
                    };
                    let drain = match drain {
                        Ok(result) => result.map_err(map_file_error),
                        Err(_) => Err(ConnectorError::new(
                            novarocks_spi::connector::ConnectorErrorKind::Internal,
                            "prepared range drain task panicked",
                        )),
                    };
                    let Some(shared) = weak.upgrade() else {
                        return;
                    };
                    let observed_failure = result
                        .as_ref()
                        .err()
                        .or_else(|| drain.as_ref().err())
                        .cloned();
                    if shared.finish_job(generation, observed_failure) {
                        let mut state = shared.lock();
                        state.reserved_bytes = 0;
                        match (result, drain) {
                            (Ok(input), Ok(())) => {
                                state.ready_input = Some(input);
                                state.phase = Phase::Ready;
                            }
                            (Err(error), _) | (_, Err(error)) => {
                                state.failure = Some(error);
                                state.phase = Phase::Failed;
                            }
                        }
                        shared.changed.notify_waiters();
                    }
                }));
                match spawn {
                    Ok(task) => {
                        let mut watcher = self
                            .shared
                            .watcher
                            .lock()
                            .unwrap_or_else(|error| error.into_inner());
                        *watcher = Some(task);
                    }
                    Err(error) => {
                        let mut state = self.shared.lock();
                        state.active_jobs = state.active_jobs.saturating_sub(1);
                        state.reserved_bytes = 0;
                        state.request_control = None;
                        state.failure = Some(map_file_error(error));
                        state.phase = Phase::Failed;
                        self.shared.changed.notify_waiters();
                        return Ok(ConnectorPreparationProgress::Ready);
                    }
                }
                Ok(ConnectorPreparationProgress::Pending)
            }
        }
    }

    pub fn take_ready(&mut self) -> Result<Option<PreparedFileInput>, ConnectorError> {
        let mut state = self.shared.lock();
        if let Some(error) = state.failure.take() {
            return Err(error);
        }
        let input = state.ready_input.take();
        if input.is_some() {
            state.phase = Phase::Stopped;
        }
        Ok(input)
    }
}

impl Drop for PreparedRangeCandidate {
    fn drop(&mut self) {
        self.shared.request_stop();
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::time::{Duration, Instant};

    use novarocks_fs::{
        FileIdentity, FileIoRuntime, FileRangeScope, FileRangeService, FileTaskSpawner,
        FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner,
    };
    use novarocks_spi::connector::StorageAccessDomainId;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn prefetch_respects_remaining_capacity_and_transfers_exact_input() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let path = dir.path().join("input.parquet");
        std::fs::write(&path, b"abcdefghijklmnop").expect("write input");
        let location = path.to_string_lossy().to_string();
        let access = FsAccessResolver::new()
            .resolve_location(
                StorageAccessDomainId::from_bytes([7; 32]),
                location.clone(),
                None,
            )
            .expect("resolve input");
        let file = access
            .bind(0, FileIdentity::new(location, 16, None))
            .expect("bind input");
        let task_spawner: Arc<dyn FileTaskSpawner> =
            Arc::new(TokioFileTaskSpawner::new(tokio::runtime::Handle::current()));
        let service = FileRangeService::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            Arc::clone(&task_spawner),
            tokio::runtime::Handle::current(),
        );
        let context = FileReadContext {
            cancellation: FileCancellation::new(),
            deadline: Some(Instant::now() + Duration::from_secs(10)),
            runtime: Arc::new(TokioFileIoRuntime::new(tokio::runtime::Handle::current()))
                as Arc<dyn FileIoRuntime>,
            task_spawner,
            range: Some(service.bind(
                FileRangeScope::try_new(1, 0, 1, 2, 0, 3).unwrap(),
                novarocks_spi::connector::read_stack::ConnectorSourceOperations::new(),
            )),
        };
        let planner = Arc::new(move |_context: FileReadContext| {
            Ok(Some(PlannedInput {
                file: file.clone(),
                range: FileReadRange::bounded(0, 16).map_err(map_file_error)?,
            }))
        });
        let mut candidate = PreparedRangeCandidate::new(context, planner).expect("candidate");
        let control = candidate.control();
        let input = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let progress = candidate.advance(4).expect("advance candidate");
                assert!(candidate.control().retained_input_bytes() <= 4);
                if progress == ConnectorPreparationProgress::Ready {
                    return candidate.take_ready().expect("ready input");
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            let state = candidate.shared.lock();
            panic!(
                "candidate readiness: phase={} jobs={} reserved={} paused={}",
                match state.phase {
                    Phase::New => "new",
                    Phase::Planned(_) => "planned",
                    Phase::Reading => "reading",
                    Phase::Ready => "ready",
                    Phase::Failed => "failed",
                    Phase::Stopped => "stopped",
                },
                state.active_jobs,
                state.reserved_bytes,
                state.paused,
            );
        })
        .expect("physical prepared input");
        assert_eq!(input.range(), 0..4);
        assert_eq!(input.retained_backing_capacity(), 4);
        assert_eq!(candidate.control().retained_input_bytes(), 0);
        drop(candidate);
        control.wait_drained().await;
        assert!(control.is_drained());
    }

    #[tokio::test]
    async fn reclaim_preserves_a_real_error_and_wakes_actual_drain_waiter() {
        let shared = Arc::new(Shared {
            state: Mutex::new(State::default()),
            watcher: Mutex::new(None),
            changed: Notify::new(),
        });
        {
            let mut state = shared.lock();
            state.active_jobs = 1;
            state.reserved_bytes = 8;
            state.phase = Phase::Failed;
            state.failure = Some(ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::CorruptData,
                "saved candidate error",
            ));
        }
        shared.request_reclaim();
        {
            let state = shared.lock();
            assert!(matches!(state.phase, Phase::Failed));
            assert!(state.failure.is_some());
            assert_eq!(state.reserved_bytes, 8);
        }
        let waiter = tokio::spawn({
            let shared = Arc::clone(&shared);
            async move { shared.wait_drained().await }
        });
        {
            let mut state = shared.lock();
            state.active_jobs = 0;
            state.reserved_bytes = 0;
        }
        shared.changed.notify_waiters();
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("actual drain wake")
            .expect("waiter task");
    }

    #[test]
    fn reclaim_keeps_a_physical_error_published_after_generation_changed() {
        let shared = Shared {
            state: Mutex::new(State::default()),
            watcher: Mutex::new(None),
            changed: Notify::new(),
        };
        {
            let mut state = shared.lock();
            state.phase = Phase::Reading;
            state.active_jobs = 1;
            state.reserved_bytes = 8;
        }
        shared.request_reclaim();
        assert!(!shared.finish_job(
            0,
            Some(ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::CorruptData,
                "physical corruption before cancellation",
            )),
        ));
        let state = shared.lock();
        assert!(matches!(state.phase, Phase::Failed));
        assert_eq!(
            state.failure.as_ref().map(ConnectorError::kind),
            Some(novarocks_spi::connector::ConnectorErrorKind::CorruptData),
        );
        assert_eq!(state.reserved_bytes, 0);
        drop(state);
        assert!(shared.is_drained());
    }
}
