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

//! Role-local original Apply join ownership, including raw-failure retirement.
//! It introduces no Connector Work/ResultWindow authority.
use super::transport_supervisor::NativeTransportEncodingPermit;
use crate::task_execution::intent::TaskOperationQueuePermit;
use novarocks_task_codec::TransportBudget;
use std::{
    any::Any,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
    time::Instant,
};
use tokio::{
    runtime::Handle,
    sync::Notify,
    task::{Id, JoinError, JoinHandle},
};

type RawValue = Box<dyn Any + Send>;

pub(crate) struct ApplyReservations {
    _queue: Vec<Box<dyn TaskOperationQueuePermit>>,
    _encoding: NativeTransportEncodingPermit,
}
impl ApplyReservations {
    pub(crate) fn new(
        queue: Vec<Box<dyn TaskOperationQueuePermit>>,
        encoding: NativeTransportEncodingPermit,
    ) -> Self {
        Self {
            _queue: queue,
            _encoding: encoding,
        }
    }
}

#[cfg(test)]
use super::original_retirement::CleanupState;
use super::original_retirement::OriginalRetirement as Retirement;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct JoinFacts {
    pub(crate) id: Id,
    pub(crate) panic: bool,
    pub(crate) cancelled: bool,
}
impl JoinFacts {
    fn of(original: &JoinError) -> Self {
        Self {
            id: original.id(),
            panic: original.is_panic(),
            cancelled: original.is_cancelled(),
        }
    }
}
mod canonical_reply;
#[cfg(test)]
pub(crate) use canonical_reply::canonical_dispatch_rejection_fixture;
pub(crate) use canonical_reply::{CanonicalAcknowledgement, OriginalReplyPublisher};
use canonical_reply::{OriginalNativeAcceptance, ReservationBacking};
type OriginalFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Requested payload layouts of the actual original Apply holder. This does
/// not estimate a Tokio task, platform mutex backing or captured pointees.
#[derive(Clone, Copy, Debug)]
pub(crate) struct OriginalInputLayouts {
    pub(crate) future: std::alloc::Layout,
    pub(crate) input: std::alloc::Layout,
}
pub(crate) fn original_input_layouts<F: Future<Output = ()>>()
-> Result<OriginalInputLayouts, novarocks_workload_control::WorkError> {
    use novarocks_type_contract::owned_resources::layout::arc_layout;
    use novarocks_workload_control::WorkError;
    use std::alloc::Layout;
    Ok(OriginalInputLayouts {
        future: Layout::new::<F>(),
        input: arc_layout(Layout::new::<OriginalInput>())
            .map_err(|_| WorkError::ArithmeticOverflow)?,
    })
}

// The one original future stays in the record even if Handle::spawn unwinds.
// This is the ORIGINAL send task, not a second observer or retry task.
struct OriginalInput {
    future: Mutex<Option<OriginalFuture>>,
    raw_cause: Mutex<Option<RawValue>>,
    backing: Arc<ReservationBacking>,
}
struct OriginalTask {
    input: Arc<OriginalInput>,
}
impl Future for OriginalTask {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<()> {
        let mut future = self.input.future.lock().unwrap_or_else(|e| e.into_inner());
        future
            .as_mut()
            .expect("one original Apply future")
            .as_mut()
            .poll(cx)
    }
}
struct BackedPair {
    values: [Option<RawValue>; 2],
    backing: Option<Arc<ReservationBacking>>,
}
impl Drop for BackedPair {
    fn drop(&mut self) {
        let mut failures: [Option<RawValue>; 2] = [None, None];
        for (index, value) in self.values.iter_mut().enumerate() {
            if let Some(value) = value.take()
                && let Err(payload) =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(value)))
            {
                failures[index] = Some(payload);
            }
        }
        if failures.iter().all(Option::is_none) {
            return;
        }
        if failures.iter().filter(|v| v.is_some()).count() == 1 {
            let index = if failures[0].is_some() { 0 } else { 1 };
            let payload = failures[index].take().expect("one original Drop failure");
            match payload.downcast::<BackedPair>() {
                Ok(mut previous)
                    if previous
                        .backing
                        .as_ref()
                        .zip(self.backing.as_ref())
                        .is_some_and(|(a, b)| Arc::ptr_eq(a, b)) =>
                {
                    failures = std::mem::take(&mut previous.values);
                    previous.backing.take();
                }
                Ok(previous) => failures[index] = Some(previous),
                Err(payload) => failures[index] = Some(payload),
            }
        }
        std::panic::resume_unwind(Box::new(BackedPair {
            values: failures,
            backing: self.backing.take(),
        }));
    }
}
impl Drop for OriginalInput {
    fn drop(&mut self) {
        let future = self
            .future
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        let cause = self
            .raw_cause
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        // Catch EACH destructor independently: a future Drop and its actual
        // raw JoinError Drop may both panic. Fixed two cells avoid double panic
        // during field unwinding and retain both genuine successor objects.
        drop(BackedPair {
            values: [future.map(|f| Box::new(f) as RawValue), cause],
            backing: Some(Arc::clone(&self.backing)),
        });
    }
}
enum OriginalState {
    Reserved,
    Loaned,
    UncommittedNative,
    RetiringNative(Retirement<ReservationBacking>),
    #[allow(
        dead_code,
        reason = "pre-spawn original input custody, not an exit observation"
    )]
    Prepared(Arc<OriginalInput>),
    Running {
        join: JoinHandle<()>,
        input: Arc<OriginalInput>,
    },
    Retiring(Retirement<ReservationBacking>),
    RetiringPhysical(Retirement<ReservationBacking>),
    RetiringAction(Retirement<ReservationBacking>),
    ReplyOnly,
    // The public API supplied no original handle after an unwind. Keep all
    // custody; never infer joined from task Drop or fabricate another handle.
    #[allow(
        dead_code,
        reason = "retain the actual input when the public API supplies no original handle"
    )]
    UnjoinableSpawn(Arc<OriginalInput>),
}
struct OriginalRecord {
    native: OriginalNativeAcceptance,
    state: OriginalState,
    reservations: Option<Arc<ReservationBacking>>,
}
struct State {
    started: bool,
    closed: bool,
    records: Box<[Option<OriginalRecord>]>,
    first_failure: Option<JoinFacts>,
    first_rejection: Option<RegistrationFailure>,
    cleanup_failures: u64,
    spawn_panicked: bool,
}
struct Core {
    state: Mutex<State>,
    changed: Notify,
    handle: Handle,
}
struct ReaperExitGuard {
    core: Arc<Core>,
}
impl Drop for ReaperExitGuard {
    fn drop(&mut self) {
        self.core
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .closed = true;
        self.core.changed.notify_one();
    }
}
#[derive(Clone)]
pub(crate) struct ApplySendPort {
    core: Arc<Core>,
}
pub(crate) struct ApplySendOwner {
    core: Arc<Core>,
    reaper: Option<JoinHandle<()>>,
    reaper_retirement: Option<Retirement<Core>>,
    reaper_failure: Option<JoinFacts>,
    children_joined: bool,
    joined: bool,
    unjoinable_reaper_spawn: bool,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegistrationFailure {
    Closed,
    MissingOriginalCharge,
    BrokenOriginalBound,
}
impl std::fmt::Display for RegistrationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Closed => "Native Apply original owner is closed",
            Self::MissingOriginalCharge => "Native Apply batch has no original process item",
            Self::BrokenOriginalBound => "Native Apply original item-backed slot invariant failed",
        })
    }
}
#[must_use = "commit the original input or release the empty reserved slot before original item Drop"]
pub(crate) struct ApplySlot {
    core: Arc<Core>,
    index: usize,
    active: bool,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OriginalDisposition {
    Started,
    ClosedAndRetiring,
    SpawnUnjoinable,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DrainObservation {
    Complete,
    Deadline,
    OriginalSendFailure,
    OriginalCleanupFailure,
    OriginalReaperFailure,
    RegistrationFailure,
}
impl std::fmt::Display for DrainObservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Native Apply original drain: {self:?}")
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FailureSnapshot {
    pub(crate) original_send: Option<JoinFacts>,
    pub(crate) cleanup_failures: u64,
    pub(crate) original_reaper: Option<JoinFacts>,
    pub(crate) rejection: Option<RegistrationFailure>,
    pub(crate) spawn_panicked: bool,
}
impl ApplySendOwner {
    pub(crate) fn new(
        handle: Handle,
        budget: TransportBudget,
    ) -> Result<(Self, ApplySendPort), std::collections::TryReserveError> {
        // Same exact process-item cap as NativeTransportSupervisor::max_items.
        // Allocate once BEFORE the reaper/role publishes any capability.
        let slots = allocate_original_slots(budget.max_backend_queued_operations())?;
        let core = Arc::new(Core {
            state: Mutex::new(State {
                started: false,
                closed: false,
                records: slots,
                first_failure: None,
                first_rejection: None,
                cleanup_failures: 0,
                spawn_panicked: false,
            }),
            changed: Notify::new(),
            handle: handle.clone(),
        });
        let port = ApplySendPort {
            core: Arc::clone(&core),
        };
        // No task exists before the original runtime moves into Host.
        // Port admission remains closed until this owner explicitly starts.
        Ok((
            Self {
                core,
                reaper: None,
                reaper_retirement: None,
                reaper_failure: None,
                children_joined: false,
                joined: false,
                unjoinable_reaper_spawn: false,
            },
            port,
        ))
    }
    pub(crate) fn start_reaper(&mut self) -> Result<(), RegistrationFailure> {
        {
            let mut state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.started || state.closed {
                return Err(RegistrationFailure::Closed);
            }
            state.started = true;
        }
        let original_core = Arc::clone(&self.core);
        let exit = ReaperExitGuard {
            core: Arc::clone(&self.core),
        };
        // Capture BEFORE spawn, including never-first-polled runtime rejection.
        let original = async move {
            let _exit = exit;
            reap_originals(original_core).await;
        };
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.core.handle.spawn(original)
        })) {
            Ok(join) => {
                self.reaper = Some(join);
                Ok(())
            }
            Err(actual) => {
                // The public API gave no handle. Retain raw cause in this
                // same owner; do not fabricate joined from future Drop.
                self.reaper_retirement = Some(Retirement::new(actual, Arc::clone(&self.core)));
                self.unjoinable_reaper_spawn = true;
                let mut state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
                state.closed = true;
                state.spawn_panicked = true;
                drop(state);
                self.core.changed.notify_one();
                Err(RegistrationFailure::Closed)
            }
        }
    }
    pub(crate) fn is_joined(&self) -> bool {
        self.joined
    }
    pub(crate) fn close(&self) {
        self.core
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .closed = true;
        self.core.changed.notify_one();
    }
    pub(crate) fn request_abort(&self) {
        self.close();
        let state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
        for record in state.records.iter().flatten() {
            if let OriginalState::Running { join, .. } = &record.state {
                join.abort();
            }
        }
    }
    pub(crate) fn failure_snapshot(&self) -> FailureSnapshot {
        let state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
        FailureSnapshot {
            original_send: state.first_failure,
            cleanup_failures: state.cleanup_failures,
            original_reaper: self.reaper_failure,
            rejection: state.first_rejection,
            spawn_panicked: state.spawn_panicked,
        }
    }
    pub(crate) async fn drain_until(&mut self, deadline: Instant) -> DrainObservation {
        self.close();
        if !self
            .core
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .started
        {
            // Before Host starts there are no original jobs, reaper or joins.
            self.children_joined = true;
        }
        if !self.children_joined {
            if let Some(join) = self.reaper.as_mut() {
                match tokio::time::timeout_at(deadline.into(), join).await {
                    Err(_) => return DrainObservation::Deadline,
                    Ok(Err(original)) => {
                        self.reaper_failure = Some(JoinFacts::of(&original));
                        if original.is_cancelled() {
                            // Public cancelled JoinError has no opaque payload.
                            // This is after its ACTUAL join, outside Core lock.
                            drop(original);
                        } else {
                            self.reaper_retirement =
                                Some(Retirement::new(original, Arc::clone(&self.core)));
                        }
                    }
                    Ok(Ok(())) => self.children_joined = true,
                }
                self.reaper.take();
            }
            if self.reaper_failure.is_some() && !self.children_joined {
                if tokio::time::timeout_at(deadline.into(), reap_originals(Arc::clone(&self.core)))
                    .await
                    .is_err()
                {
                    return DrainObservation::Deadline;
                }
                self.children_joined = true;
            }
        }
        if let Some(retirement) = self.reaper_retirement.as_mut() {
            if tokio::time::timeout_at(
                deadline.into(),
                std::future::poll_fn(|cx| retirement.poll(&self.core.handle, cx)),
            )
            .await
            .is_err()
            {
                return DrainObservation::Deadline;
            }
            self.reaper_retirement.take();
        }
        if self.unjoinable_reaper_spawn {
            return DrainObservation::Deadline;
        }
        self.joined = self.children_joined;
        if Instant::now() > deadline {
            return DrainObservation::Deadline;
        }
        if self.reaper_failure.is_some() {
            return DrainObservation::OriginalReaperFailure;
        }
        let state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.first_failure.is_some() {
            DrainObservation::OriginalSendFailure
        } else if state.cleanup_failures != 0 {
            DrainObservation::OriginalCleanupFailure
        } else if state.first_rejection.is_some() || state.spawn_panicked {
            DrainObservation::RegistrationFailure
        } else {
            DrainObservation::Complete
        }
    }
}
impl Drop for ApplySendOwner {
    fn drop(&mut self) {
        self.request_abort();
    }
}
impl ApplySendPort {
    /// Observe this original process owner's installed records. Readiness is
    /// opened only after the composition starts its original reaper.
    pub(crate) fn installed_records(&self) -> Result<usize, novarocks_workload_control::WorkError> {
        let state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.closed {
            return Err(novarocks_workload_control::WorkError::Closed);
        }
        Ok(state.records.len())
    }

    /// Refuse a closed role before starting a paid proposal. This does not
    /// reserve a row; reserve_original_slot rechecks the same state at accept.
    pub(crate) fn check_role_admission(&self) -> Result<(), RegistrationFailure> {
        let mut state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
        if !state.started || state.closed {
            state
                .first_rejection
                .get_or_insert(RegistrationFailure::Closed);
            return Err(RegistrationFailure::Closed);
        }
        Ok(())
    }

    /// Called while the ORIGINAL batch still owns its nonempty queue grants.
    /// This derives a bookkeeping slot, not another admission/wallet charge.
    pub(crate) fn reserve_original_slot(
        &self,
        original_items: usize,
    ) -> Result<ApplySlot, RegistrationFailure> {
        let mut state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
        let failure = if !state.started || state.closed {
            Some(RegistrationFailure::Closed)
        } else if original_items == 0 {
            Some(RegistrationFailure::MissingOriginalCharge)
        } else if state.records.iter().all(Option::is_some) {
            Some(RegistrationFailure::BrokenOriginalBound)
        } else {
            None
        };
        if let Some(failure) = failure {
            state.first_rejection.get_or_insert(failure);
            return Err(failure);
        }
        let index = state
            .records
            .iter()
            .position(Option::is_none)
            .expect("an original item-backed slot exists");
        state.records[index] = Some(OriginalRecord {
            native: OriginalNativeAcceptance::new(),
            state: OriginalState::Reserved,
            reservations: None,
        });
        let slot = ApplySlot {
            core: Arc::clone(&self.core),
            index,
            active: true,
        };
        drop(state);
        self.core.changed.notify_one();
        Ok(slot)
    }
}
impl ApplySlot {
    /// Native caller passes the pure apply_operations(...) future directly.
    /// There is no await or user callback between transfer and Prepared custody.
    pub(crate) fn commit<F>(
        mut self,
        reservations: ApplyReservations,
        future: F,
    ) -> OriginalDisposition
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let backing = {
            let state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
            state.records[self.index]
                .as_ref()
                .expect("original reserved slot")
                .reservations
                .as_ref()
                .map(Arc::clone)
        }
        .unwrap_or_else(|| {
            #[cfg(test)]
            {
                Arc::new(ReservationBacking::new(&self.core, 0, None, None))
            }
            #[cfg(not(test))]
            {
                panic!("original Native paid source must precede commit")
            }
        });
        *backing.physical.lock().unwrap_or_else(|e| e.into_inner()) = Some(reservations);
        let input = Arc::new(OriginalInput {
            future: Mutex::new(Some(Box::pin(future))),
            raw_cause: Mutex::new(None),
            backing: Arc::clone(&backing),
        });
        let mut state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
        let record = state.records[self.index]
            .as_mut()
            .expect("original reserved slot retained");
        assert!(
            matches!(record.state, OriginalState::Reserved),
            "single original slot commit"
        );
        record.reservations = Some(Arc::clone(&backing));
        record.state = OriginalState::Prepared(Arc::clone(&input));
        self.active = false;
        let disposition = if state.closed {
            state
                .first_rejection
                .get_or_insert(RegistrationFailure::Closed);
            state.records[self.index]
                .as_mut()
                .expect("prepared original slot")
                .state = OriginalState::Retiring(Retirement::new(input, backing));
            OriginalDisposition::ClosedAndRetiring
        } else {
            // No task can lose the original F on spawn unwind: Core already
            // owns it; dropping OriginalTask releases only its input Arc.
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.core.handle.spawn(OriginalTask {
                    input: Arc::clone(&input),
                })
            })) {
                Ok(join) => {
                    state.records[self.index]
                        .as_mut()
                        .expect("prepared original slot")
                        .state = OriginalState::Running { join, input };
                    OriginalDisposition::Started
                }
                Err(actual) => {
                    *input.raw_cause.lock().unwrap_or_else(|e| e.into_inner()) = Some(actual);
                    state.spawn_panicked = true;
                    state.records[self.index]
                        .as_mut()
                        .expect("prepared original slot")
                        .state = OriginalState::UnjoinableSpawn(input);
                    OriginalDisposition::SpawnUnjoinable
                }
            }
        };
        drop(state);
        self.core.changed.notify_one();
        disposition
    }
}
impl Drop for ApplySlot {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
        let record = state.records[self.index]
            .as_ref()
            .expect("original reserved slot retained");
        assert!(
            matches!(record.state, OriginalState::Reserved),
            "only an empty reserved slot may be released by its ticket"
        );
        if record.reservations.is_some() {
            state.records[self.index].as_mut().unwrap().state = OriginalState::UncommittedNative;
        } else {
            state.records[self.index] = None;
        }
        drop(state);
        self.core.changed.notify_one();
    }
}
fn allocate_original_slots(
    maximum_records: usize,
) -> Result<Box<[Option<OriginalRecord>]>, std::collections::TryReserveError> {
    let mut slots = Vec::new();
    slots.try_reserve_exact(maximum_records)?;
    slots.resize_with(maximum_records, || None);
    Ok(slots.into_boxed_slice())
}
async fn reap_originals(core: Arc<Core>) {
    loop {
        let notified = core.changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let settled = std::future::poll_fn(|cx| {
            let mut state = core.state.lock().unwrap_or_else(|e| e.into_inner());
            for index in 0..state.records.len() {
                let Some(record) = state.records[index].as_mut() else {
                    continue;
                };
                let mut actual_join = None;
                let mut cancelled_to_drop = None;
                let mut cleanup_failures = 0;
                let done = match &mut record.state {
                    OriginalState::ReplyOnly => record
                        .reservations
                        .as_ref()
                        .is_none_or(|backing| Arc::strong_count(backing) == 1),
                    OriginalState::UncommittedNative => true,
                    OriginalState::Reserved
                    | OriginalState::Loaned
                    | OriginalState::Prepared(_)
                    | OriginalState::UnjoinableSpawn(_) => false,
                    OriginalState::Running { join, .. } => match Pin::new(join).poll(cx) {
                        Poll::Pending => false,
                        Poll::Ready(actual) => {
                            actual_join = Some(actual);
                            false
                        }
                    },
                    OriginalState::RetiringNative(retirement)
                    | OriginalState::RetiringPhysical(retirement)
                        if retirement.cleanup_failures != 0 =>
                    {
                        // The first actual physical cleanup unwind retains its
                        // original cause/backing in this SAME fixed row. No
                        // retry/disposal can manufacture a proven physical exit.
                        false
                    }
                    OriginalState::Retiring(retirement)
                    | OriginalState::RetiringNative(retirement)
                    | OriginalState::RetiringPhysical(retirement)
                    | OriginalState::RetiringAction(retirement) => {
                        let before = retirement.cleanup_failures;
                        let done = retirement.poll(&core.handle, cx).is_ready();
                        cleanup_failures = retirement.cleanup_failures.saturating_sub(before);
                        done
                    }
                };
                state.cleanup_failures = state.cleanup_failures.saturating_add(cleanup_failures);
                if let Some(actual) = actual_join {
                    let record = state.records[index].as_mut().expect("original joined slot");
                    let input = match &record.state {
                        OriginalState::Running { input, .. } => Arc::clone(input),
                        _ => unreachable!(),
                    };
                    let failure = match actual {
                        Ok(()) => None,
                        Err(original) => {
                            let facts = JoinFacts::of(&original);
                            if original.is_cancelled() {
                                cancelled_to_drop = Some(original);
                            } else {
                                *input.raw_cause.lock().unwrap_or_else(|e| e.into_inner()) =
                                    Some(Box::new(original));
                            }
                            Some(facts)
                        }
                    };
                    let backing =
                        Arc::clone(record.reservations.as_ref().expect("original charged slot"));
                    record.state = OriginalState::Retiring(Retirement::new(input, backing));
                    if let Some(facts) = failure {
                        state.first_failure.get_or_insert(facts);
                    }
                    cx.waker().wake_by_ref();
                }
                if cancelled_to_drop.is_some() {
                    // Genuine cancellation-only source retires only AFTER
                    // its actual JoinReady and outside the Core lock.
                    drop(state);
                    return Poll::Ready((None, false, cancelled_to_drop));
                }
                if done {
                    let record = state.records[index]
                        .as_mut()
                        .expect("original completed source");
                    if !matches!(record.state, OriginalState::ReplyOnly) {
                        let backing =
                            Arc::clone(record.reservations.as_ref().expect("original backing"));
                        let next = match &record.state {
                            OriginalState::Retiring(_) | OriginalState::UncommittedNative => {
                                let native = std::mem::replace(
                                    &mut record.native,
                                    OriginalNativeAcceptance::new(),
                                );
                                OriginalState::RetiringNative(Retirement::new(
                                    native,
                                    Arc::clone(&backing),
                                ))
                            }
                            OriginalState::RetiringNative(_) => {
                                let physical = backing
                                    .physical
                                    .lock()
                                    .unwrap_or_else(|e| e.into_inner())
                                    .take();
                                OriginalState::RetiringPhysical(Retirement::new(
                                    physical,
                                    Arc::clone(&backing),
                                ))
                            }
                            OriginalState::RetiringPhysical(_) => {
                                let action = backing
                                    .action
                                    .lock()
                                    .unwrap_or_else(|e| e.into_inner())
                                    .take();
                                OriginalState::RetiringAction(Retirement::new(
                                    action,
                                    Arc::clone(&backing),
                                ))
                            }
                            OriginalState::RetiringAction(_) => OriginalState::ReplyOnly,
                            _ => unreachable!(),
                        };
                        let previous = std::mem::replace(&mut record.state, next);
                        drop(state);
                        // Completed original retirement contains no foreign
                        // payload; its final header/Arc leaves outside Core.
                        drop(previous);
                        return Poll::Ready((None, false, None));
                    }
                    let success = state.records[index].take();
                    drop(state);
                    return Poll::Ready((success, false, None));
                }
            }
            let done = state.closed && state.records.iter().all(Option::is_none);
            drop(state);
            if done {
                Poll::Ready((None, true, None))
            } else {
                Poll::Pending
            }
        });
        tokio::pin!(settled);
        tokio::select! {
            outcome = &mut settled => {
                let (success, done, cancelled) = outcome;
                drop(cancelled);
                drop(success); // Actual original joins precede original permit Drop.
                if done { return; }
                tokio::task::yield_now().await;
            },
            _ = &mut notified => {},
        }
    }
}

#[cfg(test)]
mod component_tests;
