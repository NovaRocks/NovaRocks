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

//! The original prepared driver slots, before the executor's pure handoff.
//! This is cold start/rejection ownership, not a driver-step scheduling layer.

use super::*;
use crate::runtime::fragment::ExecutionReason;
use crate::runtime::runtime_state::RuntimeErrorState;
use novarocks_failure_contract::{FailureClass, FailureFact, FailureProjection, FailureReason};
use std::{any::Any, mem::ManuallyDrop, num::NonZeroU16};

type Payload = ManuallyDrop<Box<dyn Any + Send>>;

#[derive(Clone, Copy, Debug)]
struct StartFault(u16);
impl std::fmt::Display for StartFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "original driver start/retirement phase {} did not exit",
            self.0
        )
    }
}
impl std::error::Error for StartFault {}
impl FailureProjection for StartFault {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            if [
                ExecutionReason::DriverStartDuplicateSubmission,
                ExecutionReason::DriverRetirementRetainedWork,
            ]
            .into_iter()
            .any(|reason| self.0 == reason.code().get())
            {
                FailureClass::Invariant
            } else {
                FailureClass::Unexpected
            },
            FailureReason::new(
                novarocks_failure_contract::domain::EXECUTION,
                NonZeroU16::new(self.0).expect("fixed nonzero start reason"),
            ),
        )
    }
}

/// One borrowed call, its opaque destructor and two diagnostic publications.
/// Every secondary payload remains in this same fixed phase. No Any is a Task source.
pub(crate) struct Phase {
    started: bool,
    returned: bool,
    unknown: bool,
    payloads: [Option<Payload>; 6],
}
impl Phase {
    pub(super) fn new() -> Self {
        Self {
            started: false,
            returned: false,
            unknown: false,
            payloads: std::array::from_fn(|_| None),
        }
    }
    pub(super) fn run<T: Copy>(
        &mut self,
        control: &RuntimeErrorState,
        reason: u16,
        action: impl FnOnce() -> T,
    ) -> Option<T> {
        if self.started {
            return None;
        }
        self.started = true;
        match catch_unwind(AssertUnwindSafe(action)) {
            Ok(value) => {
                self.returned = true;
                Some(value)
            }
            Err(payload) => {
                self.unknown = true;
                self.payloads[0] = Some(ManuallyDrop::new(payload));
                self.report(control, reason);
                None
            }
        }
    }
    fn report(&mut self, control: &RuntimeErrorState, reason: u16) {
        let cause = ExecutionFailure::from_domain(StartFault(reason));
        // Health precedes any opaque destructor. Reporting itself has
        // its own two fixed payload pairs and does not recurse.
        for (slot, report) in [
            (
                2,
                RuntimeErrorState::report_unresolved_exit
                    as fn(&RuntimeErrorState, &ExecutionFailure),
            ),
            (4, RuntimeErrorState::report_cleanup),
        ] {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| report(control, &cause))) {
                self.payloads[slot] = Some(ManuallyDrop::new(payload));
            }
        }
        for slot in [0, 2, 4] {
            if let Some(payload) = self.payloads[slot].take()
                && let Err(secondary) =
                    catch_unwind(AssertUnwindSafe(|| drop(ManuallyDrop::into_inner(payload))))
            {
                self.payloads[slot + 1] = Some(ManuallyDrop::new(secondary));
            }
        }
    }
    pub(super) fn run_metadata<T: Copy>(
        &mut self,
        reporter: &crate::runtime::task_control::ExecutionExitReporter,
        reason: u16,
        action: impl FnOnce() -> T,
    ) -> Option<T> {
        if self.started {
            return None;
        }
        self.started = true;
        match catch_unwind(AssertUnwindSafe(action)) {
            Ok(value) => {
                self.returned = true;
                Some(value)
            }
            Err(payload) => {
                self.unknown = true;
                self.payloads[0] = Some(ManuallyDrop::new(payload));
                // This original reporter owns only Arc<Health> + Copy origin.
                // The RuntimeErrorState may already have been destroyed.
                reporter.report(StartFault(reason).failure_fact());
                let payload = self.payloads[0].take().unwrap();
                if let Err(secondary) =
                    catch_unwind(AssertUnwindSafe(|| drop(ManuallyDrop::into_inner(payload))))
                {
                    self.payloads[1] = Some(ManuallyDrop::new(secondary));
                }
                None
            }
        }
    }
    pub(super) fn is_unknown(&self) -> bool {
        self.unknown
    }
    fn reset_observation(&mut self) {
        assert!(self.returned && !self.unknown);
        self.started = false;
        self.returned = false;
    }
}

enum Work {
    Prepared(Box<ManuallyDrop<DriverTask>>),
    Retiring(Box<Retirement>),
    Transferred,
}
struct Slot {
    work: Work,
    attachment: Phase,
    activation: Option<ManuallyDrop<Arc<FragmentCompletion>>>,
    activation_held: bool,
    activate: Phase,
    activate_drop: Phase,
    activate_control_drop: Phase,
}
impl Slot {
    fn new(task: DriverTask) -> Self {
        let activation = task.completion.clone();
        Self {
            work: Work::Prepared(Box::new(ManuallyDrop::new(task))),
            attachment: Phase::new(),
            activation: Some(ManuallyDrop::new(activation)),
            activation_held: false,
            activate: Phase::new(),
            activate_drop: Phase::new(),
            activate_control_drop: Phase::new(),
        }
    }
    fn empty(&self) -> bool {
        matches!(self.work, Work::Transferred) && self.activation.is_none()
    }
}

struct State {
    slots: Vec<Option<Slot>>,
    loaned: Vec<bool>,
    drain_started: bool,
    retirement_done: Vec<bool>,
    retirement_unresolved: bool,
    busy: usize,
    metadata: [Option<Phase>; 6],
    initial_failure: Option<ManuallyDrop<ExecutionFailure>>,
    metadata_complete: bool,
    metadata_unknown: bool,
    submission_started: bool,
}

/// Prepared owns one handle. Reaper continuations borrow the same original Arc.
/// There is no new per-poll allocation, Task conclusion port or global park map.
pub(crate) struct DriverBatch {
    inner: Arc<BatchOwner>,
    drop_drain: bool,
}
pub(super) struct BatchOwner {
    state: Mutex<State>,
    pin: Mutex<Option<Arc<Self>>>,
    count: usize,
}
impl DriverBatch {
    pub(crate) fn new(tasks: Vec<DriverTask>) -> Self {
        let count = tasks.len();
        Self {
            inner: Arc::new(BatchOwner {
                state: Mutex::new(State {
                    loaned: vec![false; count],
                    drain_started: false,
                    retirement_done: vec![false; count],
                    retirement_unresolved: false,
                    busy: 0,
                    slots: tasks
                        .into_iter()
                        .map(|task| Some(Slot::new(task)))
                        .collect(),
                    metadata: std::array::from_fn(|_| Some(Phase::new())),
                    initial_failure: None,
                    metadata_complete: false,
                    metadata_unknown: false,
                    submission_started: false,
                }),
                pin: Mutex::new(None),
                count,
            }),
            drop_drain: true,
        }
    }
    pub(crate) fn len(&self) -> usize {
        self.inner.count
    }
    pub(crate) fn control(&self) -> Option<Arc<RuntimeErrorState>> {
        let state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .slots
            .iter()
            .filter_map(Option::as_ref)
            .find_map(|slot| match &slot.work {
                Work::Prepared(task) => Some(task.completion.control.clone()),
                Work::Retiring(r) => r.completion.as_ref().map(|c| c.control.clone()),
                Work::Transferred => None,
            })
    }
    pub(crate) fn defer_activation(&self) {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        for slot in state.slots.iter_mut().filter_map(Option::as_mut) {
            if !slot.activation_held
                && let Some(completion) = slot.activation.as_ref()
            {
                completion.defer_start();
                slot.activation_held = true;
            }
        }
    }
    pub(crate) fn activate(&self) {
        for index in 0..self.inner.count {
            let Some(mut loan) = self.inner.loan(index) else {
                continue;
            };
            let slot = loan.slot.as_mut().expect("original activation slot");
            let Some(completion) = slot.activation.as_ref() else {
                continue;
            };
            let control = ManuallyDrop::new(completion.control.clone());
            let health = control.exit_health_reporter();
            if slot
                .activate
                .run(
                    &control,
                    ExecutionReason::DriverStartActivation.code().get(),
                    || completion.activate(),
                )
                .is_some()
            {
                let completion = slot
                    .activation
                    .take()
                    .expect("original activation metadata");
                slot.activate_drop.run(
                    &control,
                    ExecutionReason::DriverStartActivation.code().get(),
                    || drop(ManuallyDrop::into_inner(completion)),
                );
            }
            slot.activate_control_drop.run_metadata(
                &health,
                ExecutionReason::DriverActivationControlDrop.code().get(),
                || drop(ManuallyDrop::into_inner(control)),
            );
            if slot.empty()
                && !slot.attachment.unknown
                && !slot.activate.unknown
                && !slot.activate_drop.unknown
                && !slot.activate_control_drop.unknown
            {
                loan.slot = None;
            }
        }
        self.inner.release_if_finished();
    }
    pub(crate) fn metadata<T: Copy>(
        &self,
        ordinal: usize,
        control: &RuntimeErrorState,
        action: impl FnOnce() -> T,
    ) -> Option<T> {
        self.inner.pin();
        let phase = {
            let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            let phase = state.metadata[ordinal].take();
            if phase.is_some() {
                state.busy += 1;
            }
            phase
        };
        let mut phase = phase?;
        let reason = match ordinal {
            0 => ExecutionReason::DriverStartRuntimeMetadata,
            1 => ExecutionReason::DriverStartInitialFailurePublication,
            2 => ExecutionReason::DriverStartInitialFailureCaptureDrop,
            3 => ExecutionReason::DriverStartExecutorAdmission,
            _ => unreachable!("only the original four start metadata phases are issued"),
        };
        let result = phase.run(control, reason.code().get(), action);
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        state.metadata_unknown |= phase.unknown;
        state.metadata[ordinal] = Some(phase);
        state.busy -= 1;
        result
    }
    pub(crate) fn retire_submit_control(&self, control: Arc<RuntimeErrorState>) {
        let control = ManuallyDrop::new(control);
        let reporter = control.exit_health_reporter();
        self.inner.pin();
        let mut phase = {
            let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            state.busy += 1;
            state.metadata[5]
                .take()
                .expect("one original submit control role")
        };
        phase.run_metadata(
            &reporter,
            ExecutionReason::DriverStartControlDrop.code().get(),
            || drop(ManuallyDrop::into_inner(control)),
        );
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        state.metadata_unknown |= phase.unknown;
        state.metadata[5] = Some(phase);
        state.busy -= 1;
        drop(state);
        self.inner.release_if_finished();
    }
    #[cfg(not(test))]
    pub(crate) fn record_invariant(&self, control: &RuntimeErrorState) {
        self.inner.pin();
        let mut phase = {
            let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            state.busy += 1;
            state.metadata[4].take().expect("one fixed invariant phase")
        };
        phase.started = true;
        phase.unknown = true;
        phase.report(
            control,
            ExecutionReason::DriverStartDuplicateSubmission.code().get(),
        );
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        state.metadata_unknown = true;
        state.metadata[4] = Some(phase);
        state.busy -= 1;
    }
    pub(crate) fn install_initial_failure(&self, error: ExecutionFailure) -> ExecutionFailure {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        assert!(state.initial_failure.is_none());
        state.initial_failure = Some(ManuallyDrop::new(error));
        ExecutionFailure::clone(
            state
                .initial_failure
                .as_ref()
                .expect("original failure source"),
        )
    }
    pub(crate) fn dispose_initial_failure(&self, control: &RuntimeErrorState) -> bool {
        let error = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .initial_failure
            .take();
        error.is_none_or(|error| {
            self.metadata(2, control, || drop(ManuallyDrop::into_inner(error)))
                .is_some()
        })
    }
    pub(crate) fn begin_submission(&self) -> bool {
        self.inner.pin();
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.submission_started {
            return false;
        }
        state.submission_started = true;
        true
    }
    /// The caller may use this only for audited Core metadata mutation. A
    /// panic restores the slot before an enclosing catch releases its locks.
    pub(crate) fn borrow_prepared<T: Copy>(
        &self,
        index: usize,
        action: impl FnOnce(&DriverTask) -> T,
    ) -> Option<T> {
        let loan = self.inner.loan(index)?;
        let Work::Prepared(task) = &loan.slot.as_ref()?.work else {
            return None;
        };
        Some(action(task))
    }
    pub(crate) fn attach(&self, index: usize, attach: impl FnOnce(&DriverTask)) {
        let Some(mut loan) = self.inner.loan(index) else {
            return;
        };
        let slot = loan.slot.as_mut().expect("original slot loan");
        let Work::Prepared(task) = &slot.work else {
            return;
        };
        let control = task.completion.control.clone();
        slot.attachment.run(
            &control,
            ExecutionReason::DriverStartAttachment.code().get(),
            || attach(task),
        );
        let unknown = slot.attachment.unknown;
        drop(loan);
        if unknown {
            self.inner
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .metadata_unknown = true;
        }
    }
    pub(crate) fn mark_metadata_complete(&self) {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .metadata_complete = true;
    }
    /// All possibly foreign attachment work has returned or is in its original
    /// phase. The queue lock covers only pure ownership moves and Core counters.
    pub(crate) fn move_to_queue(
        &self,
        queue: &mut VecDeque<DriverTask>,
        shared: &Arc<ExecutorShared>,
    ) {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        queue.reserve(self.inner.count);
        state.metadata_complete = true;
        for entry in &mut state.slots {
            let Some(slot) = entry.as_mut() else {
                continue;
            };
            if matches!(slot.work, Work::Prepared(_)) {
                let Work::Prepared(task) = std::mem::replace(&mut slot.work, Work::Transferred)
                else {
                    unreachable!("matched original prepared slot")
                };
                let mut task = ManuallyDrop::into_inner(*task);
                task.lease = Some(ExecutorTaskLease::acquire(shared));
                task.mark_queued();
                queue.push_back(task);
            }
            if slot.empty() && !slot.attachment.unknown {
                *entry = None;
            }
        }
        drop(state);
        self.inner.release_if_finished();
    }
    pub(crate) fn drain(&self, shutdown_failure: bool) {
        self.inner.pin();
        self.mark_metadata_complete();
        {
            let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.drain_started {
                return;
            }
            // One original batch owns one continuation, including a repeated
            // convenience-handle Drop after an explicit refusal drain.
            state.drain_started = true;
        }
        if self.inner.poll_batch(shutdown_failure) == Poll::Pending {
            rejected_driver_reaper().enqueue_entry(ReaperEntry::Batch {
                owner: self.inner.clone(),
                shutdown_failure,
            });
        }
        self.inner.release_if_finished();
    }
}
impl Drop for DriverBatch {
    fn drop(&mut self) {
        // A start-stack unwind cannot drop an unsubmitted raw Vec. In a normal
        // handoff these slots are already empty, so this performs no foreign work.
        if self.drop_drain {
            let never_started = {
                let state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
                !state.submission_started
                    && !state.metadata[0]
                        .as_ref()
                        .is_some_and(|phase| phase.started)
            };
            if never_started {
                // No start-body obligation was installed. Retire only the
                // redundant metadata clones; do not activate a discarded plan.
                for index in 0..self.inner.count {
                    let Some(mut loan) = self.inner.loan(index) else {
                        continue;
                    };
                    let slot = loan.slot.as_mut().expect("original discarded slot");
                    if let Some(completion) = slot.activation.take() {
                        let control = completion.control.clone();
                        slot.activate_drop.run(
                            &control,
                            ExecutionReason::DriverStartActivation.code().get(),
                            || drop(ManuallyDrop::into_inner(completion)),
                        );
                    }
                }
            }
            self.drain(false);
        }
    }
}

impl BatchOwner {
    fn pin(self: &Arc<Self>) {
        let mut pin = self.pin.lock().unwrap_or_else(|e| e.into_inner());
        if pin.is_none() {
            *pin = Some(self.clone());
        }
    }
    fn release_if_finished(&self) {
        let finished = {
            let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.metadata_complete
                && state.busy == 0
                && !state.metadata_unknown
                && state.initial_failure.is_none()
                && state.slots.iter().all(Option::is_none)
        };
        if finished {
            let pin = self.pin.lock().unwrap_or_else(|e| e.into_inner()).take();
            drop(pin);
        }
    }
    fn loan(self: &Arc<Self>, index: usize) -> Option<Loan<'_>> {
        let slot = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let slot = state.slots[index].take()?;
            state.loaned[index] = true;
            state.busy += 1;
            slot
        };
        Some(Loan {
            owner: self,
            index,
            slot: Some(slot),
        })
    }
    fn poll(self: &Arc<Self>, index: usize, shutdown_failure: bool) -> Poll {
        let Some(mut loan) = self.loan(index) else {
            let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            return if state.loaned[index] {
                Poll::Pending
            } else {
                Poll::Exited
            };
        };
        let slot = loan.slot.as_mut().expect("original slot loan");
        if matches!(slot.work, Work::Prepared(_)) {
            let Work::Prepared(task) = std::mem::replace(&mut slot.work, Work::Transferred) else {
                unreachable!("matched original prepared slot")
            };
            slot.work = Work::Retiring(Box::new(Retirement::new(
                ManuallyDrop::into_inner(*task),
                shutdown_failure,
            )));
        }
        let Work::Retiring(retirement) = &mut slot.work else {
            return Poll::Exited;
        };
        let result = retirement.tick();
        if result == Poll::Exited {
            slot.work = Work::Transferred;
            if slot.empty()
                && !slot.attachment.unknown
                && !slot.activate.unknown
                && !slot.activate_drop.unknown
                && !slot.activate_control_drop.unknown
            {
                loan.slot = None;
            }
        }
        drop(loan);
        self.release_if_finished();
        result
    }
    fn poll_batch(self: &Arc<Self>, shutdown_failure: bool) -> Poll {
        // The fixed original N is the only traversal extent. A Pending or
        // Unknown sibling cannot prevent another original slot's cleanup.
        for index in 0..self.count {
            if self
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .retirement_done[index]
            {
                continue;
            }
            let result = self.poll(index, shutdown_failure);
            if result != Poll::Pending {
                let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                state.retirement_done[index] = true;
                state.retirement_unresolved |= result == Poll::Unresolved;
            }
        }
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.retirement_done.iter().any(|done| !done) {
            Poll::Pending
        } else if state.retirement_unresolved {
            Poll::Unresolved
        } else {
            Poll::Exited
        }
    }
}
/// Restores the original holders even if a future Core statement unexpectedly
/// unwinds outside one of the per-phase catches. It never invokes foreign code.
struct Loan<'a> {
    owner: &'a BatchOwner,
    index: usize,
    slot: Option<Slot>,
}
impl Drop for Loan<'_> {
    fn drop(&mut self) {
        let mut state = self.owner.state.lock().unwrap_or_else(|e| e.into_inner());
        assert!(state.slots[self.index].is_none());
        state.slots[self.index] = self.slot.take();
        state.loaned[self.index] = false;
        state.busy -= 1;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Poll {
    Pending,
    Exited,
    Unresolved,
}

pub(crate) struct Retirement {
    driver: Option<ManuallyDrop<PipelineDriver>>,
    completion: Option<ManuallyDrop<Arc<FragmentCompletion>>>,
    context: Option<ManuallyDrop<Arc<FragmentContext>>>,
    lease: Option<ManuallyDrop<Option<ExecutorTaskLease>>>,
    phases: [Phase; 13],
    control: Option<ManuallyDrop<Arc<RuntimeErrorState>>>,
    health: crate::runtime::task_control::ExecutionExitReporter,
    custody: Arc<super::super::driver_retirement::DriverExitCustody>,
    proposal: Option<ManuallyDrop<ExecutionFailure>>,
    time_slice: Duration,
    queued_at: Option<Instant>,
    scheduler: Option<ManuallyDrop<Arc<super::super::schedule::event_scheduler::EventScheduler>>>,
    last: bool,
    shutdown_failure: bool,
    unresolved: bool,
}
impl Retirement {
    fn new(task: DriverTask, shutdown_failure: bool) -> Self {
        let DriverTask {
            driver,
            completion,
            fragment_ctx,
            lease,
            time_slice,
            queued_at,
        } = task;
        let scheduler = fragment_ctx.event_scheduler();
        let custody = driver.exit_custody();
        let control = completion.control.clone();
        let health = control.exit_health_reporter();
        Self {
            control: Some(ManuallyDrop::new(control)),
            health,
            custody,
            proposal: None,
            time_slice,
            queued_at,
            scheduler: Some(ManuallyDrop::new(scheduler)),
            last: false,
            driver: Some(ManuallyDrop::new(driver)),
            completion: Some(ManuallyDrop::new(completion)),
            context: Some(ManuallyDrop::new(fragment_ctx)),
            lease: Some(ManuallyDrop::new(lease)),
            phases: std::array::from_fn(|_| Phase::new()),
            shutdown_failure,
            unresolved: false,
        }
    }
    pub(super) fn on_worker_shutdown(task: DriverTask) -> Option<DriverTask> {
        Self::on_worker_inner(task, true, None, true)
    }
    pub(super) fn on_worker(
        task: DriverTask,
        abort: bool,
        proposal: Option<ExecutionFailure>,
    ) -> Option<DriverTask> {
        Self::on_worker_inner(task, abort, proposal, false)
    }
    fn on_worker_inner(
        task: DriverTask,
        abort: bool,
        proposal: Option<ExecutionFailure>,
        shutdown_failure: bool,
    ) -> Option<DriverTask> {
        let mut retirement = Self::new(task, shutdown_failure);
        retirement.proposal = proposal.map(ManuallyDrop::new);
        if retirement.proposal.is_none() && !shutdown_failure {
            retirement.phases[0].started = true;
            retirement.phases[0].returned = true;
        }
        if !abort {
            retirement.phases[1].started = true;
            retirement.phases[1].returned = true;
        }
        match retirement.tick() {
            Poll::Exited => None,
            Poll::Pending if retirement.can_return_pending() => Some(retirement.into_pending()),
            progress => {
                let custody = retirement.custody.clone();
                custody.retain_task_exit(retirement);
                if progress == Poll::Pending {
                    rejected_driver_reaper().enqueue_entry(ReaperEntry::Task(custody));
                }
                None
            }
        }
    }
    fn can_return_pending(&self) -> bool {
        self.driver.is_some()
            && self.context.is_some()
            && self.completion.is_some()
            && self.lease.is_some()
            && self.proposal.is_none()
            && !self.phases.iter().any(|phase| phase.unknown)
            && !self.phases[3].started
            && !self.phases[4].started
            && !self.phases[5].started
    }
    fn into_pending(mut self) -> DriverTask {
        assert!(
            self.can_return_pending(),
            "only intact healthy original holders transfer"
        );
        let task = DriverTask {
            driver: ManuallyDrop::into_inner(self.driver.take().unwrap()),
            completion: ManuallyDrop::into_inner(self.completion.take().unwrap()),
            fragment_ctx: ManuallyDrop::into_inner(self.context.take().unwrap()),
            lease: ManuallyDrop::into_inner(self.lease.take().unwrap()),
            time_slice: self.time_slice,
            queued_at: self.queued_at,
        };
        // These redundant clones cannot be last: the intact task above owns
        // the same exact context/control. No foreign capture can retire here.
        drop(ManuallyDrop::into_inner(self.control.take().unwrap()));
        drop(ManuallyDrop::into_inner(self.scheduler.take().unwrap()));
        task
    }
    pub(crate) fn tick(&mut self) -> Poll {
        if self.unresolved {
            return Poll::Unresolved;
        }
        let control: &RuntimeErrorState = self.control.as_ref().expect("original control holder");
        if !self.phases[0].started {
            let context = self.context.as_ref().expect("original context");
            // A hint: a delayed stop only causes an extra losing proposal.
            // Repeated handoff preserves this retirement's causal exit order.
            let needs_failure = self.shutdown_failure && !control.is_stopped();
            // The original source is stored before cloning it for the one
            // accepted owner. A rejected candidate cannot destroy it inline.
            let proposal = self.proposal.as_deref().cloned();
            let reason = if proposal.is_some() {
                ExecutionReason::DriverRetirementFailureFinish.code().get()
            } else {
                ExecutionReason::DriverRetirementFinish.code().get()
            };
            self.phases[0].run(control, reason, || {
                if let Some(proposal) = proposal {
                    context.set_final_status(proposal);
                } else if needs_failure {
                    context.set_final_status(
                        crate::exec::pipeline::driver_failure::DriverFailure::ExecutorShutdown,
                    );
                }
            });
        }
        if let Some(proposal) = self.proposal.take() {
            self.phases[11].run(
                control,
                ExecutionReason::DriverRetirementFailureDrop.code().get(),
                || drop(ManuallyDrop::into_inner(proposal)),
            );
        }
        if !self.phases[1].started {
            let driver = self.driver.as_mut().expect("original driver");
            self.phases[1].run(
                control,
                ExecutionReason::DriverRetirementSchedulerForget
                    .code()
                    .get(),
                || {
                    driver.cancel_for_fragment_abort();
                },
            );
        }
        if self.phases[1].unknown {
            self.unresolved = true;
            return Poll::Unresolved;
        }
        if self.driver.is_some() {
            let driver = self.driver.as_mut().expect("original driver");
            match self.phases[2].run(
                control,
                ExecutionReason::DriverRetirementPendingFinish.code().get(),
                || driver.has_pending_finish(),
            ) {
                Some(true) => {
                    self.phases[2].reset_observation();
                    return Poll::Pending;
                }
                Some(false) => {}
                None => {
                    self.unresolved = true;
                    return Poll::Unresolved;
                }
            }
            let driver = self.driver.take().expect("one actual driver destructor");
            self.phases[3].run(
                control,
                ExecutionReason::DriverRetirementDriverDrop.code().get(),
                || drop(ManuallyDrop::into_inner(driver)),
            );
        }
        if self.phases[3].unknown {
            self.unresolved = true;
            return Poll::Unresolved;
        }
        match self.phases[10].run(
            control,
            ExecutionReason::DriverRetirementRetainedWork.code().get(),
            || !self.custody.has_retained_work(),
        ) {
            Some(true) => {}
            Some(false) => {
                self.phases[10].unknown = true;
                self.phases[10].report(
                    control,
                    ExecutionReason::DriverRetirementRetainedWork.code().get(),
                );
                self.unresolved = true;
                return Poll::Unresolved;
            }
            None => {
                self.unresolved = true;
                return Poll::Unresolved;
            }
        }
        if let Some(context) = self.context.take() {
            self.phases[4].run(
                control,
                ExecutionReason::DriverRetirementContextDrop.code().get(),
                || drop(ManuallyDrop::into_inner(context)),
            );
        }
        if self.phases[4].unknown {
            self.unresolved = true;
            return Poll::Unresolved;
        }
        if self.phases[11].unknown || (self.phases[0].unknown && !control.is_stopped()) {
            // This hint may lag adjudication. Conservatively retain the
            // original completion/lease instead of fabricating local exit.
            self.unresolved = true;
            return Poll::Unresolved;
        }
        if let Some(lease) = self.lease.take() {
            self.phases[5].run(
                control,
                ExecutionReason::DriverRetirementLeaseDrop.code().get(),
                || drop(ManuallyDrop::into_inner(lease)),
            );
        }
        if self.phases[5].unknown {
            self.unresolved = true;
            return Poll::Unresolved;
        }
        // This is not ordinary Drop: the real driver and its context/lease
        // already returned from their explicit once destructors.
        let completion = self.completion.as_ref().expect("original completion");
        if let Some(last) = self.phases[6].run(
            control,
            ExecutionReason::DriverRetirementCompletion.code().get(),
            || completion.driver_finished(),
        ) {
            self.last = last;
        }
        if self.phases[6].unknown {
            self.unresolved = true;
            return Poll::Unresolved;
        }
        if self.last {
            let scheduler = self
                .scheduler
                .as_ref()
                .expect("original scheduler metadata");
            self.phases[7].run(
                control,
                ExecutionReason::DriverRetirementSchedulerShutdown
                    .code()
                    .get(),
                || scheduler.shutdown(),
            );
        }
        if let Some(scheduler) = self.scheduler.take() {
            self.phases[8].run(
                control,
                ExecutionReason::DriverRetirementSchedulerDrop.code().get(),
                || drop(ManuallyDrop::into_inner(scheduler)),
            );
        }
        let completion = self
            .completion
            .take()
            .expect("original completion metadata");
        self.phases[9].run(
            control,
            ExecutionReason::DriverRetirementCompletionDrop.code().get(),
            || drop(ManuallyDrop::into_inner(completion)),
        );
        // Last control/source/observer captures are a separate metadata role.
        // Its failure cannot revoke the already frozen actual work receipt.
        let control = self
            .control
            .take()
            .expect("one final original control drop");
        self.phases[12].run_metadata(
            &self.health,
            ExecutionReason::DriverRetirementControlDrop.code().get(),
            || drop(ManuallyDrop::into_inner(control)),
        );
        self.unresolved = self.phases.iter().any(|phase| phase.unknown);
        if self.unresolved {
            Poll::Unresolved
        } else {
            Poll::Exited
        }
    }
}

/// One continuation for one original accepted batch, not one per driver or
/// poll. Removing this scheduling entry never releases an Unknown original
/// slot, executor lease, Task remaining count or resource responsibility.
pub(super) enum ReaperEntry {
    Batch {
        owner: Arc<BatchOwner>,
        shutdown_failure: bool,
    },
    Task(Arc<super::super::driver_retirement::DriverExitCustody>),
}
impl ReaperEntry {
    pub(super) fn from_pending(task: DriverTask) -> Self {
        let mut retirement = Retirement::new(task, false);
        retirement.phases[0].started = true;
        retirement.phases[0].returned = true;
        // This is already PendingFinish, never a new cancellation author.
        retirement.phases[1].started = true;
        retirement.phases[1].returned = true;
        let custody = retirement.custody.clone();
        custody.retain_task_exit(retirement);
        Self::Task(custody)
    }
    pub(super) fn poll(&self) -> Poll {
        match self {
            Self::Batch {
                owner,
                shutdown_failure,
            } => owner.poll_batch(*shutdown_failure),
            Self::Task(custody) => custody.poll_task_exit(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::pipeline::{
        operator::{FinishWatch, OperatorLifecycle},
        shape::{Source, SourceKernel},
        step::{NextStep, StepResult},
        step_context::StepContext,
    };
    use crate::runtime::{
        fragment::io::{FragmentEvent, FragmentEventSink},
        runtime_state::RuntimeState,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    fn until(test: impl Fn() -> bool) -> bool {
        let end = Instant::now() + Duration::from_secs(3);
        while Instant::now() < end {
            if test() {
                return true;
            }
            thread::yield_now();
        }
        test()
    }
    #[derive(Debug)]
    struct Data {
        bytes: Vec<u8>,
        dropped: Arc<AtomicUsize>,
    }
    impl Drop for Data {
        fn drop(&mut self) {
            assert_eq!(self.bytes.len(), 4096);
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Secondary {
        _data: Arc<Data>,
    }
    struct Primary {
        data: Option<Arc<Data>>,
        control: Arc<RuntimeErrorState>,
    }
    impl Drop for Primary {
        fn drop(&mut self) {
            assert!(
                self.control.local_exit_fault().is_some(),
                "health precedes opaque disposal"
            );
            std::panic::panic_any(Secondary {
                _data: self.data.take().unwrap(),
            });
        }
    }
    type ContextGate = Arc<(Mutex<(bool, bool)>, Condvar)>;
    struct StorageSink {
        gate: Option<ContextGate>,
        data: Option<Arc<Data>>,
        unknown: bool,
        control: Arc<RuntimeErrorState>,
    }
    impl FragmentEventSink for StorageSink {
        fn record(&self, _: FragmentEvent) {}
    }
    impl Drop for StorageSink {
        fn drop(&mut self) {
            if let Some(gate) = &self.gate {
                let mut state = gate.0.lock().unwrap();
                state.0 = true;
                gate.1.notify_all();
                while !state.1 {
                    state = gate.1.wait(state).unwrap();
                }
            }
            if self.unknown {
                std::panic::panic_any(Primary {
                    data: self.data.take(),
                    control: self.control.clone(),
                });
            }
            drop(self.data.take());
        }
    }
    struct Kernel {
        pending: Arc<AtomicBool>,
        cancelled: Arc<AtomicBool>,
        observed: Arc<AtomicBool>,
        drops: Arc<AtomicUsize>,
        observable: Arc<Observable>,
    }
    impl Drop for Kernel {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl OperatorLifecycle for Kernel {
        fn name(&self) -> &str {
            "owned_batch_actual_kernel"
        }
        fn cancel(&mut self) {
            self.cancelled.store(true, Ordering::SeqCst);
        }
        fn pending_finish(&self) -> Option<FinishWatch> {
            self.observed.store(true, Ordering::SeqCst);
            self.pending
                .load(Ordering::SeqCst)
                .then(|| FinishWatch::Notify(self.observable.clone()))
        }
    }
    impl SourceKernel for Kernel {
        fn poll_output(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            Ok(StepResult::new(None, NextStep::Finished, 0))
        }
    }
    struct Fixture {
        task: DriverTask,
        storage: Weak<Data>,
        data_drops: Arc<AtomicUsize>,
        kernel_drops: Arc<AtomicUsize>,
        pending: Arc<AtomicBool>,
        cancelled: Arc<AtomicBool>,
        observed: Arc<AtomicBool>,
        observable: Arc<Observable>,
    }
    fn fixture(
        id: i32,
        completion: Arc<FragmentCompletion>,
        unknown: bool,
        pending: bool,
    ) -> Fixture {
        fixture_options(id, completion, unknown, pending, None, None)
    }
    fn fixture_options(
        id: i32,
        completion: Arc<FragmentCompletion>,
        unknown: bool,
        pending: bool,
        gate: Option<ContextGate>,
        observable: Option<Arc<Observable>>,
    ) -> Fixture {
        let control = completion.control.clone();
        let runtime = Arc::new(RuntimeState::new(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            control.clone(),
        ));
        let data_drops = Arc::new(AtomicUsize::new(0));
        let data = Arc::new(Data {
            bytes: vec![7; 4096],
            dropped: data_drops.clone(),
        });
        let storage = Arc::downgrade(&data);
        let sink: Arc<dyn FragmentEventSink> = Arc::new(StorageSink {
            gate,
            data: Some(data),
            unknown,
            control,
        });
        let context = Arc::new(FragmentContext::new_report_neutral(
            None,
            runtime.clone(),
            Some((4300, id.into())),
            sink,
        ));
        let pending = Arc::new(AtomicBool::new(pending));
        let cancelled = Arc::new(AtomicBool::new(false));
        let observed = Arc::new(AtomicBool::new(false));
        let kernel_drops = Arc::new(AtomicUsize::new(0));
        let observable = observable.unwrap_or_else(|| Arc::new(Observable::new()));
        let kernel = Kernel {
            pending: pending.clone(),
            cancelled: cancelled.clone(),
            observed: observed.clone(),
            drops: kernel_drops.clone(),
            observable: observable.clone(),
        };
        let driver = PipelineDriver::new(
            id,
            vec![Box::new(Source::new(kernel))],
            None,
            Vec::new(),
            runtime,
            Some((4300, id.into())),
        );
        Fixture {
            task: DriverTask::new(driver, completion, context, Duration::from_millis(1)),
            storage,
            data_drops,
            kernel_drops,
            pending,
            cancelled,
            observed,
            observable,
        }
    }

    #[test]
    fn actual_worker_receipt_before_start_activation_is_preserved() {
        let executor = Arc::new(GlobalDriverExecutor::new(1));
        let control = Arc::new(RuntimeErrorState::default());
        let completion = FragmentCompletion::new_prepared(1, control);
        let Fixture {
            task,
            storage,
            data_drops,
            kernel_drops,
            ..
        } = fixture(1, completion.clone(), false, false);
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        *executor.start_activation_gate.lock().unwrap() = Some(gate.clone());
        let submitter = {
            let executor = executor.clone();
            thread::spawn(move || executor.submit(vec![task]))
        };
        // Real worker processing, last kernel/context storage Drop and the real
        // worker receipt run while only start activation is held. No count is edited.
        let actual_exited = until(|| {
            kernel_drops.load(Ordering::SeqCst) == 1
                && data_drops.load(Ordering::SeqCst) == 1
                && completion.mu.lock().unwrap().remaining == 0
        });
        let no_early_fact = completion.stopped_fact().is_none();
        *gate.0.lock().unwrap() = true;
        gate.1.notify_all();
        let admitted = submitter.join().expect("actual submitter joined");
        let stopped = until(|| completion.stopped_fact().is_some());
        executor.shutdown().expect("actual executor join");
        assert!(actual_exited && no_early_fact && admitted && stopped);
        assert!(storage.upgrade().is_none());
        assert!(completion.stopped_fact().unwrap().conclusion().is_ok());
    }

    #[test]
    fn unknown_context_payload_keeps_original_slot_while_pending_sibling_really_exits() {
        let executor = GlobalDriverExecutor::new(1);
        executor.shutdown().unwrap();
        let control = Arc::new(RuntimeErrorState::default());
        let original: ExecutionFailure =
            novarocks_functions::KernelFailure::ResourceExhausted.into();
        control.set_error(original.clone());
        let completion = FragmentCompletion::new(2, control.clone());
        let Fixture {
            task: first,
            storage: held,
            data_drops: held_drops,
            kernel_drops: first_kernel,
            ..
        } = fixture(1, completion.clone(), true, false);
        let Fixture {
            task: second,
            storage: released,
            data_drops: second_drops,
            kernel_drops: second_kernel,
            pending,
            cancelled,
            observed,
            observable,
        } = fixture(2, completion.clone(), false, true);
        let batch = DriverBatch::new(vec![first, second]);
        let refused = !executor.submit_owned(&batch);
        let requested = cancelled.load(Ordering::SeqCst) && observed.load(Ordering::SeqCst);
        let no_forged_stop = completion.stopped_fact().is_none();
        pending.store(false, Ordering::SeqCst);
        observable.notify_observers();
        let sibling_exited = until(|| {
            second_kernel.load(Ordering::SeqCst) == 1 && second_drops.load(Ordering::SeqCst) == 1
        });
        assert!(refused && requested && no_forged_stop && sibling_exited);
        assert_eq!(first_kernel.load(Ordering::SeqCst), 1);
        assert!(held.upgrade().is_some());
        assert_eq!(held_drops.load(Ordering::SeqCst), 0);
        assert!(released.upgrade().is_none());
        assert!(completion.stopped_fact().is_none());
        assert!(control.error().unwrap().same_occurrence(&original));
        assert_eq!(
            control.local_exit_fault().unwrap().fact().reason(),
            StartFault(ExecutionReason::DriverRetirementContextDrop.code().get())
                .failure_fact()
                .reason()
        );
        let state = batch.inner.state.lock().unwrap();
        let Work::Retiring(unknown) = &state.slots[0].as_ref().unwrap().work else {
            panic!("original unknown slot retains its retirement")
        };
        assert!(unknown.unresolved && unknown.completion.is_some() && unknown.lease.is_some());
        assert!(
            state.slots[1].is_none(),
            "healthy sibling's original cell actually retired"
        );
    }

    #[test]
    fn publication_unwind_after_original_acceptance_is_metadata_not_another_required_failure() {
        let executor = GlobalDriverExecutor::new(1);
        let control = Arc::new(RuntimeErrorState::default());
        control
            .stop_observable()
            .add_observer(Arc::new(|| panic!("actual stop notification")));
        let completion = FragmentCompletion::new_prepared(1, control.clone());
        let Fixture {
            task,
            storage,
            data_drops,
            ..
        } = fixture(1, completion.clone(), false, false);
        let batch = DriverBatch::new(vec![task]);
        let original: ExecutionFailure =
            novarocks_functions::KernelFailure::ResourceExhausted.into();
        let proposal = batch.install_initial_failure(original.clone());
        assert!(
            batch
                .metadata(1, &control, || control.set_error(proposal))
                .is_none()
        );
        assert!(control.is_stopped());
        assert!(batch.dispose_initial_failure(&control));
        assert!(executor.submit_owned(&batch));
        let exited = until(|| completion.stopped_fact().is_some());
        executor.shutdown().unwrap();
        assert!(exited);
        assert_eq!(data_drops.load(Ordering::SeqCst), 1);
        assert!(storage.upgrade().is_none());
        assert!(control.error().unwrap().same_occurrence(&original));
        assert!(
            completion
                .stopped_fact()
                .unwrap()
                .conclusion()
                .unwrap_err()
                .same_occurrence(&original)
        );
        let state = batch.inner.state.lock().unwrap();
        assert!(state.metadata_unknown && state.slots.iter().all(Option::is_none));
        assert!(
            state.initial_failure.is_none(),
            "metadata residue owns no source or driver fields"
        );
    }

    #[test]
    fn another_batch_activation_cannot_release_an_inflight_original_start_hold() {
        let executor = Arc::new(GlobalDriverExecutor::new(1));
        let control = Arc::new(RuntimeErrorState::default());
        let completion = FragmentCompletion::new_prepared(2, control);
        let Fixture {
            task: first,
            data_drops: first_data,
            kernel_drops: first_kernel,
            ..
        } = fixture(1, completion.clone(), false, false);
        let Fixture {
            task: second,
            data_drops: second_data,
            kernel_drops: second_kernel,
            ..
        } = fixture(2, completion.clone(), false, false);
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        *executor.start_activation_gate.lock().unwrap() = Some(gate.clone());
        let first_submit = {
            let executor = executor.clone();
            thread::spawn(move || executor.submit(vec![first]))
        };
        let first_exited = until(|| {
            first_kernel.load(Ordering::SeqCst) == 1
                && first_data.load(Ordering::SeqCst) == 1
                && completion.mu.lock().unwrap().remaining == 1
                && executor.start_activation_gate.lock().unwrap().is_none()
        });
        let second_admitted = executor.submit(vec![second]);
        let both_exited = until(|| {
            second_kernel.load(Ordering::SeqCst) == 1
                && second_data.load(Ordering::SeqCst) == 1
                && completion.mu.lock().unwrap().remaining == 0
        });
        let original_hold_preserved = completion.stopped_fact().is_none();
        *gate.0.lock().unwrap() = true;
        gate.1.notify_all();
        let first_admitted = first_submit.join().expect("actual first submitter joined");
        let stopped = until(|| completion.stopped_fact().is_some());
        executor.shutdown().expect("actual executor join");
        assert!(
            first_exited
                && second_admitted
                && both_exited
                && original_hold_preserved
                && first_admitted
                && stopped
        );
    }

    struct CleanupDependency {
        dependency: Option<Arc<AtomicBool>>,
        release_all: Arc<AtomicBool>,
        closed: Arc<AtomicBool>,
        cancelled: Arc<AtomicBool>,
        observations: Arc<AtomicUsize>,
        minimum_observations: usize,
        storage: Option<Arc<Data>>,
        observable: Arc<Observable>,
    }
    impl Drop for CleanupDependency {
        fn drop(&mut self) {
            // The dependency is satisfied by the real last data owner exiting,
            // not by cancellation, an edited completion count or a test flag.
            drop(self.storage.take());
            self.closed.store(true, Ordering::SeqCst);
        }
    }
    impl OperatorLifecycle for CleanupDependency {
        fn name(&self) -> &str {
            "rejected_batch_cleanup_dependency"
        }
        fn cancel(&mut self) {
            self.cancelled.store(true, Ordering::SeqCst);
        }
        fn pending_finish(&self) -> Option<FinishWatch> {
            let observation = self.observations.fetch_add(1, Ordering::SeqCst) + 1;
            let dependency_pending = self
                .dependency
                .as_ref()
                .is_some_and(|closed| !closed.load(Ordering::SeqCst));
            (!self.release_all.load(Ordering::SeqCst)
                && (dependency_pending || observation < self.minimum_observations))
                .then(|| FinishWatch::Notify(self.observable.clone()))
        }
    }
    impl SourceKernel for CleanupDependency {
        fn poll_output(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            panic!("a rejected original driver must only drain")
        }
    }
    struct CleanupProbe {
        closed: Arc<AtomicBool>,
        cancelled: Arc<AtomicBool>,
        observations: Arc<AtomicUsize>,
        data_drops: Arc<AtomicUsize>,
        storage: Weak<Data>,
    }
    fn dependency_task(
        id: i32,
        completion: Arc<FragmentCompletion>,
        dependency: Option<Arc<AtomicBool>>,
        release_all: Arc<AtomicBool>,
        minimum_observations: usize,
    ) -> (DriverTask, CleanupProbe) {
        let runtime = Arc::new(RuntimeState::new(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            completion.control.clone(),
        ));
        let context = Arc::new(FragmentContext::new(
            None,
            runtime.clone(),
            Some((4301, id.into())),
            None,
            None,
            None,
        ));
        let data_drops = Arc::new(AtomicUsize::new(0));
        let data = Arc::new(Data {
            bytes: vec![11; 4096],
            dropped: data_drops.clone(),
        });
        let probe = CleanupProbe {
            closed: Arc::new(AtomicBool::new(false)),
            cancelled: Arc::new(AtomicBool::new(false)),
            observations: Arc::new(AtomicUsize::new(0)),
            data_drops,
            storage: Arc::downgrade(&data),
        };
        let kernel = CleanupDependency {
            dependency,
            release_all,
            closed: probe.closed.clone(),
            cancelled: probe.cancelled.clone(),
            observations: probe.observations.clone(),
            minimum_observations,
            storage: Some(data),
            observable: Arc::new(Observable::new()),
        };
        let driver = PipelineDriver::new(
            id,
            vec![Box::new(Source::new(kernel))],
            None,
            Vec::new(),
            runtime,
            Some((4301, id.into())),
        );
        (
            DriverTask::new(driver, completion, context, Duration::from_millis(1)),
            probe,
        )
    }
    fn probes_exited(probes: &[CleanupProbe]) -> bool {
        probes.iter().all(|probe| {
            probe.closed.load(Ordering::SeqCst)
                && probe.cancelled.load(Ordering::SeqCst)
                && probe.data_drops.load(Ordering::SeqCst) == 1
                && probe.storage.upgrade().is_none()
        })
    }

    #[test]
    fn rejected_batch_beyond_256_slots_advances_dependent_tail_cleanup() {
        const N: usize = 258;
        let executor = Arc::new(GlobalDriverExecutor::new(1));
        executor.shutdown().unwrap();
        let control = Arc::new(RuntimeErrorState::default());
        let original: ExecutionFailure =
            novarocks_functions::KernelFailure::ResourceExhausted.into();
        control.set_error(original.clone());
        let completion = FragmentCompletion::new(N, control.clone());
        let release_all = Arc::new(AtomicBool::new(false));
        let (tail, tail_probe) =
            dependency_task(N as i32, completion.clone(), None, release_all.clone(), 6);
        let mut tasks = Vec::with_capacity(N);
        let mut probes = Vec::with_capacity(N);
        for index in 0..N - 1 {
            let (task, probe) = dependency_task(
                index as i32,
                completion.clone(),
                Some(tail_probe.closed.clone()),
                release_all.clone(),
                0,
            );
            tasks.push(task);
            probes.push(probe);
        }
        tasks.push(tail);
        probes.push(tail_probe);
        let submitter = {
            let executor = executor.clone();
            thread::spawn(move || executor.submit(tasks))
        };
        let independently_exited =
            until(|| probes_exited(&probes) && completion.stopped_fact().is_some());
        // Unblock an old broken implementation before joining/asserting. This
        // fallback is never the positive exit oracle under test.
        release_all.store(true, Ordering::SeqCst);
        let admitted = submitter
            .join()
            .expect("actual original batch submitter joined");
        let all_retired = until(|| probes_exited(&probes) && completion.stopped_fact().is_some());
        assert!(independently_exited && all_retired && !admitted);
        assert!(
            probes[N - 1].observations.load(Ordering::SeqCst) >= 6,
            "the real tail close needed repeated polling after first cancellation"
        );
        assert_eq!(completion.remaining_drivers(), 0);
        assert!(control.error().unwrap().same_occurrence(&original));
    }

    #[test]
    fn more_than_256_batches_do_not_block_the_original_dependency_exit() {
        const N: usize = 258;
        let executor = Arc::new(GlobalDriverExecutor::new(1));
        executor.shutdown().unwrap();
        let original: ExecutionFailure =
            novarocks_functions::KernelFailure::ResourceExhausted.into();
        let release_all = Arc::new(AtomicBool::new(false));
        let mut completions = Vec::with_capacity(N);
        for _ in 0..N {
            let control = Arc::new(RuntimeErrorState::default());
            control.set_error(original.clone());
            completions.push(FragmentCompletion::new(1, control));
        }
        let (tail, tail_probe) = dependency_task(
            N as i32,
            completions[N - 1].clone(),
            None,
            release_all.clone(),
            6,
        );
        let mut tasks = Vec::with_capacity(N);
        let mut probes = Vec::with_capacity(N);
        for (index, completion) in completions.iter().enumerate().take(N - 1) {
            let (task, probe) = dependency_task(
                index as i32,
                completion.clone(),
                Some(tail_probe.closed.clone()),
                release_all.clone(),
                0,
            );
            tasks.push(task);
            probes.push(probe);
        }
        tasks.push(tail);
        probes.push(tail_probe);
        let submitter = {
            let executor = executor.clone();
            thread::spawn(move || {
                let mut all_refused = true;
                for task in tasks {
                    all_refused &= !executor.submit(vec![task]);
                }
                all_refused
            })
        };
        let independently_exited = until(|| {
            probes_exited(&probes)
                && completions
                    .iter()
                    .all(|completion| completion.stopped_fact().is_some())
        });
        release_all.store(true, Ordering::SeqCst);
        let all_refused = submitter
            .join()
            .expect("all original batch submissions joined");
        let all_retired = until(|| {
            probes_exited(&probes)
                && completions
                    .iter()
                    .all(|completion| completion.stopped_fact().is_some())
        });
        assert!(independently_exited && all_retired && all_refused);
        assert!(probes[N - 1].observations.load(Ordering::SeqCst) >= 6);
        assert!(completions.iter().all(|completion| {
            completion.remaining_drivers() == 0
                && completion
                    .control
                    .error()
                    .unwrap()
                    .same_occurrence(&original)
        }));
    }

    #[test]
    fn healthy_worker_keeps_original_lease_until_context_drop_really_returns() {
        let executor = GlobalDriverExecutor::new(1);
        let control = Arc::new(RuntimeErrorState::default());
        let completion = FragmentCompletion::new(2, control);
        let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
        let Fixture {
            task: first,
            storage,
            data_drops,
            ..
        } = fixture_options(
            1,
            completion.clone(),
            false,
            false,
            Some(gate.clone()),
            None,
        );
        let Fixture {
            task: second,
            data_drops: sibling_drop,
            ..
        } = fixture(2, completion.clone(), false, false);
        assert!(executor.submit(vec![first, second]));
        let entered = until(|| gate.0.lock().unwrap().0);
        let original_owned = executor.shared.live_tasks.load(Ordering::SeqCst) == 2
            && completion.remaining_drivers() == 2
            && completion.stopped_fact().is_none()
            && storage.upgrade().is_some()
            && data_drops.load(Ordering::SeqCst) == 0;
        {
            let mut state = gate.0.lock().unwrap();
            state.1 = true;
            gate.1.notify_all();
        }
        let actual_exit = until(|| {
            data_drops.load(Ordering::SeqCst) == 1
                && sibling_drop.load(Ordering::SeqCst) == 1
                && completion.stopped_fact().is_some()
        });
        executor
            .shutdown()
            .expect("the real worker and dispatcher join");
        assert!(entered && original_owned && actual_exit);
        assert!(storage.upgrade().is_none());
        assert_eq!(completion.remaining_drivers(), 0);
    }

    #[test]
    fn cold_context_drop_unknown_keeps_original_real_lease_and_caller_continues_sibling() {
        let shared = Arc::new(ExecutorShared::new());
        let control = Arc::new(RuntimeErrorState::default());
        let original: ExecutionFailure =
            novarocks_functions::KernelFailure::ResourceExhausted.into();
        control.set_error(original.clone());
        let completion = FragmentCompletion::new(2, control.clone());
        let Fixture {
            mut task,
            storage,
            data_drops,
            ..
        } = fixture(1, completion.clone(), true, false);
        task.lease = Some(ExecutorTaskLease::acquire(&shared));
        let returned = catch_unwind(AssertUnwindSafe(|| task.driver_finished())).is_ok();
        let original_kept = shared.live_tasks.load(Ordering::SeqCst) == 1
            && completion.remaining_drivers() == 2
            && completion.stopped_fact().is_none()
            && storage.upgrade().is_some()
            && data_drops.load(Ordering::SeqCst) == 0;
        let Fixture {
            mut task,
            data_drops: sibling_drop,
            ..
        } = fixture(2, completion.clone(), false, false);
        task.lease = Some(ExecutorTaskLease::acquire(&shared));
        task.driver_finished();
        assert!(returned && original_kept);
        assert_eq!(sibling_drop.load(Ordering::SeqCst), 1);
        assert_eq!(shared.live_tasks.load(Ordering::SeqCst), 1);
        assert_eq!(completion.remaining_drivers(), 1);
        assert!(completion.stopped_fact().is_none());
        assert!(control.error().unwrap().same_occurrence(&original));
        assert_eq!(
            control.local_exit_fault().unwrap().fact().reason(),
            StartFault(ExecutionReason::DriverRetirementContextDrop.code().get())
                .failure_fact()
                .reason()
        );
        // This proves the real cold entry, not an entire worker-pool restart.
        // No worker thread was created and no retained count is edited.
    }

    #[test]
    fn typed_park_capacity_refusal_keeps_real_input_until_close_and_worker_progresses() {
        let executor = GlobalDriverExecutor::new(1);
        let control = Arc::new(RuntimeErrorState::default());
        let completion = FragmentCompletion::new(2, control.clone());
        let observable = Arc::new(Observable::try_bounded(1).unwrap());
        observable.add_observer(Arc::new(|| {}));
        let Fixture {
            mut task,
            storage,
            data_drops,
            pending,
            ..
        } = fixture_options(1, completion.clone(), false, true, None, Some(observable));
        let typed_refusal = matches!(
            task.refresh_pending_finish_wait(),
            Err(crate::runtime::observable::ObservableCapacityError::CapacityExceeded)
        );
        let Fixture {
            task: second,
            data_drops: sibling_drop,
            ..
        } = fixture(2, completion.clone(), false, false);
        assert!(executor.submit(vec![task, second]));
        // Context payload Drop precedes the same task's lease and completion
        // receipt. Wait for that real retirement before inspecting both slots.
        let sibling_progress = until(|| {
            sibling_drop.load(Ordering::SeqCst) == 1
                && executor.shared.live_tasks.load(Ordering::SeqCst) == 1
                && completion.remaining_drivers() == 1
        });
        let original_owned = data_drops.load(Ordering::SeqCst) == 0
            && storage.upgrade().is_some()
            && executor.shared.live_tasks.load(Ordering::SeqCst) == 1
            && completion.remaining_drivers() == 1
            && completion.stopped_fact().is_none();
        let no_fabricated_failure =
            control.error().is_none() && control.local_exit_fault().is_none();
        pending.store(false, Ordering::SeqCst);
        let actual_exit =
            until(|| data_drops.load(Ordering::SeqCst) == 1 && completion.stopped_fact().is_some());
        executor
            .shutdown()
            .expect("real worker and dispatcher join after actual close");
        assert!(
            typed_refusal
                && sibling_progress
                && original_owned
                && no_fabricated_failure
                && actual_exit,
            "typed_refusal={typed_refusal} sibling_progress={sibling_progress} original_owned={original_owned} no_fabricated_failure={no_fabricated_failure} actual_exit={actual_exit}"
        );
        assert!(storage.upgrade().is_none());
    }

    struct LastControlSource {
        data: Option<Arc<Data>>,
        health: crate::runtime::task_control::ExecutionExitReporter,
        secondary_unknown: bool,
    }
    impl std::fmt::Debug for LastControlSource {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("LastControlSource")
        }
    }
    impl std::fmt::Display for LastControlSource {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("last local control source")
        }
    }
    impl std::error::Error for LastControlSource {}
    impl FailureProjection for LastControlSource {
        fn failure_fact(&self) -> FailureFact {
            StartFault(645).failure_fact()
        }
    }
    struct LastControlPrimary {
        data: Option<Arc<Data>>,
        health: crate::runtime::task_control::ExecutionExitReporter,
        secondary_unknown: bool,
    }
    impl Drop for LastControlSource {
        fn drop(&mut self) {
            std::panic::panic_any(LastControlPrimary {
                data: self.data.take(),
                health: self.health.clone(),
                secondary_unknown: self.secondary_unknown,
            });
        }
    }
    impl Drop for LastControlPrimary {
        fn drop(&mut self) {
            assert_eq!(
                self.health.failure().unwrap().fact().reason(),
                StartFault(ExecutionReason::DriverRetirementControlDrop.code().get())
                    .failure_fact()
                    .reason()
            );
            if self.secondary_unknown {
                std::panic::panic_any(Secondary {
                    _data: self.data.take().unwrap(),
                });
            }
            drop(self.data.take());
        }
    }
    fn last_control_case(secondary_unknown: bool) {
        let executor = GlobalDriverExecutor::new(1);
        let control = Arc::new(RuntimeErrorState::default());
        let health = control.exit_health_reporter();
        let source_drops = Arc::new(AtomicUsize::new(0));
        let data = Arc::new(Data {
            bytes: vec![13; 4096],
            dropped: source_drops.clone(),
        });
        let source_weak = Arc::downgrade(&data);
        control.set_error(ExecutionFailure::from_domain(LastControlSource {
            data: Some(data),
            health: health.clone(),
            secondary_unknown,
        }));
        let completion = FragmentCompletion::new(1, control.clone());
        let receipt = Arc::new(AtomicUsize::new(0));
        let observed_reason = Arc::new(Mutex::new(None));
        {
            let receipt = receipt.clone();
            let reason = observed_reason.clone();
            completion.bind_test_resources_finisher(move |fact| {
                *reason.lock().unwrap() = Some(fact.conclusion().unwrap_err().failure_fact());
                receipt.fetch_add(1, Ordering::SeqCst);
            });
        }
        let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
        let Fixture { task: first, .. } = fixture_options(
            1,
            completion.clone(),
            false,
            false,
            Some(gate.clone()),
            None,
        );
        let weak_control = Arc::downgrade(&control);
        let weak_completion = Arc::downgrade(&completion);
        drop(control);
        drop(completion);
        let sibling_control = Arc::new(RuntimeErrorState::default());
        let sibling_completion = FragmentCompletion::new(1, sibling_control);
        let Fixture {
            task: sibling,
            data_drops: sibling_drop,
            ..
        } = fixture(2, sibling_completion.clone(), false, false);
        assert!(executor.submit(vec![first, sibling]));
        // The original submit body's redundant control clone has really
        // retired before the worker attempts the last control destructor.
        let entered = until(|| gate.0.lock().unwrap().0);
        {
            let mut state = gate.0.lock().unwrap();
            state.1 = true;
            gate.1.notify_all();
        }
        let progressed = until(|| {
            receipt.load(Ordering::SeqCst) == 1
                && sibling_drop.load(Ordering::SeqCst) == 1
                && health.failure().is_some()
                && sibling_completion.stopped_fact().is_some()
        });
        executor
            .shutdown()
            .expect("metadata fault does not retain real task lease or kill worker");
        assert!(entered && progressed);
        assert_eq!(
            observed_reason.lock().unwrap().unwrap().reason(),
            StartFault(645).failure_fact().reason()
        );
        assert!(weak_control.upgrade().is_none() && weak_completion.upgrade().is_none());
        assert_eq!(
            health.failure().unwrap().fact().reason(),
            StartFault(ExecutionReason::DriverRetirementControlDrop.code().get())
                .failure_fact()
                .reason()
        );
        assert_eq!(
            source_drops.load(Ordering::SeqCst),
            usize::from(!secondary_unknown)
        );
        assert_eq!(source_weak.upgrade().is_some(), secondary_unknown);
    }
    #[test]
    fn last_local_control_source_drop_is_supervised_after_real_work_exit() {
        last_control_case(false);
    }
    #[test]
    fn last_local_control_secondary_unknown_keeps_only_original_metadata_and_worker_progresses() {
        last_control_case(true);
    }
}
