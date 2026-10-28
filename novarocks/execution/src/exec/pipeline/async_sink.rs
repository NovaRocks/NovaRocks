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
//! Receiver-owned asynchronous sink with pre-acceptance queue credit. Data
//! completion and the actor's actual join are separate observations.

#[cfg(test)]
use crate::exec::pipeline::operator::ExitProgress;
use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::{
    exec::{
        chunk::Chunk,
        pipeline::{
            input::{AcceptedInput, InputPreparationFailure, RawInput},
            operator::{
                ExitObservation, FinishWatch, InputConstructionFactory, InputEncodingPolicy,
            },
            schedule::observer::Observable,
            shape::{
                ControlStep, HydrateInput, InputPreparation, PreparationStep, ReceiverPreparation,
                ReceiverSlot, StepOperator,
            },
            step::{AdmissionNext, InputResult, NextStep, StepResult, WaitFact, WaitReason},
            step_context::StepContext,
        },
    },
    runtime::{
        fragment::{ExecutionFailure, ExecutionResult},
        mem_tracker::MemTracker,
        runtime_state::{RuntimeErrorState, RuntimeState},
    },
};
use futures::task::{ArcWake, waker_ref};
use std::{
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};
use tokio::{sync::mpsc, task::JoinHandle};

/// Backends are dormant until activation. Their output contains finite staged
/// artifacts, never an actor/context lease. Each operation retains ownership of
/// its children; close must observe all of their exits even when it returns an
/// error. Abort and close are idempotent, including abort after close when
/// cancellation races with finalization. A failed write does not exempt either.
#[async_trait::async_trait]
pub trait AsyncSinkBackend: Send + 'static {
    type Output: Send + 'static;
    fn bind_runtime_state(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        Ok(())
    }
    /// Borrowing lets the actor report a returned failure before input drops.
    async fn write_chunk(&mut self, chunk: &Chunk) -> ExecutionResult<()>;
    async fn finish(&mut self) -> ExecutionResult<Self::Output>;
    async fn abort(&mut self) -> ExecutionResult<()>;
    async fn close(&mut self) -> ExecutionResult<()>;
}

#[derive(Debug)]
enum AsyncSinkFailure {
    InvalidCapacity(usize),
    NotActivated,
    ClosedReceiver,
    MissingPermit,
    InputAfterFinish,
    ActorInterrupted {
        phase: &'static str,
    },
    Panicked {
        phase: &'static str,
        detail: novarocks_failure_contract::BoundedDetail,
    },
}
impl std::fmt::Display for AsyncSinkFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidCapacity(value) => write!(
                f,
                "async sink capacity {value} is outside the bounded channel range"
            ),
            Self::NotActivated => f.write_str("async sink work started before activation"),
            Self::ClosedReceiver => {
                f.write_str("async sink receiver closed before input admission")
            }
            Self::MissingPermit => f.write_str("async sink accepted input has no queue permit"),
            Self::InputAfterFinish => {
                f.write_str("async sink received input after data completion")
            }
            Self::ActorInterrupted { phase } => {
                write!(f, "async sink actor was interrupted during {phase}")
            }
            Self::Panicked { phase, detail } => write!(f, "async sink {phase} panicked: {detail}"),
        }
    }
}
impl std::error::Error for AsyncSinkFailure {}
impl novarocks_failure_contract::FailureProjection for AsyncSinkFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::fragment::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureFact};
        let (class, code) = match self {
            Self::InvalidCapacity(_) => (
                FailureClass::Invariant,
                ExecutionReason::AsyncSinkInvalidCapacity,
            ),
            Self::NotActivated => (
                FailureClass::Invariant,
                ExecutionReason::AsyncSinkNotActivated,
            ),
            Self::ClosedReceiver => (
                FailureClass::Invariant,
                ExecutionReason::AsyncSinkReceiverClosed,
            ),
            Self::MissingPermit => (
                FailureClass::Invariant,
                ExecutionReason::AsyncSinkMissingPermit,
            ),
            Self::InputAfterFinish => (
                FailureClass::Invariant,
                ExecutionReason::AsyncSinkInputAfterFinish,
            ),
            Self::ActorInterrupted { .. } => (
                FailureClass::Unexpected,
                ExecutionReason::AsyncSinkInterrupted,
            ),
            Self::Panicked { .. } => (FailureClass::Unexpected, ExecutionReason::AsyncSinkPanicked),
        };
        FailureFact::new(class, code.reason())
    }
}
fn fault(cause: AsyncSinkFailure) -> ExecutionFailure {
    ExecutionFailure::from_domain(cause)
}

struct SinkShared<O> {
    phase: AtomicUsize,
    phase_required: AtomicBool,
    unresolved: AtomicBool,
    parked: AtomicBool,
    frame_exited: AtomicBool,
    primary: Mutex<[Option<ManuallyDrop<SinkPanic>>; SINK_PHASES * 2]>,
    secondary: Mutex<[Option<ManuallyDrop<SinkPanic>>; SINK_PHASES * 2]>,
    notice_failed: [AtomicBool; SINK_NOTICES],
    notice_primary: Mutex<[Option<ManuallyDrop<SinkPanic>>; SINK_NOTICES]>,
    notice_secondary: Mutex<[Option<ManuallyDrop<SinkPanic>>; SINK_NOTICES]>,
    unknown_actor: Mutex<Option<ManuallyDrop<SinkFuture>>>,
    unknown_input: Mutex<Option<ManuallyDrop<SinkBatch>>>,
    self_custody: Mutex<Option<Arc<SinkShared<O>>>>,
    observable: Arc<Observable>,
    queued: AtomicUsize,
    stop: AtomicBool,
    // A held queue permit can keep mpsc open after input retirement faults.
    // Cancellation wakes the original actor independently of sender lifetime.
    stop_signal: tokio::sync::Notify,
    #[cfg(test)]
    input_wait_entered: AtomicBool,
    // An outcome can precede backend destruction and is never an exit receipt.
    finished: AtomicBool,
    failure: Mutex<Option<ExecutionFailure>>,
    result: Mutex<Option<O>>,
}
impl<O> SinkShared<O> {
    fn new() -> Self {
        Self {
            phase: AtomicUsize::new(SinkPhase::Bind as usize),
            phase_required: AtomicBool::new(true),
            unresolved: AtomicBool::new(false),
            parked: AtomicBool::new(false),
            frame_exited: AtomicBool::new(false),
            primary: Mutex::new(std::array::from_fn(|_| None)),
            secondary: Mutex::new(std::array::from_fn(|_| None)),
            notice_failed: std::array::from_fn(|_| AtomicBool::new(false)),
            notice_primary: Mutex::new(std::array::from_fn(|_| None)),
            notice_secondary: Mutex::new(std::array::from_fn(|_| None)),
            unknown_actor: Mutex::new(None),
            unknown_input: Mutex::new(None),
            self_custody: Mutex::new(None),
            observable: Arc::new(Observable::new()),
            queued: AtomicUsize::new(0),
            stop: AtomicBool::new(false),
            stop_signal: tokio::sync::Notify::new(),
            #[cfg(test)]
            input_wait_entered: AtomicBool::new(false),
            finished: AtomicBool::new(false),
            failure: Mutex::new(None),
            result: Mutex::new(None),
        }
    }
    fn wake(&self) {
        self.observable.notify_observers();
    }
    fn failure(&self) -> Option<ExecutionFailure> {
        self.failure
            .lock()
            .expect("async sink failure lock")
            .clone()
    }
}
struct ActorWake<O> {
    shared: Arc<SinkShared<O>>,
    control: Arc<RuntimeErrorState>,
}
impl<O: Send + 'static> ArcWake for ActorWake<O> {
    fn wake_by_ref(this: &Arc<Self>) {
        // Tokio completes this one JoinHandle once. Its metadata wake owns a
        // separate finite lane from the actor and the receiver's exit caller.
        this.shared.notify_lane(
            SinkNoticeLane::Join,
            SinkNotice::Ready,
            &this.control,
            &mut || this.shared.wake(),
        );
    }
}

struct AsyncTarget<'a, O> {
    sender: &'a Option<mpsc::Sender<SinkBatch>>,
    shared: &'a SinkShared<O>,
    capacity: NonZeroUsize,
}
impl<O: Send> InputEncodingPolicy for AsyncTarget<'_, O> {}
#[derive(Default)]
struct AsyncPreparation {
    reserved: Option<mpsc::OwnedPermit<SinkBatch>>,
    hydrate: HydrateInput,
}
impl<O: Send> InputPreparation<AsyncTarget<'_, O>> for AsyncPreparation {
    fn admission(
        &mut self,
        _: &mut StepContext<'_>,
        raw: &RawInput,
        target: &AsyncTarget<'_, O>,
    ) -> ExecutionResult<Option<AdmissionNext>> {
        HydrateInput::validate_uncovered(raw)?;
        let observable = target.shared.observable.clone();
        let observed_generation = observable.generation();
        let wait = || {
            Some(AdmissionNext::Wait(WaitFact {
                reason: WaitReason::Output,
                observable: observable.clone(),
                observed_generation,
                deadline: None,
            }))
        };
        // queued includes the write currently in flight. Only this receiver
        // increments it and the actor only releases capacity.
        if target.shared.queued.load(Ordering::Acquire) >= target.capacity.get() {
            return Ok(wait());
        }
        let sender = target
            .sender
            .as_ref()
            .ok_or_else(|| fault(AsyncSinkFailure::ClosedReceiver))?;
        match sender.clone().try_reserve_owned() {
            Ok(permit) => {
                self.reserved = Some(permit);
                Ok(None)
            }
            Err(mpsc::error::TrySendError::Full(_)) => Ok(wait()),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(target
                .shared
                .failure()
                .unwrap_or_else(|| fault(AsyncSinkFailure::ClosedReceiver))),
        }
    }
    fn accept(&mut self, raw: RawInput) -> AcceptedInput {
        self.hydrate.accept_raw(raw)
    }
    fn prepare(
        &mut self,
        ctx: &mut StepContext<'_>,
        input: AcceptedInput,
        target: &AsyncTarget<'_, O>,
    ) -> Result<PreparationStep, InputPreparationFailure> {
        self.hydrate.prepare_accepted(ctx, input, target)
    }
    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.hydrate.install_tracker(tracker);
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.hydrate.install_factory(factory);
    }
}

pub struct AsyncSinkOperator<B: AsyncSinkBackend> {
    // Accepted backing exits before its reserved channel credit and bindings.
    receiver: ReceiverSlot<AsyncPreparation>,
    name: String,
    capacity: NonZeroUsize,
    backend: Option<B>,
    rx: Option<mpsc::Receiver<SinkBatch>>,
    sender: Option<mpsc::Sender<SinkBatch>>,
    shared: Arc<SinkShared<B::Output>>,
    join: Option<JoinHandle<()>>,
    join_wake: Option<Arc<ActorWake<B::Output>>>,
    actor_exit: Option<ExecutionResult<()>>,
    error_state: Option<Arc<RuntimeErrorState>>,
    next: NextStep,
    finishing: bool,
    exiting: bool,
    failure: Option<ExecutionFailure>,
    staged: Option<ManuallyDrop<SinkBatch>>,
    cold_cleanup: [Option<ExecutionFailure>; 2],
    cold_health: Option<ExecutionFailure>,
}
impl<B: AsyncSinkBackend> AsyncSinkOperator<B> {
    pub fn try_new(name: impl Into<String>, backend: B, capacity: usize) -> ExecutionResult<Self> {
        let capacity = NonZeroUsize::new(capacity)
            .filter(|capacity| capacity.get() <= tokio::sync::Semaphore::MAX_PERMITS)
            .ok_or_else(|| fault(AsyncSinkFailure::InvalidCapacity(capacity)))?;
        let (sender, rx) = mpsc::channel(capacity.get());
        let shared = Arc::new(SinkShared::new());
        Ok(Self {
            receiver: ReceiverSlot::new(AsyncPreparation::default()),
            name: name.into(),
            capacity,
            backend: Some(backend),
            rx: Some(rx),
            sender: Some(sender),
            shared,
            join: None,
            join_wake: None,
            actor_exit: None,
            error_state: None,
            next: NextStep::NeedInput,
            finishing: false,
            exiting: false,
            failure: None,
            staged: None,
            cold_cleanup: [None, None],
            cold_health: None,
        })
    }
    pub fn take_output(&self) -> Option<B::Output> {
        if self.shared.stop.load(Ordering::Acquire)
            || self.shared.unresolved.load(Ordering::Acquire)
            || !matches!(self.actor_exit, Some(Ok(())))
        {
            return None;
        }
        self.shared
            .result
            .lock()
            .expect("async sink result lock")
            .take()
    }
    fn stop(&mut self) {
        self.exiting = true;
        self.shared.stop.store(true, Ordering::Release);
        if let Some(input) = self.receiver.take_accepted_for_exit() {
            let (chunk, permit) = input.into_exit_parts();
            assert!(
                self.staged.is_none(),
                "one original sink input continuation"
            );
            let mut batch = SinkBatch::new(chunk);
            batch.permit = permit.map(ManuallyDrop::new);
            batch.credit = self
                .receiver
                .preparation_mut()
                .reserved
                .take()
                .map(ManuallyDrop::new);
            self.staged = Some(ManuallyDrop::new(batch));
        }
        self.sender = None;
        self.next = NextStep::Finished;
        if let Some(control) = self.error_state.clone() {
            self.retire_staged(&control);
            self.shared.notify_lane(
                SinkNoticeLane::Accepted,
                SinkNotice::Ready,
                &control,
                &mut || self.shared.wake(),
            );
        }
        self.shared.stop_signal.notify_one();
    }
    fn retire_staged(&mut self, control: &RuntimeErrorState) {
        let Some(batch) = self.staged.as_mut() else {
            return;
        };
        if !self.shared.unresolved.load(Ordering::Acquire)
            && batch.retire(&self.shared, control, true)
        {
            self.staged = None;
        } else {
            let mut slot = self
                .shared
                .unknown_input
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            assert!(slot.is_none(), "one original accepted sink input capsule");
            *slot = self.staged.take();
        }
    }
    fn retire_dormant(&mut self) {
        if self.join.is_some() || !self.exiting {
            return;
        }
        if let Some(backend) = self.backend.take() {
            self.retire_cold_holder(SinkPhase::Backend, 0, backend);
        }
        if let Some(rx) = self.rx.take() {
            self.retire_cold_holder(SinkPhase::Credit, 1, rx);
        }
    }
    fn retire_cold_holder<T>(&mut self, phase: SinkPhase, index: usize, holder: T) {
        if let Err(payload) = drop_sink_holder(ManuallyDrop::new(holder)) {
            if let Some(control) = self.error_state.as_ref() {
                let failure = self.shared.caught(phase, true, payload, control);
                self.shared.unknown(&failure, control);
                self.shared.dispose(phase, true, control);
            } else {
                // No local/default authority is fabricated for a dormant,
                // unbound value. The observing worker consumes these facts.
                let failure = self.shared.save_panic(phase, true, payload);
                self.shared.unresolved.store(true, Ordering::Release);
                self.shared
                    .self_custody
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .get_or_insert_with(|| self.shared.clone());
                self.cold_cleanup[index] = Some(failure.clone());
                self.cold_health.get_or_insert(failure);
                let cell = phase as usize * 2 + 1;
                let mut holder = self
                    .shared
                    .primary
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())[cell]
                    .take()
                    .unwrap();
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| unsafe {
                    ManuallyDrop::drop(&mut holder)
                })) {
                    self.shared
                        .secondary
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())[cell] =
                        Some(ManuallyDrop::new(payload));
                }
            }
        }
    }
    fn fail(&mut self, ctx: &StepContext<'_>, error: ExecutionFailure) -> ExecutionFailure {
        if !self
            .shared
            .failure()
            .is_some_and(|original| original.same_occurrence(&error))
        {
            ctx.report_failure(error.clone());
        }
        self.failure.get_or_insert(error).clone()
    }
    fn wait_finish(&mut self) -> ExecutionResult<StepResult> {
        let observable = self.shared.observable.clone();
        let observed_generation = observable.generation();
        if let Some(error) = self.shared.failure() {
            return Err(error);
        }
        self.next = if self.shared.finished.load(Ordering::Acquire) {
            NextStep::Finished
        } else {
            NextStep::Wait(WaitFact {
                reason: WaitReason::Finish,
                observable,
                observed_generation,
                deadline: None,
            })
        };
        Ok(StepResult::new(None, self.next.clone(), 0))
    }
}

type SinkPanic = Box<dyn std::any::Any + Send + 'static>;
type SinkFuture = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>>;

use std::{
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
};

#[derive(Clone, Copy)]
#[repr(usize)]
enum SinkPhase {
    Bind,
    Input,
    Write,
    Finish,
    Abort,
    Close,
    LateAbort,
    LateClose,
    Chunk,
    Permit,
    Credit,
    Backend,
    Frame,
    Join,
    Output,
    AcceptedChunk,
    AcceptedPermit,
    AcceptedCredit,
}
const SINK_PHASES: usize = 18;
impl SinkPhase {
    fn owner(self) -> SinkNoticeLane {
        if matches!(
            self,
            Self::AcceptedChunk | Self::AcceptedPermit | Self::AcceptedCredit
        ) {
            SinkNoticeLane::Accepted
        } else {
            SinkNoticeLane::Actor
        }
    }
    fn required(self) -> bool {
        matches!(self, Self::Bind | Self::Write | Self::Finish)
    }
    fn name(self) -> &'static str {
        match self {
            Self::Bind => "bind",
            Self::Input => "input",
            Self::Write => "write",
            Self::Finish => "finish",
            Self::Abort | Self::LateAbort => "abort",
            Self::Close | Self::LateClose => "close",
            Self::Chunk => "input drop",
            Self::Permit => "input permit drop",
            Self::Credit => "queue credit drop",
            Self::Backend => "backend drop",
            Self::Frame => "actor frame",
            Self::Join => "actor join",
            Self::Output => "output drop",
            Self::AcceptedChunk => "accepted input drop",
            Self::AcceptedPermit => "accepted input permit drop",
            Self::AcceptedCredit => "accepted queue credit drop",
        }
    }
}
#[derive(Clone, Copy)]
#[repr(usize)]
enum SinkNotice {
    Required,
    Cleanup,
    Health,
    Ready,
}
#[derive(Clone, Copy)]
#[repr(usize)]
enum SinkNoticeLane {
    Actor,
    Accepted,
    Join,
}
const SINK_NOTICES: usize = 12;
fn panic_failure(phase: &'static str, payload: &(dyn std::any::Any + Send)) -> ExecutionFailure {
    let detail = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("unknown panic payload");
    fault(AsyncSinkFailure::Panicked {
        phase,
        detail: novarocks_failure_contract::BoundedDetail::truncating(detail),
    })
}

/// The original Chunk retires before its queue credit. Unknown destruction
/// retains the remaining original input/credit and actor; legacy field-level
/// counters do not certify a physical byte bound or successful data exit.
struct SinkBatch {
    chunk: Option<ManuallyDrop<Chunk>>,
    permit: Option<ManuallyDrop<crate::runtime::fragment::io::RootInputPermit>>,
    credit: Option<ManuallyDrop<mpsc::OwnedPermit<SinkBatch>>>,
}
impl SinkBatch {
    fn new(chunk: Chunk) -> Self {
        Self {
            chunk: Some(ManuallyDrop::new(chunk)),
            permit: None,
            credit: None,
        }
    }
    fn chunk(&self) -> &Chunk {
        self.chunk.as_ref().expect("live sink input")
    }
    fn retire<O>(
        &mut self,
        shared: &Arc<SinkShared<O>>,
        control: &RuntimeErrorState,
        owner: bool,
    ) -> bool {
        macro_rules! retire {
            ($field:ident, $phase:expr) => {
                if let Some(mut holder) = self.$field.take() {
                    if !owner {
                        shared.phase($phase);
                    }
                    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| unsafe {
                        ManuallyDrop::drop(&mut holder)
                    })) {
                        let failure = shared.caught($phase, true, payload, control);
                        shared.unknown_lane(
                            if owner {
                                SinkNoticeLane::Accepted
                            } else {
                                SinkNoticeLane::Actor
                            },
                            &failure,
                            control,
                        );
                        shared.dispose($phase, true, control);
                        return false;
                    }
                }
            };
        }
        retire!(
            chunk,
            if owner {
                SinkPhase::AcceptedChunk
            } else {
                SinkPhase::Chunk
            }
        );
        retire!(
            permit,
            if owner {
                SinkPhase::AcceptedPermit
            } else {
                SinkPhase::Permit
            }
        );
        retire!(
            credit,
            if owner {
                SinkPhase::AcceptedCredit
            } else {
                SinkPhase::Credit
            }
        );
        true
    }
}

impl<O> SinkShared<O> {
    fn phase(&self, phase: SinkPhase) {
        self.phase.store(phase as usize, Ordering::Release);
        self.phase_required
            .store(phase.required(), Ordering::Release);
    }
    fn current_required(&self) -> bool {
        self.phase_required.load(Ordering::Acquire)
    }
    fn current_phase(&self) -> SinkPhase {
        match self.phase.load(Ordering::Acquire) {
            0 => SinkPhase::Bind,
            1 => SinkPhase::Input,
            2 => SinkPhase::Write,
            3 => SinkPhase::Finish,
            4 => SinkPhase::Abort,
            5 => SinkPhase::Close,
            6 => SinkPhase::LateAbort,
            7 => SinkPhase::LateClose,
            8 => SinkPhase::Chunk,
            9 => SinkPhase::Permit,
            10 => SinkPhase::Credit,
            11 => SinkPhase::Backend,
            12 => SinkPhase::Frame,
            13 => SinkPhase::Join,
            _ => SinkPhase::Output,
        }
    }
    fn unknown(self: &Arc<Self>, failure: &ExecutionFailure, control: &RuntimeErrorState) {
        self.unknown_lane(SinkNoticeLane::Actor, failure, control)
    }
    fn unknown_lane(
        self: &Arc<Self>,
        owner: SinkNoticeLane,
        failure: &ExecutionFailure,
        control: &RuntimeErrorState,
    ) {
        self.unresolved.store(true, Ordering::Release);
        self.self_custody
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get_or_insert_with(|| self.clone());
        self.notify_lane(owner, SinkNotice::Health, control, &mut || {
            control.report_unresolved_exit(failure)
        });
        self.notify_lane(owner, SinkNotice::Ready, control, &mut || self.wake());
        self.stop_signal.notify_one();
    }
    fn notify(
        self: &Arc<Self>,
        notice: SinkNotice,
        control: &RuntimeErrorState,
        action: &mut dyn FnMut(),
    ) {
        self.notify_lane(SinkNoticeLane::Actor, notice, control, action);
    }
    fn notify_lane(
        self: &Arc<Self>,
        owner: SinkNoticeLane,
        notice: SinkNotice,
        control: &RuntimeErrorState,
        action: &mut dyn FnMut(),
    ) {
        let index = owner as usize * 4 + notice as usize;
        if self.notice_failed[index].load(Ordering::Acquire) {
            return;
        }
        if let Err(payload) = catch_unwind(AssertUnwindSafe(action)) {
            self.notice_failed[index].store(true, Ordering::Release);
            self.notice_primary
                .lock()
                .unwrap_or_else(|p| p.into_inner())[index] = Some(ManuallyDrop::new(payload));
            let failure = {
                let primary = self
                    .notice_primary
                    .lock()
                    .unwrap_or_else(|p| p.into_inner());
                panic_failure("notification", primary[index].as_ref().unwrap().as_ref())
            };
            self.unknown_lane(owner, &failure, control);
            self.notify_lane(owner, SinkNotice::Cleanup, control, &mut || {
                control.report_cleanup(&failure)
            });
            let mut primary = self
                .notice_primary
                .lock()
                .unwrap_or_else(|p| p.into_inner())[index]
                .take()
                .unwrap();
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| unsafe {
                ManuallyDrop::drop(&mut primary)
            })) {
                self.notice_secondary
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())[index] = Some(ManuallyDrop::new(payload));
            }
        }
    }
    fn report(self: &Arc<Self>, control: &RuntimeErrorState, error: ExecutionFailure) {
        let fresh = {
            let mut slot = self.failure.lock().unwrap_or_else(|p| p.into_inner());
            if slot.is_some() {
                false
            } else {
                *slot = Some(error.clone());
                true
            }
        };
        if fresh {
            self.notify(SinkNotice::Required, control, &mut || {
                control.set_error(error.clone())
            });
        }
        self.notify(SinkNotice::Ready, control, &mut || self.wake());
    }
    fn cleanup(self: &Arc<Self>, control: &RuntimeErrorState, error: &ExecutionFailure) {
        self.cleanup_lane(SinkNoticeLane::Actor, control, error);
    }
    fn cleanup_lane(
        self: &Arc<Self>,
        owner: SinkNoticeLane,
        control: &RuntimeErrorState,
        error: &ExecutionFailure,
    ) {
        self.notify_lane(owner, SinkNotice::Cleanup, control, &mut || {
            control.report_cleanup(error)
        });
        self.notify_lane(owner, SinkNotice::Ready, control, &mut || self.wake());
    }
    fn save_panic(
        self: &Arc<Self>,
        phase: SinkPhase,
        dropping: bool,
        payload: SinkPanic,
    ) -> ExecutionFailure {
        let index = phase as usize * 2 + usize::from(dropping);
        let mut cells = self.primary.lock().unwrap_or_else(|p| p.into_inner());
        assert!(
            cells[index].is_none(),
            "an unwound sink phase is never retried"
        );
        cells[index] = Some(ManuallyDrop::new(payload));
        let failure = panic_failure(phase.name(), cells[index].as_ref().unwrap().as_ref());
        drop(cells);
        failure
    }
    fn caught(
        self: &Arc<Self>,
        phase: SinkPhase,
        dropping: bool,
        payload: SinkPanic,
        control: &RuntimeErrorState,
    ) -> ExecutionFailure {
        let failure = self.save_panic(phase, dropping, payload);
        if phase.required() && !dropping {
            self.report(control, failure.clone());
        } else {
            self.cleanup_lane(phase.owner(), control, &failure);
        }
        failure
    }
    fn dispose(self: &Arc<Self>, phase: SinkPhase, dropping: bool, control: &RuntimeErrorState) {
        let index = phase as usize * 2 + usize::from(dropping);
        let Some(mut holder) = self.primary.lock().unwrap_or_else(|p| p.into_inner())[index].take()
        else {
            return;
        };
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| unsafe {
            ManuallyDrop::drop(&mut holder)
        })) {
            self.secondary.lock().unwrap_or_else(|p| p.into_inner())[index] =
                Some(ManuallyDrop::new(payload));
            let failure = {
                let cells = self.secondary.lock().unwrap_or_else(|p| p.into_inner());
                panic_failure(
                    "panic payload drop",
                    cells[index].as_ref().unwrap().as_ref(),
                )
            };
            self.unknown_lane(phase.owner(), &failure, control);
        }
    }
}

/// The guard is constructed synchronously before submission, including the
/// never-polled case. Unknown work is never repolled or implicitly destroyed.
struct SinkActorFrame<O> {
    future: Option<ManuallyDrop<SinkFuture>>,
    shared: Arc<SinkShared<O>>,
    control: Arc<RuntimeErrorState>,
}
impl<O> SinkActorFrame<O> {
    fn park(&mut self) {
        if let Some(future) = self.future.take() {
            let mut slot = self
                .shared
                .unknown_actor
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            assert!(slot.is_none(), "one original sink actor capsule");
            *slot = Some(future);
        }
    }
}
impl<O> std::future::Future for SinkActorFrame<O> {
    type Output = ();
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let polled = catch_unwind(AssertUnwindSafe(|| {
            self.future.as_mut().unwrap().as_mut().poll(cx)
        }));
        match polled {
            Ok(Poll::Pending) if !self.shared.parked.load(Ordering::Acquire) => Poll::Pending,
            Ok(Poll::Pending) => {
                self.park();
                Poll::Ready(())
            }
            Err(payload) => {
                self.park();
                let failure = self.shared.save_panic(SinkPhase::Frame, false, payload);
                if self.shared.current_required() {
                    self.shared.report(&self.control, failure.clone());
                } else {
                    self.shared.cleanup(&self.control, &failure);
                }
                self.shared.unknown(&failure, &self.control);
                self.shared.dispose(SinkPhase::Frame, false, &self.control);
                Poll::Ready(())
            }
            Ok(Poll::Ready(())) => {
                let future = self.future.take().unwrap();
                if let Err(payload) = drop_sink_holder(future) {
                    let failure =
                        self.shared
                            .caught(SinkPhase::Frame, true, payload, &self.control);
                    self.shared.unknown(&failure, &self.control);
                    self.shared.dispose(SinkPhase::Frame, true, &self.control);
                } else {
                    self.shared.frame_exited.store(true, Ordering::Release);
                }
                Poll::Ready(())
            }
        }
    }
}
impl<O> Drop for SinkActorFrame<O> {
    fn drop(&mut self) {
        if self.future.is_some() {
            self.park();
            // A dropped runtime wrapper, including one never polled, has not
            // demonstrated provider future/queue/backend destruction.
            let failure = fault(AsyncSinkFailure::ActorInterrupted {
                phase: self.shared.current_phase().name(),
            });
            if self.shared.current_required() {
                self.shared.report(&self.control, failure.clone());
            } else {
                self.shared.cleanup(&self.control, &failure);
            }
            self.shared.unknown(&failure, &self.control);
        }
    }
}
async fn park_actor<O>(shared: &SinkShared<O>) {
    shared.parked.store(true, Ordering::Release);
    std::future::pending::<()>().await;
}
async fn backend_call<T, F: std::future::Future<Output = ExecutionResult<T>>>(
    phase: SinkPhase,
    shared: &Arc<SinkShared<impl Sized>>,
    control: &RuntimeErrorState,
    construct: impl FnOnce() -> F,
) -> ExecutionResult<T> {
    shared.phase(phase);
    let future = match catch_unwind(AssertUnwindSafe(construct)) {
        Ok(future) => future,
        Err(payload) => {
            let failure = shared.caught(phase, false, payload, control);
            // No future was returned, so there is no positive destruction
            // boundary for this original constructor borrow. Never re-enter
            // backend cleanup through a guessed ordinary Err conversion.
            shared.unknown(&failure, control);
            shared.dispose(phase, false, control);
            shared.phase_required.store(false, Ordering::Release);
            park_actor(shared).await;
            unreachable!()
        }
    };
    let mut future = Some(ManuallyDrop::new(Box::pin(future)));
    let outcome = futures::future::poll_fn(|cx| {
        match catch_unwind(AssertUnwindSafe(|| {
            future.as_mut().unwrap().as_mut().poll(cx)
        })) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(result)) => Poll::Ready(Ok(result)),
            Err(payload) => Poll::Ready(Err(payload)),
        }
    })
    .await;
    let mut outcome = ManuallyDrop::new(Some(match outcome {
        Ok(result) => {
            if let Err(error) = &result {
                if phase.required() {
                    shared.report(control, error.clone());
                } else {
                    shared.cleanup(control, error);
                }
            }
            result
        }
        Err(payload) => {
            let failure = shared.caught(phase, false, payload, control);
            shared.dispose(phase, false, control);
            Err(failure)
        }
    }));
    let future = future.take().unwrap();
    shared.phase_required.store(false, Ordering::Release);
    if let Err(payload) = drop_sink_holder(future) {
        let failure = shared.caught(phase, true, payload, control);
        shared.unknown(&failure, control);
        shared.dispose(phase, true, control);
        // This exact borrowed operation did not complete its destructor. Keep
        // the entire original frame (and successful output, if any) pinned.
        park_actor(shared).await;
        unreachable!()
    }
    // Opaque custody can remain unknown independently of this now-returned
    // borrow. The caller still advances independent abort/close exactly once.
    outcome.take().unwrap()
}

fn drop_sink_holder<T>(mut holder: ManuallyDrop<T>) -> Result<(), SinkPanic> {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        ManuallyDrop::drop(&mut holder)
    }))
}
struct SinkStart<B: AsyncSinkBackend> {
    backend: Option<ManuallyDrop<B>>,
    rx: Option<ManuallyDrop<mpsc::Receiver<SinkBatch>>>,
}
async fn drain_loop<B: AsyncSinkBackend>(
    mut start: SinkStart<B>,
    shared: Arc<SinkShared<B::Output>>,
    control: Arc<RuntimeErrorState>,
    bind_unknown: bool,
) {
    if bind_unknown {
        // A synchronous bind unwind never supplied a normally returned borrow.
        // Its original backend and receiver remain in this protected frame.
        park_actor(&shared).await;
    }
    while shared.failure().is_none() && !shared.stop.load(Ordering::Acquire) {
        shared.phase(SinkPhase::Input);
        // There is exactly one actor input waiter. notify_one retains a wake
        // across the stop-check/registration gap; recv is cancelled only before
        // it has supplied an owned batch. Backend futures remain independently
        // supervised and are never abandoned by this input-only stop branch.
        let next = tokio::select! {
            biased;
            _ = shared.stop_signal.notified() => None,
            next = futures::future::poll_fn(|cx| {
                let next = start.rx.as_mut().unwrap().poll_recv(cx);
                #[cfg(test)]
                if next.is_pending() {
                    // Observe the real receiver wait after its waker is installed.
                    shared.input_wait_entered.store(true, Ordering::Release);
                }
                next
            }) => next,
        };
        let Some(batch) = next else {
            break;
        };
        let mut current = ManuallyDrop::new(batch);
        if !shared.stop.load(Ordering::Acquire) {
            let result = backend_call(SinkPhase::Write, &shared, &control, || {
                start.backend.as_mut().unwrap().write_chunk(current.chunk())
            })
            .await;
            let _ = result; // The original call already consumed its failure event.
        }
        if shared.unresolved.load(Ordering::Acquire) {
            break;
        }
        if !current.retire(&shared, &control, false) {
            break;
        }
        shared.queued.fetch_sub(1, Ordering::AcqRel);
        shared.notify(SinkNotice::Ready, &control, &mut || shared.wake());
    }
    shared.phase(SinkPhase::Credit);
    start.rx.as_mut().unwrap().close();
    if !shared.unresolved.load(Ordering::Acquire) {
        while let Ok(batch) = start.rx.as_mut().unwrap().try_recv() {
            let mut current = ManuallyDrop::new(batch);
            if !current.retire(&shared, &control, false) {
                break;
            }
            shared.queued.fetch_sub(1, Ordering::AcqRel);
        }
    }
    let mut output: ManuallyDrop<Option<B::Output>> = ManuallyDrop::new(None);
    if shared.failure().is_none()
        && !shared.stop.load(Ordering::Acquire)
        && !shared.unresolved.load(Ordering::Acquire)
        && let Ok(value) = backend_call(SinkPhase::Finish, &shared, &control, || {
            start.backend.as_mut().unwrap().finish()
        })
        .await
    {
        *output = Some(value);
    }
    // The original finish author already consumed any finish failure event.
    let aborted = shared.failure().is_some()
        || shared.stop.load(Ordering::Acquire)
        || shared.unresolved.load(Ordering::Acquire);
    if aborted
        && let Err(_error) = backend_call(SinkPhase::Abort, &shared, &control, || {
            start.backend.as_mut().unwrap().abort()
        })
        .await
    {
        // The same backend operation already reported this cleanup event.
    }
    if let Err(_error) = backend_call(SinkPhase::Close, &shared, &control, || {
        start.backend.as_mut().unwrap().close()
    })
    .await
    {
        // The same backend operation already reported this cleanup event.
    }
    if !aborted && shared.stop.load(Ordering::Acquire) {
        if let Err(_error) = backend_call(SinkPhase::LateAbort, &shared, &control, || {
            start.backend.as_mut().unwrap().abort()
        })
        .await
        {
            // The same backend operation already reported this cleanup event.
        }
        if let Err(_error) = backend_call(SinkPhase::LateClose, &shared, &control, || {
            start.backend.as_mut().unwrap().close()
        })
        .await
        {
            // The same backend operation already reported this cleanup event.
        }
    }
    shared.finished.store(true, Ordering::Release);
    shared.notify(SinkNotice::Ready, &control, &mut || shared.wake());
    if shared.unresolved.load(Ordering::Acquire) {
        park_actor(&shared).await;
    }
    shared.phase(SinkPhase::Output);
    if shared.failure().is_none() && !shared.stop.load(Ordering::Acquire) {
        *shared.result.lock().unwrap_or_else(|p| p.into_inner()) = output.take();
    } else if let Some(value) = output.take() {
        shared.phase(SinkPhase::Output);
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(value))) {
            let failure = shared.caught(SinkPhase::Output, true, payload, &control);
            shared.unknown(&failure, &control);
            shared.dispose(SinkPhase::Output, true, &control);
            park_actor(&shared).await;
        }
    }
    shared.phase(SinkPhase::Backend);
    let backend = start.backend.take().unwrap();
    if let Err(payload) = drop_sink_holder(backend) {
        let failure = shared.caught(SinkPhase::Backend, true, payload, &control);
        shared.unknown(&failure, &control);
        shared.dispose(SinkPhase::Backend, true, &control);
        park_actor(&shared).await;
    }
    // The queue is empty and its data/credits have already exited. Its receiver
    // is still explicitly retired before the frame can prove physical exit.
    shared.phase(SinkPhase::Credit);
    if let Err(payload) = drop_sink_holder(start.rx.take().unwrap()) {
        let failure = shared.caught(SinkPhase::Credit, true, payload, &control);
        shared.unknown(&failure, &control);
        shared.dispose(SinkPhase::Credit, true, &control);
        park_actor(&shared).await;
    }
}

impl<B: AsyncSinkBackend> OperatorLifecycle for AsyncSinkOperator<B> {
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_profiles(&mut self, profiles: crate::runtime::profile::OperatorProfiles) {
        self.receiver
            .preparation_mut()
            .hydrate
            .install_profiles(profiles);
    }
    fn name(&self) -> &str {
        &self.name
    }

    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.receiver
            .preparation_mut()
            .hydrate
            .install_tracker(tracker);
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.receiver
            .preparation_mut()
            .hydrate
            .install_factory(factory);
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.error_state.get_or_insert_with(|| state.error_state());
        Ok(())
    }
    fn activate(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        if self.join.is_some() || self.actor_exit.is_some() {
            return Ok(());
        }
        let executor = state.sink_io_executor()?;
        let control = self
            .error_state
            .get_or_insert_with(|| state.error_state())
            .clone();
        self.shared.phase(SinkPhase::Bind);
        let backend = self
            .backend
            .as_mut()
            .ok_or_else(|| fault(AsyncSinkFailure::NotActivated))?;
        let mut bind_unknown = false;
        let binding = match catch_unwind(AssertUnwindSafe(|| backend.bind_runtime_state(state))) {
            Ok(result) => result,
            Err(payload) => {
                let failure = self
                    .shared
                    .caught(SinkPhase::Bind, false, payload, &control);
                bind_unknown = true;
                self.shared.unknown(&failure, &control);
                self.shared.dispose(SinkPhase::Bind, false, &control);
                Err(failure)
            }
        };
        if let Err(error) = &binding {
            self.shared.report(&control, error.clone());
        }
        self.shared.phase(SinkPhase::Input);
        let start = SinkStart {
            backend: Some(ManuallyDrop::new(self.backend.take().unwrap())),
            rx: Some(ManuallyDrop::new(self.rx.take().expect("dormant receiver"))),
        };
        let shared = self.shared.clone();
        // Synchronous outer guard exists before executor.spawn can consume or
        // drop the future. All real provider/queue fields are already protected.
        let frame = SinkActorFrame {
            future: Some(ManuallyDrop::new(Box::pin(drain_loop(
                start,
                shared.clone(),
                control.clone(),
                bind_unknown,
            )))),
            shared,
            control,
        };
        self.join_wake = Some(Arc::new(ActorWake {
            shared: self.shared.clone(),
            control: self.error_state.as_ref().unwrap().clone(),
        }));
        self.join = Some(executor.spawn(frame));
        binding
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.exiting = true;
        if !self.finishing && !self.shared.finished.load(Ordering::Acquire) {
            self.stop();
        }
        Ok(())
    }
    fn cancel(&mut self) {
        self.stop();
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.begin_exit()
    }

    fn observe_exit(&mut self) -> ExitObservation {
        self.retire_dormant();
        let mut observation = self.observe_actor();
        if let Some(failure) = self.cold_cleanup.iter_mut().find_map(Option::take) {
            observation.failure = Some(super::operator::ExitFailure::Cleanup(failure));
        }
        observation.unresolved_exit = self.cold_health.take();
        observation
    }
    fn visit_exits(&mut self, visit: &mut dyn FnMut(ExitObservation)) {
        visit(self.observe_exit());
        if self.cold_cleanup.iter().any(Option::is_some) {
            visit(self.observe_exit());
        }
    }
}
impl<B: AsyncSinkBackend> AsyncSinkOperator<B> {
    fn observe_actor(&mut self) -> ExitObservation {
        let watch = || FinishWatch::Notify(self.shared.observable.clone());
        if self.actor_exit.is_some() {
            return if self.shared.unresolved.load(Ordering::Acquire) {
                ExitObservation::unresolved(watch())
            } else {
                ExitObservation::exited()
            };
        }
        let Some(join) = self.join.as_mut() else {
            return if self.shared.unresolved.load(Ordering::Acquire) {
                ExitObservation::unresolved(watch())
            } else if self.backend.is_some() || self.rx.is_some() {
                ExitObservation::pending(watch())
            } else {
                ExitObservation::exited()
            };
        };
        let waker = waker_ref(self.join_wake.as_ref().expect("activated sink join wake"));
        match std::pin::Pin::new(join).poll(&mut Context::from_waker(&waker)) {
            Poll::Pending => {
                if self.shared.unresolved.load(Ordering::Acquire) {
                    ExitObservation::unresolved(watch())
                } else {
                    ExitObservation::pending(watch())
                }
            }
            Poll::Ready(joined) => {
                self.join = None;
                let control = self
                    .error_state
                    .as_ref()
                    .expect("the original bound sink runtime");
                match joined {
                    Ok(()) if self.shared.frame_exited.load(Ordering::Acquire) => {}
                    Ok(()) if self.shared.unresolved.load(Ordering::Acquire) => {}
                    Err(error) if error.is_panic() => {
                        let failure =
                            self.shared
                                .save_panic(SinkPhase::Join, false, error.into_panic());
                        if self.shared.current_required()
                            && !self.shared.frame_exited.load(Ordering::Acquire)
                        {
                            self.shared.report(control, failure.clone());
                        } else {
                            self.shared.cleanup(control, &failure);
                        }
                        if !self.shared.frame_exited.load(Ordering::Acquire) {
                            self.shared.unknown(&failure, control);
                        }
                        self.shared.dispose(SinkPhase::Join, false, control);
                    }
                    Err(error)
                        if error.is_cancelled()
                            && self.shared.frame_exited.load(Ordering::Acquire) =>
                    {
                        self.shared.cleanup(
                            control,
                            &fault(AsyncSinkFailure::ActorInterrupted {
                                phase: self.shared.current_phase().name(),
                            }),
                        );
                    }
                    _ => {
                        let failure = fault(AsyncSinkFailure::ActorInterrupted {
                            phase: self.shared.current_phase().name(),
                        });
                        if self.shared.current_required() {
                            self.shared.report(control, failure.clone());
                        } else {
                            self.shared.cleanup(control, &failure);
                        }
                        self.shared.unknown(&failure, control);
                    }
                }
                self.actor_exit = Some(match self.shared.failure() {
                    Some(error) => Err(error),
                    None => Ok(()),
                });
                if self.shared.unresolved.load(Ordering::Acquire) {
                    ExitObservation::unresolved(watch())
                } else {
                    ExitObservation::exited()
                }
            }
        }
    }
}
impl<B: AsyncSinkBackend> Drop for AsyncSinkOperator<B> {
    fn drop(&mut self) {
        if self.actor_exit.is_none() {
            self.stop();
            self.retire_dormant();
        }
    }
}
impl<B: AsyncSinkBackend> StepOperator for AsyncSinkOperator<B> {
    fn initial_step(&self) -> NextStep {
        self.next.clone()
    }
    fn control_observable(&self) -> Option<Arc<Observable>> {
        Some(self.shared.observable.clone())
    }
    fn poll_control(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<ControlStep> {
        if let Some(error) = self.shared.failure() {
            return Err(self.fail(ctx, error));
        }
        Ok(ControlStep::Active)
    }
    fn try_push(&mut self, ctx: &mut StepContext<'_>, raw: RawInput) -> InputResult {
        let failure = self
            .failure
            .clone()
            .or_else(|| self.shared.failure())
            .or_else(|| {
                (self.finishing || self.exiting || matches!(self.next, NextStep::Finished))
                    .then(|| fault(AsyncSinkFailure::InputAfterFinish))
            })
            .or_else(|| {
                self.join
                    .is_none()
                    .then(|| fault(AsyncSinkFailure::NotActivated))
            })
            .or_else(|| ctx.checkpoint().err());
        if let Some(failure) = failure {
            return InputResult::Rejected {
                input: raw,
                failure,
            };
        }
        let target = AsyncTarget {
            sender: &self.sender,
            shared: &self.shared,
            capacity: self.capacity,
        };
        let result = self.receiver.try_accept(ctx, raw, &target);
        if matches!(result, InputResult::Accepted(_)) {
            self.next = NextStep::Runnable;
        }
        result
    }
    fn drive(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        if let Some(error) = self.failure.clone().or_else(|| self.shared.failure()) {
            return Err(self.fail(ctx, error));
        }
        ctx.checkpoint()?;
        if matches!(&self.next, NextStep::Wait(wait) if !wait.is_ready()) {
            return Ok(StepResult::new(None, self.next.clone(), 0));
        }
        let target = AsyncTarget {
            sender: &self.sender,
            shared: &self.shared,
            capacity: self.capacity,
        };
        match self.receiver.prepare(ctx, &target) {
            Ok(ReceiverPreparation::Ready(input)) => {
                let permit = match self.receiver.preparation_mut().reserved.take() {
                    Some(permit) => permit,
                    None => return Err(self.fail(ctx, fault(AsyncSinkFailure::MissingPermit))),
                };
                let chunk = input.into_uncovered_chunk()?;
                let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
                assert!(self.staged.is_none(), "one actual prepared sink handoff");
                let mut batch = SinkBatch::new(chunk);
                batch.credit = Some(ManuallyDrop::new(permit));
                self.staged = Some(ManuallyDrop::new(batch));
                let batch = self.staged.as_mut().unwrap();
                let permit = ManuallyDrop::into_inner(batch.credit.take().unwrap());
                let batch = ManuallyDrop::into_inner(self.staged.take().unwrap());
                self.shared.queued.fetch_add(1, Ordering::AcqRel);
                permit.send(batch);
                if self.finishing {
                    self.sender = None;
                    self.next = NextStep::Runnable;
                } else {
                    self.next = NextStep::NeedInput;
                }
                Ok(StepResult::new(None, self.next.clone(), rows))
            }
            Ok(ReceiverPreparation::Pending(next)) => {
                self.next = next;
                Ok(StepResult::new(None, self.next.clone(), 0))
            }
            Err(error) => Err(self.fail(ctx, error)),
            Ok(ReceiverPreparation::Empty) if self.finishing => {
                self.sender = None;
                self.wait_finish().map_err(|error| self.fail(ctx, error))
            }
            Ok(ReceiverPreparation::Empty) => Ok(StepResult::new(None, self.next.clone(), 0)),
        }
    }
    fn finish(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<()> {
        if let Some(error) = self.failure.clone().or_else(|| self.shared.failure()) {
            return Err(error);
        }
        if self.finishing || self.exiting || matches!(self.next, NextStep::Finished) {
            return Ok(());
        }
        if self.join.is_none() {
            return Err(fault(AsyncSinkFailure::NotActivated));
        }
        self.finishing = true;
        self.next = NextStep::Runnable;
        if !self.receiver.is_occupied() {
            self.sender = None;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Condvar, Mutex};
    use std::time::{Duration, Instant};

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::exec::chunk::ChunkSchema;
    use crate::exec::pipeline::driver::{DriverState, PipelineDriver};
    use crate::exec::pipeline::operator::BlockedReason;
    use crate::runtime::runtime_state::RuntimeState;
    use novarocks_types::SlotId;

    #[derive(Debug)]
    struct TestWriteFailure(usize);
    impl std::fmt::Display for TestWriteFailure {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "forced failure at chunk {}", self.0)
        }
    }
    impl std::error::Error for TestWriteFailure {}
    impl novarocks_failure_contract::FailureProjection for TestWriteFailure {
        fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
            use crate::runtime::fragment::ExecutionReason;
            use novarocks_failure_contract::{FailureClass, FailureFact};
            FailureFact::new(
                FailureClass::Dependency,
                ExecutionReason::TestWriteFailure.reason(),
            )
        }
    }
    impl<B: AsyncSinkBackend> AsyncSinkOperator<B> {
        fn consume_for_test(&mut self, state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
            let control = state.error_state();
            let mut work =
                crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, 64);
            let mut value_conversions =
                novarocks_value_arrays::decode::ValueConversionContext::default();
            let mut ctx = StepContext::new(&control, &mut work, &mut value_conversions);
            match self.try_push(&mut ctx, RawInput::for_test(chunk)) {
                InputResult::Accepted(result) => {
                    result?;
                }
                InputResult::Rejected { failure, .. } => return Err(failure),
                InputResult::Deferred { .. } => panic!("test offered input without capacity"),
            }
            let result = self.drive(&mut ctx)?;
            assert!(result.output.is_none());
            Ok(())
        }
        fn finish_for_test(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
            let control = state.error_state();
            let mut work =
                crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, 64);
            let mut value_conversions =
                novarocks_value_arrays::decode::ValueConversionContext::default();
            let mut ctx = StepContext::new(&control, &mut work, &mut value_conversions);
            StepOperator::finish(self, &mut ctx)?;
            self.drive(&mut ctx)?;
            Ok(())
        }
        fn pending_for_test(&mut self) -> Option<FinishWatch> {
            match self.observe_exit().progress {
                ExitProgress::Pending(watch) | ExitProgress::Unresolved(watch) => Some(watch),
                ExitProgress::Exited => None,
            }
        }
        fn can_offer(&self) -> bool {
            !self.finishing
                && !self.exiting
                && self.shared.failure().is_none()
                && self.shared.queued.load(Ordering::Acquire) < self.capacity.get()
        }
    }

    fn make_chunk(rows: usize) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Int32, true)]));
        let data: Vec<i32> = (0..rows as i32).collect();
        let array = Arc::new(Int32Array::from(data)) as _;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1)],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    /// Test backend: records rows seen; optional per-chunk gate and fail point.
    struct TestAsyncSink {
        rows: Arc<AtomicUsize>,
        chunks: Arc<AtomicUsize>,
        binds: Arc<AtomicUsize>,
        started: Arc<AtomicUsize>,
        // When set, write_chunk waits until released (simulates slow I/O / backpressure).
        gate: Arc<tokio::sync::Semaphore>,
        // When Some(n), the n-th (0-based) write_chunk fails.
        fail_at: Option<usize>,
        // Delay applied inside finish() to exercise pending_finish.
        finish_delay: Duration,
        finish_gate: Option<Arc<tokio::sync::Semaphore>>,
        finish_entered: Option<std::sync::mpsc::Sender<()>>,
    }

    impl TestAsyncSink {
        fn new(
            gate_permits: usize,
        ) -> (Self, Arc<AtomicUsize>, Arc<AtomicUsize>, Arc<AtomicUsize>) {
            let rows = Arc::new(AtomicUsize::new(0));
            let chunks = Arc::new(AtomicUsize::new(0));
            let binds = Arc::new(AtomicUsize::new(0));
            let sink = Self {
                rows: Arc::clone(&rows),
                chunks: Arc::clone(&chunks),
                binds: Arc::clone(&binds),
                started: Arc::new(AtomicUsize::new(0)),
                gate: Arc::new(tokio::sync::Semaphore::new(gate_permits)),
                fail_at: None,
                finish_delay: Duration::ZERO,
                finish_gate: None,
                finish_entered: None,
            };
            (sink, rows, chunks, binds)
        }
    }

    #[async_trait::async_trait]
    impl AsyncSinkBackend for TestAsyncSink {
        async fn abort(&mut self) -> ExecutionResult<()> {
            Ok(())
        }
        async fn close(&mut self) -> ExecutionResult<()> {
            Ok(())
        }

        type Output = usize;

        fn bind_runtime_state(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
            self.binds.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }

        async fn write_chunk(&mut self, chunk: &Chunk) -> ExecutionResult<()> {
            self.started.fetch_add(1, Ordering::AcqRel);
            let permit = self.gate.acquire().await.expect("gate");
            permit.forget();
            let idx = self.chunks.fetch_add(1, Ordering::AcqRel);
            if self.fail_at == Some(idx) {
                return Err(ExecutionFailure::from_domain(TestWriteFailure(idx)));
            }
            self.rows.fetch_add(chunk.len(), Ordering::AcqRel);
            Ok(())
        }

        async fn finish(&mut self) -> ExecutionResult<usize> {
            if let Some(entered) = self.finish_entered.take() {
                entered.send(()).unwrap();
            }
            if let Some(gate) = &self.finish_gate {
                gate.acquire().await.expect("finish gate").forget();
            }
            if !self.finish_delay.is_zero() {
                tokio::time::sleep(self.finish_delay).await;
            }
            Ok(self.rows.load(Ordering::Acquire))
        }
    }

    struct ExitGate {
        entered: AtomicBool,
        released: Mutex<bool>,
        cv: Condvar,
    }

    impl ExitGate {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                entered: AtomicBool::new(false),
                released: Mutex::new(false),
                cv: Condvar::new(),
            })
        }

        fn release(&self) {
            *self.released.lock().expect("exit gate lock") = true;
            self.cv.notify_all();
        }
    }

    struct ExitGatedSink {
        gate: Arc<ExitGate>,
        fail_write: bool,
    }

    impl Drop for ExitGatedSink {
        fn drop(&mut self) {
            self.gate.entered.store(true, Ordering::Release);
            let mut released = self.gate.released.lock().expect("exit gate lock");
            while !*released {
                released = self.gate.cv.wait(released).expect("exit gate wait");
            }
        }
    }

    #[async_trait::async_trait]
    impl AsyncSinkBackend for ExitGatedSink {
        async fn abort(&mut self) -> ExecutionResult<()> {
            Ok(())
        }
        async fn close(&mut self) -> ExecutionResult<()> {
            Ok(())
        }

        type Output = ();

        async fn write_chunk(&mut self, _chunk: &Chunk) -> ExecutionResult<()> {
            if self.fail_write {
                Err(ExecutionFailure::from_domain(TestWriteFailure(0)))
            } else {
                Ok(())
            }
        }

        async fn finish(&mut self) -> ExecutionResult<()> {
            Ok(())
        }
    }

    struct PanickingSink;

    #[async_trait::async_trait]
    impl AsyncSinkBackend for PanickingSink {
        async fn abort(&mut self) -> ExecutionResult<()> {
            Ok(())
        }
        async fn close(&mut self) -> ExecutionResult<()> {
            Ok(())
        }

        type Output = ();

        async fn write_chunk(&mut self, _chunk: &Chunk) -> ExecutionResult<()> {
            panic!("injected sink panic")
        }

        async fn finish(&mut self) -> ExecutionResult<()> {
            Ok(())
        }
    }

    #[test]
    fn full_refuses_the_same_raw_before_transfer_and_accepts_it_once_after_write_exit() {
        let state = test_runtime_state();
        let (backend, rows, chunks, _) = TestAsyncSink::new(0);
        let gate = backend.gate.clone();
        let started = backend.started.clone();
        let mut op = AsyncSinkOperator::try_new("credit_sink", backend, 1).unwrap();
        let receiver = MemTracker::new_root("async receiver");
        op.set_mem_tracker(receiver.clone());
        op.bind_runtime_state(&state).unwrap();
        op.activate(&state).unwrap();
        op.consume_for_test(&state, make_chunk(1)).unwrap();
        assert!(poll_until(
            || started.load(Ordering::Acquire) == 1,
            Duration::from_secs(5)
        ));
        let edge = MemTracker::new_root("deferred async raw");
        let mut chunk = make_chunk(2);
        chunk.transfer_to(&edge);
        let original = chunk.columns()[0].clone();
        let edge_bytes = edge.current();
        let receiver_bytes = receiver.current();
        let control = state.error_state();
        let mut work =
            crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, 64);
        let mut value_conversions =
            novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = StepContext::new(&control, &mut work, &mut value_conversions);
        let mut raw = RawInput::for_test(chunk);
        let mut previous = None;
        for _ in 0..2 {
            raw = match op.try_push(&mut ctx, raw) {
                InputResult::Deferred {
                    input,
                    next: AdmissionNext::Wait(wait),
                } => {
                    assert!(Arc::ptr_eq(
                        &original,
                        &input.original_for_test().columns()[0]
                    ));
                    assert!(Arc::ptr_eq(&wait.observable, &op.shared.observable));
                    if let Some(generation) = previous {
                        assert_eq!(wait.observed_generation, generation);
                    }
                    previous = Some(wait.observed_generation);
                    input
                }
                _ => panic!("full must leave the same input unaccepted"),
            };
            assert_eq!(edge.current(), edge_bytes);
            assert_eq!(receiver.current(), receiver_bytes);
        }
        gate.add_permits(1);
        assert!(poll_until(
            || op.shared.queued.load(Ordering::Acquire) == 0,
            Duration::from_secs(5)
        ));
        assert!(matches!(
            op.try_push(&mut ctx, raw),
            InputResult::Accepted(Ok(_))
        ));
        assert_eq!(edge.current(), 0);
        assert!(receiver.current() > 0);
        let step = op.drive(&mut ctx).unwrap();
        assert_eq!(step.committed_rows, 2);
        assert!(step.output.is_none());
        StepOperator::finish(&mut op, &mut ctx).unwrap();
        gate.add_permits(1);
        assert!(poll_until(
            || op.pending_for_test().is_none(),
            Duration::from_secs(5)
        ));
        assert_eq!(rows.load(Ordering::Acquire), 3);
        assert_eq!(chunks.load(Ordering::Acquire), 2);
        assert_eq!(op.take_output(), Some(3));
        drop(original);
        assert_eq!(receiver.current(), 0);
        assert!(state.error().is_none());
    }

    #[test]
    fn a_write_failure_is_reported_before_input_drop_and_cleanup_cannot_hide_live_exit() {
        struct ObserveInputDrop {
            control: Arc<RuntimeErrorState>,
            observed: Arc<Mutex<Option<ExecutionFailure>>>,
        }
        impl novarocks_type_contract::ValueTypeStorageHold for ObserveInputDrop {}
        impl Drop for ObserveInputDrop {
            fn drop(&mut self) {
                *self.observed.lock().unwrap() = self.control.error();
            }
        }
        struct FailingCleanup {
            original: ExecutionFailure,
            aborted: Arc<AtomicBool>,
            closed: Arc<AtomicBool>,
            gate: Arc<tokio::sync::Semaphore>,
        }
        #[async_trait::async_trait]
        impl AsyncSinkBackend for FailingCleanup {
            type Output = ();
            async fn write_chunk(&mut self, _: &Chunk) -> ExecutionResult<()> {
                Err(self.original.clone())
            }
            async fn finish(&mut self) -> ExecutionResult<()> {
                panic!("failed write must not finalize")
            }
            async fn abort(&mut self) -> ExecutionResult<()> {
                self.aborted.store(true, Ordering::Release);
                Err(ExecutionFailure::from_domain(TestWriteFailure(8)))
            }
            async fn close(&mut self) -> ExecutionResult<()> {
                self.closed.store(true, Ordering::Release);
                self.gate.acquire().await.unwrap().forget();
                Err(ExecutionFailure::from_domain(TestWriteFailure(9)))
            }
        }
        let state = test_runtime_state();
        let original = ExecutionFailure::from_domain(TestWriteFailure(7));
        let aborted = Arc::new(AtomicBool::new(false));
        let closed = Arc::new(AtomicBool::new(false));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let observed = Arc::new(Mutex::new(None));
        let marker =
            novarocks_type_contract::ValueTypeStorageLifetime::held(Arc::new(ObserveInputDrop {
                control: state.error_state(),
                observed: observed.clone(),
            }));
        let chunk = make_chunk(1).retaining_array_type_storage_observed::<novarocks_type_contract::owned_resources::layout::LayoutResourceError>(&marker, |_| Ok(())).unwrap();
        drop(marker);
        let backend = FailingCleanup {
            original: original.clone(),
            aborted: aborted.clone(),
            closed: closed.clone(),
            gate: gate.clone(),
        };
        let mut op = AsyncSinkOperator::try_new("cleanup_failure", backend, 1).unwrap();
        op.activate(&state).unwrap();
        op.consume_for_test(&state, chunk).unwrap();
        assert!(poll_until(
            || closed.load(Ordering::Acquire),
            Duration::from_secs(5)
        ));
        assert!(aborted.load(Ordering::Acquire));
        let before_drop = observed
            .lock()
            .unwrap()
            .clone()
            .expect("failure preceded input destruction");
        assert!(before_drop.same_occurrence(&original));
        assert!(state.error().unwrap().same_occurrence(&original));
        assert!(
            op.pending_for_test().is_some(),
            "close is still the original live operation"
        );
        assert!(!op.shared.finished.load(Ordering::Acquire));
        assert!(op.take_output().is_none());
        gate.add_permits(1);
        assert!(poll_until(
            || op.pending_for_test().is_none(),
            Duration::from_secs(5)
        ));
        assert!(op.join.is_none());
        let Some(Err(error)) = &op.actor_exit else {
            panic!("failed actor exit");
        };
        assert!(error.same_occurrence(&original));
        assert!(state.error().unwrap().same_occurrence(&original));
    }

    #[test]
    fn invalid_queue_capacity_is_refused_without_starting_a_backend() {
        for capacity in [0, tokio::sync::Semaphore::MAX_PERMITS + 1] {
            let (backend, _, _, binds) = TestAsyncSink::new(0);
            assert!(AsyncSinkOperator::try_new("invalid_capacity", backend, capacity).is_err());
            assert_eq!(binds.load(Ordering::Acquire), 0);
        }
    }

    #[test]
    fn backend_panic_latches_failure_and_releases_pending_finish() {
        let state = test_runtime_state();
        let mut op =
            AsyncSinkOperator::try_new("panic_sink", PanickingSink, 1).expect("bounded sink");
        op.activate(&state).expect("activate sink");
        op.consume_for_test(&state, make_chunk(1))
            .expect("push chunk");

        assert!(
            poll_until(
                || state.error().is_some() && op.pending_for_test().is_none(),
                Duration::from_secs(5)
            ),
            "panic must become a terminal error after the actor exits"
        );
        assert!(
            state
                .error()
                .expect("panic error")
                .detail()
                .contains("async sink write panicked: injected sink panic")
        );
        assert!(op.shared.finished.load(Ordering::Acquire));
        assert!(op.join.is_none(), "actual join result was consumed");
        assert!(matches!(op.actor_exit, Some(Err(_))));
    }

    #[test]
    fn pending_finish_waits_for_backend_drop_after_normal_failure_and_cancel() {
        for path in ["normal", "failure", "cancel"] {
            let state = test_runtime_state();
            let gate = ExitGate::new();
            let backend = ExitGatedSink {
                gate: Arc::clone(&gate),
                fail_write: path == "failure",
            };
            let mut op = AsyncSinkOperator::try_new(path, backend, 1).expect("bounded sink");
            op.activate(&state).expect("activate sink");
            match path {
                "normal" => op.finish_for_test(&state).expect("finish sink"),
                "failure" => op
                    .consume_for_test(&state, make_chunk(1))
                    .expect("push chunk"),
                "cancel" => op.cancel(),
                _ => unreachable!(),
            }
            let entered = poll_until(
                || gate.entered.load(Ordering::Acquire),
                Duration::from_secs(5),
            );
            let finished = op.shared.finished.load(Ordering::Acquire);
            let pending = op.pending_for_test();
            let handle_live = !op
                .join
                .as_ref()
                .expect("actor handle retained")
                .is_finished();
            gate.release();
            assert!(entered, "{path}: backend destructor did not start");
            assert!(finished, "{path}: semantic terminal state must be visible");
            assert!(
                pending.is_some() && handle_live,
                "{path}: live actor must hold PendingFinish"
            );
            assert!(
                poll_until(|| op.pending_for_test().is_none(), Duration::from_secs(5)),
                "{path}: actor did not exit after release"
            );
        }
    }

    fn poll_until<F: FnMut() -> bool>(mut pred: F, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if pred() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        pred()
    }

    fn test_runtime_state() -> RuntimeState {
        test_runtime_state_with_control(Arc::new(RuntimeErrorState::default()))
    }
    fn test_runtime_state_with_control(control: Arc<RuntimeErrorState>) -> RuntimeState {
        let runtime = std::sync::Arc::new(
            crate::runtime::ExecutionRuntime::new(
                crate::runtime::ExecutionRuntimeConfig {
                    driver_threads: 1,
                    exchange_wait_ms: 120_000,
                    exchange_io_threads: 1,
                    exchange_io_max_inflight_bytes: 1024,
                    exchange_max_transmit_batched_bytes: 1024,
                    operator_buffer_chunks: 1,
                    local_exchange_buffer_mem_limit_per_driver: 1024,
                    local_exchange_max_buffered_rows: 1024,
                    runtime_filter_scan_wait_time_ms_override: None,
                    runtime_filter_wait_timeout_ms_override: None,
                    sink_io_worker_threads: 1,
                    sink_io_max_blocking_threads: 1,
                },
                crate::runtime::execution_runtime::test_execution_function_set(),
                crate::runtime::execution_runtime::test_memory_authority(),
            )
            .expect("test execution runtime"),
        );
        RuntimeState::new(None, None, None, None, None, None, Some(runtime), control)
    }

    #[test]
    fn drains_all_chunks_with_backpressure() {
        let state = test_runtime_state();
        // gate starts open (large permit count) so writes flow.
        let (backend, rows, chunks, _binds) = TestAsyncSink::new(1_000);
        let mut op =
            AsyncSinkOperator::try_new("test_async_sink", backend, 2).expect("bounded sink");
        op.bind_runtime_state(&state).expect("bind");
        op.activate(&state).expect("activate");

        // Push 5 chunks of 3 rows each, respecting need_input backpressure.
        let mut pushed = 0;
        let deadline = Instant::now() + Duration::from_secs(5);
        while pushed < 5 {
            if op.can_offer() {
                op.consume_for_test(&state, make_chunk(3)).expect("push");
                pushed += 1;
            } else {
                assert!(Instant::now() < deadline, "stuck on backpressure");
                std::thread::sleep(Duration::from_millis(2));
            }
        }
        op.finish_for_test(&state).expect("finish");

        assert!(
            poll_until(
                || op.shared.finished.load(Ordering::Acquire) && op.pending_for_test().is_none(),
                Duration::from_secs(5)
            ),
            "sink did not finish"
        );
        assert_eq!(chunks.load(Ordering::Acquire), 5);
        assert_eq!(rows.load(Ordering::Acquire), 15);
        assert_eq!(op.take_output(), Some(15));
    }

    #[test]
    fn need_input_goes_false_when_queue_full_then_recovers() {
        let state = test_runtime_state();
        // gate starts closed (0 permits): background blocks on the first write.
        let (backend, _rows, _chunks, _binds) = TestAsyncSink::new(0);
        let gate = Arc::clone(&backend.gate);
        let mut op = AsyncSinkOperator::try_new("bp_sink", backend, 2).expect("bounded sink");
        op.bind_runtime_state(&state).expect("bind");
        op.activate(&state).expect("activate");

        // Fill the queue: capacity=2, plus 1 in-flight pulled by the bg task.
        // Push until need_input() reports full.
        let mut pushed = 0;
        while op.can_offer() && pushed < 8 {
            op.consume_for_test(&state, make_chunk(1)).expect("push");
            pushed += 1;
        }
        assert!(!op.can_offer(), "sink should report backpressure when full");

        // Release the gate; background drains; need_input must recover.
        gate.add_permits(100);
        assert!(
            poll_until(|| op.can_offer(), Duration::from_secs(5)),
            "need_input did not recover after drain"
        );

        op.finish_for_test(&state).expect("finish");
        assert!(
            poll_until(
                || op.shared.finished.load(Ordering::Acquire) && op.pending_for_test().is_none(),
                Duration::from_secs(5)
            ),
            "sink did not finish"
        );
    }

    #[test]
    fn pending_finish_true_while_finishing_then_clears() {
        let state = test_runtime_state();
        let (mut backend, _rows, _chunks, _binds) = TestAsyncSink::new(1_000);
        backend.finish_delay = Duration::from_millis(200);
        let mut op = AsyncSinkOperator::try_new("finish_sink", backend, 4).expect("bounded sink");
        op.bind_runtime_state(&state).expect("bind");
        op.activate(&state).expect("activate");

        op.consume_for_test(&state, make_chunk(2)).expect("push");
        op.finish_for_test(&state).expect("finish");

        // While finish() sleeps, pending_finish must be true and is_finished false.
        assert!(
            poll_until(|| op.pending_for_test().is_some(), Duration::from_secs(1)),
            "expected pending_finish during async finish"
        );
        assert!(
            op.take_output().is_none(),
            "output must not be ready mid-finish"
        );

        // After finish completes, pending_finish clears and is_finished is true.
        assert!(
            poll_until(
                || op.shared.finished.load(Ordering::Acquire) && op.pending_for_test().is_none(),
                Duration::from_secs(5)
            ),
            "sink did not finish"
        );
        assert!(
            op.pending_for_test().is_none(),
            "pending_finish must clear after finish"
        );
        assert_eq!(op.take_output(), Some(2));
    }

    #[test]
    fn background_failure_sets_query_error_and_does_not_hang() {
        let state = test_runtime_state();
        let (mut backend, _rows, _chunks, _binds) = TestAsyncSink::new(1_000);
        backend.fail_at = Some(1); // second chunk fails
        let mut op = AsyncSinkOperator::try_new("err_sink", backend, 4).expect("bounded sink");
        op.bind_runtime_state(&state).expect("bind");
        op.activate(&state).expect("activate");

        // Push a few chunks; one of them triggers the failure in the bg task.
        for _ in 0..3 {
            if op.can_offer() {
                let _ = op.consume_for_test(&state, make_chunk(1));
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
            poll_until(
                || op.shared.finished.load(Ordering::Acquire),
                Duration::from_secs(5)
            ),
            "errored sink did not converge to finished"
        );
        assert!(!op.can_offer(), "errored sink must stop accepting input");
        assert!(
            state.error().unwrap().detail().contains("forced failure"),
            "unexpected error text"
        );
    }

    #[test]
    fn cancel_retains_the_active_write_until_it_and_cleanup_actually_exit() {
        let state = test_runtime_state();
        let (backend, _, chunks, _) = TestAsyncSink::new(0);
        let gate = backend.gate.clone();
        let started = backend.started.clone();
        let mut op = AsyncSinkOperator::try_new("cancel_sink", backend, 4).expect("bounded sink");
        op.bind_runtime_state(&state).unwrap();
        op.activate(&state).unwrap();
        let tracker = MemTracker::new_root("active async sink write");
        let mut chunk = make_chunk(1);
        chunk.transfer_to(&tracker);
        let weak = Arc::downgrade(&chunk.columns()[0]);
        op.consume_for_test(&state, chunk).unwrap();
        assert!(poll_until(
            || started.load(Ordering::Acquire) == 1,
            Duration::from_secs(5)
        ));
        let retained = tracker.current();
        assert!(retained > 0);
        let begin = Instant::now();
        op.cancel();
        assert!(begin.elapsed() < Duration::from_secs(1));
        assert!(op.pending_for_test().is_some());
        assert!(!op.shared.finished.load(Ordering::Acquire));
        assert!(weak.upgrade().is_some());
        assert_eq!(tracker.current(), retained);
        assert_eq!(chunks.load(Ordering::Acquire), 0);
        assert!(op.take_output().is_none());
        gate.add_permits(1);
        assert!(poll_until(
            || op.pending_for_test().is_none(),
            Duration::from_secs(5)
        ));
        assert_eq!(started.load(Ordering::Acquire), 1);
        assert_eq!(
            chunks.load(Ordering::Acquire),
            1,
            "the original operation settled before cleanup"
        );
        assert!(op.join.is_none());
        assert!(weak.upgrade().is_none());
        assert_eq!(tracker.current(), 0);
        assert!(
            state.error().is_none(),
            "observed stop does not create an originating failure"
        );
        assert!(op.take_output().is_none());
    }

    #[test]
    fn backend_binding_and_drain_start_wait_for_activation() {
        let state = test_runtime_state();
        let (backend, _rows, _chunks, binds) = TestAsyncSink::new(1_000);
        let mut op = AsyncSinkOperator::try_new("bind_sink", backend, 2).expect("bounded sink");

        op.bind_runtime_state(&state).expect("bind");
        assert_eq!(binds.load(Ordering::Acquire), 0);
        op.activate(&state).expect("activate");

        assert_eq!(
            binds.load(Ordering::Acquire),
            1,
            "backend must observe runtime state during activation"
        );
    }

    /// Finite native source: the last output and data Finished are one step.
    struct TestSource {
        remaining: usize,
    }
    impl TestSource {
        fn source(n: usize) -> super::super::shape::Source<Self> {
            super::super::shape::Source::new(Self { remaining: n })
        }
    }
    impl OperatorLifecycle for TestSource {
        fn name(&self) -> &str {
            "test_source"
        }
    }
    impl super::super::shape::SourceKernel for TestSource {
        fn poll_output(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            if self.remaining == 0 {
                return Ok(StepResult::new(None, NextStep::Finished, 0));
            }
            self.remaining -= 1;
            Ok(StepResult::new(
                Some(make_chunk(1)),
                if self.remaining == 0 {
                    NextStep::Finished
                } else {
                    NextStep::Runnable
                },
                0,
            ))
        }
    }

    #[test]
    fn driver_parks_on_capacity_and_mandatory_finish_then_observes_actor_exit() {
        let runtime_state = Arc::new(test_runtime_state());

        // gate starts closed so the sink saturates and the driver must block.
        let (mut backend, rows, chunks, _binds) = TestAsyncSink::new(0);
        let finish_gate = Arc::new(tokio::sync::Semaphore::new(0));
        let (entered, finish_started) = std::sync::mpsc::channel();
        backend.finish_gate = Some(finish_gate.clone());
        backend.finish_entered = Some(entered);
        let gate = Arc::clone(&backend.gate);
        let mut sink = AsyncSinkOperator::try_new("driver_sink", backend, 2).expect("bounded sink");
        // A directly-constructed PipelineDriver does not bind runtime state.
        // Activate with the same state so its drain task can serve the driver.
        sink.bind_runtime_state(&runtime_state).expect("bind sink");
        sink.activate(&runtime_state).expect("activate sink");

        let driver_state = Arc::clone(&runtime_state);
        let mut driver = PipelineDriver::new(
            1,
            vec![Box::new(TestSource::source(6)), Box::new(sink)],
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

        gate.add_permits(1_000);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let state = driver.process(Duration::from_millis(10));
            assert!(
                !matches!(state, DriverState::Finished | DriverState::Failed(_)),
                "mandatory finish has not settled: {state:?}"
            );
            if finish_started.try_recv().is_ok() {
                break;
            }
            assert!(Instant::now() < deadline, "backend finish was not entered");
            std::thread::yield_now();
        }
        assert_eq!(rows.load(Ordering::Acquire), 6);
        assert_eq!(chunks.load(Ordering::Acquire), 6);
        assert_eq!(
            driver.process(Duration::from_millis(10)),
            DriverState::Blocked(BlockedReason::OutputFull)
        );
        finish_gate.add_permits(1);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match driver.process(Duration::from_millis(10)) {
                DriverState::Finished => break,
                DriverState::Failed(error) => panic!("driver failed: {error}"),
                _ => {}
            }
            assert!(
                Instant::now() < deadline,
                "driver did not observe actor exit"
            );
            std::thread::yield_now();
        }
        assert_eq!(rows.load(Ordering::Acquire), 6);
        assert_eq!(chunks.load(Ordering::Acquire), 6);
    }
    // The existing Task author owns acceptance. This adapter records only the
    // real event calls and exposes the same process Health to the oracle.
    struct SinkPort {
        task: Arc<crate::runtime::task_control::test_support::Owner>,
        health: Arc<crate::runtime::execution_health::ExecutionHealth>,
        required: AtomicUsize,
        cleanup: Mutex<Vec<ExecutionFailure>>,
    }
    impl novarocks_execution_contract::TaskConclusionPort<ExecutionFailure> for SinkPort {
        fn identity(&self) -> novarocks_execution_contract::TaskIdentity {
            self.task.identity()
        }
        fn accepted(
            &self,
        ) -> Option<Arc<novarocks_execution_contract::AcceptedTaskConclusion<ExecutionFailure>>>
        {
            self.task.accepted()
        }
        fn submit(
            &self,
            proposal: novarocks_execution_contract::TaskConclusionProposal<ExecutionFailure>,
        ) {
            self.required.fetch_add(1, Ordering::AcqRel);
            self.task.submit(proposal)
        }
    }
    impl crate::runtime::task_control::TaskExecutionPort for SinkPort {
        fn execution_health(&self) -> Arc<crate::runtime::execution_health::ExecutionHealth> {
            self.health.clone()
        }
        fn report_cleanup(
            &self,
            failure: &ExecutionFailure,
            _: novarocks_execution_contract::TaskFailurePhase,
        ) {
            self.cleanup.lock().unwrap().push(failure.clone());
        }
        fn report_unresolved_exit(
            &self,
            fact: novarocks_failure_contract::FailureFact,
            phase: novarocks_execution_contract::TaskFailurePhase,
        ) {
            use novarocks_execution_contract::TaskConclusionPort;
            self.health.report_unresolved(
                crate::runtime::execution_health::ExecutionExitOrigin::Task {
                    identity: self.identity(),
                    phase,
                },
                fact,
            );
        }
        fn is_stopped(&self) -> bool {
            self.task.is_stopped()
        }
        fn stop_observable(&self) -> Arc<Observable> {
            self.task.stop_observable()
        }
        fn wait_until_stopped(&self, duration: Duration) -> bool {
            self.task.wait_until_stopped(duration)
        }
    }
    fn task_binding() -> (Arc<SinkPort>, RuntimeState) {
        use novarocks_execution_contract::{TaskConclusionPort, TaskFailurePhase};
        let port = Arc::new(SinkPort {
            task: crate::runtime::task_control::test_support::Owner::new(),
            health: Arc::new(crate::runtime::execution_health::ExecutionHealth::default()),
            required: AtomicUsize::new(0),
            cleanup: Mutex::new(Vec::new()),
        });
        let control = Arc::new(RuntimeErrorState::for_task(Arc::new(
            crate::runtime::task_control::TaskExecutionControl::new(
                port.identity(),
                port.clone(),
                TaskFailurePhase::Execution,
            )
            .unwrap(),
        )));
        (port, test_runtime_state_with_control(control))
    }
    fn pure_observation<B: AsyncSinkBackend>(op: &mut AsyncSinkOperator<B>) -> ExitProgress {
        let event = op.observe_exit();
        assert!(
            event.failure.is_none() && event.unresolved_exit.is_none(),
            "the original runtime already consumed these events"
        );
        event.progress
    }
    struct RoleSink {
        fail_finish: Option<ExecutionFailure>,
        abort_error: Option<ExecutionFailure>,
        close_error: Option<ExecutionFailure>,
        finish_entered: Option<std::sync::mpsc::Sender<()>>,
        finish_gate: Arc<tokio::sync::Semaphore>,
        drops: Arc<AtomicUsize>,
    }
    impl Drop for RoleSink {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::AcqRel);
        }
    }
    #[async_trait::async_trait]
    impl AsyncSinkBackend for RoleSink {
        type Output = usize;
        async fn write_chunk(&mut self, _: &Chunk) -> ExecutionResult<()> {
            Ok(())
        }
        async fn finish(&mut self) -> ExecutionResult<usize> {
            if let Some(entered) = self.finish_entered.take() {
                entered.send(()).unwrap();
            }
            self.finish_gate.acquire().await.unwrap().forget();
            match self.fail_finish.take() {
                Some(error) => Err(error),
                None => Ok(37),
            }
        }
        async fn abort(&mut self) -> ExecutionResult<()> {
            self.abort_error.take().map_or(Ok(()), Err)
        }
        async fn close(&mut self) -> ExecutionResult<()> {
            self.close_error.take().map_or(Ok(()), Err)
        }
    }
    #[test]
    fn cleanup_abort_close_never_refine_a_peer_or_reappear_during_exit_observation() {
        use novarocks_execution_contract::{
            AbortCause, TaskConclusionPort, TaskConclusionProposal,
        };
        let (port, state) = task_binding();
        port.task
            .submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
        let first = port.task.accepted().unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let abort = ExecutionFailure::from_domain(TestWriteFailure(81));
        let close = ExecutionFailure::from_domain(TestWriteFailure(82));
        let mut op = AsyncSinkOperator::try_new(
            "cleanup roles",
            RoleSink {
                fail_finish: None,
                abort_error: Some(abort.clone()),
                close_error: Some(close.clone()),
                finish_entered: None,
                finish_gate: Arc::new(tokio::sync::Semaphore::new(1)),
                drops: drops.clone(),
            },
            1,
        )
        .unwrap();
        op.activate(&state).unwrap();
        assert!(Arc::ptr_eq(
            op.error_state.as_ref().unwrap(),
            &state.error_state()
        ));
        op.cancel();
        assert!(poll_until(
            || matches!(pure_observation(&mut op), ExitProgress::Exited),
            Duration::from_secs(5)
        ));
        assert_eq!(drops.load(Ordering::Acquire), 1);
        let observed = port.cleanup.lock().unwrap();
        assert_eq!(observed.len(), 2);
        assert!(observed[0].same_occurrence(&abort) && observed[1].same_occurrence(&close));
        drop(observed);
        for _ in 0..4 {
            assert!(matches!(pure_observation(&mut op), ExitProgress::Exited));
        }
        assert_eq!(port.required.load(Ordering::Acquire), 0);
        assert!(Arc::ptr_eq(&port.task.accepted().unwrap(), &first));
        assert!(port.health.failure().is_none());
    }
    #[test]
    fn finish_is_required_even_when_peer_abort_arrives_while_that_exact_operation_is_pending() {
        use novarocks_execution_contract::{
            AbortCause, TaskConclusionPort, TaskConclusionProposal,
        };
        let (port, state) = task_binding();
        let original = ExecutionFailure::from_domain(TestWriteFailure(83));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let (entered, receiver) = std::sync::mpsc::channel();
        let drops = Arc::new(AtomicUsize::new(0));
        let mut op = AsyncSinkOperator::try_new(
            "required finish",
            RoleSink {
                fail_finish: Some(original.clone()),
                abort_error: None,
                close_error: None,
                finish_entered: Some(entered),
                finish_gate: gate.clone(),
                drops: drops.clone(),
            },
            1,
        )
        .unwrap();
        op.activate(&state).unwrap();
        op.finish_for_test(&state).unwrap();
        receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        port.task
            .submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
        let first = port.task.accepted().unwrap();
        assert!(matches!(
            pure_observation(&mut op),
            ExitProgress::Pending(_)
        ));
        gate.add_permits(1);
        assert!(poll_until(
            || matches!(pure_observation(&mut op), ExitProgress::Exited),
            Duration::from_secs(5)
        ));
        let accepted = port.task.accepted().unwrap();
        assert!(!Arc::ptr_eq(&accepted, &first));
        assert!(
            accepted
                .originating_failure()
                .unwrap()
                .same_occurrence(&original)
        );
        assert_eq!(port.required.load(Ordering::Acquire), 1);
        assert_eq!(drops.load(Ordering::Acquire), 1);
        assert!(port.cleanup.lock().unwrap().is_empty());
        for _ in 0..4 {
            assert!(matches!(pure_observation(&mut op), ExitProgress::Exited));
        }
        assert_eq!(port.required.load(Ordering::Acquire), 1);
    }
    #[test]
    fn close_refusal_is_cleanup_after_successful_required_finish_and_keeps_output() {
        let (port, state) = task_binding();
        let original = ExecutionFailure::from_domain(TestWriteFailure(84));
        let drops = Arc::new(AtomicUsize::new(0));
        let mut op = AsyncSinkOperator::try_new(
            "close after finish",
            RoleSink {
                fail_finish: None,
                abort_error: None,
                close_error: Some(original.clone()),
                finish_entered: None,
                finish_gate: Arc::new(tokio::sync::Semaphore::new(1)),
                drops: drops.clone(),
            },
            1,
        )
        .unwrap();
        op.activate(&state).unwrap();
        op.finish_for_test(&state).unwrap();
        assert!(poll_until(
            || matches!(pure_observation(&mut op), ExitProgress::Exited),
            Duration::from_secs(5)
        ));
        assert_eq!(op.take_output(), Some(37));
        assert!(state.error().is_none());
        assert_eq!(port.required.load(Ordering::Acquire), 0);
        assert_eq!(drops.load(Ordering::Acquire), 1);
        let errors = port.cleanup.lock().unwrap();
        assert_eq!(errors.len(), 1);
        assert!(errors[0].same_occurrence(&original));
    }

    struct SinkPrimary {
        chunk: Option<ManuallyDrop<Chunk>>,
        data: ManuallyDrop<Arc<Vec<u8>>>,
        primary: Arc<AtomicUsize>,
        secondary: Arc<AtomicUsize>,
    }
    struct SinkSecondary {
        _chunk: Option<ManuallyDrop<Chunk>>,
        _data: ManuallyDrop<Arc<Vec<u8>>>,
        drops: Arc<AtomicUsize>,
    }
    impl Drop for SinkSecondary {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::AcqRel);
            panic!("secondary sink payload must not be implicitly destroyed");
        }
    }
    impl Drop for SinkPrimary {
        fn drop(&mut self) {
            self.primary.fetch_add(1, Ordering::AcqRel);
            let data = unsafe { ManuallyDrop::take(&mut self.data) };
            std::panic::panic_any(SinkSecondary {
                _chunk: self.chunk.take(),
                _data: ManuallyDrop::new(data),
                drops: self.secondary.clone(),
            });
        }
    }
    struct OpaqueSink {
        data: Option<Arc<Vec<u8>>>,
        primary: Arc<AtomicUsize>,
        secondary: Arc<AtomicUsize>,
        aborts: Arc<AtomicUsize>,
        closes: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }
    impl Drop for OpaqueSink {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::AcqRel);
        }
    }
    #[async_trait::async_trait]
    impl AsyncSinkBackend for OpaqueSink {
        type Output = ();
        async fn write_chunk(&mut self, chunk: &Chunk) -> ExecutionResult<()> {
            std::panic::panic_any(SinkPrimary {
                chunk: Some(ManuallyDrop::new(chunk.clone())),
                data: ManuallyDrop::new(self.data.take().unwrap()),
                primary: self.primary.clone(),
                secondary: self.secondary.clone(),
            });
        }
        async fn finish(&mut self) -> ExecutionResult<()> {
            panic!("no finish after required write panic")
        }
        async fn abort(&mut self) -> ExecutionResult<()> {
            self.aborts.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
        async fn close(&mut self) -> ExecutionResult<()> {
            self.closes.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
    }
    #[test]
    fn unknown_opaque_custody_keeps_real_chunk_and_credit_but_returned_borrow_still_runs_abort_close()
     {
        let (port, state) = task_binding();
        let data = Arc::new(vec![91u8; 32 * 1024]);
        let weak = Arc::downgrade(&data);
        let primary = Arc::new(AtomicUsize::new(0));
        let secondary = Arc::new(AtomicUsize::new(0));
        let aborts = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let chunk = make_chunk(3);
        let array = Arc::downgrade(&chunk.columns()[0]);
        let mut op = AsyncSinkOperator::try_new(
            "opaque write",
            OpaqueSink {
                data: Some(data),
                primary: primary.clone(),
                secondary: secondary.clone(),
                aborts: aborts.clone(),
                closes: closes.clone(),
                drops: drops.clone(),
            },
            1,
        )
        .unwrap();
        op.activate(&state).unwrap();
        op.consume_for_test(&state, chunk).unwrap();
        assert!(poll_until(
            || aborts.load(Ordering::Acquire) == 1
                && closes.load(Ordering::Acquire) == 1
                && op.shared.parked.load(Ordering::Acquire),
            Duration::from_secs(5)
        ));
        assert!(poll_until(
            || {
                let _ = pure_observation(&mut op);
                op.join.is_none()
            },
            Duration::from_secs(5)
        ));
        for _ in 0..4 {
            assert!(matches!(
                pure_observation(&mut op),
                ExitProgress::Unresolved(_)
            ));
        }
        assert_eq!(primary.load(Ordering::Acquire), 1);
        assert_eq!(secondary.load(Ordering::Acquire), 0);
        assert_eq!(port.required.load(Ordering::Acquire), 1);
        assert!(port.health.failure().is_some());
        assert_eq!(op.shared.queued.load(Ordering::Acquire), 1);
        assert_eq!(drops.load(Ordering::Acquire), 0);
        assert_eq!(
            array
                .upgrade()
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[0, 1, 2]
        );
        let shared = Arc::downgrade(&op.shared);
        drop(op);
        drop(state);
        assert!(
            shared
                .upgrade()
                .unwrap()
                .unknown_actor
                .lock()
                .unwrap()
                .is_some()
        );
        assert_eq!(weak.upgrade().unwrap().len(), 32 * 1024);
        assert!(array.upgrade().is_some());
    }

    struct WriteFuture<'a> {
        chunk: &'a Chunk,
        original: ExecutionFailure,
        drops: Arc<AtomicUsize>,
    }
    impl std::future::Future for WriteFuture<'_> {
        type Output = ExecutionResult<()>;
        fn poll(self: std::pin::Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(Err(self.original.clone()))
        }
    }
    impl Drop for WriteFuture<'_> {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::AcqRel);
            std::panic::panic_any(self.chunk.clone());
        }
    }
    struct FutureDropSink {
        original: ExecutionFailure,
        future_drops: Arc<AtomicUsize>,
        aborts: Arc<AtomicUsize>,
        closes: Arc<AtomicUsize>,
    }
    // This is the actual async_trait ABI. Returning this future makes its Drop
    // a distinct phase from its poll, rather than panicking inside an async body.
    impl AsyncSinkBackend for FutureDropSink {
        type Output = ();
        fn write_chunk<'life0, 'life1, 'async_trait>(
            &'life0 mut self,
            chunk: &'life1 Chunk,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = ExecutionResult<()>> + Send + 'async_trait>,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(WriteFuture {
                chunk,
                original: self.original.clone(),
                drops: self.future_drops.clone(),
            })
        }
        fn finish<'life0, 'async_trait>(
            &'life0 mut self,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = ExecutionResult<()>> + Send + 'async_trait>,
        >
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async { Ok(()) })
        }
        fn abort<'life0, 'async_trait>(
            &'life0 mut self,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = ExecutionResult<()>> + Send + 'async_trait>,
        >
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            self.aborts.fetch_add(1, Ordering::AcqRel);
            Box::pin(async { Ok(()) })
        }
        fn close<'life0, 'async_trait>(
            &'life0 mut self,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = ExecutionResult<()>> + Send + 'async_trait>,
        >
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            self.closes.fetch_add(1, Ordering::AcqRel);
            Box::pin(async { Ok(()) })
        }
    }
    #[test]
    fn returned_required_error_is_accepted_before_unknown_future_drop_and_borrow_is_not_reentered()
    {
        use novarocks_execution_contract::TaskConclusionPort;
        let (port, state) = task_binding();
        let original = ExecutionFailure::from_domain(TestWriteFailure(85));
        let future_drops = Arc::new(AtomicUsize::new(0));
        let aborts = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let chunk = make_chunk(3);
        let array = Arc::downgrade(&chunk.columns()[0]);
        let mut op = AsyncSinkOperator::try_new(
            "future drop",
            FutureDropSink {
                original: original.clone(),
                future_drops: future_drops.clone(),
                aborts: aborts.clone(),
                closes: closes.clone(),
            },
            1,
        )
        .unwrap();
        op.activate(&state).unwrap();
        op.consume_for_test(&state, chunk).unwrap();
        assert!(poll_until(
            || {
                let _ = pure_observation(&mut op);
                op.join.is_none()
            },
            Duration::from_secs(5)
        ));
        assert!(matches!(
            pure_observation(&mut op),
            ExitProgress::Unresolved(_)
        ));
        assert!(
            port.task
                .accepted()
                .unwrap()
                .originating_failure()
                .unwrap()
                .same_occurrence(&original)
        );
        assert_eq!(port.required.load(Ordering::Acquire), 1);
        assert!(port.health.failure().is_some());
        assert_eq!(future_drops.load(Ordering::Acquire), 1);
        assert_eq!(aborts.load(Ordering::Acquire), 0);
        assert_eq!(closes.load(Ordering::Acquire), 0);
        assert_eq!(op.shared.queued.load(Ordering::Acquire), 1);
        assert!(array.upgrade().is_some());
    }

    #[test]
    fn actual_never_polled_spawn_guard_keeps_original_backend_and_receiver_after_runtime_abort() {
        let state = test_runtime_state();
        let exec = state.sink_io_executor().unwrap();
        let gate = ExitGate::new();
        struct Release(Arc<ExitGate>);
        impl Drop for Release {
            fn drop(&mut self) {
                self.0.release();
            }
        }
        let _release = Release(gate.clone());
        let blocking = gate.clone();
        let blocker = exec.spawn(async move {
            blocking.entered.store(true, Ordering::Release);
            let mut released = blocking.released.lock().unwrap();
            while !*released {
                released = blocking.cv.wait(released).unwrap();
            }
        });
        assert!(poll_until(
            || gate.entered.load(Ordering::Acquire),
            Duration::from_secs(5)
        ));
        let (backend, _, _, _) = TestAsyncSink::new(1);
        let started = backend.started.clone();
        let mut op = AsyncSinkOperator::try_new("never polled", backend, 1).unwrap();
        op.activate(&state).unwrap();
        op.join.as_ref().unwrap().abort();
        gate.release();
        futures::executor::block_on(blocker).unwrap();
        assert!(poll_until(
            || {
                let _ = pure_observation(&mut op);
                op.join.is_none()
            },
            Duration::from_secs(5)
        ));
        assert_eq!(started.load(Ordering::Acquire), 0);
        assert!(matches!(
            pure_observation(&mut op),
            ExitProgress::Unresolved(_)
        ));
        assert!(state.error_state().local_exit_fault().is_some());
        assert!(
            state.error().is_none(),
            "never-polled input wait has no Required operation failure"
        );
        assert!(op.shared.unknown_actor.lock().unwrap().is_some());
        assert!(!op.shared.frame_exited.load(Ordering::Acquire));
    }
    struct AcceptedStorage {
        data: Option<Arc<Vec<u8>>>,
        primary: Arc<AtomicUsize>,
        secondary: Arc<AtomicUsize>,
    }
    impl novarocks_type_contract::ValueTypeStorageHold for AcceptedStorage {}
    impl Drop for AcceptedStorage {
        fn drop(&mut self) {
            std::panic::panic_any(SinkPrimary {
                chunk: None,
                data: ManuallyDrop::new(self.data.take().unwrap()),
                primary: self.primary.clone(),
                secondary: self.secondary.clone(),
            });
        }
    }
    #[test]
    fn accepted_input_cleanup_moves_pure_custody_before_drop_and_never_returns_unknown_queue_credit()
     {
        let (port, state) = task_binding();
        let data = Arc::new(vec![103u8; 32 * 1024]);
        let weak = Arc::downgrade(&data);
        let primary = Arc::new(AtomicUsize::new(0));
        let secondary = Arc::new(AtomicUsize::new(0));
        let hold =
            novarocks_type_contract::ValueTypeStorageLifetime::held(Arc::new(AcceptedStorage {
                data: Some(data),
                primary: primary.clone(),
                secondary: secondary.clone(),
            }));
        let chunk = make_chunk(3).retaining_array_type_storage_observed::<novarocks_type_contract::owned_resources::layout::LayoutResourceError>(&hold, |_| Ok(())).unwrap();
        drop(hold);
        let (backend, _, _, _) = TestAsyncSink::new(1);
        let mut op = AsyncSinkOperator::try_new("accepted cleanup", backend, 1).unwrap();
        op.activate(&state).unwrap();
        assert!(
            poll_until(
                || op.shared.input_wait_entered.load(Ordering::Acquire),
                Duration::from_secs(5)
            ),
            "the real actor must be waiting for input before the accepted-input fault"
        );
        let channel = op.sender.as_ref().unwrap().clone();
        let control = state.error_state();
        let mut work =
            crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, 64);
        let mut value_conversions =
            novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = StepContext::new(&control, &mut work, &mut value_conversions);
        assert!(matches!(
            op.try_push(&mut ctx, RawInput::for_test(chunk)),
            InputResult::Accepted(Ok(_))
        ));
        assert_eq!(
            channel.capacity(),
            0,
            "the actual accepted input owns the original queue permit before drive"
        );
        op.cancel();
        assert!(poll_until(
            || op.shared.parked.load(Ordering::Acquire),
            Duration::from_secs(5)
        ));
        assert!(matches!(
            pure_observation(&mut op),
            ExitProgress::Unresolved(_)
        ));
        assert!(!op.receiver.is_occupied());
        assert_eq!(channel.capacity(), 0);
        assert_eq!(primary.load(Ordering::Acquire), 1);
        assert_eq!(secondary.load(Ordering::Acquire), 0);
        assert!(port.health.failure().is_some());
        assert_eq!(port.required.load(Ordering::Acquire), 0);
        let held = op.shared.unknown_input.lock().unwrap();
        assert!(held.as_ref().unwrap().credit.is_some());
        assert!(
            held.as_ref().unwrap().permit.is_none(),
            "ordinary sink admission never accepts a Root grant"
        );
        drop(held);
        let shared = Arc::downgrade(&op.shared);
        drop(op);
        drop(state);
        assert!(
            shared
                .upgrade()
                .unwrap()
                .unknown_input
                .lock()
                .unwrap()
                .is_some()
        );
        assert_eq!(channel.capacity(), 0);
        assert_eq!(
            weak.upgrade().unwrap().as_slice(),
            vec![103u8; 32 * 1024].as_slice()
        );
    }
    #[test]
    fn required_notification_unwind_saves_original_and_secondary_opaque_owners_before_any_drop() {
        use crate::runtime::task_control::TaskExecutionPort;
        let (port, state) = task_binding();
        let primary = Arc::new(AtomicUsize::new(0));
        let secondary = Arc::new(AtomicUsize::new(0));
        let data = Arc::new(vec![91u8; 32 * 1024]);
        let notice_data = Arc::new(vec![107u8; 16 * 1024]);
        let weak_notice = Arc::downgrade(&notice_data);
        let notice_data = Mutex::new(Some(notice_data));
        let notice_primary = Arc::new(AtomicUsize::new(0));
        let notice_secondary = Arc::new(AtomicUsize::new(0));
        let checked = Arc::new(AtomicBool::new(false));
        let aborts = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let mut op = AsyncSinkOperator::try_new(
            "notification custody",
            OpaqueSink {
                data: Some(data),
                primary: primary.clone(),
                secondary: secondary.clone(),
                aborts: aborts.clone(),
                closes: closes.clone(),
                drops: Arc::new(AtomicUsize::new(0)),
            },
            1,
        )
        .unwrap();
        op.activate(&state).unwrap();
        let shared = Arc::downgrade(&op.shared);
        let callback_checked = checked.clone();
        let notice_dropped = notice_primary.clone();
        let notice_secondary_dropped = notice_secondary.clone();
        port.stop_observable().add_observer(Arc::new(move || {
            let shared = shared.upgrade().unwrap();
            let original_saved =
                shared.primary.lock().unwrap()[SinkPhase::Write as usize * 2].is_some();
            callback_checked.store(original_saved, Ordering::Release);
            std::panic::panic_any(SinkPrimary {
                chunk: None,
                data: ManuallyDrop::new(notice_data.lock().unwrap().take().unwrap()),
                primary: notice_dropped.clone(),
                secondary: notice_secondary_dropped.clone(),
            });
        }));
        op.consume_for_test(&state, make_chunk(3)).unwrap();
        assert!(poll_until(
            || aborts.load(Ordering::Acquire) == 1
                && closes.load(Ordering::Acquire) == 1
                && op.shared.parked.load(Ordering::Acquire),
            Duration::from_secs(5)
        ));
        assert!(checked.load(Ordering::Acquire));
        assert_eq!(primary.load(Ordering::Acquire), 1);
        assert_eq!(secondary.load(Ordering::Acquire), 0);
        assert_eq!(notice_primary.load(Ordering::Acquire), 1);
        assert_eq!(notice_secondary.load(Ordering::Acquire), 0);
        assert!(
            op.shared.notice_secondary.lock().unwrap()[SinkNotice::Required as usize].is_some()
        );
        assert!(port.health.failure().is_some());
        assert_eq!(port.required.load(Ordering::Acquire), 1);
        assert!(
            state
                .error()
                .unwrap()
                .same_occurrence(&op.shared.failure().unwrap())
        );
        assert!(matches!(
            pure_observation(&mut op),
            ExitProgress::Unresolved(_)
        ));
        assert_eq!(
            weak_notice.upgrade().unwrap().as_slice(),
            vec![107u8; 16 * 1024].as_slice()
        );
    }
    struct ConcurrentSink {
        inner: OpaqueSink,
        entered: Option<std::sync::mpsc::Sender<()>>,
        gate: Arc<tokio::sync::Semaphore>,
    }
    #[async_trait::async_trait]
    impl AsyncSinkBackend for ConcurrentSink {
        type Output = ();
        async fn write_chunk(&mut self, chunk: &Chunk) -> ExecutionResult<()> {
            self.entered.take().unwrap().send(()).unwrap();
            self.gate.acquire().await.unwrap().forget();
            self.inner.write_chunk(chunk).await
        }
        async fn finish(&mut self) -> ExecutionResult<()> {
            self.inner.finish().await
        }
        async fn abort(&mut self) -> ExecutionResult<()> {
            self.inner.abort().await
        }
        async fn close(&mut self) -> ExecutionResult<()> {
            self.inner.close().await
        }
    }
    #[test]
    fn accepted_and_actor_cleanup_keep_separate_custody_without_changing_actual_required_phase() {
        let (port, state) = task_binding();
        let actor_data = Arc::new(vec![113u8; 32 * 1024]);
        let actor_weak = Arc::downgrade(&actor_data);
        let owner_data = Arc::new(vec![127u8; 32 * 1024]);
        let owner_weak = Arc::downgrade(&owner_data);
        let actor_primary = Arc::new(AtomicUsize::new(0));
        let actor_secondary = Arc::new(AtomicUsize::new(0));
        let owner_primary = Arc::new(AtomicUsize::new(0));
        let owner_secondary = Arc::new(AtomicUsize::new(0));
        let aborts = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let (entered, receiver) = std::sync::mpsc::channel();
        let mut op = AsyncSinkOperator::try_new(
            "two original owners",
            ConcurrentSink {
                inner: OpaqueSink {
                    data: Some(actor_data),
                    primary: actor_primary.clone(),
                    secondary: actor_secondary.clone(),
                    aborts: aborts.clone(),
                    closes: closes.clone(),
                    drops: Arc::new(AtomicUsize::new(0)),
                },
                entered: Some(entered),
                gate: gate.clone(),
            },
            2,
        )
        .unwrap();
        op.activate(&state).unwrap();
        op.consume_for_test(&state, make_chunk(3)).unwrap();
        receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        let hold =
            novarocks_type_contract::ValueTypeStorageLifetime::held(Arc::new(AcceptedStorage {
                data: Some(owner_data),
                primary: owner_primary.clone(),
                secondary: owner_secondary.clone(),
            }));
        let chunk = make_chunk(2).retaining_array_type_storage_observed::<novarocks_type_contract::owned_resources::layout::LayoutResourceError>(&hold, |_| Ok(())).unwrap();
        drop(hold);
        let control = state.error_state();
        let mut work =
            crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, 64);
        let mut value_conversions =
            novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = StepContext::new(&control, &mut work, &mut value_conversions);
        assert!(matches!(
            op.try_push(&mut ctx, RawInput::for_test(chunk)),
            InputResult::Accepted(Ok(_))
        ));
        op.cancel();
        let original_phase_still_write = matches!(op.shared.current_phase(), SinkPhase::Write);
        let owner_unknown_already_saved = op.shared.secondary.lock().unwrap()
            [SinkPhase::AcceptedChunk as usize * 2 + 1]
            .is_some();
        gate.add_permits(1);
        assert!(poll_until(
            || aborts.load(Ordering::Acquire) == 1
                && closes.load(Ordering::Acquire) == 1
                && op.shared.parked.load(Ordering::Acquire),
            Duration::from_secs(5)
        ));
        assert!(original_phase_still_write && owner_unknown_already_saved);
        assert_eq!(actor_primary.load(Ordering::Acquire), 1);
        assert_eq!(owner_primary.load(Ordering::Acquire), 1);
        assert_eq!(actor_secondary.load(Ordering::Acquire), 0);
        assert_eq!(owner_secondary.load(Ordering::Acquire), 0);
        assert_eq!(
            port.required.load(Ordering::Acquire),
            1,
            "earlier cleanup Health never suppresses the original Required write failure"
        );
        assert!(op.shared.secondary.lock().unwrap()[SinkPhase::Write as usize * 2].is_some());
        assert!(
            op.shared
                .unknown_input
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .credit
                .is_some()
        );
        assert!(matches!(
            pure_observation(&mut op),
            ExitProgress::Unresolved(_)
        ));
        assert_eq!(actor_weak.upgrade().unwrap()[0], 113);
        assert_eq!(owner_weak.upgrade().unwrap()[0], 127);
    }
    struct DropPanicSink {
        data: Option<Arc<Vec<u8>>>,
        primary: Arc<AtomicUsize>,
        secondary: Arc<AtomicUsize>,
        closed: Arc<AtomicBool>,
    }
    impl Drop for DropPanicSink {
        fn drop(&mut self) {
            std::panic::panic_any(SinkPrimary {
                chunk: None,
                data: ManuallyDrop::new(self.data.take().unwrap()),
                primary: self.primary.clone(),
                secondary: self.secondary.clone(),
            });
        }
    }
    #[async_trait::async_trait]
    impl AsyncSinkBackend for DropPanicSink {
        type Output = usize;
        async fn write_chunk(&mut self, _: &Chunk) -> ExecutionResult<()> {
            Ok(())
        }
        async fn finish(&mut self) -> ExecutionResult<usize> {
            Ok(41)
        }
        async fn abort(&mut self) -> ExecutionResult<()> {
            Ok(())
        }
        async fn close(&mut self) -> ExecutionResult<()> {
            self.closed.store(true, Ordering::Release);
            Ok(())
        }
    }
    #[test]
    fn actual_backend_drop_unwind_after_finish_is_cleanup_and_never_mints_physical_exit() {
        let (port, state) = task_binding();
        let data = Arc::new(vec![131u8; 32 * 1024]);
        let weak = Arc::downgrade(&data);
        let primary = Arc::new(AtomicUsize::new(0));
        let secondary = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(AtomicBool::new(false));
        let mut op = AsyncSinkOperator::try_new(
            "actual backend drop",
            DropPanicSink {
                data: Some(data),
                primary: primary.clone(),
                secondary: secondary.clone(),
                closed: closed.clone(),
            },
            1,
        )
        .unwrap();
        op.activate(&state).unwrap();
        op.finish_for_test(&state).unwrap();
        assert!(poll_until(
            || {
                let _ = pure_observation(&mut op);
                op.join.is_none()
            },
            Duration::from_secs(5)
        ));
        assert!(closed.load(Ordering::Acquire));
        assert!(matches!(
            pure_observation(&mut op),
            ExitProgress::Unresolved(_)
        ));
        assert!(
            op.take_output().is_none(),
            "no output is released as successful after unknown actor retirement"
        );
        assert!(state.error().is_none());
        assert_eq!(port.required.load(Ordering::Acquire), 0);
        assert!(port.health.failure().is_some());
        assert_eq!(primary.load(Ordering::Acquire), 1);
        assert_eq!(secondary.load(Ordering::Acquire), 0);
        assert_eq!(weak.upgrade().unwrap()[0], 131);
    }
    struct ConstructorPanicSink(OpaqueSink);
    impl AsyncSinkBackend for ConstructorPanicSink {
        type Output = ();
        fn write_chunk<'life0, 'life1, 'async_trait>(
            &'life0 mut self,
            chunk: &'life1 Chunk,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = ExecutionResult<()>> + Send + 'async_trait>,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            std::panic::panic_any(SinkPrimary {
                chunk: Some(ManuallyDrop::new(chunk.clone())),
                data: ManuallyDrop::new(self.0.data.take().unwrap()),
                primary: self.0.primary.clone(),
                secondary: self.0.secondary.clone(),
            });
        }
        fn finish<'life0, 'async_trait>(
            &'life0 mut self,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = ExecutionResult<()>> + Send + 'async_trait>,
        >
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async { Ok(()) })
        }
        fn abort<'life0, 'async_trait>(
            &'life0 mut self,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = ExecutionResult<()>> + Send + 'async_trait>,
        >
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            self.0.aborts.fetch_add(1, Ordering::AcqRel);
            Box::pin(async { Ok(()) })
        }
        fn close<'life0, 'async_trait>(
            &'life0 mut self,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = ExecutionResult<()>> + Send + 'async_trait>,
        >
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            self.0.closes.fetch_add(1, Ordering::AcqRel);
            Box::pin(async { Ok(()) })
        }
    }
    #[test]
    fn actual_backend_future_constructor_unwind_never_reenters_the_unknown_borrow() {
        let (port, state) = task_binding();
        let data = Arc::new(vec![137u8; 32 * 1024]);
        let weak = Arc::downgrade(&data);
        let primary = Arc::new(AtomicUsize::new(0));
        let secondary = Arc::new(AtomicUsize::new(0));
        let aborts = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let mut op = AsyncSinkOperator::try_new(
            "real constructor",
            ConstructorPanicSink(OpaqueSink {
                data: Some(data),
                primary: primary.clone(),
                secondary: secondary.clone(),
                aborts: aborts.clone(),
                closes: closes.clone(),
                drops: Arc::new(AtomicUsize::new(0)),
            }),
            1,
        )
        .unwrap();
        op.activate(&state).unwrap();
        op.consume_for_test(&state, make_chunk(3)).unwrap();
        assert!(poll_until(
            || {
                let _ = pure_observation(&mut op);
                op.join.is_none()
            },
            Duration::from_secs(5)
        ));
        assert_eq!(aborts.load(Ordering::Acquire), 0);
        assert_eq!(closes.load(Ordering::Acquire), 0);
        assert_eq!(port.required.load(Ordering::Acquire), 1);
        assert!(port.health.failure().is_some());
        assert!(matches!(
            pure_observation(&mut op),
            ExitProgress::Unresolved(_)
        ));
        assert_eq!(op.shared.queued.load(Ordering::Acquire), 1);
        assert_eq!(primary.load(Ordering::Acquire), 1);
        assert_eq!(secondary.load(Ordering::Acquire), 0);
        assert_eq!(weak.upgrade().unwrap()[0], 137);
    }

    struct BindPanicSink(OpaqueSink);
    #[async_trait::async_trait]
    impl AsyncSinkBackend for BindPanicSink {
        type Output = ();
        fn bind_runtime_state(&mut self, _: &RuntimeState) -> ExecutionResult<()> {
            std::panic::panic_any(SinkPrimary {
                chunk: None,
                data: ManuallyDrop::new(self.0.data.take().unwrap()),
                primary: self.0.primary.clone(),
                secondary: self.0.secondary.clone(),
            });
        }
        async fn write_chunk(&mut self, _: &Chunk) -> ExecutionResult<()> {
            unreachable!()
        }
        async fn finish(&mut self) -> ExecutionResult<()> {
            unreachable!()
        }
        async fn abort(&mut self) -> ExecutionResult<()> {
            self.0.aborts.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
        async fn close(&mut self) -> ExecutionResult<()> {
            self.0.closes.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
    }
    #[test]
    fn synchronous_bind_unwind_keeps_original_backend_without_guessing_cleanup_is_safe() {
        let (port, state) = task_binding();
        let data = Arc::new(vec![139u8; 32 * 1024]);
        let weak = Arc::downgrade(&data);
        let primary = Arc::new(AtomicUsize::new(0));
        let secondary = Arc::new(AtomicUsize::new(0));
        let aborts = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let mut op = AsyncSinkOperator::try_new(
            "real bind unwind",
            BindPanicSink(OpaqueSink {
                data: Some(data),
                primary: primary.clone(),
                secondary: secondary.clone(),
                aborts: aborts.clone(),
                closes: closes.clone(),
                drops: drops.clone(),
            }),
            1,
        )
        .unwrap();
        let failure = op.activate(&state).unwrap_err();
        assert!(state.error().unwrap().same_occurrence(&failure));
        assert!(poll_until(
            || {
                let _ = pure_observation(&mut op);
                op.join.is_none()
            },
            Duration::from_secs(5)
        ));
        assert_eq!(aborts.load(Ordering::Acquire), 0);
        assert_eq!(closes.load(Ordering::Acquire), 0);
        assert_eq!(drops.load(Ordering::Acquire), 0);
        assert_eq!(port.required.load(Ordering::Acquire), 1);
        assert!(port.health.failure().is_some());
        assert!(matches!(
            pure_observation(&mut op),
            ExitProgress::Unresolved(_)
        ));
        assert_eq!(primary.load(Ordering::Acquire), 1);
        assert_eq!(secondary.load(Ordering::Acquire), 0);
        assert_eq!(weak.upgrade().unwrap()[0], 139);
    }

    struct SuccessfulStorage {
        data: Arc<Vec<u8>>,
        dropped: Arc<AtomicUsize>,
    }
    impl novarocks_type_contract::ValueTypeStorageHold for SuccessfulStorage {}
    impl Drop for SuccessfulStorage {
        fn drop(&mut self) {
            assert_eq!(self.data[0], 149);
            self.dropped.fetch_add(1, Ordering::AcqRel);
        }
    }
    #[test]
    fn successful_write_retires_real_original_storage_before_queue_reuse_and_actual_join() {
        let (port, state) = task_binding();
        let data = Arc::new(vec![149u8; 32 * 1024]);
        let weak = Arc::downgrade(&data);
        let dropped = Arc::new(AtomicUsize::new(0));
        let hold =
            novarocks_type_contract::ValueTypeStorageLifetime::held(Arc::new(SuccessfulStorage {
                data,
                dropped: dropped.clone(),
            }));
        let chunk = make_chunk(3).retaining_array_type_storage_observed::<novarocks_type_contract::owned_resources::layout::LayoutResourceError>(&hold, |_| Ok(())).unwrap();
        drop(hold);
        let (backend, _, _, _) = TestAsyncSink::new(1);
        let mut op = AsyncSinkOperator::try_new("real successful storage", backend, 1).unwrap();
        op.activate(&state).unwrap();
        op.consume_for_test(&state, chunk).unwrap();
        assert!(poll_until(
            || op.shared.queued.load(Ordering::Acquire) == 0,
            Duration::from_secs(5)
        ));
        assert_eq!(dropped.load(Ordering::Acquire), 1);
        assert!(weak.upgrade().is_none());
        op.finish_for_test(&state).unwrap();
        assert!(poll_until(
            || matches!(pure_observation(&mut op), ExitProgress::Exited),
            Duration::from_secs(5)
        ));
        assert!(op.join.is_none());
        assert!(op.shared.frame_exited.load(Ordering::Acquire));
        assert_eq!(op.take_output(), Some(3));
        assert_eq!(port.required.load(Ordering::Acquire), 0);
        assert!(port.cleanup.lock().unwrap().is_empty());
        assert!(port.health.failure().is_none());
    }
}
