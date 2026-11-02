// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! The last upstream pull owns its complete input overlap before allocation.
//! Drivers transfer an opaque grant and Chunk; a host's finite producer owns
//! carrier validation/hydration/rendering. No driver does those operations.

use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::runtime::fragment::ExecutionResult;

#[cfg(test)]
use crate::exec::pipeline::operator::ExitProgress;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Wake, Waker};

use novarocks_execution_contract::native_result_support::NativeResultSupportGeometry;

use crate::exec::chunk::Chunk;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::{
    input::RawInput,
    operator::{ExitObservation, FinishWatch, Operator},
    shape::{
        ControlStep, ReceiverPreparation, ReceiverSlot, RootInputPreparation, RootInputTarget,
        StepOperator,
    },
    step::{AdmissionNext, InputResult, NextStep, StepResult, UpstreamPull, WaitFact, WaitReason},
    step_context::StepContext,
};
use crate::runtime::fragment::io::{
    ResultAbort, RootInputAdmission, RootInputPermit, RootProducerState, RootResultInput,
    RootResultSession, RootWorkExitProgress,
};
use crate::runtime::observable::Observable;
use crate::runtime::runtime_state::RuntimeState;
#[cfg(test)]
use arrow::datatypes::DataType;
#[cfg(test)]
use novarocks_types::SlotId;

const NAME: &str = "BOUNDED_ROOT_RESULT_SINK";

// This wake owns only its dedicated readiness registry, never a Task, input or runtime
// capability. One factory supplies the same identity to all of its drivers.
struct RootPhysicalExitWake {
    readiness: Arc<Observable>,
}
impl Wake for RootPhysicalExitWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.readiness.notify_observers();
    }
}

pub struct RootResultSinkFactory {
    session: Arc<dyn RootResultSession>,
    exit_readiness: Arc<Observable>,
    exit_waker: Waker,
    remaining: Arc<AtomicI32>,
    maximum_dop: i32,
    actual_dop: OnceLock<i32>,
    created: AtomicU64,
}
impl RootResultSinkFactory {
    pub fn try_new(
        session: Arc<dyn RootResultSession>,
        dop: i32,
    ) -> Result<Self, RootReceiverFailure> {
        if dop <= 0 || dop as u64 > NativeResultSupportGeometry::V1.root_maximum_root_drivers {
            return Err(RootReceiverFailure::InvalidDop);
        }
        let exit_readiness = session.producer_exit_observable();
        let exit_waker = Waker::from(Arc::new(RootPhysicalExitWake {
            readiness: Arc::clone(&exit_readiness),
        }));
        Ok(Self {
            session,
            exit_readiness,
            exit_waker,
            remaining: Arc::new(AtomicI32::new(0)),
            maximum_dop: dop,
            actual_dop: OnceLock::new(),
            created: AtomicU64::new(0),
        })
    }
    /// The host covers the finite operators, callback registrations, shared
    /// counter, and simultaneous readiness snapshot capacity before creating
    /// the factory. Static names do not allocate per-driver strings.
    pub fn metadata_capacity_bytes(dop: usize) -> Result<usize, RootReceiverFailure> {
        if dop == 0 || dop as u64 > NativeResultSupportGeometry::V1.root_maximum_root_drivers {
            return Err(RootReceiverFailure::InvalidDop);
        }
        // The session creates this registry with its original metadata grant
        // before the factory is constructed. Slots/snapshots retain that grant.
        let exit_backing =
            Observable::bounded_backing_bytes(dop).map_err(RootReceiverFailure::ExitReadiness)?;
        let shared_exit = exit_backing
            .checked_add(std::mem::size_of::<Observable>())
            .and_then(|n| n.checked_add(std::mem::size_of::<RootPhysicalExitWake>()))
            // Two Arc headers, both types have pointer alignment.
            .and_then(|n| n.checked_add(4 * std::mem::size_of::<usize>()))
            .ok_or(RootReceiverFailure::MetadataExtent)?;
        let per_driver = std::mem::size_of::<RootResultSink>()
            + std::mem::size_of::<Option<(usize, RootInputPermit)>>()
            // One transient producer snapshot per driver. Its neutral detail
            // is bounded but owns a String allocation independently of Self.
            + std::mem::size_of::<RootProducerState>()
            + novarocks_execution_contract::status::SAFE_DETAIL_MAX_BYTES
            // Driver sink/finish callbacks and their Arc registrations,
            // driver sink/finish forwarding, live slots and notify snapshots.
            + 32 * std::mem::size_of::<usize>();
        dop.checked_mul(per_driver)
            .and_then(|bytes| {
                bytes
                    .checked_add(std::mem::size_of::<Self>() + 4 * std::mem::size_of::<usize>())
                    .and_then(|n| n.checked_add(shared_exit))
            })
            .ok_or(RootReceiverFailure::MetadataExtent)
    }
}
impl OperatorFactory for RootResultSinkFactory {
    fn name(&self) -> &str {
        NAME
    }
    fn is_sink(&self) -> bool {
        true
    }
    fn bind_pipeline_dop(&self, dop: i32) -> ExecutionResult<()> {
        if dop <= 0 || dop > self.maximum_dop {
            return Err(RootReceiverFailure::InvalidDop.into());
        }
        let actual = *self.actual_dop.get_or_init(|| {
            self.remaining.store(dop, Ordering::Release);
            dop
        });
        if actual != dop {
            return Err(RootReceiverFailure::DopRebound.into());
        }
        Ok(())
    }
    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let valid = self.actual_dop.get() == Some(&dop) && driver_id >= 0 && driver_id < dop;
        let valid = valid && {
            let bit = 1u64 << driver_id;
            self.created.fetch_or(bit, Ordering::AcqRel) & bit == 0
        };
        Box::new(RootResultSink {
            session: Arc::clone(&self.session),
            exit_readiness: Arc::clone(&self.exit_readiness),
            exit_waker: self.exit_waker.clone(),
            remaining: Arc::clone(&self.remaining),
            receiver: ReceiverSlot::new(RootInputPreparation),
            pending_input: None,
            retiring_input: None,
            input_panic_custody: [None, None],
            runtime_error: None,
            next: NextStep::NeedInput,
            activated: false,
            finishing: false,
            contributed: false,
            finish_pending: false,
            cancelled: false,
            exiting: false,
            failure: None,
            valid_dop: valid,
        })
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum LocalInputExit {
    NotStarted,
    Started,
    Exited,
    Unresolved,
}

/// One original Root input, under its original permit. Neither field has
/// implicit Drop glue while it protects an unknown input destructor.
struct RetiringRootInput {
    chunk: Option<std::mem::ManuallyDrop<Chunk>>,
    permit: Option<std::mem::ManuallyDrop<RootInputPermit>>,
    chunk_exit: LocalInputExit,
    permit_exit: LocalInputExit,
}
impl RetiringRootInput {
    fn new(input: RootResultInput) -> Self {
        // No inspection, allocation or foreign call is allowed between these
        // pure moves and installation of both protected owners.
        let (chunk, permit) = input.into_cleanup_parts();
        Self {
            chunk: Some(std::mem::ManuallyDrop::new(chunk)),
            permit: Some(std::mem::ManuallyDrop::new(permit)),
            chunk_exit: LocalInputExit::NotStarted,
            permit_exit: LocalInputExit::NotStarted,
        }
    }
}

struct RootResultSink {
    receiver: ReceiverSlot<RootInputPreparation>,
    pending_input: Option<RootResultInput>,
    retiring_input: Option<RetiringRootInput>,
    // Two fixed phase slots; caught opaque payloads are never put in Task's
    // originating cause and never implicitly destructed or retried.
    input_panic_custody: [Option<std::mem::ManuallyDrop<Box<dyn std::any::Any + Send>>>; 2],
    runtime_error: Option<Arc<crate::runtime::runtime_state::RuntimeErrorState>>,
    session: Arc<dyn RootResultSession>,
    exit_readiness: Arc<Observable>,
    exit_waker: Waker,
    remaining: Arc<AtomicI32>,
    next: NextStep,
    failure: Option<crate::runtime::fragment::ExecutionFailure>,
    activated: bool,
    finishing: bool,
    contributed: bool,
    // Only the last driver owes this host call. A stopped publication window
    // cannot repeat the shared decrement or discard the original finish intent.
    finish_pending: bool,
    cancelled: bool,
    exiting: bool,
    valid_dop: bool,
}
enum RootObservation {
    Ready(RootProducerState),
    AwaitOwner(WaitFact),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RootReceiverFailure {
    ExitReadiness(crate::runtime::observable::ObservableCapacityError),
    DopRebound,
    MetadataExtent,
    InvalidDop,
    NotActivated,
    InputAfterFinish,
    FinishUnderflow,
    InputChunkDropPanicked,
    InputPermitDropPanicked,
    InputSubmissionContract,
}
impl std::fmt::Display for RootReceiverFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::ExitReadiness(source) => return source.fmt(f),
            Self::DopRebound => "bounded root factory was rebound to a different pipeline DOP",
            Self::MetadataExtent => "bounded root metadata capacity overflow",
            Self::InvalidDop => "bounded root DOP is outside its frozen precovered profile",
            Self::NotActivated => "bounded root work started before activation",
            Self::InputAfterFinish => "bounded root input reached a closed receiver",
            Self::FinishUnderflow => "bounded root driver finish count underflow",
            Self::InputChunkDropPanicked => {
                "root input chunk destruction did not prove actual exit"
            }
            Self::InputPermitDropPanicked => "root input permit destruction did not complete",
            Self::InputSubmissionContract => {
                "root input submission returned success without accepting its original slot"
            }
        })
    }
}
impl std::error::Error for RootReceiverFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ExitReadiness(source) => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for RootReceiverFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::fragment::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureFact};
        if matches!(self, Self::MetadataExtent) {
            return FailureFact::new(
                FailureClass::Resource,
                ExecutionReason::RootMetadataExtent.reason(),
            );
        }
        let code = match self {
            Self::ExitReadiness(source) => return source.failure_fact(),
            Self::DopRebound => ExecutionReason::RootDopRebound,
            Self::MetadataExtent => unreachable!("metadata extent handled above"),
            Self::InvalidDop => ExecutionReason::RootInvalidDop,
            Self::NotActivated => ExecutionReason::RootNotActivated,
            Self::InputAfterFinish => ExecutionReason::RootInputAfterFinish,
            Self::FinishUnderflow => ExecutionReason::RootFinishUnderflow,
            Self::InputChunkDropPanicked => ExecutionReason::RootInputChunkDropPanicked,
            Self::InputPermitDropPanicked => ExecutionReason::RootInputPermitDropPanicked,
            Self::InputSubmissionContract => ExecutionReason::RootInputSubmissionContract,
        };
        let class = if matches!(
            self,
            Self::InputChunkDropPanicked | Self::InputPermitDropPanicked
        ) {
            FailureClass::Unexpected
        } else {
            FailureClass::Invariant
        };
        FailureFact::new(class, code.reason())
    }
}
impl From<RootReceiverFailure> for crate::runtime::fragment::ExecutionFailure {
    fn from(cause: RootReceiverFailure) -> Self {
        Self::from_domain(cause)
    }
}
impl RootResultSink {
    fn retain_prepared_input(&mut self, input: crate::exec::pipeline::input::PreparedInput) {
        let (chunk, permit) = input.into_parts();
        self.pending_input = Some(RootResultInput::new(
            chunk,
            permit.expect("accepted Root input retains its original permit"),
        ));
    }
    fn record_input_exit_panic(&mut self, phase: usize, payload: Box<dyn std::any::Any + Send>) {
        let payload = std::mem::ManuallyDrop::new(payload);
        if self.input_panic_custody[phase].is_none() {
            self.input_panic_custody[phase] = Some(payload);
        }
        let cause: crate::runtime::fragment::ExecutionFailure = if phase == 0 {
            RootReceiverFailure::InputChunkDropPanicked.into()
        } else {
            RootReceiverFailure::InputPermitDropPanicked.into()
        };
        let runtime = self
            .runtime_error
            .as_ref()
            .expect("an accepted Root input has its original runtime binding");
        runtime.report_cleanup(&cause);
        runtime.report_unresolved_exit(&cause);
    }
    fn retire_local_input(&mut self) -> bool {
        if let Some(input) = self.receiver.take_original_for_exit() {
            // This concrete policy only holds one accepted original, and no
            // further input is admitted while this continuation is occupied.
            self.pending_input = Some(input);
        }
        if self.retiring_input.is_none() {
            self.retiring_input = self.pending_input.take().map(RetiringRootInput::new);
        }
        let Some(input) = self.retiring_input.as_mut() else {
            return true;
        };
        if input.chunk_exit == LocalInputExit::NotStarted {
            input.chunk_exit = LocalInputExit::Started;
            let owner = input.chunk.take();
            let exited = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                drop(owner.map(std::mem::ManuallyDrop::into_inner));
            }));
            match exited {
                Ok(()) => self.retiring_input.as_mut().unwrap().chunk_exit = LocalInputExit::Exited,
                Err(payload) => {
                    self.retiring_input.as_mut().unwrap().chunk_exit = LocalInputExit::Unresolved;
                    self.record_input_exit_panic(0, payload);
                    return false;
                }
            }
        }
        if self.retiring_input.as_ref().unwrap().chunk_exit != LocalInputExit::Exited {
            return false;
        }
        let input = self.retiring_input.as_mut().unwrap();
        if input.permit_exit == LocalInputExit::NotStarted {
            input.permit_exit = LocalInputExit::Started;
            let exited = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let receipt = input
                    .permit
                    .as_mut()
                    .map(|permit| permit.retire())
                    .unwrap_or(RootWorkExitProgress::Exited);
                if receipt == RootWorkExitProgress::Exited {
                    drop(input.permit.take().map(std::mem::ManuallyDrop::into_inner));
                }
                receipt
            }));
            match exited {
                Ok(RootWorkExitProgress::Exited) => {
                    self.retiring_input.as_mut().unwrap().permit_exit = LocalInputExit::Exited
                }
                Ok(RootWorkExitProgress::Pending | RootWorkExitProgress::Unresolved) => {
                    self.retiring_input.as_mut().unwrap().permit_exit = LocalInputExit::Unresolved;
                    return false;
                }
                Err(payload) => {
                    self.retiring_input.as_mut().unwrap().permit_exit = LocalInputExit::Unresolved;
                    self.record_input_exit_panic(1, payload);
                    return false;
                }
            }
        }
        if self.retiring_input.as_ref().unwrap().permit_exit != LocalInputExit::Exited {
            return false;
        }
        // Only empty, already-exited metadata is destroyed here.
        self.retiring_input = None;
        true
    }
    fn observe_producer(
        &self,
        control: &crate::runtime::runtime_state::RuntimeErrorState,
        operation_stopped: bool,
    ) -> ExecutionResult<RootObservation> {
        if !self.valid_dop {
            return Err(crate::runtime::fragment::ExecutionFailure::from_domain(
                RootReceiverFailure::InvalidDop,
            ));
        }
        // Sample before observing the producer. Publication may notify between
        // this read and the returned WaitFact; the driver's recheck covers it.
        let observable = control.stop_observable();
        let observed_generation = observable.generation();
        let producer = self.session.producer_state();
        if let Some(error) = control.error() {
            return Err(error);
        }
        if operation_stopped
            || matches!(
                producer,
                RootProducerState::Failed(_) | RootProducerState::AwaitTerminalControl
            )
        {
            // Metadata never supplies an originating error. A concrete stop
            // returned by a raced host call follows this same owner boundary.
            Ok(RootObservation::AwaitOwner(WaitFact {
                reason: WaitReason::Dependency,
                observable,
                observed_generation,
                deadline: None,
            }))
        } else {
            Ok(RootObservation::Ready(producer))
        }
    }
    fn park_owner(&mut self, wait: WaitFact) -> StepResult {
        self.next = NextStep::Wait(wait);
        StepResult::new(None, self.next.clone(), 0)
    }
    fn host_failure(
        &mut self,
        ctx: &StepContext<'_>,
        error: crate::runtime::fragment::ExecutionFailure,
    ) -> ExecutionResult<StepResult> {
        use novarocks_failure_contract::{FailureEvidence, FailureProjection};
        if error.failure_fact().evidence() != FailureEvidence::StopObservation {
            return Err(self.fail(ctx, error));
        }
        match self.observe_producer(ctx.failure_port(), true)? {
            RootObservation::AwaitOwner(wait) => Ok(self.park_owner(wait)),
            RootObservation::Ready(_) => unreachable!("a concrete stop cannot reopen data work"),
        }
    }
    fn fail(
        &mut self,
        ctx: &StepContext<'_>,
        error: crate::runtime::fragment::ExecutionFailure,
    ) -> crate::runtime::fragment::ExecutionFailure {
        ctx.report_failure(error.clone());
        self.failure.get_or_insert(error).clone()
    }
    fn finish_step(&mut self, ctx: &StepContext<'_>) -> ExecutionResult<StepResult> {
        if let RootObservation::AwaitOwner(wait) =
            self.observe_producer(ctx.failure_port(), false)?
        {
            return Ok(self.park_owner(wait));
        }
        let observable = self.session.writable_observable();
        let observed_generation = observable.generation();
        if !self.contributed {
            let old = self
                .remaining
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| {
                    old.checked_sub(1).filter(|next| *next >= 0)
                })
                .map_err(|_| {
                    crate::runtime::fragment::ExecutionFailure::from_domain(
                        RootReceiverFailure::FinishUnderflow,
                    )
                })?;
            self.contributed = true;
            self.finish_pending = old == 1;
        }
        if self.finish_pending {
            if let Err(error) = self.session.finish_input() {
                return self.host_failure(ctx, error.into());
            }
            self.finish_pending = false;
        }
        self.next = match self.observe_producer(ctx.failure_port(), false)? {
            RootObservation::AwaitOwner(wait) => NextStep::Wait(wait),
            RootObservation::Ready(RootProducerState::ContextHeld) => NextStep::Finished,
            RootObservation::Ready(_) => NextStep::Wait(WaitFact {
                reason: WaitReason::Finish,
                observable,
                observed_generation,
                deadline: None,
            }),
        };
        Ok(StepResult::new(None, self.next.clone(), 0))
    }
    fn submit_pending(&mut self, ctx: &StepContext<'_>) -> ExecutionResult<StepResult> {
        if let RootObservation::AwaitOwner(wait) =
            self.observe_producer(ctx.failure_port(), false)?
        {
            return Ok(self.park_owner(wait));
        }
        let rows = u64::try_from(self.pending_input.as_ref().unwrap().chunk().len())
            .expect("supported row count fits u64");
        if let Err(error) = self.session.submit_input(&mut self.pending_input) {
            return self.host_failure(ctx, error.into());
        }
        if self.pending_input.is_some() {
            return Err(self.fail(ctx, RootReceiverFailure::InputSubmissionContract.into()));
        }
        self.next = if self.finishing {
            NextStep::Runnable
        } else {
            NextStep::NeedInput
        };
        Ok(StepResult::new(None, self.next.clone(), rows))
    }
}
impl Drop for RootResultSink {
    fn drop(&mut self) {
        self.retire_local_input();
        if !self.finishing && !self.cancelled {
            self.session.abort(ResultAbort::NeverStarted);
        }
    }
}
impl OperatorLifecycle for RootResultSink {
    fn name(&self) -> &str {
        NAME
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.runtime_error = Some(state.error_state());
        Ok(())
    }
    fn activate(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        // Direct local execution tests activate against their real local
        // RuntimeState; production binds the same Task owner during prepare.
        if self.runtime_error.is_none() {
            self.runtime_error = Some(state.error_state());
        }
        if self.activated {
            return Ok(());
        }
        if let RootObservation::AwaitOwner(wait) = self.observe_producer(
            self.runtime_error
                .as_ref()
                .expect("activation binds the actual runtime owner"),
            false,
        )? {
            self.next = NextStep::Wait(wait);
        }
        self.activated = true;
        Ok(())
    }

    fn cancel(&mut self) {
        if !self.cancelled {
            self.cancelled = true;
            self.exiting = true;
            self.retire_local_input();
            self.next = NextStep::Finished;
            let reason = if self.activated {
                ResultAbort::Cancelled("root driver stopped".to_string())
            } else {
                ResultAbort::NeverStarted
            };
            self.session.abort(reason);
        }
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.exiting = true;
        if !self.finishing && !self.cancelled {
            self.cancel();
        }
        Ok(())
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.begin_exit()
    }
    fn observe_exit(&mut self) -> ExitObservation {
        if !self.exiting && !self.cancelled {
            return ExitObservation::exited();
        }
        let local_exited = self.retire_local_input();
        let producer = self.session.poll_producer_exit(&self.exit_waker);
        let watch = || FinishWatch::Notify(Arc::clone(&self.exit_readiness));
        if !local_exited || matches!(producer, RootWorkExitProgress::Unresolved) {
            // Both original supervisors have already published their neutral
            // health incident. Metadata observation cannot discharge custody.
            return ExitObservation::unresolved(watch());
        }
        match producer {
            RootWorkExitProgress::Exited => ExitObservation::exited(),
            RootWorkExitProgress::Pending => ExitObservation::pending(watch()),
            RootWorkExitProgress::Unresolved => unreachable!("handled above"),
        }
    }
}
impl StepOperator for RootResultSink {
    fn initial_step(&self) -> NextStep {
        self.next.clone()
    }
    fn control_observable(&self) -> Option<Arc<Observable>> {
        Some(self.session.writable_observable())
    }
    fn poll_control(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<ControlStep> {
        if self.cancelled {
            return Ok(ControlStep::Active);
        }
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if let RootObservation::AwaitOwner(wait) =
            self.observe_producer(ctx.failure_port(), false)?
        {
            self.next = NextStep::Wait(wait);
        }
        Ok(ControlStep::Active)
    }
    fn reserve_upstream(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<UpstreamPull> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if !self.activated {
            return Err(crate::runtime::fragment::ExecutionFailure::from_domain(
                RootReceiverFailure::NotActivated,
            ));
        }
        if let RootObservation::AwaitOwner(wait) =
            self.observe_producer(ctx.failure_port(), false)?
        {
            self.next = NextStep::Wait(wait.clone());
            return Ok(UpstreamPull::Wait(wait));
        }
        if self.finishing
            || self.cancelled
            || self.receiver.is_occupied()
            || self.pending_input.is_some()
            || self.retiring_input.is_some()
        {
            return Ok(UpstreamPull::Busy);
        }
        let observable = self.session.writable_observable();
        let observed_generation = observable.generation();
        match self.session.try_acquire_input() {
            Ok(RootInputAdmission::Granted(permit)) => Ok(UpstreamPull::Root(permit)),
            Ok(RootInputAdmission::Blocked) => Ok(UpstreamPull::Wait(WaitFact {
                reason: WaitReason::Output,
                observable,
                observed_generation,
                deadline: None,
            })),
            Err(error) => match self.host_failure(ctx, error.into())? {
                StepResult {
                    next: NextStep::Wait(wait),
                    ..
                } => Ok(UpstreamPull::Wait(wait)),
                _ => unreachable!("a concrete stop waits for its original owner"),
            },
        }
    }
    fn try_push(&mut self, ctx: &mut StepContext<'_>, input: RawInput) -> InputResult {
        if let Some(failure) = self.failure.clone().or_else(|| ctx.checkpoint().err()) {
            return InputResult::Rejected { input, failure };
        }
        if !self.activated {
            return InputResult::Rejected {
                input,
                failure: crate::runtime::fragment::ExecutionFailure::from_domain(
                    RootReceiverFailure::NotActivated,
                ),
            };
        }
        match self.observe_producer(ctx.failure_port(), false) {
            Err(failure) => return InputResult::Rejected { input, failure },
            Ok(RootObservation::AwaitOwner(wait)) => {
                self.next = NextStep::Wait(wait.clone());
                return InputResult::Deferred {
                    input,
                    next: AdmissionNext::Wait(wait),
                };
            }
            Ok(RootObservation::Ready(_)) => {}
        }
        if self.finishing || self.cancelled || matches!(self.next, NextStep::Finished) {
            return InputResult::Rejected {
                input,
                failure: crate::runtime::fragment::ExecutionFailure::from_domain(
                    RootReceiverFailure::InputAfterFinish,
                ),
            };
        }
        if self.pending_input.is_some() || self.retiring_input.is_some() {
            return InputResult::Deferred {
                input,
                next: AdmissionNext::ResumeExisting,
            };
        }
        let target = RootInputTarget {
            task: self.session.spec().task,
        };
        let result = self.receiver.try_accept(ctx, input, &target);
        if matches!(result, InputResult::Accepted(_)) {
            self.next = NextStep::Runnable;
        }
        result
    }
    fn drive(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if !self.activated {
            return Err(crate::runtime::fragment::ExecutionFailure::from_domain(
                RootReceiverFailure::NotActivated,
            ));
        }
        if let RootObservation::AwaitOwner(wait) =
            self.observe_producer(ctx.failure_port(), false)?
        {
            return Ok(self.park_owner(wait));
        }
        if matches!(&self.next, NextStep::Wait(wait) if !wait.is_ready()) {
            return Ok(StepResult::new(None, self.next.clone(), 0));
        }
        if self.pending_input.is_some() {
            return self.submit_pending(ctx);
        }
        let target = RootInputTarget {
            task: self.session.spec().task,
        };
        match self.receiver.prepare(ctx, &target) {
            Ok(ReceiverPreparation::Ready(input)) => {
                // Install original ownership before a stop check or any host
                // code. Rejection/panic leaves this same continuation intact.
                self.retain_prepared_input(input);
                self.submit_pending(ctx)
            }
            Ok(ReceiverPreparation::Pending(next)) => {
                self.next = next;
                Ok(StepResult::new(None, self.next.clone(), 0))
            }
            Ok(ReceiverPreparation::Empty) if self.finishing => self.finish_step(ctx),
            Ok(ReceiverPreparation::Empty) => Ok(StepResult::new(None, self.next.clone(), 0)),
            Err(error) => Err(self.fail(ctx, error)),
        }
    }
    fn finish(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<()> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        self.finishing = true;
        if let RootObservation::AwaitOwner(wait) =
            self.observe_producer(ctx.failure_port(), false)?
        {
            self.next = NextStep::Wait(wait);
            return Ok(());
        }
        if matches!(self.next, NextStep::NeedInput) {
            self.next = NextStep::Runnable;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
    use crate::exec::pipeline::driver::{DriverState, PipelineDriver};
    use crate::runtime::fragment::io::{
        FragmentIoError, FragmentIoErrorKind, FragmentIoOperation, ResultWriteCredit,
        RootFailureObservation, RootInputAuthority, RootResultWriteSpec,
    };
    use arrow::array::Int32Array;
    use novarocks_execution_contract::TaskIdentity;
    use novarocks_result_contract::{FrozenRootOutput, RootOutputContract, RootProfileId};
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::time::Duration;

    struct Session {
        spec: RootResultWriteSpec,
        authority: RootInputAuthority,
        input: Mutex<Option<RootResultInput>>,
        state: Mutex<RootProducerState>,
        exited: AtomicBool,
        unresolved: AtomicBool,
        exit_readiness: Arc<Observable>,
        exit_waker: Mutex<Option<Waker>>,
        finishes: AtomicUsize,
        acquisitions: AtomicUsize,
        held_bytes: Arc<AtomicUsize>,
        input_backing: Arc<Mutex<Option<std::sync::Weak<Int32Array>>>>,
        released_live_backing: Arc<AtomicBool>,
        panic_in_state: AtomicBool,
        panic_in_submit: AtomicBool,
        reject_before_take: Mutex<Option<FragmentIoError>>,
        error_after_take: Mutex<Option<FragmentIoError>>,
        stop_race: AtomicUsize,
    }
    impl Session {
        fn new() -> Arc<Self> {
            let spec = RootResultWriteSpec {
                task: TaskIdentity::new(
                    QueryExecutionId::new(QueryId::new(51, 52), AttemptId::new(1).unwrap())
                        .unwrap(),
                    StageId::new(1).unwrap(),
                    TaskId::new(1).unwrap(),
                    BackendProcessId::new_v7(),
                ),
                contract: Arc::new(RootOutputContract::new(
                    RootProfileId::V1,
                    FrozenRootOutput::CountOnly,
                )),
            };
            Arc::new(Self {
                authority: RootInputAuthority::new(
                    &spec,
                    Arc::new(crate::runtime::execution_health::ExecutionHealth::new()),
                ),
                spec,
                input: Mutex::new(None),
                state: Mutex::new(RootProducerState::Accepting),
                exited: AtomicBool::new(false),
                unresolved: AtomicBool::new(false),
                exit_readiness: Arc::new(Observable::try_bounded(64).unwrap()),
                exit_waker: Mutex::new(None),
                finishes: AtomicUsize::new(0),
                acquisitions: AtomicUsize::new(0),
                held_bytes: Arc::new(AtomicUsize::new(0)),
                input_backing: Arc::new(Mutex::new(None)),
                released_live_backing: Arc::new(AtomicBool::new(false)),
                panic_in_state: AtomicBool::new(false),
                panic_in_submit: AtomicBool::new(false),
                reject_before_take: Mutex::new(None),
                error_after_take: Mutex::new(None),
                stop_race: AtomicUsize::new(0),
            })
        }
        fn complete_input(&self) {
            let input = self.input.lock().unwrap().take();
            if let Some(input) = input {
                let (chunk, mut permit) = input.into_cleanup_parts();
                drop(chunk);
                assert_eq!(
                    permit.retire(),
                    crate::runtime::fragment::io::RootWorkExitProgress::Exited
                );
            }
        }
        fn fail(&self, owner: &crate::runtime::runtime_state::RuntimeErrorState) {
            let original: crate::runtime::fragment::ExecutionFailure =
                novarocks_functions::KernelFailure::Internal(
                    novarocks_functions::KernelDiagnostic::new("root producer failed"),
                )
                .into();
            let observation = RootFailureObservation::from_failure(&original);
            *self.state.lock().unwrap() = RootProducerState::Failed(observation);
            owner.set_error(original);
            self.writable_observable().notify_observers();
        }
        fn stop_at(&self, point: usize) -> Result<(), FragmentIoError> {
            if self
                .stop_race
                .compare_exchange(point, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                *self.state.lock().unwrap() = RootProducerState::AwaitTerminalControl;
                return Err(FragmentIoError::from_domain(
                    FragmentIoOperation::ResultWrite,
                    FragmentIoErrorKind::Cancelled,
                    RootRaceStop,
                ));
            }
            Ok(())
        }
        fn actual_exit(&self) {
            self.complete_input();
            let mut state = self.state.lock().unwrap();
            if !matches!(
                *state,
                RootProducerState::Failed(_) | RootProducerState::AwaitTerminalControl
            ) {
                *state = RootProducerState::ContextHeld;
            }
            drop(state);
            self.exited.store(true, Ordering::Release);
            let wake = self.exit_waker.lock().unwrap().clone();
            if let Some(wake) = wake {
                wake.wake();
            }
            self.writable_observable().notify_observers();
        }
    }
    impl RootResultSession for Session {
        fn spec(&self) -> &RootResultWriteSpec {
            &self.spec
        }
        fn try_acquire_input(&self) -> Result<RootInputAdmission, FragmentIoError> {
            self.acquisitions.fetch_add(1, Ordering::AcqRel);
            self.stop_at(1)?;
            let crate::runtime::fragment::io::RootInputReservationAdmission::Granted(
                mut reservation,
            ) = self.authority.try_reserve()?
            else {
                return Ok(RootInputAdmission::Blocked);
            };
            let bytes = reservation.required_bytes();
            self.held_bytes.fetch_add(bytes, Ordering::SeqCst);
            let held = Arc::clone(&self.held_bytes);
            let backing = Arc::clone(&self.input_backing);
            let released_live = Arc::clone(&self.released_live_backing);
            let mut credit = Some(ResultWriteCredit::new(bytes, move |released| {
                if backing
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_some_and(|weak| weak.upgrade().is_some())
                {
                    released_live.store(true, Ordering::SeqCst);
                }
                held.fetch_sub(released, Ordering::SeqCst);
            }));
            let admission = reservation.try_bind(&mut credit);
            if !matches!(admission, Ok(RootInputAdmission::Granted(_))) {
                reservation.retire_credit(&mut credit);
            }
            admission
        }
        fn writable_observable(&self) -> Arc<Observable> {
            self.authority.observable()
        }
        fn submit_input(
            &self,
            submitted: &mut Option<RootResultInput>,
        ) -> Result<(), FragmentIoError> {
            assert!(
                !self.panic_in_submit.load(Ordering::SeqCst),
                "host entry panic before inspection"
            );
            let original = submitted
                .as_ref()
                .expect("the caller retains its original slot");
            if !self.authority.owns_original(original.permit()) {
                return Err(FragmentIoError::new(
                    FragmentIoOperation::ResultWrite,
                    FragmentIoErrorKind::InvalidResponse,
                    "foreign root input grant",
                ));
            }
            self.stop_at(2)?;
            if let Some(error) = self.reject_before_take.lock().unwrap().as_ref() {
                return Err(error.clone());
            }
            let mut input = self.input.lock().unwrap();
            assert!(input.is_none());
            *input = submitted.take();
            drop(input);
            self.stop_at(3)?;
            // Like a host's metadata wake, this failure happens after transfer.
            if let Some(error) = self.error_after_take.lock().unwrap().as_ref() {
                return Err(error.clone());
            }
            Ok(())
        }
        fn finish_input(&self) -> Result<(), FragmentIoError> {
            self.finishes.fetch_add(1, Ordering::SeqCst);
            self.stop_at(4)?;
            *self.state.lock().unwrap() = RootProducerState::Finishing;
            Ok(())
        }
        fn producer_state(&self) -> RootProducerState {
            assert!(
                !self.panic_in_state.load(Ordering::SeqCst),
                "host state panic"
            );
            self.state.lock().unwrap().clone()
        }
        fn producer_exit_observable(&self) -> Arc<Observable> {
            Arc::clone(&self.exit_readiness)
        }
        fn poll_producer_exit(&self, waker: &Waker) -> RootWorkExitProgress {
            let mut current = self.exit_waker.lock().unwrap();
            if let Some(current) = current.as_ref() {
                assert!(
                    current.will_wake(waker),
                    "one factory owns runtime readiness"
                );
            } else {
                *current = Some(waker.clone());
            }
            let issuer = self.authority.input_exit_progress();
            if self.unresolved.load(Ordering::Acquire) || issuer == RootWorkExitProgress::Unresolved
            {
                RootWorkExitProgress::Unresolved
            } else if self.exited.load(Ordering::Acquire) && issuer == RootWorkExitProgress::Exited
            {
                RootWorkExitProgress::Exited
            } else {
                RootWorkExitProgress::Pending
            }
        }
        fn abort(&self, _reason: ResultAbort) {
            self.authority.close();
        }
    }
    #[derive(Debug)]
    struct RootRaceStop;
    impl std::fmt::Display for RootRaceStop {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("root host observed its closed producer")
        }
    }
    impl std::error::Error for RootRaceStop {}
    impl novarocks_failure_contract::FailureProjection for RootRaceStop {
        fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
            novarocks_failure_contract::FailureFact::stop_observation(
                crate::runtime::fragment::ExecutionReason::TaskControlObservedStop.reason(),
            )
        }
    }
    fn factory(session: &Arc<Session>, dop: i32) -> RootResultSinkFactory {
        let factory =
            RootResultSinkFactory::try_new(Arc::clone(session) as Arc<dyn RootResultSession>, dop)
                .unwrap();
        factory.bind_pipeline_dop(dop).unwrap();
        factory
    }
    fn with_context<T>(
        action: impl FnOnce(&mut StepContext<'_>) -> ExecutionResult<T>,
    ) -> ExecutionResult<T> {
        let control = crate::runtime::runtime_state::RuntimeErrorState::default();
        let mut work =
            crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, 64);
        action(&mut StepContext::new(
            &control,
            &mut work,
            &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
        ))
    }
    fn reserve(operator: &mut Box<dyn Operator>) -> ExecutionResult<UpstreamPull> {
        operator.activate(&RuntimeState::default())?;
        with_context(|ctx| operator.as_mut().reserve_upstream(ctx))
    }
    fn grant(operator: &mut Box<dyn Operator>) -> RootInputPermit {
        match reserve(operator).unwrap() {
            UpstreamPull::Root(grant) => grant,
            _ => panic!("expected pre-pull grant"),
        }
    }
    fn submit(
        operator: &mut Box<dyn Operator>,
        chunk: Chunk,
        grant: RootInputPermit,
    ) -> ExecutionResult<StepResult> {
        with_context(|ctx| {
            let step = operator.as_mut();
            match step.try_push(ctx, RawInput::covered_for_test(chunk, grant)) {
                InputResult::Accepted(result) => {
                    result?;
                }
                InputResult::Rejected { failure, .. } => return Err(failure),
                InputResult::Deferred { .. } => panic!("precovered input was not accepted"),
            }
            step.drive(ctx)
        })
    }
    fn finish(operator: &mut Box<dyn Operator>) -> ExecutionResult<StepResult> {
        operator.activate(&RuntimeState::default())?;
        with_context(|ctx| {
            let step = operator.as_mut();
            step.finish(ctx)?;
            step.drive(ctx)
        })
    }
    fn pending_exit(operator: &mut Box<dyn Operator>) -> bool {
        matches!(
            operator.observe_exit().progress,
            ExitProgress::Pending(_) | ExitProgress::Unresolved(_)
        )
    }

    #[test]
    fn drivers_share_one_pre_pull_position_through_actual_input_exit() {
        let session = Session::new();
        let factory = factory(&session, 2);
        let mut first = factory.create(2, 0);
        let mut second = factory.create(2, 1);
        let first_grant = grant(&mut first);
        assert!(matches!(
            reserve(&mut second).unwrap(),
            UpstreamPull::Wait(_)
        ));
        assert_eq!(
            session.held_bytes.load(Ordering::SeqCst),
            session.authority.required_bytes()
        );
        drop(first_grant);
        let second_grant = grant(&mut second);
        submit(&mut second, Chunk::default(), second_grant).unwrap();
        assert!(
            matches!(reserve(&mut first).unwrap(), UpstreamPull::Wait(_)),
            "submitted input still owns its unique position"
        );
        session.complete_input();
        drop(grant(&mut first));
        assert!(matches!(
            finish(&mut first).unwrap().next,
            NextStep::Wait(_)
        ));
        assert_eq!(session.finishes.load(Ordering::SeqCst), 0);
        assert!(matches!(
            finish(&mut second).unwrap().next,
            NextStep::Wait(_)
        ));
        assert_eq!(session.finishes.load(Ordering::SeqCst), 1);
        first.begin_exit().unwrap();
        assert!(pending_exit(&mut first));
        session.actual_exit();
        assert!(!pending_exit(&mut first));
        assert_eq!(session.held_bytes.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn root_push_cannot_acquire_a_grant_after_materialization() {
        let session = Session::new();
        let factory = factory(&session, 1);
        let mut sink = factory.create(1, 0);
        sink.activate(&RuntimeState::default()).unwrap();
        let result = with_context(|ctx| {
            Ok(sink
                .as_mut()
                .try_push(ctx, RawInput::for_test(Chunk::default())))
        })
        .unwrap();
        assert!(matches!(result, InputResult::Rejected { .. }));
        assert_eq!(session.held_bytes.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn actual_sixty_four_drivers_finish_once_without_input_multiplication() {
        let session = Session::new();
        let factory = factory(&session, 64);
        let mut sinks: Vec<_> = (0..64).map(|id| factory.create(64, id)).collect();
        for sink in &mut sinks {
            assert!(
                !pending_exit(sink),
                "an idle producer must not park a live driver"
            );
        }
        let original = grant(&mut sinks[0]);
        for sink in &mut sinks[1..] {
            assert!(matches!(reserve(sink).unwrap(), UpstreamPull::Wait(_)));
        }
        // The caller owns the unused pre-pull grant, not the root receiver.
        drop(original);
        for (index, sink) in sinks.iter_mut().enumerate() {
            assert!(matches!(finish(sink).unwrap().next, NextStep::Wait(_)));
            assert_eq!(
                session.finishes.load(Ordering::SeqCst),
                usize::from(index == 63)
            );
            sink.begin_exit().unwrap();
        }
        assert_eq!(session.held_bytes.load(Ordering::SeqCst), 0);
        session.actual_exit();
        assert!(sinks.iter_mut().all(|sink| !pending_exit(sink)));
    }
    #[test]
    fn sixty_four_root_drivers_use_real_scheduler_through_full_finish_and_actual_exit() {
        use crate::exec::pipeline::{
            fragment_context::FragmentContext,
            global_driver_executor::{DriverTask, FragmentCompletion, GlobalDriverExecutor},
        };
        struct FiniteSource {
            session: Arc<Session>,
            pulls: Arc<AtomicUsize>,
        }
        impl OperatorLifecycle for FiniteSource {
            fn name(&self) -> &str {
                "ROOT_SCHEDULER_SOURCE"
            }
        }
        impl StepOperator for FiniteSource {
            fn try_push(&mut self, _: &mut StepContext<'_>, _: RawInput) -> InputResult {
                panic!("source does not accept input")
            }
            fn drive(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
                assert!(
                    !self.session.authority.is_available(),
                    "pre-pull coverage precedes work"
                );
                self.pulls.fetch_add(1, Ordering::AcqRel);
                Ok(StepResult::new(
                    Some(Chunk::default()),
                    NextStep::Finished,
                    0,
                ))
            }
            fn finish(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<()> {
                Ok(())
            }
        }
        struct ExitGuard(Arc<Session>);
        impl Drop for ExitGuard {
            fn drop(&mut self) {
                self.0.actual_exit();
            }
        }
        fn await_fact(mut fact: impl FnMut() -> bool) {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while !fact() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "root scheduler phase did not progress"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        let executor = GlobalDriverExecutor::new(1);
        let session = Session::new();
        let _exit = ExitGuard(session.clone());
        let root_factory = factory(&session, 64);
        let pulls = Arc::new(AtomicUsize::new(0));
        let runtime = Arc::new(RuntimeState::default());
        let finst = (61_001, 61_002);
        let context = Arc::new(FragmentContext::new(
            None,
            runtime.clone(),
            Some(finst),
            None,
            None,
            None,
        ));
        let completion = FragmentCompletion::new(64, runtime.error_state());
        let event = session.authority.observable();
        let host = event.try_subscribe(Arc::new(|| {})).unwrap();
        let held = match session.try_acquire_input().unwrap() {
            RootInputAdmission::Granted(permit) => permit,
            RootInputAdmission::Blocked => panic!("initial overlap is available"),
        };
        let tasks = (0..64)
            .map(|id| {
                DriverTask::new(
                    PipelineDriver::new(
                        id,
                        vec![
                            Box::new(FiniteSource {
                                session: session.clone(),
                                pulls: pulls.clone(),
                            }),
                            root_factory.create(64, id),
                        ],
                        None,
                        vec![],
                        runtime.clone(),
                        Some(finst),
                    ),
                    completion.clone(),
                    context.clone(),
                    Duration::from_millis(10),
                )
            })
            .collect();
        assert!(executor.submit(tasks));
        await_fact(|| session.acquisitions.load(Ordering::Acquire) >= 65);
        assert_eq!(pulls.load(Ordering::Acquire), 0);
        assert!(completion.conclusion().is_none());
        // The bounded issuer has exactly one control registration per live
        // driver, plus this host. Data parking reuses that same cover.
        let spare: Vec<_> = (0..64)
            .map(|_| event.try_subscribe(Arc::new(|| {})).unwrap())
            .collect();
        assert!(matches!(
            event.try_subscribe(Arc::new(|| {})),
            Err(crate::runtime::observable::ObservableCapacityError::CapacityExceeded)
        ));
        drop(spare);
        drop(held);
        await_fact(|| {
            session.complete_input();
            assert!(completion.conclusion().is_none());
            session.finishes.load(Ordering::Acquire) == 1
        });
        session.complete_input();
        assert_eq!(pulls.load(Ordering::Acquire), 64);
        assert_eq!(session.held_bytes.load(Ordering::Acquire), 0);
        assert!(completion.stopped_fact().is_none());
        *session.state.lock().unwrap() = RootProducerState::ContextHeld;
        event.notify_observers();
        // ContextHeld closes data flow, but the physical owner is still live.
        std::thread::sleep(Duration::from_millis(20));
        assert!(completion.stopped_fact().is_none());
        session.actual_exit();
        await_fact(|| completion.stopped_fact().is_some());
        assert_eq!(completion.stopped_fact().unwrap().conclusion(), Ok(()));
        assert_eq!(session.finishes.load(Ordering::Acquire), 1);
        executor.shutdown().unwrap();
        // Retirement also returns every scoped slot to the exact issuer.
        drop(host);
        let retired: Vec<_> = (0..129)
            .map(|_| event.try_subscribe(Arc::new(|| {})).unwrap())
            .collect();
        drop(retired);
    }

    #[test]
    fn cancellation_and_logical_failure_keep_pending_finish_until_actual_exit() {
        let session = Session::new();
        let factory = factory(&session, 1);
        let mut sink = factory.create(1, 0);
        let original = grant(&mut sink);
        submit(&mut sink, Chunk::default(), original).unwrap();
        let error = with_context(|ctx| {
            session.fail(ctx.failure_port());
            sink.as_mut().poll_control(ctx)
        })
        .unwrap_err();
        assert!(error.detail().contains("root producer failed"));
        sink.cancel();
        assert!(pending_exit(&mut sink));
        assert!(
            session.held_bytes.load(Ordering::SeqCst) > 0,
            "logical failure did not drop the host input"
        );
        session.actual_exit();
        assert!(!pending_exit(&mut sink));
        assert_eq!(session.held_bytes.load(Ordering::SeqCst), 0);
    }

    fn delayed_root_fixture() -> (
        Arc<Session>,
        RootResultSinkFactory,
        RuntimeState,
        Arc<crate::runtime::task_control::test_support::DeferredOwner>,
        Arc<crate::runtime::task_control::TaskExecutionControl>,
    ) {
        use novarocks_execution_contract::TaskFailurePhase;
        let owner = crate::runtime::task_control::test_support::DeferredOwner::new();
        let control = Arc::new(owner.view(TaskFailurePhase::Execution));
        let state = RuntimeState::new(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::for_task(
                control.clone(),
            )),
        );
        let mut session = Session::new();
        let original = Arc::get_mut(&mut session).unwrap();
        original.spec.task = control.identity();
        original.authority = RootInputAuthority::new(
            &original.spec,
            Arc::new(crate::runtime::execution_health::ExecutionHealth::new()),
        );
        let factory = factory(&session, 1);
        (session, factory, state, owner, control)
    }

    fn owner_wait(next: NextStep, state: &RuntimeState) -> WaitFact {
        let NextStep::Wait(wait) = next else {
            panic!("the original owner's publication must remain pending");
        };
        assert_eq!(wait.reason, WaitReason::Dependency);
        assert!(Arc::ptr_eq(
            &wait.observable,
            &state.error_state().stop_observable()
        ));
        assert!(wait.deadline.is_none());
        assert!(!wait.is_ready());
        wait
    }

    #[test]
    fn every_root_entry_waits_for_the_real_task_failure_before_publication() {
        #[derive(Clone, Copy, Debug)]
        enum Entry {
            Activate,
            Control,
            Reserve,
            Push,
            Drive,
            Finish,
            FinishStep,
        }
        for failed_metadata in [false, true] {
            for entry in [
                Entry::Activate,
                Entry::Control,
                Entry::Reserve,
                Entry::Push,
                Entry::Drive,
                Entry::Finish,
                Entry::FinishStep,
            ] {
                let (session, factory, state, owner, control) = delayed_root_fixture();
                let mut sink = factory.create(1, 0);
                sink.bind_runtime_state(&state).unwrap();
                if !matches!(entry, Entry::Activate) {
                    sink.activate(&state).unwrap();
                }
                let permit = matches!(entry, Entry::Push).then(|| {
                    input_exit_context(&state, |ctx| match sink.reserve_upstream(ctx).unwrap() {
                        UpstreamPull::Root(permit) => permit,
                        _ => panic!("the original finite input position is available"),
                    })
                });
                if matches!(entry, Entry::FinishStep) {
                    input_exit_context(&state, |ctx| sink.finish(ctx)).unwrap();
                }
                let original: crate::runtime::fragment::ExecutionFailure =
                    novarocks_functions::KernelFailure::ResourceExhausted.into();
                *session.state.lock().unwrap() = if failed_metadata {
                    RootProducerState::Failed(RootFailureObservation::from_failure(&original))
                } else {
                    RootProducerState::AwaitTerminalControl
                };
                let before_acquisitions = session.acquisitions.load(Ordering::SeqCst);
                let mut retained = None;
                let wait = match entry {
                    Entry::Activate => {
                        sink.activate(&state).unwrap();
                        owner_wait(sink.initial_step(), &state)
                    }
                    Entry::Control => {
                        assert_eq!(
                            input_exit_context(&state, |ctx| sink.poll_control(ctx)).unwrap(),
                            ControlStep::Active
                        );
                        owner_wait(sink.initial_step(), &state)
                    }
                    Entry::Reserve => {
                        let UpstreamPull::Wait(wait) =
                            input_exit_context(&state, |ctx| sink.reserve_upstream(ctx)).unwrap()
                        else {
                            panic!("failed metadata cannot acquire another Root permit");
                        };
                        owner_wait(NextStep::Wait(wait), &state)
                    }
                    Entry::Push => {
                        let array: arrow::array::ArrayRef =
                            Arc::new(Int32Array::from(vec![21, 34, 55]));
                        let schema = Arc::new(
                            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                                SlotId::new(1),
                                arrow::datatypes::Field::new("original", DataType::Int32, false),
                                None,
                                None,
                            )])
                            .unwrap(),
                        );
                        let batch = arrow::record_batch::RecordBatch::try_new(
                            schema.arrow_schema_ref(),
                            vec![array.clone()],
                        )
                        .unwrap();
                        let chunk = Chunk::new_with_chunk_schema(batch, schema);
                        let input = RawInput::covered_for_test(chunk, permit.unwrap());
                        let InputResult::Deferred {
                            input,
                            next: AdmissionNext::Wait(wait),
                        } = input_exit_context(&state, |ctx| sink.try_push(ctx, input))
                        else {
                            panic!("the caller retains the exact covered input until publication");
                        };
                        assert_eq!(input.rows(), 3);
                        assert!(Arc::ptr_eq(
                            input.original_for_test().record_batch().column(0),
                            &array
                        ));
                        assert!(session.held_bytes.load(Ordering::SeqCst) > 0);
                        retained = Some(input);
                        owner_wait(NextStep::Wait(wait), &state)
                    }
                    Entry::Drive | Entry::FinishStep => {
                        let step = input_exit_context(&state, |ctx| sink.drive(ctx)).unwrap();
                        assert!(step.output.is_none());
                        assert_eq!(step.committed_rows, 0);
                        owner_wait(step.next, &state)
                    }
                    Entry::Finish => {
                        input_exit_context(&state, |ctx| sink.finish(ctx)).unwrap();
                        owner_wait(sink.initial_step(), &state)
                    }
                };
                assert_eq!(session.finishes.load(Ordering::SeqCst), 0, "{entry:?}");
                assert_eq!(factory.remaining.load(Ordering::SeqCst), 1, "{entry:?}");
                assert_eq!(
                    session.acquisitions.load(Ordering::SeqCst),
                    before_acquisitions,
                    "{entry:?}"
                );
                assert_eq!(owner.pending_len(), 0, "metadata submitted no new cause");
                assert!(state.error_state().error().is_none());
                control.report_failure(original.clone());
                assert_eq!(owner.pending_len(), 1);
                assert!(!wait.is_ready(), "submission is not accepted publication");
                owner.advance_all();
                assert!(
                    wait.is_ready(),
                    "the actual owner wakes its original waiter"
                );
                let observed =
                    input_exit_context(&state, |ctx| sink.poll_control(ctx)).unwrap_err();
                assert!(observed.same_occurrence(&original));
                assert_eq!(owner.pending_len(), 0);
                assert_eq!(session.finishes.load(Ordering::SeqCst), 0);
                drop(retained);
                sink.cancel();
                session.actual_exit();
                assert!(!pending_exit(&mut sink));
                assert_eq!(session.held_bytes.load(Ordering::SeqCst), 0);
            }
        }
    }

    #[test]
    fn raced_root_stop_observations_cannot_overtake_the_original_task_cause() {
        for point in [1, 2, 3, 4] {
            let (session, factory, state, owner, control) = delayed_root_fixture();
            let mut sink = factory.create(1, 0);
            sink.bind_runtime_state(&state).unwrap();
            sink.activate(&state).unwrap();
            if matches!(point, 2 | 3) {
                accept_exit_input(&mut sink, &state, Chunk::default());
            }
            if point == 4 {
                input_exit_context(&state, |ctx| sink.finish(ctx)).unwrap();
            }
            session.stop_race.store(point, Ordering::SeqCst);
            let next = if point == 1 {
                match input_exit_context(&state, |ctx| sink.reserve_upstream(ctx)).unwrap() {
                    UpstreamPull::Wait(wait) => NextStep::Wait(wait),
                    _ => panic!("a raced stopped acquire waits for the actual owner"),
                }
            } else {
                let step = input_exit_context(&state, |ctx| sink.drive(ctx)).unwrap();
                assert!(step.output.is_none());
                assert_eq!(step.committed_rows, 0);
                step.next
            };
            let wait = owner_wait(next, &state);
            assert_eq!(
                owner.pending_len(),
                0,
                "no unowned stop proposal was submitted"
            );
            assert!(state.error_state().error().is_none());
            assert_eq!(session.input.lock().unwrap().is_some(), point == 3);
            if matches!(point, 2 | 3) {
                assert!(session.held_bytes.load(Ordering::SeqCst) > 0);
            }
            for _ in 0..3 {
                let step = input_exit_context(&state, |ctx| sink.drive(ctx)).unwrap();
                assert_eq!(step.committed_rows, 0);
                owner_wait(step.next, &state);
            }
            assert_eq!(
                session.finishes.load(Ordering::SeqCst),
                usize::from(point == 4)
            );
            assert_eq!(
                factory.remaining.load(Ordering::SeqCst),
                i32::from(point != 4),
                "a raced finish contributes to the shared count only once"
            );
            let original: crate::runtime::fragment::ExecutionFailure =
                novarocks_functions::KernelFailure::ResourceExhausted.into();
            control.report_failure(original.clone());
            assert_eq!(owner.pending_len(), 1);
            owner.advance_all();
            assert!(wait.is_ready());
            let observed = input_exit_context(&state, |ctx| sink.drive(ctx)).unwrap_err();
            assert!(observed.same_occurrence(&original));
            assert_eq!(owner.pending_len(), 0);
            sink.cancel();
            session.actual_exit();
            assert!(!pending_exit(&mut sink));
            assert_eq!(session.held_bytes.load(Ordering::SeqCst), 0);
        }
    }
    #[test]
    fn unresolved_job_keeps_its_finish_obligation_and_original_input() {
        let session = Session::new();
        let factory = factory(&session, 1);
        let mut sink = factory.create(1, 0);
        let original = grant(&mut sink);
        submit(&mut sink, Chunk::default(), original).unwrap();
        sink.cancel();
        assert!(pending_exit(&mut sink));
        session.unresolved.store(true, Ordering::Release);
        assert!(pending_exit(&mut sink));
        assert!(session.held_bytes.load(Ordering::Acquire) > 0);
        assert!(session.input.lock().unwrap().is_some());
        // Test teardown owns the fake input; this is not a physical-exit fact.
        session.complete_input();
    }

    #[test]
    fn all_root_drivers_share_one_exit_wake_independent_of_writable_credit() {
        let session = Session::new();
        let factory = factory(&session, 2);
        let mut first = factory.create(2, 0);
        let mut second = factory.create(2, 1);
        first.cancel();
        second.cancel();
        let first_watch = match first.observe_exit().progress {
            ExitProgress::Pending(FinishWatch::Notify(watch)) => watch,
            _ => panic!("expected the pending physical job watch"),
        };
        let second_watch = match second.observe_exit().progress {
            ExitProgress::Pending(FinishWatch::Notify(watch)) => watch,
            _ => panic!("expected the pending physical job watch"),
        };
        assert!(Arc::ptr_eq(&first_watch, &second_watch));
        assert!(!Arc::ptr_eq(&first_watch, &session.writable_observable()));
        let generation = first_watch.generation();
        session.writable_observable().notify_observers();
        assert_eq!(first_watch.generation(), generation);
        session.actual_exit();
        assert!(first_watch.generation() > generation);
        assert!(!pending_exit(&mut first));
        assert!(!pending_exit(&mut second));
    }

    #[test]
    fn driver_identity_and_frozen_dop_have_no_fallback() {
        let session = Session::new();
        assert!(RootResultSinkFactory::try_new(session.clone(), 0).is_err());
        assert!(RootResultSinkFactory::metadata_capacity_bytes(65).is_err());
        let factory = factory(&session, 1);
        let state = RuntimeState::default();
        let mut first = factory.create(1, 0);
        assert!(first.activate(&state).is_ok());
        let mut repeated = factory.create(1, 0);
        assert!(repeated.activate(&state).is_err());
        let mut mismatch = factory.create(2, 1);
        assert!(mismatch.activate(&state).is_err());
    }
    #[test]
    fn built_root_dop_can_collapse_within_its_precovered_upper_bound() {
        let session = Session::new();
        let factory = RootResultSinkFactory::try_new(session.clone(), 64).unwrap();
        let mut unbound = factory.create(1, 0);
        assert!(unbound.activate(&RuntimeState::default()).is_err());
        factory.bind_pipeline_dop(1).unwrap();
        assert!(factory.bind_pipeline_dop(2).is_err());
        let mut sink = factory.create(1, 0);
        finish(&mut sink).unwrap();
        assert_eq!(session.finishes.load(Ordering::SeqCst), 1);
        session.actual_exit();
    }
    #[test]
    fn producer_exit_alone_cannot_replace_end_and_context_handoff() {
        let session = Session::new();
        let factory = factory(&session, 1);
        let mut sink = factory.create(1, 0);
        finish(&mut sink).unwrap();
        session.exited.store(true, Ordering::Release);
        assert!(
            !pending_exit(&mut sink),
            "the actual producer exit is independent"
        );
        let step = with_context(|ctx| sink.as_mut().drive(ctx)).unwrap();
        assert!(
            matches!(step.next, NextStep::Wait(_)),
            "End and protected context handoff are still missing"
        );
        assert!(matches!(step.next, NextStep::Wait(_)));
        *session.state.lock().unwrap() = RootProducerState::ContextHeld;
        session.writable_observable().notify_observers();
        assert!(matches!(
            with_context(|ctx| sink.as_mut().drive(ctx)).unwrap().next,
            NextStep::Finished
        ));
    }

    struct Source {
        session: Arc<Session>,
        pulls: Arc<AtomicUsize>,
        produced: bool,
        empty: bool,
        fail_in_pull: bool,
    }
    impl OperatorLifecycle for Source {
        fn name(&self) -> &str {
            "PRE_PULL_PROOF_SOURCE"
        }
    }
    impl crate::exec::pipeline::shape::SourceKernel for Source {
        fn poll_output(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            assert!(!self.produced, "last pull is delivered only once");
            assert_eq!(
                self.session.held_bytes.load(Ordering::SeqCst),
                self.session.authority.required_bytes(),
                "coverage must exist before this owner can allocate its final output"
            );
            self.pulls.fetch_add(1, Ordering::SeqCst);
            self.produced = true;
            let chunk = if self.fail_in_pull {
                let array = Arc::new(Int32Array::from(vec![1, 2, 3]));
                *self.session.input_backing.lock().unwrap() = Some(Arc::downgrade(&array));
                let schema = Arc::new(
                    ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                        SlotId::new(1),
                        arrow::datatypes::Field::new("input", DataType::Int32, false),
                        None,
                        None,
                    )])
                    .unwrap(),
                );
                let batch = arrow::record_batch::RecordBatch::try_new(
                    schema.arrow_schema_ref(),
                    vec![array],
                )
                .unwrap();
                let chunk = Chunk::new_with_chunk_schema(batch, schema);
                self.session.fail(ctx.failure_port());
                Some(chunk)
            } else {
                (!self.empty).then(Chunk::default)
            };
            Ok(StepResult::new(chunk, NextStep::Finished, 0))
        }
    }
    #[test]
    fn actual_driver_last_pull_is_precovered_and_empty_pull_returns_the_grant() {
        for empty in [false, true] {
            let session = Session::new();
            let factory = factory(&session, 1);
            let pulls = Arc::new(AtomicUsize::new(0));
            let mut driver = PipelineDriver::new(
                1,
                vec![
                    Box::new(crate::exec::pipeline::shape::Source::new(Source {
                        session: Arc::clone(&session),
                        pulls: Arc::clone(&pulls),
                        produced: false,
                        empty,
                        fail_in_pull: false,
                    })),
                    factory.create(1, 0),
                ],
                None,
                Vec::new(),
                Arc::new(RuntimeState::default()),
                None,
            );
            let mut state = driver.process(Duration::from_millis(100));
            // A None pull can make the source terminal without moving an
            // edge. The ordinary driver yields before closing that edge.
            if state == DriverState::Ready {
                state = driver.process(Duration::from_millis(100));
            }
            assert_eq!(
                state,
                DriverState::Blocked(crate::exec::pipeline::operator::BlockedReason::OutputFull)
            );
            assert_eq!(pulls.load(Ordering::SeqCst), 1);
            assert_eq!(
                session.held_bytes.load(Ordering::SeqCst),
                if empty {
                    0
                } else {
                    session.authority.required_bytes()
                }
            );
            session.actual_exit();
            assert_eq!(
                driver.process(Duration::from_millis(100)),
                DriverState::Finished
            );
        }
    }
    #[test]
    fn failed_last_pull_drops_real_edge_backing_before_returning_credit() {
        let session = Session::new();
        let factory = factory(&session, 1);
        let mut driver = PipelineDriver::new(
            1,
            vec![
                Box::new(crate::exec::pipeline::shape::Source::new(Source {
                    session: Arc::clone(&session),
                    pulls: Arc::new(AtomicUsize::new(0)),
                    produced: false,
                    empty: false,
                    fail_in_pull: true,
                })),
                factory.create(1, 0),
            ],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            None,
        );
        assert_eq!(
            driver.process(Duration::from_millis(100)),
            DriverState::PendingFinish
        );
        assert!(
            !session.released_live_backing.load(Ordering::SeqCst),
            "input credit returned before the real Arrow edge owner exited"
        );
        assert!(
            session
                .input_backing
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .upgrade()
                .is_none()
        );
        assert_eq!(session.held_bytes.load(Ordering::SeqCst), 0);
        session.actual_exit();
        assert!(matches!(
            driver.process(Duration::from_millis(100)),
            DriverState::Failed(_)
        ));
    }
    #[test]
    fn late_producer_failure_cannot_turn_pending_success_into_finished() {
        let session = Session::new();
        let factory = factory(&session, 1);
        let runtime = Arc::new(RuntimeState::default());
        let mut driver = PipelineDriver::new(
            1,
            vec![
                Box::new(crate::exec::pipeline::shape::Source::new(Source {
                    session: Arc::clone(&session),
                    pulls: Arc::new(AtomicUsize::new(0)),
                    produced: false,
                    empty: false,
                    fail_in_pull: false,
                })),
                factory.create(1, 0),
            ],
            None,
            Vec::new(),
            runtime.clone(),
            None,
        );
        assert_eq!(
            driver.process(Duration::from_millis(100)),
            DriverState::Blocked(crate::exec::pipeline::operator::BlockedReason::OutputFull)
        );
        session.fail(runtime.error_state().as_ref());
        assert_eq!(
            driver.process(Duration::from_millis(100)),
            DriverState::PendingFinish
        );
        session.actual_exit();
        assert!(
            matches!(driver.process(Duration::from_millis(100)), DriverState::Failed(error) if error.detail().contains("root producer failed"))
        );
    }

    #[derive(Clone, Copy)]
    enum QuantumExit {
        Chunk,
        Empty,
        Error,
        Panic,
    }
    struct QuantumSource {
        session: Arc<Session>,
        pulls: Arc<AtomicUsize>,
        generation: Arc<AtomicU64>,
        workspace: Option<Arc<Int32Array>>,
        exit: QuantumExit,
        last_turn: Option<u64>,
        pause: Option<Arc<Observable>>,
    }
    impl OperatorLifecycle for QuantumSource {
        fn name(&self) -> &str {
            "ROOT_QUANTUM_PROOF_SOURCE"
        }
    }
    impl crate::exec::pipeline::shape::StepOperator for QuantumSource {
        fn initial_step(&self) -> crate::exec::pipeline::step::NextStep {
            crate::exec::pipeline::step::NextStep::Runnable
        }
        fn try_push(
            &mut self,
            _: &mut crate::exec::pipeline::step_context::StepContext<'_>,
            _: crate::exec::pipeline::input::RawInput,
        ) -> crate::exec::pipeline::step::InputResult {
            panic!("source must not receive input")
        }
        fn drive(
            &mut self,
            _: &mut crate::exec::pipeline::step_context::StepContext<'_>,
        ) -> ExecutionResult<crate::exec::pipeline::step::StepResult> {
            panic!("protected source must borrow the original root grant")
        }
        fn finish(
            &mut self,
            _: &mut crate::exec::pipeline::step_context::StepContext<'_>,
        ) -> ExecutionResult<()> {
            panic!("source must not receive input EOS")
        }
        fn drive_with_root_input(
            &mut self,
            ctx: &mut crate::exec::pipeline::step_context::StepContext<'_>,
            input: &RootInputPermit,
        ) -> ExecutionResult<crate::exec::pipeline::step::RootStep> {
            (|| -> ExecutionResult<crate::exec::pipeline::step::RootStep> {
                use crate::exec::pipeline::step::{NextStep, RootPending, RootStep, StepResult};
                assert_eq!(input.task(), self.session.spec.task);
                assert_eq!(
                    input.retained_bytes(),
                    self.session.authority.required_bytes()
                );
                assert_eq!(
                    self.session.held_bytes.load(Ordering::SeqCst),
                    input.retained_bytes()
                );
                let turn = self.pulls.fetch_add(1, Ordering::SeqCst);
                if turn == 0 {
                    self.generation.store(input.generation(), Ordering::SeqCst);
                    let array = Arc::new(Int32Array::from(vec![1, 2, 3]));
                    *self.session.input_backing.lock().unwrap() = Some(Arc::downgrade(&array));
                    self.workspace = Some(array);
                }
                assert_eq!(input.generation(), self.generation.load(Ordering::SeqCst));
                assert_ne!(self.last_turn, Some(ctx.work().generation().get()));
                self.last_turn = Some(ctx.work().generation().get());
                if turn == 1
                    && let Some(observable) = self.pause.take()
                {
                    let observed_generation = observable.generation();
                    return Ok(RootStep::Pending(RootPending::Wait(
                        crate::exec::pipeline::step::WaitFact {
                            reason: crate::exec::pipeline::step::WaitReason::Output,
                            observable,
                            observed_generation,
                            deadline: None,
                        },
                    )));
                }
                if turn < 2 {
                    return Ok(RootStep::Pending(RootPending::Yield));
                }
                match self.exit {
                    QuantumExit::Empty => Ok(RootStep::Complete(StepResult::new(
                        None,
                        NextStep::Finished,
                        0,
                    ))),
                    QuantumExit::Error => Err(novarocks_functions::KernelFailure::Internal(
                        novarocks_functions::KernelDiagnostic::new("protected materializer failed"),
                    )
                    .into()),
                    QuantumExit::Panic => panic!("protected materializer panic"),
                    QuantumExit::Chunk => {
                        let schema = Arc::new(
                            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                                SlotId::new(1),
                                arrow::datatypes::Field::new("input", DataType::Int32, false),
                                None,
                                None,
                            )])
                            .unwrap(),
                        );
                        let batch = arrow::record_batch::RecordBatch::try_new(
                            schema.arrow_schema_ref(),
                            vec![self.workspace.as_ref().unwrap().clone()],
                        )
                        .unwrap();
                        // Keep construction scratch until the driver synchronously
                        // clears it before transferring the grant into the host.
                        Ok(RootStep::Complete(StepResult::new(
                            Some(Chunk::new_with_chunk_schema(batch, schema)),
                            NextStep::Finished,
                            3,
                        )))
                    }
                }
            })()
        }
        fn release_root_workspace(&mut self) {
            if self.workspace.is_some() {
                assert_eq!(
                    self.session.held_bytes.load(Ordering::SeqCst),
                    self.session.authority.required_bytes()
                );
                drop(self.workspace.take());
            }
        }
    }
    fn quantum_driver(
        session: &Arc<Session>,
        exit: QuantumExit,
    ) -> (PipelineDriver, Arc<AtomicUsize>, Arc<AtomicU64>) {
        quantum_driver_with_pause(session, exit, None)
    }
    fn quantum_driver_with_pause(
        session: &Arc<Session>,
        exit: QuantumExit,
        pause: Option<Arc<Observable>>,
    ) -> (PipelineDriver, Arc<AtomicUsize>, Arc<AtomicU64>) {
        quantum_driver_with_state(session, exit, pause, Arc::new(RuntimeState::default()))
    }
    fn quantum_driver_with_state(
        session: &Arc<Session>,
        exit: QuantumExit,
        pause: Option<Arc<Observable>>,
        state: Arc<RuntimeState>,
    ) -> (PipelineDriver, Arc<AtomicUsize>, Arc<AtomicU64>) {
        let factory = factory(session, 1);
        let pulls = Arc::new(AtomicUsize::new(0));
        let generation = Arc::new(AtomicU64::new(0));
        let driver = PipelineDriver::new(
            1,
            vec![
                Box::new(QuantumSource {
                    session: session.clone(),
                    pulls: pulls.clone(),
                    generation: generation.clone(),
                    workspace: None,
                    exit,
                    last_turn: None,
                    pause,
                }),
                factory.create(1, 0),
            ],
            None,
            Vec::new(),
            state,
            None,
        );
        (driver, pulls, generation)
    }
    fn assert_workspace_exited_before_credit(session: &Session) {
        assert!(
            session
                .input_backing
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .upgrade()
                .is_none(),
            "the actual Arrow workspace must have exited"
        );
        assert!(
            !session.released_live_backing.load(Ordering::SeqCst),
            "returning the original credit cannot precede actual backing exit"
        );
        assert_eq!(session.held_bytes.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn yielded_root_pull_keeps_the_same_original_grant_until_host_input_exit() {
        let session = Session::new();
        let (mut driver, pulls, generation) = quantum_driver(&session, QuantumExit::Chunk);
        for turn in 1..=2 {
            assert_eq!(
                driver.process(Duration::from_millis(100)),
                DriverState::Ready
            );
            assert_eq!(pulls.load(Ordering::SeqCst), turn);
            assert_eq!(
                session.held_bytes.load(Ordering::SeqCst),
                session.authority.required_bytes()
            );
            assert!(
                session
                    .input_backing
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .upgrade()
                    .is_some()
            );
            assert!(session.input.lock().unwrap().is_none());
        }
        assert_eq!(
            driver.process(Duration::from_millis(100)),
            DriverState::Blocked(crate::exec::pipeline::operator::BlockedReason::OutputFull)
        );
        assert_eq!(pulls.load(Ordering::SeqCst), 3);
        assert_eq!(
            session
                .input
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .permit()
                .generation(),
            generation.load(Ordering::SeqCst)
        );
        session.complete_input();
        assert_workspace_exited_before_credit(&session);
        session.actual_exit();
        assert_eq!(
            driver.process(Duration::from_millis(100)),
            DriverState::Finished
        );
    }
    #[test]
    fn waiting_root_workspace_keeps_its_original_grant_and_resumes_only_on_its_event() {
        let session = Session::new();
        let event = Arc::new(Observable::new());
        let (mut driver, pulls, generation) =
            quantum_driver_with_pause(&session, QuantumExit::Chunk, Some(event.clone()));
        assert_eq!(
            driver.process(Duration::from_millis(100)),
            DriverState::Ready
        );
        let original_generation = generation.load(Ordering::SeqCst);
        for _ in 0..2 {
            assert_eq!(
                driver.process(Duration::from_millis(100)),
                DriverState::Blocked(crate::exec::pipeline::operator::BlockedReason::InputEmpty)
            );
            let (wait, observed, _) = driver.blocked_observable_snapshot().unwrap();
            assert_eq!(wait.generation(), observed);
            assert_eq!(
                pulls.load(Ordering::SeqCst),
                2,
                "same-generation wait must not reenter producer work"
            );
            assert_eq!(
                session.held_bytes.load(Ordering::SeqCst),
                session.authority.required_bytes()
            );
            assert!(session.input.lock().unwrap().is_none());
        }
        let (endpoint, observed, _) = driver.blocked_observable_snapshot().unwrap();
        event.notify_observers();
        assert_ne!(endpoint.generation(), observed);
        driver.set_ready();
        assert_eq!(
            driver.process(Duration::from_millis(100)),
            DriverState::Blocked(crate::exec::pipeline::operator::BlockedReason::OutputFull)
        );
        assert_eq!(pulls.load(Ordering::SeqCst), 3);
        assert_eq!(
            session
                .input
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .permit()
                .generation(),
            original_generation
        );
        session.complete_input();
        assert_workspace_exited_before_credit(&session);
        session.actual_exit();
        assert_eq!(
            driver.process(Duration::from_millis(100)),
            DriverState::Finished
        );
    }

    #[test]
    fn empty_and_failed_quantum_drop_workspace_before_returning_original_grant() {
        for exit in [QuantumExit::Empty, QuantumExit::Error] {
            let session = Session::new();
            let (mut driver, pulls, _) = quantum_driver(&session, exit);
            assert_eq!(
                driver.process(Duration::from_millis(100)),
                DriverState::Ready
            );
            assert_eq!(
                driver.process(Duration::from_millis(100)),
                DriverState::Ready
            );
            let mut state = driver.process(Duration::from_millis(100));
            if state == DriverState::Ready {
                state = driver.process(Duration::from_millis(100));
            }
            match exit {
                QuantumExit::Empty => assert_eq!(
                    state,
                    DriverState::Blocked(
                        crate::exec::pipeline::operator::BlockedReason::OutputFull
                    )
                ),
                QuantumExit::Error => assert_eq!(state, DriverState::PendingFinish),
                _ => unreachable!(),
            }
            assert_eq!(pulls.load(Ordering::SeqCst), 3);
            assert_workspace_exited_before_credit(&session);
            session.actual_exit();
            let state = driver.process(Duration::from_millis(100));
            match exit {
                QuantumExit::Empty => assert_eq!(state, DriverState::Finished),
                QuantumExit::Error => assert!(matches!(state,
                    DriverState::Failed(error) if error.to_string().ends_with("protected materializer failed"))),
                QuantumExit::Chunk | QuantumExit::Panic => unreachable!(),
            }
        }
    }
    #[test]
    fn cancelled_and_dropped_driver_clear_yielded_workspace_before_original_grant() {
        for cancel in [true, false] {
            let session = Session::new();
            let (mut driver, _, _) = quantum_driver(&session, QuantumExit::Chunk);
            assert_eq!(
                driver.process(Duration::from_millis(100)),
                DriverState::Ready
            );
            if cancel {
                assert_eq!(
                    driver.cancel_for_fragment_abort(),
                    DriverState::PendingFinish
                );
                assert_workspace_exited_before_credit(&session);
                session.actual_exit();
                assert_eq!(
                    driver.process(Duration::from_millis(100)),
                    DriverState::Canceled
                );
            } else {
                drop(driver);
                assert_workspace_exited_before_credit(&session);
            }
        }
    }

    #[test]
    fn host_state_panic_drops_original_chunk_before_edge_input_grant() {
        for in_submit in [false, true] {
            let session = Session::new();
            let factory = factory(&session, 1);
            let mut sink = factory.create(1, 0);
            let input = grant(&mut sink);
            let array = Arc::new(Int32Array::from(vec![1, 2, 3]));
            *session.input_backing.lock().unwrap() = Some(Arc::downgrade(&array));
            let schema = Arc::new(
                ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                    SlotId::new(1),
                    arrow::datatypes::Field::new("input", DataType::Int32, false),
                    None,
                    None,
                )])
                .unwrap(),
            );
            let batch =
                arrow::record_batch::RecordBatch::try_new(schema.arrow_schema_ref(), vec![array])
                    .unwrap();
            let chunk = Chunk::new_with_chunk_schema(batch, schema);
            session.panic_in_state.store(!in_submit, Ordering::SeqCst);
            session.panic_in_submit.store(in_submit, Ordering::SeqCst);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                submit(&mut sink, chunk, input)
            }));
            assert!(result.is_err());
            session.panic_in_state.store(false, Ordering::SeqCst);
            session.panic_in_submit.store(false, Ordering::SeqCst);
            if in_submit {
                assert!(session.input.lock().unwrap().is_none());
                assert!(
                    session
                        .input_backing
                        .lock()
                        .unwrap()
                        .as_ref()
                        .unwrap()
                        .upgrade()
                        .is_some()
                );
                assert_eq!(
                    session.held_bytes.load(Ordering::SeqCst),
                    session.authority.required_bytes()
                );
            }
            sink.cancel();
            assert_workspace_exited_before_credit(&session);
            assert!(pending_exit(&mut sink));
            session.actual_exit();
            assert!(!pending_exit(&mut sink));
        }
    }

    #[test]
    fn pull_panic_retains_original_grant_until_executor_failure_cleanup() {
        let session = Session::new();
        let state = Arc::new(RuntimeState::default());
        let failures = state.error_state();
        let (mut driver, _, _) =
            quantum_driver_with_state(&session, QuantumExit::Panic, None, state);
        assert_eq!(
            driver.process(Duration::from_millis(100)),
            DriverState::Ready
        );
        assert_eq!(
            driver.process(Duration::from_millis(100)),
            DriverState::Ready
        );
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            driver.process(Duration::from_millis(100))
        }));
        let payload = result.expect_err("the actual upstream materializer panicked");
        assert_eq!(
            session.held_bytes.load(Ordering::SeqCst),
            session.authority.required_bytes()
        );
        assert!(
            session
                .input_backing
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .upgrade()
                .is_some()
        );
        let mut published = None;
        assert_eq!(
            driver.capture_process_panic(payload, |cause| {
                failures.set_error(cause.clone());
                published = Some(cause);
            }),
            DriverState::PendingFinish
        );
        let failure = published.expect("the actual executor handoff publishes its original cause");
        assert!(failures.error().unwrap().same_occurrence(&failure));
        assert_workspace_exited_before_credit(&session);
        session.actual_exit();
        match driver.process(Duration::from_millis(100)) {
            DriverState::Failed(actual) => assert!(actual.same_occurrence(&failure)),
            other => panic!("panic cause remains after actual exit: {other:?}"),
        }
    }

    /// Keep the established Task acceptance fixture, while exposing this
    /// scope's real neutral diagnostics/health without production visibility.
    struct InputExitTaskPort {
        owner: Arc<crate::runtime::task_control::test_support::Owner>,
        health: Arc<crate::runtime::execution_health::ExecutionHealth>,
        cleanup: Mutex<[Option<novarocks_failure_contract::FailureFact>; 4]>,
    }
    impl
        novarocks_execution_contract::TaskConclusionPort<crate::runtime::fragment::ExecutionFailure>
        for InputExitTaskPort
    {
        fn identity(&self) -> TaskIdentity {
            novarocks_execution_contract::TaskConclusionPort::identity(self.owner.as_ref())
        }
        fn accepted(
            &self,
        ) -> Option<
            Arc<
                novarocks_execution_contract::AcceptedTaskConclusion<
                    crate::runtime::fragment::ExecutionFailure,
                >,
            >,
        > {
            novarocks_execution_contract::TaskConclusionPort::accepted(self.owner.as_ref())
        }
        fn submit(
            &self,
            proposed: novarocks_execution_contract::TaskConclusionProposal<
                crate::runtime::fragment::ExecutionFailure,
            >,
        ) {
            novarocks_execution_contract::TaskConclusionPort::submit(self.owner.as_ref(), proposed)
        }
    }
    impl crate::runtime::task_control::TaskExecutionPort for InputExitTaskPort {
        fn execution_health(&self) -> Arc<crate::runtime::execution_health::ExecutionHealth> {
            self.health.clone()
        }
        fn report_cleanup(
            &self,
            cause: &crate::runtime::fragment::ExecutionFailure,
            _: novarocks_execution_contract::TaskFailurePhase,
        ) {
            use novarocks_failure_contract::FailureProjection;
            let mut cleanup = self.cleanup.lock().unwrap();
            if let Some(slot) = cleanup.iter_mut().find(|slot| slot.is_none()) {
                *slot = Some(cause.failure_fact());
            }
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
            crate::runtime::task_control::TaskExecutionPort::is_stopped(self.owner.as_ref())
        }
        fn stop_observable(&self) -> Arc<Observable> {
            crate::runtime::task_control::TaskExecutionPort::stop_observable(self.owner.as_ref())
        }
        fn wait_until_stopped(&self, duration: Duration) -> bool {
            crate::runtime::task_control::TaskExecutionPort::wait_until_stopped(
                self.owner.as_ref(),
                duration,
            )
        }
    }
    fn input_exit_task_fixture() -> (
        Arc<Session>,
        Box<dyn Operator>,
        RuntimeState,
        Arc<InputExitTaskPort>,
    ) {
        use novarocks_execution_contract::{TaskConclusionPort, TaskFailurePhase};
        let port = Arc::new(InputExitTaskPort {
            owner: crate::runtime::task_control::test_support::Owner::new(),
            health: Arc::new(crate::runtime::execution_health::ExecutionHealth::new()),
            cleanup: Mutex::new([None; 4]),
        });
        let control = Arc::new(
            crate::runtime::task_control::TaskExecutionControl::new(
                port.identity(),
                port.clone(),
                TaskFailurePhase::Execution,
            )
            .unwrap(),
        );
        let state = RuntimeState::new(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::for_task(
                control,
            )),
        );
        let mut session = Session::new();
        let original = Arc::get_mut(&mut session).unwrap();
        original.spec.task = port.identity();
        original.authority = RootInputAuthority::new(
            &original.spec,
            Arc::new(crate::runtime::execution_health::ExecutionHealth::new()),
        );
        let mut sink = factory(&session, 1).create(1, 0);
        sink.bind_runtime_state(&state).unwrap();
        sink.activate(&state).unwrap();
        (session, sink, state, port)
    }
    fn input_exit_context<T>(
        state: &RuntimeState,
        action: impl FnOnce(&mut StepContext<'_>) -> T,
    ) -> T {
        let control = state.error_state();
        let mut work =
            crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, 64);
        action(&mut StepContext::new(
            &control,
            &mut work,
            &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
        ))
    }
    fn accept_exit_input(sink: &mut Box<dyn Operator>, state: &RuntimeState, chunk: Chunk) {
        let permit = input_exit_context(state, |ctx| match sink.reserve_upstream(ctx).unwrap() {
            UpstreamPull::Root(permit) => permit,
            _ => panic!("the original Root overlap must be available"),
        });
        input_exit_context(state, |ctx| {
            match sink.try_push(ctx, RawInput::covered_for_test(chunk, permit)) {
                InputResult::Accepted(step) => {
                    step.unwrap();
                }
                _ => panic!("the actual Root receiver must accept this covered input"),
            }
        });
    }
    struct RootForeignBacking {
        _values: Box<[i32]>,
        drops: Arc<AtomicUsize>,
        panic_on_drop: Arc<AtomicBool>,
        payload_drops: Arc<AtomicUsize>,
    }
    struct RootForeignDropPayload(Arc<AtomicUsize>);
    impl Drop for RootForeignDropPayload {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl Drop for RootForeignBacking {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            if self.panic_on_drop.load(Ordering::SeqCst) {
                std::panic::panic_any(RootForeignDropPayload(self.payload_drops.clone()));
            }
        }
    }
    fn foreign_exit_chunk(
        session: &Session,
        drops: Arc<AtomicUsize>,
        panic_on_drop: Arc<AtomicBool>,
        payload_drops: Arc<AtomicUsize>,
    ) -> Chunk {
        use arrow::buffer::{Buffer, ScalarBuffer};
        let values = vec![7_i32, 11, 19].into_boxed_slice();
        let pointer = std::ptr::NonNull::new(values.as_ptr().cast_mut().cast::<u8>()).unwrap();
        let bytes = values.len() * std::mem::size_of::<i32>();
        let owner = Arc::new(RootForeignBacking {
            _values: values,
            drops,
            panic_on_drop,
            payload_drops,
        });
        // SAFETY: the custom owner retains the initialized aligned i32 region
        // until the final Arrow buffer alias invokes this actual destructor.
        let buffer = unsafe { Buffer::from_custom_allocation(pointer, bytes, owner) };
        let array = Arc::new(Int32Array::new(ScalarBuffer::new(buffer, 0, 3), None));
        *session.input_backing.lock().unwrap() = Some(Arc::downgrade(&array));
        let schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                arrow::datatypes::Field::new("foreign", DataType::Int32, false),
                None,
                None,
            )])
            .unwrap(),
        );
        let batch =
            arrow::record_batch::RecordBatch::try_new(schema.arrow_schema_ref(), vec![array])
                .unwrap();
        Chunk::new_with_chunk_schema(batch, schema)
    }
    fn input_rejection() -> FragmentIoError {
        FragmentIoError::from_domain(
            FragmentIoOperation::ResultWrite,
            FragmentIoErrorKind::RemoteRejected,
            novarocks_functions::KernelFailure::ResourceExhausted,
        )
    }

    #[test]
    fn accepted_original_cancelled_before_prepare_retires_real_backing_before_its_permit() {
        use novarocks_execution_contract::{
            CancelReason, TaskConclusionPort, TaskConclusionProposal,
        };
        let (session, mut sink, state, port) = input_exit_task_fixture();
        let drops = Arc::new(AtomicUsize::new(0));
        let chunk = foreign_exit_chunk(
            &session,
            drops.clone(),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicUsize::new(0)),
        );
        accept_exit_input(&mut sink, &state, chunk);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(session.input.lock().unwrap().is_none());
        assert_eq!(
            session.held_bytes.load(Ordering::SeqCst),
            session.authority.required_bytes()
        );
        port.submit(TaskConclusionProposal::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        ));
        let accepted = port.accepted().unwrap();
        sink.cancel();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_workspace_exited_before_credit(&session);
        assert!(pending_exit(&mut sink));
        assert!(Arc::ptr_eq(&port.accepted().unwrap(), &accepted));
        assert!(port.health.is_healthy());
        assert!(port.cleanup.lock().unwrap().iter().all(Option::is_none));
        session.actual_exit();
        assert!(!pending_exit(&mut sink));
    }

    #[test]
    fn unaccepted_original_drop_panic_keeps_its_permit_and_exact_task_cause_after_host_exit() {
        use novarocks_execution_contract::{TaskConclusionPort, TaskFailurePhase};
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureProjection, FailureReason, domain,
        };
        for entry_panics in [false, true] {
            let (session, mut sink, state, port) = input_exit_task_fixture();
            let drops = Arc::new(AtomicUsize::new(0));
            let panic_on_drop = Arc::new(AtomicBool::new(false));
            let payload_drops = Arc::new(AtomicUsize::new(0));
            let chunk = foreign_exit_chunk(
                &session,
                drops.clone(),
                panic_on_drop.clone(),
                payload_drops.clone(),
            );
            accept_exit_input(&mut sink, &state, chunk);
            panic_on_drop.store(true, Ordering::SeqCst);
            let rejected = input_rejection();
            if entry_panics {
                session.panic_in_submit.store(true, Ordering::SeqCst);
            } else {
                *session.reject_before_take.lock().unwrap() = Some(rejected.clone());
            }
            let before = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                input_exit_context(&state, |ctx| sink.drive(ctx))
            }));
            let original = if entry_panics {
                let payload = before.unwrap_err();
                session.panic_in_submit.store(false, Ordering::SeqCst);
                let cause: crate::runtime::fragment::ExecutionFailure =
                    crate::exec::pipeline::driver_failure::DriverFailure::from_panic(
                        payload.as_ref(),
                    )
                    .into();
                drop(payload);
                assert_eq!(
                    cause.failure_fact(),
                    FailureFact::new(
                        FailureClass::Unexpected,
                        FailureReason::new(
                            domain::EXECUTION,
                            std::num::NonZeroU16::new(128).unwrap()
                        )
                    )
                );
                assert_eq!(
                    cause.detail(),
                    "panic in driver execution: host entry panic before inspection"
                );
                state.error_state().set_error(cause);
                state.error_state().error().unwrap()
            } else {
                let error = before.unwrap().unwrap_err();
                assert_eq!(error.failure_fact(), rejected.failure_fact());
                assert_eq!(error.detail(), rejected.to_string());
                assert!(error.same_occurrence(&state.error_state().error().unwrap()));
                error
            };
            let accepted = port.accepted().unwrap();
            assert!(
                accepted
                    .originating_failure()
                    .unwrap()
                    .same_occurrence(&original)
            );
            assert_eq!(
                drops.load(Ordering::SeqCst),
                0,
                "the original Sink slot still owns the rejected input"
            );
            assert!(session.input.lock().unwrap().is_none());
            assert_eq!(
                session.held_bytes.load(Ordering::SeqCst),
                session.authority.required_bytes()
            );
            sink.cancel();
            assert!(
                !session.authority.is_available(),
                "independent abort closes admission even when local Drop is unknown"
            );
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
            assert_eq!(
                session.held_bytes.load(Ordering::SeqCst),
                session.authority.required_bytes()
            );
            let expected = FailureFact::new(
                FailureClass::Unexpected,
                FailureReason::new(domain::EXECUTION, std::num::NonZeroU16::new(587).unwrap()),
            );
            let fault = port.health.failure().unwrap();
            assert_eq!(fault.fact(), expected);
            assert_eq!(
                fault.origin(),
                crate::runtime::execution_health::ExecutionExitOrigin::Task {
                    identity: port.identity(),
                    phase: TaskFailurePhase::Execution
                }
            );
            assert_eq!(
                *port.cleanup.lock().unwrap(),
                [Some(expected), None, None, None]
            );
            session.actual_exit();
            for _ in 0..3 {
                sink.cancel();
                sink.close().unwrap();
                assert!(
                    pending_exit(&mut sink),
                    "host whole exit cannot erase the unknown local input destructor"
                );
            }
            assert!(Arc::ptr_eq(&port.accepted().unwrap(), &accepted));
            assert!(
                state
                    .error_state()
                    .error()
                    .unwrap()
                    .same_occurrence(&original)
            );
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
            assert_eq!(
                *port.cleanup.lock().unwrap(),
                [Some(expected), None, None, None]
            );
            assert_eq!(
                session.held_bytes.load(Ordering::SeqCst),
                session.authority.required_bytes()
            );
            drop(sink);
            assert_eq!(
                payload_drops.load(Ordering::SeqCst),
                0,
                "unknown payload is not silently destructed during fixture retirement"
            );
            assert_eq!(
                session.held_bytes.load(Ordering::SeqCst),
                session.authority.required_bytes()
            );
        }
    }

    #[test]
    fn accepted_slot_followed_by_host_error_still_waits_for_actual_producer_exit() {
        use novarocks_execution_contract::TaskConclusionPort;
        use novarocks_failure_contract::FailureProjection;
        let (session, mut sink, state, port) = input_exit_task_fixture();
        let drops = Arc::new(AtomicUsize::new(0));
        let chunk = foreign_exit_chunk(
            &session,
            drops.clone(),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicUsize::new(0)),
        );
        accept_exit_input(&mut sink, &state, chunk);
        let after_take = input_rejection();
        *session.error_after_take.lock().unwrap() = Some(after_take.clone());
        let error = input_exit_context(&state, |ctx| sink.drive(ctx)).unwrap_err();
        assert_eq!(error.failure_fact(), after_take.failure_fact());
        assert_eq!(error.detail(), after_take.to_string());
        let accepted = port.accepted().unwrap();
        assert!(
            accepted
                .originating_failure()
                .unwrap()
                .same_occurrence(&error)
        );
        assert!(
            session.input.lock().unwrap().is_some(),
            "the real accepted input moved into the host's original slot"
        );
        sink.cancel();
        assert!(
            pending_exit(&mut sink),
            "slot removal acknowledges transfer, never physical exit"
        );
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(
            session.held_bytes.load(Ordering::SeqCst),
            session.authority.required_bytes()
        );
        assert!(port.health.is_healthy());
        session.actual_exit();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_workspace_exited_before_credit(&session);
        assert!(!pending_exit(&mut sink));
        assert!(Arc::ptr_eq(&port.accepted().unwrap(), &accepted));
        assert!(state.error_state().error().unwrap().same_occurrence(&error));
    }
}
