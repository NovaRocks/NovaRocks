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

//! A scan source driven by the driver itself.
//!
//! The scan's one output stream is polled directly by the driver of the
//! scan pipeline: no scan worker, no output queue. The stream's waker only
//! notifies the source observable the driver parks on, and the observation
//! is tied to the poll: its generation is sampled before the poll, so a wake
//! during or after the poll can never be mistaken for the one the driver
//! already consumed.
//!
//! Each chunk passes the scan's conjuncts, runtime filters and LIMIT in that
//! order before it leaves the scan.

// Design: ADR-0159 (docs/adr/ADR-0159-driver-polled-connector-scan-streams.md)

#[cfg(test)]
use crate::exec::chunk::Chunk;
#[cfg(test)]
use crate::exec::node::scan::ScanExitObservation;
use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::runtime::task::{ExecutionFailure, ExecutionResult};

use std::mem::ManuallyDrop;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use futures::future::BoxFuture;
use novarocks_spi::connector::read_stack::ConnectorPollBudget;
use tracing::warn;

use super::output_filter::{
    CompiledOrderedOutputFactory, ScanDriverFilter, ScanLimitDecision, ScanOutputFilter,
    record_rows_read, scan_limit_decision,
};
use crate::exec::node::scan::{ScanExitBodyProof, ScanOp, ScanOutputStream, ScanStreamSource};
use crate::exec::operators::runtime_filter::{CompiledRuntimeFilterConsumers, RuntimeFilterGate};
use crate::exec::pipeline::operator::{FinishWatch, Operator, forward_observable};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::exec::pipeline::shape::{Source, SourceKernel};
use crate::exec::pipeline::step::{NextStep, StepResult, WaitFact, WaitReason};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::OperatorProfiles;
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};
use crate::runtime::scan_stream_metrics::{ScanStreamPending, observe_scan_stream_pending};
use crate::runtime::task::io::{NoopTaskEventSink, TaskEventSink};

/// CPU budget of one driver turn inside a scan stream, in provider work
/// units (roughly one decoded batch each).
#[cfg(test)]
pub(crate) const SCAN_STREAM_TURN_BUDGET: u64 =
    crate::exec::pipeline::driver_step::DEFAULT_TURN_WORK_UNITS as u64;

/// The operator and profile name of a scan source, always carrying the plan
/// node id.
fn scan_source_name(node_id: Option<i32>, op: &dyn ScanOp) -> String {
    let name = op
        .profile_name()
        .unwrap_or_else(|| "ScanSource".to_string());
    if name.contains("plan_node_id=") || name.contains("(id=") {
        return name;
    }
    if let Some(node_id) = node_id {
        // A scan op's profile name template does not carry the plan node id;
        // appending it keeps profile naming consistent.
        return format!("{name} (plan_node_id={node_id})");
    }
    warn!(
        "scan profile name missing plan_node_id and node_id, using plan_node_id=-1: name={}",
        name
    );
    format!("{name} (plan_node_id=-1)")
}

/// Scan runtime-filter waits: the scan-specific timeout, else the general one.
fn scan_runtime_filter_wait_timeout(state: &RuntimeState) -> Duration {
    state
        .runtime_filter_scan_wait_timeout()
        .or_else(|| state.runtime_filter_wait_timeout())
        .unwrap_or(Duration::from_secs(1))
}

/// The waker of a scan stream: it only notifies the scan's source
/// observable, which is what the driver parks on. It never polls anything.
pub(crate) struct SourceReadiness {
    observable: Arc<Observable>,
}

impl SourceReadiness {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            observable: Arc::new(Observable::new()),
        })
    }

    pub(crate) fn observable(&self) -> Arc<Observable> {
        Arc::clone(&self.observable)
    }

    pub(crate) fn generation(&self) -> u64 {
        self.observable.generation()
    }

    pub(crate) fn waker(self: &Arc<Self>) -> Waker {
        Waker::from(Arc::clone(self))
    }
}

impl Wake for SourceReadiness {
    fn wake(self: Arc<Self>) {
        self.observable.notify_observers();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.observable.notify_observers();
    }
}

/// Factory for the scan source of a scan that hands its driver one stream.
pub(crate) struct StreamScanSourceFactory {
    name: String,
    node_id: Option<i32>,
    limit: Option<usize>,
    op: Arc<dyn ScanOp>,
    source: Arc<dyn ScanStreamSource>,
    filter: ScanOutputFilter,
}

impl StreamScanSourceFactory {
    /// The direct ordered lane owns residual/live/membership/limit before
    /// the original whole-chunk handoff; no downstream residual is installed.
    pub(crate) fn new_compiled_ordered(
        display_id: i32,
        op: Arc<dyn ScanOp>,
        runtime_filters: Option<Arc<CompiledRuntimeFilterConsumers>>,
        ordered: crate::exec::operators::runtime_filter::CompiledOrderedScanFactory,
        residual: Option<
            crate::exec::operators::compiled_expression::CompiledFilterProcessorFactory,
        >,
        limit: Option<usize>,
        host: crate::runtime_filter::OrderedTopNAllocationHost,
        control: crate::exec::operators::compiled_expression::RuntimeKernelControl,
    ) -> Self {
        let mut factory = Self::new_compiled(display_id, op, runtime_filters.clone());
        factory.limit = limit;
        factory.filter = ScanOutputFilter::compiled_ordered(
            runtime_filters,
            CompiledOrderedOutputFactory {
                ordered,
                residual,
                host,
                control,
            },
        );
        factory
    }

    /// Build the source of a compiled Scan (local-compiler output). It owns
    /// no legacy expression arena and no conjunct: a compiled residual runs
    /// as a compiled filter after the source, and a compiled scan has no
    /// scan-level limit. Its blocking membership `runtime_filters`, if any,
    /// gate the first read and filter every chunk by their compiled key
    /// roots; every other chunk the stream delivers is handed downstream as
    /// read.
    pub(crate) fn new_compiled(
        display_id: i32,
        op: Arc<dyn ScanOp>,
        runtime_filters: Option<Arc<CompiledRuntimeFilterConsumers>>,
    ) -> Self {
        let source = op.stream_source();
        Self {
            name: scan_source_name(Some(display_id), op.as_ref()),
            node_id: Some(display_id),
            limit: None,
            op,
            source,
            filter: ScanOutputFilter::compiled(runtime_filters),
        }
    }
}

impl OperatorFactory for StreamScanSourceFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let readiness = SourceReadiness::new();
        // A scan held at its runtime-filter gate waits on the same stable
        // source observable as a scan waiting for its stream.
        if let Some(blocking) = self.filter.blocking() {
            forward_observable(&blocking.gate_observable(), &readiness.observable());
        }
        Box::new(Source::new(StreamScanSourceOperator {
            name: self.name.clone(),
            limit: self.limit,
            op: Arc::clone(&self.op),
            source: Arc::clone(&self.source),
            stage: StreamStage::Unclaimed,
            closing: None,
            close_phase: ExitFuturePhase::NotStarted,
            close_result: None,
            actual_exit: None,
            actual_exit_phase: ExitFuturePhase::NotStarted,
            actual_exit_result: None,
            body_proof: None,
            stream_exit: None,
            close_failure_reported: false,
            termination_started: false,
            termination: None,
            exit_panics: ScanExitPanicCustody::default(),
            readiness,
            budget: ConnectorPollBudget::new(),
            filter: self.filter.for_driver(),
            rows_emitted: 0,
            profiles: None,
            event_sink: Arc::new(NoopTaskEventSink),
            runtime_error: None,
            output_tracker_label: format!(
                "scan_stream_output node={} driver={driver_id}",
                self.node_id.unwrap_or(-1)
            ),
            output_tracker: None,
            downstream_paused: false,
            construction: None,
        }))
    }

    fn is_source(&self) -> bool {
        true
    }
}

enum StreamStage {
    /// Not handed over yet: claimed right before the first poll, once the
    /// runtime-filter gate is open.
    Unclaimed,
    Open(ScanOutputStream),
    /// Delivery ended: end of stream, an error, the scan LIMIT, or a stop.
    Ended,
}

/// A call is marked before entering foreign code. If it unwinds, the phase
/// stays at that call and the same hook/future is never entered again.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ExitFuturePhase {
    NotStarted,
    Constructing,
    Pending,
    Polling,
    Dropping,
    Complete,
}

/// The shared Driver catches only this marker as an already-reported exit
/// health fault. It carries no original Any, Task source or foreign capture.
#[derive(Clone, Copy, Debug)]
struct ScanExitUnwind;
pub(crate) fn is_scan_exit_unwind(payload: &(dyn std::any::Any + Send)) -> bool {
    payload.is::<ScanExitUnwind>()
}
fn resume_exit_unwind() -> ! {
    std::panic::resume_unwind(Box::new(ScanExitUnwind))
}

#[derive(Clone, Copy, Debug)]
#[repr(usize)]
enum ScanExitFault {
    ObservationConstruction,
    CloseConstruction,
    ClosePoll,
    CloseDrop,
    ObservationPoll,
    ObservationDrop,
    Termination,
    BackpressureRelease,
}
impl std::fmt::Display for ScanExitFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::ObservationConstruction => "scan exit observation construction panicked",
            Self::CloseConstruction => "scan close construction panicked",
            Self::ClosePoll => "scan close future poll panicked",
            Self::CloseDrop => "scan close future destruction panicked",
            Self::ObservationPoll => "scan exit observation poll panicked",
            Self::ObservationDrop => "scan exit observation destruction panicked",
            Self::Termination => "scan termination hook panicked",
            Self::BackpressureRelease => "scan exit backpressure release panicked",
        })
    }
}
impl std::error::Error for ScanExitFault {}
impl novarocks_failure_contract::FailureProjection for ScanExitFault {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        let code = match self {
            Self::ObservationConstruction => 579,
            Self::CloseConstruction => 580,
            Self::ClosePoll => 581,
            Self::CloseDrop => 582,
            Self::ObservationPoll => 583,
            Self::ObservationDrop => 584,
            Self::Termination => 585,
            Self::BackpressureRelease => 586,
        };
        FailureFact::new(
            FailureClass::Unexpected,
            FailureReason::new(domain::EXECUTION, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}
type ScanPanicPayload = Box<dyn std::any::Any + Send + 'static>;
// A closed phase list: one constructor/poll/drop per future, one termination,
// one exit backpressure release. No append, arbitrary threshold or global park.
const SCAN_EXIT_PHASES: usize = 8;
#[derive(Default)]
struct ScanExitPanicCustody {
    attempted: [bool; SCAN_EXIT_PHASES],
    secondary: [Option<ManuallyDrop<ScanPanicPayload>>; SCAN_EXIT_PHASES],
}
impl ScanExitPanicCustody {
    fn record(
        &mut self,
        kind: ScanExitFault,
        payload: ScanPanicPayload,
        failures: &RuntimeErrorState,
    ) {
        let index = kind as usize;
        assert!(
            !self.attempted[index],
            "an unwound scan exit phase is never retried"
        );
        self.attempted[index] = true;
        let cause = ExecutionFailure::from_domain(kind);
        // RuntimeErrorState projects to the exact accepted Task's original
        // health owner (or its explicit local owner), never a Task conclusion.
        failures.report_cleanup(&cause);
        failures.report_unresolved_exit(&cause);
        // The original Any is an independent once cleanup. A secondary opaque
        // destructor panic remains in this same scan/Driver continuation.
        if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
            self.secondary[index] = Some(ManuallyDrop::new(secondary));
        }
    }
    fn has_unknown_payload(&self) -> bool {
        self.secondary.iter().any(Option::is_some)
    }
}
fn catch_exit_action<T>(
    custody: &mut ScanExitPanicCustody,
    failures: &RuntimeErrorState,
    kind: ScanExitFault,
    action: impl FnOnce() -> T,
) -> Result<T, ScanExitUnwind> {
    catch_unwind(AssertUnwindSafe(action)).map_err(|payload| {
        custody.record(kind, payload, failures);
        ScanExitUnwind
    })
}

fn poll_exit_observation<T: Clone>(
    future: &mut Option<BoxFuture<'static, T>>,
    phase: &mut ExitFuturePhase,
    ready_result: &mut Option<T>,
    cx: &mut Context<'_>,
) -> Option<T> {
    if *phase != ExitFuturePhase::Pending {
        return None;
    }
    *phase = ExitFuturePhase::Polling;
    match future
        .as_mut()
        .expect("pending exit has its original future")
        .as_mut()
        .poll(cx)
    {
        Poll::Pending => {
            *phase = ExitFuturePhase::Pending;
            None
        }
        Poll::Ready(result) => {
            // Keep the known result in the original owner before the foreign
            // Future destructor. Unwind cannot implicitly destroy its source.
            *ready_result = Some(result);
            *phase = ExitFuturePhase::Dropping;
            drop(future.take());
            *phase = ExitFuturePhase::Complete;
            ready_result.clone()
        }
    }
}
fn poll_exit_component<T: Clone>(
    future: &mut Option<BoxFuture<'static, T>>,
    phase: &mut ExitFuturePhase,
    ready_result: &mut Option<T>,
    custody: &mut ScanExitPanicCustody,
    failures: &RuntimeErrorState,
    observation: bool,
    cx: &mut Context<'_>,
) -> bool {
    match catch_unwind(AssertUnwindSafe(|| {
        poll_exit_observation(future, phase, ready_result, cx)
    })) {
        Ok(_) => false,
        Err(payload) => {
            let kind = match (observation, *phase) {
                (false, ExitFuturePhase::Dropping) => ScanExitFault::CloseDrop,
                (true, ExitFuturePhase::Dropping) => ScanExitFault::ObservationDrop,
                (false, _) => ScanExitFault::ClosePoll,
                (true, _) => ScanExitFault::ObservationPoll,
            };
            custody.record(kind, payload, failures);
            true
        }
    }
}

struct StreamScanSourceOperator {
    name: String,
    /// Scan-level LIMIT: delivery ends once this many rows were emitted.
    limit: Option<usize>,
    op: Arc<dyn ScanOp>,
    source: Arc<dyn ScanStreamSource>,
    stage: StreamStage,
    /// The exit observation of an ended stream, until it resolves.
    closing: Option<BoxFuture<'static, crate::runtime::task::ExecutionResult<()>>>,
    close_phase: ExitFuturePhase,
    close_result: Option<ExecutionResult<()>>,
    /// Acquired before close consumes the stream; retained across a close panic.
    actual_exit: Option<BoxFuture<'static, crate::exec::node::scan::ScanExitProof>>,
    actual_exit_phase: ExitFuturePhase,
    actual_exit_result: Option<crate::exec::node::scan::ScanExitProof>,
    body_proof: Option<ScanExitBodyProof>,
    /// The immutable outcome remains observable after consuming the close future.
    stream_exit: Option<crate::exec::node::scan::ScanExitProof>,
    close_failure_reported: bool,
    termination_started: bool,
    termination: Option<ExecutionResult<()>>,
    exit_panics: ScanExitPanicCustody,
    readiness: Arc<SourceReadiness>,
    budget: ConnectorPollBudget,
    filter: ScanDriverFilter,
    rows_emitted: usize,
    profiles: Option<OperatorProfiles>,
    event_sink: Arc<dyn TaskEventSink>,
    runtime_error: Option<Arc<RuntimeErrorState>>,
    output_tracker_label: String,
    /// Charges every chunk this source hands downstream to the fragment.
    output_tracker: Option<Arc<MemTracker>>,
    downstream_paused: bool,
    construction: Option<Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
}

impl StreamScanSourceOperator {
    /// Ends delivery: the stream is closed, which stops its operations
    /// before returning, and only its exit observation is kept.
    fn end_delivery(&mut self) -> bool {
        if !matches!(self.stage, StreamStage::Open(_)) && !self.downstream_paused {
            self.stage = StreamStage::Ended;
            return false;
        }
        let failures = self
            .runtime_error
            .as_ref()
            .expect("a bound scan owns its runtime failure state")
            .clone();
        let mut unwound = false;
        if let StreamStage::Open(stream) = std::mem::replace(&mut self.stage, StreamStage::Ended) {
            // Even an observation constructor fault cannot skip the independent
            // once stop/close of the still-owned original stream.
            self.close_phase = ExitFuturePhase::Constructing;
            match catch_exit_action(
                &mut self.exit_panics,
                &failures,
                ScanExitFault::CloseConstruction,
                || stream.close(failures.clone()),
            ) {
                Ok(closing) => {
                    self.closing = Some(closing);
                    self.close_phase = ExitFuturePhase::Pending;
                }
                Err(_) => unwound = true,
            }
        }
        if self.downstream_paused {
            self.downstream_paused = false;
            unwound |= catch_exit_action(
                &mut self.exit_panics,
                &failures,
                ScanExitFault::BackpressureRelease,
                || self.op.on_output_backpressure(false),
            )
            .is_err();
        }
        if unwound {
            let (termination, _) = self.request_termination_inner();
            if let Err(cause) = termination {
                failures.report_cleanup(&cause);
            }
        }
        // Data authors must still return their original output/EOS/read error.
        // Only terminal lifecycle callers propagate the already-reported marker.
        unwound
    }

    fn poll_actual_exit(&mut self) -> bool {
        let Some(failures) = self.runtime_error.as_ref().cloned() else {
            return false;
        };
        let waker = self.readiness.waker();
        let mut cx = Context::from_waker(&waker);
        poll_exit_component(
            &mut self.actual_exit,
            &mut self.actual_exit_phase,
            &mut self.actual_exit_result,
            &mut self.exit_panics,
            &failures,
            true,
            &mut cx,
        )
    }

    fn set_downstream_paused(&mut self, paused: bool) {
        if self.downstream_paused != paused {
            self.downstream_paused = paused;
            self.op.on_output_backpressure(paused);
        }
    }

    fn request_termination_inner(&mut self) -> (ExecutionResult<()>, bool) {
        let mut unwound = false;
        if !self.termination_started {
            self.termination_started = true;
            let failures = self
                .runtime_error
                .as_ref()
                .expect("a bound scan owns its runtime failure state")
                .clone();
            match catch_exit_action(
                &mut self.exit_panics,
                &failures,
                ScanExitFault::Termination,
                || self.op.terminate(),
            ) {
                Ok(result) => self.termination = Some(result),
                Err(_) => unwound = true,
            }
        }
        (self.termination.clone().unwrap_or(Ok(())), unwound)
    }
}

impl OperatorLifecycle for StreamScanSourceOperator {
    fn bind_poll_budget(&mut self, budget: ConnectorPollBudget) {
        self.budget = budget;
    }

    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(
        &mut self,
        factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) {
        self.filter.bind_construction_factory(Arc::clone(&factory));
        self.construction = Some(factory);
    }

    fn set_mem_tracker(&mut self, tracker: Arc<crate::runtime::mem_tracker::MemTracker>) {
        self.filter.bind_mem_tracker(tracker);
    }
    fn name(&self) -> &str {
        &self.name
    }

    fn set_profiles(&mut self, profiles: OperatorProfiles) {
        self.profiles = Some(profiles);
    }

    fn set_task_event_sink(&mut self, sink: Arc<dyn TaskEventSink>) {
        self.event_sink = sink;
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        // Bind the original failure owner before any later fallible binding;
        // partial preparation cleanup must not mint a separate health latch.
        self.runtime_error = Some(state.error_state());
        self.filter.bind_runtime_state(state)?;
        if let Some(consumers) = self.filter.blocking() {
            consumers.bind(state)?;
            consumers.set_wait_timeout(scan_runtime_filter_wait_timeout(state));
        }
        self.output_tracker = state
            .mem_tracker()
            .map(|root| MemTracker::new_child(self.output_tracker_label.clone(), &root));
        Ok(())
    }

    fn begin_exit(&mut self) -> ExecutionResult<()> {
        let abandoned_delivery = !matches!(self.stage, StreamStage::Ended);
        let mut unwound = self.end_delivery();
        let result = if abandoned_delivery {
            let (result, termination_unwound) = self.request_termination_inner();
            unwound |= termination_unwound;
            result
        } else {
            Ok(())
        };
        if unwound {
            resume_exit_unwind();
        }
        result
    }
    fn close(&mut self) -> ExecutionResult<()> {
        Ok(())
    }

    /// Ends delivery and starts the scan's terminal cleanup, which stops
    /// what its stream does not own itself, such as queued work.
    fn cancel(&mut self) {
        let mut unwound = self.end_delivery();
        let (termination, termination_unwound) = self.request_termination_inner();
        unwound |= termination_unwound;
        if let Err(error) = termination
            && let Some(signal) = &self.runtime_error
        {
            signal.report_cleanup(&error);
        }
        if unwound {
            resume_exit_unwind();
        }
    }

    /// An ended stream is observed until its operations exited, so its
    /// driver completes only after the real exit and reports its error.
    fn pending_finish(&self) -> Option<FinishWatch> {
        (self.stream_exit.is_none()
            && (self.close_phase != ExitFuturePhase::NotStarted
                || (self.termination_started && self.termination.is_none())))
        .then(|| FinishWatch::Notify(self.readiness.observable()))
    }
    fn observe_exit(&mut self) -> crate::exec::pipeline::operator::ExitObservation {
        use crate::exec::node::scan::ScanExitProof;
        use crate::exec::pipeline::operator::{ExitObservation, FinishWatch};
        let readiness = self.readiness.clone();
        let watch = || FinishWatch::Notify(readiness.observable());
        if self.stream_exit == Some(ScanExitProof::Exited) {
            return ExitObservation::exited();
        }
        let observation_unwound = self.poll_actual_exit();
        if self.close_phase == ExitFuturePhase::NotStarted
            && !self.exit_panics.has_unknown_payload()
            && (!self.termination_started || self.termination.is_some())
        {
            if observation_unwound {
                resume_exit_unwind();
            }
            // No exit obligation has started. Never cache this as physical exit.
            return if self.actual_exit_result == Some(ScanExitProof::Unresolved) {
                ExitObservation::unresolved(watch())
            } else {
                ExitObservation::exited()
            };
        }
        let waker = self.readiness.waker();
        let mut context = Context::from_waker(&waker);
        let failures = self
            .runtime_error
            .as_ref()
            .expect("bound scan exit owner")
            .clone();
        let close_unwound = poll_exit_component(
            &mut self.closing,
            &mut self.close_phase,
            &mut self.close_result,
            &mut self.exit_panics,
            &failures,
            false,
            &mut context,
        );
        if !self.close_failure_reported
            && let Some(Err(cause)) = &self.close_result
        {
            self.close_failure_reported = true;
            failures.report_cleanup(cause);
        }
        if close_unwound || observation_unwound {
            resume_exit_unwind();
        }
        let termination_known = !self.termination_started || self.termination.is_some();
        let never_claimed = self.close_phase == ExitFuturePhase::NotStarted;
        let body_exited = self.close_phase == ExitFuturePhase::Complete
            || (self.body_proof == Some(ScanExitBodyProof::Included)
                && self.close_phase == ExitFuturePhase::Constructing);
        let opaque_exited = !self.exit_panics.has_unknown_payload();
        let actual_exited = self.actual_exit_phase == ExitFuturePhase::Complete
            && self.actual_exit_result == Some(ScanExitProof::Exited);
        if opaque_exited && termination_known && (never_claimed || (body_exited && actual_exited)) {
            self.stream_exit = Some(ScanExitProof::Exited);
            return ExitObservation::exited();
        }
        let unknown = !termination_known
            || self.actual_exit_result == Some(ScanExitProof::Unresolved)
            || !opaque_exited
            || matches!(
                self.close_phase,
                ExitFuturePhase::Constructing
                    | ExitFuturePhase::Polling
                    | ExitFuturePhase::Dropping
            )
            || matches!(
                self.actual_exit_phase,
                ExitFuturePhase::Constructing
                    | ExitFuturePhase::Polling
                    | ExitFuturePhase::Dropping
            );
        if unknown {
            ExitObservation::unresolved(watch())
        } else {
            ExitObservation::pending(watch())
        }
    }
}

impl SourceKernel for StreamScanSourceOperator {
    fn control_observable(&self) -> Option<Arc<crate::runtime::observable::Observable>> {
        Some(self.readiness.observable())
    }
    fn poll_control(
        &mut self,
        ctx: &mut crate::exec::pipeline::driver_step::DriverStep<'_>,
    ) -> ExecutionResult<crate::exec::pipeline::shape::ControlStep> {
        if self.poll_actual_exit() {
            resume_exit_unwind();
        }
        ctx.checkpoint()?;
        Ok(crate::exec::pipeline::shape::ControlStep::Active)
    }
    fn poll_output(
        &mut self,
        ctx: &mut crate::exec::pipeline::driver_step::DriverStep<'_>,
    ) -> ExecutionResult<StepResult> {
        if matches!(self.stage, StreamStage::Ended) {
            return Ok(StepResult::new(None, NextStep::Finished, 0));
        }
        // The gate's wait starts where the scan would read its first page.
        let gate_generation = self.readiness.generation();
        if let Some(consumers) = self.filter.blocking()
            && !matches!(consumers.poll_gate(), RuntimeFilterGate::Open)
        {
            return Ok(StepResult::new(
                None,
                NextStep::Wait(WaitFact {
                    reason: WaitReason::Dependency,
                    observable: self.readiness.observable(),
                    observed_generation: gate_generation,
                    deadline: consumers.gate_deadline(),
                }),
                0,
            ));
        }
        if matches!(self.stage, StreamStage::Unclaimed) {
            let profile = self
                .profiles
                .as_ref()
                .map(|profiles| profiles.unique.clone());
            let construction = self
                .construction
                .as_ref()
                .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
            self.stage = StreamStage::Open(self.source.claim(
                self.budget.clone(),
                profile,
                Arc::clone(construction),
            )?);
            self.actual_exit_phase = ExitFuturePhase::Constructing;
            let failures = self
                .runtime_error
                .as_ref()
                .expect("bound scan source")
                .clone();
            let StreamStage::Open(stream) = &mut self.stage else {
                unreachable!()
            };
            let acquired = catch_exit_action(
                &mut self.exit_panics,
                &failures,
                ScanExitFault::ObservationConstruction,
                || stream.as_mut().take_exit_observation(failures.clone()),
            );
            match acquired {
                Ok(Ok(observation)) => {
                    self.body_proof = Some(observation.body_proof);
                    self.actual_exit = Some(observation.actual_exit);
                    self.actual_exit_phase = ExitFuturePhase::Pending;
                }
                Ok(Err(cause)) => {
                    failures.report_unresolved_exit(&cause);
                    return Err(cause);
                }
                Err(_) => resume_exit_unwind(),
            }
        }
        if self.poll_actual_exit() {
            resume_exit_unwind();
        }
        ctx.checkpoint()?;
        let waker = self.readiness.waker();
        let mut context = Context::from_waker(&waker);
        loop {
            let StreamStage::Open(stream) = &mut self.stage else {
                return Ok(StepResult::new(None, NextStep::Finished, 0));
            };
            // Sampled before the poll: a wake during or after it moves the
            // generation past this value, so it cannot be lost.
            let generation = self.readiness.generation();
            let exhaustions = self.budget.exhaustions();
            match stream.as_mut().poll_next(&mut context) {
                Poll::Pending => {
                    let yielded = self.budget.exhaustions() > exhaustions;
                    observe_scan_stream_pending(if yielded {
                        ScanStreamPending::BudgetYield
                    } else {
                        ScanStreamPending::Wait
                    });
                    let next = if yielded {
                        NextStep::Yield
                    } else {
                        NextStep::Wait(WaitFact {
                            reason: WaitReason::Input,
                            observable: self.readiness.observable(),
                            observed_generation: generation,
                            deadline: None,
                        })
                    };
                    return Ok(StepResult::new(None, next, 0));
                }
                Poll::Ready(None) => {
                    let _already_reported_exit_unwind = self.end_delivery();
                    return Ok(StepResult::new(None, NextStep::Finished, 0));
                }
                Poll::Ready(Some(Err(error))) => {
                    // Provider close may run synchronous stop hooks. Publish
                    // the actual read cause before starting that cleanup.
                    ctx.report_failure(error.clone());
                    let _already_reported_exit_unwind = self.end_delivery();
                    return Err(error);
                }
                Poll::Ready(Some(Ok(mut chunk))) => {
                    if self.filter.requires_input_accounting() {
                        let tracker = self.output_tracker.as_ref().ok_or(
                            crate::exec::pipeline::operator::ConstructionFailure::MissingOwner,
                        )?;
                        // Use the original whole-source author before any
                        // ordered transformation borrows or retains this input.
                        // An existing provider charge remains its sole owner.
                        chunk.try_transfer_to(tracker).map_err(
                            crate::exec::pipeline::operator::ConstructionFailure::Memory,
                        )?;
                    }
                    let Some(chunk) =
                        self.filter
                            .apply(chunk, self.profiles.as_ref(), &self.event_sink)?
                    else {
                        continue;
                    };
                    let Some(chunk) =
                        self.filter
                            .restrict_limit(chunk, self.limit, self.rows_emitted)?
                    else {
                        let _already_reported_exit_unwind = self.end_delivery();
                        return Ok(StepResult::new(None, NextStep::Finished, 0));
                    };
                    let rows = chunk.len();
                    match scan_limit_decision(self.limit, self.rows_emitted, rows) {
                        ScanLimitDecision::Stop => {
                            let _already_reported_exit_unwind = self.end_delivery();
                            return Ok(StepResult::new(None, NextStep::Finished, 0));
                        }
                        ScanLimitDecision::EmitThenStop => {
                            self.rows_emitted = self.rows_emitted.saturating_add(rows);
                            let _already_reported_exit_unwind = self.end_delivery();
                        }
                        ScanLimitDecision::Emit => {
                            self.rows_emitted = self.rows_emitted.saturating_add(rows);
                        }
                    }
                    record_rows_read(self.profiles.as_ref(), rows);
                    self.op.on_nonempty_chunk_consumed();
                    let mut chunk = chunk;
                    if let Some(tracker) = self.output_tracker.as_ref() {
                        chunk.transfer_to(tracker);
                    }
                    let next = if matches!(self.stage, StreamStage::Ended) {
                        NextStep::Finished
                    } else {
                        NextStep::Runnable
                    };
                    return Ok(StepResult::new(Some(chunk), next, 0));
                }
            }
        }
    }
    fn on_downstream_backpressure(&mut self, paused: bool) {
        if !matches!(self.stage, StreamStage::Ended) {
            self.set_downstream_paused(paused);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, mpsc};
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use futures::Stream;
    use futures::future::BoxFuture;
    use novarocks_spi::connector::read_stack::{
        BudgetConsume, ConnectorPollBudget, ConnectorSourceOperations,
    };
    use novarocks_types::SlotId;

    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::node::scan::{ScanChunkStream, ScanOp};
    use crate::exec::operators::local_exchanger::LocalExchanger;
    use crate::exec::operators::{LocalExchangeSinkFactory, LocalExchangeSourceFactory};
    use crate::exec::pipeline::driver::{DriverState, PipelineDriver};
    use crate::exec::pipeline::operator::BlockedReason;

    fn one_row(value: i32) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![value])) as ArrayRef],
        )
        .expect("record batch");
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1)],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    fn value_of(chunk: &Chunk) -> i32 {
        chunk.columns()[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 column")
            .value(0)
    }

    /// What the scripted stream does, shared with the test.
    #[derive(Default)]
    struct StreamControl {
        chunks: Mutex<VecDeque<Chunk>>,
        error: Mutex<Option<crate::runtime::task::ExecutionFailure>>,
        close_check: Mutex<Option<Box<dyn FnOnce() + Send>>>,
        observation_check: Mutex<Option<Box<dyn FnOnce() + Send>>>,
        termination_check: Mutex<Option<Box<dyn FnOnce() + Send>>>,
        backpressure_release_check: Mutex<Option<Box<dyn FnOnce() + Send>>>,
        ended: AtomicBool,
        waker: Mutex<Option<Waker>>,
        /// On the next empty poll, make `staged` available and wake the
        /// task before returning `Pending`, as work finishing mid-poll does.
        wake_inside_poll: AtomicBool,
        staged: Mutex<Option<Chunk>>,
        polls: AtomicUsize,
        close_requested: AtomicBool,
        /// Exercises a stream whose work body is not covered by its ledger.
        requires_close_body: AtomicBool,
        operations: ConnectorSourceOperations,
        terminations: AtomicUsize,
    }

    impl StreamControl {
        fn push(&self, chunk: Chunk) {
            self.chunks.lock().expect("chunks").push_back(chunk);
        }

        fn wake(&self) {
            if let Some(waker) = self.waker.lock().expect("waker").take() {
                waker.wake();
            }
        }
    }

    struct ScriptedStream {
        control: Arc<StreamControl>,
        budget: ConnectorPollBudget,
        /// One unit of budget is spent on every chunk before it is handed out.
        spending: Option<BudgetConsume>,
    }

    impl Stream for ScriptedStream {
        type Item = ExecutionResult<Chunk>;

        fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let this = self.get_mut();
            this.control.polls.fetch_add(1, Ordering::AcqRel);
            if let Some(error) = this.control.error.lock().unwrap().take() {
                return Poll::Ready(Some(Err(error)));
            }
            if this.control.chunks.lock().expect("chunks").is_empty() {
                if this.control.ended.load(Ordering::Acquire) {
                    return Poll::Ready(None);
                }
                *this.control.waker.lock().expect("waker") = Some(context.waker().clone());
                if this.control.wake_inside_poll.swap(false, Ordering::AcqRel) {
                    if let Some(chunk) = this.control.staged.lock().expect("staged").take() {
                        this.control.push(chunk);
                    }
                    context.waker().wake_by_ref();
                }
                return Poll::Pending;
            }
            let spending = this.spending.get_or_insert_with(|| this.budget.consume(1));
            if Pin::new(spending).poll(context).is_pending() {
                return Poll::Pending;
            }
            this.spending = None;
            let chunk = this
                .control
                .chunks
                .lock()
                .expect("chunks")
                .pop_front()
                .expect("a chunk was available");
            Poll::Ready(Some(Ok(chunk)))
        }
    }

    async fn observe_fixture_source(
        operations: novarocks_spi::connector::read_stack::ConnectorSourceOperations,
        failures: Arc<dyn crate::runtime::task::ExecutionFailureReporter>,
    ) -> crate::exec::node::scan::ScanExitProof {
        use novarocks_spi::connector::read_stack::{
            ConnectorSourceExitProof, ConnectorSourceFailureEvent,
        };
        let mut events = operations
            .claim_failure_receiver()
            .expect("one fixture source receiver");
        let mut exit = Box::pin(operations.exited());
        let mut events_ended = false;
        futures::future::poll_fn(move |cx| {
            if !events_ended { loop { match events.poll_event(cx) {
                Poll::Ready(Some(ConnectorSourceFailureEvent::Required(cause))) => failures.report_failure(
                    crate::runtime::task::connector_failure::ConnectorCallFailure::new(
                        crate::runtime::task::connector_failure::ConnectorOperation::SourceExit,cause).into()),
                Poll::Ready(Some(ConnectorSourceFailureEvent::CleanupUnresolved(cause))) => {
                    let cause = crate::runtime::task::connector_failure::ConnectorCallFailure::new(
                        crate::runtime::task::connector_failure::ConnectorOperation::SourceExit,cause).into();
                    failures.report_cleanup(&cause); failures.report_unresolved_exit(&cause);
                }
                Poll::Ready(None) => {events_ended=true;break;}, Poll::Pending=>break,
            }}}
            match exit.as_mut().poll(cx) {
                Poll::Ready(proof) if events_ended => Poll::Ready(match proof {
                    ConnectorSourceExitProof::Exited=>crate::exec::node::scan::ScanExitProof::Exited,
                    ConnectorSourceExitProof::Unresolved=>crate::exec::node::scan::ScanExitProof::Unresolved,
                }),
                _=>Poll::Pending,
            }
        }).await
    }
    impl ScanChunkStream for ScriptedStream {
        fn take_exit_observation(
            self: Pin<&mut Self>,
            failures: Arc<dyn crate::runtime::task::ExecutionFailureReporter>,
        ) -> ExecutionResult<ScanExitObservation> {
            let check = self.control.observation_check.lock().unwrap().take();
            if let Some(check) = check {
                check();
            }
            Ok(ScanExitObservation {
                body_proof: if self.control.requires_close_body.load(Ordering::Acquire) {
                    ScanExitBodyProof::CloseBodyRequired
                } else {
                    ScanExitBodyProof::Included
                },
                actual_exit: Box::pin(observe_fixture_source(
                    self.control.operations.clone(),
                    failures,
                )),
            })
        }
        fn close(
            self: Pin<Box<Self>>,
            _failures: Arc<dyn crate::runtime::task::ExecutionFailureReporter>,
        ) -> BoxFuture<'static, crate::runtime::task::ExecutionResult<()>> {
            let check = self.control.close_check.lock().unwrap().take();
            if let Some(check) = check {
                check();
            }
            self.control.close_requested.store(true, Ordering::Release);
            self.control.operations.seal();
            let exit = self.control.operations.exited();
            Box::pin(async move {
                match exit.await {
                    novarocks_spi::connector::read_stack::ConnectorSourceExitProof::Exited => {
                        Ok(())
                    }
                    novarocks_spi::connector::read_stack::ConnectorSourceExitProof::Unresolved => {
                        futures::future::pending().await
                    }
                }
            })
        }
    }

    fn actual_test_factory() -> Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory> {
        crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            MemTracker::new_root("scan stream fixture creator"),
            Arc::new(RuntimeErrorState::default()),
        )
        .unwrap()
    }
    struct ScriptedSource {
        control: Arc<StreamControl>,
        claims: AtomicUsize,
    }

    impl ScanStreamSource for ScriptedSource {
        fn claim(
            &self,
            budget: ConnectorPollBudget,
            _profile: Option<crate::runtime::profile::RuntimeProfile>,
            _construction: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
        ) -> ExecutionResult<ScanOutputStream> {
            if self.claims.fetch_add(1, Ordering::AcqRel) > 0 {
                return Err(novarocks_functions::KernelFailure::Internal(
                    novarocks_functions::KernelDiagnostic::new("scan stream already claimed"),
                )
                .into());
            }
            Ok(Box::pin(ScriptedStream {
                control: Arc::clone(&self.control),
                budget,
                spending: None,
            }))
        }
    }

    struct StreamScanOp {
        source: Arc<ScriptedSource>,
        backpressure: Arc<Mutex<Vec<bool>>>,
    }

    impl ScanOp for StreamScanOp {
        fn on_output_backpressure(&self, paused: bool) {
            self.backpressure.lock().expect("backpressure").push(paused);
            if !paused {
                let check = self
                    .source
                    .control
                    .backpressure_release_check
                    .lock()
                    .unwrap()
                    .take();
                if let Some(check) = check {
                    check();
                }
            }
        }

        fn terminate(&self) -> crate::runtime::task::ExecutionResult<()> {
            self.source
                .control
                .terminations
                .fetch_add(1, Ordering::AcqRel);
            self.source.control.operations.seal();
            let check = self.source.control.termination_check.lock().unwrap().take();
            if let Some(check) = check {
                check();
            }
            Ok(())
        }

        fn stream_source(&self) -> Arc<dyn ScanStreamSource> {
            Arc::clone(&self.source) as Arc<dyn ScanStreamSource>
        }
    }

    struct CollectSink {
        values: Arc<Mutex<Vec<i32>>>,
        on_finish: Option<Box<dyn FnOnce() + Send>>,
    }
    impl OperatorLifecycle for CollectSink {
        fn name(&self) -> &str {
            "COLLECT_SINK"
        }
    }
    impl crate::exec::pipeline::operator::InputEncodingPolicy for CollectSink {}
    impl crate::exec::pipeline::shape::SinkKernel for CollectSink {
        fn consume(
            &mut self,
            _: &mut crate::exec::pipeline::driver_step::DriverStep<'_>,
            input: crate::exec::pipeline::input::PreparedInput,
        ) -> ExecutionResult<crate::exec::pipeline::shape::SinkStep> {
            let chunk = input.into_uncovered_chunk()?;
            self.values.lock().expect("values").push(value_of(&chunk));
            Ok(crate::exec::pipeline::shape::SinkStep::new(
                NextStep::NeedInput,
                chunk.len() as u64,
            ))
        }
        fn finish(
            &mut self,
            _: &mut crate::exec::pipeline::driver_step::DriverStep<'_>,
        ) -> ExecutionResult<crate::exec::pipeline::shape::SinkStep> {
            if let Some(finish) = self.on_finish.take() {
                finish();
            }
            Ok(crate::exec::pipeline::shape::SinkStep::new(
                NextStep::Finished,
                0,
            ))
        }
    }

    type ObservedStreamScanSource = (
        Box<dyn Operator>,
        Arc<ScriptedSource>,
        Arc<Mutex<Vec<bool>>>,
    );

    fn stream_scan_source(
        control: &Arc<StreamControl>,
    ) -> (Box<dyn Operator>, Arc<ScriptedSource>) {
        let (operator, source, _) = observed_stream_scan_source(control);
        (operator, source)
    }

    fn observed_stream_scan_source(control: &Arc<StreamControl>) -> ObservedStreamScanSource {
        observed_stream_scan_source_in(control, &RuntimeState::default())
    }

    fn observed_stream_scan_source_in(
        control: &Arc<StreamControl>,
        state: &RuntimeState,
    ) -> ObservedStreamScanSource {
        observed_stream_scan_source_in_with_limit(control, state, None)
    }

    fn observed_stream_scan_source_in_with_limit(
        control: &Arc<StreamControl>,
        state: &RuntimeState,
        limit: Option<usize>,
    ) -> ObservedStreamScanSource {
        let source = Arc::new(ScriptedSource {
            control: Arc::clone(control),
            claims: AtomicUsize::new(0),
        });
        let backpressure = Arc::new(Mutex::new(Vec::new()));
        let op: Arc<dyn ScanOp> = Arc::new(StreamScanOp {
            source: Arc::clone(&source),
            backpressure: Arc::clone(&backpressure),
        });
        let mut factory = StreamScanSourceFactory::new_compiled(7, op, None);
        factory.limit = limit;
        let mut operator = factory.create(1, 0);
        operator.set_construction_factory(actual_test_factory());
        operator
            .bind_runtime_state(state)
            .expect("bind stream scan");
        (operator, source, backpressure)
    }

    struct Pipeline {
        state: Arc<RuntimeState>,
        driver: PipelineDriver,
        control: Arc<StreamControl>,
        source: Arc<ScriptedSource>,
        values: Arc<Mutex<Vec<i32>>>,
    }

    fn pipeline() -> Pipeline {
        let state = Arc::new(RuntimeState::default());
        let control = Arc::new(StreamControl::default());
        let (scan, source, _) = observed_stream_scan_source_in(&control, &state);
        let values = Arc::new(Mutex::new(Vec::new()));
        let driver = PipelineDriver::new(
            1,
            vec![
                scan,
                Box::new(crate::exec::pipeline::shape::Transform::hydrating(
                    CollectSink {
                        values: Arc::clone(&values),
                        on_finish: None,
                    },
                )),
            ],
            None,
            Vec::new(),
            state.clone(),
            None,
        );
        Pipeline {
            state,
            driver,
            control,
            source,
            values,
        }
    }

    const TURN: Duration = Duration::from_secs(5);

    /// Runs turns while the driver asks to be requeued, as the scheduler does.
    fn run_until_parked(driver: &mut PipelineDriver) -> DriverState {
        for _ in 0..16 {
            let state = driver.process(TURN);
            if !matches!(state, DriverState::Ready) {
                return state;
            }
        }
        panic!("the driver kept asking for turns without progress");
    }

    #[test]
    fn an_exited_operation_scope_cannot_replace_an_unknown_reader_body() {
        use crate::exec::pipeline::operator::ExitProgress;
        let control = Arc::new(StreamControl::default());
        control.requires_close_body.store(true, Ordering::Release);
        let (mut scan, _) = stream_scan_source(&control);
        let primed = scan.drive_for_test(&RuntimeState::default());
        let entered = Arc::new(AtomicUsize::new(0));
        *control.close_check.lock().unwrap() = Some(Box::new({
            let entered = entered.clone();
            move || {
                entered.fetch_add(1, Ordering::AcqRel);
                panic!("unknown reader close body");
            }
        }));
        let begin = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| scan.begin_exit()));
        let first_pending = matches!(scan.observe_exit().progress, ExitProgress::Unresolved(_));
        let second_begin = scan.begin_exit();
        let second_pending = matches!(scan.observe_exit().progress, ExitProgress::Unresolved(_));
        let actual_scope_exited = control.operations.is_exited();
        let calls = entered.load(Ordering::Acquire);
        drop(scan);
        assert!(primed.is_ok());
        assert!(begin.is_err());
        assert!(second_begin.is_ok());
        assert!(actual_scope_exited, "the real ledger is sealed and empty");
        assert_eq!(calls, 1, "cleanup must not retry the consumed close hook");
        assert!(
            first_pending && second_pending,
            "neither an error nor an exited ledger proves an unknown reader body exited"
        );
    }

    #[test]
    fn a_panicked_exit_observation_is_not_repolled_or_released_as_completed() {
        // The same future retains its real captured data after poll unwinds;
        // a subsequent cleanup observation must not reenter foreign code.
        let captured = Arc::new(vec![7_u8; 1024]);
        let weak = Arc::downgrade(&captured);
        let calls = Arc::new(AtomicUsize::new(0));
        let poll_calls = calls.clone();
        let mut future: Option<BoxFuture<'static, ExecutionResult<()>>> =
            Some(Box::pin(futures::future::poll_fn(move |_| {
                let _live_data = captured.len();
                poll_calls.fetch_add(1, Ordering::AcqRel);
                panic!("actual exit observer poll panic");
                #[allow(unreachable_code)]
                Poll::Ready(Ok(()))
            })));
        let mut phase = ExitFuturePhase::Pending;
        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        let mut ready_result = None;
        let first = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            poll_exit_observation(&mut future, &mut phase, &mut ready_result, &mut context)
        }));
        let retained_after_panic = weak.upgrade().is_some();
        let second =
            poll_exit_observation(&mut future, &mut phase, &mut ready_result, &mut context);
        let unresolved = phase == ExitFuturePhase::Polling;
        let calls = calls.load(Ordering::Acquire);
        drop(future);
        let released_on_actual_drop = weak.upgrade().is_none();
        assert!(first.is_err());
        assert!(retained_after_panic);
        assert!(second.is_none());
        assert!(unresolved);
        assert_eq!(calls, 1);
        assert!(released_on_actual_drop);
    }

    struct SecondaryScanPayload {
        drops: Arc<AtomicUsize>,
        _actual_capture: Arc<Vec<u8>>,
    }
    impl Drop for SecondaryScanPayload {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct OriginalScanPayload {
        drops: Arc<AtomicUsize>,
        secondary: Option<SecondaryScanPayload>,
    }
    impl Drop for OriginalScanPayload {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            if let Some(secondary) = self.secondary.take() {
                std::panic::panic_any(secondary);
            }
        }
    }
    fn explicit_scan_fact(code: u16) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        FailureFact::new(
            FailureClass::Unexpected,
            FailureReason::new(domain::EXECUTION, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
    #[test]
    fn close_unwind_reports_original_health_without_selecting_a_task_failure() {
        use crate::exec::pipeline::operator::ExitProgress;
        let state = RuntimeState::default();
        let failures = state.error_state();
        let control = Arc::new(StreamControl::default());
        let (mut scan, _, _) = observed_stream_scan_source_in(&control, &state);
        let primed = scan.drive_for_test(&state);
        let primary = Arc::new(AtomicUsize::new(0));
        let payload = OriginalScanPayload {
            drops: primary.clone(),
            secondary: None,
        };
        *control.close_check.lock().unwrap() =
            Some(Box::new(move || std::panic::panic_any(payload)));
        let begin = catch_unwind(AssertUnwindSafe(|| scan.begin_exit()));
        let exit = scan.observe_exit().progress;
        let marker = begin
            .as_ref()
            .err()
            .is_some_and(|payload| is_scan_exit_unwind(payload.as_ref()));
        assert!(primed.is_ok());
        assert!(marker, "only the safe marker may reach the shared Driver");
        assert_eq!(primary.load(Ordering::SeqCst), 1);
        assert!(
            failures.error().is_none(),
            "health is not Task failure acceptance"
        );
        assert_eq!(
            failures.local_exit_fault().unwrap().fact(),
            explicit_scan_fact(580)
        );
        assert!(control.operations.is_exited());
        assert!(
            matches!(exit, ExitProgress::Exited),
            "Included fixture's actual ledger and normal payload disposal are real exit proof"
        );
        assert_eq!(
            control.terminations.load(Ordering::Acquire),
            1,
            "close panic cannot skip the independent stop hook"
        );
    }
    #[test]
    fn opaque_secondary_payload_keeps_original_scan_pending_even_with_included_scope() {
        use crate::exec::pipeline::operator::ExitProgress;
        let state = RuntimeState::default();
        let failures = state.error_state();
        let control = Arc::new(StreamControl::default());
        let (mut scan, _, _) = observed_stream_scan_source_in(&control, &state);
        let primed = scan.drive_for_test(&state);
        let primary = Arc::new(AtomicUsize::new(0));
        let secondary = Arc::new(AtomicUsize::new(0));
        let capture = Arc::new(vec![9_u8; 512]);
        let weak = Arc::downgrade(&capture);
        let payload = OriginalScanPayload {
            drops: primary.clone(),
            secondary: Some(SecondaryScanPayload {
                drops: secondary.clone(),
                _actual_capture: capture,
            }),
        };
        *control.close_check.lock().unwrap() =
            Some(Box::new(move || std::panic::panic_any(payload)));
        let begin = catch_unwind(AssertUnwindSafe(|| scan.begin_exit()));
        let first = scan.observe_exit().progress;
        let second_begin = scan.begin_exit();
        let second = scan.observe_exit().progress;
        assert!(primed.is_ok());
        assert!(
            begin
                .as_ref()
                .err()
                .is_some_and(|payload| is_scan_exit_unwind(payload.as_ref()))
        );
        assert!(second_begin.is_ok());
        assert!(control.operations.is_exited());
        assert!(
            matches!(first, ExitProgress::Unresolved(_))
                && matches!(second, ExitProgress::Unresolved(_))
        );
        assert_eq!(primary.load(Ordering::SeqCst), 1);
        assert_eq!(secondary.load(Ordering::SeqCst), 0);
        assert!(
            weak.upgrade().is_some(),
            "the actual opaque capture is retained by original scan custody"
        );
        assert_eq!(
            failures.local_exit_fault().unwrap().fact(),
            explicit_scan_fact(580)
        );
        assert!(failures.error().is_none());
        // The production Driver keeps this Pending operator. This unit fixture
        // has no live reader or Task grant; its bounded opaque cell is not a
        // claim that dropping an unresolved production Driver is permitted.
    }
    #[test]
    fn observation_constructor_unwind_still_closes_and_terminates_real_stream_once() {
        use crate::exec::pipeline::operator::ExitProgress;
        let state = RuntimeState::default();
        let control = Arc::new(StreamControl::default());
        let (mut scan, _, _) = observed_stream_scan_source_in(&control, &state);
        let calls = Arc::new(AtomicUsize::new(0));
        *control.observation_check.lock().unwrap() = Some(Box::new({
            let calls = calls.clone();
            move || {
                calls.fetch_add(1, Ordering::SeqCst);
                std::panic::panic_any(17_u32);
            }
        }));
        let primed = catch_unwind(AssertUnwindSafe(|| scan.drive_for_test(&state)));
        let begin = catch_unwind(AssertUnwindSafe(|| scan.begin_exit()));
        let first = scan.observe_exit().progress;
        let second_begin = scan.begin_exit();
        let second = scan.observe_exit().progress;
        assert!(
            primed
                .as_ref()
                .err()
                .is_some_and(|payload| is_scan_exit_unwind(payload.as_ref()))
        );
        assert!(
            begin.is_ok(),
            "separate cleanup follows the captured constructor fault"
        );
        assert!(second_begin.is_ok());
        assert!(control.close_requested.load(Ordering::Acquire));
        assert_eq!(control.terminations.load(Ordering::Acquire), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            matches!(first, ExitProgress::Unresolved(_))
                && matches!(second, ExitProgress::Unresolved(_))
        );
        assert_eq!(
            state.error_state().local_exit_fault().unwrap().fact(),
            explicit_scan_fact(579)
        );
        assert!(state.error().is_none());
    }
    #[test]
    fn a_termination_unwind_does_not_retry_or_replace_the_original_execution_cause() {
        use crate::exec::pipeline::operator::ExitProgress;
        let state = RuntimeState::default();
        let control = Arc::new(StreamControl::default());
        let (mut scan, _, _) = observed_stream_scan_source_in(&control, &state);
        let primed = scan.drive_for_test(&state);
        let original: ExecutionFailure =
            novarocks_functions::KernelFailure::ResourceExhausted.into();
        state.error_state().set_error(original.clone());
        *control.termination_check.lock().unwrap() =
            Some(Box::new(|| std::panic::panic_any(23_u32)));
        let begin = catch_unwind(AssertUnwindSafe(|| scan.begin_exit()));
        let first = scan.observe_exit().progress;
        let second_begin = scan.begin_exit();
        let second = scan.observe_exit().progress;
        assert!(primed.is_ok());
        assert!(
            begin
                .as_ref()
                .err()
                .is_some_and(|payload| is_scan_exit_unwind(payload.as_ref()))
        );
        assert!(second_begin.is_ok());
        assert_eq!(control.terminations.load(Ordering::Acquire), 1);
        assert!(
            matches!(first, ExitProgress::Unresolved(_))
                && matches!(second, ExitProgress::Unresolved(_))
        );
        assert_eq!(
            state.error_state().local_exit_fault().unwrap().fact(),
            explicit_scan_fact(585)
        );
        assert!(state.error().unwrap().same_occurrence(&original));
    }
    #[test]
    fn a_normally_pending_exit_never_marks_runtime_health_unresolved() {
        let failures = RuntimeErrorState::default();
        let mut custody = ScanExitPanicCustody::default();
        let mut future: Option<BoxFuture<'static, ExecutionResult<()>>> =
            Some(Box::pin(futures::future::pending()));
        let mut phase = ExitFuturePhase::Pending;
        let mut result = None;
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        for _ in 0..2 {
            assert!(!poll_exit_component(
                &mut future,
                &mut phase,
                &mut result,
                &mut custody,
                &failures,
                false,
                &mut cx
            ));
        }
        assert!(phase == ExitFuturePhase::Pending);
        assert!(failures.local_exit_fault().is_none());
        assert!(failures.error().is_none());
        assert!(!custody.has_unknown_payload());
    }
    #[test]
    fn observed_poll_panic_has_exact_health_and_keeps_original_future_without_reentry() {
        let failures = RuntimeErrorState::default();
        let mut custody = ScanExitPanicCustody::default();
        let calls = Arc::new(AtomicUsize::new(0));
        let capture = Arc::new(vec![5_u8; 512]);
        let weak = Arc::downgrade(&capture);
        let mut future: Option<BoxFuture<'static, ExecutionResult<()>>> =
            Some(Box::pin(futures::future::poll_fn({
                let calls = calls.clone();
                move |_| {
                    let _actual_capture = capture.len();
                    calls.fetch_add(1, Ordering::SeqCst);
                    std::panic::panic_any(31_u32);
                    #[allow(unreachable_code)]
                    Poll::Ready(Ok(()))
                }
            })));
        let mut phase = ExitFuturePhase::Pending;
        let mut result = None;
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let first = poll_exit_component(
            &mut future,
            &mut phase,
            &mut result,
            &mut custody,
            &failures,
            true,
            &mut cx,
        );
        let second = poll_exit_component(
            &mut future,
            &mut phase,
            &mut result,
            &mut custody,
            &failures,
            true,
            &mut cx,
        );
        assert!(first && !second);
        assert!(phase == ExitFuturePhase::Polling);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(weak.upgrade().is_some());
        assert!(future.is_some());
        assert_eq!(
            failures.local_exit_fault().unwrap().fact(),
            explicit_scan_fact(583)
        );
        assert!(failures.error().is_none());
    }
    struct ReadyDropPanic {
        result: ExecutionFailure,
        drops: Arc<AtomicUsize>,
    }
    impl std::future::Future for ReadyDropPanic {
        type Output = ExecutionResult<()>;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(Err(self.result.clone()))
        }
    }
    impl Drop for ReadyDropPanic {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            std::panic::panic_any(41_u32);
        }
    }
    #[test]
    fn a_ready_error_is_kept_in_original_owner_before_future_drop_can_unwind() {
        let failures = RuntimeErrorState::default();
        let mut custody = ScanExitPanicCustody::default();
        let original: ExecutionFailure =
            novarocks_functions::KernelFailure::ResourceExhausted.into();
        let drops = Arc::new(AtomicUsize::new(0));
        let mut future: Option<BoxFuture<'static, ExecutionResult<()>>> =
            Some(Box::pin(ReadyDropPanic {
                result: original.clone(),
                drops: drops.clone(),
            }));
        let mut phase = ExitFuturePhase::Pending;
        let mut result = None;
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let first = poll_exit_component(
            &mut future,
            &mut phase,
            &mut result,
            &mut custody,
            &failures,
            false,
            &mut cx,
        );
        let second = poll_exit_component(
            &mut future,
            &mut phase,
            &mut result,
            &mut custody,
            &failures,
            false,
            &mut cx,
        );
        assert!(first && !second);
        assert!(phase == ExitFuturePhase::Dropping);
        assert!(future.is_none());
        assert!(
            result
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap_err()
                .same_occurrence(&original)
        );
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(
            failures.local_exit_fault().unwrap().fact(),
            explicit_scan_fact(582)
        );
        assert!(failures.error().is_none());
    }

    #[test]
    fn emit_then_stop_preserves_the_real_last_chunk_when_close_construction_unwinds() {
        use crate::exec::pipeline::operator::ExitProgress;
        let state = RuntimeState::default();
        let control = Arc::new(StreamControl::default());
        control.requires_close_body.store(true, Ordering::Release);
        control.push(one_row(73));
        let (mut scan, _, _) = observed_stream_scan_source_in_with_limit(&control, &state, Some(1));
        let budget = ConnectorPollBudget::new();
        budget.refill(SCAN_STREAM_TURN_BUDGET);
        scan.bind_poll_budget(budget);
        *control.close_check.lock().unwrap() = Some(Box::new(|| std::panic::panic_any(71_u32)));
        // This is the real Source adapter and EmitThenStop lane. No catch can
        // hide loss of the local Chunk before it enters its actual StepResult.
        let first = scan.drive_for_test(&state);
        let step = first.expect("cleanup unwind must not escape the data author");
        let next_is_finished = matches!(step.next, NextStep::Finished);
        let chunk = step
            .output
            .expect("the required limit prefix must be delivered");
        let delivered = value_of(&chunk);
        let exit = scan.observe_exit().progress;
        assert!(next_is_finished);
        assert_eq!(delivered, 73);
        assert_eq!(chunk.len(), 1);
        assert_eq!(control.polls.load(Ordering::Acquire), 1);
        assert_eq!(control.terminations.load(Ordering::Acquire), 1);
        assert!(
            state.error().is_none(),
            "exit health cannot replace the successful data result"
        );
        assert_eq!(
            state.error_state().local_exit_fault().unwrap().fact(),
            explicit_scan_fact(580)
        );
        assert!(
            matches!(exit, ExitProgress::Unresolved(_)),
            "the production-style unknown body still has no exit proof"
        );
    }
    #[test]
    fn originating_read_error_is_returned_unchanged_when_close_construction_unwinds() {
        use crate::exec::pipeline::operator::ExitProgress;
        use crate::runtime::task::connector_failure::{ConnectorCallFailure, ConnectorOperation};
        use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
        let state = RuntimeState::default();
        let control = Arc::new(StreamControl::default());
        control.requires_close_body.store(true, Ordering::Release);
        let original: ExecutionFailure = ConnectorCallFailure::new(
            ConnectorOperation::ReadPage,
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "original read data is corrupt",
            ),
        )
        .into();
        *control.error.lock().unwrap() = Some(original.clone());
        let (mut scan, _, _) = observed_stream_scan_source_in(&control, &state);
        *control.close_check.lock().unwrap() = Some(Box::new(|| std::panic::panic_any(79_u32)));
        let returned = scan.drive_for_test(&state);
        let exit = scan.observe_exit().progress;
        assert!(
            returned
                .as_ref()
                .err()
                .is_some_and(|cause| cause.same_occurrence(&original))
        );
        assert!(state.error().unwrap().same_occurrence(&original));
        assert_eq!(
            state.error_state().local_exit_fault().unwrap().fact(),
            explicit_scan_fact(580)
        );
        assert_eq!(control.polls.load(Ordering::Acquire), 1);
        assert_eq!(control.terminations.load(Ordering::Acquire), 1);
        assert!(matches!(exit, ExitProgress::Unresolved(_)));
    }

    #[test]
    fn an_unclaimed_scan_cannot_discard_an_unknown_exit_control_payload() {
        use crate::exec::pipeline::operator::ExitProgress;
        let state = RuntimeState::default();
        let control = Arc::new(StreamControl::default());
        let (mut scan, source, _) = observed_stream_scan_source_in(&control, &state);
        scan.on_downstream_backpressure(true);
        let primary = Arc::new(AtomicUsize::new(0));
        let secondary = Arc::new(AtomicUsize::new(0));
        let capture = Arc::new(vec![3_u8; 512]);
        let weak = Arc::downgrade(&capture);
        let payload = OriginalScanPayload {
            drops: primary.clone(),
            secondary: Some(SecondaryScanPayload {
                drops: secondary.clone(),
                _actual_capture: capture,
            }),
        };
        *control.backpressure_release_check.lock().unwrap() =
            Some(Box::new(move || std::panic::panic_any(payload)));
        let begin = catch_unwind(AssertUnwindSafe(|| scan.begin_exit()));
        let first = scan.observe_exit().progress;
        let second = scan.observe_exit().progress;
        assert_eq!(
            source.claims.load(Ordering::Acquire),
            0,
            "no reader or operation exit was invented"
        );
        assert!(
            begin
                .as_ref()
                .err()
                .is_some_and(|payload| is_scan_exit_unwind(payload.as_ref()))
        );
        assert_eq!(control.terminations.load(Ordering::Acquire), 1);
        assert_eq!(primary.load(Ordering::SeqCst), 1);
        assert_eq!(secondary.load(Ordering::SeqCst), 0);
        assert!(weak.upgrade().is_some());
        assert!(
            matches!(first, ExitProgress::Unresolved(_))
                && matches!(second, ExitProgress::Unresolved(_))
        );
        assert_eq!(
            state.error_state().local_exit_fault().unwrap().fact(),
            explicit_scan_fact(586)
        );
        assert!(state.error().is_none());
    }

    #[test]
    fn close_construction_panic_keeps_a_live_source_exit_pending() {
        use crate::exec::pipeline::operator::ExitProgress;

        let control = Arc::new(StreamControl::default());
        let ticket = control
            .operations
            .admit(Arc::new(|| {}))
            .expect("one real operation");
        let (release, released) = mpsc::channel::<()>();
        let operation = std::thread::spawn(move || {
            // Dropping the sender also releases the real operation on unwind.
            let _ = released.recv();
            ticket.end(novarocks_spi::connector::read_stack::ConnectorOperationExit::Exited);
        });
        let (mut scan, _source) = stream_scan_source(&control);
        let primed = scan.drive_for_test(&RuntimeState::default());
        let entered = Arc::new(AtomicBool::new(false));
        *control.close_check.lock().unwrap() = Some(Box::new({
            let entered = entered.clone();
            move || {
                entered.store(true, Ordering::Release);
                panic!("injected close constructor panic before the exit future exists");
            }
        }));
        let begin = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| scan.begin_exit()));
        let was_pending = matches!(scan.observe_exit().progress, ExitProgress::Unresolved(_));
        let live_before = control.operations.live_operations();
        let exited_before = control.operations.is_exited();
        let entered_before = entered.load(Ordering::Acquire);

        // Release the actual operation and join it before every test oracle.
        // Sealing does not end its ticket; only the operation above does that.
        control.operations.seal();
        drop(release);
        let joined = operation.join();
        drop(scan);

        assert!(joined.is_ok(), "the real operation must return");
        assert!(
            primed.is_ok(),
            "the production source was claimed before cleanup"
        );
        assert!(
            entered_before,
            "the existing close_check reached construction"
        );
        assert!(begin.is_err(), "the fixture must inject an actual panic");
        assert_eq!(live_before, 1);
        assert!(!exited_before);
        assert!(control.operations.is_exited());
        assert!(
            was_pending,
            "a caught close-constructor panic cannot turn an outstanding operation into Ready(Ok)"
        );
    }

    #[test]
    fn close_construction_panic_cannot_publish_driver_stopped_before_the_source_operation_exits() {
        use crate::exec::pipeline::global_driver_executor::{
            DriverGroupCompletion, DriverRunner, GlobalDriverExecutor,
        };
        use crate::exec::pipeline::task_context::TaskContext;
        use crate::runtime::task::ExecutionFailure;

        let mut pipeline = pipeline();
        let control = pipeline.control.clone();
        let state = pipeline.state.clone();
        let ticket = control
            .operations
            .admit(Arc::new(|| {}))
            .expect("one real source operation");
        let (release, released) = mpsc::channel::<()>();
        let operation = std::thread::spawn(move || {
            let _ = released.recv();
            ticket.end(novarocks_spi::connector::read_stack::ConnectorOperationExit::Exited);
        });
        let primed = pipeline.driver.process(TURN);
        let entered = Arc::new(AtomicBool::new(false));
        *control.close_check.lock().unwrap() = Some(Box::new({
            let entered = entered.clone();
            move || {
                entered.store(true, Ordering::Release);
                panic!("injected driver close constructor panic");
            }
        }));
        let original: ExecutionFailure =
            novarocks_functions::KernelFailure::ResourceExhausted.into();
        state.error_state().set_error(original.clone());
        let completion = DriverGroupCompletion::new(1, state.error_state());
        let context = Arc::new(TaskContext::new(
            None,
            state.clone(),
            None,
            None,
            None,
            None,
        ));
        let task = DriverRunner::new(pipeline.driver, completion.clone(), context, TURN);

        // A second real DriverTask on the same one-worker queue is a barrier:
        // its stopped notification follows the scan driver's stop/park turn.
        // It does not manufacture completion for the scan or its operation.
        let barrier_state = Arc::new(RuntimeState::default());
        let barrier_completion = DriverGroupCompletion::new(1, barrier_state.error_state());
        let barrier_context = Arc::new(TaskContext::new(
            None,
            barrier_state.clone(),
            None,
            None,
            None,
            None,
        ));
        let barrier_driver =
            PipelineDriver::new(2, Vec::new(), None, Vec::new(), barrier_state, None);
        let barrier_task = DriverRunner::new(
            barrier_driver,
            barrier_completion.clone(),
            barrier_context,
            TURN,
        );
        let (barrier_sent, barrier_received) = mpsc::channel();
        let mut barrier_finisher: Option<
            crate::exec::pipeline::global_driver_executor::completion_custody::Callback<
                crate::exec::pipeline::global_driver_executor::DriverGroupStoppedFact,
            >,
        > = Some(Box::new(move |fact| {
            let _ = barrier_sent.send(fact.clone());
            true
        }));
        barrier_completion
            .bind_resources_finisher(&mut barrier_finisher)
            .expect("one original barrier resource finisher");
        let executor = GlobalDriverExecutor::new(1);
        let admitted = executor.submit(vec![task, barrier_task]);
        let barrier = barrier_received.recv_timeout(Duration::from_secs(2));
        let stopped_before = completion.stopped_fact();
        let live_before = control.operations.live_operations();
        let exited_before = control.operations.is_exited();
        let entered_before = entered.load(Ordering::Acquire);

        // No assertion is inside the engine catch or before real cleanup.
        // Unpark and join the source operation, then join the actual executor.
        control.operations.seal();
        drop(release);
        let joined = operation.join();
        control.wake();
        let shutdown = executor.shutdown();
        let stopped_after = completion.stopped_fact();
        let observed = state.error();

        assert!(joined.is_ok());
        assert!(
            shutdown.is_ok(),
            "executor must join after the real source exit: {shutdown:?}"
        );
        assert!(admitted);
        assert!(
            matches!(primed, DriverState::Blocked(BlockedReason::InputEmpty)),
            "prime the real stream before stop: {primed:?}"
        );
        assert!(
            barrier.is_ok(),
            "the independent queue barrier must really run: {barrier:?}"
        );
        assert!(entered_before);
        assert_eq!(live_before, 1);
        assert!(!exited_before);
        assert!(control.operations.is_exited());
        assert!(
            stopped_before.is_none(),
            "a caught constructor panic cannot certify the live source operation exited: {stopped_before:?}"
        );
        assert!(
            stopped_after.is_some(),
            "actual source exit must eventually allow real driver completion"
        );
        assert!(
            observed.unwrap().same_occurrence(&original),
            "cleanup panic cannot replace the original work failure"
        );
    }

    #[test]
    fn an_original_background_work_failure_wakes_the_live_scan_before_any_close() {
        use crate::runtime::task::connector_failure::ConnectorOperation;
        use novarocks_spi::connector::{
            ConnectorError, ConnectorErrorKind, read_stack::ConnectorOperationExit,
        };
        let mut pipeline = pipeline();
        let failed = pipeline.control.operations.admit(Arc::new(|| {})).unwrap();
        let live = pipeline.control.operations.admit(Arc::new(|| {})).unwrap();
        assert!(matches!(
            pipeline.driver.process(TURN),
            DriverState::Blocked(BlockedReason::InputEmpty)
        ));
        assert!(!pipeline.control.close_requested.load(Ordering::Acquire));
        assert_eq!(pipeline.control.terminations.load(Ordering::Acquire), 0);
        let (observable, generation, _) = pipeline
            .driver
            .blocked_observable_snapshot()
            .expect("actual scan waiter");
        failed.end(ConnectorOperationExit::WorkFailed(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "actual accepted background operation failed",
        )));
        assert!(
            observable.generation() > generation,
            "the original registered source receiver wakes its blocked driver before close"
        );
        assert_eq!(pipeline.driver.process(TURN), DriverState::PendingFinish);
        let original = pipeline
            .state
            .error()
            .expect("the original runtime already accepted the work author");
        let crate::runtime::task::ExecutionFailureCause::Connector(cause) = original.cause() else {
            panic!("the real SourceExit author remains typed");
        };
        assert_eq!(cause.operation(), ConnectorOperation::SourceExit);
        assert_eq!(cause.cause().kind(), ConnectorErrorKind::CorruptData);
        assert_eq!(
            cause.cause().message(),
            "actual accepted background operation failed"
        );
        assert_eq!(pipeline.control.operations.live_operations(), 1);
        assert!(!pipeline.control.operations.is_exited());
        assert!(pipeline.control.close_requested.load(Ordering::Acquire));
        drop(live);
        match pipeline.driver.process(TURN) {
            DriverState::Failed(observed) => assert!(observed.same_occurrence(&original)),
            other => panic!("only actual sibling exit completes this failed driver: {other:?}"),
        }
    }

    #[test]
    fn originating_read_failure_is_visible_before_close_and_keeps_its_step_location() {
        use crate::runtime::task::connector_failure::{ConnectorCallFailure, ConnectorOperation};
        use crate::runtime::task::{ExecutionFailure, PipelineOperation};
        use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
        let mut pipeline = pipeline();
        let original: ExecutionFailure = ConnectorCallFailure::new(
            ConnectorOperation::ReadPage,
            ConnectorError::new(ConnectorErrorKind::CorruptData, "original corrupt page"),
        )
        .at_sequence(42)
        .into();
        let expected = original.clone().at_operator(0, PipelineOperation::Pull);
        let error_state = pipeline.state.error_state();
        let checked = Arc::new(AtomicBool::new(false));
        *pipeline.control.close_check.lock().unwrap() = Some(Box::new({
            let expected = expected.clone();
            let checked = checked.clone();
            let error_state = error_state.clone();
            move || {
                assert_eq!(error_state.error(), Some(expected));
                checked.store(true, Ordering::Release);
            }
        }));
        let exit = pipeline.control.operations.admit(Arc::new(|| {})).unwrap();
        *pipeline.control.error.lock().unwrap() = Some(original);
        assert_eq!(pipeline.driver.process(TURN), DriverState::PendingFinish);
        assert!(checked.load(Ordering::Acquire));
        assert_eq!(error_state.error(), Some(expected.clone()));
        assert_eq!(pipeline.source.claims.load(Ordering::Acquire), 1);
        exit.end(
            novarocks_spi::connector::read_stack::ConnectorOperationExit::WorkFailed(
                ConnectorError::new(ConnectorErrorKind::Unavailable, "later source exit failure"),
            ),
        );
        assert_eq!(
            pipeline.driver.process(TURN),
            DriverState::Failed(expected.clone())
        );
        assert_eq!(error_state.error(), Some(expected));
    }

    #[test]
    fn a_wake_inside_the_poll_is_rechecked_before_the_driver_parks() {
        let mut pipeline = pipeline();
        *pipeline.control.staged.lock().expect("staged") = Some(one_row(1));
        pipeline
            .control
            .wake_inside_poll
            .store(true, Ordering::Release);

        // The poll returns Pending after waking itself: the driver must not
        // park on a wake it already received. The worker rechecks and delivers
        // that row in the current turn, then parks on the next empty poll.
        assert!(matches!(
            pipeline.driver.process(TURN),
            DriverState::Blocked(BlockedReason::InputEmpty)
        ));
        assert_eq!(*pipeline.values.lock().expect("values"), vec![1]);
    }

    #[test]
    fn a_wake_after_the_poll_moves_past_the_parked_generation() {
        let mut pipeline = pipeline();
        assert!(matches!(
            pipeline.driver.process(TURN),
            DriverState::Blocked(BlockedReason::InputEmpty)
        ));
        let (observable, generation, _) = pipeline
            .driver
            .blocked_observable_snapshot()
            .expect("parked on the stream");

        pipeline.control.push(one_row(2));
        pipeline.control.wake();
        assert!(
            observable.generation() > generation,
            "the stream's waker wakes the parked driver"
        );
        assert!(matches!(
            pipeline.driver.process(TURN),
            DriverState::Blocked(BlockedReason::InputEmpty)
        ));
        assert_eq!(*pipeline.values.lock().expect("values"), vec![2]);
        assert_eq!(
            pipeline.source.claims.load(Ordering::Acquire),
            1,
            "one stream, claimed once"
        );
    }

    #[test]
    fn a_turn_that_runs_out_of_budget_yields_and_the_next_one_resumes() {
        use crate::runtime::scan_stream_metrics::{ScanStreamPending, scan_stream_pending_count};

        let mut pipeline = pipeline();
        let total = usize::try_from(SCAN_STREAM_TURN_BUDGET).expect("budget fits") * 2 + 5;
        for value in 0..total {
            pipeline
                .control
                .push(one_row(i32::try_from(value).expect("value fits")));
        }
        // Process-wide counters other tests also move: only their growth is
        // this test's.
        let yields = scan_stream_pending_count(ScanStreamPending::BudgetYield);
        let waits = scan_stream_pending_count(ScanStreamPending::Wait);

        assert!(
            matches!(pipeline.driver.process(TURN), DriverState::Ready),
            "a stream out of budget ends the turn instead of running on"
        );
        assert_eq!(
            pipeline.values.lock().expect("values").len(),
            usize::try_from(SCAN_STREAM_TURN_BUDGET).expect("budget fits")
        );
        assert!(matches!(pipeline.driver.process(TURN), DriverState::Ready));
        assert!(matches!(
            pipeline.driver.process(TURN),
            DriverState::Blocked(BlockedReason::InputEmpty)
        ));
        let values = pipeline.values.lock().expect("values").clone();
        assert_eq!(values.len(), total, "nothing is lost across yields");
        assert!(values.windows(2).all(|pair| pair[0] + 1 == pair[1]));
        assert!(
            scan_stream_pending_count(ScanStreamPending::BudgetYield) >= yields + 2,
            "both spent turns are counted as yields"
        );
        assert!(
            scan_stream_pending_count(ScanStreamPending::Wait) > waits,
            "the empty stream at the end is counted as a wait"
        );
    }

    #[test]
    fn actual_stream_close_outcome_stays_immutable_after_operation_exit() {
        use crate::exec::pipeline::operator::ExitProgress;
        use crate::runtime::task::connector_failure::ConnectorOperation;
        use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
        for fail in [false, true] {
            let control = Arc::new(StreamControl::default());
            let ticket = control.operations.admit(Arc::new(|| {})).unwrap();
            let state = RuntimeState::default();
            let (mut scan, _, _) = observed_stream_scan_source_in(&control, &state);
            scan.drive_for_test(&state).unwrap();
            scan.begin_exit().unwrap();
            assert!(control.close_requested.load(Ordering::Acquire));
            assert!(!control.operations.is_exited());
            assert!(matches!(
                scan.observe_exit().progress,
                ExitProgress::Pending(_)
            ));
            ticket.end(if fail {
                novarocks_spi::connector::read_stack::ConnectorOperationExit::WorkFailed(
                    ConnectorError::new(
                        ConnectorErrorKind::Unavailable,
                        "original stream exit refusal",
                    ),
                )
            } else {
                novarocks_spi::connector::read_stack::ConnectorOperationExit::Exited
            });
            assert!(control.operations.is_exited());
            let observation = scan.observe_exit();
            assert!(matches!(observation.progress, ExitProgress::Exited));
            assert!(
                observation.failure.is_none(),
                "the bound runtime consumed the author event"
            );
            let original = state.error();
            assert_eq!(original.is_some(), fail);
            if let Some(error) = &original {
                let crate::runtime::task::ExecutionFailureCause::Connector(source) = error.cause()
                else {
                    panic!("the actual SourceExit domain remains intact");
                };
                assert_eq!(source.operation(), ConnectorOperation::SourceExit);
                assert_eq!(source.cause().kind(), ConnectorErrorKind::Unavailable);
            }
            for _ in 0..3 {
                let observed = scan.observe_exit();
                assert!(matches!(observed.progress, ExitProgress::Exited));
                assert!(observed.failure.is_none());
                match (&original, state.error()) {
                    (None, None) => {}
                    (Some(original), Some(observed)) => assert!(observed.same_occurrence(original)),
                    _ => panic!("the original runtime outcome cannot change after consumption"),
                }
            }
        }
    }

    #[test]
    fn end_of_stream_waits_for_the_sources_exit_before_the_driver_ends() {
        let mut pipeline = pipeline();
        let ticket = pipeline
            .control
            .operations
            .admit(Arc::new(|| {}))
            .expect("one running operation");
        pipeline.control.push(one_row(3));
        pipeline.control.ended.store(true, Ordering::Release);

        let state = run_until_parked(&mut pipeline.driver);
        assert!(matches!(state, DriverState::PendingFinish), "{state:?}");
        assert!(
            pipeline.control.close_requested.load(Ordering::Acquire),
            "end of stream closes the stream, which stops its operations"
        );
        assert_eq!(*pipeline.values.lock().expect("values"), vec![3]);
        assert!(matches!(
            pipeline.driver.process(TURN),
            DriverState::PendingFinish
        ));

        ticket.end(novarocks_spi::connector::read_stack::ConnectorOperationExit::Exited);
        assert!(matches!(
            pipeline.driver.process(TURN),
            DriverState::Finished
        ));
    }

    #[test]
    fn pending_source_close_does_not_prevent_downstream_eos() {
        let state = Arc::new(RuntimeState::default());
        let control = Arc::new(StreamControl::default());
        let operation = control.operations.admit(Arc::new(|| {})).unwrap();
        let (scan, source, _) = observed_stream_scan_source_in(&control, &state);
        let values = Arc::new(Mutex::new(Vec::new()));
        let finishes = Arc::new(AtomicUsize::new(0));
        let finish_count = finishes.clone();
        let sink = crate::exec::pipeline::shape::Transform::hydrating(CollectSink {
            values: values.clone(),
            // This admitted source operation owes the consumer its input EOS.
            // Its explicit end occurs in that same mandatory finalization.
            on_finish: Some(Box::new(move || {
                finish_count.fetch_add(1, Ordering::AcqRel);
                operation.end(novarocks_spi::connector::read_stack::ConnectorOperationExit::Exited);
            })),
        });
        let mut driver =
            PipelineDriver::new(1, vec![scan, Box::new(sink)], None, vec![], state, None);
        control.push(one_row(3));
        control.ended.store(true, Ordering::Release);
        assert_eq!(run_until_parked(&mut driver), DriverState::Finished);
        assert_eq!(*values.lock().unwrap(), vec![3]);
        assert_eq!(finishes.load(Ordering::Acquire), 1);
        assert!(control.close_requested.load(Ordering::Acquire));
        assert!(control.operations.is_exited());
        assert_eq!(source.claims.load(Ordering::Acquire), 1);
        drop(driver);
        assert_eq!(finishes.load(Ordering::Acquire), 1);
    }

    #[test]
    fn beginning_exit_without_observing_it_still_stops_the_stream() {
        let control = Arc::new(StreamControl::default());
        let (mut scan, _source) = stream_scan_source(&control);
        let _ticket = control
            .operations
            .admit(Arc::new(|| {}))
            .expect("one running operation");
        let step = scan.drive_for_test(&RuntimeState::default()).expect("poll");
        assert!(step.output.is_none());
        assert!(matches!(step.next, NextStep::Wait(_)));

        scan.begin_exit().expect("begin exit");
        scan.close().expect("close");
        drop(scan);
        assert!(control.close_requested.load(Ordering::Acquire));
        assert!(
            control.operations.is_sealed(),
            "the stop was requested; exit stays owned by the source's operations"
        );
    }

    // A compiled scan source owns no conjunct, runtime filter or limit: every
    // nonempty chunk its stream delivers is handed on as read.
    #[test]
    fn a_compiled_scan_source_hands_every_chunk_on_as_read() {
        let control = Arc::new(StreamControl::default());
        let source = Arc::new(ScriptedSource {
            control: Arc::clone(&control),
            claims: AtomicUsize::new(0),
        });
        let op: Arc<dyn ScanOp> = Arc::new(StreamScanOp {
            source: Arc::clone(&source),
            backpressure: Arc::new(Mutex::new(Vec::new())),
        });
        let factory = StreamScanSourceFactory::new_compiled(3, op, None);
        assert!(
            factory.name().contains("plan_node_id=3"),
            "{}",
            factory.name()
        );
        let mut scan = factory.create(1, 0);
        scan.set_construction_factory(actual_test_factory());
        scan.bind_runtime_state(&RuntimeState::default())
            .expect("bind compiled stream scan");
        let values = Arc::new(Mutex::new(Vec::new()));
        let mut driver = PipelineDriver::new(
            1,
            vec![
                scan,
                Box::new(crate::exec::pipeline::shape::Transform::hydrating(
                    CollectSink {
                        values: Arc::clone(&values),
                        on_finish: None,
                    },
                )),
            ],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            None,
        );
        for value in [4, 5, 6] {
            control.push(one_row(value));
        }
        control.ended.store(true, Ordering::Release);

        let mut state = run_until_parked(&mut driver);
        if matches!(state, DriverState::PendingFinish) {
            state = driver.process(TURN);
        }
        assert!(matches!(state, DriverState::Finished), "{state:?}");
        assert_eq!(*values.lock().expect("values"), vec![4, 5, 6]);
        assert_eq!(source.claims.load(Ordering::Acquire), 1);
    }

    #[test]
    fn cancelling_a_stream_scan_starts_its_scan_s_terminal_cleanup() {
        let control = Arc::new(StreamControl::default());
        let (mut scan, _source) = stream_scan_source(&control);
        scan.cancel();
        assert_eq!(control.terminations.load(Ordering::Acquire), 1);
        let ended = scan.drive_for_test(&RuntimeState::default()).unwrap();
        assert!(ended.output.is_none());
        assert!(matches!(ended.next, NextStep::Finished));
    }

    #[test]
    fn a_stream_scan_charges_the_chunks_it_hands_out_to_its_fragment() {
        let fragment = crate::runtime::mem_tracker::MemTracker::new_root("fragment_under_test");
        let state = RuntimeState::for_local_execution(
            None,
            None,
            None,
            None,
            None,
            Some(Arc::clone(&fragment)),
            None,
        );
        let control = Arc::new(StreamControl::default());
        let (mut scan, _source, _) = observed_stream_scan_source_in(&control, &state);
        control.push(one_row(3));
        let budget = ConnectorPollBudget::new();
        budget.refill(SCAN_STREAM_TURN_BUDGET);
        scan.bind_poll_budget(budget);
        let chunk = scan
            .drive_for_test(&state)
            .map(|step| step.output)
            .expect("poll")
            .expect("the scripted chunk");
        assert_eq!(value_of(&chunk), 3);
        assert!(
            fragment.current() > 0,
            "a handed-out chunk is charged to the fragment"
        );
        drop(chunk);
        assert_eq!(fragment.current(), 0);
    }

    /// A sink that accepts input only while the test lets it.
    struct GatedSink {
        open: Arc<AtomicBool>,
        values: Arc<Mutex<Vec<i32>>>,
        observable: Arc<Observable>,
    }

    impl OperatorLifecycle for GatedSink {
        fn name(&self) -> &str {
            "GATED_SINK"
        }
    }
    impl crate::exec::pipeline::operator::InputEncodingPolicy for GatedSink {}

    impl crate::exec::pipeline::shape::SinkKernel for GatedSink {
        fn admission(
            &self,
            _: &mut crate::exec::pipeline::driver_step::DriverStep<'_>,
            _: &crate::exec::pipeline::input::RawInput,
        ) -> ExecutionResult<Option<crate::exec::pipeline::step::AdmissionNext>> {
            let generation = self.observable.generation();
            Ok((!self.open.load(Ordering::Acquire)).then(|| {
                crate::exec::pipeline::step::AdmissionNext::Wait(WaitFact {
                    reason: WaitReason::Output,
                    observable: self.observable.clone(),
                    observed_generation: generation,
                    deadline: None,
                })
            }))
        }
        fn consume(
            &mut self,
            _: &mut crate::exec::pipeline::driver_step::DriverStep<'_>,
            input: crate::exec::pipeline::input::PreparedInput,
        ) -> ExecutionResult<crate::exec::pipeline::shape::SinkStep> {
            let chunk = input.into_uncovered_chunk()?;
            self.values.lock().expect("values").push(value_of(&chunk));
            Ok(crate::exec::pipeline::shape::SinkStep::new(
                NextStep::NeedInput,
                chunk.len() as u64,
            ))
        }
    }

    #[test]
    fn a_refused_chunk_pauses_the_scan_and_its_acceptance_resumes_it() {
        let control = Arc::new(StreamControl::default());
        let (scan, _source, backpressure) = observed_stream_scan_source(&control);
        let open = Arc::new(AtomicBool::new(false));
        let sink_observable = Arc::new(Observable::new());
        let values = Arc::new(Mutex::new(Vec::new()));
        let mut driver = PipelineDriver::new(
            1,
            vec![
                scan,
                Box::new(crate::exec::pipeline::shape::Transform::hydrating(
                    GatedSink {
                        open: Arc::clone(&open),
                        values: Arc::clone(&values),
                        observable: Arc::clone(&sink_observable),
                    },
                )),
            ],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            None,
        );
        control.push(one_row(20));
        control.push(one_row(21));

        assert!(matches!(
            driver.process(TURN),
            DriverState::Blocked(BlockedReason::OutputFull)
        ));
        assert_eq!(
            *backpressure.lock().expect("backpressure"),
            vec![true],
            "the downstream refused the scan's output"
        );

        open.store(true, Ordering::Release);
        sink_observable.notify_observers();
        assert!(matches!(
            driver.process(TURN),
            DriverState::Blocked(BlockedReason::InputEmpty)
        ));
        assert_eq!(*values.lock().expect("values"), vec![20, 21]);
        assert_eq!(
            *backpressure.lock().expect("backpressure"),
            vec![true, false],
            "accepting the output resumes the scan"
        );
    }

    /// A sink whose push blocks until the test releases it, so the downstream
    /// is demonstrably busy.
    struct BlockingSink {
        entered: mpsc::Sender<i32>,
        release: Arc<Mutex<mpsc::Receiver<()>>>,
    }

    impl OperatorLifecycle for BlockingSink {
        fn name(&self) -> &str {
            "BLOCKING_SINK"
        }
    }
    impl crate::exec::pipeline::operator::InputEncodingPolicy for BlockingSink {}

    impl crate::exec::pipeline::shape::SinkKernel for BlockingSink {
        fn consume(
            &mut self,
            _: &mut crate::exec::pipeline::driver_step::DriverStep<'_>,
            input: crate::exec::pipeline::input::PreparedInput,
        ) -> ExecutionResult<crate::exec::pipeline::shape::SinkStep> {
            let chunk = input.into_uncovered_chunk()?;
            self.entered.send(value_of(&chunk)).expect("entered");
            self.release
                .lock()
                .expect("release")
                .recv()
                .expect("released");
            Ok(crate::exec::pipeline::shape::SinkStep::new(
                NextStep::NeedInput,
                chunk.len() as u64,
            ))
        }
    }

    #[test]
    fn the_scan_keeps_producing_while_a_second_worker_runs_the_downstream() {
        let control = Arc::new(StreamControl::default());
        let (scan, _source) = stream_scan_source(&control);
        let exchanger = LocalExchanger::new_handoff(1, 1, 8);
        let state = Arc::new(RuntimeState::default());
        let mut exchange_sink =
            LocalExchangeSinkFactory::new(-1, Arc::clone(&exchanger)).create(1, 0);
        exchange_sink.set_construction_factory(actual_test_factory());
        exchange_sink
            .bind_runtime_state(&state)
            .expect("bind exchange queue ownership");
        let mut scan_driver = PipelineDriver::new(
            1,
            vec![scan, exchange_sink],
            None,
            Vec::new(),
            Arc::clone(&state),
            None,
        );
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let mut downstream = PipelineDriver::new(
            2,
            vec![
                LocalExchangeSourceFactory::new(-1, 1, Arc::clone(&exchanger)).create(1, 0),
                Box::new(crate::exec::pipeline::shape::Transform::hydrating(
                    BlockingSink {
                        entered: entered_tx,
                        release: Arc::new(Mutex::new(release_rx)),
                    },
                )),
            ],
            None,
            Vec::new(),
            Arc::clone(&state),
            None,
        );

        control.push(one_row(10));
        assert!(matches!(
            scan_driver.process(TURN),
            DriverState::Blocked(BlockedReason::InputEmpty)
        ));
        let queued = |exchanger: &Arc<LocalExchanger>| {
            exchanger
                .partition_buffered_chunks(0)
                .map(|(chunks, _)| chunks)
                .expect("partition")
        };
        assert_eq!(queued(&exchanger), 1);

        std::thread::scope(|scope| {
            let worker = scope.spawn(move || downstream.process(TURN));
            assert_eq!(
                entered_rx
                    .recv_timeout(TURN)
                    .expect("downstream is working"),
                10
            );

            // The downstream is busy inside its sink on another worker; the
            // scan driver still reads and hands its output on.
            control.push(one_row(11));
            control.push(one_row(12));
            control.wake();
            assert!(matches!(
                scan_driver.process(TURN),
                DriverState::Blocked(BlockedReason::InputEmpty)
            ));
            assert_eq!(queued(&exchanger), 2, "the source moved on meanwhile");

            for _ in 0..3 {
                release_tx.send(()).expect("release");
            }
            let _ = worker.join().expect("downstream worker");
        });
    }
}
