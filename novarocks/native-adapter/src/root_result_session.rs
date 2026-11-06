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

//! A finite BE root producer; publication and consumption have separate owners.

use novarocks_execution::runtime::task::{ExecutionFailure, ExecutionResult};
use std::alloc::Layout;
use std::mem::ManuallyDrop;
#[cfg(test)]
use std::sync::Weak;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

#[cfg(test)]
use novarocks_execution::exec::chunk::Chunk;
use novarocks_execution::exec::chunk::{RootArrayStorageLimits, borrowed_root_chunk_storage};
use novarocks_execution::runtime::observable::{Observable, ObserverSubscription};
use novarocks_execution::runtime::task::io::{
    ResultAbort, ResultWriteAdmission, ResultWriteCredit, RootFailureObservation,
    RootInputAdmission, RootInputAuthority, RootInputPermit, RootInputReservationAdmission,
    RootProducerState, RootResultSession, RootResultWriteSpec, RootWorkExitProgress, TaskIoError,
    TaskIoErrorKind, TaskIoOperation,
};
use novarocks_execution_contract::native_result_support::NativeResultSupportGeometry;
use novarocks_execution_contract::root_lifetime::RootRetentionClose;
use novarocks_result_contract::{
    BorrowedScalarLeaf, FrozenRootOutput, InternalResultDomain, RootOutputKind, RootProfileV1,
    ScalarLeafCursor,
};
use novarocks_result_render::{
    BoundedClientRowEncoder, ClientRowEncoder, RenderTurn, RenderTurnStatus,
};
use novarocks_worker::root_result_channel::{
    RootMetadataReservation, RootProducerExit, RootResultChannel, RootSegmentBuilder,
};

use crate::root_producer_pool::{
    RootCleanupPlan, RootCleanupPredecessors, RootCleanupProgress, RootExitOwner,
    RootProducerExitObservation, RootProducerExitStatus, RootProducerJob, RootProducerRegistration,
    RootProducerReserveFailure, RootProducerTurn, RootProducerWake, root_exit_metadata_bytes,
    weak_core_backing_bytes,
};
pub use crate::root_producer_pool::{RootProducerLimits, RootProducerPool};

use crate::root_cow_selection_codec::{
    CowSelectionEncoder, CowSelectionRecipe, CowSelectionTotals,
};
use crate::root_scalar_container_codec::{NativeScalarContainerEncoder, is_container};
use crate::root_scalar_leaf_codec::{NativeScalarLeafEncoder, ScalarSchemaOwner};
use crate::root_statistics_codec::{
    StatisticsArtifactEncoder, StatisticsCodecStatus, StatisticsCodecTotals,
};
use crate::root_write_commit_codec::{WriteCommitEncoder, WriteCommitTotals};

#[derive(Debug)]
enum RootSessionFailure {
    StatePoisoned,
    StatisticsScratch,
    WriteScratch,
    ScalarCardinality,
    RendererScratch,
    NoInputCursor,
    ScalarSegment,
    ScalarBytesWithoutRow,
    ScalarRowWithoutRecord,
    NoRowsIncomplete,
    InputClosed,
    ForeignPermit,
    InputNotAccepting,
    ProducerClosed,
    ObservedStop,
    RowCountWidth {
        requested: usize,
        source: std::num::TryFromIntError,
    },
}
impl std::fmt::Display for RootSessionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StatePoisoned => f.write_str("root producer state was poisoned"),
            Self::StatisticsScratch => {
                f.write_str("statistics cursor scratch exceeds its pregrant")
            }
            Self::WriteScratch => f.write_str("write commit cursor scratch exceeds its pregrant"),
            Self::ScalarCardinality => f.write_str("scalar root input has more than one row"),
            Self::RendererScratch => f.write_str("root renderer scratch exceeds its pregrant"),
            Self::NoInputCursor => f.write_str("root purpose has no input cursor"),
            Self::ScalarSegment => f.write_str("scalar record exceeds one root segment"),
            Self::ScalarBytesWithoutRow => {
                f.write_str("scalar root has bytes without an accepted row")
            }
            Self::ScalarRowWithoutRecord => {
                f.write_str("scalar root accepted a row without its record")
            }
            Self::NoRowsIncomplete => {
                f.write_str("scalar NoRows record did not complete in its admitted output")
            }
            Self::InputClosed => f.write_str("root input is closed"),
            Self::ForeignPermit => {
                f.write_str("root input permit belongs to a different issuer or generation")
            }
            Self::InputNotAccepting => f.write_str("root input position is not accepting"),
            Self::ProducerClosed => f.write_str("root producer is closed"),
            Self::ObservedStop => f.write_str("root producer was stopped by its host"),
            Self::RowCountWidth { requested, .. } => {
                write!(f, "root row count exceeds its u64 carrier: {requested}")
            }
        }
    }
}
impl std::error::Error for RootSessionFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::RowCountWidth { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl novarocks_execution_contract::failure::FailureProjection for RootSessionFailure {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        let (class, code) = match self {
            Self::StatePoisoned => (FailureClass::Unexpected, 500),
            Self::StatisticsScratch => (FailureClass::Invariant, 501),
            Self::WriteScratch => (FailureClass::Invariant, 502),
            Self::ScalarCardinality => (FailureClass::Data, 503),
            Self::RendererScratch => (FailureClass::Invariant, 504),
            Self::NoInputCursor => (FailureClass::Invariant, 505),
            Self::ScalarSegment => (FailureClass::Invariant, 506),
            Self::ScalarBytesWithoutRow => (FailureClass::Invariant, 507),
            Self::ScalarRowWithoutRecord => (FailureClass::Invariant, 508),
            Self::NoRowsIncomplete => (FailureClass::Invariant, 509),
            Self::InputClosed => (FailureClass::Control, 512),
            Self::ForeignPermit => (FailureClass::Invariant, 513),
            Self::InputNotAccepting => (FailureClass::Invariant, 514),
            Self::ProducerClosed => (FailureClass::Control, 515),
            Self::ObservedStop => (FailureClass::Control, 516),
            Self::RowCountWidth { .. } => (FailureClass::Representation, 522),
        };
        let fact = FailureFact::new(
            class,
            FailureReason::new(domain::NATIVE, std::num::NonZeroU16::new(code).unwrap()),
        );
        if matches!(
            self,
            Self::InputClosed | Self::ProducerClosed | Self::ObservedStop
        ) {
            return FailureFact::stop_observation(fact.reason());
        }
        if matches!(self, Self::RowCountWidth { .. }) {
            fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::MinimumUnit,
                layout: RepresentationLayout {
                    domain: domain::WORKER,
                    code: std::num::NonZeroU16::new(1).unwrap(),
                },
                required: None,
                extent_limit: Some(u64::MAX),
                representable_prefix: None,
            })
        } else {
            fact
        }
    }
}
impl From<RootSessionFailure> for ExecutionFailure {
    fn from(cause: RootSessionFailure) -> Self {
        Self::from_domain(cause)
    }
}
enum InputEncoder {
    Client(Box<ClientRowEncoder>),
    Statistics(Box<StatisticsArtifactEncoder>),
    WriteCommit(Box<WriteCommitEncoder>),
    CowSelection(Box<CowSelectionEncoder>),
    ScalarLeaf(Box<NativeScalarLeafEncoder>),
    ScalarContainer(Box<NativeScalarContainerEncoder>),
    /// A schema-validated empty container batch: no record, no row.
    ScalarEmpty,
}
impl InputEncoder {
    fn step(&mut self, output: &mut [u8]) -> ExecutionResult<RenderTurn> {
        match self {
            Self::Client(encoder) => encoder.step(output).map_err(ExecutionFailure::from_domain),
            Self::WriteCommit(encoder) => {
                encoder.step(output).map_err(ExecutionFailure::from_domain)
            }
            Self::CowSelection(encoder) => Ok(encoder.step(output)),
            Self::ScalarLeaf(encoder) => {
                encoder.step(output).map_err(ExecutionFailure::from_domain)
            }
            Self::ScalarContainer(encoder) => Ok(encoder.step(output)),
            Self::ScalarEmpty => Ok(RenderTurn {
                emitted_bytes: 0,
                examined_bytes: 0,
                visited_cells: 0,
                completed_rows: 0,
                status: RenderTurnStatus::InputComplete,
            }),
            Self::Statistics(encoder) => encoder
                .step(output)
                .map(|turn| RenderTurn {
                    emitted_bytes: turn.emitted_bytes,
                    examined_bytes: turn.examined_bytes,
                    visited_cells: turn.work,
                    completed_rows: turn.completed_rows,
                    status: match turn.status {
                        StatisticsCodecStatus::Yielded => RenderTurnStatus::Yielded,
                        StatisticsCodecStatus::NeedsOutput => RenderTurnStatus::NeedsOutput,
                        StatisticsCodecStatus::InputComplete => RenderTurnStatus::InputComplete,
                    },
                })
                .map_err(ExecutionFailure::from_domain),
        }
    }
    fn statistics_totals(&self) -> Option<StatisticsCodecTotals> {
        match self {
            Self::Statistics(encoder) => Some(encoder.totals()),
            _ => None,
        }
    }
    fn cow_selection_totals(&self) -> Option<CowSelectionTotals> {
        match self {
            Self::CowSelection(encoder) => Some(encoder.totals()),
            _ => None,
        }
    }
    fn write_commit_totals(&self) -> Option<WriteCommitTotals> {
        match self {
            Self::WriteCommit(encoder) => Some(encoder.totals()),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InputExitPhase {
    Encoder,
    Chunk,
    Scratch,
    Permit,
}
impl InputExitPhase {
    const ALL: [Self; 4] = [Self::Encoder, Self::Chunk, Self::Scratch, Self::Permit];
    fn index(self) -> usize {
        match self {
            Self::Encoder => 0,
            Self::Chunk => 1,
            Self::Scratch => 2,
            Self::Permit => 3,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InputOwnerExit {
    NotStarted,
    Started,
    Exited,
    Unresolved,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InputExecution {
    Idle,
    Constructing,
    Rendering,
}
struct Input {
    encoder: Option<std::mem::ManuallyDrop<InputEncoder>>,
    scratch: Option<std::mem::ManuallyDrop<ResultWriteCredit>>,
    chunk: Option<std::mem::ManuallyDrop<novarocks_execution::exec::chunk::Chunk>>,
    _permit:
        Option<std::mem::ManuallyDrop<novarocks_execution::runtime::task::io::RootInputPermit>>,
    retiring: bool,
    execution: InputExecution,
    exits: [InputOwnerExit; 4],
}
impl Input {
    fn new(submitted: novarocks_execution::runtime::task::io::RootResultInput) -> Self {
        let (chunk, permit) = submitted.into_cleanup_parts();
        Self {
            encoder: None,
            scratch: None,
            chunk: Some(std::mem::ManuallyDrop::new(chunk)),
            _permit: Some(std::mem::ManuallyDrop::new(permit)),
            retiring: false,
            execution: InputExecution::Idle,
            exits: [InputOwnerExit::NotStarted; 4],
        }
    }
    fn chunk(&self) -> &novarocks_execution::exec::chunk::Chunk {
        self.chunk
            .as_ref()
            .expect("live input retains original backing")
    }
    fn data_exited(&self) -> bool {
        self.execution == InputExecution::Idle
            && self.exits[0] == InputOwnerExit::Exited
            && self.exits[1] == InputOwnerExit::Exited
    }
    fn fully_exited(&self) -> bool {
        self.exits
            .iter()
            .all(|state| *state == InputOwnerExit::Exited)
    }
    /// Called only while the original ProducerState mutex is held. Record
    /// Started before taking a single field. The action is executed after
    /// unlocking; a destructor unwind cannot implicitly drop another field.
    fn take_exit_action(&mut self, phase: InputExitPhase) -> Option<InputExitAction> {
        self.retiring = true;
        // A cursor/normalizer outside the lock still touches original data.
        // Empty local fields do not establish that this work has exited.
        if self.execution != InputExecution::Idle {
            return None;
        }
        if self.exits[phase.index()] != InputOwnerExit::NotStarted {
            return None;
        }
        if matches!(phase, InputExitPhase::Scratch | InputExitPhase::Permit) && !self.data_exited()
        {
            // These are protection responsibilities for the unknown data
            // holder, not independent siblings eligible for early cleanup.
            return None;
        }
        self.exits[phase.index()] = InputOwnerExit::Started;
        Some(match phase {
            InputExitPhase::Encoder => InputExitAction::Encoder(self.encoder.take()),
            InputExitPhase::Chunk => InputExitAction::Chunk(self.chunk.take()),
            InputExitPhase::Scratch => InputExitAction::Scratch(self.scratch.take()),
            InputExitPhase::Permit => InputExitAction::Permit(self._permit.take()),
        })
    }
    fn note_exit(&mut self, phase: InputExitPhase) {
        assert_eq!(self.exits[phase.index()], InputOwnerExit::Started);
        self.exits[phase.index()] = InputOwnerExit::Exited;
    }
}
enum InputExitAction {
    Encoder(Option<std::mem::ManuallyDrop<InputEncoder>>),
    Chunk(Option<std::mem::ManuallyDrop<novarocks_execution::exec::chunk::Chunk>>),
    Scratch(Option<std::mem::ManuallyDrop<ResultWriteCredit>>),
    Permit(Option<std::mem::ManuallyDrop<novarocks_execution::runtime::task::io::RootInputPermit>>),
}
enum InputExitOutcome {
    Exited,
    RetainedPermit(std::mem::ManuallyDrop<RootInputPermit>),
}
impl InputExitAction {
    fn run(self) -> InputExitOutcome {
        match self {
            Self::Encoder(owner) => drop(owner.map(std::mem::ManuallyDrop::into_inner)),
            Self::Chunk(owner) => drop(owner.map(std::mem::ManuallyDrop::into_inner)),
            Self::Scratch(owner) => drop(owner.map(std::mem::ManuallyDrop::into_inner)),
            Self::Permit(Some(mut owner)) => match owner.retire() {
                RootWorkExitProgress::Exited => drop(std::mem::ManuallyDrop::into_inner(owner)),
                RootWorkExitProgress::Unresolved => return InputExitOutcome::RetainedPermit(owner),
                RootWorkExitProgress::Pending => {
                    unreachable!("synchronous grant exit has a final exact receipt")
                }
            },
            Self::Permit(None) => {}
        }
        InputExitOutcome::Exited
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FailureSourcePhase {
    Pending,
    Started,
    Transferred,
    Disposed,
    Unresolved,
}
/// One first-wins record. Neutral metadata survives the original source's
/// transfer; taking that source never turns a failure into normal completion.
struct RootFailureRecord {
    observation: Option<RootFailureObservation>,
    live_source: Option<ManuallyDrop<ExecutionFailure>>,
    phase: FailureSourcePhase,
}
impl RootFailureRecord {
    fn new(source: ExecutionFailure) -> Self {
        Self {
            observation: None,
            live_source: Some(ManuallyDrop::new(source)),
            phase: FailureSourcePhase::Pending,
        }
    }
    fn begin_action(&mut self) -> Result<Option<ManuallyDrop<ExecutionFailure>>, ()> {
        match self.phase {
            FailureSourcePhase::Transferred | FailureSourcePhase::Disposed => Ok(None),
            FailureSourcePhase::Started | FailureSourcePhase::Unresolved => Err(()),
            FailureSourcePhase::Pending => {
                let Some(source) = self.live_source.take() else {
                    self.phase = FailureSourcePhase::Unresolved;
                    return Err(());
                };
                self.phase = FailureSourcePhase::Started;
                Ok(Some(source))
            }
        }
    }
    fn finish_action(&mut self, phase: FailureSourcePhase) {
        assert_eq!(
            self.phase,
            FailureSourcePhase::Started,
            "one original source action"
        );
        assert!(
            self.live_source.is_none(),
            "source responsibility has moved"
        );
        self.phase = phase;
    }
    fn restore_unresolved(&mut self, source: Option<ManuallyDrop<ExecutionFailure>>) {
        // Only Started holds this source slot; other state writers cannot
        // replace the first record or install a second source during its call.
        if let Some(source) = source {
            self.live_source = Some(source);
        }
        self.phase = FailureSourcePhase::Unresolved;
    }
    fn producer_state(&self) -> RootProducerState {
        self.observation
            .as_ref()
            .map_or(RootProducerState::AwaitTerminalControl, |observation| {
                RootProducerState::Failed(observation.clone())
            })
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FailureRecordKind {
    Primary,
    Cleanup,
}
/// The original synchronous producer owns this action. Unwind restores an
/// untransferred opaque to its same fixed record and leaves unknown custody;
/// it never implicitly destroys the source under a metadata lock.
struct FailureSourceAction<'a> {
    owner: &'a RootSessionState,
    kind: FailureRecordKind,
    source: Option<ManuallyDrop<ExecutionFailure>>,
    completed: bool,
}
impl FailureSourceAction<'_> {
    fn source(&self) -> &ExecutionFailure {
        self.source.as_deref().expect("original source action")
    }
    fn take_source(&mut self) -> ExecutionFailure {
        ManuallyDrop::into_inner(self.source.take().expect("one source transfer"))
    }
    fn publish_observation(&self, observation: RootFailureObservation) {
        let mut state = self.owner.state.lock().unwrap_or_else(|p| p.into_inner());
        let record = state
            .failure_record_mut(self.kind)
            .expect("original source record");
        record.observation = Some(observation);
    }
    fn finish(mut self, phase: FailureSourcePhase) {
        let mut state = self.owner.state.lock().unwrap_or_else(|p| p.into_inner());
        state
            .failure_record_mut(self.kind)
            .expect("original source record")
            .finish_action(phase);
        self.completed = true;
    }
}
impl Drop for FailureSourceAction<'_> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let mut state = self.owner.state.lock().unwrap_or_else(|p| p.into_inner());
        let record = state
            .failure_record_mut(self.kind)
            .expect("original source record");
        // A consumed source may already belong to the Task owner; this empty
        // field does not convert its unknown handoff into positive exit.
        record.restore_unresolved(self.source.take());
    }
}

struct ProducerState {
    input: Option<Input>,
    /// Reuse a proof only for the same immutable typed input owner. The cursor
    /// also retains the proof; no whole input or query is kept by this cache.
    client_types: Option<novarocks_result_render::ValidatedClientInput>,
    builder: Option<RootSegmentBuilder>,
    used: usize,
    statistics_totals: StatisticsCodecTotals,
    /// PreparedWriteCommitV1 records completed across every input batch.
    write_totals: WriteCommitTotals,
    /// CowSelectionArrowV1 complete batch records completed so far.
    cow_totals: CowSelectionTotals,
    /// ScalarValueV1 rows accepted across every input batch: 0 or 1. Its
    /// record stays in the unpublished segment until the sealed normal End.
    scalar_rows: u8,
    sealed: bool,
    /// The producer published its normal End. Its terminal is final: a later
    /// cancel (for example a pool shutdown racing the job's removal) cannot
    /// turn a completed producer into a failed one.
    completed: bool,
    /// The original pool has entered cleanup, including cold cancellation.
    /// This seals further work; it is not an actual exit receipt.
    cleanup_started: bool,
    // Rollback is not a runtime result terminal. Observation follows actual exit.
    abort_is_rollback: bool,
    terminal_observed: bool,
    failed: Option<RootFailureRecord>,
    cleanup_failure: Option<RootFailureRecord>,
    producer: Option<RootProducerExit>,
}
impl ProducerState {
    fn failure_record_mut(&mut self, kind: FailureRecordKind) -> Option<&mut RootFailureRecord> {
        match kind {
            FailureRecordKind::Primary => self.failed.as_mut(),
            FailureRecordKind::Cleanup => self.cleanup_failure.as_mut(),
        }
    }
    fn take_terminal_observation(&mut self) -> Option<&'static str> {
        if self.terminal_observed {
            return None;
        }
        let terminal = if self.completed {
            "finished"
        } else if self.failed.is_some() && !self.abort_is_rollback {
            "aborted"
        } else {
            return None;
        };
        self.terminal_observed = true;
        Some(terminal)
    }
}
// A notification snapshot can outlive the session subscription. Its guard
// follows the real callback and its Weak control tails through that exit.
struct WakeBridge {
    wake: RootProducerWake,
    input_readiness: novarocks_execution::runtime::task::io::RootInputReadinessNotifier,
    _metadata: Arc<RootMetadataReservation>,
}
impl WakeBridge {
    fn notify(&self) {
        let _ = self.wake.wake_active();
        self.input_readiness.notify();
    }
}

type RootFailureReporter = Arc<novarocks_execution::runtime::task_control::TaskExecutionControl>;

/// Driver calls transfer one original input. Rendering runs only on the shared
/// fixed CPU pool; neither a full queue nor a pending ACK occupies that pool.
/// The composition retains the pool and explicitly shuts it down before its
/// driver executor. Runtime Task owners retain the channel, not this session.
struct RootSessionState {
    cow_recipe: Option<Arc<CowSelectionRecipe>>,
    channel: Arc<RootResultChannel>,
    authority: RootInputAuthority,
    state: Mutex<ProducerState>,
    wake: RootProducerWake,
    // Actual producer work owns this capability through its real exit. Late
    // metadata/output aliases see None and cannot retain or report to a Task.
    failure_reporter: Mutex<Option<RootFailureReporter>>,
    #[cfg(test)]
    terminal_observations: Mutex<Vec<&'static str>>,
    #[cfg(test)]
    before_finish_wake: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    before_pool_turn: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    after_cleanup_close: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    _metadata: Arc<RootMetadataReservation>,
}
pub struct NativeRootResultSession {
    shared: Arc<RootSessionState>,
    _registration: RootProducerRegistration,
    _wake_subscription: ObserverSubscription,
    exit: RootProducerExitObservation,
    exit_observable: Arc<Observable>,
}

// The pool owns this work independently of the externally retained facade.
// The facade's guard only requests cancellation; it never publishes exit.
struct NativeRootWork {
    shared: Arc<RootSessionState>,
}

// Four distinct input phases allow independent data holders to exit even if
// another destructor unwinds. The original protection grants depend on both.
const ROOT_CLEANUP_PHASES: &[u16] = &[0, 1, 2, 3, 4, 5, 6, 7, 8, 14, 15, 9, 10, 11, 12, 13];
const ROOT_CLEANUP: RootCleanupPlan = RootCleanupPlan::new(ROOT_CLEANUP_PHASES);

/// Empty payload fields are not exit receipts: their destructors may have
/// unwound after taking ownership. The pool owns the actual completed prefix.
pub(crate) fn encoder_cleanup_ready(
    predecessors: RootCleanupPredecessors,
    input_present: bool,
    builder_present: bool,
) -> bool {
    predecessors.all_exited() && !input_present && !builder_present
}

/// A completed CPU turn is a scheduling fact. Only the channel's published
/// End supplies the positive result fact, independently of retained sources.
pub(crate) fn turn_has_published_end(turn: RootProducerTurn, end_sequence: Option<u64>) -> bool {
    turn == RootProducerTurn::Complete && end_sequence.is_some()
}

struct NativeRootPreparationChild {
    authority: RootInputAuthority,
    observation: RootProducerExitObservation,
    preparation_cleared: AtomicBool,
    wake: RootProducerWake,
    readiness: std::task::Waker,
}
impl novarocks_worker::TaskPreparationChild for NativeRootPreparationChild {
    fn request_rollback(&self) {
        let _ = self.wake.request_cancel();
    }
    fn poll_exit(&self) -> novarocks_worker::TaskPreparationChildExit {
        use novarocks_worker::TaskPreparationChildExit as Exit;
        let pool = match self
            .observation
            .poll(RootExitOwner::Preparation, &self.readiness)
        {
            Ok(status) => status,
            Err(_) => return Exit::Unresolved,
        };
        let pool = match pool {
            RootProducerExitStatus::Pending => RootWorkExitProgress::Pending,
            RootProducerExitStatus::NoJobInstalled | RootProducerExitStatus::Exited => {
                RootWorkExitProgress::Exited
            }
            RootProducerExitStatus::Unresolved => RootWorkExitProgress::Unresolved,
        };
        match join_root_exit_progress(pool, self.authority.poll_input_exit(&self.readiness)) {
            RootWorkExitProgress::Pending => Exit::Pending,
            // This original producer/input observation does not include this
            // child's own metadata or retained Root data. The outer owner
            // observes full output only after actually dropping this child.
            RootWorkExitProgress::Exited => Exit::Exited {
                output: novarocks_worker::TaskPreparationOutputExit::Unknown,
            },
            RootWorkExitProgress::Unresolved => Exit::Unresolved,
        }
    }

    fn adopt_runtime(&self) {
        if !self.preparation_cleared.swap(true, Ordering::AcqRel) {
            self.observation.clear_preparation();
        }
    }
}
impl NativeRootResultSession {
    #[cfg(test)]
    pub(crate) fn try_open_prepared(
        channel: Arc<RootResultChannel>,
        pool: &Arc<RootProducerPool>,
        preparation: &novarocks_worker::TaskPreparationContext,
        preparation_child: &Arc<
            crate::backend_task_execution::prepared_owner::NativePreparationChild,
        >,
    ) -> Result<Arc<Self>, TaskIoError> {
        Self::try_open_prepared_with_cow_recipe(channel, pool, preparation, preparation_child, None)
    }
    pub(crate) fn try_open_prepared_with_cow_recipe(
        channel: Arc<RootResultChannel>,
        pool: &Arc<RootProducerPool>,
        preparation: &novarocks_worker::TaskPreparationContext,
        preparation_child: &Arc<
            crate::backend_task_execution::prepared_owner::NativePreparationChild,
        >,
        cow_recipe: Option<Arc<CowSelectionRecipe>>,
    ) -> Result<Arc<Self>, TaskIoError> {
        let control = preparation.runtime_control();
        if control.identity() != channel.spec().task
            || preparation_child.identity() != channel.spec().task
            || !preparation_child.root_birth_matches(&channel)
        {
            return Err(io_error(
                novarocks_execution::runtime::task_control::TaskControlIdentityMismatch,
            ));
        }
        let cow = channel.spec().contract.kind()
            == RootOutputKind::InternalFacts(InternalResultDomain::CowSelectionArrowV1);
        if cow != cow_recipe.is_some() {
            return Err(io_error(
                crate::root_cow_selection_codec::CowSelectionCodecError::RecipeMismatch,
            ));
        }
        Self::open_in_scope(
            channel,
            pool,
            control,
            preparation_child,
            preparation.readiness_waker(),
            cow_recipe,
        )
    }

    fn open_in_scope(
        channel: Arc<RootResultChannel>,
        pool: &Arc<RootProducerPool>,
        failure_reporter: RootFailureReporter,
        preparation: &Arc<crate::backend_task_execution::prepared_owner::NativePreparationChild>,
        readiness: std::task::Waker,
        cow_recipe: Option<Arc<CowSelectionRecipe>>,
    ) -> Result<Arc<Self>, TaskIoError> {
        let contract = &channel.spec().contract;
        // Every closed internal domain has its producer codec; only a scalar
        // domain identity without its typed schema has none.
        contract.validate_purpose().map_err(io_error)?;
        // All fixed session/issuer/callback scaffolds are covered before
        // creating them. Cursor/batch-clone heaps use separate scratch credit.
        #[repr(C, align(2))]
        struct Header {
            strong: AtomicUsize,
            weak: AtomicUsize,
        }
        fn arc<T>() -> usize {
            Layout::new::<Header>()
                .extend(Layout::new::<T>())
                .expect("fixed Arc layout fits usize")
                .0
                .pad_to_align()
                .size()
        }
        let metadata_bytes = arc::<Self>()
            + arc::<RootSessionState>()
            + arc::<NativeRootWork>()
            + arc::<NativeRootPreparationChild>()
            + root_exit_metadata_bytes(ROOT_CLEANUP).map_err(|source| {
                io_error(RootProducerReserveFailure::Layout {
                    steps: ROOT_CLEANUP_PHASES.len(),
                    source,
                })
            })?
            + novarocks_execution::exec::operators::RootResultSinkFactory::metadata_capacity_bytes(
                NativeResultSupportGeometry::V1.root_maximum_root_drivers as usize,
            )
            .map_err(io_error)?
            + RootInputAuthority::initial_backing_bytes()
            + arc::<WakeBridge>()
            + arc::<Arc<dyn Fn() + Send + Sync>>()
            + 2 * std::mem::size_of::<std::sync::Weak<Arc<dyn Fn() + Send + Sync>>>()
            + arc::<RootMetadataReservation>()
            + weak_core_backing_bytes()
            // Two persistent summaries plus one serial freeze action's
            // bounded rendering/normalization temporaries, never a source.
            + 4 * novarocks_execution_contract::status::SAFE_DETAIL_MAX_BYTES;
        #[cfg(target_vendor = "apple")]
        let metadata_bytes = metadata_bytes + 3 * Layout::new::<(isize, [u8; 56])>().size();
        let metadata = channel
            .try_reserve_metadata(metadata_bytes)
            .map_err(io_error)?;
        let metadata = Arc::new(metadata);
        let reservation = pool
            .reserve(ROOT_CLEANUP, Arc::clone(&metadata))
            .map_err(io_error)?;
        let exit = reservation.exit_observation();
        let wake = reservation.wake_handle();
        let authority = RootInputAuthority::try_new_retained(
            channel.spec(),
            pool.execution_health(),
            Arc::clone(&metadata) as Arc<dyn std::any::Any + Send + Sync>,
        )?;
        {
            let mut child: Option<Arc<dyn novarocks_worker::TaskPreparationChild>> =
                Some(Arc::new(NativeRootPreparationChild {
                    authority: authority.clone(),
                    observation: exit.clone(),
                    preparation_cleared: AtomicBool::new(false),
                    wake: wake.clone(),
                    readiness,
                }));
            preparation.install_root(&mut child).map_err(io_error)?;
        }
        let exit_observable = Arc::new(
            Observable::try_bounded_retained(
                NativeResultSupportGeometry::V1.root_maximum_root_drivers as usize,
                Arc::clone(&metadata) as Arc<dyn std::any::Any + Send + Sync>,
            )
            .map_err(io_error)?,
        );
        let shared = Arc::new(RootSessionState {
            cow_recipe,
            channel,
            authority,
            state: Mutex::new(ProducerState {
                input: None,
                client_types: None,
                builder: None,
                used: 0,
                statistics_totals: StatisticsCodecTotals::default(),
                write_totals: WriteCommitTotals::default(),
                cow_totals: CowSelectionTotals::default(),
                scalar_rows: 0,
                sealed: false,
                completed: false,
                cleanup_started: false,
                abort_is_rollback: false,
                terminal_observed: false,
                failed: None,
                cleanup_failure: None,
                producer: None,
            }),
            wake: wake.clone(),
            failure_reporter: Mutex::new(Some(failure_reporter)),
            #[cfg(test)]
            terminal_observations: Mutex::new(Vec::new()),
            #[cfg(test)]
            before_finish_wake: Mutex::new(None),
            #[cfg(test)]
            before_pool_turn: Mutex::new(None),
            #[cfg(test)]
            after_cleanup_close: Mutex::new(None),
            _metadata: metadata,
        });
        let bridge = WakeBridge {
            wake,
            input_readiness: shared.authority.readiness_notifier(),
            _metadata: Arc::clone(&shared._metadata),
        };
        let subscription = shared
            .channel
            .writable_observable()
            .try_subscribe_retained(
                Arc::new(move || {
                    // No session/job mutex is taken here. Readiness callbacks can run
                    // synchronously while a producer destroys input or returns credit.
                    bridge.notify();
                }),
                Arc::clone(&shared._metadata) as Arc<dyn std::any::Any + Send + Sync>,
            )
            .map_err(io_error)?;
        let work = Arc::new(NativeRootWork {
            shared: shared.clone(),
        }) as Arc<dyn RootProducerJob>;
        let registration = match reservation.commit(work) {
            Ok(registration) => registration,
            Err(refusal) => {
                let cause = refusal.cause;
                // No input, cursor or builder exists before commit. Actual
                // destruction stays in the original synchronous parent body.
                drop(refusal.work);
                drop(subscription);
                drop(shared);
                return Err(io_error(cause));
            }
        };
        Ok(Arc::new(Self {
            shared,
            _registration: registration,
            _wake_subscription: subscription,
            exit,
            exit_observable,
        }))
    }
    pub fn channel(&self) -> &Arc<RootResultChannel> {
        &self.shared.channel
    }
    /// Observe the pool's receipt without registering another watch owner.
    /// Synchronous callbacks must never wait for their own running CPU job.
    pub fn producer_exit_progress(&self) -> RootWorkExitProgress {
        let pool = match self.exit.status() {
            RootProducerExitStatus::Pending => RootWorkExitProgress::Pending,
            RootProducerExitStatus::Exited => RootWorkExitProgress::Exited,
            RootProducerExitStatus::NoJobInstalled | RootProducerExitStatus::Unresolved => {
                RootWorkExitProgress::Unresolved
            }
        };
        join_root_exit_progress(pool, self.shared.authority.input_exit_progress())
    }
    /// Register only the exact original pool cleanup phase, not a generic
    /// writable notification which may have another physical drainer.
    #[cfg(test)]
    pub(crate) fn observe_cleanup_close_for_test(&self, observer: Box<dyn FnOnce() + Send>) {
        let mut original = self.shared.after_cleanup_close.lock().unwrap();
        assert!(original.is_none(), "one exact cleanup-close observer");
        *original = Some(observer);
    }
    #[cfg(test)]
    pub(crate) fn assert_scalar_origin_handoff_at_cleanup(
        &self,
    ) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::FailureProjection;
        let state = self
            .shared
            .state
            .lock()
            .expect("the actual cleanup callback reenters the producer mutex");
        assert!(
            state.cleanup_started,
            "the original pool cleanup has started"
        );
        let failed = state
            .failed
            .as_ref()
            .expect("the original scalar failure remains observable at cleanup");
        assert_eq!(failed.phase, FailureSourcePhase::Transferred);
        assert!(
            failed.live_source.is_none(),
            "the producer handed off its unique E before releasing its local input"
        );
        let fact = failed
            .observation
            .as_ref()
            .expect("the original handoff published its neutral failure observation")
            .fact();
        assert_eq!(fact, RootSessionFailure::ScalarCardinality.failure_fact());
        drop(state);
        assert_eq!(
            self.producer_exit_progress(),
            RootWorkExitProgress::Pending,
            "the actual producer has not exited at its own cleanup gate"
        );
        fact
    }
    #[cfg(test)]
    pub(crate) fn assert_scalar_origin_stop_reentry(&self, origin: &ExecutionFailure) {
        use novarocks_execution_contract::failure::FailureProjection;
        // Source projection and error-chain inspection also stay outside the
        // two real Root locks whose reentry the observer is verifying.
        let fact = origin.failure_fact();
        let mut source: Option<&(dyn std::error::Error + 'static)> = Some(origin);
        let mut original_scalar = false;
        while let Some(error) = source {
            if let Some(original) = error.downcast_ref::<RootSessionFailure>() {
                assert!(matches!(original, RootSessionFailure::ScalarCardinality));
                original_scalar = true;
                break;
            }
            source = error.source();
        }
        assert!(
            original_scalar,
            "the actual original scalar codec source reaches the Task"
        );
        // These actual callbacks must reenter both mutexes. A different
        // legitimate reader is not evidence that this callback owns a lock.
        // The original receiver/Node deadlines still bound a reentry failure.
        drop(
            self.shared
                .failure_reporter
                .lock()
                .expect("the actual Task callback reenters the reporter mutex"),
        );
        let state = self
            .shared
            .state
            .lock()
            .expect("the actual Task callback reenters the producer mutex");
        let failed = state
            .failed
            .as_ref()
            .expect("the original producer failure remains observable");
        assert_eq!(failed.phase, FailureSourcePhase::Transferred);
        assert!(
            failed.live_source.is_none(),
            "Root transferred its unique original source"
        );
        assert_eq!(failed.observation.as_ref().unwrap().fact(), fact);
        assert!(matches!(
            failed.producer_state(),
            RootProducerState::Failed(_)
        ));
        drop(state);
        assert_eq!(
            self.producer_exit_progress(),
            RootWorkExitProgress::Pending,
            "the observer reenters while the original producer cleanup is still physically gated"
        );
    }
    #[cfg(test)]
    fn state(&self) -> MutexGuard<'_, ProducerState> {
        self.shared.state()
    }
    #[cfg(test)]
    fn fail(&self, cause: ExecutionFailure) {
        self.shared.capture_failure(cause);
    }
}
impl RootSessionState {
    fn spec(&self) -> &RootResultWriteSpec {
        self.channel.spec()
    }
    fn observe_terminal(&self, terminal: &'static str) {
        crate::backend_metrics::record_task_result_terminal(terminal);
        #[cfg(test)]
        self.terminal_observations
            .lock()
            .expect("root terminal observations lock")
            .push(terminal);
    }

    fn state(&self) -> MutexGuard<'_, ProducerState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.completed |= self.channel.snapshot().end_sequence.is_some();
                if state.completed {
                    if state.cleanup_failure.is_none() {
                        state.cleanup_failure = Some(RootFailureRecord::new(
                            RootSessionFailure::StatePoisoned.into(),
                        ));
                    }
                } else {
                    if state.failed.is_none() {
                        state.failed = Some(RootFailureRecord::new(
                            RootSessionFailure::StatePoisoned.into(),
                        ));
                    }
                }
                state.sealed = true;
                // The following real pool turn still destroys input/cursor and
                // calls exited. Recovery never equates a panic with reclamation.
                self.state.clear_poison();
                state
            }
        }
    }

    fn wake(&self) -> Result<(), TaskIoError> {
        self.wake.wake().map_err(io_error)
    }
    fn start(&self, state: &mut ProducerState) -> Result<(), TaskIoError> {
        if state.producer.is_none() && !state.cleanup_started {
            state.producer = Some(self.channel.start_producer().map_err(io_error)?);
        }
        Ok(())
    }
    /// A real producer return supplies the original source outside its state
    /// lock. A later contender is disposed on that original supervised stack.
    fn capture_failure(&self, cause: ExecutionFailure) {
        let mut cause = Some(cause);
        {
            let mut state = self.state();
            state.completed |= self.channel.snapshot().end_sequence.is_some();
            let record = if state.completed {
                // A published normal End cannot be replaced by a later fault.
                &mut state.cleanup_failure
            } else {
                &mut state.failed
            };
            if record.is_none() {
                *record = Some(RootFailureRecord::new(cause.take().unwrap()));
            }
            state.sealed = true;
        }
        drop(cause);
    }
    fn retire_failure_source(&self, kind: FailureRecordKind) -> RootCleanupProgress {
        let (source, rollback) = {
            let mut state = self.state();
            let rollback = state.abort_is_rollback;
            let Some(record) = state.failure_record_mut(kind) else {
                return RootCleanupProgress::Exited;
            };
            let source = match record.begin_action() {
                Ok(Some(source)) => source,
                Ok(None) => return RootCleanupProgress::Exited,
                Err(()) => return RootCleanupProgress::Unresolved,
            };
            (source, rollback)
        };
        let mut action = FailureSourceAction {
            owner: self,
            kind,
            source: Some(source),
            completed: false,
        };
        // Projection, Task handoff and opaque destruction are all outside
        // both producer/reporter locks. This owner supplies the only live E.
        let observation = RootFailureObservation::from_failure(action.source());
        action.publish_observation(observation);
        if kind == FailureRecordKind::Primary && !rollback {
            let reporter = self
                .failure_reporter
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            let Some(reporter) = reporter else {
                return RootCleanupProgress::Unresolved;
            };
            reporter.report_failure(action.take_source());
            drop(reporter);
            action.finish(FailureSourcePhase::Transferred);
        } else {
            drop(action.take_source());
            action.finish(FailureSourcePhase::Disposed);
        }
        RootCleanupProgress::Exited
    }

    fn advance(&self, state: &mut ProducerState) -> ExecutionResult<RootProducerTurn> {
        if state.failed.is_some() {
            return Ok(RootProducerTurn::Complete);
        }
        if self.channel.is_closed() {
            return Err(RootSessionFailure::ObservedStop.into());
        }
        if let Some(input) = state.input.as_mut() {
            if self.spec().contract.kind() == RootOutputKind::CountOnly {
                let requested = input.chunk().len();
                let rows = match u64::try_from(requested) {
                    Ok(rows) => rows,
                    Err(source) => {
                        return Err(
                            (RootSessionFailure::RowCountWidth { requested, source }).into()
                        );
                    }
                };
                if let Err(cause) = self.channel.note_rows(rows) {
                    return Err(cause.into_execution());
                }
                state.input.as_mut().unwrap().retiring = true;
                return Ok(RootProducerTurn::Yielded);
            }
            if input.encoder.is_none() {
                let capacity = NativeResultSupportGeometry::V1.root_scratch_capacity_bytes as usize;
                let credit = match self.channel.try_reserve(capacity) {
                    Ok(ResultWriteAdmission::Granted(credit)) => credit,
                    Ok(ResultWriteAdmission::Blocked) => return Ok(RootProducerTurn::Blocked),
                    Err(cause) => return Err(cause.into_execution()),
                };
                input.scratch = Some(std::mem::ManuallyDrop::new(credit));
                // This clone only allocates the finite columns Vec. Original
                // Chunk accounting/source capabilities remain live in input.
                let cloning = input.chunk().record_batch().num_columns()
                    * std::mem::size_of::<arrow::array::ArrayRef>();
                input.execution = InputExecution::Constructing;
                let construction = self.begin_encoder(
                    input,
                    capacity,
                    cloning,
                    &mut state.scalar_rows,
                    state.statistics_totals,
                    state.write_totals,
                    &mut state.client_types,
                );
                input.execution = InputExecution::Idle;
                let encoder = construction?;
                input.encoder = Some(std::mem::ManuallyDrop::new(encoder));
                return Ok(RootProducerTurn::Yielded);
            }
            if state.builder.is_none() {
                match self.channel.try_segment() {
                    Ok(Some(builder)) => {
                        state.builder = Some(builder);
                        state.used = 0;
                    }
                    Ok(None) => return Ok(RootProducerTurn::Blocked),
                    Err(cause) => return Err(cause.into_execution()),
                }
            }
            input.execution = InputExecution::Rendering;
            let rendering = input
                .encoder
                .as_mut()
                .unwrap()
                .step(state.builder.as_mut().unwrap().output_at(state.used));
            input.execution = InputExecution::Idle;
            let turn = rendering?;
            state.used += turn.emitted_bytes;
            if let Err(cause) = self.channel.note_rows(turn.completed_rows) {
                return Err(cause.into_execution());
            }
            let input_complete = turn.status == RenderTurnStatus::InputComplete;
            if self.scalar() {
                // The whole record fits one segment and stays unpublished:
                // only the sealed normal End publishes it (or NoRows).
                if turn.status == RenderTurnStatus::NeedsOutput {
                    return Err(RootSessionFailure::ScalarSegment.into());
                }
                if input_complete {
                    state.input.as_mut().unwrap().retiring = true;
                }
                return Ok(RootProducerTurn::Yielded);
            }
            if input_complete {
                let encoder = input.encoder.as_ref().unwrap();
                if let Some(totals) = encoder.statistics_totals() {
                    state.statistics_totals = totals;
                }
                if let Some(totals) = encoder.write_commit_totals() {
                    state.write_totals = totals;
                }
                if let Some(totals) = encoder.cow_selection_totals() {
                    state.cow_totals = totals;
                }
                state.input.as_mut().unwrap().retiring = true;
            }
            let finishing = input_complete && state.sealed;
            if finishing && let Err(message) = self.sealed_domain_complete(state) {
                return Err(message);
            }
            if finishing && let Err(cause) = self.channel.request_finish() {
                return Err(cause.into_execution());
            }
            let flush = input_complete
                || turn.completed_rows != 0
                || turn.status == RenderTurnStatus::NeedsOutput
                || state.used == RootProfileV1::SEGMENT_BYTES;
            if flush {
                let builder = state.builder.take().unwrap();
                let bytes = std::mem::take(&mut state.used);
                if bytes == 0 {
                    drop(builder);
                } else if let Err(cause) = self.channel.publish_segment(builder, bytes, finishing) {
                    return Err(cause.into_execution());
                }
                if finishing {
                    if bytes == 0
                        && let Err(cause) = self.channel.publish_end()
                    {
                        return Err(cause.into_execution());
                    }
                    return Ok(RootProducerTurn::Complete);
                }
            }
            return Ok(RootProducerTurn::Yielded);
        }
        if state.sealed {
            if self.scalar() {
                return self.finish_scalar(state);
            }
            self.sealed_domain_complete(state)?;
            if let Err(cause) = self
                .channel
                .request_finish()
                .and_then(|_| self.channel.publish_end())
            {
                return Err(cause.into_execution());
            }
            return Ok(RootProducerTurn::Complete);
        }
        Ok(RootProducerTurn::Idle)
    }
    /// One cursor for one original input, chosen by the frozen root purpose.
    /// Every cursor's storage is prepaid by `capacity` before it is created.
    fn begin_encoder(
        &self,
        input: &Input,
        capacity: usize,
        cloning: usize,
        scalar_rows: &mut u8,
        statistics_totals: StatisticsCodecTotals,
        write_totals: WriteCommitTotals,
        client_types: &mut Option<novarocks_result_render::ValidatedClientInput>,
    ) -> ExecutionResult<InputEncoder> {
        let fits = |inline: usize| inline.checked_add(cloning).is_some_and(|n| n <= capacity);
        match self.spec().contract.output() {
            FrozenRootOutput::InternalFacts(InternalResultDomain::StatisticsArtifactV1) => {
                // Fixed cursor storage plus the finite columns Vec clone.
                if !fits(StatisticsArtifactEncoder::inline_capacity_bytes()) {
                    return Err(RootSessionFailure::StatisticsScratch.into());
                }
                StatisticsArtifactEncoder::try_new(
                    input.chunk().record_batch().clone(),
                    statistics_totals,
                )
                .map(|encoder| InputEncoder::Statistics(Box::new(encoder)))
                .map_err(ExecutionFailure::from_domain)
            }
            FrozenRootOutput::InternalFacts(InternalResultDomain::PreparedWriteCommitV1) => {
                if !fits(WriteCommitEncoder::inline_capacity_bytes()) {
                    return Err(RootSessionFailure::WriteScratch.into());
                }
                WriteCommitEncoder::try_new(input.chunk().record_batch().clone(), write_totals)
                    .map(|encoder| InputEncoder::WriteCommit(Box::new(encoder)))
                    .map_err(ExecutionFailure::from_domain)
            }
            FrozenRootOutput::InternalFacts(InternalResultDomain::CowSelectionArrowV1) => {
                // COW has its own out-of-lock construction/step path.
                Err(RootSessionFailure::NoInputCursor.into())
            }
            FrozenRootOutput::ScalarValue(schema) => {
                // Cumulative cardinality is decided before any cursor: a
                // second row is refused before it can reach a segment.
                let rows = input.chunk().len();
                if rows > 1 || (rows == 1 && *scalar_rows != 0) {
                    return Err(RootSessionFailure::ScalarCardinality.into());
                }
                let encoder = if is_container(schema.field()) {
                    if rows == 0 {
                        NativeScalarContainerEncoder::validate_empty(input.chunk(), schema)
                            .map(|()| InputEncoder::ScalarEmpty)
                    } else {
                        NativeScalarContainerEncoder::try_encode(input.chunk(), schema, capacity)
                            .map(|encoder| InputEncoder::ScalarContainer(Box::new(encoder)))
                    }
                } else {
                    ScalarSchemaOwner::try_from_contract(Arc::clone(&self.spec().contract))
                        .and_then(|owner| {
                            if rows == 0 {
                                NativeScalarLeafEncoder::try_validate_empty(
                                    input.chunk(),
                                    owner,
                                    capacity,
                                )
                            } else {
                                NativeScalarLeafEncoder::try_begin(input.chunk(), owner, capacity)
                            }
                        })
                        .map(|encoder| InputEncoder::ScalarLeaf(Box::new(encoder)))
                }
                .map_err(ExecutionFailure::from_domain)?;
                if rows == 1 {
                    *scalar_rows = 1;
                }
                Ok(encoder)
            }
            FrozenRootOutput::ClientRows(_) => {
                let types: Arc<dyn novarocks_type_contract::ClientInputTypeBindings> =
                    input.chunk().chunk_schema_ref();
                if !client_types
                    .as_ref()
                    .is_some_and(|proof| proof.is_for(&self.spec().contract, &types))
                {
                    let shared = client_types
                        .as_ref()
                        .and_then(|proof| proof.rebind_shared(types.clone()));
                    *client_types = Some(match shared {
                        Some(proof) => proof,
                        None => novarocks_result_render::ValidatedClientInput::try_new(
                            novarocks_type_contract::ValidatedClientInputTypes::try_new(
                                Arc::clone(&self.spec().contract),
                                types,
                            )
                            .map_err(ExecutionFailure::from_domain)?,
                        )
                        .map_err(ExecutionFailure::from_domain)?,
                    });
                }
                let encoder = ClientRowEncoder::try_new_typed_root(
                    client_types.as_ref().unwrap().clone(),
                    input.chunk().record_batch().clone(),
                )
                .map_err(ExecutionFailure::from_domain)?;
                if !encoder
                    .scratch_capacity_bytes()
                    .checked_add(size_of::<ClientRowEncoder>())
                    .is_some_and(fits)
                {
                    return Err(RootSessionFailure::RendererScratch.into());
                }
                Ok(InputEncoder::Client(Box::new(encoder)))
            }
            // Session opening refuses an untyped scalar identity, and
            // CountOnly counts rows without a cursor.
            FrozenRootOutput::InternalFacts(InternalResultDomain::ScalarValueV1)
            | FrozenRootOutput::CountOnly => Err(RootSessionFailure::NoInputCursor.into()),
        }
    }
    /// Domain facts a normal sealed End must already hold. A write root
    /// without its SUMMARY fails instead of publishing a success End.
    fn sealed_domain_complete(&self, state: &ProducerState) -> ExecutionResult<()> {
        if self.spec().contract.kind()
            == RootOutputKind::InternalFacts(InternalResultDomain::PreparedWriteCommitV1)
        {
            state
                .write_totals
                .finish()
                .map_err(ExecutionFailure::from_domain)?;
        }
        Ok(())
    }
    fn scalar(&self) -> bool {
        matches!(
            self.spec().contract.output(),
            FrozenRootOutput::ScalarValue(_)
        )
    }
    /// Normal sealed End of a ScalarValueV1 root: exactly one record, the
    /// accepted value or the unique NoRows, published together with End.
    /// Failure and cancellation never reach here, so they publish neither.
    fn finish_scalar(&self, state: &mut ProducerState) -> ExecutionResult<RootProducerTurn> {
        let FrozenRootOutput::ScalarValue(schema) = self.spec().contract.output() else {
            unreachable!("scalar finish is reached only for a scalar root");
        };
        if state.builder.is_none() {
            match self.channel.try_segment() {
                Ok(Some(builder)) => {
                    state.builder = Some(builder);
                    state.used = 0;
                }
                Ok(None) => return Ok(RootProducerTurn::Blocked),
                Err(cause) => return Err(cause.into_execution()),
            }
        }
        if state.scalar_rows == 0 {
            if state.used != 0 {
                return Err(RootSessionFailure::ScalarBytesWithoutRow.into());
            }
            let output = state.builder.as_mut().unwrap().output_at(0);
            let written =
                ScalarLeafCursor::try_new(schema, BorrowedScalarLeaf::NoRows).and_then(|cursor| {
                    let turn = cursor.copy_range(0, output)?;
                    Ok((turn.complete, turn.emitted_bytes))
                });
            match written {
                Ok((true, bytes)) => state.used = bytes,
                Ok((false, _)) => return Err(RootSessionFailure::NoRowsIncomplete.into()),
                Err(cause) => return Err(ExecutionFailure::from_domain(cause)),
            }
        } else if state.used == 0 {
            return Err(RootSessionFailure::ScalarRowWithoutRecord.into());
        }
        if let Err(cause) = self.channel.request_finish() {
            return Err(cause.into_execution());
        }
        let builder = state.builder.take().unwrap();
        let bytes = std::mem::take(&mut state.used);
        if let Err(cause) = self.channel.publish_segment(builder, bytes, true) {
            return Err(cause.into_execution());
        }
        Ok(RootProducerTurn::Complete)
    }
}
impl RootResultSession for NativeRootResultSession {
    fn spec(&self) -> &RootResultWriteSpec {
        self.shared.channel.spec()
    }
    fn writable_observable(&self) -> Arc<Observable> {
        self.shared.authority.observable()
    }
    fn try_acquire_input(&self) -> Result<RootInputAdmission, TaskIoError> {
        {
            let state = self.shared.state();
            if state.failed.is_some() {
                return Err(io_error(RootSessionFailure::ProducerClosed));
            }
            if self.shared.channel.is_closed() || state.sealed || state.cleanup_started {
                return Err(io_error(RootSessionFailure::InputClosed));
            }
        }
        if self.shared.state().input.is_some() {
            return Ok(RootInputAdmission::Blocked);
        }
        let RootInputReservationAdmission::Granted(mut reservation) =
            self.shared.authority.try_reserve()?
        else {
            return Ok(RootInputAdmission::Blocked);
        };
        let mut credit = match self
            .shared
            .channel
            .try_reserve(reservation.required_bytes())
        {
            Ok(ResultWriteAdmission::Granted(credit)) => Some(credit),
            Ok(ResultWriteAdmission::Blocked) => {
                reservation.release_unfunded();
                return Ok(RootInputAdmission::Blocked);
            }
            Err(cause) => return Err(io_error(cause)),
        };
        match reservation.try_bind(&mut credit) {
            Ok(RootInputAdmission::Granted(permit)) => Ok(RootInputAdmission::Granted(permit)),
            Ok(RootInputAdmission::Blocked) => {
                reservation.retire_credit(&mut credit);
                Ok(RootInputAdmission::Blocked)
            }
            Err(cause) => {
                reservation.retire_credit(&mut credit);
                Err(cause)
            }
        }
    }

    fn submit_input(
        &self,
        submitted: &mut Option<novarocks_execution::runtime::task::io::RootResultInput>,
    ) -> Result<(), TaskIoError> {
        let original = submitted
            .as_ref()
            .expect("submission borrows one original input slot");
        if !self.shared.authority.owns_original(original.permit()) {
            return Err(io_error(RootSessionFailure::ForeignPermit));
        }
        {
            let state = self.shared.state();
            if let Some(cause) = self.shared.input_refusal(&state) {
                return Err(io_error(cause));
            }
        }
        // Bounded structural inspection only: no render/hydrate/row counting
        // in the driver. An oversized/unknown original cannot enter the queue.
        let cap = if self.shared.spec().contract.kind()
            == RootOutputKind::InternalFacts(InternalResultDomain::StatisticsArtifactV1)
        {
            novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES
        } else {
            NativeResultSupportGeometry::V1.root_original_input_backing_capacity_bytes as usize
        };
        if let Err(error) = borrowed_root_chunk_storage(
            original.chunk(),
            RootArrayStorageLimits {
                bytes: cap,
                nodes: 2 * RootProfileV1::SCHEMA_TYPE_NODES,
                depth: RootProfileV1::MAX_DEPTH,
            },
        ) {
            return Err(io_error(error));
        }
        let mut state = self.shared.state();
        if let Some(cause) = self.shared.input_refusal(&state) {
            drop(state);
            return Err(io_error(cause));
        }
        if let Err(error) = self.shared.start(&mut state) {
            drop(state);
            return Err(error);
        }
        // The one linear transfer is pure and occurs only after every
        // fallible pre-acceptance operation. A rejected/unwound call leaves
        // the original carrier in the caller's existing cleanup continuation.
        state.input = Some(Input::new(
            submitted.take().expect("accepted original input"),
        ));
        drop(state);
        self.shared.wake()
    }
    fn finish_input(&self) -> Result<(), TaskIoError> {
        let mut state = self.shared.state();
        if state.failed.is_some() {
            return Err(io_error(RootSessionFailure::ProducerClosed));
        }
        if self.shared.channel.is_closed() {
            return Err(io_error(RootSessionFailure::ProducerClosed));
        }
        self.shared.start(&mut state)?;
        state.sealed = true;
        #[cfg(test)]
        {
            let hook = self.shared.before_finish_wake.lock().unwrap().clone();
            if let Some(hook) = hook {
                hook();
            }
        }
        // Activate while the session lock still excludes a Complete turn.
        // A normal End must not retire this registration before its wake.
        let wake = self.shared.wake();
        drop(state);
        wake?;
        Ok(())
    }
    fn producer_state(&self) -> RootProducerState {
        let state = self.shared.state();
        if state.completed {
            return self.shared.channel.producer_state();
        }
        if let Some(failure) = &state.failed {
            return failure.producer_state();
        }
        if self.shared.channel.is_closed() {
            return RootProducerState::AwaitTerminalControl;
        }
        let published = self.shared.channel.producer_state();
        if state.sealed && published == RootProducerState::Accepting {
            RootProducerState::Finishing
        } else {
            published
        }
    }
    fn producer_exit_observable(&self) -> Arc<Observable> {
        self.exit_observable.clone()
    }
    fn poll_producer_exit(&self, waker: &std::task::Waker) -> RootWorkExitProgress {
        let pool = match self.exit.poll(RootExitOwner::Runtime, waker) {
            Ok(RootProducerExitStatus::Exited) => RootWorkExitProgress::Exited,
            Ok(RootProducerExitStatus::Pending) => RootWorkExitProgress::Pending,
            Ok(RootProducerExitStatus::Unresolved | RootProducerExitStatus::NoJobInstalled)
            | Err(_) => RootWorkExitProgress::Unresolved,
        };
        // Pool completion can precede an independent issuer notification tail.
        // Register both actual owners, including after the pool already exited.
        join_root_exit_progress(pool, self.shared.authority.poll_input_exit(waker))
    }
    fn abort(&self, reason: ResultAbort) {
        self.shared.request_abort(reason);
        let _ = self.shared.wake.request_cancel();
    }
}
impl RootProducerJob for NativeRootWork {
    fn turn(&self) -> RootProducerTurn {
        #[cfg(test)]
        {
            let hook = self.shared.before_pool_turn.lock().unwrap().clone();
            if let Some(hook) = hook {
                hook();
            }
        }
        if self.shared.cow() {
            let turn = match self.shared.advance_cow() {
                Ok(turn) => turn,
                Err(source) => {
                    self.shared.capture_failure(source);
                    RootProducerTurn::Complete
                }
            };
            {
                let mut state = self.shared.state();
                state.completed |=
                    turn_has_published_end(turn, self.shared.channel.snapshot().end_sequence);
            }
            if self
                .shared
                .retire_failure_source(FailureRecordKind::Primary)
                == RootCleanupProgress::Unresolved
                || self.shared.retire_completed_input() == RootCleanupProgress::Unresolved
            {
                return RootProducerTurn::Complete;
            }
            return turn;
        }
        let mut state = self.shared.state();
        state.completed |= self.shared.channel.snapshot().end_sequence.is_some();
        if state.completed {
            assert!(
                state.input.is_none() && state.builder.is_none(),
                "published End must have released its original input and builder"
            );
            return RootProducerTurn::Complete;
        }
        let turn = match self.shared.advance(&mut state) {
            Ok(turn) => turn,
            Err(source) => {
                // advance never changes failed and returns Complete without
                // constructing a source when that record already exists. This
                // pure move reserves the first record before abort can enter;
                // no projection, callback or opaque destructor runs here.
                state.failed = Some(RootFailureRecord::new(source));
                state.sealed = true;
                RootProducerTurn::Complete
            }
        };
        state.completed |=
            turn_has_published_end(turn, self.shared.channel.snapshot().end_sequence);
        drop(state);
        if self
            .shared
            .retire_failure_source(FailureRecordKind::Primary)
            == RootCleanupProgress::Unresolved
        {
            return RootProducerTurn::Complete;
        }
        if self.shared.retire_completed_input() == RootCleanupProgress::Unresolved {
            // No new Task cause or normal End: the exact owner is unresolved.
            return RootProducerTurn::Complete;
        }
        turn
    }
    fn cancel(&self) {
        self.shared
            .request_abort(ResultAbort::Cancelled(String::new()));
    }
    fn cleanup_step(
        &self,
        phase: u16,
        predecessors: RootCleanupPredecessors,
    ) -> RootCleanupProgress {
        self.shared.cleanup_step(phase, predecessors)
    }
}

impl RootSessionState {
    fn input_refusal(&self, state: &ProducerState) -> Option<ExecutionFailure> {
        if state.failed.is_some() {
            return Some(RootSessionFailure::ProducerClosed.into());
        }
        if state.sealed || state.cleanup_started || self.channel.is_closed() {
            return Some(RootSessionFailure::InputClosed.into());
        }
        state
            .input
            .is_some()
            .then(|| RootSessionFailure::InputNotAccepting.into())
    }
    fn request_abort(&self, reason: ResultAbort) {
        let mut state = self.state();
        state.completed |= self.channel.snapshot().end_sequence.is_some();
        if state.completed || state.cleanup_started {
            return;
        }
        if state.failed.is_none() {
            state.abort_is_rollback = matches!(
                reason,
                ResultAbort::PrepareRollback | ResultAbort::NeverStarted
            );
        }
        if state.failed.is_none() {
            state.failed = Some(RootFailureRecord::new(
                RootSessionFailure::ObservedStop.into(),
            ));
        }
        state.sealed = true;
    }
    fn exit_input_phase(&self, phase: InputExitPhase) -> RootCleanupProgress {
        let action = self
            .state()
            .input
            .as_mut()
            .and_then(|input| input.take_exit_action(phase));
        let Some(action) = action else {
            let state = self.state();
            return if state
                .input
                .as_ref()
                .is_none_or(|input| input.exits[phase.index()] == InputOwnerExit::Exited)
            {
                RootCleanupProgress::Exited
            } else {
                RootCleanupProgress::Unresolved
            };
        };
        match action.run() {
            InputExitOutcome::RetainedPermit(permit) => {
                let mut state = self.state();
                let input = state
                    .input
                    .as_mut()
                    .expect("original input remains during retirement");
                input._permit = Some(permit);
                input.exits[phase.index()] = InputOwnerExit::Unresolved;
                RootCleanupProgress::Unresolved
            }
            InputExitOutcome::Exited => {
                let removed = {
                    let mut state = self.state();
                    let input = state
                        .input
                        .as_mut()
                        .expect("input retained during retirement");
                    input.note_exit(phase);
                    if input.fully_exited() {
                        state.input.take()
                    } else {
                        None
                    }
                };
                if removed.is_some() {
                    {
                        let _removed = removed;
                    }
                    // The permit receipt precedes reopening this Native slot.
                    self.authority.notify_readiness();
                }
                RootCleanupProgress::Exited
            }
        }
    }
    fn retire_completed_input(&self) -> RootCleanupProgress {
        let mut progress = RootCleanupProgress::Exited;
        if self
            .state()
            .input
            .as_ref()
            .is_some_and(|input| input.retiring)
        {
            for phase in InputExitPhase::ALL {
                if self.exit_input_phase(phase) == RootCleanupProgress::Unresolved {
                    progress = RootCleanupProgress::Unresolved;
                }
            }
        }
        progress
    }
    fn cleanup_step(
        &self,
        phase: u16,
        predecessors: RootCleanupPredecessors,
    ) -> RootCleanupProgress {
        match phase {
            0 => {
                {
                    let mut state = self.state();
                    state.cleanup_started = true;
                    state.sealed = true;
                    state.completed |= self.channel.snapshot().end_sequence.is_some();
                }
                self.authority.close();
            }
            1 => {
                if !self.state().completed {
                    self.channel.close(RootRetentionClose::ParticipationAborted);
                    #[cfg(test)]
                    {
                        let observer = self.after_cleanup_close.lock().unwrap().take();
                        if let Some(observer) = observer {
                            // The actual channel close returned; this worker
                            // holds neither Root mutex, and the original CPU
                            // cleanup phase has not advanced to input disposal.
                            observer();
                        }
                    }
                }
            }
            2..=5 => return self.exit_input_phase(InputExitPhase::ALL[(phase - 2) as usize]),
            6 => {
                let owner = self.state().client_types.take();
                drop(owner);
            }
            7 => {
                let owner = {
                    let mut state = self.state();
                    state.used = 0;
                    state.builder.take()
                };
                drop(owner);
            }
            8 => {
                // Unknown data must retain its encoder responsibility. The
                // pool already owns the failed phase and its original work.
                let producer = {
                    let mut state = self.state();
                    if !encoder_cleanup_ready(
                        predecessors,
                        state.input.is_some(),
                        state.builder.is_some(),
                    ) {
                        return RootCleanupProgress::Unresolved;
                    }
                    state.producer.take()
                };
                if let Some(producer) = producer
                    && let Some(source) = producer.finish_encoder()
                {
                    self.capture_failure(source);
                    return self.retire_failure_source(FailureRecordKind::Primary);
                }
            }
            14 | 15 => {
                // These fixed source actions cannot use field emptiness or a
                // logical terminal as the missing data/capture exit proof.
                if !predecessors.all_exited() {
                    return RootCleanupProgress::Unresolved;
                }
                return self.retire_failure_source(if phase == 14 {
                    FailureRecordKind::Primary
                } else {
                    FailureRecordKind::Cleanup
                });
            }
            9 => {
                let owner = self.failure_reporter.lock().unwrap().take();
                drop(owner);
            }
            10 => {
                let terminal = self.state().take_terminal_observation();
                if let Some(terminal) = terminal {
                    self.observe_terminal(terminal);
                }
            }
            11 => self.authority.notify_readiness(),
            12 => {
                #[cfg(test)]
                {
                    let owner = self.before_finish_wake.lock().unwrap().take();
                    drop(owner);
                }
            }
            13 => {
                #[cfg(test)]
                {
                    let owner = self.before_pool_turn.lock().unwrap().take();
                    drop(owner);
                }
            }
            _ => unreachable!("closed Root cleanup plan"),
        }
        RootCleanupProgress::Exited
    }
}
fn join_root_exit_progress(
    pool: RootWorkExitProgress,
    issuer: RootWorkExitProgress,
) -> RootWorkExitProgress {
    match (pool, issuer) {
        (RootWorkExitProgress::Unresolved, _) | (_, RootWorkExitProgress::Unresolved) => {
            RootWorkExitProgress::Unresolved
        }
        (RootWorkExitProgress::Exited, RootWorkExitProgress::Exited) => {
            RootWorkExitProgress::Exited
        }
        _ => RootWorkExitProgress::Pending,
    }
}
fn io_error<E>(source: E) -> TaskIoError
where
    E: std::error::Error
        + novarocks_execution_contract::failure::FailureProjection
        + Send
        + Sync
        + 'static,
{
    TaskIoError::from_domain(
        TaskIoOperation::ResultWrite,
        TaskIoErrorKind::Unavailable,
        source,
    )
}

#[cfg(test)]
pub(crate) mod terminal_observation_tests {
    use super::*;
    use std::num::NonZeroUsize;
    use std::sync::Condvar;
    use std::time::{Duration, Instant};

    use arrow::array::Int64Array;
    use arrow::datatypes::DataType;
    use novarocks_execution::exec::chunk::{ChunkSchema, ChunkSlotSchema};
    use novarocks_execution_contract::TaskIdentity;
    use novarocks_result_contract::{RootOutputContract, RootProfileId};
    use novarocks_types::arrow_metadata_owner::{
        ArrowMetadataOwner, FieldMetadataOrigins, MetadataOwnerLimits,
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, SlotId, StageId, TaskId,
    };
    use novarocks_worker::WorkerResultRetainedLimits;
    use novarocks_worker::result_buffer::ResultRetainedBudget;

    const WAIT: Duration = Duration::from_secs(5);

    use crate::backend_task_execution::tests::node_fixture::{
        NativeNodeFixture, await_original, factory_from_host,
    };
    use novarocks_execution::runtime::task_control::TaskExecutionControl;
    use novarocks_execution_contract::{
        CreateTask, CreationContent, ExchangeTopology, FrozenBytes, OperationOutcome,
        ParticipationRef, TaskCreationInput, TaskDescriptor, TaskOperationId,
    };
    use novarocks_worker::{
        HostRejection, ParticipationHost, PreparedTaskInstallation, ReleasedParticipationEvidence,
        RunnableTask, SharedFactsRequest, TaskExecutionHost, TaskIngressRetirement,
        TaskPreparationContext, TaskStatusReporter, WorkerCreateReply,
        WorkerParticipationReplyBody,
    };

    struct RootParticipationHost;
    impl ParticipationHost for RootParticipationHost {
        fn materialize(&self, _: SharedFactsRequest<'_>) -> Result<(), HostRejection> {
            Ok(())
        }
        fn release(&self, _: ParticipationRef) -> ReleasedParticipationEvidence {
            ReleasedParticipationEvidence::none()
        }
        fn advance_shared_domain(
            &self,
            _: ParticipationRef,
            _: &novarocks_execution_contract::ParticipationDomainUpdate,
        ) -> Result<(), HostRejection> {
            unreachable!("Root fixture has no shared-domain updates")
        }
    }
    #[derive(Debug)]
    struct RootPreparationReturned;
    impl std::fmt::Display for RootPreparationReturned {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("the original Root fixture preparation returned")
        }
    }
    impl std::error::Error for RootPreparationReturned {}
    impl novarocks_execution_contract::failure::FailureProjection for RootPreparationReturned {
        fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
            use novarocks_execution_contract::failure::*;
            FailureFact::new(
                FailureClass::Control,
                FailureReason::new(domain::NATIVE, std::num::NonZeroU16::new(60046).unwrap()),
            )
        }
    }
    #[derive(Debug)]
    struct RootAssignment;
    impl CreationContent for RootAssignment {
        fn retained_bytes(&self) -> usize {
            std::mem::size_of::<Self>()
        }
        fn encoded_len(&self) -> usize {
            0
        }
        fn into_stored(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
            self
        }
    }
    type RootPreparationCallback = Box<dyn for<'a> FnOnce(TaskPreparationContext<'a>) + Send>;
    struct RootCallbackHost(Mutex<Option<RootPreparationCallback>>);
    impl TaskExecutionHost for RootCallbackHost {
        fn close_participation_admission(&self, _: ParticipationRef) {}

        fn forget_participation_admission(&self, _: ParticipationRef) {}
        fn install_receiver(
            &self,
            _: &TaskDescriptor,
            _: TaskCreationInput,
            preparation: TaskPreparationContext<'_>,
        ) -> Result<PreparedTaskInstallation, HostRejection> {
            let callback = self
                .0
                .lock()
                .unwrap()
                .take()
                .expect("one original preparation callback");
            callback(preparation);
            Err(HostRejection::from_domain(
                novarocks_execution_contract::TaskFailureCategory::Execution,
                RootPreparationReturned,
            ))
        }
        fn remove_receiver(&self, _: &TaskDescriptor) {}
        fn retire_receiver_checked(
            &self,
            _: &TaskDescriptor,
            _: TaskIngressRetirement,
        ) -> Result<(), HostRejection> {
            Ok(())
        }
        fn install_inbound_capability(&self, _: &TaskDescriptor) -> Result<(), HostRejection> {
            unreachable!("the Root fixture pauses before installation")
        }
        fn remove_inbound_capability(&self, _: &TaskDescriptor) {}
        fn submit_runnable(
            &self,
            _: &TaskDescriptor,
            _: TaskStatusReporter,
        ) -> Result<Arc<dyn RunnableTask>, HostRejection> {
            unreachable!("the Root fixture returns its explicit preparation refusal")
        }
        fn apply_task_domain(
            &self,
            _: &TaskDescriptor,
            _: &novarocks_execution_contract::TaskDomainUpdate,
            _: TaskOperationId,
        ) -> Result<Option<u64>, HostRejection> {
            unreachable!("no task domains")
        }
    }
    pub(crate) struct OriginalRootNode {
        node: Mutex<Option<NativeNodeFixture>>,
        participation: ParticipationRef,
        task: TaskIdentity,
        release: Arc<(Mutex<bool>, Condvar)>,
        // The test host owns its actual born outputs until the same close
        // obligation as a production preparation refusal has run. This is
        // custody of existing channels, never another output or Task role.
        original_roots: Mutex<Vec<Arc<RootResultChannel>>>,
    }
    impl OriginalRootNode {
        pub(crate) fn retain_root_for_teardown(&self, original: Arc<RootResultChannel>) {
            assert_eq!(
                original.spec().task,
                self.task,
                "retain only this original Task's Root"
            );
            let mut roots = self.original_roots.lock().unwrap();
            if !roots.iter().any(|owned| Arc::ptr_eq(owned, &original)) {
                roots.push(original);
            }
        }
        pub(crate) fn release_body(&self) {
            *self.release.0.lock().unwrap() = true;
            self.release.1.notify_all();
        }
    }
    impl Drop for OriginalRootNode {
        fn drop(&mut self) {
            let original_roots = std::mem::take(
                self.original_roots
                    .get_mut()
                    .unwrap_or_else(|poison| poison.into_inner()),
            );
            // A successful producer End intentionally preserves the channel
            // for reads. This fixture returns a preparation refusal instead
            // of publishing Installed, so its original host must close that
            // retained output before waiting for the real preparation exit.
            for original in original_roots {
                original.close(RootRetentionClose::ParticipationAborted);
                drop(original);
            }
            self.release_body();
            drop(
                self.node
                    .get_mut()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .take(),
            );
        }
    }
    // Only the actual Worker preparation callback can supply this loan. The
    // returned value observes its original work; it cannot mint another role.
    pub(crate) fn in_original_preparation<R: Send + 'static>(
        task: TaskIdentity,
        health: Arc<novarocks_execution::runtime::execution_health::ExecutionHealth>,
        callback: impl for<'a, 'b> FnOnce(&'b TaskPreparationContext<'a>) -> R + Send + 'static,
    ) -> (R, Arc<OriginalRootNode>) {
        let result = Arc::new((Mutex::new(None), Condvar::new()));
        let output = result.clone();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let body_release = release.clone();
        let execution = Arc::new(RootCallbackHost(Mutex::new(Some(Box::new(
            move |preparation| {
                let value = callback(&preparation);
                *output.0.lock().unwrap() = Some(value);
                output.1.notify_all();
                let guard = body_release.0.lock().unwrap();
                let (guard, _) = body_release
                    .1
                    .wait_timeout_while(guard, Duration::from_secs(10), |released| !*released)
                    .unwrap();
                assert!(*guard, "the original Root preparation must be released");
            },
        )))));
        let participation = ParticipationRef::new(
            task.query_execution_id(),
            novarocks_types::FrontendProcessId::new_v7(),
            task.backend_process_id(),
        );
        let node = NativeNodeFixture::start(
            task.backend_process_id(),
            factory_from_host(Arc::new(RootParticipationHost)),
            execution,
            health,
            1,
            1,
        );
        node.establish(participation);
        let descriptor = TaskDescriptor::try_new(
            task,
            novarocks_types::UniqueId::new(907, 908),
            NonZeroUsize::new(1).unwrap(),
            vec![],
            ExchangeTopology::default(),
        )
        .unwrap();
        let created = node.create(
            CreateTask::try_new(TaskOperationId::new_v7(), participation, descriptor, vec![])
                .unwrap(),
            TaskCreationInput::new(
                FrozenBytes::freeze(bytes::Bytes::from_static(&[1])),
                Box::new(RootAssignment),
            ),
        );
        assert!(matches!(
            created.reply(),
            WorkerCreateReply::Participation(WorkerParticipationReplyBody::Create {
                outcome: OperationOutcome::Accepted,
                ..
            })
        ));
        drop(created);
        let value = result.0.lock().unwrap();
        let (mut value, _) = result
            .1
            .wait_timeout_while(value, WAIT, |value| value.is_none())
            .unwrap();
        let value = value
            .take()
            .expect("the actual Root preparation produced its observation");
        (
            value,
            Arc::new(OriginalRootNode {
                node: Mutex::new(Some(node)),
                participation,
                task,
                release,
                original_roots: Mutex::new(Vec::new()),
            }),
        )
    }
    pub(crate) fn open_root_in_preparation(
        preparation: &TaskPreparationContext<'_>,
        pool: &Arc<RootProducerPool>,
        spec: RootResultWriteSpec,
        budget: Arc<ResultRetainedBudget>,
        limits: WorkerResultRetainedLimits,
        health: Arc<novarocks_execution::runtime::execution_health::ExecutionHealth>,
    ) -> Result<Arc<NativeRootResultSession>, TaskIoError> {
        open_root_in_preparation_with_layout(preparation, pool, spec, budget, limits, health, None)
    }
    pub(crate) fn open_root_in_preparation_with_layout(
        preparation: &TaskPreparationContext<'_>,
        pool: &Arc<RootProducerPool>,
        spec: RootResultWriteSpec,
        budget: Arc<ResultRetainedBudget>,
        limits: WorkerResultRetainedLimits,
        health: Arc<novarocks_execution::runtime::execution_health::ExecutionHealth>,
        root_layout: Option<&novarocks_local_program::StaticLayout>,
    ) -> Result<Arc<NativeRootResultSession>, TaskIoError> {
        let child =
            crate::backend_task_execution::prepared_owner::NativePreparationChild::register(
                preparation,
                health.clone(),
            )
            .unwrap();
        child.begin_root_birth().unwrap();
        let channel = match RootResultChannel::try_open_with_owner_metadata(
            spec, budget, limits, health,
            crate::backend_task_execution::prepared_owner::NativePreparationChild::root_birth_metadata_bytes(),
        ) {
            Ok(channel) => channel,
            Err(error) => { child.root_birth_refused(); return Err(io_error(error)); }
        };
        child.record_root_birth(&channel);
        channel
            .bind_preparation_exit_waker(preparation.readiness_waker())
            .unwrap_or_else(|_| panic!("one original Root exit waker"));
        let recipe = if channel.spec().contract.kind()
            == RootOutputKind::InternalFacts(InternalResultDomain::CowSelectionArrowV1)
        {
            let prepared = root_layout
                .ok_or_else(|| {
                    io_error(
                        crate::root_cow_selection_codec::CowSelectionCodecError::RecipeMismatch,
                    )
                })
                .and_then(|layout| {
                    crate::root_cow_selection_codec::prepare_recipe(
                        &channel,
                        layout,
                        preparation.runtime_control().as_ref(),
                    )
                    .map_err(io_error)
                });
            match prepared {
                Ok(recipe) => Some(recipe),
                Err(cause) => {
                    channel.close(RootRetentionClose::ParticipationAborted);
                    return Err(cause);
                }
            }
        } else {
            None
        };
        let result = NativeRootResultSession::try_open_prepared_with_cow_recipe(
            channel.clone(),
            pool,
            preparation,
            &child,
            recipe,
        );
        if result.is_err() {
            channel.close(RootRetentionClose::ParticipationAborted);
        }
        result
    }
    struct OriginalRootTask {
        probe: Arc<TaskExecutionControl>,
        node: Arc<OriginalRootNode>,
    }
    impl OriginalRootTask {
        fn status(&self) -> &Arc<TaskExecutionControl> {
            &self.probe
        }
        fn abort(&self, cause: novarocks_execution_contract::AbortCause) {
            let (control, deadline) = {
                let held = self.node.node.lock().unwrap();
                let node = held.as_ref().unwrap();
                (node.control.clone(), node.deadline())
            };
            drop(
                await_original(control.abort(
                    novarocks_execution_contract::AbortParticipation::new(
                        TaskOperationId::new_v7(),
                        self.node.participation,
                        cause,
                    ),
                    deadline,
                ))
                .unwrap(),
            );
            let until = Instant::now() + WAIT;
            while !self.probe.is_stopped() {
                assert!(
                    Instant::now() < until,
                    "the original Node must publish its accepted abort"
                );
                std::thread::yield_now();
            }
        }
        fn cancel(&self, reason: novarocks_execution_contract::CancelReason) {
            let (control, deadline) = {
                let held = self.node.node.lock().unwrap();
                let node = held.as_ref().unwrap();
                (node.control.clone(), node.deadline())
            };
            let receipt = await_original(control.cancel(
                novarocks_execution_contract::CancelTask::new(
                    TaskOperationId::new_v7(),
                    self.node.task,
                    reason,
                ),
                deadline,
            ))
            .unwrap();
            drop(receipt);
            let until = Instant::now() + WAIT;
            while !self.probe.is_stopped() {
                assert!(
                    Instant::now() < until,
                    "the original Node must publish its accepted stop"
                );
                std::thread::yield_now();
            }
        }
    }

    // Occupies the one real CPU worker. The native session still owns its
    // admitted input and producer guard; its turn cannot exit until release.
    struct GatedTurn {
        state: Mutex<(bool, bool)>,
        changed: Condvar,
    }
    impl GatedTurn {
        fn wait_entered(&self) {
            let state = self.state.lock().unwrap();
            let (state, _) = self
                .changed
                .wait_timeout_while(state, WAIT, |state| !state.0)
                .unwrap();
            assert!(
                state.0,
                "the real pool worker must enter the controlled turn"
            );
        }
        fn release(&self) {
            self.state.lock().unwrap().1 = true;
            self.changed.notify_all();
        }
    }
    impl RootProducerJob for GatedTurn {
        fn turn(&self) -> RootProducerTurn {
            let mut state = self.state.lock().unwrap();
            state.0 = true;
            self.changed.notify_all();
            let (state, _) = self
                .changed
                .wait_timeout_while(state, WAIT, |state| !state.1)
                .unwrap();
            assert!(
                state.1,
                "controlled pool turn must be released within its budget"
            );
            RootProducerTurn::Complete
        }
        fn cancel(&self) {
            self.release();
        }
        fn cleanup_step(
            &self,
            _phase: u16,
            _predecessors: RootCleanupPredecessors,
        ) -> RootCleanupProgress {
            RootCleanupProgress::Exited
        }
    }

    struct Fixture {
        session: Arc<NativeRootResultSession>,
        pool: Arc<RootProducerPool>,
        blocker: Option<(Arc<GatedTurn>, RootProducerRegistration)>,
        health: Arc<novarocks_execution::runtime::execution_health::ExecutionHealth>,
        budget: Arc<ResultRetainedBudget>,
        original_nodes: Mutex<Vec<Arc<OriginalRootNode>>>,
    }
    impl Fixture {
        fn new(gated: bool) -> Self {
            let (fixture, task, _) = actual_task_fixture_with_gate(gated);
            drop(task);
            fixture
        }
        fn with_task_control(
            gated: bool,
            control: &TaskPreparationContext<'_>,
            health: Arc<novarocks_execution::runtime::execution_health::ExecutionHealth>,
        ) -> Self {
            let limits =
                WorkerResultRetainedLimits::try_new(256 * 1024 * 1024, 1024 * 1024 * 1024).unwrap();
            let budget = ResultRetainedBudget::new(limits.per_process());
            let pool = RootProducerPool::try_new(
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(2).unwrap(),
                1024 * 1024,
                Arc::clone(&budget),
                Arc::clone(&health),
            )
            .unwrap();
            let blocker = gated.then(|| {
                let job = Arc::new(GatedTurn {
                    state: Mutex::new((false, false)),
                    changed: Condvar::new(),
                });
                let erased = Arc::clone(&job) as Arc<dyn RootProducerJob>;
                let registration = pool
                    .reserve_for_test(RootCleanupPlan::new(&[0]))
                    .unwrap()
                    .commit(erased)
                    .unwrap_or_else(|_| panic!("the real blocker reservation must commit"));
                registration.wake().unwrap();
                job.wait_entered();
                (job, registration)
            });
            let session = open_root_in_preparation(
                control,
                &pool,
                RootResultWriteSpec {
                    task: control.runtime_control().identity(),
                    contract: Arc::new(RootOutputContract::new(
                        RootProfileId::V1,
                        FrozenRootOutput::CountOnly,
                    )),
                },
                budget.clone(),
                limits,
                health.clone(),
            )
            .unwrap();
            Self {
                session,
                pool,
                blocker,
                health,
                budget,
                original_nodes: Mutex::new(Vec::new()),
            }
        }
        fn observations(&self) -> Vec<&'static str> {
            self.session
                .shared
                .terminal_observations
                .lock()
                .unwrap()
                .clone()
        }
        fn wait_terminal(&self, expected: &'static str) {
            let deadline = Instant::now() + WAIT;
            loop {
                let observed = self.observations();
                if observed == [expected] {
                    return;
                }
                assert!(
                    Instant::now() < deadline,
                    "native Root did not reach its real exit; observed={observed:?}"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        fn submit(&self) {
            let owner = ArrowMetadataOwner::try_new(
                Vec::new(),
                MetadataOwnerLimits {
                    entries: 0,
                    construction_bytes: 0,
                },
            )
            .unwrap()
            .into_field("value".into(), DataType::Int64, false);
            let slot = ChunkSlotSchema::try_new_with_metadata_origins(
                SlotId::new(1),
                Arc::clone(owner.field()),
                FieldMetadataOrigins::try_new(vec![owner], 1).unwrap(),
                None,
                None,
            )
            .unwrap();
            let schema = Arc::new(ChunkSchema::try_new(vec![slot]).unwrap());
            assert!(schema.field_metadata_origins().is_some());
            assert!(schema.schema_metadata_origin().is_some());
            let chunk =
                Chunk::try_new_with_columns(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))])
                    .unwrap();
            let RootInputAdmission::Granted(permit) = self.session.try_acquire_input().unwrap()
            else {
                panic!("the actual native Root must admit its original input");
            };
            {
                let mut submitted = Some(
                    novarocks_execution::runtime::task::io::RootResultInput::new(chunk, permit),
                );
                self.session.submit_input(&mut submitted).unwrap();
                assert!(
                    submitted.is_none(),
                    "accepted input transfers its exact caller slot"
                );
            };
        }
        fn release(&self) {
            if let Some((job, _registration)) = &self.blocker {
                job.release();
            }
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            eprintln!(
                "original Root fixture before teardown: ownership={:?} producer={:?} producer_exit={:?}",
                self.session.channel().try_ownership_snapshot(),
                self.session.producer_state(),
                self.session.producer_exit_progress(),
            );
            self.release();
            let _ = self.pool.shutdown();
            for node in self
                .original_nodes
                .get_mut()
                .unwrap_or_else(|poison| poison.into_inner())
                .iter()
            {
                node.release_body();
            }
        }
    }

    fn root_task() -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(907, 908), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        )
    }
    fn root_control_lifetime(
        session: &NativeRootResultSession,
    ) -> Weak<novarocks_execution::runtime::task_control::TaskExecutionControl> {
        Arc::downgrade(
            session
                .shared
                .failure_reporter
                .lock()
                .unwrap()
                .as_ref()
                .unwrap(),
        )
    }
    fn actual_task_fixture_with_gate(
        gated: bool,
    ) -> (Fixture, OriginalRootTask, Weak<TaskExecutionControl>) {
        let health =
            Arc::new(novarocks_execution::runtime::execution_health::ExecutionHealth::new());
        let body_health = health.clone();
        let ((mut fixture, probe), node) =
            in_original_preparation(root_task(), health, move |preparation| {
                let probe = preparation.runtime_control();
                let fixture = Fixture::with_task_control(gated, preparation, body_health);
                (fixture, probe)
            });
        let weak_control = root_control_lifetime(&fixture.session);
        node.retain_root_for_teardown(fixture.session.channel().clone());
        fixture.original_nodes.get_mut().unwrap().push(node.clone());
        (fixture, OriginalRootTask { probe, node }, weak_control)
    }
    fn actual_task_fixture() -> (Fixture, OriginalRootTask, Weak<TaskExecutionControl>) {
        actual_task_fixture_with_gate(false)
    }

    #[test]
    fn cold_root_cancellation_and_rollback_do_not_claim_exit_while_real_cleanup_is_queued() {
        use novarocks_execution_contract::{CancelReason, TaskFailurePhase};
        let mut observed = Vec::new();
        for reason in [
            ResultAbort::PrepareRollback,
            ResultAbort::Cancelled("cold root deadline".into()),
        ] {
            let (fixture, task, _) = actual_task_fixture_with_gate(true);
            let controller_lifetime = root_control_lifetime(&fixture.session);
            let rollback = matches!(&reason, ResultAbort::PrepareRollback);
            if rollback {
                task.status()
                    .in_phase(TaskFailurePhase::Preparation)
                    .report_failure(novarocks_execution::runtime::task::ExecutionFailure::from(
                        novarocks_functions::KernelFailure::ResourceExhausted,
                    ));
            } else {
                task.cancel(CancelReason::UpstreamNoLongerNeeded);
            }
            assert!(fixture.session.state().producer.is_none());
            assert_eq!(
                fixture.session.producer_exit_progress(),
                RootWorkExitProgress::Pending,
                "open committed real cold work before activation"
            );
            fixture.session.abort(reason);
            // The one real CPU worker is inside the fixture's existing gate.
            // The cold cleanup owns a real queued job and its Task controller,
            // but has had no opportunity to execute or release either.
            observed.push((
                rollback,
                fixture.session.producer_exit_progress(),
                controller_lifetime.upgrade().is_some(),
            ));
            fixture.release();
            fixture.pool.shutdown().unwrap();
            assert!(fixture.session.producer_exit_progress() == RootWorkExitProgress::Exited);
            assert!(controller_lifetime.upgrade().is_none());
        }
        for (rollback, exited, controller_pinned) in observed {
            assert!(
                controller_pinned,
                "the queued real Root work still owns its reporting scope"
            );
            assert!(
                exited == RootWorkExitProgress::Pending,
                "a stop request is not actual cold Root cleanup exit (rollback={rollback})"
            );
        }
    }

    // Runtime-origin reentry is covered by the actual adopted Native scalar
    // codec case in execution_host::tests. A paused preparation has not yet
    // produced the real runtime/finisher cut that adjudicates that origin.

    #[test]
    fn root_success_releases_actual_task_authority_before_independent_metadata_late_drop() {
        let (fixture, task, control) = actual_task_fixture();
        let weak_task = Arc::downgrade(task.status());
        fixture.submit();
        fixture.session.finish_input().unwrap();
        fixture.wait_terminal("finished");
        fixture.pool.shutdown().unwrap();
        assert!(fixture.session.producer_exit_progress() == RootWorkExitProgress::Exited);
        assert!(control.upgrade().is_none());
        assert!(
            task.status().accepted().is_none(),
            "producer exit alone cannot conclude a whole Task"
        );
        drop(task);
        assert!(
            weak_task.upgrade().is_none(),
            "live independent session/metadata has no Task capability"
        );
    }

    #[test]
    fn root_preparation_rollback_releases_work_authority_without_inventing_a_task_failure() {
        for reason in [ResultAbort::PrepareRollback, ResultAbort::NeverStarted] {
            let (fixture, task, control) = actual_task_fixture();
            fixture.session.abort(reason);
            fixture.pool.shutdown().unwrap();
            assert!(fixture.session.producer_exit_progress() == RootWorkExitProgress::Exited);
            assert!(control.upgrade().is_none());
            assert!(
                task.status().accepted().is_none(),
                "rollback stand-down is not an originating runtime failure"
            );
            assert!(fixture.observations().is_empty());
        }
    }

    #[test]
    fn root_refuses_foreign_actual_task_control_before_creating_a_session() {
        use novarocks_execution_contract::failure::FailureProjection;
        let fixture = Fixture::new(false);
        let channel = fixture.session.channel().clone();
        let health = fixture.health.clone();
        let pool = fixture.pool.clone();
        let (fact, original) = in_original_preparation(root_task(), health, move |preparation| {
            assert_ne!(
                preparation.runtime_control().identity(),
                channel.spec().task
            );
            let child =
                crate::backend_task_execution::prepared_owner::NativePreparationChild::register(
                    preparation,
                    pool.execution_health(),
                )
                .unwrap();
            let error =
                NativeRootResultSession::try_open_prepared(channel, &pool, preparation, &child)
                    .err()
                    .expect("foreign original Task control must be refused");
            assert!(preparation.runtime_control().accepted().is_none());
            error.failure_fact()
        });
        assert_eq!(
            fact,
            novarocks_execution::runtime::task_control::TaskControlIdentityMismatch.failure_fact()
        );
        drop(original);
    }

    #[test]
    fn root_closed_sources_are_observations_while_real_producer_faults_originate() {
        use novarocks_execution_contract::failure::{FailureEvidence, FailureProjection};
        for (source, reason) in [
            (RootSessionFailure::InputClosed, 512),
            (RootSessionFailure::ProducerClosed, 515),
            (RootSessionFailure::ObservedStop, 516),
        ] {
            let fact = source.failure_fact();
            assert_eq!(fact.evidence(), FailureEvidence::StopObservation);
            assert_eq!(fact.reason().code(), reason);
        }
        assert_eq!(
            RootSessionFailure::StatePoisoned.failure_fact().evidence(),
            FailureEvidence::Originating
        );
    }

    #[test]
    fn root_refusal_observes_stop_until_original_failure_reaches_task_owner() {
        use novarocks_execution_contract::failure::{FailureEvidence, FailureProjection};
        let (fixture, task, _) = actual_task_fixture_with_gate(true);
        fixture.submit();
        fixture.session.shared.channel.note_rows(u64::MAX).unwrap();
        let original = fixture
            .session
            .shared
            .channel
            .note_rows(1)
            .unwrap_err()
            .into_execution();
        let expected = original.failure_fact();
        assert_eq!(expected.evidence(), FailureEvidence::Originating);
        assert_eq!(expected.reason().code(), 10);
        fixture.session.fail(original.clone());
        let admission = fixture
            .session
            .try_acquire_input()
            .err()
            .expect("original refusal before new input");
        let finish = fixture
            .session
            .finish_input()
            .expect_err("original refusal at finishing");
        for error in [admission, finish] {
            assert_eq!(
                error.failure_fact().evidence(),
                FailureEvidence::StopObservation
            );
            assert!(matches!(
                crate::backend_test_support::find_source::<RootSessionFailure>(&error),
                Some(RootSessionFailure::ProducerClosed)
            ));
        }
        assert!(task.status().accepted().is_none());
        assert_eq!(
            fixture.session.producer_state(),
            RootProducerState::AwaitTerminalControl
        );
        {
            let state = fixture.session.state();
            let failure = state.failed.as_ref().expect("same original failure record");
            assert_eq!(failure.phase, FailureSourcePhase::Pending);
            assert!(failure.observation.is_none());
            let retained = failure
                .live_source
                .as_deref()
                .expect("original source awaits its producer");
            assert!(std::ptr::eq(retained.cause(), original.cause()));
        }
        // Release the original producer; its physical action transfers E to
        // the same Task owner. Admission never manufactures that handoff.
        fixture.release();
        let until = Instant::now() + WAIT;
        let accepted = loop {
            if let Some(accepted) = task.status().accepted() {
                break accepted;
            }
            assert!(
                Instant::now() < until,
                "original Root failure was not accepted by its Task"
            );
            std::thread::yield_now();
        };
        let retained = accepted
            .originating_failure()
            .expect("same original accepted source");
        assert_eq!(retained.failure_fact(), expected);
        assert!(std::ptr::eq(retained.cause(), original.cause()));
        fixture.wait_terminal("aborted");
        assert!(matches!(
            fixture.session.producer_state(),
            RootProducerState::Failed(observation) if observation.fact() == expected
        ));
        let state = fixture.session.state();
        let failure = state.failed.as_ref().expect("same neutral failure record");
        assert_eq!(failure.phase, FailureSourcePhase::Transferred);
        assert!(failure.live_source.is_none());
        assert_eq!(failure.observation.as_ref().unwrap().fact(), expected);
    }

    #[test]
    fn native_root_finish_wakes_before_a_queued_worker_can_retire_registration() {
        let fixture = Fixture::new(true);
        fixture.submit();
        let entry = Arc::new(GatedTurn {
            state: Mutex::new((false, false)),
            changed: Condvar::new(),
        });
        let worker_entry = Arc::clone(&entry);
        *fixture.session.shared.before_pool_turn.lock().unwrap() = Some(Arc::new(move || {
            // This is the real CPU worker, before it acquires the session lock.
            RootProducerJob::turn(worker_entry.as_ref());
        }));
        let worker_entry = Arc::clone(&entry);
        let blocker = Arc::clone(&fixture.blocker.as_ref().unwrap().0);
        let session = Arc::downgrade(&fixture.session);
        *fixture.session.shared.before_finish_wake.lock().unwrap() = Some(Arc::new(move || {
            blocker.release();
            struct ReleaseGate(Arc<GatedTurn>);
            impl Drop for ReleaseGate {
                fn drop(&mut self) {
                    self.0.release();
                }
            }
            let release_gate = ReleaseGate(Arc::clone(&worker_entry));
            worker_entry.wait_entered();
            let session = session.upgrade().unwrap();
            let holds_state = matches!(
                session.shared.state.try_lock(),
                Err(std::sync::TryLockError::WouldBlock)
            );
            let progress = session.producer_exit_progress();
            let no_end = session.channel().snapshot().end_sequence.is_none();
            // Release before asserting so a failing old-source control still
            // lets its real worker exit through the fixture's existing owner.
            drop(release_gate);
            assert!(
                holds_state,
                "finish must activate before releasing the session state"
            );
            assert_eq!(
                progress,
                RootWorkExitProgress::Pending,
                "the queued producer must not claim actual exit"
            );
            assert!(no_end, "the queued producer must not publish End yet");
        }));
        fixture.session.finish_input().unwrap();
        fixture.wait_terminal("finished");
        fixture.pool.shutdown().unwrap();
        let snapshot = fixture
            .session
            .channel()
            .try_ownership_snapshot()
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.ends_published, 1);
        assert_eq!(snapshot.producers_running, 0);
        assert_eq!(snapshot.producers_exited, 1);
        assert!(fixture.session.producer_exit_progress() == RootWorkExitProgress::Exited);
        let state = fixture.session.state();
        assert!(state.input.is_none());
        assert!(state.builder.is_none());
        assert!(state.producer.is_none());
        drop(state);
        assert_eq!(fixture.observations(), ["finished"]);
    }

    #[test]
    fn native_root_finish_preserves_a_real_pool_shutdown_error() {
        let fixture = Fixture::new(true);
        fixture.submit();
        let pool = Arc::downgrade(&fixture.pool);
        let shutdown = Arc::new(Mutex::new(None));
        let shutdown_owner = Arc::clone(&shutdown);
        *fixture.session.shared.before_finish_wake.lock().unwrap() = Some(Arc::new(move || {
            let pool = pool.upgrade().unwrap();
            let stopping_pool = Arc::clone(&pool);
            *shutdown_owner.lock().unwrap() =
                Some(std::thread::spawn(move || stopping_pool.shutdown()));
            let deadline = Instant::now() + WAIT;
            loop {
                match pool.reserve_for_test(RootCleanupPlan::new(&[0])) {
                    Err(RootProducerReserveFailure::Control(
                        crate::root_producer_pool::RootProducerControlFailure::Stopping,
                    )) => break,
                    Ok(reservation) => drop(reservation),
                    Err(error) => assert!(matches!(
                        error,
                        RootProducerReserveFailure::Control(
                            crate::root_producer_pool::RootProducerControlFailure::Positions { .. }
                        )
                    )),
                }
                assert!(Instant::now() < deadline, "the pool must begin shutdown");
                std::thread::yield_now();
            }
        }));
        let error = fixture.session.finish_input().unwrap_err();
        use novarocks_execution_contract::failure::{FailureClass, FailureProjection};
        assert_eq!(error.failure_fact().class(), FailureClass::Control);
        assert_eq!(
            error.failure_fact().reason().domain(),
            novarocks_execution_contract::failure::domain::NATIVE.get()
        );
        assert_eq!(error.failure_fact().reason().code(), 601);
        let mut source = std::error::Error::source(&error);
        let mut actual_pool = None;
        while let Some(cause) = source {
            if let Some(cause) =
                cause.downcast_ref::<crate::root_producer_pool::RootProducerControlFailure>()
            {
                actual_pool = Some(*cause);
                break;
            }
            source = cause.source();
        }
        assert_eq!(
            actual_pool,
            Some(crate::root_producer_pool::RootProducerControlFailure::Stopping)
        );
        assert_eq!(
            fixture.session.producer_exit_progress(),
            RootWorkExitProgress::Pending,
            "the admitted native work cannot exit behind the blocked real worker"
        );
        // Shutdown only requests cancellation. The one worker is still in
        // this fixture's turn and cannot execute that request until released.
        // Keep every original error assertion before releasing its gate.
        fixture.release();
        shutdown
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .join()
            .unwrap()
            .unwrap();
        assert!(fixture.session.producer_exit_progress() == RootWorkExitProgress::Exited);
    }

    #[test]
    fn native_root_normal_end_observes_once_after_exit_and_late_abort_cannot_replace_it() {
        let fixture = Fixture::new(false);
        fixture.submit();
        fixture.session.finish_input().unwrap();
        fixture.wait_terminal("finished");
        fixture.pool.shutdown().unwrap();
        let snapshot = fixture
            .session
            .channel()
            .try_ownership_snapshot()
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.ends_published, 1);
        assert_eq!(snapshot.producers_running, 0);
        assert_eq!(snapshot.producers_exited, 1);
        assert!(fixture.session.producer_exit_progress() == RootWorkExitProgress::Exited);
        fixture
            .session
            .abort(ResultAbort::Cancelled("late cancellation".into()));
        fixture
            .session
            .abort(ResultAbort::Failed("late failure".into()));
        assert!(
            fixture
                .session
                .state()
                .take_terminal_observation()
                .is_none()
        );
        assert!(
            fixture
                .session
                .state()
                .take_terminal_observation()
                .is_none()
        );
        assert_eq!(fixture.observations(), ["finished"]);
    }

    #[test]
    fn native_root_published_end_wakeup_panic_keeps_normal_end_and_channel_metadata_custody() {
        if !crate::root_unknown_test::enter() {
            return;
        }

        let fixture = Fixture::new(false);
        // This local encoding fixture explicitly supplies the Participation owner
        // required for the same successful handoff as production installation.
        fixture
            .session
            .channel()
            .mark_participation_owned()
            .unwrap();
        let metadata_before_fault = fixture
            .session
            .channel()
            .try_ownership_snapshot()
            .unwrap()
            .unwrap()
            .metadata_holders;
        let panicked = Arc::new(AtomicBool::new(false));
        let notified = Arc::clone(&panicked);
        let channel = Arc::downgrade(fixture.session.channel());
        let subscription = fixture
            .session
            .channel()
            .writable_observable()
            .try_subscribe(Arc::new(move || {
                if channel
                    .upgrade()
                    .is_some_and(|channel| channel.snapshot().end_sequence.is_some())
                    && !notified.swap(true, Ordering::AcqRel)
                {
                    std::panic::panic_any("published End wakeup panic");
                }
            }))
            .unwrap();
        fixture.submit();
        fixture.session.finish_input().unwrap();
        fixture.wait_terminal("finished");
        let shutdown = fixture.pool.shutdown();
        shutdown.expect("the actual CPU job exits after the independent channel catch");
        assert!(fixture.pool.poll_failure().is_none());
        assert_eq!(
            fixture.session.exit.status(),
            RootProducerExitStatus::Exited
        );
        assert_eq!(
            fixture.session.shared.authority.input_exit_progress(),
            RootWorkExitProgress::Exited
        );
        use novarocks_execution_contract::failure::{FailureClass, FailureReason, domain};
        let original = fixture
            .health
            .failure()
            .expect("the channel retains its actual health incident");
        assert_eq!(
            original.origin(),
            novarocks_execution::runtime::execution_health::ExecutionExitOrigin::Task {
                identity: fixture.session.spec().task,
                phase: novarocks_execution_contract::TaskFailurePhase::Execution,
            }
        );
        assert_eq!(original.fact().class(), FailureClass::Unexpected);
        assert_eq!(
            original.fact().reason(),
            FailureReason::new(domain::WORKER, std::num::NonZeroU16::new(163).unwrap())
        );
        assert!(panicked.load(Ordering::Acquire));
        assert_eq!(
            fixture.session.producer_exit_progress(),
            RootWorkExitProgress::Exited
        );
        let snapshot = fixture
            .session
            .channel()
            .try_ownership_snapshot()
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.ends_published, 1);
        assert_eq!(snapshot.producers_running, 0);
        assert_eq!(snapshot.producers_exited, 1);
        assert!(snapshot.metadata_holders > 0);
        assert!(
            snapshot.metadata_holders > metadata_before_fault,
            "the failed channel notification retains an additional actual metadata owner"
        );
        assert!(
            !fixture.session.channel().physical_idle(),
            "channel metadata responsibility did not exit with the CPU job"
        );
        assert!(!fixture.session.channel().is_closed());
        let state = fixture.session.state();
        assert!(state.completed);
        assert!(
            state.failed.is_none(),
            "committed normal End keeps its successful root conclusion"
        );
        assert!(
            state.cleanup_failure.is_none(),
            "channel metadata health is not a fabricated Native cleanup failure"
        );
        assert!(state.input.is_none());
        assert!(state.builder.is_none());
        assert!(state.producer.is_none());
        drop(state);
        fixture
            .session
            .abort(ResultAbort::Cancelled("late cancellation".into()));
        assert!(
            fixture
                .session
                .state()
                .take_terminal_observation()
                .is_none()
        );
        assert_eq!(fixture.observations(), ["finished"]);
        assert_eq!(
            fixture.session.producer_state(),
            RootProducerState::ParticipationHeld
        );
        assert_eq!(fixture.session.channel().snapshot().end_sequence, Some(1));
        assert_eq!(fixture.session.channel().snapshot().consumed_through, 0);
        assert_eq!(fixture.health.failure(), Some(original));
        assert_eq!(
            fixture.session.producer_exit_progress(),
            RootWorkExitProgress::Exited
        );
        drop(subscription);
        assert!(!fixture.session.channel().physical_idle());
        crate::root_unknown_test::complete_and_abort();
    }

    #[test]
    fn native_root_cold_installed_runtime_cancel_observes_exactly_one_abort() {
        let fixture = Fixture::new(false);
        assert!(fixture.session.state().producer.is_none());
        assert_eq!(
            fixture.session.producer_exit_progress(),
            RootWorkExitProgress::Pending
        );
        fixture
            .session
            .abort(ResultAbort::Cancelled("cold root deadline".into()));
        fixture.wait_terminal("aborted");
        fixture.pool.shutdown().unwrap();
        assert_eq!(
            fixture.session.producer_exit_progress(),
            RootWorkExitProgress::Exited
        );
        fixture
            .session
            .abort(ResultAbort::Cancelled("repeated cancellation".into()));
        assert!(
            fixture
                .session
                .state()
                .take_terminal_observation()
                .is_none()
        );
        assert!(
            fixture
                .session
                .state()
                .take_terminal_observation()
                .is_none()
        );
        assert_eq!(fixture.observations(), ["aborted"]);
        assert_eq!(
            fixture
                .session
                .channel()
                .try_ownership_snapshot()
                .unwrap()
                .unwrap()
                .ends_published,
            0
        );
    }

    #[test]
    fn native_root_live_cancel_or_failure_observes_only_after_real_pool_exit() {
        for reason in [
            ResultAbort::Cancelled("live root deadline".into()),
            ResultAbort::Failed("live root failure".into()),
        ] {
            let fixture = Fixture::new(true);
            fixture.submit();
            assert_eq!(
                fixture.session.producer_exit_progress(),
                RootWorkExitProgress::Pending
            );
            fixture.session.abort(reason.clone());
            fixture.session.abort(reason);
            // The only pool worker is already inside the gate. This assertion
            // cannot pass merely because the canceled worker has not scheduled.
            assert!(fixture.session.state().input.is_some());
            assert!(fixture.observations().is_empty());
            assert_eq!(
                fixture.session.producer_exit_progress(),
                RootWorkExitProgress::Pending
            );
            assert_eq!(
                fixture
                    .session
                    .channel()
                    .try_ownership_snapshot()
                    .unwrap()
                    .unwrap()
                    .producers_running,
                1
            );
            fixture.release();
            fixture.wait_terminal("aborted");
            fixture.pool.shutdown().unwrap();
            assert_eq!(
                fixture.session.producer_exit_progress(),
                RootWorkExitProgress::Exited
            );
            let state = fixture.session.state();
            assert!(state.input.is_none());
            assert!(state.builder.is_none());
            assert!(state.producer.is_none());
            drop(state);
            let snapshot = fixture
                .session
                .channel()
                .try_ownership_snapshot()
                .unwrap()
                .unwrap();
            assert_eq!(snapshot.producers_running, 0);
            assert_eq!(snapshot.producers_exited, 1);
            assert_eq!(snapshot.ends_published, 0);
            assert!(
                fixture
                    .session
                    .state()
                    .take_terminal_observation()
                    .is_none()
            );
            assert_eq!(fixture.observations(), ["aborted"]);
        }
    }

    #[test]
    fn native_root_prepare_rollback_and_never_started_do_not_publish_runtime_terminal() {
        for reason in [ResultAbort::PrepareRollback, ResultAbort::NeverStarted] {
            let fixture = Fixture::new(false);
            fixture.session.abort(reason);
            // Shutdown joins the real worker, including its dormant/cold job.
            fixture.pool.shutdown().unwrap();
            assert!(fixture.session.producer_exit_progress() == RootWorkExitProgress::Exited);
            assert!(
                fixture
                    .session
                    .state()
                    .take_terminal_observation()
                    .is_none()
            );
            assert!(fixture.observations().is_empty());
            assert_eq!(
                fixture
                    .session
                    .channel()
                    .try_ownership_snapshot()
                    .unwrap()
                    .unwrap()
                    .ends_published,
                0
            );
        }
    }

    fn assert_unresolved_health(fixture: &Fixture) {
        use novarocks_execution::runtime::execution_health::ExecutionExitOrigin;
        use novarocks_execution_contract::failure::FailureProjection;
        assert_eq!(
            fixture.session.producer_exit_progress(),
            RootWorkExitProgress::Unresolved
        );
        let original = fixture
            .health
            .failure()
            .expect("actual phase failure reaches runtime health");
        assert_eq!(original.origin(), ExecutionExitOrigin::Process);
        assert_eq!(
            original.fact(),
            fixture
                .pool
                .poll_failure()
                .expect("real pool failure")
                .failure_fact()
        );
        fixture
            .session
            .abort(ResultAbort::Cancelled("late unresolved observation".into()));
        assert_eq!(
            fixture.health.failure(),
            Some(original),
            "late cancellation cannot replace first health fault"
        );
        assert_eq!(
            fixture.session.producer_exit_progress(),
            RootWorkExitProgress::Unresolved
        );
    }

    #[test]
    fn native_root_real_hook_capture_drop_failure_is_unresolved_after_independent_cleanup() {
        if !crate::root_unknown_test::enter() {
            return;
        }
        let mut original_unknown_owners = Vec::new();

        struct PanicCapture(Arc<AtomicUsize>);
        impl Drop for PanicCapture {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
                panic!("injected Root cleanup hook capture destruction");
            }
        }
        for phase in [12, 13] {
            let fixture = Fixture::new(false);
            let drops = Arc::new(AtomicUsize::new(0));
            let capture = PanicCapture(Arc::clone(&drops));
            let callback: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
                std::hint::black_box(&capture);
            });
            let hook = match phase {
                12 => &fixture.session.shared.before_finish_wake,
                13 => &fixture.session.shared.before_pool_turn,
                _ => unreachable!(),
            };
            *hook.lock().unwrap() = Some(callback);
            fixture.submit();
            fixture.session.finish_input().unwrap();
            fixture.wait_terminal("finished");
            assert!(
                fixture.pool.shutdown().is_err(),
                "actual hook Drop phase must fail shutdown"
            );
            assert_eq!(
                fixture.pool.poll_failure(),
                Some(crate::root_producer_pool::RootProducerPoolFailure::Cleanup)
            );
            assert_unresolved_health(&fixture);
            assert_eq!(
                drops.load(Ordering::SeqCst),
                1,
                "the actual capture is destroyed once"
            );
            let state = fixture.session.state();
            assert!(state.completed);
            assert!(state.failed.is_none());
            assert!(state.input.is_none());
            assert!(state.builder.is_none());
            assert!(state.producer.is_none());
            drop(state);
            assert_eq!(fixture.observations(), ["finished"]);
            let ownership = fixture
                .session
                .channel()
                .try_ownership_snapshot()
                .unwrap()
                .unwrap();
            assert_eq!(ownership.ends_published, 1);
            assert_eq!(ownership.producers_exited, 1);
            assert_eq!(ownership.producers_running, 0);
            original_unknown_owners.push(fixture);
        }
        crate::root_unknown_test::complete_and_abort();
    }
    // Insert these items inside terminal_observation_tests. The existing Fixture,
    // GatedTurn and real NativeRootResultSession/RootProducerPool are reused.

    struct InputBacking {
        values: Box<[i64]>,
        dropped: Arc<AtomicUsize>,
    }
    impl Drop for InputBacking {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct InputOpaqueSecondary {
        data: std::mem::ManuallyDrop<Arc<InputBacking>>,
        dropped: Arc<AtomicUsize>,
    }
    impl Drop for InputOpaqueSecondary {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
            // This runs only if the supervision owner wrongly destroys an unknown
            // payload. The assertion also observes the original data's real Drop.
            unsafe { std::mem::ManuallyDrop::drop(&mut self.data) };
        }
    }
    struct InputOpaquePrimary {
        data: std::mem::ManuallyDrop<Arc<InputBacking>>,
        dropped: Arc<AtomicUsize>,
        secondary_dropped: Arc<AtomicUsize>,
    }
    impl Drop for InputOpaquePrimary {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
            // SAFETY: this payload's Drop executes once. Moving its original data
            // into the next payload keeps that exact allocation alive on unwind;
            // implicit field Drop must not release the data behind the pool's back.
            let data = unsafe { std::mem::ManuallyDrop::take(&mut self.data) };
            std::panic::panic_any(InputOpaqueSecondary {
                data: std::mem::ManuallyDrop::new(data),
                dropped: Arc::clone(&self.secondary_dropped),
            });
        }
    }
    struct InputForeignAllocation {
        data: Option<std::mem::ManuallyDrop<Arc<InputBacking>>>,
        armed: Arc<AtomicBool>,
        dropped: Arc<AtomicUsize>,
        primary_dropped: Arc<AtomicUsize>,
        secondary_dropped: Arc<AtomicUsize>,
    }
    impl Drop for InputForeignAllocation {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
            let data = self.data.take().expect("one real custom allocation owner");
            if std::thread::panicking() {
                // A setup/assertion failure must not abort the whole test binary
                // through a second unwind. ManuallyDrop retains the actual data.
                return;
            }
            if !self.armed.load(Ordering::SeqCst) {
                drop(std::mem::ManuallyDrop::into_inner(data));
                return;
            }
            std::panic::panic_any(InputOpaquePrimary {
                data,
                dropped: Arc::clone(&self.primary_dropped),
                secondary_dropped: Arc::clone(&self.secondary_dropped),
            });
        }
    }
    struct InputAllocationProbe {
        data: Weak<InputBacking>,
        armed: Arc<AtomicBool>,
        owner_dropped: Arc<AtomicUsize>,
        data_dropped: Arc<AtomicUsize>,
        primary_dropped: Arc<AtomicUsize>,
        secondary_dropped: Arc<AtomicUsize>,
    }
    fn actual_foreign_input() -> (Chunk, InputAllocationProbe) {
        use arrow::buffer::{Buffer, ScalarBuffer};
        use arrow::datatypes::Field;
        use novarocks_type_contract::{
            TypedValueBinding, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime,
        };
        let armed = Arc::new(AtomicBool::new(false));
        let owner_dropped = Arc::new(AtomicUsize::new(0));
        let data_dropped = Arc::new(AtomicUsize::new(0));
        let primary_dropped = Arc::new(AtomicUsize::new(0));
        let secondary_dropped = Arc::new(AtomicUsize::new(0));
        let data = Arc::new(InputBacking {
            values: vec![42_i64].into_boxed_slice(),
            dropped: Arc::clone(&data_dropped),
        });
        let weak = Arc::downgrade(&data);
        let pointer = std::ptr::NonNull::new(data.values.as_ptr().cast_mut().cast::<u8>()).unwrap();
        let owner = Arc::new(InputForeignAllocation {
            data: Some(std::mem::ManuallyDrop::new(data)),
            armed: Arc::clone(&armed),
            dropped: Arc::clone(&owner_dropped),
            primary_dropped: Arc::clone(&primary_dropped),
            secondary_dropped: Arc::clone(&secondary_dropped),
        });
        // SAFETY: the original aligned initialized i64 is retained by this real
        // Allocation owner through all Arrow aliases. Its exceptional exit moves
        // that same Arc into opaque payload custody, never frees it on unwind.
        let buffer =
            unsafe { Buffer::from_custom_allocation(pointer, std::mem::size_of::<i64>(), owner) };
        let array = Arc::new(Int64Array::new(ScalarBuffer::new(buffer, 0, 1), None));
        let pure = ValueTypeStorageLifetime::pure();
        let binding = TypedValueBinding::try_new(
            ValueTypeRef::try_new(ValueKind::Scalar(DataType::Int64), false, pure.clone()).unwrap(),
            ValueLayout::try_new(&DataType::Int64, pure.clone()).unwrap(),
        )
        .unwrap();
        let slot = ChunkSlotSchema::try_new_typed(
            SlotId::new(1),
            Arc::new(Field::new("value", DataType::Int64, false)),
            binding,
            None,
            pure.clone(),
        )
        .unwrap();
        let schema = Arc::new(ChunkSchema::try_new_typed(vec![slot], pure).unwrap());
        let chunk = Chunk::try_new_with_columns(schema, vec![array]).unwrap();
        (
            chunk,
            InputAllocationProbe {
                data: weak,
                armed,
                owner_dropped,
                data_dropped,
                primary_dropped,
                secondary_dropped,
            },
        )
    }

    fn input_resource_fixture(
        output: FrozenRootOutput,
    ) -> (
        Fixture,
        Arc<ResultRetainedBudget>,
        WorkerResultRetainedLimits,
    ) {
        let limits = WorkerResultRetainedLimits::try_new(256 << 20, 1024 << 20).unwrap();
        let budget = ResultRetainedBudget::new(limits.per_process());
        let health =
            Arc::new(novarocks_execution::runtime::execution_health::ExecutionHealth::new());
        let pool = RootProducerPool::try_new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(3).unwrap(),
            1 << 20,
            Arc::clone(&budget),
            Arc::clone(&health),
        )
        .unwrap();
        let blocker = Arc::new(GatedTurn {
            state: Mutex::new((false, false)),
            changed: Condvar::new(),
        });
        let registration = pool
            .reserve_for_test(RootCleanupPlan::new(&[0]))
            .unwrap()
            .commit(Arc::clone(&blocker) as Arc<dyn RootProducerJob>)
            .unwrap_or_else(|_| panic!("actual blocker must commit"));
        registration.wake().unwrap();
        blocker.wait_entered();
        let callback_pool = pool.clone();
        let callback_budget = budget.clone();
        let callback_health = health.clone();
        let task = root_task();
        let (session, node) = in_original_preparation(task, health.clone(), move |preparation| {
            let session = open_root_in_preparation(
                preparation,
                &callback_pool,
                RootResultWriteSpec {
                    task,
                    contract: Arc::new(RootOutputContract::new(RootProfileId::V1, output)),
                },
                callback_budget,
                limits,
                callback_health,
            )
            .unwrap();
            session.channel().mark_participation_owned().unwrap();
            session
        });
        node.retain_root_for_teardown(session.channel().clone());
        (
            Fixture {
                session,
                pool,
                blocker: Some((blocker, registration)),
                health,
                budget: budget.clone(),
                original_nodes: Mutex::new(vec![node]),
            },
            budget,
            limits,
        )
    }
    fn accepted_safe_sibling(
        fixture: &Fixture,
        budget: &Arc<ResultRetainedBudget>,
        limits: WorkerResultRetainedLimits,
    ) -> Arc<NativeRootResultSession> {
        let task = root_task();
        let pool = fixture.pool.clone();
        let budget = budget.clone();
        let health = fixture.health.clone();
        let body_health = health.clone();
        let (session, node) = in_original_preparation(task, health, move |preparation| {
            let session = open_root_in_preparation(
                preparation,
                &pool,
                RootResultWriteSpec {
                    task,
                    contract: Arc::new(RootOutputContract::new(
                        RootProfileId::V1,
                        FrozenRootOutput::CountOnly,
                    )),
                },
                budget,
                limits,
                body_health,
            )
            .unwrap();
            session.channel().mark_participation_owned().unwrap();
            session
        });
        node.retain_root_for_teardown(session.channel().clone());
        fixture.original_nodes.lock().unwrap().push(node);
        session
    }
    // Every observation below is an actual funded grant and real release against
    // the original ResultRetainedBudget, not a private counter or new ledger. The
    // worker is gated before the baseline and actually joined before the after
    // sample, so the only intervening retained credit is the bad input's protection.
    fn reserve_all_actual_spare(
        budget: &Arc<ResultRetainedBudget>,
        cap: usize,
    ) -> ResultWriteCredit {
        let (mut lower, mut upper) = (0, cap);
        while lower < upper {
            let requested = lower + (upper - lower).div_ceil(2);
            match budget.try_reserve_process(requested).unwrap() {
                ResultWriteAdmission::Granted(credit) => {
                    drop(credit);
                    lower = requested;
                }
                ResultWriteAdmission::Blocked => upper = requested - 1,
            }
        }
        let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(lower).unwrap()
        else {
            panic!("exact observed spare must still fund");
        };
        assert!(
            matches!(
                budget.try_reserve_process(1).unwrap(),
                ResultWriteAdmission::Blocked
            ),
            "the original budget is actually full while this grant lives"
        );
        credit
    }
    fn wait_input_resource_fact(mut observed: impl FnMut() -> bool) {
        let deadline = Instant::now() + WAIT;
        while !observed() {
            assert!(
                Instant::now() < deadline,
                "actual Native input resource fact did not arrive"
            );
            std::thread::yield_now();
        }
    }

    #[tokio::test]
    async fn native_client_root_preserves_the_explicit_frozen_trino_presentation() {
        use novarocks_execution_contract::root_result::{RootReadOutcome, RootResultRead};
        use novarocks_result_contract::{
            ClientPresentationFamily, ClientRenderSchema, NativeRenderType, RenderColumn,
            RenderField, RenderPresentation,
        };
        for (family, presentation, expected) in [(
            ClientPresentationFamily::TrinoJson(Default::default()),
            RenderPresentation::TrinoValue,
            b"\x04\0\0\0[42]".as_slice(),
        )] {
            let output = FrozenRootOutput::ClientRows(
                ClientRenderSchema::try_new(
                    family,
                    vec![RenderColumn {
                        source_ordinal: 0,
                        source_slot: Some(1),
                        name: "value".into(),
                        field: RenderField {
                            native_type: NativeRenderType::SignedInteger(64),
                            presentation,
                            nullable: false,
                        },
                    }],
                    1,
                )
                .unwrap(),
            );
            let (fixture, _, _) = input_resource_fixture(output);
            let (chunk, allocation) = actual_foreign_input();
            let RootInputAdmission::Granted(permit) = fixture.session.try_acquire_input().unwrap()
            else {
                panic!("the ClientRows root must admit its original input");
            };
            let mut submitted =
                Some(novarocks_execution::runtime::task::io::RootResultInput::new(chunk, permit));
            fixture.session.submit_input(&mut submitted).unwrap();
            assert!(submitted.is_none());
            fixture.session.finish_input().unwrap();
            fixture.release();
            fixture.wait_terminal("finished");
            fixture.pool.shutdown().unwrap();
            assert_eq!(
                fixture.session.producer_exit_progress(),
                RootWorkExitProgress::Exited
            );
            let channel = fixture.session.channel();
            let request = RootResultRead::try_new(
                channel.spec().task,
                channel.spec().contract.profile(),
                channel.spec().contract.kind(),
                Some(std::num::NonZeroU64::new(1).unwrap()),
                0,
                Duration::from_millis(1),
            )
            .unwrap();
            let delivery = channel.read(&request).await.unwrap();
            let RootReadOutcome::Data(data) = &delivery.reply().outcome else {
                panic!("the finished ClientRows root must retain its encoded segment");
            };
            assert_eq!(data.body().as_ref(), expected);
            assert_eq!(data.end_after_data().unwrap().output_rows, 1);
            assert!(
                allocation.data.upgrade().is_none(),
                "actual typed input exits before retained delivery"
            );
        }
    }

    #[test]
    fn native_chunk_unknown_exit_keeps_funding_and_allows_accepted_sibling_exit() {
        if !crate::root_unknown_test::enter() {
            return;
        }

        use novarocks_result_contract::{
            ClientRenderSchema, NativeRenderType, RenderColumn, RenderField, RenderPresentation,
        };
        let mut unknown_owners = Vec::new();
        for client in [false, true] {
            let output = if client {
                FrozenRootOutput::ClientRows(
                    ClientRenderSchema::try_new(
                        novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                            novarocks_result_contract::TrinoJsonFacts {
                                parametric_datetime: true,
                                variant: true,
                                variant_binary: true,
                            },
                        ),
                        vec![RenderColumn {
                            source_ordinal: 0,
                            source_slot: Some(1),
                            name: "value".into(),
                            field: RenderField {
                                native_type: NativeRenderType::SignedInteger(64),
                                presentation: RenderPresentation::TrinoValue,
                                nullable: false,
                            },
                        }],
                        1,
                    )
                    .unwrap(),
                )
            } else {
                FrozenRootOutput::CountOnly
            };
            let (fixture, budget, limits) = input_resource_fixture(output);
            // Strong sibling work is committed before the failure and receives its
            // normal finish request while the one real CPU worker is still gated.
            let safe = accepted_safe_sibling(&fixture, &budget, limits);
            let baseline = reserve_all_actual_spare(&budget, limits.per_process().get());
            let available_before = baseline.bytes();
            drop(baseline);
            let (chunk, allocation) = actual_foreign_input();
            let RootInputAdmission::Granted(permit) = fixture.session.try_acquire_input().unwrap()
            else {
                panic!("actual original input must fund before arming foreign Drop");
            };
            let input_bytes = permit.retained_bytes();
            assert_eq!(input_bytes, if client { 192 << 20 } else { 96 << 20 });
            let mut submitted =
                Some(novarocks_execution::runtime::task::io::RootResultInput::new(chunk, permit));
            fixture.session.submit_input(&mut submitted).unwrap();
            assert!(
                submitted.is_none(),
                "the Native work accepted the original named slot"
            );
            allocation.armed.store(true, Ordering::SeqCst);
            safe.finish_input().unwrap();
            assert_eq!(
                fixture.session.producer_exit_progress(),
                RootWorkExitProgress::Pending
            );
            assert_eq!(safe.producer_exit_progress(), RootWorkExitProgress::Pending);
            fixture.release();
            wait_input_resource_fact(|| {
                fixture.session.producer_exit_progress() == RootWorkExitProgress::Unresolved
                    && safe.producer_exit_progress() == RootWorkExitProgress::Exited
            });
            assert!(
                fixture.pool.shutdown().is_err(),
                "actual worker join reports the bad owner's unknown exit"
            );
            assert_unresolved_health(&fixture);
            assert_eq!(
                fixture.pool.poll_failure(),
                Some(crate::root_producer_pool::RootProducerPoolFailure::Turn)
            );
            assert_eq!(safe.producer_exit_progress(), RootWorkExitProgress::Exited);
            assert_eq!(safe.producer_state(), RootProducerState::ParticipationHeld);
            assert_eq!(safe.channel().snapshot().end_sequence, Some(1));
            let safe_ownership = safe.channel().try_ownership_snapshot().unwrap().unwrap();
            assert_eq!(safe_ownership.producers_running, 0);
            assert_eq!(safe_ownership.producers_exited, 1);
            assert_eq!(safe_ownership.ends_published, 1);
            let bad_ownership = fixture
                .session
                .channel()
                .try_ownership_snapshot()
                .unwrap()
                .unwrap();
            assert_eq!(
                bad_ownership.producers_running, 1,
                "unknown original input cannot release encoder responsibility"
            );
            assert_eq!(bad_ownership.producers_exited, 0);
            assert_eq!(
                fixture.session.channel().snapshot().produced_through,
                if client { 1 } else { 0 },
                "the ClientRows case passed the actual typed renderer before the final Chunk owner unwound"
            );
            assert_eq!(allocation.owner_dropped.load(Ordering::SeqCst), 1);
            assert_eq!(
                allocation.primary_dropped.load(Ordering::SeqCst),
                1,
                "the primary opaque payload was actually disposed"
            );
            assert_eq!(
                allocation.secondary_dropped.load(Ordering::SeqCst),
                0,
                "the secondary unknown payload remains in original custody"
            );
            assert_eq!(allocation.data_dropped.load(Ordering::SeqCst), 0);
            let data = allocation.data.upgrade().expect(
                "the original physical input allocation remains held by the unknown payload",
            );
            assert_eq!(data.values.as_ref(), &[42]);
            drop(data);
            let after = reserve_all_actual_spare(&budget, limits.per_process().get());
            let actual_protection = available_before.checked_sub(after.bytes()).unwrap();
            assert_eq!(
                actual_protection,
                input_bytes + if client { 2 << 20 } else { 0 },
                "real input and scratch grants cannot refund ahead of unknown Chunk data"
            );
            // With all other funded spare held, neither one byte nor the original
            // actual permit extent can be re-reserved from that same process.
            assert!(matches!(
                budget.try_reserve_process(input_bytes).unwrap(),
                ResultWriteAdmission::Blocked
            ));
            assert_eq!(
                fixture.session.producer_exit_progress(),
                RootWorkExitProgress::Unresolved
            );
            assert_eq!(allocation.data.upgrade().unwrap().values.as_ref(), &[42]);
            drop(after);
            unknown_owners.push((fixture, safe));
        }
        crate::root_unknown_test::complete_and_abort();
    }

    #[test]
    fn native_retiring_input_reentry_blocks_until_slot_reopens_and_second_wake_grants() {
        use novarocks_execution_contract::failure::{FailureFact, FailureProjection};
        enum Reentry {
            Blocked(bool),
            Granted(bool),
            Refused(FailureFact),
        }
        let fixture = Fixture::new(true);
        let (chunk, allocation) = actual_foreign_input();
        // This case leaves the real custom owner unarmed; it actually drops its
        // backing. The first case above exercises the same Arrow path's unwind.
        let RootInputAdmission::Granted(permit) = fixture.session.try_acquire_input().unwrap()
        else {
            panic!("actual first input position");
        };
        let first_generation = permit.generation();
        let mut submitted =
            Some(novarocks_execution::runtime::task::io::RootResultInput::new(chunk, permit));
        fixture.session.submit_input(&mut submitted).unwrap();
        assert!(submitted.is_none());
        let reactions = Arc::new(Mutex::new(Vec::with_capacity(4)));
        let granted = Arc::new(Mutex::new(None));
        let ready = Arc::new(AtomicBool::new(false));
        let in_callback = Arc::new(AtomicBool::new(false));
        let session = Arc::downgrade(&fixture.session);
        let observed = Arc::clone(&reactions);
        let retained = Arc::clone(&granted);
        let finished = Arc::clone(&ready);
        let callback_active = Arc::clone(&in_callback);
        let data = allocation.data.clone();
        let subscription = fixture
            .session
            .writable_observable()
            .subscribe(Arc::new(move || {
                if finished.load(Ordering::Acquire) || callback_active.swap(true, Ordering::AcqRel)
                {
                    return;
                }
                let session = session
                    .upgrade()
                    .expect("external fixture still owns the session");
                let data_exited = data.upgrade().is_none();
                match session.try_acquire_input() {
                    Ok(RootInputAdmission::Blocked) => {
                        observed.lock().unwrap().push(Reentry::Blocked(data_exited))
                    }
                    Ok(RootInputAdmission::Granted(permit)) => {
                        // Keep this real permit outside the callback. Dropping it here
                        // would recursively notify the very observer under test.
                        *retained.lock().unwrap() = Some(permit);
                        observed.lock().unwrap().push(Reentry::Granted(data_exited));
                        finished.store(true, Ordering::Release);
                    }
                    Err(failure) => {
                        observed
                            .lock()
                            .unwrap()
                            .push(Reentry::Refused(failure.failure_fact()));
                        finished.store(true, Ordering::Release);
                    }
                }
                callback_active.store(false, Ordering::Release);
            }));
        fixture.release();
        wait_input_resource_fact(|| ready.load(Ordering::Acquire));
        drop(subscription);
        let next = granted
            .lock()
            .unwrap()
            .take()
            .expect("a post-retirement wake must actually grant the next input");
        assert!(next.generation() > first_generation);
        let observations = reactions.lock().unwrap();
        assert!(
            observations.len() >= 2,
            "a refund-time Blocked and a later Grant are independently observed"
        );
        for observation in &observations[..observations.len() - 1] {
            match observation {
                Reentry::Blocked(true) => {}
                Reentry::Blocked(false) => {
                    panic!("credit notification preceded actual original data exit")
                }
                Reentry::Granted(_) => panic!("the first input was granted more than once"),
                Reentry::Refused(fact) => {
                    panic!("normal reentrant input admission failed: {fact:?}")
                }
            }
        }
        assert!(
            matches!(observations.last(), Some(Reentry::Granted(true))),
            "only the wake after the actual input slot reopens can grant"
        );
        drop(observations);
        assert!(allocation.data.upgrade().is_none());
        assert_eq!(allocation.owner_dropped.load(Ordering::SeqCst), 1);
        assert_eq!(allocation.data_dropped.load(Ordering::SeqCst), 1);
        assert_eq!(allocation.primary_dropped.load(Ordering::SeqCst), 0);
        assert_eq!(allocation.secondary_dropped.load(Ordering::SeqCst), 0);
        drop(next);
        fixture.session.finish_input().unwrap();
        fixture.wait_terminal("finished");
        fixture.pool.shutdown().unwrap();
        assert_eq!(
            fixture.session.producer_exit_progress(),
            RootWorkExitProgress::Exited
        );
        assert!(fixture.health.is_healthy());
    }

    #[test]
    fn actual_root_budget_blocked_refund_waits_for_real_funding_wake_without_self_excitation() {
        let (fixture, _budget, _limits) = input_resource_fixture(FrozenRootOutput::CountOnly);
        let channel = fixture.session.channel();
        let bytes = fixture.session.shared.authority.required_bytes();
        let ResultWriteAdmission::Granted(first) = channel.try_reserve(bytes).unwrap() else {
            panic!("first real retained credit")
        };
        let ResultWriteAdmission::Granted(second) = channel.try_reserve(bytes).unwrap() else {
            panic!("second real retained credit")
        };
        assert!(
            matches!(
                channel.try_reserve(bytes).unwrap(),
                ResultWriteAdmission::Blocked
            ),
            "the original per-root budget actually refuses another complete input"
        );
        let observable = fixture.session.writable_observable();
        let notifications = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&notifications);
        let subscription = observable
            .try_subscribe(Arc::new(move || {
                observed.fetch_add(1, Ordering::SeqCst);
            }))
            .unwrap();
        let before = observable.generation();
        let blocked = (0..3)
            .map(|_| fixture.session.try_acquire_input())
            .collect::<Vec<_>>();
        let after_blocked = observable.generation();
        let self_notifications = notifications.load(Ordering::SeqCst);
        // This is the real channel credit's budget release and original
        // budget->channel->WakeBridge path, not a fabricated notification.
        drop(first);
        let after_funding = observable.generation();
        let funding_notifications = notifications.load(Ordering::SeqCst);
        let admitted = fixture.session.try_acquire_input();
        let RootInputAdmission::Granted(mut permit) = admitted.unwrap() else {
            panic!("released original funding must admit the exact input")
        };
        let grant_exit = permit.retire();
        drop(permit);
        drop(second);
        drop(subscription);
        fixture.session.finish_input().unwrap();
        fixture.release();
        let joined = fixture.pool.shutdown();
        assert!(joined.is_ok(), "actual finite worker cleanup and join");
        assert!(
            blocked
                .into_iter()
                .all(|result| matches!(result.unwrap(), RootInputAdmission::Blocked))
        );
        assert_eq!(before, after_blocked);
        assert_eq!(self_notifications, 0);
        assert!(after_funding > after_blocked);
        assert!(funding_notifications > 0);
        assert_eq!(grant_exit, RootWorkExitProgress::Exited);
        assert_eq!(
            fixture.session.producer_exit_progress(),
            RootWorkExitProgress::Exited
        );
        assert!(fixture.health.is_healthy());
    }

    struct IssuerExitWake(AtomicUsize);
    impl std::task::Wake for IssuerExitWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[test]
    fn issuer_notification_after_actual_pool_exit_wakes_original_runtime_observer_again() {
        use std::sync::mpsc;
        struct Gate(Option<mpsc::Sender<()>>);
        impl Drop for Gate {
            fn drop(&mut self) {
                if let Some(send) = self.0.take() {
                    let _ = send.send(());
                }
            }
        }
        let fixture = Fixture::new(false);
        let RootInputAdmission::Granted(mut permit) = fixture.session.try_acquire_input().unwrap()
        else {
            panic!("original funded grant")
        };
        let data = Arc::new(vec![79_u8; 8192]);
        let weak = Arc::downgrade(&data);
        let observations = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&observations);
        let (entered_send, entered) = mpsc::channel();
        let (release_send, release) = mpsc::channel();
        let mut gate = Gate(Some(release_send));
        let release = Mutex::new(release);
        let subscription = fixture
            .session
            .writable_observable()
            .try_subscribe(Arc::new(move || {
                std::hint::black_box(&data);
                if observed.fetch_add(1, Ordering::SeqCst) == 0 {
                    entered_send.send(()).unwrap();
                    release.lock().unwrap().recv().unwrap();
                }
            }))
            .unwrap();
        let credit_exit = std::thread::spawn(move || permit.retire());
        entered.recv().unwrap();
        let wake = Arc::new(IssuerExitWake(AtomicUsize::new(0)));
        let waker = std::task::Waker::from(Arc::clone(&wake));
        let initially_pending = fixture.session.poll_producer_exit(&waker);
        fixture.session.abort(ResultAbort::PrepareRollback);
        // The blocked foreign notification runs on another real owner. The
        // pool's own finite cleanup can actually finish and be joined first.
        let pool_exit = fixture.pool.shutdown();
        let pool_status = fixture.session.exit.status();
        let pending_after_pool = fixture.session.poll_producer_exit(&waker);
        let wakes_before_notification_exit = wake.0.load(Ordering::SeqCst);
        if let Some(send) = gate.0.take() {
            let _ = send.send(());
        }
        drop(gate);
        let original_grant_exit = credit_exit.join();
        let wakes_after_notification_exit = wake.0.load(Ordering::SeqCst);
        let whole_exit = fixture.session.poll_producer_exit(&waker);
        drop(subscription);
        assert!(pool_exit.is_ok());
        assert_eq!(pool_status, RootProducerExitStatus::Exited);
        assert_eq!(initially_pending, RootWorkExitProgress::Pending);
        assert_eq!(pending_after_pool, RootWorkExitProgress::Pending);
        assert_eq!(original_grant_exit.unwrap(), RootWorkExitProgress::Exited);
        assert!(
            wakes_before_notification_exit > 0,
            "the pool really published to the original observer"
        );
        assert!(
            wakes_after_notification_exit > wakes_before_notification_exit,
            "actual later issuer completion publishes a separate covered wake"
        );
        assert_eq!(whole_exit, RootWorkExitProgress::Exited);
        assert!(
            weak.upgrade().is_none(),
            "actual notification capture exits after its real thread join"
        );
        assert!(fixture.health.is_healthy());
    }

    struct CreditOpaqueSecondary {
        _original: std::mem::ManuallyDrop<Arc<Vec<u8>>>,
    }
    struct CreditOpaquePrimary {
        original: std::mem::ManuallyDrop<Arc<Vec<u8>>>,
        drops: Arc<AtomicUsize>,
    }
    impl Drop for CreditOpaquePrimary {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            let original = unsafe { std::mem::ManuallyDrop::take(&mut self.original) };
            std::panic::panic_any(CreditOpaqueSecondary {
                _original: std::mem::ManuallyDrop::new(original),
            });
        }
    }
    #[test]
    fn channel_credit_notification_unknown_keeps_opaque_custody_after_input_exit_and_preserves_peer()
     {
        if !crate::root_unknown_test::enter() {
            return;
        }

        use novarocks_execution_contract::AbortCause;
        let (fixture, task, _) = actual_task_fixture_with_gate(true);
        let health = fixture.health.clone();
        let baseline = reserve_all_actual_spare(&fixture.budget, 1024 << 20);
        let available_before_input = baseline.bytes();
        drop(baseline);
        let metadata_before_fault = fixture
            .session
            .channel()
            .try_ownership_snapshot()
            .unwrap()
            .unwrap()
            .metadata_holders;
        let original = Arc::new(vec![97_u8; 8192]);
        let weak = Arc::downgrade(&original);
        let armed = Arc::new(AtomicBool::new(false));
        let should_panic = Arc::clone(&armed);
        let payload_drops = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::clone(&payload_drops);
        // Native already owns the other bounded channel callback. This one
        // faults the channel notification nested in real credit retirement.
        // Its opaque custody is independent of the original input grant.
        let subscription = fixture
            .session
            .channel()
            .writable_observable()
            .try_subscribe(Arc::new(move || {
                if should_panic.swap(false, Ordering::SeqCst) {
                    std::panic::panic_any(CreditOpaquePrimary {
                        original: std::mem::ManuallyDrop::new(Arc::clone(&original)),
                        drops: Arc::clone(&dropped),
                    });
                }
            }))
            .unwrap();
        let (chunk, actual_data) = actual_foreign_input();
        let RootInputAdmission::Granted(permit) = fixture.session.try_acquire_input().unwrap()
        else {
            panic!("funded actual original")
        };
        assert_eq!(permit.retained_bytes(), 96 << 20);
        let mut offered =
            Some(novarocks_execution::runtime::task::io::RootResultInput::new(chunk, permit));
        fixture.session.submit_input(&mut offered).unwrap();
        assert!(offered.is_none());
        task.abort(AbortCause::PeerTaskFailed);
        let peer = task.status().accepted().unwrap();
        armed.store(true, Ordering::SeqCst);
        fixture.release();
        wait_input_resource_fact(|| {
            actual_data.data_dropped.load(Ordering::SeqCst) == 1
                && payload_drops.load(Ordering::SeqCst) == 1
        });
        let joined = fixture.pool.shutdown();
        drop(subscription);
        let accepted = task.status().accepted().unwrap();
        joined
            .expect("the actual input and CPU job exit despite separate channel metadata unknown");
        assert!(fixture.pool.poll_failure().is_none());
        assert_eq!(
            fixture.session.exit.status(),
            RootProducerExitStatus::Exited
        );
        assert_eq!(
            fixture.session.shared.authority.input_exit_progress(),
            RootWorkExitProgress::Exited
        );
        assert!(
            Arc::ptr_eq(&peer, &accepted),
            "pure channel metadata health cannot refine the legitimate Peer placeholder"
        );
        assert!(accepted.originating_failure().is_none());
        assert_eq!(
            fixture.session.producer_exit_progress(),
            RootWorkExitProgress::Exited
        );
        let state = fixture.session.state();
        assert!(
            state.input.is_none(),
            "all actual input phases, including the exact grant, exited"
        );
        drop(state);
        assert!(actual_data.data.upgrade().is_none());
        assert_eq!(actual_data.data_dropped.load(Ordering::SeqCst), 1);
        let ResultWriteAdmission::Granted(returned) = fixture
            .budget
            .try_reserve_process(available_before_input)
            .unwrap()
        else {
            panic!("the real 96 MiB input grant was not returned to its original budget");
        };
        drop(returned);
        let snapshot = fixture
            .session
            .channel()
            .try_ownership_snapshot()
            .unwrap()
            .unwrap();
        assert!(snapshot.metadata_holders > 0);
        assert!(snapshot.metadata_holders > metadata_before_fault);
        assert!(!fixture.session.channel().physical_idle());
        assert_eq!(payload_drops.load(Ordering::SeqCst), 1);
        assert_eq!(weak.upgrade().unwrap().as_slice(), &[97_u8; 8192]);
        let first = health.failure().unwrap();
        use novarocks_execution_contract::failure::{FailureClass, FailureReason, domain};
        assert_eq!(first.fact().class(), FailureClass::Unexpected);
        assert_eq!(
            first.fact().reason(),
            FailureReason::new(domain::WORKER, std::num::NonZeroU16::new(163).unwrap())
        );
        assert_eq!(
            first.origin(),
            novarocks_execution::runtime::execution_health::ExecutionExitOrigin::Task {
                identity: fixture.session.spec().task,
                phase: novarocks_execution_contract::TaskFailurePhase::Execution
            }
        );
        fixture
            .session
            .abort(ResultAbort::Cancelled("late cleanup observation".into()));
        assert_eq!(health.failure(), Some(first));
        assert!(Arc::ptr_eq(&peer, &task.status().accepted().unwrap()));
        assert_eq!(
            fixture.session.producer_exit_progress(),
            RootWorkExitProgress::Exited
        );
        assert!(fixture.session.try_acquire_input().is_err());
        assert!(!fixture.session.channel().physical_idle());
        crate::root_unknown_test::complete_and_abort();
    }
}

#[cfg(test)]
mod failure_record_tests {
    use super::*;
    use novarocks_execution_contract::{SourceRetentionCapability, failure::FailureProjection};

    #[derive(Debug)]
    struct SourceExit {
        exits: Arc<AtomicUsize>,
        unwound: Arc<AtomicBool>,
    }
    impl SourceRetentionCapability for SourceExit {}
    impl Drop for SourceExit {
        fn drop(&mut self) {
            self.unwound
                .store(std::thread::panicking(), Ordering::Release);
            self.exits.fetch_add(1, Ordering::AcqRel);
        }
    }
    fn source() -> (ExecutionFailure, Arc<AtomicUsize>, Arc<AtomicBool>) {
        let source: ExecutionFailure = novarocks_functions::KernelFailure::DeadlineExceeded.into();
        let exits = Arc::new(AtomicUsize::new(0));
        let unwound = Arc::new(AtomicBool::new(false));
        source.retain_source_lifetime(Box::new(SourceExit {
            exits: exits.clone(),
            unwound: unwound.clone(),
        }));
        (source, exits, unwound)
    }

    #[test]
    fn neutral_record_retains_the_first_fact_without_retaining_the_source_graph() {
        let (source, exits, _) = source();
        let original = source.clone();
        let fact = source.failure_fact();
        let mut record = RootFailureRecord::new(source);
        assert_eq!(
            record.producer_state(),
            RootProducerState::AwaitTerminalControl
        );
        let source = ManuallyDrop::into_inner(record.begin_action().unwrap().unwrap());
        assert!(std::ptr::eq(source.cause(), original.cause()));
        record.observation = Some(RootFailureObservation::from_failure(&source));
        record.finish_action(FailureSourcePhase::Transferred);
        assert!(record.begin_action().unwrap().is_none());
        let retained = record.producer_state();
        assert!(
            matches!(&retained, RootProducerState::Failed(observation) if observation.fact() == fact)
        );
        drop(source);
        assert_eq!(
            exits.load(Ordering::Acquire),
            0,
            "an actual caller still owns E"
        );
        drop(original);
        assert_eq!(exits.load(Ordering::Acquire), 1);
        assert_eq!(record.producer_state(), retained);
        assert_eq!(record.phase, FailureSourcePhase::Transferred);
    }

    #[test]
    fn a_started_source_cannot_be_inferred_exited_from_its_empty_field() {
        let (source, exits, _) = source();
        let mut record = RootFailureRecord::new(source);
        let source = record.begin_action().unwrap().unwrap();
        assert!(record.live_source.is_none());
        assert!(record.begin_action().is_err());
        record.restore_unresolved(Some(source));
        assert_eq!(record.phase, FailureSourcePhase::Unresolved);
        assert!(record.live_source.is_some());
        assert!(record.begin_action().is_err());
        assert_eq!(exits.load(Ordering::Acquire), 0);
        // Test teardown destroys the actual retained object, without changing
        // or discharging the unknown record's independent state.
        drop(ManuallyDrop::into_inner(record.live_source.take().unwrap()));
        assert_eq!(exits.load(Ordering::Acquire), 1);
        assert_eq!(record.phase, FailureSourcePhase::Unresolved);
    }

    #[test]
    fn a_source_consumed_during_unwind_does_not_become_a_successful_handoff() {
        let (source, exits, unwound) = source();
        let mut record = RootFailureRecord::new(source);
        let source = record.begin_action().unwrap().unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _original = ManuallyDrop::into_inner(source);
            panic!("the actual source-consuming call did not return");
        }));
        assert!(result.is_err());
        record.restore_unresolved(None);
        assert!(record.live_source.is_none());
        assert_eq!(exits.load(Ordering::Acquire), 1);
        assert!(unwound.load(Ordering::Acquire));
        assert_eq!(record.phase, FailureSourcePhase::Unresolved);
        assert!(record.begin_action().is_err());
        assert_eq!(
            record.producer_state(),
            RootProducerState::AwaitTerminalControl
        );
    }

    struct ReentrantSourceExit {
        target: Arc<std::sync::OnceLock<std::sync::Weak<Mutex<RootFailureRecord>>>>,
        observed: Arc<AtomicBool>,
    }
    impl std::fmt::Debug for ReentrantSourceExit {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("ReentrantSourceExit")
        }
    }
    impl SourceRetentionCapability for ReentrantSourceExit {}
    impl Drop for ReentrantSourceExit {
        fn drop(&mut self) {
            let record = self.target.get().unwrap().upgrade().unwrap();
            let available = record.try_lock().is_ok();
            self.observed.store(available, Ordering::Release);
        }
    }
    #[test]
    fn source_extraction_leaves_the_original_lock_free_for_its_real_destructor() {
        let source: ExecutionFailure = novarocks_functions::KernelFailure::ResourceExhausted.into();
        let target = Arc::new(std::sync::OnceLock::new());
        let observed = Arc::new(AtomicBool::new(false));
        source.retain_source_lifetime(Box::new(ReentrantSourceExit {
            target: target.clone(),
            observed: observed.clone(),
        }));
        let record = Arc::new(Mutex::new(RootFailureRecord::new(source)));
        assert!(target.set(Arc::downgrade(&record)).is_ok());
        let source = record.lock().unwrap().begin_action().unwrap().unwrap();
        let observation = RootFailureObservation::from_failure(&source);
        record.lock().unwrap().observation = Some(observation);
        drop(ManuallyDrop::into_inner(source));
        assert!(observed.load(Ordering::Acquire));
        record
            .lock()
            .unwrap()
            .finish_action(FailureSourcePhase::Disposed);
        assert!(matches!(
            record.lock().unwrap().producer_state(),
            RootProducerState::Failed(_)
        ));
    }
}

#[cfg(test)]
mod producer_tests;

// Cursor/builder ownership is returned even during
// unwind; the original input and permit remain in ProducerState throughout.
struct CowFlight<'a> {
    owner: &'a RootSessionState,
    generation: u64,
    encoder: Option<std::mem::ManuallyDrop<InputEncoder>>,
    builder: Option<RootSegmentBuilder>,
    completed: bool,
}
impl CowFlight<'_> {
    fn restore(&mut self, state: &mut ProducerState) {
        let input = state.input.as_mut().expect("original in-flight COW input");
        assert_eq!(
            input
                ._permit
                .as_ref()
                .expect("original COW permit")
                .generation(),
            self.generation,
            "in-flight COW cursor must return to its original generation"
        );
        assert!(input.encoder.is_none() && state.builder.is_none());
        input.encoder = self.encoder.take();
        state.builder = self.builder.take();
        // Only normal return proves that synchronous work stopped touching the
        // input. An unwind retains the execution marker and original owners.
        if self.completed {
            input.execution = InputExecution::Idle;
        }
    }
}
impl Drop for CowFlight<'_> {
    fn drop(&mut self) {
        // Moving owners back is metadata only. No encoder, array, byte receipt
        // or builder is destroyed under ProducerState's mutex.
        let mut state = self.owner.state();
        self.restore(&mut state);
    }
}

/// One actual normalization scope under the original Root budget. Deep value
/// requests extend the same receipt, so depth never creates credit positions.
pub(crate) struct CowNormalizationRetention {
    scope: novarocks_worker::root_result_channel::RootConstructionReservation,
}
impl CowNormalizationRetention {
    fn reserve(&mut self, layout: std::alloc::Layout) -> ExecutionResult<()> {
        self.reserve_bytes(layout.size())
    }
    fn reserve_bytes(&mut self, bytes: usize) -> ExecutionResult<()> {
        self.scope
            .try_grow(bytes)
            .map_err(|error| error.into_execution())
    }
}

impl RootSessionState {
    fn cow(&self) -> bool {
        self.spec().contract.kind()
            == RootOutputKind::InternalFacts(InternalResultDomain::CowSelectionArrowV1)
    }
    fn cow_checkpoint(&self) -> ExecutionResult<()> {
        let state = self.state();
        if state.failed.is_some() || state.cleanup_started || self.channel.is_closed() {
            Err(RootSessionFailure::ObservedStop.into())
        } else {
            Ok(())
        }
    }
    fn construct_cow_encoder(
        &self,
        chunk: &novarocks_execution::exec::chunk::Chunk,
        totals: CowSelectionTotals,
        capacity: usize,
    ) -> ExecutionResult<InputEncoder> {
        use crate::root_cow_selection_codec::{CowSelectionCodecError, CowSelectionRecipe};
        use novarocks_value_arrays::{
            ValueFailure,
            hydrate::{HydrateOptions, HydratePlan, HydrateProjection},
        };
        CowSelectionRecipe::validate_supported(chunk.record_batch().schema_ref())
            .map_err(ExecutionFailure::from_domain)?;
        let recipe = self
            .cow_recipe
            .as_ref()
            .ok_or_else(|| ExecutionFailure::from_domain(CowSelectionCodecError::RecipeMismatch))?;
        if chunk.columns().len() != recipe.declarations().len() {
            return Err(ExecutionFailure::from_domain(
                CowSelectionCodecError::SchemaChanged,
            ));
        }
        for (slot, expected) in chunk
            .chunk_schema()
            .slots()
            .iter()
            .zip(recipe.declarations())
        {
            if !expected
                .typed_binding()
                .borrowed()
                .exactly_equals_observed(slot.typed_binding().borrowed(), || {
                    self.cow_checkpoint()
                })?
            {
                return Err(ExecutionFailure::from_domain(
                    CowSelectionCodecError::RecipeMismatch,
                ));
            }
        }
        let mut retained = CowNormalizationRetention {
            scope: self
                .channel
                .begin_cow_construction()
                .map_err(|error| error.into_execution())?,
        };
        let mut context = novarocks_value_arrays::decode::ValueConversionContext::new(0);
        retained.reserve(
            std::alloc::Layout::array::<arrow::array::ArrayRef>(chunk.columns().len())
                .map_err(|_| ExecutionFailure::from_domain(CowSelectionCodecError::ScratchLimit))?,
        )?;
        let mut columns = Vec::with_capacity(chunk.columns().len());
        let value_failure = |error| match error {
            ValueFailure::Caller(error) => error,
            ValueFailure::Value(error) => ExecutionFailure::from_domain(error),
        };
        for ((source, declaration), field) in chunk
            .columns()
            .iter()
            .zip(recipe.declarations())
            .zip(recipe.schema().fields())
        {
            self.cow_checkpoint()?;
            let mut growth = |layout| retained.reserve(layout);
            let mut work = |_| self.cow_checkpoint();
            let plan = HydratePlan::plan_with_options_observed(
                source,
                declaration.typed_binding(),
                HydrateOptions {
                    dictionary_value_layout: None,
                    projection: HydrateProjection::FrozenNamesAndNullability,
                },
                &mut context,
                &mut |_, _| false,
                &mut growth,
                &mut work,
                &mut |_| Ok(()),
            )
            .map_err(value_failure)?;
            plan.require_complete()
                .map_err(ExecutionFailure::from_domain)?;
            let column = plan
                .execute_observed(&mut context, &mut growth, &mut work, &mut |_| Ok(()))
                .map_err(value_failure)?;
            let retained = std::cell::RefCell::new(&mut retained);
            let column = novarocks_functions::array_projection::rebuild_array_observed(
                &column,
                field.data_type(),
                &mut |step| {
                    novarocks_functions::array_projection::observe_array_normalization_construction::<
                        ExecutionFailure,
                    >(
                        step,
                        &mut |layout| {
                            self.cow_checkpoint()?;
                            retained.borrow_mut().reserve(layout)
                        },
                        &mut |bound| {
                            self.cow_checkpoint()?;
                            retained.borrow_mut().reserve_bytes(bound.bytes_upper)
                        },
                        &mut |_| self.cow_checkpoint(),
                    )
                },
            )?;
            columns.push(column);
        }
        self.cow_checkpoint()?;
        let batch = arrow::record_batch::RecordBatch::try_new(Arc::clone(recipe.schema()), columns).map_err(|source| {
            ExecutionFailure::from_domain(novarocks_functions::array_projection::ArrayProjectionFailure::Arrow {
                operation: novarocks_functions::array_projection::ArrayProjectionOperation::RebuildArray,
                source,
            })
        })?;
        let mut encoder = CowSelectionEncoder::try_new(&batch, recipe, totals, capacity)
            .map_err(ExecutionFailure::from_domain)?;
        encoder.retain_normalization(retained);
        Ok(InputEncoder::CowSelection(Box::new(encoder)))
    }
    fn advance_cow(&self) -> ExecutionResult<RootProducerTurn> {
        let mut state = self.state();
        state.completed |= self.channel.snapshot().end_sequence.is_some();
        if state.completed || state.failed.is_some() {
            return Ok(RootProducerTurn::Complete);
        }
        if self.channel.is_closed() {
            return Err(RootSessionFailure::ObservedStop.into());
        }
        if state.input.is_none() {
            return self.advance(&mut state);
        }
        let constructing = state.input.as_ref().unwrap().encoder.is_none();
        let capacity = NativeResultSupportGeometry::V1.root_scratch_capacity_bytes as usize;
        if constructing {
            let credit = match self.channel.try_reserve(capacity) {
                Ok(ResultWriteAdmission::Granted(credit)) => credit,
                Ok(ResultWriteAdmission::Blocked) => return Ok(RootProducerTurn::Blocked),
                Err(error) => return Err(error.into_execution()),
            };
            state.input.as_mut().unwrap().scratch = Some(std::mem::ManuallyDrop::new(credit));
        } else if state.builder.is_none() {
            match self.channel.try_segment() {
                Ok(Some(builder)) => {
                    state.builder = Some(builder);
                    state.used = 0;
                }
                Ok(None) => return Ok(RootProducerTurn::Blocked),
                Err(error) => return Err(error.into_execution()),
            }
        }
        let totals = state.cow_totals.clone();
        let used = state.used;
        let source = if constructing {
            // Chunk clones only its existing Arc/cold owners, with no column
            // allocation or traversal. The original funded input stays here.
            Some(state.input.as_ref().unwrap().chunk().clone())
        } else {
            None
        };
        let builder = state.builder.take();
        let input = state.input.as_mut().unwrap();
        input.execution = if constructing {
            InputExecution::Constructing
        } else {
            InputExecution::Rendering
        };
        let generation = input._permit.as_ref().unwrap().generation();
        let mut flight = CowFlight {
            owner: self,
            generation,
            encoder: input.encoder.take(),
            builder,
            completed: false,
        };
        drop(state);
        let rendered = if let Some(source) = source {
            let encoder = self.construct_cow_encoder(&source, totals, capacity);
            // The temporary source alias exits on the actual producer stack.
            drop(source);
            match encoder {
                Ok(encoder) => {
                    flight.encoder = Some(std::mem::ManuallyDrop::new(encoder));
                    Ok(None)
                }
                Err(error) => Err(error),
            }
        } else {
            flight
                .encoder
                .as_mut()
                .unwrap()
                .step(flight.builder.as_mut().unwrap().output_at(used))
                .map(Some)
        };
        flight.completed = true;
        drop(flight);
        let mut state = self.state();
        // Concurrent cancel/seal observations cannot be overwritten by a late
        // cursor return. Its exact generation was restored before this check.
        if state.failed.is_some() || state.cleanup_started || self.channel.is_closed() {
            return Ok(RootProducerTurn::Complete);
        }
        let Some(turn) = rendered? else {
            return Ok(RootProducerTurn::Yielded);
        };
        state.used += turn.emitted_bytes;
        self.channel
            .note_rows(turn.completed_rows)
            .map_err(|error| error.into_execution())?;
        let complete = turn.status == RenderTurnStatus::InputComplete;
        if complete {
            state.cow_totals = state
                .input
                .as_ref()
                .unwrap()
                .encoder
                .as_ref()
                .unwrap()
                .cow_selection_totals()
                .unwrap();
            state.input.as_mut().unwrap().retiring = true;
        }
        let finishing = complete && state.sealed;
        if finishing {
            self.channel
                .request_finish()
                .map_err(|error| error.into_execution())?;
        }
        let flush = complete
            || turn.completed_rows != 0
            || turn.status == RenderTurnStatus::NeedsOutput
            || state.used == RootProfileV1::SEGMENT_BYTES;
        if flush {
            let builder = state.builder.take().unwrap();
            let bytes = std::mem::take(&mut state.used);
            if bytes != 0 {
                self.channel
                    .publish_segment(builder, bytes, finishing)
                    .map_err(|error| error.into_execution())?;
            } else {
                drop(state);
                drop(builder);
            }
            if finishing {
                if bytes == 0 {
                    self.channel
                        .publish_end()
                        .map_err(|error| error.into_execution())?;
                }
                return Ok(RootProducerTurn::Complete);
            }
        }
        Ok(RootProducerTurn::Yielded)
    }
}
