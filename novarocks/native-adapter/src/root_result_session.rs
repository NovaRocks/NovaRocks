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

use std::alloc::Layout;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};

use novarocks_execution::exec::chunk::{
    Chunk, RootArrayStorageLimits, borrowed_root_chunk_storage,
};
use novarocks_execution::runtime::fragment::io::{
    FragmentIoError, FragmentIoErrorKind, FragmentIoOperation, ResultAbort, ResultWriteAdmission,
    ResultWriteCredit, RootInputAdmission, RootInputAuthority, RootInputPermit, RootProducerState,
    RootResultSession, RootResultWriteSpec,
};
use novarocks_execution::runtime::observable::{Observable, ObserverSubscription};
use novarocks_execution_contract::native_result_support::NativeResultSupportGeometry;
use novarocks_execution_contract::root_lifetime::RootRetentionClose;
use novarocks_result_contract::{
    BorrowedScalarLeaf, FrozenRootOutput, InternalResultDomain, RootOutputKind, RootProfileV1,
    ScalarLeafCursor,
};
use novarocks_result_render::{
    ArrowMysqlTextEncoder, BoundedMysqlTextEncoder, RenderTurn, RenderTurnStatus,
};
use novarocks_worker::root_result_channel::{
    RootMetadataReservation, RootProducerExit, RootResultChannel, RootSegmentBuilder,
};

use crate::root_producer_pool::{
    RootProducerJob, RootProducerRegistration, RootProducerTurn, RootProducerWake,
    weak_core_backing_bytes,
};
pub use crate::root_producer_pool::{RootProducerLimits, RootProducerPool};

use crate::root_cow_selection_codec::{CowSelectionEncoder, CowSelectionTotals};
use crate::root_scalar_container_codec::{NativeScalarContainerEncoder, is_container};
use crate::root_scalar_leaf_codec::{NativeScalarLeafEncoder, ScalarSchemaOwner};
use crate::root_statistics_codec::{
    StatisticsArtifactEncoder, StatisticsCodecStatus, StatisticsCodecTotals,
};
use crate::root_write_commit_codec::{WriteCommitEncoder, WriteCommitTotals};

enum InputEncoder {
    Client(Box<ArrowMysqlTextEncoder>),
    Statistics(Box<StatisticsArtifactEncoder>),
    WriteCommit(Box<WriteCommitEncoder>),
    CowSelection(Box<CowSelectionEncoder>),
    ScalarLeaf(Box<NativeScalarLeafEncoder>),
    ScalarContainer(Box<NativeScalarContainerEncoder>),
    /// A schema-validated empty container batch: no record, no row.
    ScalarEmpty,
}
impl InputEncoder {
    fn step(&mut self, output: &mut [u8]) -> Result<RenderTurn, &'static str> {
        match self {
            Self::Client(encoder) => encoder.step(output).map_err(|error| {
                tracing::warn!(
                    ?error,
                    "Root client encoding failed under its frozen schema"
                );
                use novarocks_result_render::RenderErrorKind;
                match error.kind {
                    RenderErrorKind::SchemaMismatch => {
                        "root client encoding violated frozen schema"
                    }
                    RenderErrorKind::UnsupportedCarrier => {
                        "root client encoding encountered an unsupported carrier"
                    }
                    RenderErrorKind::UnsupportedPresentation => {
                        "root client encoding encountered an unsupported presentation"
                    }
                    RenderErrorKind::RowTooLarge => "root client row exceeds its frozen size bound",
                    RenderErrorKind::ElementLimit => {
                        "root client row exceeds its frozen element bound"
                    }
                    RenderErrorKind::DepthLimit => {
                        "root client value exceeds its frozen depth bound"
                    }
                    RenderErrorKind::ArithmeticOverflow => {
                        "root client encoding arithmetic overflow"
                    }
                    RenderErrorKind::Cancelled => "root client encoding was cancelled",
                }
            }),
            Self::WriteCommit(encoder) => encoder
                .step(output)
                .map_err(|_| "root encoding failed under its frozen domain"),
            Self::CowSelection(encoder) => Ok(encoder.step(output)),
            Self::ScalarLeaf(encoder) => encoder
                .step(output)
                .map_err(|_| "root encoding failed under its frozen domain"),
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
                .map_err(|_| "root encoding failed under its frozen domain"),
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

// Destruction order is deliberate: original/cursor Arrow owners are gone
// before input overlap credit can wake another driver. Scratch follows cursor.
struct Input {
    encoder: Option<InputEncoder>,
    chunk: Chunk,
    permit: RootInputPermit,
    scratch: Option<ResultWriteCredit>,
}
struct ProducerState {
    input: Option<Input>,
    builder: Option<RootSegmentBuilder>,
    used: usize,
    statistics_totals: StatisticsCodecTotals,
    /// PreparedWriteCommitV1 records completed across every input batch.
    write_totals: WriteCommitTotals,
    /// CowSelectionArrowV1 schema and batch records completed so far.
    cow_totals: CowSelectionTotals,
    /// ScalarValueV1 rows accepted across every input batch: 0 or 1. Its
    /// record stays in the unpublished segment until the sealed normal End.
    scalar_rows: u8,
    sealed: bool,
    /// The producer published its normal End. Its terminal is final: a later
    /// cancel (for example a pool shutdown racing the job's removal) cannot
    /// turn a completed producer into a failed one.
    completed: bool,
    // Rollback is not a runtime result terminal. Observation follows actual exit.
    abort_is_rollback: bool,
    terminal_observed: bool,
    failed: Option<&'static str>,
    producer: Option<RootProducerExit>,
    cleanup_panic: Option<Box<dyn std::any::Any + Send>>,
}
impl ProducerState {
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

    fn cleanup(&mut self, action: impl FnOnce()) {
        if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(action))
            && self.cleanup_panic.is_none()
        {
            self.cleanup_panic = Some(payload);
        }
    }
}
// A notification snapshot can outlive the session subscription. Its guard
// follows the real callback and its Weak control tails through that exit.
struct WakeBridge {
    wake: RootProducerWake,
    driver: Weak<Observable>,
    _metadata: Arc<RootMetadataReservation>,
}
impl WakeBridge {
    fn notify(&self) {
        let _ = self.wake.wake_active();
        if let Some(observable) = self.driver.upgrade() {
            observable.notify_observers();
        }
    }
}

/// Driver calls transfer one original input. Rendering runs only on the shared
/// fixed CPU pool; neither a full queue nor a pending ACK occupies that pool.
/// The composition retains the pool and explicitly shuts it down before its
/// driver executor. Runtime Task owners retain the channel, not this session.
pub struct NativeRootResultSession {
    channel: Arc<RootResultChannel>,
    authority: RootInputAuthority,
    state: Mutex<ProducerState>,
    registration: OnceLock<RootProducerRegistration>,
    wake_subscription: OnceLock<ObserverSubscription>,
    exited: AtomicBool,
    #[cfg(test)]
    terminal_observations: Mutex<Vec<&'static str>>,
    _metadata: Arc<RootMetadataReservation>,
}
impl NativeRootResultSession {
    pub fn try_open(
        channel: Arc<RootResultChannel>,
        pool: &Arc<RootProducerPool>,
    ) -> Result<Arc<Self>, FragmentIoError> {
        let contract = &channel.spec().contract;
        // Every closed internal domain has its producer codec; only a scalar
        // domain identity without its typed schema has none.
        if contract.validate_purpose().is_err() {
            return Err(io_error("explicit internal root codec is not installed"));
        }
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
            + novarocks_execution::exec::operators::RootResultSinkFactory::metadata_capacity_bytes(
                NativeResultSupportGeometry::V1.root_maximum_root_drivers as usize,
            )
            .map_err(|_| io_error("root driver metadata exceeds its frozen profile"))?
            + RootInputAuthority::initial_backing_bytes()
            + arc::<WakeBridge>()
            + arc::<Arc<dyn Fn() + Send + Sync>>()
            + 2 * std::mem::size_of::<std::sync::Weak<Arc<dyn Fn() + Send + Sync>>>()
            + arc::<RootMetadataReservation>()
            + weak_core_backing_bytes();
        #[cfg(target_vendor = "apple")]
        let metadata_bytes = metadata_bytes + Layout::new::<(isize, [u8; 56])>().size();
        let metadata = channel
            .try_reserve_metadata(metadata_bytes)
            .map_err(|_| io_error("root session fixed metadata exceeds its envelope"))?;
        let metadata = Arc::new(metadata);
        let authority = RootInputAuthority::try_new_retained(
            channel.spec(),
            Arc::clone(&metadata) as Arc<dyn std::any::Any + Send + Sync>,
        )?;
        let session = Arc::new(Self {
            channel,
            authority,
            state: Mutex::new(ProducerState {
                input: None,
                builder: None,
                used: 0,
                statistics_totals: StatisticsCodecTotals::default(),
                write_totals: WriteCommitTotals::default(),
                cow_totals: CowSelectionTotals::default(),
                scalar_rows: 0,
                sealed: false,
                completed: false,
                abort_is_rollback: false,
                terminal_observed: false,
                failed: None,
                producer: None,
                cleanup_panic: None,
            }),
            registration: OnceLock::new(),
            wake_subscription: OnceLock::new(),
            exited: AtomicBool::new(true),
            #[cfg(test)]
            terminal_observations: Mutex::new(Vec::new()),
            _metadata: metadata,
        });
        drop(
            session
                .state
                .lock()
                .expect("root session initial state lock"),
        );
        let job = Arc::clone(&session) as Arc<dyn RootProducerJob>;
        let registration = pool
            .register(Arc::downgrade(&job))
            .map_err(|_| io_error("root CPU pool has no installation position"))?;
        drop(job);
        let wake = registration.wake_handle();
        session
            .registration
            .set(registration)
            .map_err(|_| io_error("root producer registered twice"))?;
        let bridge = WakeBridge {
            wake,
            driver: Arc::downgrade(&session.authority.observable()),
            _metadata: Arc::clone(&session._metadata),
        };
        let subscription = session
            .channel
            .writable_observable()
            .try_subscribe_retained(
                Arc::new(move || {
                    // No session/job mutex is taken here. Readiness callbacks can run
                    // synchronously while a producer destroys input or returns credit.
                    bridge.notify();
                }),
                Arc::clone(&session._metadata) as Arc<dyn std::any::Any + Send + Sync>,
            )
            .map_err(|_| io_error("root readiness has no producer bridge position"))?;
        session
            .wake_subscription
            .set(subscription)
            .map_err(|_| io_error("root readiness registered twice"))?;
        Ok(session)
    }
    pub fn channel(&self) -> &Arc<RootResultChannel> {
        &self.channel
    }

    fn observe_terminal(&self, terminal: &'static str) {
        crate::backend_metrics::record_fragment_result_terminal(terminal);
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
                state.failed.get_or_insert("root producer turn panicked");
                state.sealed = true;
                // The following real pool turn still destroys input/cursor and
                // calls exited. Recovery never equates a panic with reclamation.
                self.state.clear_poison();
                state
            }
        }
    }

    fn wake(&self) -> Result<(), FragmentIoError> {
        self.registration
            .get()
            .expect("root is registered")
            .wake()
            .map_err(|_| io_error("root CPU pool is closed"))
    }
    fn start(&self, state: &mut ProducerState) -> Result<(), FragmentIoError> {
        if state.producer.is_none() && self.exited.load(Ordering::Acquire) {
            state.producer = Some(
                self.channel
                    .start_producer()
                    .map_err(|_| io_error("root producer cannot start"))?,
            );
            self.exited.store(false, Ordering::Release);
        }
        Ok(())
    }
    fn fail(&self, state: &mut ProducerState, message: &'static str) -> RootProducerTurn {
        state.failed.get_or_insert(message);
        state.sealed = true;
        // Observer panics cannot turn a finite cleanup into repeated turns
        // that never destroy input. Each independent release/notification is
        // attempted once, and the first unwind is resumed by exited() only
        // after actual input, segment and producer ownership have ended.
        state.cleanup(|| self.authority.close());
        state.cleanup(|| self.channel.close(RootRetentionClose::ContextAborted));
        // Original owners, cursor and scratch are destroyed before exited().
        let input = state.input.take();
        state.cleanup(|| drop(input));
        let builder = state.builder.take();
        state.cleanup(|| drop(builder));
        state.used = 0;
        RootProducerTurn::Complete
    }
    fn advance(&self, state: &mut ProducerState) -> RootProducerTurn {
        if state.failed.is_some() || self.channel.is_closed() {
            return self.fail(state, "root producer was cancelled");
        }
        if let Some(input) = state.input.as_mut() {
            if self.spec().contract.kind() == RootOutputKind::CountOnly {
                let Ok(rows) = u64::try_from(input.chunk.len()) else {
                    return self.fail(state, "root row count exceeds u64");
                };
                if self.channel.note_rows(rows).is_err() {
                    return self.fail(state, "root row count overflow");
                }
                drop(state.input.take());
                return RootProducerTurn::Yielded;
            }
            if input.encoder.is_none() {
                let capacity = NativeResultSupportGeometry::V1.root_scratch_capacity_bytes as usize;
                let credit = match self.channel.try_reserve(capacity) {
                    Ok(ResultWriteAdmission::Granted(credit)) => credit,
                    Ok(ResultWriteAdmission::Blocked) => return RootProducerTurn::Blocked,
                    Err(_) => return self.fail(state, "root scratch reservation was rejected"),
                };
                input.scratch = Some(credit);
                // This clone only allocates the finite columns Vec. Original
                // Chunk accounting/source capabilities remain live in input.
                let cloning =
                    input.chunk.batch.num_columns() * std::mem::size_of::<arrow::array::ArrayRef>();
                let encoder = match self.begin_encoder(
                    input,
                    capacity,
                    cloning,
                    &mut state.scalar_rows,
                    state.statistics_totals,
                    state.write_totals,
                    &state.cow_totals,
                ) {
                    Ok(encoder) => encoder,
                    Err(message) => return self.fail(state, message),
                };
                input.encoder = Some(encoder);
                return RootProducerTurn::Yielded;
            }
            if state.builder.is_none() {
                match self.channel.try_segment() {
                    Ok(Some(builder)) => {
                        state.builder = Some(builder);
                        state.used = 0;
                    }
                    Ok(None) => return RootProducerTurn::Blocked,
                    Err(_) => return self.fail(state, "root segment reservation was rejected"),
                }
            }
            let turn = match input
                .encoder
                .as_mut()
                .unwrap()
                .step(state.builder.as_mut().unwrap().output_at(state.used))
            {
                Ok(turn) => turn,
                Err(message) => return self.fail(state, message),
            };
            state.used += turn.emitted_bytes;
            if self.channel.note_rows(turn.completed_rows).is_err() {
                return self.fail(state, "root row count overflow");
            }
            let input_complete = turn.status == RenderTurnStatus::InputComplete;
            if self.scalar() {
                // The whole record fits one segment and stays unpublished:
                // only the sealed normal End publishes it (or NoRows).
                if turn.status == RenderTurnStatus::NeedsOutput {
                    return self.fail(state, "scalar record exceeds one root segment");
                }
                if input_complete {
                    drop(state.input.take());
                }
                return RootProducerTurn::Yielded;
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
                drop(state.input.take());
            }
            let finishing = input_complete && state.sealed;
            if finishing && let Err(message) = self.sealed_domain_complete(state) {
                return self.fail(state, message);
            }
            if finishing && self.channel.request_finish().is_err() {
                return self.fail(state, "root finish request was rejected");
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
                } else if self
                    .channel
                    .publish_segment(builder, bytes, finishing)
                    .is_err()
                {
                    return self.fail(state, "root data publication failed");
                }
                if finishing {
                    if bytes == 0 && self.channel.publish_end().is_err() {
                        return self.fail(state, "root End publication failed");
                    }
                    return RootProducerTurn::Complete;
                }
            }
            return RootProducerTurn::Yielded;
        }
        if state.sealed {
            if self.scalar() {
                return self.finish_scalar(state);
            }
            if let Err(message) = self.sealed_domain_complete(state) {
                return self.fail(state, message);
            }
            if self
                .channel
                .request_finish()
                .and_then(|_| self.channel.publish_end())
                .is_err()
            {
                return self.fail(state, "root End publication failed");
            }
            return RootProducerTurn::Complete;
        }
        RootProducerTurn::Idle
    }
    /// One cursor for one original input, chosen by the frozen root purpose.
    /// Every cursor's storage is prepaid by `capacity` before it is created.
    #[allow(clippy::too_many_arguments)]
    fn begin_encoder(
        &self,
        input: &Input,
        capacity: usize,
        cloning: usize,
        scalar_rows: &mut u8,
        statistics_totals: StatisticsCodecTotals,
        write_totals: WriteCommitTotals,
        cow_totals: &CowSelectionTotals,
    ) -> Result<InputEncoder, &'static str> {
        let fits = |inline: usize| inline.checked_add(cloning).is_some_and(|n| n <= capacity);
        match self.spec().contract.output() {
            FrozenRootOutput::InternalFacts(InternalResultDomain::StatisticsArtifactV1) => {
                // Fixed cursor storage plus the finite columns Vec clone.
                if !fits(StatisticsArtifactEncoder::inline_capacity_bytes()) {
                    return Err("statistics cursor scratch exceeds its pregrant");
                }
                StatisticsArtifactEncoder::try_new(input.chunk.batch.clone(), statistics_totals)
                    .map(|encoder| InputEncoder::Statistics(Box::new(encoder)))
                    .map_err(|_| "statistics input differs from its frozen domain")
            }
            FrozenRootOutput::InternalFacts(InternalResultDomain::PreparedWriteCommitV1) => {
                if !fits(WriteCommitEncoder::inline_capacity_bytes()) {
                    return Err("write commit cursor scratch exceeds its pregrant");
                }
                WriteCommitEncoder::try_new(input.chunk.batch.clone(), write_totals)
                    .map(|encoder| InputEncoder::WriteCommit(Box::new(encoder)))
                    .map_err(|_| "write commit input differs from its fixed contract")
            }
            FrozenRootOutput::InternalFacts(InternalResultDomain::CowSelectionArrowV1) => {
                // Node/buffer tables and the record prefix are sized exactly
                // from this input before anything is allocated.
                CowSelectionEncoder::try_new(&input.chunk.batch, cow_totals.clone(), capacity)
                    .map(|encoder| InputEncoder::CowSelection(Box::new(encoder)))
                    .map_err(|_| "COW selection input differs from its domain codec")
            }
            FrozenRootOutput::ScalarValue(schema) => {
                // Cumulative cardinality is decided before any cursor: a
                // second row is refused before it can reach a segment.
                let rows = input.chunk.len();
                if rows > 1 || (rows == 1 && *scalar_rows != 0) {
                    return Err("scalar root input has more than one row");
                }
                let encoder = if is_container(schema.field()) {
                    if rows == 0 {
                        NativeScalarContainerEncoder::validate_empty(&input.chunk, schema)
                            .map(|()| InputEncoder::ScalarEmpty)
                    } else {
                        NativeScalarContainerEncoder::try_encode(&input.chunk, schema, capacity)
                            .map(|encoder| InputEncoder::ScalarContainer(Box::new(encoder)))
                    }
                } else {
                    ScalarSchemaOwner::try_from_contract(Arc::clone(&self.spec().contract))
                        .and_then(|owner| {
                            if rows == 0 {
                                NativeScalarLeafEncoder::try_validate_empty(
                                    &input.chunk,
                                    owner,
                                    capacity,
                                )
                            } else {
                                NativeScalarLeafEncoder::try_begin(&input.chunk, owner, capacity)
                            }
                        })
                        .map(|encoder| InputEncoder::ScalarLeaf(Box::new(encoder)))
                }
                .map_err(|error| {
                    tracing::warn!(?error, "Scalar root encoding refused its input");
                    match error {
                        crate::root_scalar_leaf_codec::NativeScalarLeafError::Leaf(
                            novarocks_result_contract::ScalarLeafError::ValueLimit,
                        ) => "scalar value exceeds frozen 64 KiB record bound",
                        _ => "scalar input differs from its frozen root value",
                    }
                })?;
                if rows == 1 {
                    *scalar_rows = 1;
                }
                Ok(encoder)
            }
            FrozenRootOutput::ClientRows(_) => {
                let encoder = ArrowMysqlTextEncoder::try_new_root(
                    Arc::clone(&self.spec().contract),
                    input.chunk.batch.clone(),
                )
                .map_err(|_| "root input cannot be rendered under its frozen schema")?;
                if !encoder
                    .scratch_capacity_bytes()
                    .checked_add(size_of::<ArrowMysqlTextEncoder>())
                    .is_some_and(fits)
                {
                    return Err("root renderer scratch exceeds its pregrant");
                }
                Ok(InputEncoder::Client(Box::new(encoder)))
            }
            // Session opening refuses an untyped scalar identity, and
            // CountOnly counts rows without a cursor.
            FrozenRootOutput::InternalFacts(InternalResultDomain::ScalarValueV1)
            | FrozenRootOutput::CountOnly => Err("root purpose has no input cursor"),
        }
    }
    /// Domain facts a normal sealed End must already hold. A write root
    /// without its SUMMARY fails instead of publishing a success End.
    fn sealed_domain_complete(&self, state: &ProducerState) -> Result<(), &'static str> {
        if self.spec().contract.kind()
            == RootOutputKind::InternalFacts(InternalResultDomain::PreparedWriteCommitV1)
        {
            state
                .write_totals
                .finish()
                .map_err(|_| "write commit stream ended without its SUMMARY")?;
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
    fn finish_scalar(&self, state: &mut ProducerState) -> RootProducerTurn {
        let FrozenRootOutput::ScalarValue(schema) = self.spec().contract.output() else {
            unreachable!("scalar finish is reached only for a scalar root");
        };
        if state.builder.is_none() {
            match self.channel.try_segment() {
                Ok(Some(builder)) => {
                    state.builder = Some(builder);
                    state.used = 0;
                }
                Ok(None) => return RootProducerTurn::Blocked,
                Err(_) => return self.fail(state, "root segment reservation was rejected"),
            }
        }
        if state.scalar_rows == 0 {
            if state.used != 0 {
                return self.fail(state, "scalar root has bytes without an accepted row");
            }
            let output = state.builder.as_mut().unwrap().output_at(0);
            let written =
                ScalarLeafCursor::try_new(schema, BorrowedScalarLeaf::NoRows).and_then(|cursor| {
                    let turn = cursor.copy_range(0, output)?;
                    Ok((turn.complete, turn.emitted_bytes))
                });
            match written {
                Ok((true, bytes)) => state.used = bytes,
                _ => return self.fail(state, "scalar NoRows record cannot be encoded"),
            }
        } else if state.used == 0 {
            return self.fail(state, "scalar root accepted a row without its record");
        }
        if self.channel.request_finish().is_err() {
            return self.fail(state, "root finish request was rejected");
        }
        let builder = state.builder.take().unwrap();
        let bytes = std::mem::take(&mut state.used);
        if self.channel.publish_segment(builder, bytes, true).is_err() {
            return self.fail(state, "root data publication failed");
        }
        RootProducerTurn::Complete
    }
}
impl RootResultSession for NativeRootResultSession {
    fn spec(&self) -> &RootResultWriteSpec {
        self.channel.spec()
    }
    fn writable_observable(&self) -> Arc<Observable> {
        self.authority.observable()
    }
    fn try_acquire_input(&self) -> Result<RootInputAdmission, FragmentIoError> {
        if self.channel.is_closed() || self.state().sealed {
            return Err(io_error("root input is closed"));
        }
        if !self.authority.is_available() {
            return Ok(RootInputAdmission::Blocked);
        }
        match self.channel.try_reserve(self.authority.required_bytes()) {
            Ok(ResultWriteAdmission::Granted(credit)) => self.authority.try_acquire(credit),
            Ok(ResultWriteAdmission::Blocked) => Ok(RootInputAdmission::Blocked),
            Err(_) => Err(io_error("root input reservation was rejected")),
        }
    }
    fn submit_input(&self, chunk: Chunk, permit: RootInputPermit) -> Result<(), FragmentIoError> {
        let input = Input {
            encoder: None,
            chunk,
            permit,
            scratch: None,
        };
        if !self.authority.owns(&input.permit) {
            drop(input);
            return Err(io_error(
                "root input permit belongs to a different issuer or generation",
            ));
        }
        // Bounded structural inspection only: no render/hydrate/row counting
        // in the driver. An oversized/unknown original cannot enter the queue.
        let cap = if self.spec().contract.kind()
            == RootOutputKind::InternalFacts(InternalResultDomain::StatisticsArtifactV1)
        {
            novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES
        } else {
            NativeResultSupportGeometry::V1.root_original_input_backing_capacity_bytes as usize
        };
        if let Err(error) = borrowed_root_chunk_storage(
            &input.chunk,
            RootArrayStorageLimits {
                bytes: cap,
                nodes: 2 * RootProfileV1::SCHEMA_TYPE_NODES,
                depth: RootProfileV1::MAX_DEPTH,
            },
        ) {
            let schema = input.chunk.chunk_schema();
            let unknown_metadata = if schema.field_metadata_origins().is_none() {
                "root input field metadata origins are missing"
            } else if schema.schema_metadata_origin().is_none() {
                "root input schema metadata origin is missing"
            } else if schema
                .schema_metadata_origin()
                .unwrap()
                .backing_bytes_for(&input.chunk.batch.schema())
                .is_none()
            {
                "root input actual schema differs from its metadata owner"
            } else {
                "root input nested metadata ownership is unknown"
            };
            drop(input);
            return Err(io_error(match error {
                novarocks_execution::exec::chunk::RootArrayStorageError::UnsupportedCarrier => {
                    "root input inspection refused an unsupported carrier"
                }
                novarocks_execution::exec::chunk::RootArrayStorageError::UnknownMetadataOwner => {
                    unknown_metadata
                }
                novarocks_execution::exec::chunk::RootArrayStorageError::CapacityExceeded => {
                    "root input backing exceeds its frozen capacity"
                }
                novarocks_execution::exec::chunk::RootArrayStorageError::WorkExceeded => {
                    "root input structural work exceeds its frozen profile"
                }
            }));
        }
        let mut state = self.state();
        if state.sealed
            || state.failed.is_some()
            || state.input.is_some()
            || self.channel.is_closed()
        {
            drop(state);
            drop(input);
            return Err(io_error("root input position is not accepting"));
        }
        if let Err(error) = self.start(&mut state) {
            drop(state);
            drop(input);
            return Err(error);
        }
        state.input = Some(input);
        drop(state);
        self.wake()
    }
    fn finish_input(&self) -> Result<(), FragmentIoError> {
        let mut state = self.state();
        if state.failed.is_some() || self.channel.is_closed() {
            return Err(io_error("root producer is closed"));
        }
        self.start(&mut state)?;
        state.sealed = true;
        state.cleanup(|| self.authority.close());
        let notification_failed = state.cleanup_panic.is_some();
        if notification_failed {
            state
                .failed
                .get_or_insert("root readiness notification panicked");
        }
        drop(state);
        self.wake()?;
        if notification_failed {
            return Err(io_error("root readiness notification panicked"));
        }
        Ok(())
    }
    fn producer_state(&self) -> RootProducerState {
        let state = self.state();
        if let Some(message) = state.failed {
            return RootProducerState::Failed(message.into());
        }
        if self.channel.is_closed() {
            return RootProducerState::Failed("root channel is closed".into());
        }
        let published = self.channel.producer_state();
        if state.sealed && published == RootProducerState::Accepting {
            RootProducerState::Finishing
        } else {
            published
        }
    }
    fn producer_exited(&self) -> bool {
        self.exited.load(Ordering::Acquire)
    }
    fn abort(&self, reason: ResultAbort) {
        let mut state = self.state();
        // End is committed before observer wakeups. A wakeup panic cannot
        // replace that immutable publication with a later cancellation.
        state.completed |= self.channel.snapshot().end_sequence.is_some();
        if state.completed {
            return;
        }
        if state.failed.is_none() {
            state.abort_is_rollback = matches!(
                reason,
                ResultAbort::PrepareRollback | ResultAbort::NeverStarted
            );
        }
        state.failed.get_or_insert("root producer was cancelled");
        state.sealed = true;
        state.cleanup(|| self.authority.close());
        state.cleanup(|| self.channel.close(RootRetentionClose::ContextAborted));
        drop(state);
        let _ = self.wake();
    }
}
impl RootProducerJob for NativeRootResultSession {
    fn turn(&self) -> RootProducerTurn {
        let mut state = self.state();
        state.completed |= self.channel.snapshot().end_sequence.is_some();
        if state.completed {
            assert!(
                state.input.is_none() && state.builder.is_none(),
                "published End must have released its original input and builder"
            );
            return RootProducerTurn::Complete;
        }
        let turn = self.advance(&mut state);
        if turn == RootProducerTurn::Complete && state.failed.is_none() {
            state.completed = true;
        }
        turn
    }
    fn cancel(&self) {
        self.abort(ResultAbort::Cancelled(String::new()));
    }
    fn exited(&self) {
        let (producer, cleanup_panic, terminal) = {
            let mut state = self.state();
            assert!(
                state.input.is_none() && state.builder.is_none(),
                "producer's real backing exits before its guard"
            );
            (
                state.producer.take(),
                state.cleanup_panic.take(),
                state.take_terminal_observation(),
            )
        };
        // Input/cursor/builder destruction and the actual pool turn have
        // completed. Publish this physical fact before guard notification:
        // observers of ContextHeld must already see the producer's exit, and
        // a synchronous callback unwind cannot skip it. Fixed control backing
        // remains covered by the metadata lease until its actual owner drops.
        self.exited.store(true, Ordering::Release);
        let producer_exit =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(producer)));
        let terminal_exit = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if let Some(terminal) = terminal {
                self.observe_terminal(terminal);
            }
        }));
        let readiness_exit = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.authority.observable().notify_observers();
        }));
        // A channel observer cannot strand the independent driver waiters
        // after the pool has removed this completed job. Preserve the first
        // unwind only after both finite notification domains have advanced.
        if let Some(panic) = cleanup_panic {
            std::panic::resume_unwind(panic);
        }
        if let Err(panic) = producer_exit {
            std::panic::resume_unwind(panic);
        }
        if let Err(panic) = terminal_exit {
            std::panic::resume_unwind(panic);
        }
        if let Err(panic) = readiness_exit {
            std::panic::resume_unwind(panic);
        }
    }
}
fn io_error(message: &'static str) -> FragmentIoError {
    FragmentIoError::new(
        FragmentIoOperation::ResultWrite,
        FragmentIoErrorKind::Unavailable,
        message,
    )
}

#[cfg(test)]
mod terminal_observation_tests {
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
        fn exited(&self) {}
    }

    struct Fixture {
        session: Arc<NativeRootResultSession>,
        pool: Arc<RootProducerPool>,
        blocker: Option<(Arc<GatedTurn>, RootProducerRegistration)>,
    }
    impl Fixture {
        fn new(gated: bool) -> Self {
            let limits =
                WorkerResultRetainedLimits::try_new(256 * 1024 * 1024, 1024 * 1024 * 1024).unwrap();
            let budget = ResultRetainedBudget::new(limits.per_process());
            let pool = RootProducerPool::try_new(
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(2).unwrap(),
                1024 * 1024,
                Arc::clone(&budget),
            )
            .unwrap();
            let blocker = gated.then(|| {
                let job = Arc::new(GatedTurn {
                    state: Mutex::new((false, false)),
                    changed: Condvar::new(),
                });
                let erased = Arc::clone(&job) as Arc<dyn RootProducerJob>;
                let registration = pool.register(Arc::downgrade(&erased)).unwrap();
                registration.wake().unwrap();
                job.wait_entered();
                (job, registration)
            });
            let task = TaskIdentity::new(
                QueryExecutionId::new(QueryId::new(907, 908), AttemptId::new(1).unwrap()).unwrap(),
                StageId::new(1).unwrap(),
                TaskId::new(1).unwrap(),
                BackendProcessId::new_v7(),
            );
            let channel = RootResultChannel::try_open(
                RootResultWriteSpec {
                    task,
                    contract: Arc::new(RootOutputContract::new(
                        RootProfileId::V1,
                        FrozenRootOutput::CountOnly,
                    )),
                },
                budget,
                limits,
            )
            .unwrap();
            let session = NativeRootResultSession::try_open(channel, &pool).unwrap();
            Self {
                session,
                pool,
                blocker,
            }
        }
        fn observations(&self) -> Vec<&'static str> {
            self.session.terminal_observations.lock().unwrap().clone()
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
            self.session.submit_input(chunk, permit).unwrap();
        }
        fn release(&self) {
            if let Some((job, _registration)) = &self.blocker {
                job.release();
            }
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            self.release();
            let _ = self.pool.shutdown();
        }
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
        assert!(fixture.session.producer_exited());
        fixture
            .session
            .abort(ResultAbort::Cancelled("late cancellation".into()));
        fixture
            .session
            .abort(ResultAbort::Failed("late failure".into()));
        RootProducerJob::exited(fixture.session.as_ref());
        RootProducerJob::exited(fixture.session.as_ref());
        assert_eq!(fixture.observations(), ["finished"]);
    }

    #[test]
    fn native_root_published_end_wakeup_panic_preserves_finished_after_actual_exit() {
        let fixture = Fixture::new(false);
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
        fixture.pool.shutdown().unwrap();
        assert!(panicked.load(Ordering::Acquire));
        assert!(fixture.session.producer_exited());
        let snapshot = fixture
            .session
            .channel()
            .try_ownership_snapshot()
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.ends_published, 1);
        assert_eq!(snapshot.producers_running, 0);
        assert_eq!(snapshot.producers_exited, 1);
        assert!(!fixture.session.channel().is_closed());
        let state = fixture.session.state();
        assert!(state.completed);
        assert_eq!(state.failed, Some("root producer turn panicked"));
        assert!(state.input.is_none());
        assert!(state.builder.is_none());
        assert!(state.producer.is_none());
        drop(state);
        fixture
            .session
            .abort(ResultAbort::Cancelled("late cancellation".into()));
        RootProducerJob::exited(fixture.session.as_ref());
        assert_eq!(fixture.observations(), ["finished"]);
        drop(subscription);
    }

    #[test]
    fn native_root_cold_installed_runtime_cancel_observes_exactly_one_abort() {
        let fixture = Fixture::new(false);
        assert!(fixture.session.state().producer.is_none());
        assert!(fixture.session.producer_exited());
        fixture
            .session
            .abort(ResultAbort::Cancelled("cold root deadline".into()));
        fixture.wait_terminal("aborted");
        fixture.pool.shutdown().unwrap();
        fixture
            .session
            .abort(ResultAbort::Cancelled("repeated cancellation".into()));
        RootProducerJob::exited(fixture.session.as_ref());
        RootProducerJob::exited(fixture.session.as_ref());
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
            assert!(!fixture.session.producer_exited());
            fixture.session.abort(reason.clone());
            fixture.session.abort(reason);
            // The only pool worker is already inside the gate. This assertion
            // cannot pass merely because the canceled worker has not scheduled.
            assert!(fixture.session.state().input.is_some());
            assert!(fixture.observations().is_empty());
            assert!(!fixture.session.producer_exited());
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
            RootProducerJob::exited(fixture.session.as_ref());
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
            assert!(fixture.session.producer_exited());
            RootProducerJob::exited(fixture.session.as_ref());
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
}
