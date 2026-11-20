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

//! A typed scan as the one stream its driver polls.
//!
//! The scan's split queue, its speculative successor window and every
//! split's page stream are advanced from the driver's polls: a split is
//! claimed from the queue, its provider page stream is polled until it ends,
//! and its close is observed before the next split opens. Nothing here
//! blocks. An empty queue, a page stream waiting for I/O and a spent turn
//! budget all return `Pending` with the driver's waker registered, and every
//! poll and close runs inside the scan I/O runtime's context.
//!
//! Closing the stream stops the successor window, closes the queue, and
//! seals the scan's task source, so no operation of the scan starts after
//! it; the returned future only observes the exit of what already runs.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use futures::Stream;
use futures::future::BoxFuture;
use futures::task::AtomicWaker;
use novarocks_execution::connector::{
    ScheduledSplitFacts, SourcePageConverter, SplitPoll, SplitQueue,
};
use novarocks_execution::exec::chunk::Chunk;
use novarocks_execution::exec::node::scan::{
    ScanChunkStream, ScanExitBodyProof, ScanExitObservation, ScanOutputStream, ScanStreamSource,
};
use novarocks_execution::runtime::profile::{ProfileUnit, RuntimeProfile};
use novarocks_execution::runtime::task::ExecutionResult;
use novarocks_execution::runtime::task::connector_failure::{
    ConnectorCallFailure, ConnectorOperation,
};
use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::read_stack::{
    BudgetConsume, ConnectorPollBudget, ConnectorPreparationStart, ConnectorPreparedPageSource,
    ConnectorReadPageSourceProvider, ConnectorSourceOperations, OwnedConnectorPageStream,
    PageSourceFileMetrics, SourcePage,
};

use super::TypedConnectorScanShared;
#[cfg(novarocks_dev_controls)]
use super::emit_page_source_marker;
use crate::read_attempt::ReceivedReadSplit;
#[cfg(novarocks_dev_controls)]
use crate::typed_page_source::TypedConnectorReaderMarker;
use crate::typed_page_source::flush_page_source_file_metrics;
use crate::typed_preparation_flow::StreamPreparationFlow;

/// The concrete stream Box is admitted before its constructor runs. The
/// resulting stream keeps this finite scope; page chunks use separate scopes.
fn create_scan_stream<T>(
    schema: &novarocks_execution::exec::chunk::ChunkSchema,
    factory: &dyn novarocks_execution::exec::pipeline::operator::InputConstructionFactory,
    create: impl FnOnce(novarocks_type_contract::ValueTypeStorageLifetime) -> T,
) -> novarocks_execution::runtime::task::ExecutionResult<Pin<Box<T>>> {
    let owner = factory.begin_typed_construction(schema)?;
    owner.request(std::alloc::Layout::new::<T>())?;
    let stream = create(owner.storage_lifetime());
    Ok(Box::pin(stream))
}

/// Runtime streams require the exact Task execution source. Planning-only
/// requests never acquire a runtime reader or construction scope here.
fn required_source_operations(
    request: &novarocks_spi::connector::ConnectorRequestContext,
) -> ExecutionResult<ConnectorSourceOperations> {
    if request.range_scope().is_none() {
        return Err(super::TypedScanBindingFailure::MissingExecutionSource.into());
    }
    request
        .source_operations()
        .cloned()
        .ok_or_else(|| super::TypedScanBindingFailure::MissingExecutionSource.into())
}

/// Hands a typed scan's one stream to the driver that runs it.
pub(super) struct TypedScanStreamSource {
    shared: Arc<TypedConnectorScanShared>,
    queue: Arc<SplitQueue<ReceivedReadSplit>>,
    flow: Arc<StreamPreparationFlow>,
    split_wake: Arc<AtomicWaker>,
    claimed: AtomicBool,
}

impl TypedScanStreamSource {
    pub(super) fn new(
        shared: Arc<TypedConnectorScanShared>,
        queue: Arc<SplitQueue<ReceivedReadSplit>>,
        flow: Arc<StreamPreparationFlow>,
    ) -> Arc<Self> {
        let split_wake = Arc::new(AtomicWaker::new());
        // Weak, so the attempt's queue never keeps this scan alive.
        let woken = Arc::downgrade(&split_wake);
        queue.observable().add_observer(Arc::new(move || {
            if let Some(wake) = woken.upgrade() {
                wake.wake();
            }
        }));
        Arc::new(Self {
            shared,
            queue,
            flow,
            split_wake,
            claimed: AtomicBool::new(false),
        })
    }

    /// Wakes a stream parked on an empty queue, as a terminal stop must.
    pub(super) fn wake(&self) {
        self.split_wake.wake();
    }
}

impl ScanStreamSource for TypedScanStreamSource {
    fn claim(
        &self,
        budget: ConnectorPollBudget,
        profile: Option<RuntimeProfile>,
        construction: std::sync::Arc<
            dyn novarocks_execution::exec::pipeline::operator::InputConstructionFactory,
        >,
    ) -> novarocks_execution::runtime::task::ExecutionResult<ScanOutputStream> {
        if self.claimed.swap(true, Ordering::AcqRel) {
            return Err(super::TypedScanBindingFailure::ReclaimedStream.into());
        }
        let source_operations = required_source_operations(&self.shared.request)?;
        let source_failures = source_operations
            .claim_failure_receiver()
            .map_err(|cause| ConnectorCallFailure::new(ConnectorOperation::SourceExit, cause))?;
        let provider = self.shared.provider.resolve()?;
        let read_schema = self
            .shared
            .output_materialization
            .as_ref()
            .map_or(&self.shared.output_schema, |materialization| {
                materialization.transform.read_schema()
            });
        let converter = SourcePageConverter::with_output_schema(Arc::clone(read_schema));
        let stream = create_scan_stream(read_schema, construction.as_ref(), |stream_storage| {
            TypedConnectorScanStream {
                provider,
                source_operations,
                source_failures: Some(source_failures),
                failure_reporter: None,
                _construction_storage: stream_storage,
                construction: Arc::clone(&construction),
                converter,
                shared: Arc::clone(&self.shared),
                queue: Arc::clone(&self.queue),
                split_wake: Arc::clone(&self.split_wake),
                flow: Arc::clone(&self.flow),
                budget,
                profile,
                step: SplitStep::Idle,
                claims: VecDeque::new(),
                ended: false,
            }
        })?;
        Ok(stream)
    }
}

struct TypedConnectorScanStream {
    source_operations: ConnectorSourceOperations,
    source_failures: Option<novarocks_spi::connector::read_stack::ConnectorSourceFailureReceiver>,
    failure_reporter: Option<Arc<dyn novarocks_execution::runtime::task::ExecutionFailureReporter>>,
    shared: Arc<TypedConnectorScanShared>,
    provider: Arc<dyn ConnectorReadPageSourceProvider>,
    queue: Arc<SplitQueue<ReceivedReadSplit>>,
    split_wake: Arc<AtomicWaker>,
    flow: Arc<StreamPreparationFlow>,
    budget: ConnectorPollBudget,
    profile: Option<RuntimeProfile>,
    converter: SourcePageConverter,
    construction:
        std::sync::Arc<dyn novarocks_execution::exec::pipeline::operator::InputConstructionFactory>,
    // Only the concrete stream Box's creating scope, independent of page chunks.
    _construction_storage: novarocks_type_contract::ValueTypeStorageLifetime,
    step: SplitStep,
    /// Splits taken from the queue ahead of the one being read.
    claims: VecDeque<PreparedClaim>,
    /// Delivery ended: the queue is exhausted or the stream failed.
    ended: bool,
}

enum SplitStep {
    /// Between two splits.
    Idle,
    Reading(Box<OpenSplit>),
    /// A finished split whose exit is observed before the next one opens.
    Closing(ClosingSplit),
    /// Charges the turn for moving to the next split, so a run of splits
    /// that end at once still yields the driver's turn.
    Switching(BudgetConsume),
}

struct OpenSplit {
    stream: OwnedConnectorPageStream,
    #[cfg(novarocks_dev_controls)]
    reader_marker: Option<TypedConnectorReaderMarker>,
    #[cfg(novarocks_dev_controls)]
    first_file_batch_observed: bool,
    #[cfg(novarocks_dev_controls)]
    first_chunk_observed: bool,
    successor_control_id: Option<u64>,
    file_metrics: PageSourceFileMetrics,
}

struct ClosingSplit {
    exit: BoxFuture<'static, Result<(), ConnectorError>>,
    #[cfg(novarocks_dev_controls)]
    reader_marker: Option<TypedConnectorReaderMarker>,
    #[cfg(novarocks_dev_controls)]
    reader_rows_decoded: u64,
    #[cfg(novarocks_dev_controls)]
    reader_batches_delivered: u64,
}

impl ClosingSplit {
    /// Closes the split: its reads are stopped now, and their exit is what
    /// `exit` observes.
    fn of(split: OpenSplit, flow: &StreamPreparationFlow) -> Self {
        if let Some(id) = split.successor_control_id {
            flow.unregister(id);
        }
        Self {
            exit: split.stream.close(),
            #[cfg(novarocks_dev_controls)]
            reader_marker: split.reader_marker,
            #[cfg(novarocks_dev_controls)]
            reader_rows_decoded: split.file_metrics.rows_decoded,
            #[cfg(novarocks_dev_controls)]
            reader_batches_delivered: split.file_metrics.batches_delivered,
        }
    }
}

struct PreparedClaim {
    split: ReceivedReadSplit,
    candidate: ClaimCandidate,
    control_id: Option<u64>,
}

enum ClaimCandidate {
    Unprepared,
    Prepared(Box<dyn ConnectorPreparedPageSource>),
    /// Its error is reported when the split's turn comes.
    Failed(ConnectorCallFailure),
}

enum NextSplit {
    Ready(ReceivedReadSplit),
    Pending,
    Exhausted,
}

impl TypedConnectorScanStream {
    /// Ends delivery on `error`. Whatever is open stays for `close`, which
    /// observes its exit.
    fn fail(
        &mut self,
        error: impl Into<novarocks_execution::runtime::task::ExecutionFailure>,
    ) -> Poll<Option<novarocks_execution::runtime::task::ExecutionResult<Chunk>>> {
        self.ended = true;
        self.flow.stop();
        Poll::Ready(Some(Err(error.into())))
    }

    fn next_split(&self, cx: &mut Context<'_>) -> NextSplit {
        match self.queue.poll() {
            SplitPoll::Ready(split) => NextSplit::Ready(split),
            SplitPoll::Exhausted => NextSplit::Exhausted,
            SplitPoll::Blocked => {
                // Registered before looking again, so a split that arrives
                // between the two looks wakes this stream.
                self.split_wake.register(cx.waker());
                match self.queue.poll() {
                    SplitPoll::Ready(split) => NextSplit::Ready(split),
                    SplitPoll::Blocked => NextSplit::Pending,
                    SplitPoll::Exhausted => NextSplit::Exhausted,
                }
            }
        }
    }

    fn open_split(&mut self, split: ReceivedReadSplit) -> ExecutionResult<()> {
        self.shared.check_liveness(ConnectorOperation::OpenReader)?;
        let stream = self
            .provider
            .create_page_stream(
                &self.shared.session,
                self.shared.descriptor.relation(),
                split.split(),
                split.sequence_id(),
                self.shared.descriptor.assignments(),
                &self.shared.dynamic_filter,
                &self.budget,
            )
            .map_err(|error| {
                ConnectorCallFailure::new(ConnectorOperation::OpenReader, error)
                    .at_sequence(split.sequence_id())
            })?;
        self.install(&split, stream);
        Ok(())
    }

    fn open_claim(&mut self, claim: PreparedClaim) -> ExecutionResult<()> {
        let PreparedClaim {
            split,
            candidate,
            control_id,
        } = claim;
        match candidate {
            ClaimCandidate::Unprepared => self.open_split(split),
            ClaimCandidate::Failed(error) => {
                // Stopped when it failed; it keeps its occupancy until it
                // drained.
                if let Some(id) = control_id {
                    self.flow.retire(id);
                }
                Err(error.into())
            }
            ClaimCandidate::Prepared(prepared) => {
                self.shared
                    .check_liveness(ConnectorOperation::PromoteReader)?;
                let stream = prepared
                    .promote(&self.shared.dynamic_filter, &self.budget)
                    .map_err(|error| {
                        ConnectorCallFailure::new(ConnectorOperation::PromoteReader, error)
                            .at_sequence(split.sequence_id())
                    })?;
                if let Some(id) = control_id {
                    self.flow.retire(id);
                }
                self.install(&split, stream);
                Ok(())
            }
        }
    }

    fn install(&mut self, split: &ReceivedReadSplit, stream: OwnedConnectorPageStream) {
        #[cfg(novarocks_dev_controls)]
        let reader_marker =
            TypedConnectorReaderMarker::for_split(split, self.shared.test_observation.clone());
        #[cfg(novarocks_dev_controls)]
        if let Some(marker) = reader_marker.as_ref() {
            marker.emit(novarocks_failpoint::observation_marker::CONNECTOR_UNIT_READER_OPEN);
        }
        // Acceptance evidence: a distributed run proves a page stream was
        // opened on this backend for this exact scheduled split, which a
        // result-only assertion cannot show.
        #[cfg(novarocks_dev_controls)]
        emit_page_source_marker(
            self.shared.test_observation.as_deref(),
            novarocks_failpoint::observation_marker::CONNECTOR_PAGE_SOURCE_OPEN,
            self.shared.plan_node_id,
            Some(split.sequence_id()),
        );
        if let Some(profile) = self.profile.as_ref() {
            profile.counter_add("TypedConnectorPageSourcesOpened", ProfileUnit::Unit, 1);
        }
        self.step = SplitStep::Reading(Box::new(OpenSplit {
            stream,
            #[cfg(novarocks_dev_controls)]
            reader_marker,
            #[cfg(novarocks_dev_controls)]
            first_file_batch_observed: false,
            #[cfg(novarocks_dev_controls)]
            first_chunk_observed: false,
            successor_control_id: None,
            file_metrics: PageSourceFileMetrics::default(),
        }));
    }

    // Design: ADR-0158 (docs/adr/ADR-0158-bounded-parquet-range-preparation.md)
    fn advance_preparation(&mut self, split: &mut OpenSplit) {
        if !self.flow.may_prepare() {
            return;
        }
        let config = self.shared.stream_host.preparation();
        if split.successor_control_id.is_none()
            && let Some(control) = split.stream.successor_preparation_control()
        {
            split.successor_control_id = Some(self.flow.register(control));
        }
        let current_candidates = split.stream.successor_preparation_candidate_count();
        let used = self
            .claims
            .len()
            .saturating_add(current_candidates)
            .saturating_add(self.flow.retired_count());
        if used < config.max_candidates {
            let _ = split.stream.as_mut().advance_successor_preparation(
                self.remaining_input_bytes(),
                config.max_candidates - used,
            );
        }
        for index in 0..self.claims.len() {
            let remaining = self.remaining_input_bytes();
            let claim = &mut self.claims[index];
            let ClaimCandidate::Prepared(prepared) = &mut claim.candidate else {
                continue;
            };
            if let Err(error) = prepared.advance(remaining) {
                prepared.control().request_stop();
                claim.candidate = ClaimCandidate::Failed(
                    ConnectorCallFailure::new(ConnectorOperation::PrepareReader, error)
                        .at_sequence(claim.split.sequence_id()),
                );
            }
        }
        while self
            .claims
            .len()
            .saturating_add(current_candidates)
            .saturating_add(self.flow.retired_count())
            < config.max_candidates
        {
            let SplitPoll::Ready(next) = self.queue.poll() else {
                break;
            };
            let candidate = match self.provider.prepare_page_source(
                &self.shared.session,
                self.shared.descriptor.relation(),
                next.split(),
                next.sequence_id(),
                self.shared.descriptor.assignments(),
                &self.shared.dynamic_filter,
            ) {
                Ok(ConnectorPreparationStart::Unsupported) => ClaimCandidate::Unprepared,
                Ok(ConnectorPreparationStart::Prepared(prepared)) => {
                    ClaimCandidate::Prepared(prepared)
                }
                Err(error) => ClaimCandidate::Failed(
                    ConnectorCallFailure::new(ConnectorOperation::PrepareReader, error)
                        .at_sequence(next.sequence_id()),
                ),
            };
            let control_id = match &candidate {
                ClaimCandidate::Prepared(prepared) => Some(self.flow.register(prepared.control())),
                ClaimCandidate::Unprepared | ClaimCandidate::Failed(_) => None,
            };
            let unsupported = matches!(candidate, ClaimCandidate::Unprepared);
            self.claims.push_back(PreparedClaim {
                split: next,
                candidate,
                control_id,
            });
            if unsupported {
                break;
            }
        }
    }

    fn remaining_input_bytes(&self) -> u64 {
        u64::try_from(self.shared.stream_host.preparation().input_bytes_per_stream)
            .unwrap_or(u64::MAX)
            .saturating_sub(self.flow.retained_input_bytes())
    }

    fn chunk_of(
        &mut self,
        page: SourcePage,
    ) -> novarocks_execution::runtime::task::ExecutionResult<Chunk> {
        let chunk = self
            .converter
            .convert_with_factory(page, self.construction.as_ref())?;
        self.shared
            .materialize_output(chunk, self.construction.as_ref())
    }
}

impl Stream for TypedConnectorScanStream {
    type Item = novarocks_execution::runtime::task::ExecutionResult<Chunk>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let _context = this.shared.stream_host.runtime().enter();
        loop {
            if this.ended {
                return Poll::Ready(None);
            }
            if let Err(error) = this.shared.check_liveness(ConnectorOperation::ReadPage) {
                return this.fail(error);
            }
            match std::mem::replace(&mut this.step, SplitStep::Idle) {
                SplitStep::Closing(mut closing) => match closing.exit.as_mut().poll(cx) {
                    Poll::Pending => {
                        this.step = SplitStep::Closing(closing);
                        return Poll::Pending;
                    }
                    Poll::Ready(result) => {
                        #[cfg(novarocks_dev_controls)]
                        emit_split_close_markers(
                            closing.reader_marker.as_ref(),
                            closing.reader_rows_decoded,
                            closing.reader_batches_delivered,
                            this.shared.test_observation.as_deref(),
                            this.shared.plan_node_id,
                        );
                        if let Err(error) = result {
                            let cause =
                                ConnectorCallFailure::new(ConnectorOperation::CloseReader, error)
                                    .into();
                            this.failure_reporter
                                .as_ref()
                                .expect("source observer is bound before reading")
                                .report_cleanup(&cause);
                        }
                        if let Some(profile) = this.profile.as_ref() {
                            profile.counter_add("TypedConnectorSplitsRead", ProfileUnit::Unit, 1);
                        }
                        this.step = SplitStep::Switching(this.budget.consume(1));
                    }
                },
                SplitStep::Switching(mut switching) => {
                    if Pin::new(&mut switching).poll(cx).is_pending() {
                        this.step = SplitStep::Switching(switching);
                        return Poll::Pending;
                    }
                }
                SplitStep::Reading(mut split) => {
                    this.advance_preparation(&mut split);
                    let next = split.stream.as_mut().poll_next(cx);
                    flush_page_source_file_metrics(
                        this.profile.as_ref(),
                        &mut split.file_metrics,
                        split.stream.metrics().file,
                    );
                    #[cfg(novarocks_dev_controls)]
                    if !split.first_file_batch_observed
                        && let Some(marker) = &split.reader_marker
                        && let Some(batch) = split.stream.first_file_batch()
                    {
                        marker.emit_first_file_batch(this.shared.plan_node_id, batch);
                        split.first_file_batch_observed = true;
                    }
                    match next {
                        Poll::Pending => {
                            this.step = SplitStep::Reading(split);
                            return Poll::Pending;
                        }
                        Poll::Ready(Some(Ok(page))) => {
                            let output = this.chunk_of(page);
                            #[cfg(novarocks_dev_controls)]
                            if !split.first_chunk_observed
                                && let Some(marker) = &split.reader_marker
                                && let Ok(chunk) = &output
                                && !chunk.is_empty()
                            {
                                marker.emit_first_chunk(this.shared.plan_node_id, chunk);
                                split.first_chunk_observed = true;
                            }
                            this.step = SplitStep::Reading(split);
                            return match output {
                                Ok(chunk) => Poll::Ready(Some(Ok(chunk))),
                                Err(error) => this.fail(error),
                            };
                        }
                        Poll::Ready(Some(Err(error))) => {
                            // Kept open for `close` to observe its exit.
                            this.step = SplitStep::Reading(split);
                            return this.fail(ConnectorCallFailure::new(
                                ConnectorOperation::ReadPage,
                                error,
                            ));
                        }
                        Poll::Ready(None) => {
                            this.step = SplitStep::Closing(ClosingSplit::of(*split, &this.flow));
                        }
                    }
                }
                SplitStep::Idle => {
                    if let Some(claim) = this.claims.pop_front() {
                        if let Err(error) = this.open_claim(claim) {
                            return this.fail(error);
                        }
                        continue;
                    }
                    match this.next_split(cx) {
                        NextSplit::Ready(split) => {
                            if let Err(error) = this.open_split(split) {
                                return this.fail(error);
                            }
                        }
                        NextSplit::Pending => return Poll::Pending,
                        // The only end of stream: drained after the terminal
                        // marker, or closed.
                        NextSplit::Exhausted => {
                            this.ended = true;
                            return Poll::Ready(None);
                        }
                    }
                }
            }
        }
    }
}

/// Observe every real exit while reporting each known failure immediately.
/// Waiting for sibling work never postpones the originating cause.
async fn observe_scan_close(
    reader_exit: impl Future<Output = Result<(), ConnectorError>>,
    source_exit: impl Future<Output = novarocks_spi::connector::read_stack::ConnectorSourceExitProof>,
    failures: Arc<dyn novarocks_execution::runtime::task::ExecutionFailureReporter>,
) -> ExecutionResult<()> {
    let reader = async {
        if let Err(error) = reader_exit.await {
            let cause: novarocks_execution::runtime::task::ExecutionFailure =
                ConnectorCallFailure::new(ConnectorOperation::CloseReader, error).into();
            failures.report_cleanup(&cause);
        }
    };
    let source = async {
        let _proof = source_exit.await;
        // The independent observer owns Required events and the actual proof.
        // This future proves only its own reader/close body completed.
    };
    futures::future::join(reader, source).await;
    Ok(())
}

async fn observe_source_exit(
    mut events: novarocks_spi::connector::read_stack::ConnectorSourceFailureReceiver,
    source: impl Future<Output = novarocks_spi::connector::read_stack::ConnectorSourceExitProof>,
    drained: impl Future<Output = novarocks_spi::connector::read_stack::ConnectorPreparationExit>,
    failures: Arc<dyn novarocks_execution::runtime::task::ExecutionFailureReporter>,
) -> novarocks_execution::exec::node::scan::ScanExitProof {
    use novarocks_spi::connector::read_stack::{
        ConnectorSourceExitProof, ConnectorSourceFailureEvent,
    };
    futures::pin_mut!(source, drained);
    let mut proof = None;
    let mut flow_proof = None;
    let mut events_ended = false;
    futures::future::poll_fn(move |cx| {
        if !events_ended {
            loop {
                match events.poll_event(cx) {
                    Poll::Ready(Some(ConnectorSourceFailureEvent::Required(error))) => {
                        failures.report_failure(
                            ConnectorCallFailure::new(ConnectorOperation::SourceExit, error).into(),
                        );
                    }
                    Poll::Ready(Some(ConnectorSourceFailureEvent::CleanupUnresolved(error))) => {
                        let cause: novarocks_execution::runtime::task::ExecutionFailure =
                            ConnectorCallFailure::new(ConnectorOperation::SourceExit, error).into();
                        failures.report_cleanup(&cause);
                        failures.report_unresolved_exit(&cause);
                    }
                    Poll::Ready(None) => {
                        events_ended = true;
                        break;
                    }
                    Poll::Pending => break,
                }
            }
        }
        if proof.is_none()
            && let Poll::Ready(observed) = source.as_mut().poll(cx)
        {
            proof = Some(observed);
        }
        if flow_proof.is_none()
            && let Poll::Ready(observed) = drained.as_mut().poll(cx)
        {
            flow_proof = Some(observed);
        }
        if events_ended
            && let Some(flow_proof) = &flow_proof
            && let Some(proof) = proof
        {
            if matches!(
                flow_proof,
                novarocks_spi::connector::read_stack::ConnectorPreparationExit::Unresolved(_)
            ) {
                return Poll::Ready(
                    novarocks_execution::exec::node::scan::ScanExitProof::Unresolved,
                );
            }
            return Poll::Ready(match proof {
                ConnectorSourceExitProof::Exited => {
                    novarocks_execution::exec::node::scan::ScanExitProof::Exited
                }
                ConnectorSourceExitProof::Unresolved => {
                    novarocks_execution::exec::node::scan::ScanExitProof::Unresolved
                }
            });
        }
        // An unresolved stop never skips later events or another live scope.
        Poll::Pending
    })
    .await
}

impl ScanChunkStream for TypedConnectorScanStream {
    fn take_exit_observation(
        self: Pin<&mut Self>,
        failures: Arc<dyn novarocks_execution::runtime::task::ExecutionFailureReporter>,
    ) -> ExecutionResult<ScanExitObservation> {
        let this = self.get_mut();
        let events = this.source_failures.take().ok_or_else(|| {
            ConnectorCallFailure::new(ConnectorOperation::SourceExit,
                ConnectorError::from_domain(novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                    novarocks_spi::connector::read_stack::ConnectorSourceFailure::ReceiverAlreadyClaimed))
        })?;
        this.failure_reporter = Some(failures.clone());
        let source = this.source_operations.exited();
        let runtime = this.shared.stream_host.runtime().clone();
        let drained = this.flow.observe_exit();
        Ok(ScanExitObservation {
            body_proof: ScanExitBodyProof::CloseBodyRequired,
            actual_exit: Box::pin(InScanRuntime {
                runtime,
                inner: Box::pin(observe_source_exit(events, source, drained, failures)),
            }),
        })
    }
    fn close(
        self: Pin<Box<Self>>,
        failures: Arc<dyn novarocks_execution::runtime::task::ExecutionFailureReporter>,
    ) -> BoxFuture<'static, novarocks_execution::runtime::task::ExecutionResult<()>> {
        let this = *Pin::into_inner(self);
        let runtime = this.shared.stream_host.runtime().clone();
        let _context = runtime.enter();
        this.flow.stop();
        // Nothing reads this queue after its one stream.
        this.queue.close();
        let split = match this.step {
            SplitStep::Reading(split) => Some(ClosingSplit::of(*split, &this.flow)),
            SplitStep::Closing(closing) => Some(closing),
            SplitStep::Idle | SplitStep::Switching(_) => None,
        };
        // Stopped with the flow, which observes their exit.
        drop(this.claims);
        this.source_operations.seal();
        let task_exit = this.source_operations.exited();
        let drained = this.flow.observe_exit();
        #[cfg(novarocks_dev_controls)]
        let markers = (
            this.shared.test_observation.clone(),
            this.shared.plan_node_id,
        );
        Box::pin(InScanRuntime {
            runtime,
            inner: Box::pin(observe_scan_close(
                async move {
                    if let Some(split) = split {
                        let result = split.exit.await;
                        #[cfg(novarocks_dev_controls)]
                        emit_split_close_markers(
                            split.reader_marker.as_ref(),
                            split.reader_rows_decoded,
                            split.reader_batches_delivered,
                            markers.0.as_deref(),
                            markers.1,
                        );
                        result
                    } else {
                        Ok(())
                    }
                },
                async move {
                    drained.await;
                    task_exit.await
                },
                failures,
            )),
        })
    }
}

#[cfg(novarocks_dev_controls)]
fn emit_split_close_markers(
    reader_marker: Option<&TypedConnectorReaderMarker>,
    reader_rows_decoded: u64,
    reader_batches_delivered: u64,
    observation: Option<&dyn novarocks_execution::runtime::exchange::TestEventObserver>,
    plan_node_id: i32,
) {
    if let Some(marker) = reader_marker {
        marker.emit_close(reader_rows_decoded, reader_batches_delivered);
    }
    #[cfg(novarocks_dev_controls)]
    emit_page_source_marker(
        observation,
        novarocks_failpoint::observation_marker::CONNECTOR_PAGE_SOURCE_CLOSE,
        plan_node_id,
        None,
    );
}

/// Polls `inner` inside the scan I/O runtime's context, where page streams
/// may use its timers and spawn onto it.
struct InScanRuntime<T> {
    runtime: tokio::runtime::Handle,
    inner: BoxFuture<'static, T>,
}

impl<T> Future for InScanRuntime<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let _context = self.runtime.enter();
        self.inner.as_mut().poll(cx)
    }
}

#[cfg(test)]
#[path = "stream/tests.rs"]
mod stream_construction_tests;
