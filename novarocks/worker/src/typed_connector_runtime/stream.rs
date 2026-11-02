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
use novarocks_execution::runtime::fragment::ExecutionResult;
use novarocks_execution::runtime::fragment::connector_failure::{
    ConnectorCallFailure, ConnectorOperation,
};
use novarocks_execution::runtime::profile::{ProfileUnit, RuntimeProfile};
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
) -> novarocks_execution::runtime::fragment::ExecutionResult<Pin<Box<T>>> {
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
    ) -> novarocks_execution::runtime::fragment::ExecutionResult<ScanOutputStream> {
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
    failure_reporter:
        Option<Arc<dyn novarocks_execution::runtime::fragment::ExecutionFailureReporter>>,
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
        error: impl Into<novarocks_execution::runtime::fragment::ExecutionFailure>,
    ) -> Poll<Option<novarocks_execution::runtime::fragment::ExecutionResult<Chunk>>> {
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
                self.shared.descriptor.table(),
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
            marker.emit("OPEN");
        }
        // Acceptance evidence: a distributed run proves a page stream was
        // opened on this backend for this exact scheduled split, which a
        // result-only assertion cannot show.
        #[cfg(novarocks_dev_controls)]
        emit_page_source_marker(
            self.shared.test_observation.as_deref(),
            "NOVAROCKS_CONNECTOR_PAGE_SOURCE_OPEN",
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
                self.shared.descriptor.table(),
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
    ) -> novarocks_execution::runtime::fragment::ExecutionResult<Chunk> {
        let chunk = self
            .converter
            .convert_with_factory(page, self.construction.as_ref())?;
        self.shared
            .materialize_output(chunk, self.construction.as_ref())
    }
}

impl Stream for TypedConnectorScanStream {
    type Item = novarocks_execution::runtime::fragment::ExecutionResult<Chunk>;

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
                    match next {
                        Poll::Pending => {
                            this.step = SplitStep::Reading(split);
                            return Poll::Pending;
                        }
                        Poll::Ready(Some(Ok(page))) => {
                            this.step = SplitStep::Reading(split);
                            return match this.chunk_of(page) {
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
    failures: Arc<dyn novarocks_execution::runtime::fragment::ExecutionFailureReporter>,
) -> ExecutionResult<()> {
    let reader = async {
        if let Err(error) = reader_exit.await {
            let cause: novarocks_execution::runtime::fragment::ExecutionFailure =
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
    failures: Arc<dyn novarocks_execution::runtime::fragment::ExecutionFailureReporter>,
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
                        let cause: novarocks_execution::runtime::fragment::ExecutionFailure =
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
        failures: Arc<dyn novarocks_execution::runtime::fragment::ExecutionFailureReporter>,
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
        failures: Arc<dyn novarocks_execution::runtime::fragment::ExecutionFailureReporter>,
    ) -> BoxFuture<'static, novarocks_execution::runtime::fragment::ExecutionResult<()>> {
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
        "NOVAROCKS_CONNECTOR_PAGE_SOURCE_CLOSE",
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
mod stream_construction_tests {
    use super::*;
    use novarocks_execution::exec::chunk::ChunkSchema;
    use novarocks_execution::exec::pipeline::operator::input_construction::{
        ConstructionFailure, InputConstructionFactory, try_runtime_input_construction_factory,
    };
    use novarocks_execution::runtime::fragment::{ExecutionFailure, ExecutionFailureCause};
    use novarocks_execution::runtime::{mem_tracker::MemTracker, runtime_state::RuntimeErrorState};
    use novarocks_type_contract::ValueTypeStorageLifetime;

    #[test]
    fn close_failure_is_reported_while_sibling_exit_is_still_pending() {
        use novarocks_execution::runtime::fragment::ExecutionFailureReporter;
        use novarocks_spi::connector::{
            ConnectorErrorKind,
            read_stack::{ConnectorOperationExit, ConnectorSourceExitProof},
        };
        use std::sync::Mutex;
        #[derive(Default)]
        struct Reports {
            required: Mutex<Vec<ExecutionFailure>>,
            cleanup: Mutex<Vec<ExecutionFailure>>,
            health: Mutex<Vec<ExecutionFailure>>,
        }
        impl ExecutionFailureReporter for Reports {
            fn report_failure(&self, cause: ExecutionFailure) {
                self.required.lock().unwrap().push(cause);
            }
            fn report_cleanup(&self, cause: &ExecutionFailure) {
                self.cleanup.lock().unwrap().push(cause.clone());
            }
            fn report_unresolved_exit(&self, cause: &ExecutionFailure) {
                self.health.lock().unwrap().push(cause.clone());
            }
        }
        for reader_first in [true, false] {
            let reports = Arc::new(Reports::default());
            let operations = ConnectorSourceOperations::new();
            let receiver = operations.claim_failure_receiver().unwrap();
            let mut work = Some(operations.admit(Arc::new(|| {})).unwrap());
            let sibling = operations.admit(Arc::new(|| {})).unwrap();
            let reader_error = ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "actual reader cleanup failed",
            );
            let work_error = ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                "actual source operation failed",
            );
            let expected_reader: ExecutionFailure =
                ConnectorCallFailure::new(ConnectorOperation::CloseReader, reader_error.clone())
                    .into();
            let expected_work: ExecutionFailure =
                ConnectorCallFailure::new(ConnectorOperation::SourceExit, work_error.clone())
                    .into();
            let (send, wait) = futures::channel::oneshot::channel::<ConnectorError>();
            let reader: BoxFuture<'static, Result<(), ConnectorError>> = if reader_first {
                Box::pin(futures::future::ready(Err(reader_error.clone())))
            } else {
                Box::pin(async move { Err(wait.await.unwrap()) })
            };
            if !reader_first {
                work.take()
                    .unwrap()
                    .end(ConnectorOperationExit::WorkFailed(work_error.clone()));
            }
            operations.seal();
            let mut observe = Box::pin(observe_source_exit(
                receiver,
                operations.exited(),
                futures::future::ready(
                    novarocks_spi::connector::read_stack::ConnectorPreparationExit::Exited,
                ),
                reports.clone(),
            ));
            let mut close = Box::pin(observe_scan_close(
                reader,
                operations.exited(),
                reports.clone(),
            ));
            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            assert!(observe.as_mut().poll(&mut cx).is_pending());
            assert!(close.as_mut().poll(&mut cx).is_pending());
            assert_eq!(
                reports.required.lock().unwrap().len(),
                usize::from(!reader_first)
            );
            assert_eq!(
                reports.cleanup.lock().unwrap().len(),
                usize::from(reader_first)
            );
            if reader_first {
                work.take()
                    .unwrap()
                    .end(ConnectorOperationExit::WorkFailed(work_error));
            } else {
                send.send(reader_error).unwrap();
            }
            assert!(observe.as_mut().poll(&mut cx).is_pending());
            assert!(close.as_mut().poll(&mut cx).is_pending());
            assert_eq!(*reports.required.lock().unwrap(), vec![expected_work]);
            assert_eq!(*reports.cleanup.lock().unwrap(), vec![expected_reader]);
            assert!(reports.health.lock().unwrap().is_empty());
            assert!(!operations.is_exited());
            drop(sibling);
            assert_eq!(
                futures::executor::block_on(operations.exited()),
                ConnectorSourceExitProof::Exited
            );
            assert_eq!(
                observe.as_mut().poll(&mut cx),
                Poll::Ready(novarocks_execution::exec::node::scan::ScanExitProof::Exited)
            );
            assert_eq!(close.as_mut().poll(&mut cx), Poll::Ready(Ok(())));
            assert_eq!(reports.required.lock().unwrap().len(), 1);
            assert_eq!(reports.cleanup.lock().unwrap().len(), 1);
        }
    }

    #[test]
    fn preparation_unknown_reports_cleanup_and_health_before_later_required_and_real_sibling_exit()
    {
        use novarocks_execution::runtime::fragment::ExecutionFailureReporter;
        use novarocks_spi::connector::{
            ConnectorErrorKind,
            read_stack::{ConnectorOperationExit, ConnectorPreparationExit},
        };
        #[derive(Default)]
        struct Reports {
            required: std::sync::Mutex<Vec<ExecutionFailure>>,
            cleanup: std::sync::Mutex<Vec<ExecutionFailure>>,
            health: std::sync::Mutex<Vec<ExecutionFailure>>,
        }
        impl ExecutionFailureReporter for Reports {
            fn report_failure(&self, cause: ExecutionFailure) {
                self.required.lock().unwrap().push(cause);
            }
            fn report_cleanup(&self, cause: &ExecutionFailure) {
                self.cleanup.lock().unwrap().push(cause.clone());
            }
            fn report_unresolved_exit(&self, cause: &ExecutionFailure) {
                self.health.lock().unwrap().push(cause.clone());
            }
        }
        let reports = Arc::new(Reports::default());
        let operations = ConnectorSourceOperations::new();
        let events = operations.claim_failure_receiver().unwrap();
        let unknown = operations.admit(Arc::new(|| {})).unwrap();
        let work = operations.admit(Arc::new(|| {})).unwrap();
        let healthy = operations.admit(Arc::new(|| {})).unwrap();
        let cleanup = ConnectorError::new(
            ConnectorErrorKind::Internal,
            "original preparation owner could not prove physical exit",
        );
        let original = ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "later accepted source operation failed",
        );
        let expected_cleanup: ExecutionFailure =
            ConnectorCallFailure::new(ConnectorOperation::SourceExit, cleanup.clone()).into();
        let expected_work: ExecutionFailure =
            ConnectorCallFailure::new(ConnectorOperation::SourceExit, original.clone()).into();
        let (release_flow, flow) = futures::channel::oneshot::channel();
        let mut observer = Box::pin(observe_source_exit(
            events,
            operations.exited(),
            async move { flow.await.unwrap() },
            reports.clone(),
        ));
        unknown.end(ConnectorOperationExit::CleanupUnresolved(cleanup.clone()));
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        assert!(observer.as_mut().poll(&mut cx).is_pending());
        assert!(reports.required.lock().unwrap().is_empty());
        assert_eq!(
            *reports.cleanup.lock().unwrap(),
            vec![expected_cleanup.clone()]
        );
        assert_eq!(*reports.health.lock().unwrap(), vec![expected_cleanup]);
        work.report_work_failure(original);
        assert!(observer.as_mut().poll(&mut cx).is_pending());
        assert_eq!(*reports.required.lock().unwrap(), vec![expected_work]);
        work.end(ConnectorOperationExit::Exited);
        operations.seal();
        release_flow
            .send(ConnectorPreparationExit::Unresolved(cleanup))
            .unwrap();
        assert!(
            observer.as_mut().poll(&mut cx).is_pending(),
            "early unknown never drops the remaining work receiver"
        );
        assert_eq!(operations.live_operations(), 1);
        healthy.end(ConnectorOperationExit::Exited);
        assert_eq!(
            observer.as_mut().poll(&mut cx),
            Poll::Ready(novarocks_execution::exec::node::scan::ScanExitProof::Unresolved)
        );
        assert_eq!(reports.required.lock().unwrap().len(), 1);
        assert_eq!(reports.cleanup.lock().unwrap().len(), 1);
        assert_eq!(reports.health.lock().unwrap().len(), 1);
    }

    #[test]
    fn runtime_stream_requires_the_actual_task_source_before_construction() {
        use novarocks_execution_contract::failure::{FailureClass, FailureProjection};
        use novarocks_spi::connector::{ConnectorRequestContext, ConnectorStopOwner};
        let request = ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(60),
            ConnectorStopOwner::new().view(),
            1 << 20,
            1 << 22,
        )
        .unwrap();
        let failure = match required_source_operations(&request) {
            Err(failure) => failure,
            Ok(_) => panic!("planning-only context must not authorize a runtime stream"),
        };
        let expected = super::super::TypedScanBindingFailure::MissingExecutionSource;
        assert_eq!(
            failure.failure_fact(),
            novarocks_execution_contract::failure::FailureFact::new(
                FailureClass::Invariant,
                novarocks_execution_contract::failure::FailureReason::new(
                    novarocks_execution_contract::failure::domain::WORKER,
                    std::num::NonZeroU16::new(14).unwrap(),
                ),
            )
        );
        assert_eq!(failure.failure_fact(), expected.failure_fact());
        assert_eq!(failure.to_string(), expected.to_string());
        assert!(
            matches!(failure.cause(), ExecutionFailureCause::Domain(source)
            if source.downcast_ref::<super::super::TypedScanBindingFailure>().is_some())
        );
        let operations = ConnectorSourceOperations::new();
        let request = request.with_execution_source(
            novarocks_spi::connector::ConnectorRangeScope::try_new(1, 2, 1, 3, 4, 7).unwrap(),
            operations.clone(),
        );
        let same = required_source_operations(&request).unwrap();
        let ticket = same.admit(Arc::new(|| {})).unwrap();
        assert_eq!(operations.live_operations(), 1);
        operations.seal();
        assert!(!same.is_exited());
        ticket.end(novarocks_spi::connector::read_stack::ConnectorOperationExit::Exited);
        assert!(same.is_exited());
    }

    struct StreamStorage {
        _storage: ValueTypeStorageLifetime,
        initialized: [u8; 256],
    }
    fn declared_schema() -> ChunkSchema {
        ChunkSchema::try_new_typed(vec![], ValueTypeStorageLifetime::pure()).unwrap()
    }
    fn factory(
        tracker: &Arc<MemTracker>,
        error: Arc<RuntimeErrorState>,
    ) -> Arc<dyn InputConstructionFactory> {
        try_runtime_input_construction_factory(
            novarocks_execution::exec::pipeline::pipeline::representation_construction_profile()
                .unwrap(),
            tracker.clone(),
            error,
        )
        .unwrap()
    }

    #[test]
    fn claim_box_keeps_its_own_finite_storage_after_factory_and_control_exit() {
        let tracker = MemTracker::new_root("actual claim Box");
        let error = Arc::new(RuntimeErrorState::default());
        let controller = Arc::downgrade(&error);
        let factory = factory(&tracker, error);
        let boxed = create_scan_stream(&declared_schema(), factory.as_ref(), |storage| {
            StreamStorage {
                _storage: storage,
                initialized: [7; 256],
            }
        })
        .unwrap();
        assert_eq!(boxed.initialized, [7; 256]);
        drop(factory);
        assert!(controller.upgrade().is_none());
        assert!(tracker.current() > 0);
        drop(boxed);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn claim_box_memory_and_original_stop_refuse_before_the_constructor_runs() {
        let tracker = MemTracker::new_root("claim constructor admission");
        let error = Arc::new(RuntimeErrorState::default());
        let factory = factory(&tracker, error.clone());
        let initial = tracker.current();
        let schema = declared_schema();
        let sample = create_scan_stream(&schema, factory.as_ref(), |storage| StreamStorage {
            _storage: storage,
            initialized: [0; 256],
        })
        .unwrap();
        let requests = tracker.current() - initial;
        drop(sample);
        assert_eq!(tracker.current(), initial);
        tracker.install_limit_once(initial + requests - 1).unwrap();
        let entered = std::cell::Cell::new(false);
        let refusal = create_scan_stream(&schema, factory.as_ref(), |storage| {
            entered.set(true);
            StreamStorage {
                _storage: storage,
                initialized: [1; 256],
            }
        })
        .err()
        .expect("real Box request refuses");
        assert!(matches!(
            refusal.cause(),
            ExecutionFailureCause::Construction(ConstructionFailure::Memory(_))
        ));
        assert!(
            !entered.get(),
            "no constructor tail after the original tracker refusal"
        );
        assert_eq!(tracker.current(), initial);
        let original = ExecutionFailure::from(novarocks_functions::KernelFailure::Cancelled);
        error.set_error(original.clone());
        let refusal = create_scan_stream(&schema, factory.as_ref(), |storage| {
            entered.set(true);
            StreamStorage {
                _storage: storage,
                initialized: [2; 256],
            }
        })
        .err()
        .unwrap();
        assert_eq!(refusal, original);
        assert!(!entered.get());
        assert_eq!(tracker.current(), initial);
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }
}
