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
use novarocks_execution::exec::node::scan::{ScanChunkStream, ScanOutputStream, ScanStreamSource};
use novarocks_execution::runtime::profile::{ProfileUnit, RuntimeProfile};
use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::read_stack::{
    BudgetConsume, ConnectorPollBudget, ConnectorPreparationStart, ConnectorPreparedPageSource,
    ConnectorReadPageSourceProvider, ConnectorReadSystemTableProvider, OwnedConnectorPageStream,
    PageSourceFileMetrics, SourcePage,
};

use super::{TypedConnectorScanShared, TypedSystemTableScanShared, emit_page_source_marker};
use crate::read_attempt::ReceivedReadSplit;
use crate::typed_page_source::{TypedConnectorReaderMarker, flush_page_source_file_metrics};
use crate::typed_preparation_flow::StreamPreparationFlow;

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
    ) -> Result<ScanOutputStream, String> {
        if self.claimed.swap(true, Ordering::AcqRel) {
            return Err("typed connector scan stream is already claimed".to_string());
        }
        Ok(Box::pin(TypedConnectorScanStream {
            provider: self.shared.provider.resolve()?,
            converter: SourcePageConverter::new(self.shared.slot_ids.clone()),
            shared: Arc::clone(&self.shared),
            queue: Arc::clone(&self.queue),
            split_wake: Arc::clone(&self.split_wake),
            flow: Arc::clone(&self.flow),
            budget,
            profile,
            step: SplitStep::Idle,
            claims: VecDeque::new(),
            ended: false,
        }))
    }
}

struct TypedConnectorScanStream {
    shared: Arc<TypedConnectorScanShared>,
    provider: Arc<dyn ConnectorReadPageSourceProvider>,
    queue: Arc<SplitQueue<ReceivedReadSplit>>,
    split_wake: Arc<AtomicWaker>,
    flow: Arc<StreamPreparationFlow>,
    budget: ConnectorPollBudget,
    profile: Option<RuntimeProfile>,
    converter: SourcePageConverter,
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
    reader_marker: Option<TypedConnectorReaderMarker>,
    successor_control_id: Option<u64>,
    file_metrics: PageSourceFileMetrics,
}

struct ClosingSplit {
    exit: BoxFuture<'static, Result<(), ConnectorError>>,
    reader_marker: Option<TypedConnectorReaderMarker>,
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
            reader_marker: split.reader_marker,
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
    Failed(String),
}

enum NextSplit {
    Ready(ReceivedReadSplit),
    Pending,
    Exhausted,
}

impl TypedConnectorScanStream {
    /// Ends delivery on `error`. Whatever is open stays for `close`, which
    /// observes its exit.
    fn fail(&mut self, error: String) -> Poll<Option<Result<Chunk, String>>> {
        self.ended = true;
        self.flow.stop();
        Poll::Ready(Some(Err(error)))
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

    fn open_split(&mut self, split: ReceivedReadSplit) -> Result<(), String> {
        self.shared.check_liveness("page stream open")?;
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
                format!(
                    "create typed connector page stream for sequence {}: {error}",
                    split.sequence_id()
                )
            })?;
        self.install(&split, stream);
        Ok(())
    }

    fn open_claim(&mut self, claim: PreparedClaim) -> Result<(), String> {
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
                Err(error)
            }
            ClaimCandidate::Prepared(prepared) => {
                self.shared
                    .check_liveness("prepared page stream promotion")?;
                let stream = prepared
                    .promote(&self.shared.dynamic_filter, &self.budget)
                    .map_err(|error| {
                        format!(
                            "promote typed connector page stream for sequence {}: {error}",
                            split.sequence_id()
                        )
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
        let reader_marker =
            TypedConnectorReaderMarker::for_split(split, self.shared.emit_reader_markers);
        if let Some(marker) = reader_marker.as_ref() {
            marker.emit("OPEN");
        }
        // Acceptance evidence: a distributed run proves a page stream was
        // opened on this backend for this exact scheduled split, which a
        // result-only assertion cannot show.
        emit_page_source_marker(
            self.shared.emit_reader_markers,
            "NOVAROCKS_CONNECTOR_PAGE_SOURCE_OPEN",
            self.shared.plan_node_id,
            Some(split.sequence_id()),
        );
        if let Some(profile) = self.profile.as_ref() {
            profile.counter_add("TypedConnectorPageSourcesOpened", ProfileUnit::Unit, 1);
        }
        self.step = SplitStep::Reading(Box::new(OpenSplit {
            stream,
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
                claim.candidate = ClaimCandidate::Failed(format!(
                    "prepare typed connector page stream for sequence {}: {error}",
                    claim.split.sequence_id()
                ));
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
                Err(error) => ClaimCandidate::Failed(format!(
                    "prepare typed connector page stream for sequence {}: {error}",
                    next.sequence_id()
                )),
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

    fn chunk_of(&mut self, page: SourcePage) -> Result<Chunk, String> {
        let chunk = self
            .converter
            .convert(page)
            .map_err(|error| error.to_string())?;
        self.shared.materialize_output(chunk)
    }
}

impl Stream for TypedConnectorScanStream {
    type Item = Result<Chunk, String>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let _context = this.shared.stream_host.runtime().enter();
        loop {
            if this.ended {
                return Poll::Ready(None);
            }
            if let Err(error) = this.shared.check_liveness("split driver") {
                return this.fail(error);
            }
            match std::mem::replace(&mut this.step, SplitStep::Idle) {
                SplitStep::Closing(mut closing) => match closing.exit.as_mut().poll(cx) {
                    Poll::Pending => {
                        this.step = SplitStep::Closing(closing);
                        return Poll::Pending;
                    }
                    Poll::Ready(result) => {
                        emit_split_close_markers(
                            closing.reader_marker.as_ref(),
                            this.shared.emit_reader_markers,
                            this.shared.plan_node_id,
                        );
                        if let Err(error) = result {
                            return this
                                .fail(format!("close typed connector page stream: {error}"));
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
                            return this.fail(format!("read typed connector page stream: {error}"));
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

impl ScanChunkStream for TypedConnectorScanStream {
    fn close(self: Pin<Box<Self>>) -> BoxFuture<'static, Result<(), String>> {
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
        let task_exit = seal_task_source(&this.shared.request);
        let drained = this.flow.drained();
        let markers = (this.shared.emit_reader_markers, this.shared.plan_node_id);
        Box::pin(InScanRuntime {
            runtime,
            inner: Box::pin(async move {
                let mut first_error = None;
                if let Some(split) = split {
                    let result = split.exit.await;
                    emit_split_close_markers(split.reader_marker.as_ref(), markers.0, markers.1);
                    if let Err(error) = result {
                        first_error
                            .get_or_insert(format!("close typed connector page stream: {error}"));
                    }
                }
                drained.await;
                if let Some(exit) = task_exit
                    && let Err(error) = exit.await
                {
                    first_error.get_or_insert(format!("typed connector scan source exit: {error}"));
                }
                first_error.map_or(Ok(()), Err)
            }),
        })
    }
}

fn emit_split_close_markers(
    reader_marker: Option<&TypedConnectorReaderMarker>,
    enabled: bool,
    plan_node_id: i32,
) {
    if let Some(marker) = reader_marker {
        marker.emit("CLOSE");
    }
    emit_page_source_marker(
        enabled,
        "NOVAROCKS_CONNECTOR_PAGE_SOURCE_CLOSE",
        plan_node_id,
        None,
    );
}

/// Seals the scan's task source, so none of its operations starts after
/// this, and returns the observation of their exit.
fn seal_task_source(
    request: &novarocks_spi::connector::ConnectorRequestContext,
) -> Option<novarocks_spi::connector::read_stack::ConnectorSourceExit> {
    request.source_operations().map(|operations| {
        operations.seal();
        operations.exited()
    })
}

/// Polls `inner` inside the scan I/O runtime's context, where page streams
/// may use its timers and spawn onto it.
struct InScanRuntime {
    runtime: tokio::runtime::Handle,
    inner: BoxFuture<'static, Result<(), String>>,
}

impl Future for InScanRuntime {
    type Output = Result<(), String>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let _context = self.runtime.enter();
        self.inner.as_mut().poll(cx)
    }
}

// ---------------------------------------------------------------------------
// System relations read by exactly one backend
// ---------------------------------------------------------------------------

/// Hands a system relation scan's one stream to the driver that runs it.
pub(super) struct TypedSystemTableStreamSource {
    shared: Arc<TypedSystemTableScanShared>,
    claimed: AtomicBool,
}

impl TypedSystemTableStreamSource {
    pub(super) fn new(shared: Arc<TypedSystemTableScanShared>) -> Arc<Self> {
        Arc::new(Self {
            shared,
            claimed: AtomicBool::new(false),
        })
    }
}

impl ScanStreamSource for TypedSystemTableStreamSource {
    fn claim(
        &self,
        budget: ConnectorPollBudget,
        profile: Option<RuntimeProfile>,
    ) -> Result<ScanOutputStream, String> {
        if self.claimed.swap(true, Ordering::AcqRel) {
            return Err("typed system relation scan stream is already claimed".to_string());
        }
        Ok(Box::pin(TypedSystemTableStream {
            provider: self.shared.provider.resolve()?,
            converter: SourcePageConverter::new(self.shared.slot_ids.clone()),
            shared: Arc::clone(&self.shared),
            budget,
            profile,
            step: SystemStep::Unopened,
            failed: false,
        }))
    }
}

struct TypedSystemTableStream {
    shared: Arc<TypedSystemTableScanShared>,
    provider: Arc<dyn ConnectorReadSystemTableProvider>,
    budget: ConnectorPollBudget,
    profile: Option<RuntimeProfile>,
    converter: SourcePageConverter,
    step: SystemStep,
    /// Delivery ended on an error; what is open stays for `close`.
    failed: bool,
}

enum SystemStep {
    Unopened,
    Reading {
        stream: OwnedConnectorPageStream,
        file_metrics: PageSourceFileMetrics,
    },
    Closing(BoxFuture<'static, Result<(), ConnectorError>>),
    Ended,
}

impl TypedSystemTableStream {
    fn open(&mut self) -> Result<OwnedConnectorPageStream, String> {
        self.shared.check_liveness("open")?;
        let stream = self
            .provider
            .create_system_page_stream(
                &self.shared.session,
                self.shared.descriptor.table(),
                self.shared.descriptor.assignments(),
                &self.budget,
            )
            .map_err(|error| format!("create typed system relation page stream: {error}"))?;
        // No sequence: a system relation read has no split, and printing one
        // would be the first step toward asserting scheduling identity it does
        // not have.
        emit_page_source_marker(
            self.shared.emit_reader_markers,
            "NOVAROCKS_CONNECTOR_PAGE_SOURCE_OPEN",
            self.shared.plan_node_id,
            None,
        );
        if let Some(profile) = self.profile.as_ref() {
            profile.counter_add("TypedSystemTablePageSourcesOpened", ProfileUnit::Unit, 1);
        }
        Ok(stream)
    }

    fn chunk_of(&mut self, page: SourcePage) -> Result<Chunk, String> {
        let chunk = self
            .converter
            .convert(page)
            .map_err(|error| error.to_string())?;
        self.shared.materialize_output(chunk)
    }

    fn fail(&mut self, error: String) -> Poll<Option<Result<Chunk, String>>> {
        self.failed = true;
        Poll::Ready(Some(Err(error)))
    }
}

impl Stream for TypedSystemTableStream {
    type Item = Result<Chunk, String>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let _context = this.shared.stream_runtime.enter();
        loop {
            if this.failed {
                return Poll::Ready(None);
            }
            match std::mem::replace(&mut this.step, SystemStep::Ended) {
                SystemStep::Ended => return Poll::Ready(None),
                SystemStep::Unopened => match this.open() {
                    Ok(stream) => {
                        this.step = SystemStep::Reading {
                            stream,
                            file_metrics: PageSourceFileMetrics::default(),
                        };
                    }
                    Err(error) => return this.fail(error),
                },
                SystemStep::Reading {
                    mut stream,
                    mut file_metrics,
                } => {
                    if let Err(error) = this.shared.check_liveness("read") {
                        this.step = SystemStep::Reading {
                            stream,
                            file_metrics,
                        };
                        return this.fail(error);
                    }
                    let next = stream.as_mut().poll_next(cx);
                    flush_page_source_file_metrics(
                        this.profile.as_ref(),
                        &mut file_metrics,
                        stream.metrics().file,
                    );
                    match next {
                        Poll::Ready(None) => this.step = SystemStep::Closing(stream.close()),
                        next => {
                            this.step = SystemStep::Reading {
                                stream,
                                file_metrics,
                            };
                            return match next {
                                Poll::Pending => Poll::Pending,
                                Poll::Ready(Some(Ok(page))) => match this.chunk_of(page) {
                                    Ok(chunk) => Poll::Ready(Some(Ok(chunk))),
                                    Err(error) => this.fail(error),
                                },
                                Poll::Ready(Some(Err(error))) => this.fail(format!(
                                    "read typed system relation page stream: {error}"
                                )),
                                Poll::Ready(None) => unreachable!("handled above"),
                            };
                        }
                    }
                }
                SystemStep::Closing(mut exit) => match exit.as_mut().poll(cx) {
                    Poll::Pending => {
                        this.step = SystemStep::Closing(exit);
                        return Poll::Pending;
                    }
                    Poll::Ready(result) => {
                        emit_page_source_marker(
                            this.shared.emit_reader_markers,
                            "NOVAROCKS_CONNECTOR_PAGE_SOURCE_CLOSE",
                            this.shared.plan_node_id,
                            None,
                        );
                        return match result {
                            Ok(()) => Poll::Ready(None),
                            Err(error) => this
                                .fail(format!("close typed system relation page stream: {error}")),
                        };
                    }
                },
            }
        }
    }
}

impl ScanChunkStream for TypedSystemTableStream {
    fn close(self: Pin<Box<Self>>) -> BoxFuture<'static, Result<(), String>> {
        let this = *Pin::into_inner(self);
        let runtime = this.shared.stream_runtime.clone();
        let _context = runtime.enter();
        let exit = match this.step {
            SystemStep::Reading { stream, .. } => Some(stream.close()),
            SystemStep::Closing(exit) => Some(exit),
            SystemStep::Unopened | SystemStep::Ended => None,
        };
        let task_exit = seal_task_source(&this.shared.request);
        let markers = (this.shared.emit_reader_markers, this.shared.plan_node_id);
        Box::pin(InScanRuntime {
            runtime,
            inner: Box::pin(async move {
                let mut first_error = None;
                if let Some(exit) = exit {
                    let result = exit.await;
                    emit_page_source_marker(
                        markers.0,
                        "NOVAROCKS_CONNECTOR_PAGE_SOURCE_CLOSE",
                        markers.1,
                        None,
                    );
                    if let Err(error) = result {
                        first_error.get_or_insert(format!(
                            "close typed system relation page stream: {error}"
                        ));
                    }
                }
                if let Some(exit) = task_exit
                    && let Err(error) = exit.await
                {
                    first_error
                        .get_or_insert(format!("typed system relation scan source exit: {error}"));
                }
                first_error.map_or(Ok(()), Err)
            }),
        })
    }
}
