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

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use futures::future::BoxFuture;
use novarocks_spi::connector::read_stack::ConnectorPollBudget;
use tracing::warn;

use super::output_filter::{
    ScanLimitDecision, ScanOutputFilter, record_rows_read, scan_limit_decision,
};
use crate::exec::chunk::Chunk;
use crate::exec::expr::ExprArena;
use crate::exec::node::scan::{ScanNode, ScanOp, ScanOutputStream, ScanStreamSource};
use crate::exec::operators::runtime_filter::{
    NativeOrderedLiveConsumerSet, RuntimeFilterConsumerSet, RuntimeFilterGate,
};
use crate::exec::pipeline::operator::{
    DriverBlockDeadline, FinishWatch, Operator, ProcessorOperator, forward_observable,
};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::fragment::io::{FragmentEventSink, NoopFragmentEventSink};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::OperatorProfiles;
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};
use crate::runtime::scan_stream_metrics::{ScanStreamPending, observe_scan_stream_pending};

/// CPU budget of one driver turn inside a scan stream, in provider work
/// units (roughly one decoded batch each).
pub(crate) const SCAN_STREAM_TURN_BUDGET: u64 = 64;

/// A scan's runtime-filter consumers: blocking membership filters behind one
/// gate, and live ordered filters.
fn native_scan_consumers(
    scan: &ScanNode,
    arena: &Arc<ExprArena>,
) -> Result<(RuntimeFilterConsumerSet, NativeOrderedLiveConsumerSet), String> {
    let mut membership_specs = Vec::new();
    let mut ordered_live_specs = Vec::new();
    let mut seen_bindings = HashSet::new();
    for spec in scan.native_runtime_filter_specs() {
        if !seen_bindings.insert(spec.binding_id()) {
            return Err(format!(
                "duplicate native scan runtime-filter consumer binding_id={}",
                spec.binding_id()
            ));
        }
        match spec.execution_contract() {
            crate::exec::node::runtime_filter::RuntimeFilterExecutionContract::Membership(_) => {
                membership_specs.push(spec.clone())
            }
            crate::exec::node::runtime_filter::RuntimeFilterExecutionContract::Ordered(_) => {
                ordered_live_specs.push(spec.clone())
            }
        }
    }
    let blocking =
        RuntimeFilterConsumerSet::from_plan("Scan", &membership_specs, Arc::clone(arena))?;
    let ordered_live =
        NativeOrderedLiveConsumerSet::from_plan(&ordered_live_specs, Arc::clone(arena))?;
    Ok((blocking, ordered_live))
}

/// The operator and profile name of a scan source, always carrying the plan
/// node id.
fn scan_source_name(scan: &ScanNode, op: &dyn ScanOp) -> String {
    let name = op
        .profile_name()
        .unwrap_or_else(|| "ScanSource".to_string());
    if name.contains("plan_node_id=") || name.contains("(id=") {
        return name;
    }
    if let Some(node_id) = scan.node_id() {
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
    scan: ScanNode,
    op: Arc<dyn ScanOp>,
    source: Arc<dyn ScanStreamSource>,
    filter: ScanOutputFilter,
    blocking: RuntimeFilterConsumerSet,
}

impl StreamScanSourceFactory {
    pub(crate) fn new_native(
        scan: ScanNode,
        op: Arc<dyn ScanOp>,
        arena: Arc<ExprArena>,
    ) -> Result<Self, String> {
        let source = op.stream_source();
        let (blocking, ordered_live) = native_scan_consumers(&scan, &arena)?;
        let name = scan_source_name(&scan, op.as_ref());
        let filter =
            ScanOutputFilter::new(&scan, arena, Some(blocking.clone()), Some(ordered_live));
        Ok(Self {
            name,
            scan,
            op,
            source,
            filter,
            blocking,
        })
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
        forward_observable(&self.blocking.gate_observable(), &readiness.observable());
        Box::new(StreamScanSourceOperator {
            name: self.name.clone(),
            scan: self.scan.clone(),
            op: Arc::clone(&self.op),
            source: Arc::clone(&self.source),
            stage: StreamStage::Unclaimed,
            closing: Mutex::new(None),
            readiness,
            budget: ConnectorPollBudget::new(),
            wake_generation: None,
            filter: self.filter.clone(),
            rows_emitted: 0,
            profiles: None,
            event_sink: Arc::new(NoopFragmentEventSink),
            runtime_error: None,
            output_tracker_label: format!(
                "scan_stream_output node={} driver={driver_id}",
                self.scan.node_id().unwrap_or(-1)
            ),
            output_tracker: None,
            downstream_paused: false,
        })
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

struct StreamScanSourceOperator {
    name: String,
    scan: ScanNode,
    op: Arc<dyn ScanOp>,
    source: Arc<dyn ScanStreamSource>,
    stage: StreamStage,
    /// The exit observation of an ended stream, until it resolves.
    closing: Mutex<Option<BoxFuture<'static, Result<(), String>>>>,
    readiness: Arc<SourceReadiness>,
    budget: ConnectorPollBudget,
    /// The readiness generation sampled before the last poll, when that poll
    /// returned `Pending`. A different generation means the stream woke up.
    wake_generation: Option<u64>,
    filter: ScanOutputFilter,
    rows_emitted: usize,
    profiles: Option<OperatorProfiles>,
    event_sink: Arc<dyn FragmentEventSink>,
    runtime_error: Option<Arc<RuntimeErrorState>>,
    output_tracker_label: String,
    /// Charges every chunk this source hands downstream to the fragment.
    output_tracker: Option<Arc<MemTracker>>,
    downstream_paused: bool,
}

impl StreamScanSourceOperator {
    /// Ends delivery: the stream is closed, which stops its operations
    /// before returning, and only its exit observation is kept.
    fn end_delivery(&mut self) {
        if let StreamStage::Open(stream) = std::mem::replace(&mut self.stage, StreamStage::Ended) {
            *self.closing.lock().expect("scan stream close lock") = Some(stream.close());
        }
        self.wake_generation = None;
        self.set_downstream_paused(false);
    }

    fn set_downstream_paused(&mut self, paused: bool) {
        if self.downstream_paused != paused {
            self.downstream_paused = paused;
            self.op.on_output_backpressure(paused);
        }
    }

    fn gate_holds_input(&self) -> bool {
        self.filter
            .blocking()
            .is_some_and(RuntimeFilterConsumerSet::gate_holds_input)
    }
}

impl Operator for StreamScanSourceOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn set_profiles(&mut self, profiles: OperatorProfiles) {
        self.profiles = Some(profiles);
    }

    fn set_fragment_event_sink(&mut self, sink: Arc<dyn FragmentEventSink>) {
        self.event_sink = sink;
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> Result<(), String> {
        if let Some(consumers) = self.filter.blocking() {
            consumers.set_wait_timeout(scan_runtime_filter_wait_timeout(state));
            consumers.bind(state)?;
        }
        if let Some(consumers) = self.filter.ordered_live() {
            consumers.bind(state)?;
        }
        self.runtime_error = Some(state.error_state());
        self.output_tracker = state
            .mem_tracker()
            .map(|root| MemTracker::new_child(self.output_tracker_label.clone(), &root));
        Ok(())
    }

    fn close(&mut self) -> Result<(), String> {
        self.end_delivery();
        Ok(())
    }

    /// Ends delivery and starts the scan's terminal cleanup, which stops
    /// what its stream does not own itself, such as queued work.
    fn cancel(&mut self) {
        self.end_delivery();
        let _ = self.op.terminate();
    }

    fn is_finished(&self) -> bool {
        matches!(self.stage, StreamStage::Ended)
    }

    /// An ended stream is observed until its operations exited, so its
    /// driver completes only after the real exit and reports its error.
    fn pending_finish(&self) -> Option<FinishWatch> {
        let mut closing = self.closing.lock().expect("scan stream close lock");
        let future = closing.as_mut()?;
        let waker = self.readiness.waker();
        let mut context = Context::from_waker(&waker);
        match future.as_mut().poll(&mut context) {
            Poll::Pending => Some(FinishWatch::Notify(self.readiness.observable())),
            Poll::Ready(result) => {
                *closing = None;
                if let Err(error) = result
                    && let Some(runtime_error) = self.runtime_error.as_ref()
                {
                    runtime_error.set_error(error);
                }
                None
            }
        }
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
}

impl ProcessorOperator for StreamScanSourceOperator {
    fn need_input(&self) -> bool {
        false
    }

    /// Whether polling now can make progress: always after the stream
    /// delivered, and after a `Pending` only once it woke the driver.
    fn has_output(&self) -> bool {
        match &self.stage {
            StreamStage::Ended => false,
            StreamStage::Unclaimed | StreamStage::Open(_) => {
                !self.gate_holds_input()
                    && self
                        .wake_generation
                        .is_none_or(|generation| self.readiness.generation() != generation)
            }
        }
    }

    fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
        Err("scan source operator does not accept input".to_string())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        if matches!(self.stage, StreamStage::Ended) {
            return Ok(None);
        }
        // The gate's wait starts where the scan would read its first page.
        if let Some(consumers) = self.filter.blocking()
            && !matches!(consumers.poll_gate(), RuntimeFilterGate::Open)
        {
            return Ok(None);
        }
        if matches!(self.stage, StreamStage::Unclaimed) {
            let profile = self
                .profiles
                .as_ref()
                .map(|profiles| profiles.unique.clone());
            self.stage = StreamStage::Open(self.source.claim(self.budget.clone(), profile)?);
        }
        let waker = self.readiness.waker();
        let mut context = Context::from_waker(&waker);
        loop {
            let StreamStage::Open(stream) = &mut self.stage else {
                return Ok(None);
            };
            // Sampled before the poll: a wake during or after it moves the
            // generation past this value, so it cannot be lost.
            let generation = self.readiness.generation();
            let exhaustions = self.budget.exhaustions();
            match stream.as_mut().poll_next(&mut context) {
                Poll::Pending => {
                    self.wake_generation = Some(generation);
                    observe_scan_stream_pending(if self.budget.exhaustions() > exhaustions {
                        ScanStreamPending::BudgetYield
                    } else {
                        ScanStreamPending::Wait
                    });
                    return Ok(None);
                }
                Poll::Ready(None) => {
                    self.end_delivery();
                    return Ok(None);
                }
                Poll::Ready(Some(Err(error))) => {
                    self.end_delivery();
                    return Err(error);
                }
                Poll::Ready(Some(Ok(chunk))) => {
                    self.wake_generation = None;
                    self.filter.poll_live_updates()?;
                    let Some(chunk) =
                        self.filter
                            .apply(chunk, self.profiles.as_ref(), &self.event_sink)?
                    else {
                        continue;
                    };
                    let rows = chunk.len();
                    match scan_limit_decision(self.scan.limit(), self.rows_emitted, rows) {
                        ScanLimitDecision::Stop => {
                            self.end_delivery();
                            return Ok(None);
                        }
                        ScanLimitDecision::EmitThenStop => {
                            self.rows_emitted = self.rows_emitted.saturating_add(rows);
                            self.end_delivery();
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
                    return Ok(Some(chunk));
                }
            }
        }
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        Ok(())
    }

    fn source_observable(&self) -> Option<Arc<Observable>> {
        Some(self.readiness.observable())
    }

    fn source_block_deadline(&self) -> Option<DriverBlockDeadline> {
        self.filter
            .blocking()
            .and_then(RuntimeFilterConsumerSet::gate_deadline)
    }

    fn begin_turn(&mut self) {
        self.budget.refill(SCAN_STREAM_TURN_BUDGET);
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
    use crate::exec::node::scan::{ScanChunkStream, ScanNode, ScanOp};
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
        ended: AtomicBool,
        waker: Mutex<Option<Waker>>,
        /// On the next empty poll, make `staged` available and wake the
        /// task before returning `Pending`, as work finishing mid-poll does.
        wake_inside_poll: AtomicBool,
        staged: Mutex<Option<Chunk>>,
        polls: AtomicUsize,
        close_requested: AtomicBool,
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
        type Item = Result<Chunk, String>;

        fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let this = self.get_mut();
            this.control.polls.fetch_add(1, Ordering::AcqRel);
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

    impl ScanChunkStream for ScriptedStream {
        fn close(self: Pin<Box<Self>>) -> BoxFuture<'static, Result<(), String>> {
            self.control.close_requested.store(true, Ordering::Release);
            self.control.operations.seal();
            let exit = self.control.operations.exited();
            Box::pin(async move { exit.await.map_err(|error| error.to_string()) })
        }
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
        ) -> Result<ScanOutputStream, String> {
            if self.claims.fetch_add(1, Ordering::AcqRel) > 0 {
                return Err("scan stream already claimed".to_string());
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
        }

        fn terminate(&self) -> Result<(), String> {
            self.source
                .control
                .terminations
                .fetch_add(1, Ordering::AcqRel);
            Ok(())
        }

        fn stream_source(&self) -> Arc<dyn ScanStreamSource> {
            Arc::clone(&self.source) as Arc<dyn ScanStreamSource>
        }
    }

    struct CollectSink {
        values: Arc<Mutex<Vec<i32>>>,
        finished: bool,
        observable: Arc<Observable>,
    }

    impl Operator for CollectSink {
        fn name(&self) -> &str {
            "COLLECT_SINK"
        }

        fn is_finished(&self) -> bool {
            self.finished
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for CollectSink {
        fn need_input(&self) -> bool {
            !self.finished
        }

        fn has_output(&self) -> bool {
            false
        }

        fn push_chunk(&mut self, _: &RuntimeState, chunk: Chunk) -> Result<(), String> {
            self.values.lock().expect("values").push(value_of(&chunk));
            Ok(())
        }

        fn pull_chunk(&mut self, _: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(None)
        }

        fn set_finishing(&mut self, _: &RuntimeState) -> Result<(), String> {
            self.finished = true;
            Ok(())
        }

        fn sink_observable(&self) -> Option<Arc<Observable>> {
            Some(Arc::clone(&self.observable))
        }
    }

    fn stream_scan_source(
        control: &Arc<StreamControl>,
    ) -> (Box<dyn Operator>, Arc<ScriptedSource>) {
        let (operator, source, _) = observed_stream_scan_source(control);
        (operator, source)
    }

    #[expect(
        clippy::type_complexity,
        reason = "The scan operator is returned with the source and the backpressure log it reports to."
    )]
    fn observed_stream_scan_source(
        control: &Arc<StreamControl>,
    ) -> (
        Box<dyn Operator>,
        Arc<ScriptedSource>,
        Arc<Mutex<Vec<bool>>>,
    ) {
        observed_stream_scan_source_in(control, &RuntimeState::default())
    }

    #[expect(
        clippy::type_complexity,
        reason = "The scan operator is returned with the source and the backpressure log it reports to."
    )]
    fn observed_stream_scan_source_in(
        control: &Arc<StreamControl>,
        state: &RuntimeState,
    ) -> (
        Box<dyn Operator>,
        Arc<ScriptedSource>,
        Arc<Mutex<Vec<bool>>>,
    ) {
        let source = Arc::new(ScriptedSource {
            control: Arc::clone(control),
            claims: AtomicUsize::new(0),
        });
        let backpressure = Arc::new(Mutex::new(Vec::new()));
        let op: Arc<dyn ScanOp> = Arc::new(StreamScanOp {
            source: Arc::clone(&source),
            backpressure: Arc::clone(&backpressure),
        });
        let scan = ScanNode::new_for_test(Arc::clone(&op)).with_node_id(7);
        let factory = StreamScanSourceFactory::new_native(scan, op, Arc::new(ExprArena::default()))
            .expect("stream scan factory");
        let mut operator = factory.create(1, 0);
        operator
            .bind_runtime_state(state)
            .expect("bind stream scan");
        (operator, source, backpressure)
    }

    struct Pipeline {
        driver: PipelineDriver,
        control: Arc<StreamControl>,
        source: Arc<ScriptedSource>,
        values: Arc<Mutex<Vec<i32>>>,
    }

    fn pipeline() -> Pipeline {
        let control = Arc::new(StreamControl::default());
        let (scan, source) = stream_scan_source(&control);
        let values = Arc::new(Mutex::new(Vec::new()));
        let driver = PipelineDriver::new(
            1,
            vec![
                scan,
                Box::new(CollectSink {
                    values: Arc::clone(&values),
                    finished: false,
                    observable: Arc::new(Observable::new()),
                }),
            ],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            None,
        );
        Pipeline {
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
    fn a_wake_inside_the_poll_keeps_the_driver_runnable() {
        let mut pipeline = pipeline();
        *pipeline.control.staged.lock().expect("staged") = Some(one_row(1));
        pipeline
            .control
            .wake_inside_poll
            .store(true, Ordering::Release);

        // The poll returns Pending after waking itself: the driver must not
        // park on a wake it already received.
        assert!(matches!(pipeline.driver.process(TURN), DriverState::Ready));
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

        ticket.end(Ok(()));
        assert!(matches!(
            pipeline.driver.process(TURN),
            DriverState::Finished
        ));
    }

    #[test]
    fn a_close_that_is_never_observed_still_stops_the_stream() {
        let control = Arc::new(StreamControl::default());
        let (mut scan, _source) = stream_scan_source(&control);
        let _ticket = control
            .operations
            .admit(Arc::new(|| {}))
            .expect("one running operation");
        let processor = scan.as_processor_mut().expect("stream scan processor");
        assert!(
            processor
                .pull_chunk(&RuntimeState::default())
                .expect("poll")
                .is_none()
        );

        scan.close().expect("close");
        drop(scan);
        assert!(control.close_requested.load(Ordering::Acquire));
        assert!(
            control.operations.is_sealed(),
            "the stop was requested; exit stays owned by the source's operations"
        );
    }

    #[test]
    fn cancelling_a_stream_scan_starts_its_scan_s_terminal_cleanup() {
        let control = Arc::new(StreamControl::default());
        let (mut scan, _source) = stream_scan_source(&control);
        scan.cancel();
        assert_eq!(control.terminations.load(Ordering::Acquire), 1);
        assert!(scan.is_finished());
    }

    #[test]
    fn a_stream_scan_charges_the_chunks_it_hands_out_to_its_fragment() {
        let fragment = crate::runtime::mem_tracker::MemTracker::new_root("fragment_under_test");
        let state = RuntimeState::new(
            None,
            None,
            None,
            None,
            None,
            Some(Arc::clone(&fragment)),
            None,
            None,
            None,
        );
        let control = Arc::new(StreamControl::default());
        let (mut scan, _source, _) = observed_stream_scan_source_in(&control, &state);
        control.push(one_row(3));
        let processor = scan.as_processor_mut().expect("stream scan processor");
        processor.begin_turn();
        let chunk = processor
            .pull_chunk(&state)
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

    impl Operator for GatedSink {
        fn name(&self) -> &str {
            "GATED_SINK"
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for GatedSink {
        fn need_input(&self) -> bool {
            self.open.load(Ordering::Acquire)
        }

        fn has_output(&self) -> bool {
            false
        }

        fn push_chunk(&mut self, _: &RuntimeState, chunk: Chunk) -> Result<(), String> {
            self.values.lock().expect("values").push(value_of(&chunk));
            Ok(())
        }

        fn pull_chunk(&mut self, _: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(None)
        }

        fn set_finishing(&mut self, _: &RuntimeState) -> Result<(), String> {
            Ok(())
        }

        fn sink_observable(&self) -> Option<Arc<Observable>> {
            Some(Arc::clone(&self.observable))
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
                Box::new(GatedSink {
                    open: Arc::clone(&open),
                    values: Arc::clone(&values),
                    observable: Arc::clone(&sink_observable),
                }),
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
        observable: Arc<Observable>,
    }

    impl Operator for BlockingSink {
        fn name(&self) -> &str {
            "BLOCKING_SINK"
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for BlockingSink {
        fn need_input(&self) -> bool {
            true
        }

        fn has_output(&self) -> bool {
            false
        }

        fn push_chunk(&mut self, _: &RuntimeState, chunk: Chunk) -> Result<(), String> {
            self.entered.send(value_of(&chunk)).expect("entered");
            self.release
                .lock()
                .expect("release")
                .recv()
                .expect("released");
            Ok(())
        }

        fn pull_chunk(&mut self, _: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(None)
        }

        fn set_finishing(&mut self, _: &RuntimeState) -> Result<(), String> {
            Ok(())
        }

        fn sink_observable(&self) -> Option<Arc<Observable>> {
            Some(Arc::clone(&self.observable))
        }
    }

    #[test]
    fn the_scan_keeps_producing_while_a_second_worker_runs_the_downstream() {
        let control = Arc::new(StreamControl::default());
        let (scan, _source) = stream_scan_source(&control);
        let exchanger = LocalExchanger::new_handoff(1, 1, 8, Arc::new(ExprArena::default()));
        let state = Arc::new(RuntimeState::default());
        let mut scan_driver = PipelineDriver::new(
            1,
            vec![
                scan,
                LocalExchangeSinkFactory::new(-1, Arc::clone(&exchanger)).create(1, 0),
            ],
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
                Box::new(BlockingSink {
                    entered: entered_tx,
                    release: Arc::new(Mutex::new(release_rx)),
                    observable: Arc::new(Observable::new()),
                }),
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
