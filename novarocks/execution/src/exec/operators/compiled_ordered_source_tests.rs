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

//! Actual driver-polled source composition using the ancestor's checked Scan
//! and real typed input. The scripted connector owns one stream and the
//! original operation-exit fence. This component is not Native qualification.
use super::*;
use crate::exec::node::scan::{ScanChunkStream, ScanOp, ScanOutputStream, ScanStreamSource};
use crate::exec::operators::scan::StreamScanSourceFactory;
use crate::exec::pipeline::OperatorInstance;
use crate::exec::pipeline::driver_step::{DEFAULT_TURN_WORK_UNITS, DriverStep, TurnWork};
use crate::exec::pipeline::operator::FinishWatch;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::shape::StepOperator;
use crate::runtime::task::ExecutionResult;
use futures::{Stream, future::BoxFuture};
use novarocks_spi::connector::read_stack::{
    BudgetConsume, ConnectorPollBudget, ConnectorSourceOperations,
};
use std::future::Future;
use std::{
    pin::Pin,
    sync::atomic::AtomicBool,
    task::{Context, Poll, Waker},
};

struct Pages {
    chunks: Mutex<VecDeque<Chunk>>,
    ended: AtomicBool,
    waker: Mutex<Option<Waker>>,
    polls: AtomicUsize,
    closes: AtomicUsize,
    stops: AtomicUsize,
    consumed: AtomicUsize,
    operations: ConnectorSourceOperations,
}
impl Pages {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            chunks: Mutex::new(VecDeque::new()),
            ended: AtomicBool::new(false),
            waker: Mutex::new(None),
            polls: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
            consumed: AtomicUsize::new(0),
            operations: ConnectorSourceOperations::new(),
        })
    }
    fn push(&self, chunk: Chunk) {
        self.chunks.lock().unwrap().push_back(chunk);
        self.wake();
    }
    fn end(&self) {
        self.ended.store(true, Ordering::SeqCst);
        self.wake();
    }
    fn wake(&self) {
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
}
struct PageStream {
    pages: Arc<Pages>,
    budget: ConnectorPollBudget,
    spending: Option<BudgetConsume>,
}
impl Stream for PageStream {
    type Item = ExecutionResult<Chunk>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.pages.polls.fetch_add(1, Ordering::SeqCst);
        if this.pages.chunks.lock().unwrap().is_empty() {
            if this.pages.ended.load(Ordering::SeqCst) {
                return Poll::Ready(None);
            }
            *this.pages.waker.lock().unwrap() = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let spending = this.spending.get_or_insert_with(|| this.budget.consume(1));
        if Pin::new(spending).poll(cx).is_pending() {
            return Poll::Pending;
        }
        this.spending = None;
        Poll::Ready(Some(Ok(this
            .pages
            .chunks
            .lock()
            .unwrap()
            .pop_front()
            .expect("declared page"))))
    }
}
impl ScanChunkStream for PageStream {
    fn take_exit_observation(
        self: Pin<&mut Self>,
        failures: Arc<dyn crate::runtime::task::ExecutionFailureReporter>,
    ) -> ExecutionResult<crate::exec::node::scan::ScanExitObservation> {
        Ok(crate::exec::node::scan::ScanExitObservation {
            // Pages retains the real queue of chunks; its body must also drop.
            body_proof: crate::exec::node::scan::ScanExitBodyProof::CloseBodyRequired,
            actual_exit: Box::pin(observe_fixture_source(
                self.pages.operations.clone(),
                failures,
            )),
        })
    }
    fn close(
        self: Pin<Box<Self>>,
        _failures: Arc<dyn crate::runtime::task::ExecutionFailureReporter>,
    ) -> BoxFuture<'static, ExecutionResult<()>> {
        self.pages.closes.fetch_add(1, Ordering::SeqCst);
        self.pages.operations.seal();
        let exit = self.pages.operations.exited();
        Box::pin(async move {
            match exit.await {
                novarocks_spi::connector::read_stack::ConnectorSourceExitProof::Exited => Ok(()),
                novarocks_spi::connector::read_stack::ConnectorSourceExitProof::Unresolved => {
                    futures::future::pending().await
                }
            }
        })
    }
}
struct Source {
    pages: Arc<Pages>,
    claims: AtomicUsize,
    construction: Arc<dyn InputConstructionFactory>,
}
impl ScanStreamSource for Source {
    fn claim(
        &self,
        budget: ConnectorPollBudget,
        _: Option<crate::runtime::profile::RuntimeProfile>,
        construction: Arc<dyn InputConstructionFactory>,
    ) -> ExecutionResult<ScanOutputStream> {
        assert!(
            Arc::ptr_eq(&construction, &self.construction),
            "the original typed construction owner reaches the connector"
        );
        assert_eq!(
            self.claims.fetch_add(1, Ordering::SeqCst),
            0,
            "one actual source stream"
        );
        Ok(Box::pin(PageStream {
            pages: self.pages.clone(),
            budget,
            spending: None,
        }))
    }
}
struct BoundOp(Arc<Source>);
impl ScanOp for BoundOp {
    fn stream_source(&self) -> Arc<dyn ScanStreamSource> {
        self.0.clone()
    }
    fn terminate(&self) -> ExecutionResult<()> {
        self.0.pages.stops.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn on_nonempty_chunk_consumed(&self) {
        self.0.pages.consumed.fetch_add(1, Ordering::SeqCst);
    }
    fn profile_name(&self) -> Option<String> {
        Some("OrderedSourceComponent".into())
    }
}
struct SourceCase {
    consumer: Consumer,
    source: Arc<Source>,
    _factory: StreamScanSourceFactory,
    operator: OperatorInstance,
    turn: TurnWork,
}
impl SourceCase {
    fn new(consumer: Consumer, limit: Option<usize>) -> Self {
        let source = Arc::new(Source {
            pages: Pages::new(),
            claims: AtomicUsize::new(0),
            construction: consumer._creation.clone(),
        });
        let residuals = consumer
            .fixture
            .program
            .graph()
            .nodes()
            .iter()
            .enumerate()
            .filter_map(|(node, graph_node)| match graph_node.kind() {
                ProgramNodeKind::Scan { residuals, .. } if !residuals.is_empty() => {
                    Some(ProgramNodeId::new(node))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(
            residuals.len() <= 1,
            "one actual checked Scan residual owner"
        );
        let residual = residuals.first().map(|&node| {
            crate::exec::operators::compiled_expression::CompiledFilterProcessorFactory::try_new(
                consumer.fixture.program.clone(),
                novarocks_local_program::ProgramExpressionRootSite::Node {
                    node,
                    role: novarocks_local_program::ProgramNodeExpressionRole::ScanResidual {
                        predicate: 0,
                    },
                },
                consumer.state.error_state(),
            )
            .unwrap()
        });
        let factory = StreamScanSourceFactory::new_compiled_ordered(
            7,
            Arc::new(BoundOp(source.clone())),
            None,
            consumer._factory.clone(),
            residual,
            limit,
            consumer._host.clone(),
            consumer._control.clone(),
        );
        let mut operator = factory.create(1, 0);
        let turn = TurnWork::new(std::num::NonZeroU64::MIN, DEFAULT_TURN_WORK_UNITS);
        operator.bind_poll_budget(turn.poll_budget());
        operator.set_mem_tracker(consumer.tracker.clone());
        operator.set_construction_factory(consumer._creation.clone());
        operator.set_task_event_sink(consumer.effects.clone());
        operator.prepare().unwrap();
        let tracker = consumer.tracker.clone();
        let runtime_error = consumer.state.error_state();
        Self {
            consumer,
            source,
            _factory: factory,
            operator: OperatorInstance::new(0, operator, Some(tracker), runtime_error),
            turn,
        }
    }
    fn bind(&mut self) {
        self.consumer.tracker.install_limit_once(64 << 20).unwrap();
        self.operator
            .bind_runtime_state(&self.consumer.state)
            .unwrap();
    }
    fn push_original(&self) {
        self.source.pages.push(self.consumer.fixture.input.clone());
    }
    fn poll_exit(&mut self) {
        use crate::exec::pipeline::operator::ExitProgress;
        let observation = self.operator.observe_exit();
        assert!(observation.failure.is_none());
        assert!(observation.unresolved_exit.is_none());
        assert!(
            !matches!(observation.progress, ExitProgress::Unresolved(_)),
            "the scripted connector has no unresolved exit"
        );
    }
    fn pull(&mut self) -> ExecutionResult<Option<Chunk>> {
        self.turn.begin_turn(DEFAULT_TURN_WORK_UNITS)?;
        let control = self.consumer.state.error_state();
        let step = self.operator.drive(&mut DriverStep::new(
            &control,
            &mut self.turn,
            &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
        ))?;
        if self.operator.is_finished() {
            self.poll_exit();
        }
        Ok(step.output)
    }
    fn finish(mut self) {
        self.poll_exit();
        assert_eq!(self.source.claims.load(Ordering::SeqCst), 1);
        assert_eq!(self.source.pages.closes.load(Ordering::SeqCst), 1);
        assert!(self.source.pages.operations.is_exited());
        assert!(self.operator.pending_finish().is_none());
        let tracker = self.consumer.tracker.clone();
        drop(self);
        assert_eq!(tracker.current(), 0);
    }
}

#[test]
fn compiled_ordered_source_updated_rows_effect_and_original_stream_exit_are_real() {
    let consumer = Consumer::new(Bind::Live);
    let query = LiteralQuery::new();
    consumer.script([rf::LivePollOutcome::Updated {
        snapshot: consumer.snapshot(1, query.clone()),
        terminal: Some(rf::LiveTerminal::Completed),
    }]);
    let mut source = SourceCase::new(consumer, None);
    let stop_observed = Arc::new(AtomicBool::new(false));
    let stopped = stop_observed.clone();
    let ticket = source
        .source
        .pages
        .operations
        .admit(Arc::new(move || {
            stopped.store(true, Ordering::SeqCst);
        }))
        .unwrap();
    source.push_original();
    source.source.pages.end();
    source.bind();
    let output = source.pull().unwrap().unwrap();
    rows(&output);
    assert_eq!(query.calls.load(Ordering::SeqCst), 8);
    assert_eq!(
        output.source_authority().owner_identity(),
        source
            .consumer
            .fixture
            .input
            .source_authority()
            .owner_identity()
    );
    assert_eq!(source.source.pages.consumed.load(Ordering::SeqCst), 1);
    let effects = source.consumer.effects.0.lock().unwrap();
    assert_eq!(effects.len(), 1);
    assert_eq!((effects[0].input_rows(), effects[0].output_rows()), (8, 4));
    assert_eq!(effects[0].logical_version(), rf::LogicalVersion::new(1));
    drop(effects);
    drop(output);
    assert!(source.pull().unwrap().is_none());
    assert!(source.operator.is_finished());
    assert!(matches!(
        source.operator.pending_finish(),
        Some(FinishWatch::Notify(_))
    ));
    assert!(stop_observed.load(Ordering::SeqCst));
    assert!(!source.source.pages.operations.is_exited());
    ticket.end(novarocks_spi::connector::read_stack::ConnectorOperationExit::Exited);
    source.poll_exit();
    assert!(source.operator.pending_finish().is_none());
    source.finish();
}

#[test]
fn compiled_ordered_source_empty_survivors_continue_to_the_next_actual_page() {
    let consumer = Consumer::new(Bind::Live);
    let query = LiteralQuery::new();
    consumer.script([
        rf::LivePollOutcome::Updated {
            snapshot: consumer.snapshot(1, query.clone()),
            terminal: None,
        },
        rf::LivePollOutcome::Idle {
            latest_version: Some(rf::LogicalVersion::new(1)),
            terminal: Some(rf::LiveTerminal::Completed),
        },
    ]);
    let mut source = SourceCase::new(consumer, None);
    // The original first row is 'c' (rejected); the next is 'a' (kept).
    // Both pages use the original observed typed slice constructor.
    for offset in [0, 1] {
        source.source.pages.push(super::super::slice(
            &source.consumer.fixture.input,
            offset,
            1,
            source.consumer._creation.as_ref(),
        ));
    }
    source.bind();
    let output = source.pull().unwrap().unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(
        output.columns()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "a"
    );
    assert_eq!(
        output.columns()[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        10
    );
    assert_eq!(source.source.pages.polls.load(Ordering::SeqCst), 2);
    assert_eq!(query.calls.load(Ordering::SeqCst), 2);
    assert!(
        !source.operator.is_finished(),
        "an empty filtered page was not EOS"
    );
    assert_eq!(source.source.pages.consumed.load(Ordering::SeqCst), 1);
    let effects = source.consumer.effects.0.lock().unwrap();
    assert_eq!(
        effects
            .iter()
            .map(|e| (e.input_rows(), e.output_rows()))
            .collect::<Vec<_>>(),
        vec![(1, 0), (1, 1)]
    );
    drop(effects);
    drop(output);
    source.source.pages.end();
    assert!(source.pull().unwrap().is_none());
    source.finish();
}

#[test]
fn compiled_ordered_source_limit_slices_after_filter_and_keeps_the_last_values_buffer() {
    let consumer = Consumer::new(Bind::Live);
    let query = LiteralQuery::new();
    consumer.script([rf::LivePollOutcome::Updated {
        snapshot: consumer.snapshot(1, query),
        terminal: Some(rf::LiveTerminal::Completed),
    }]);
    let mut source = SourceCase::new(consumer, Some(2));
    source.push_original();
    source.bind();
    let original = Arc::downgrade(&source.consumer.fixture.input.columns()[0]);
    let mut output = source.pull().unwrap().unwrap();
    assert_eq!(output.len(), 2);
    assert_eq!(
        output.columns()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("a"), None]
    );
    assert_eq!(
        output.columns()[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(10), Some(7)]
    );
    assert!(
        output.take_memory_lease().is_none(),
        "a sliced AdditionalBuffers output is not a whole-batch lease"
    );
    let effects = source.consumer.effects.0.lock().unwrap();
    assert_eq!(effects.len(), 1);
    assert_eq!(
        (effects[0].input_rows(), effects[0].output_rows()),
        (8, 4),
        "the effect belongs to RF before LIMIT"
    );
    drop(effects);
    assert!(source.operator.is_finished());
    assert!(source.operator.pending_finish().is_none());
    assert_eq!(source.source.pages.closes.load(Ordering::SeqCst), 1);
    assert!(source.pull().unwrap().is_none());
    let data = output.columns()[0].to_data();
    let last = data.buffers()[1].clone();
    drop(data);
    drop(output);
    let tracker = source.consumer.tracker.clone();
    drop(source);
    assert!(original.upgrade().is_some());
    assert!(
        tracker.current() > 0,
        "the actual handoff/slice keeps input and additional stock"
    );
    std::thread::spawn(move || {
        drop(last);
        assert!(original.upgrade().is_none());
    })
    .join()
    .unwrap();
    assert_eq!(tracker.current(), 0);
}

#[test]
fn compiled_ordered_source_idle_passes_original_whole_input_without_query_or_new_stock() {
    let consumer = Consumer::new(Bind::Live);
    consumer.script([rf::LivePollOutcome::Idle {
        latest_version: None,
        terminal: Some(rf::LiveTerminal::CompletedWithoutArtifact),
    }]);
    let mut source = SourceCase::new(consumer, None);
    source.push_original();
    source.source.pages.end();
    source.bind();
    let before = source.consumer.tracker.current();
    let output = source.pull().unwrap().unwrap();
    assert!(Arc::ptr_eq(
        &output.columns()[0],
        &source.consumer.fixture.input.columns()[0]
    ));
    assert_eq!(
        output.source_authority().owner_identity(),
        source
            .consumer
            .fixture
            .input
            .source_authority()
            .owner_identity()
    );
    assert_eq!(
        source.consumer.tracker.current(),
        before,
        "the source handoff neither instantiates the idle key nor charges a second whole input"
    );
    assert!(source.consumer.effects.0.lock().unwrap().is_empty());
    assert_eq!(
        *source.consumer.session.cursor.observed.lock().unwrap(),
        vec![None]
    );
    drop(output);
    assert!(source.pull().unwrap().is_none());
    source.finish();
}

#[test]
fn compiled_ordered_source_query_failure_keeps_the_first_cause_and_closes_real_operations() {
    for cause in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        let consumer = Consumer::new(Bind::Live);
        let query = Arc::new(LiteralQuery {
            calls: AtomicUsize::new(0),
            failure: Some((2, cause.clone())),
        });
        consumer.script([rf::LivePollOutcome::Updated {
            snapshot: consumer.snapshot(1, query.clone()),
            terminal: None,
        }]);
        let mut source = SourceCase::new(consumer, None);
        source.push_original();
        source.push_original();
        let stop_observed = Arc::new(AtomicBool::new(false));
        let stopped = stop_observed.clone();
        let ticket = source
            .source
            .pages
            .operations
            .admit(Arc::new(move || {
                stopped.store(true, Ordering::SeqCst);
            }))
            .unwrap();
        source.bind();
        let error = source.pull().unwrap_err();
        assert_eq!(error.cause(), &ExecutionFailureCause::Kernel(cause.clone()));
        assert_eq!(query.calls.load(Ordering::SeqCst), 3);
        assert!(source.consumer.effects.0.lock().unwrap().is_empty());
        source.consumer.state.error_state().set_error(
            if cause == KernelFailure::DeadlineExceeded {
                KernelFailure::Cancelled
            } else {
                KernelFailure::DeadlineExceeded
            },
        );
        assert_eq!(source.pull().unwrap_err().cause(), error.cause());
        assert_eq!(
            source
                .consumer
                .session
                .cursor
                .observed
                .lock()
                .unwrap()
                .len(),
            1
        );
        source.operator.cancel();
        assert_eq!(source.source.pages.stops.load(Ordering::SeqCst), 1);
        assert!(stop_observed.load(Ordering::SeqCst));
        assert!(matches!(
            source.operator.pending_finish(),
            Some(FinishWatch::Notify(_))
        ));
        ticket.end(novarocks_spi::connector::read_stack::ConnectorOperationExit::Exited);
        source.poll_exit();
        assert!(source.operator.pending_finish().is_none());
        source.finish();
    }
}

#[test]
fn compiled_ordered_source_bind_refusal_never_claims_a_stream_and_refunds_its_prefix() {
    for resource in [false, true] {
        let consumer = Consumer::new(if resource { Bind::Live } else { Bind::Blocking });
        let mut source = SourceCase::new(consumer, None);
        let before = source.consumer.tracker.current();
        if resource {
            source
                .consumer
                .tracker
                .install_limit_once(before.checked_add(1).unwrap())
                .unwrap();
        } else {
            source
                .consumer
                .tracker
                .install_limit_once(64 << 20)
                .unwrap();
        }
        let error = source
            .operator
            .bind_runtime_state(&source.consumer.state)
            .unwrap_err();
        if resource {
            assert_eq!(
                error.cause(),
                &ExecutionFailureCause::Kernel(KernelFailure::ResourceExhausted)
            );
            assert!(source.consumer.session.requests.lock().unwrap().is_empty());
        } else {
            contract_error(
                &error,
                "compiled ordered Scan received a non-live subscription",
            );
        }
        if resource {
            assert_eq!(source.consumer.tracker.current(), before);
        }
        source
            .consumer
            .state
            .error_state()
            .set_error(KernelFailure::DeadlineExceeded);
        assert_eq!(
            source
                .operator
                .bind_runtime_state(&source.consumer.state)
                .unwrap_err()
                .cause(),
            error.cause()
        );
        assert_eq!(source.source.claims.load(Ordering::SeqCst), 0);
        assert_eq!(source.source.pages.polls.load(Ordering::SeqCst), 0);
        assert!(source.consumer.effects.0.lock().unwrap().is_empty());
        source.operator.retire_data().unwrap();
        source.poll_exit();
        assert!(source.operator.is_finished());
        assert!(source.operator.pending_finish().is_none());
        assert_eq!(
            source.source.pages.closes.load(Ordering::SeqCst),
            0,
            "no stream existed to close"
        );
        let tracker = source.consumer.tracker.clone();
        drop(source);
        assert_eq!(tracker.current(), 0);
    }
}

#[test]
fn compiled_ordered_source_actual_residual_finishes_before_live_key_evaluation() {
    fn residual(builder: &mut FragmentBuilder, scan: NodeId, values: &[ValueId]) -> Box<[ExprId]> {
        let input = builder
            .add_expression(
                scan,
                FunctionValueType::new(DataType::Int64, true),
                ExprKind::Value(values[1]),
            )
            .unwrap();
        let seven = builder
            .add_expression(
                scan,
                FunctionValueType::new(DataType::Int64, false),
                ExprKind::Literal(LiteralValue::Int64(7)),
            )
            .unwrap();
        Box::from([builder
            .add_expression(
                scan,
                FunctionValueType::new(DataType::Boolean, true),
                ExprKind::Binary {
                    left: input,
                    op: novarocks_physical_plan::BinaryOperator::Gt,
                    right: seven,
                    decimal_overflow_policy:
                        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                    allow_throw_exception: None,
                },
            )
            .unwrap()])
    }
    let consumer = Consumer::with_fixture(
        Bind::Live,
        super::super::ordered_fixture_with_residual(Some(residual)),
    );
    let query = LiteralQuery::new();
    consumer.script([rf::LivePollOutcome::Updated {
        snapshot: consumer.snapshot(1, query.clone()),
        terminal: Some(rf::LiveTerminal::Completed),
    }]);
    let mut source = SourceCase::new(consumer, None);
    source.push_original();
    source.source.pages.end();
    source.bind();
    let output = source.pull().unwrap().unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(
        output.columns()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "a"
    );
    assert_eq!(
        output.columns()[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        10
    );
    assert_eq!(
        query.calls.load(Ordering::SeqCst),
        4,
        "only the original residual's c30/a10/b20/d40 survivors reach the artifact"
    );
    let effects = source.consumer.effects.0.lock().unwrap();
    assert_eq!(effects.len(), 1);
    assert_eq!((effects[0].input_rows(), effects[0].output_rows()), (4, 1));
    drop(effects);
    drop(output);
    assert!(source.pull().unwrap().is_none());
    source.finish();
}

#[test]
fn compiled_ordered_source_empty_actual_residual_never_polls_an_invalid_live_snapshot() {
    fn residual(builder: &mut FragmentBuilder, scan: NodeId, _: &[ValueId]) -> Box<[ExprId]> {
        Box::from([builder
            .add_expression(
                scan,
                FunctionValueType::new(DataType::Boolean, false),
                ExprKind::Literal(LiteralValue::Boolean(false)),
            )
            .unwrap()])
    }
    let consumer = Consumer::with_fixture(
        Bind::Live,
        super::super::ordered_fixture_with_residual(Some(residual)),
    );
    let query = LiteralQuery::new();
    let invalid = Arc::new(rf::RuntimeFilterSnapshot::new(
        consumer.session.expected.binding_id(),
        rf::LogicalVersion::new(1),
        [0; 32],
        query.clone(),
    ));
    consumer.script([rf::LivePollOutcome::Updated {
        snapshot: invalid,
        terminal: None,
    }]);
    let mut source = SourceCase::new(consumer, None);
    source.push_original();
    source.source.pages.end();
    source.bind();
    assert!(source.pull().unwrap().is_none());
    assert!(source.operator.is_finished());
    assert_eq!(query.calls.load(Ordering::SeqCst), 0);
    assert!(
        source
            .consumer
            .session
            .cursor
            .observed
            .lock()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        source.consumer.session.cursor.script.lock().unwrap().len(),
        1,
        "the invalid snapshot was never requested"
    );
    assert!(source.consumer.effects.0.lock().unwrap().is_empty());
    source.finish();
}

#[test]
fn compiled_ordered_source_required_residual_row_error_precedes_invalid_live_snapshot() {
    use novarocks_type_contract::{
        SemanticParameterId, SemanticParameterKey, SemanticParameterRef, SemanticParameterValue,
        SemanticParameters,
    };
    fn allow_ref() -> SemanticParameterRef {
        SemanticParameterRef {
            id: SemanticParameterId::new(73),
            expected_key: SemanticParameterKey::AllowThrowException,
        }
    }
    fn residual(builder: &mut FragmentBuilder, scan: NodeId, values: &[ValueId]) -> Box<[ExprId]> {
        let input = builder
            .add_expression(
                scan,
                FunctionValueType::new(DataType::Int64, true),
                ExprKind::Value(values[1]),
            )
            .unwrap();
        let zero = builder
            .add_expression(
                scan,
                FunctionValueType::new(DataType::Int64, false),
                ExprKind::Literal(LiteralValue::Int64(0)),
            )
            .unwrap();
        let modulo = builder
            .add_expression(
                scan,
                FunctionValueType::new(DataType::Int64, true),
                ExprKind::Binary {
                    left: input,
                    op: novarocks_physical_plan::BinaryOperator::Modulo,
                    right: zero,
                    decimal_overflow_policy:
                        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                    allow_throw_exception: Some(allow_ref()),
                },
            )
            .unwrap();
        Box::from([builder
            .add_expression(
                scan,
                FunctionValueType::new(DataType::Boolean, true),
                ExprKind::Binary {
                    left: modulo,
                    op: novarocks_physical_plan::BinaryOperator::Gt,
                    right: zero,
                    decimal_overflow_policy:
                        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                    allow_throw_exception: None,
                },
            )
            .unwrap()])
    }
    let fixture = super::super::ordered_fixture_with_parameters(
        Some(residual),
        SemanticParameters::try_new([(
            allow_ref().id,
            SemanticParameterValue::AllowThrowException(true),
        )])
        .unwrap(),
    );
    let consumer = Consumer::with_fixture(Bind::Live, fixture);
    let query = LiteralQuery::new();
    consumer.script([rf::LivePollOutcome::Updated {
        snapshot: Arc::new(rf::RuntimeFilterSnapshot::new(
            consumer.session.expected.binding_id(),
            rf::LogicalVersion::new(1),
            [0; 32],
            query.clone(),
        )),
        terminal: None,
    }]);
    let mut source = SourceCase::new(consumer, None);
    source.push_original();
    let ticket = source
        .source
        .pages
        .operations
        .admit(Arc::new(|| {}))
        .unwrap();
    source.bind();
    let error = source.pull().unwrap_err();
    let ExecutionFailureCause::RequiredRow(required) = error.cause() else {
        panic!("the original required row error was replaced: {error:?}");
    };
    let scans = source
        .consumer
        .fixture
        .program
        .graph()
        .nodes()
        .iter()
        .enumerate()
        .filter_map(|(node, definition)| {
            matches!(definition.kind(), ProgramNodeKind::Scan { .. })
                .then_some(ProgramNodeId::new(node))
        })
        .collect::<Vec<_>>();
    assert_eq!(scans.len(), 1);
    assert_eq!(
        required.root(),
        novarocks_local_program::ProgramExpressionRootSite::Node {
            node: scans[0],
            role: novarocks_local_program::ProgramNodeExpressionRole::ScanResidual { predicate: 0 },
        }
    );
    assert_eq!(required.batch_row(), 0);
    assert_eq!(required.error().selected_ordinal(), 0);
    // Signed modulo by zero retains its exact Data reason before any snapshot I/O.
    assert_eq!(required.error().message(), "Division by zero");
    assert_eq!(
        required.error().reason(),
        novarocks_functions::DataFailureReason::DivisionByZero
    );
    assert_eq!(query.calls.load(Ordering::SeqCst), 0);
    assert!(
        source
            .consumer
            .session
            .cursor
            .observed
            .lock()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        source.consumer.session.cursor.script.lock().unwrap().len(),
        1,
        "the invalid live snapshot was never polled"
    );
    assert!(source.consumer.effects.0.lock().unwrap().is_empty());
    source.operator.retire_data().unwrap();
    source.poll_exit();
    assert!(matches!(
        source.operator.pending_finish(),
        Some(FinishWatch::Notify(_))
    ));
    ticket.end(novarocks_spi::connector::read_stack::ConnectorOperationExit::Exited);
    source.poll_exit();
    assert!(source.operator.pending_finish().is_none());
    source.finish();
}

#[path = "compiled_ordered_pipeline_tests.rs"]
mod pipeline_tests;

#[path = "compiled_ordered_membership_source_tests.rs"]
mod membership_tests;

#[path = "compiled_ordered_page_source_tests.rs"]
mod page_source_tests;

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
