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

//! Checked-program ordered consumer components. The local artifact declares
//! literal membership only; it is not a Worker ordering oracle. The real typed
//! source, tracked factory and original filter creator execute synchronously.
use super::*;
use crate::exec::operators::runtime_filter::{
    CompiledOrderedScanEvaluator, CompiledOrderedScanFactory,
};
use crate::runtime::task::io::{TaskEvent, TaskEventSink};
use crate::runtime_filter::evaluator::{
    RuntimeFilterArtifactQuery, RuntimeFilterArtifactQueryError, RuntimeFilterControlledQueryError,
    RuntimeFilterControlledScalarQuery, RuntimeFilterScalarRef,
};
use novarocks_functions::KernelEvaluationControl;
use novarocks_spi::connector::ConnectorScalarValue;
use std::{
    collections::VecDeque,
    sync::atomic::{AtomicUsize, Ordering},
};

struct LiteralQuery {
    calls: AtomicUsize,
    failure: Option<(usize, KernelFailure)>,
}
impl LiteralQuery {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            failure: None,
        })
    }
    fn check(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), RuntimeFilterControlledQueryError> {
        control.checkpoint(1)?;
        let row = self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some((at, cause)) = &self.failure
            && row == *at
        {
            return Err(cause.clone().into());
        }
        Ok(())
    }
}
impl RuntimeFilterArtifactQuery for LiteralQuery {
    fn data_type(&self) -> &DataType {
        &DataType::Utf8
    }
    fn controlled_scalar_query(&self) -> Option<&dyn RuntimeFilterControlledScalarQuery> {
        Some(self)
    }
    fn matches_null(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
        panic!("uncontrolled NULL query")
    }
    fn has_non_null_matches(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
        panic!("uncontrolled existence query")
    }
    fn non_null_value_may_match(
        &self,
        _: RuntimeFilterScalarRef<'_>,
    ) -> Result<bool, RuntimeFilterArtifactQueryError> {
        panic!("uncontrolled scalar query")
    }
    fn non_null_range_may_match(
        &self,
        _: &ConnectorScalarValue,
        _: &ConnectorScalarValue,
    ) -> Result<bool, RuntimeFilterArtifactQueryError> {
        panic!("uncontrolled range query")
    }
}
impl RuntimeFilterControlledScalarQuery for LiteralQuery {
    fn matches_null_controlled(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<bool, RuntimeFilterControlledQueryError> {
        self.check(control)?;
        Ok(true)
    }
    fn non_null_value_may_match_controlled(
        &self,
        value: RuntimeFilterScalarRef<'_>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<bool, RuntimeFilterControlledQueryError> {
        self.check(control)?;
        let RuntimeFilterScalarRef::Utf8(value) = value else {
            panic!("declared Utf8 artifact")
        };
        Ok(value == "a")
    }
}
struct Cursor {
    script: Mutex<VecDeque<rf::LivePollOutcome>>,
    observed: Mutex<Vec<Option<rf::LogicalVersion>>>,
}
impl rf::NonBlockingLiveSubscription for Cursor {
    fn snapshot(&self) -> Option<Arc<rf::RuntimeFilterSnapshot>> {
        panic!("the live driver must poll its explicit cursor")
    }
    fn poll_after(&self, observed: Option<rf::LogicalVersion>) -> rf::LivePollOutcome {
        self.observed.lock().unwrap().push(observed);
        self.script
            .lock()
            .unwrap()
            .pop_front()
            .expect("missing explicit live outcome")
    }
}
struct Blocking;
impl rf::BlockingSnapshotSubscription for Blocking {
    fn try_outcome(&self) -> Option<rf::SnapshotAcquireOutcome> {
        panic!("invalid subscription must not be queried")
    }
    fn outcome_observable(&self) -> Arc<crate::runtime::observable::Observable> {
        panic!("invalid subscription must not be observed")
    }
    fn record_consumer_outcome(&self, _: &rf::SnapshotAcquireOutcome) {
        panic!("invalid subscription must not be used")
    }
    fn snapshot(&self) -> Option<Arc<rf::RuntimeFilterSnapshot>> {
        panic!("invalid subscription must not be queried")
    }
}
#[derive(Clone, Copy)]
enum Bind {
    Live,
    Unavailable,
    Closed,
    Blocking,
}
struct ScanSession {
    expected: rf::RuntimeFilterConsumerContract,
    requests: Mutex<Vec<rf::RuntimeFilterSubscriptionRequest>>,
    cursor: Arc<Cursor>,
    mode: Bind,
}
impl rf::RuntimeFilterSession for ScanSession {
    fn open_producer(
        &self,
        _: rf::RuntimeFilterProducerOpenRequest,
    ) -> Result<
        rf::RuntimeFilterBindOutcome<rf::SharedRuntimeFilterProducer>,
        rf::RuntimeFilterContractViolation,
    > {
        panic!("consumer-only session")
    }
    fn open_final_domain_completion(
        &self,
        _: rf::RuntimeFilterFinalDomainOpenRequest,
    ) -> Result<
        rf::RuntimeFilterBindOutcome<rf::SharedRuntimeFilterFinalDomainCompletion>,
        rf::RuntimeFilterContractViolation,
    > {
        panic!("consumer-only session")
    }
    fn subscribe(
        &self,
        request: rf::RuntimeFilterSubscriptionRequest,
    ) -> Result<
        rf::RuntimeFilterBindOutcome<rf::RuntimeFilterSubscriptionPin>,
        rf::RuntimeFilterContractViolation,
    > {
        assert_eq!(
            request.contract(),
            &self.expected,
            "the checked Scan authors the exact binding/digests"
        );
        self.requests.lock().unwrap().push(request);
        match self.mode {
            Bind::Live => Ok(rf::RuntimeFilterBindOutcome::Bound(
                rf::RuntimeFilterSubscriptionPin::Live(self.cursor.clone()),
            )),
            Bind::Unavailable => Ok(rf::RuntimeFilterBindOutcome::Unavailable(
                rf::UnavailableReason::ResourceLimit,
            )),
            Bind::Closed => Err(rf::RuntimeFilterContractViolation::new(
                rf::RuntimeFilterContractViolationKind::SessionClosed,
                "fixture session is closed",
            )),
            Bind::Blocking => Ok(rf::RuntimeFilterBindOutcome::Bound(
                rf::RuntimeFilterSubscriptionPin::Blocking(Arc::new(Blocking)),
            )),
        }
    }
}
#[derive(Default)]
struct Effects(Mutex<Vec<rf::RuntimeFilterRowEffect>>);
impl TaskEventSink for Effects {
    fn record(&self, event: TaskEvent) {
        let TaskEvent::RuntimeFilterRowEffect(effect) = event else {
            panic!("unexpected component event")
        };
        self.0.lock().unwrap().push(effect);
    }
}
struct Consumer {
    fixture: Fixture,
    tracker: Arc<MemTracker>,
    state: RuntimeState,
    _creation: Arc<dyn InputConstructionFactory>,
    _host: rf::OrderedTopNAllocationHost,
    _control: RuntimeKernelControl,
    _factory: CompiledOrderedScanFactory,
    evaluator: CompiledOrderedScanEvaluator,
    session: Arc<ScanSession>,
    effects: Arc<Effects>,
}
impl Consumer {
    fn new(mode: Bind) -> Self {
        Self::with_fixture(mode, ordered_fixture())
    }
    fn with_fixture(mode: Bind, mut fixture: Fixture) -> Self {
        let tracker = MemTracker::new_root("compiled-ordered-scan-real-owner");
        fixture.input.try_transfer_to(&tracker).unwrap();
        let state = state(tracker.clone());
        let creation = construction(&tracker, &state);
        let producer = session(&fixture);
        let scans = fixture
            .program
            .graph()
            .nodes()
            .iter()
            .enumerate()
            .filter_map(|(id, node)| {
                matches!(node.kind(), ProgramNodeKind::Scan { .. })
                    .then_some(ProgramNodeId::new(id))
            })
            .collect::<Vec<_>>();
        assert_eq!(
            scans.len(),
            1,
            "the original checked fixture has one actual Scan"
        );
        let scan = scans[0];
        let ProgramNodeKind::Scan {
            runtime_filters, ..
        } = fixture.program.graph().nodes()[scan.index()].kind()
        else {
            unreachable!()
        };
        let mut ordered = runtime_filters.iter().filter(|binding| {
            matches!(
                binding.consumer.contract(),
                novarocks_local_program::StaticFilterContract::Ordered { .. }
            )
        });
        let spec = &ordered
            .next()
            .expect("one original ordered binding")
            .consumer;
        assert!(
            ordered.next().is_none(),
            "exactly one original ordered binding"
        );
        let expected = rf::RuntimeFilterConsumerContract::ordered_live(
            rf::RuntimeFilterBindingId::new(spec.binding_id()),
            rf::RuntimeFilterChannelId::new(spec.channel_id()),
            rf::RuntimeFilterLateApplyGranularity::Batch,
            producer.expected.contract().clone(),
        )
        .unwrap();
        drop(producer);
        let session = Arc::new(ScanSession {
            expected,
            mode,
            requests: Mutex::new(vec![]),
            cursor: Arc::new(Cursor {
                script: Mutex::new(VecDeque::new()),
                observed: Mutex::new(vec![]),
            }),
        });
        let host = rf::OrderedTopNAllocationHost::new(tracker.clone());
        let mut control = RuntimeKernelControl::new(state.error_state());
        control.bind_mem_tracker(tracker.clone());
        let factory = CompiledOrderedScanFactory::try_new(
            fixture.program.clone(),
            scan,
            session.clone(),
            &host,
            &control,
        )
        .unwrap();
        let mut evaluator = factory.for_driver();
        evaluator.bind_mem_tracker(tracker.clone());
        evaluator.bind_construction_factory(creation.clone());
        Self {
            fixture,
            tracker,
            state,
            _creation: creation,
            _host: host,
            _control: control,
            _factory: factory,
            evaluator,
            session,
            effects: Arc::new(Effects::default()),
        }
    }
    fn snapshot(&self, version: u64, query: Arc<LiteralQuery>) -> Arc<rf::RuntimeFilterSnapshot> {
        let rf::RuntimeFilterExecutionContract::Ordered(order) = self.session.expected.contract()
        else {
            unreachable!()
        };
        Arc::new(rf::RuntimeFilterSnapshot::new(
            self.session.expected.binding_id(),
            rf::LogicalVersion::new(version),
            order.digest(),
            query,
        ))
    }
    fn script(&self, outcomes: impl IntoIterator<Item = rf::LivePollOutcome>) {
        self.session.cursor.script.lock().unwrap().extend(outcomes);
    }
    fn bind(&mut self) {
        self.tracker.install_limit_once(64 << 20).unwrap();
        self.evaluator.bind(&self.state).unwrap();
    }
    fn apply(&mut self) -> crate::runtime::task::ExecutionResult<Option<Chunk>> {
        let sink: Arc<dyn TaskEventSink> = self.effects.clone();
        self.evaluator.apply(self.fixture.input.clone(), &sink)
    }
    fn finish(self) {
        let tracker = self.tracker.clone();
        drop(self);
        assert_eq!(tracker.current(), 0);
    }
}
fn rows(output: &Chunk) {
    assert_eq!(output.len(), 4);
    assert_eq!(
        output.columns()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("a"), None, None, Some("a")]
    );
    assert_eq!(
        output.columns()[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(10), Some(7), Some(2), Some(1)]
    );
}
fn contract_error(error: &crate::runtime::task::ExecutionFailure, detail: &str) {
    assert!(
        matches!(error.cause(), ExecutionFailureCause::RuntimeFilter(rf::RuntimeFilterFailure::Contract(cause)) if cause.kind() == rf::RuntimeFilterContractViolationKind::ContractMismatch && cause.detail() == detail),
        "{error:?}"
    );
}

#[test]
fn compiled_ordered_scan_idle_retains_original_input_without_query_or_new_stock() {
    let mut c = Consumer::new(Bind::Live);
    c.script([
        rf::LivePollOutcome::Idle {
            latest_version: None,
            terminal: None,
        },
        rf::LivePollOutcome::Idle {
            latest_version: None,
            terminal: None,
        },
    ]);
    c.bind();
    let before = c.tracker.current();
    for _ in 0..2 {
        let output = c.apply().unwrap().unwrap();
        assert_eq!(
            output.source_authority().owner_identity(),
            c.fixture.input.source_authority().owner_identity()
        );
        assert!(Arc::ptr_eq(
            &output.columns()[0],
            &c.fixture.input.columns()[0]
        ));
        assert_eq!(
            c.tracker.current(),
            before,
            "an idle binding creates neither key instance nor mask stock"
        );
        drop(output);
    }
    assert!(c.effects.0.lock().unwrap().is_empty());
    assert_eq!(c.session.requests.lock().unwrap().len(), 1);
    assert_eq!(*c.session.cursor.observed.lock().unwrap(), vec![None, None]);
    c.finish();
}

#[test]
fn compiled_ordered_scan_updated_mask_effect_and_last_buffer_keep_actual_input_stock() {
    let mut c = Consumer::new(Bind::Live);
    let query = LiteralQuery::new();
    c.script([rf::LivePollOutcome::Updated {
        snapshot: c.snapshot(3, query.clone()),
        terminal: Some(rf::LiveTerminal::Completed),
    }]);
    c.bind();
    let original = Arc::downgrade(&c.fixture.input.columns()[0]);
    let output = c.apply().unwrap().unwrap();
    rows(&output);
    assert_eq!(query.calls.load(Ordering::SeqCst), 8);
    let effect = c.effects.0.lock().unwrap()[0];
    assert_eq!(effect.binding_id(), c.session.expected.binding_id());
    assert_eq!(effect.logical_version(), rf::LogicalVersion::new(3));
    assert_eq!((effect.input_rows(), effect.output_rows()), (8, 4));
    assert_eq!(
        output.source_authority().owner_identity(),
        c.fixture.input.source_authority().owner_identity()
    );
    let data = output.columns()[0].to_data();
    let last = data.buffers()[1].clone();
    drop(data);
    drop(output);
    let tracker = c.tracker.clone();
    drop(c);
    assert!(original.upgrade().is_some());
    assert!(tracker.current() > 0);
    std::thread::spawn(move || {
        drop(last);
        assert!(original.upgrade().is_none());
    })
    .join()
    .unwrap();
    assert_eq!(tracker.current(), 0);
}

#[test]
fn compiled_ordered_scan_equal_or_stale_terminal_keeps_last_mask_and_stops_polling() {
    for incoming in [2, 3] {
        let mut c = Consumer::new(Bind::Live);
        let original = LiteralQuery::new();
        let ignored = LiteralQuery::new();
        c.script([
            rf::LivePollOutcome::Updated {
                snapshot: c.snapshot(3, original.clone()),
                terminal: None,
            },
            rf::LivePollOutcome::Updated {
                snapshot: c.snapshot(incoming, ignored.clone()),
                terminal: Some(rf::LiveTerminal::Completed),
            },
        ]);
        c.bind();
        for _ in 0..3 {
            rows(&c.apply().unwrap().unwrap());
        }
        assert_eq!(
            *c.session.cursor.observed.lock().unwrap(),
            vec![None, Some(rf::LogicalVersion::new(3))]
        );
        assert_eq!(original.calls.load(Ordering::SeqCst), 24);
        assert_eq!(ignored.calls.load(Ordering::SeqCst), 0);
        let effects = c.effects.0.lock().unwrap();
        assert_eq!(effects.len(), 3);
        assert!(
            effects
                .iter()
                .all(|e| e.logical_version() == rf::LogicalVersion::new(3))
        );
        drop(effects);
        c.finish();
    }
}

#[test]
fn compiled_ordered_scan_terminal_without_artifact_unavailable_and_closed_pass_through() {
    for mode in [Bind::Live, Bind::Unavailable, Bind::Closed] {
        let mut c = Consumer::new(mode);
        if matches!(mode, Bind::Live) {
            c.script([rf::LivePollOutcome::Idle {
                latest_version: None,
                terminal: Some(rf::LiveTerminal::CompletedWithoutArtifact),
            }]);
        }
        c.bind();
        let before = c.tracker.current();
        for _ in 0..2 {
            let output = c.apply().unwrap().unwrap();
            assert!(Arc::ptr_eq(
                &output.columns()[0],
                &c.fixture.input.columns()[0]
            ));
            assert_eq!(c.tracker.current(), before);
        }
        assert!(c.effects.0.lock().unwrap().is_empty());
        assert_eq!(
            c.session.cursor.observed.lock().unwrap().len(),
            usize::from(matches!(mode, Bind::Live))
        );
        c.finish();
    }
}

#[test]
fn compiled_ordered_scan_refuses_invalid_subscription_binding_and_digest_with_first_cause() {
    let mut c = Consumer::new(Bind::Blocking);
    let error = c.evaluator.bind(&c.state).unwrap_err();
    contract_error(
        &error,
        "compiled ordered Scan received a non-live subscription",
    );
    c.state
        .error_state()
        .set_error(KernelFailure::DeadlineExceeded);
    assert_eq!(
        c.evaluator.bind(&c.state).unwrap_err().cause(),
        error.cause()
    );
    assert_eq!(c.apply().unwrap_err().cause(), error.cause());
    assert!(c.effects.0.lock().unwrap().is_empty());
    c.finish();
    for wrong_binding in [true, false] {
        let mut c = Consumer::new(Bind::Live);
        let query = LiteralQuery::new();
        let valid = c.snapshot(1, query.clone());
        let snapshot = Arc::new(rf::RuntimeFilterSnapshot::new(
            if wrong_binding {
                rf::RuntimeFilterBindingId::new(valid.binding_id().get().checked_add(1).unwrap())
            } else {
                valid.binding_id()
            },
            valid.logical_version(),
            if wrong_binding {
                valid.contract_digest()
            } else {
                let mut digest = valid.contract_digest();
                digest[0] ^= 1;
                digest
            },
            query.clone(),
        ));
        c.script([rf::LivePollOutcome::Updated {
            snapshot,
            terminal: None,
        }]);
        c.bind();
        let error = c.apply().unwrap_err();
        contract_error(
            &error,
            "compiled ordered Scan snapshot differs from its frozen binding or contract",
        );
        c.state.error_state().set_error(KernelFailure::Cancelled);
        assert_eq!(c.apply().unwrap_err().cause(), error.cause());
        assert_eq!(query.calls.load(Ordering::SeqCst), 0);
        assert!(c.effects.0.lock().unwrap().is_empty());
        drop(valid);
        c.finish();
    }
}

#[test]
fn compiled_ordered_scan_query_failure_publishes_no_effect_and_real_bind_refusal_refunds() {
    let mut c = Consumer::new(Bind::Live);
    let query = Arc::new(LiteralQuery {
        calls: AtomicUsize::new(0),
        failure: Some((3, KernelFailure::Cancelled)),
    });
    c.script([rf::LivePollOutcome::Updated {
        snapshot: c.snapshot(1, query.clone()),
        terminal: None,
    }]);
    c.bind();
    let error = c.apply().unwrap_err();
    assert_eq!(
        error.cause(),
        &ExecutionFailureCause::Kernel(KernelFailure::Cancelled)
    );
    assert_eq!(query.calls.load(Ordering::SeqCst), 4);
    assert!(
        c.effects.0.lock().unwrap().is_empty(),
        "a partial scalar evaluation never publishes an application effect"
    );
    c.state
        .error_state()
        .set_error(KernelFailure::DeadlineExceeded);
    assert_eq!(c.apply().unwrap_err().cause(), error.cause());
    c.finish();

    let mut c = Consumer::new(Bind::Live);
    let before = c.tracker.current();
    assert!(before > 0);
    c.tracker
        .install_limit_once(before.checked_add(1).unwrap())
        .unwrap();
    let error = c.evaluator.bind(&c.state).unwrap_err();
    assert_eq!(
        error.cause(),
        &ExecutionFailureCause::Kernel(KernelFailure::ResourceExhausted)
    );
    assert_eq!(
        c.tracker.current(),
        before,
        "failed driver slots refund before the first subscription"
    );
    assert!(c.session.requests.lock().unwrap().is_empty());
    assert_eq!(
        c.evaluator.bind(&c.state).unwrap_err().cause(),
        error.cause()
    );
    c.finish();
}

#[test]
fn compiled_ordered_scan_guard_refuses_a_legal_topk_reduction_without_rewriting_it() {
    use novarocks_local_program::{FilterReduction, StaticFilterConsumer};
    use std::num::NonZeroU32;

    let fixture = ordered_fixture();
    let scans = fixture
        .program
        .graph()
        .nodes()
        .iter()
        .filter_map(|node| match node.kind() {
            ProgramNodeKind::Scan {
                runtime_filters, ..
            } => Some(runtime_filters),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(scans.len(), 1);
    assert_eq!(scans[0].len(), 1);
    let original = &scans[0][0].consumer;
    CompiledOrderedScanFactory::validate_binding(original).unwrap();
    let reduction = FilterReduction::MergeTopKSummary {
        k: NonZeroU32::new(2).unwrap(),
    };
    // This is a legal frozen consumer declaration, not a forged LocalProgram.
    // The constructor uses this same guard; this leaf test does not qualify a
    // TopK producer, subscription or runtime implementation.
    let topk = StaticFilterConsumer::try_new(
        original.binding_id(),
        original.channel_id(),
        original.activation(),
        original.contract().clone(),
        reduction,
    )
    .unwrap();
    assert_eq!(topk.reduction(), reduction);
    let error = CompiledOrderedScanFactory::validate_binding(&topk).unwrap_err();
    assert!(matches!(
        error.cause(),
        ExecutionFailureCause::Kernel(KernelFailure::InvalidProgram(detail))
        if detail.message() == "compiled ordered Scan does not support this frozen reduction; TightenOrderedBound is required"
    ));
    assert_eq!(topk.reduction(), reduction);
    assert_eq!(topk.contract(), original.contract());
}

#[path = "compiled_ordered_source_tests.rs"]
mod source_tests;
