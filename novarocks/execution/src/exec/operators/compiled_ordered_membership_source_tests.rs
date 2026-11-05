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

//! Ordered and membership coexistence under the original checked component
//! rebuild author. This is NOT physical-compiler mixed-route, Worker feedback
//! or Native qualification. The real source, gate, key evaluators and creators
//! execute; local artifacts only declare literal scalar membership.
use super::*;
use crate::exec::operators::runtime_filter::CompiledRuntimeFilterConsumers;
use crate::exec::pipeline::schedule::observer::Observable;
use std::time::Duration;

struct NumericQuery(AtomicUsize);
impl RuntimeFilterArtifactQuery for NumericQuery {
    fn data_type(&self) -> &DataType {
        &DataType::Int64
    }
    fn controlled_scalar_query(&self) -> Option<&dyn RuntimeFilterControlledScalarQuery> {
        Some(self)
    }
    fn matches_null(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
        panic!("uncontrolled membership NULL port")
    }
    fn has_non_null_matches(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
        panic!("uncontrolled membership existence port")
    }
    fn non_null_value_may_match(
        &self,
        _: RuntimeFilterScalarRef<'_>,
    ) -> Result<bool, RuntimeFilterArtifactQueryError> {
        panic!("uncontrolled membership scalar port")
    }
    fn non_null_range_may_match(
        &self,
        _: &ConnectorScalarValue,
        _: &ConnectorScalarValue,
    ) -> Result<bool, RuntimeFilterArtifactQueryError> {
        panic!("uncontrolled membership range port")
    }
}
impl RuntimeFilterControlledScalarQuery for NumericQuery {
    fn matches_null_controlled(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<bool, RuntimeFilterControlledQueryError> {
        control.checkpoint(1)?;
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(false)
    }
    fn non_null_value_may_match_controlled(
        &self,
        value: RuntimeFilterScalarRef<'_>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<bool, RuntimeFilterControlledQueryError> {
        control.checkpoint(1)?;
        self.0.fetch_add(1, Ordering::SeqCst);
        let RuntimeFilterScalarRef::Int64(value) = value else {
            panic!("independently declared nullable Int64 membership key")
        };
        Ok(matches!(value, 7 | 1))
    }
}
// The original public blocking-subscription protocol, with one explicit
// publication. It authors no row, matching, gate or timeout computation.
struct Publication {
    outcome: Mutex<Option<rf::SnapshotAcquireOutcome>>,
    ready: Arc<Observable>,
    accepted: AtomicUsize,
}
impl Publication {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            outcome: Mutex::new(None),
            ready: Arc::new(Observable::new()),
            accepted: AtomicUsize::new(0),
        })
    }
    fn publish(&self, snapshot: Arc<rf::RuntimeFilterSnapshot>) {
        let mut outcome = self.outcome.lock().unwrap();
        assert!(outcome.is_none(), "one explicit publication");
        *outcome = Some(rf::SnapshotAcquireOutcome::Published(snapshot));
        drop(outcome);
        self.ready.notify_observers();
    }
}
impl rf::BlockingSnapshotSubscription for Publication {
    fn try_outcome(&self) -> Option<rf::SnapshotAcquireOutcome> {
        self.outcome.lock().unwrap().clone()
    }
    fn outcome_observable(&self) -> Arc<Observable> {
        self.ready.clone()
    }
    fn record_consumer_outcome(&self, outcome: &rf::SnapshotAcquireOutcome) {
        assert!(
            matches!(outcome, rf::SnapshotAcquireOutcome::Published(_)),
            "only the explicit published snapshot is accepted"
        );
        self.accepted.fetch_add(1, Ordering::SeqCst);
    }
    fn snapshot(&self) -> Option<Arc<rf::RuntimeFilterSnapshot>> {
        match self.outcome.lock().unwrap().as_ref() {
            Some(rf::SnapshotAcquireOutcome::Published(snapshot)) => Some(snapshot.clone()),
            None => None,
            _ => panic!("no other publication outcome was declared"),
        }
    }
}
struct MixedSession {
    ordered: Arc<ScanSession>,
    membership: rf::RuntimeFilterConsumerContract,
    publication: Arc<Publication>,
    requests: Mutex<Vec<rf::RuntimeFilterSubscriptionRequest>>,
}
impl rf::RuntimeFilterSession for MixedSession {
    fn open_producer(
        &self,
        _: rf::RuntimeFilterProducerOpenRequest,
    ) -> Result<
        rf::RuntimeFilterBindOutcome<rf::SharedRuntimeFilterProducer>,
        rf::RuntimeFilterContractViolation,
    > {
        panic!("source-only component has no producer calls")
    }
    fn open_final_domain_completion(
        &self,
        _: rf::RuntimeFilterFinalDomainOpenRequest,
    ) -> Result<
        rf::RuntimeFilterBindOutcome<rf::SharedRuntimeFilterFinalDomainCompletion>,
        rf::RuntimeFilterContractViolation,
    > {
        panic!("source-only component has no FinalDomain calls")
    }
    fn subscribe(
        &self,
        request: rf::RuntimeFilterSubscriptionRequest,
    ) -> Result<
        rf::RuntimeFilterBindOutcome<rf::RuntimeFilterSubscriptionPin>,
        rf::RuntimeFilterContractViolation,
    > {
        self.requests.lock().unwrap().push(request.clone());
        if request.contract().binding_id() == self.membership.binding_id() {
            assert_eq!(
                request.contract(),
                &self.membership,
                "the complete declared membership contract is preserved"
            );
            Ok(rf::RuntimeFilterBindOutcome::Bound(
                rf::RuntimeFilterSubscriptionPin::Blocking(self.publication.clone()),
            ))
        } else {
            rf::RuntimeFilterSession::subscribe(self.ordered.as_ref(), request)
        }
    }
}

#[test]
fn compiled_ordered_source_checked_component_membership_keeps_full_ordinal_gate_and_limit_order() {
    let mut fixture = super::super::super::ordered_fixture();
    let (program, scan, membership) =
        crate::exec::pipeline::builder::ordered_scan_with_numeric_membership_fixture(
            &fixture.program,
        )
        .unwrap();
    fixture.program = program;
    let mut consumer = Consumer::with_fixture(Bind::Live, fixture);
    let ordered_query = LiteralQuery::new();
    consumer.script([rf::LivePollOutcome::Updated {
        snapshot: consumer.snapshot(1, ordered_query.clone()),
        terminal: Some(rf::LiveTerminal::Completed),
    }]);
    let publication = Publication::new();
    let mixed = Arc::new(MixedSession {
        ordered: consumer.session.clone(),
        membership: membership.clone(),
        publication: publication.clone(),
        requests: Mutex::new(vec![]),
    });
    consumer.state = consumer
        .state
        .clone()
        .with_runtime_filter_session(Some(mixed.clone()));
    // Reindexing this membership root to zero would read the real Utf8 order
    // key. The original FVT guard must reject that mistake, not reinterpret it.
    let wrong = CompiledRuntimeFilterConsumers::try_new_at_ordinals(
        "Scan",
        consumer.fixture.program.clone(),
        scan,
        vec![(0, membership.clone())],
        consumer.state.error_state(),
    )
    .err()
    .expect("a Utf8 root is not the declared Int64 membership key");
    assert_eq!(
        novarocks_failure_contract::FailureProjection::failure_fact(&wrong).class(),
        novarocks_failure_contract::FailureClass::Invariant
    );
    assert!(
        wrong.detail().contains("not its membership type Int64"),
        "{wrong}"
    );
    let members = Arc::new(
        CompiledRuntimeFilterConsumers::try_new_at_ordinals(
            "Scan",
            consumer.fixture.program.clone(),
            scan,
            vec![(1, membership.clone())],
            consumer.state.error_state(),
        )
        .unwrap(),
    );
    let source = Arc::new(Source {
        pages: Pages::new(),
        claims: AtomicUsize::new(0),
        construction: consumer._creation.clone(),
    });
    let factory = StreamScanSourceFactory::new_compiled_ordered(
        7,
        Arc::new(BoundOp(source.clone())),
        Some(members.clone()),
        consumer._factory.clone(),
        None,
        Some(1),
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
    let mut case = SourceCase {
        consumer,
        source,
        _factory: factory,
        operator: OperatorInstance::new(0, operator, Some(tracker), runtime_error),
        turn,
    };
    case.push_original();
    let stopped = Arc::new(AtomicBool::new(false));
    let stop_notice = stopped.clone();
    let ticket = case
        .source
        .pages
        .operations
        .admit(Arc::new(move || {
            stop_notice.store(true, Ordering::SeqCst);
        }))
        .unwrap();
    case.bind();
    // Explicit component wait policy, independent of wall-clock deadlines.
    // No timeout is used to open the gate: only the publication below does so.
    members.state().set_wait_timeout(Duration::from_secs(300));
    assert!(case.pull().unwrap().is_none());
    assert_eq!(
        case.source.claims.load(Ordering::SeqCst),
        0,
        "the blocking membership gate holds the first claim"
    );
    assert_eq!(case.source.pages.polls.load(Ordering::SeqCst), 0);
    assert_eq!(ordered_query.calls.load(Ordering::SeqCst), 0);
    assert!(
        case.consumer
            .session
            .cursor
            .observed
            .lock()
            .unwrap()
            .is_empty()
    );
    assert!(case.consumer.effects.0.lock().unwrap().is_empty());
    let requests = mixed.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].contract(), &membership);
    drop(requests);
    let ordered_requests = case.consumer.session.requests.lock().unwrap();
    assert_eq!(ordered_requests.len(), 1);
    assert_eq!(
        ordered_requests[0].contract(),
        &case.consumer.session.expected
    );
    drop(ordered_requests);
    let numeric = Arc::new(NumericQuery(AtomicUsize::new(0)));
    let rf::RuntimeFilterExecutionContract::Membership(schema) = membership.contract() else {
        unreachable!()
    };
    publication.publish(Arc::new(rf::RuntimeFilterSnapshot::new(
        membership.binding_id(),
        rf::LogicalVersion::FIRST,
        schema.digest(),
        numeric.clone(),
    )));
    let original = Arc::downgrade(&case.consumer.fixture.input.columns()[0]);
    let authority = case
        .consumer
        .fixture
        .input
        .source_authority()
        .owner_identity();
    let mut output = case.pull().unwrap().unwrap();
    assert_eq!(publication.accepted.load(Ordering::SeqCst), 1);
    assert_eq!(ordered_query.calls.load(Ordering::SeqCst), 8);
    assert_eq!(
        numeric.0.load(Ordering::SeqCst),
        4,
        "membership sees only actual ordered-filter survivors"
    );
    assert_eq!(output.len(), 1);
    assert!(output.columns()[0].is_null(0));
    assert_eq!(
        output.columns()[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        7
    );
    assert_eq!(output.source_authority().owner_identity(), authority);
    assert!(output.take_memory_lease().is_none());
    let effects = case.consumer.effects.0.lock().unwrap();
    assert_eq!(effects.len(), 2);
    assert_eq!(
        (
            effects[0].binding_id(),
            effects[0].input_rows(),
            effects[0].output_rows()
        ),
        (case.consumer.session.expected.binding_id(), 8, 4)
    );
    assert_eq!(
        (
            effects[1].binding_id(),
            effects[1].input_rows(),
            effects[1].output_rows()
        ),
        (membership.binding_id(), 4, 2)
    );
    drop(effects);
    assert_eq!(case.source.claims.load(Ordering::SeqCst), 1);
    assert_eq!(
        case.source.pages.closes.load(Ordering::SeqCst),
        1,
        "LIMIT closes the actual stream after both filters"
    );
    assert!(stopped.load(Ordering::SeqCst));
    assert!(case.operator.is_finished());
    assert!(matches!(
        case.operator.pending_finish(),
        Some(FinishWatch::Notify(_))
    ));
    ticket.end(novarocks_spi::connector::read_stack::ConnectorOperationExit::Exited);
    case.poll_exit();
    assert!(case.operator.pending_finish().is_none());
    assert!(case.source.pages.operations.is_exited());
    let data = output.columns()[1].to_data();
    let last = data.buffers()[0].clone();
    assert!(
        !last.is_empty(),
        "the final numeric values Buffer is real nonzero stock"
    );
    drop(data);
    drop(output);
    let tracker = case.consumer.tracker.clone();
    drop(case);
    drop(members);
    drop(mixed);
    drop(publication);
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
